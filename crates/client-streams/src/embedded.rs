//! Drives one task's processor graph for an embedder that owns the I/O.
//!
//! A simulator, a browser playground, or another host that talks to brokers it
//! controls fetches the source records itself and produces what the graph
//! emits. [`EmbeddedTask`] gives it the same graph that the native runtime's
//! stream task runs, instantiated from the same [`BuiltTopology`], so a
//! topology behaves the same in either. The embedder plays the runtime's part:
//! it pipes each fetched record, produces the sink [`OutputRecord`]s and the
//! [`ChangelogRecord`]s, replays a changelog to restore a store, and drives the
//! punctuation clocks.
//!
//! Every method is synchronous. The graph's futures are async because a store
//! backend may be, but the in-memory backend this module opens never waits, so
//! each future completes on its first poll and `pollster::block_on` returns
//! without parking a thread. That is what lets the type run on
//! `wasm32-unknown-unknown`, where no thread can block. A task is
//! single-threaded: nothing runs in the background, and each method borrows the
//! task mutably for as long as it drives the graph.

use std::collections::HashSet;

use bytes::Bytes;
use krabka_units::prelude::*;

pub use crate::processor::erased::OutputRecord;
use crate::{
    error::StreamsClientError,
    processor::{erased::ProcessorError, graph::Graph},
    runtime::global::GlobalStateManager,
    store::{backend::StoreBackend, snapshot::TaskSnapshot},
    topology::BuiltTopology,
};

/// One record a state store logged, for the embedder to produce to the
/// changelog topic.
///
/// The native runtime pins every changelog record to the task's partition, so
/// a restore reads it back from the same partition the task processes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangelogRecord {
    /// The changelog topic of the store.
    pub topic: String,
    /// The serialized store key.
    pub key: Bytes,
    /// The serialized store value, or `None` for a tombstone.
    pub value: Option<Bytes>,
    /// The record timestamp to produce with, or `None` for the producer
    /// default, which is the send time. A versioned store sets the version
    /// timestamp (KIP-889).
    pub timestamp: Option<i64>,
}

/// One task's processor graph, driven by an embedder instead of the runtime.
///
/// The graph runs with in-memory stores and with the record cache disabled,
/// which is the JVM `TopologyTestDriver` default, so a store emits on every
/// update. Changelog topics derive from the built topology's application id,
/// exactly as [`BuiltTopology::changelog_topics`] lists them.
///
/// A KIP-1071 task is one `(subtopology, partition)` of the group's
/// assignment, and [`for_subtopology`](Self::for_subtopology) instantiates
/// exactly that subtopology's graph. [`new`](Self::new) instantiates every
/// subtopology, which is the task itself when the topology has one.
///
/// # Examples
///
/// ```
/// use assert2::assert;
/// use krabka_client_streams::{EmbeddedTask, NodeHandle, Record, Topology, impl_processor};
///
/// struct Upper;
/// impl_processor! {
///     impl Upper: (String, String) -> (String, String) {
///         async fn process(&mut self, ctx, r) {
///             ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
///         }
///     }
/// }
///
/// let mut topo = Topology::new();
/// let src: NodeHandle<String, String> = topo.add_source("src", ["in"]);
/// let up = topo.add_processor("up", || Upper, [&src]);
/// topo.add_sink("out", "out", [&up]);
/// let built = topo.build("app").unwrap();
///
/// let mut task = EmbeddedTask::new(&built).unwrap();
/// task.pipe("in", 0, 0, Some(b"k"), b"hello", 0).unwrap();
/// let out = task.take_output();
/// assert!(out[0].value.as_deref() == Some(&b"HELLO"[..]));
/// ```
pub struct EmbeddedTask {
    graph: Graph,
    /// The topology's source topics. A store whose changelog topic is one of
    /// these is a `REUSE_KTABLE_SOURCE_TOPICS` reuse-source store, and the task
    /// never hands its changelog back for production, because the source topic
    /// already holds those records.
    reuse_source_topics: HashSet<String>,
}

impl EmbeddedTask {
    /// Instantiates the whole topology's graph, every subtopology included,
    /// with in-memory stores, and calls `init` on every processor.
    ///
    /// Use it for a topology with one subtopology, where the task is the
    /// topology. A KIP-1071 task of a topology with several subtopologies
    /// must use [`for_subtopology`](Self::for_subtopology): a graph that
    /// holds every subtopology runs every branch that reads a piped topic, so
    /// two tasks of different subtopologies over the same source topic would
    /// each emit the other's sink output and store updates, and each would
    /// hold the other's stores and fire its punctuators.
    ///
    /// # Errors
    ///
    /// Returns the [`ProcessorError`] that instantiation or a processor's
    /// `init` raises. A topology that [`Topology::build`](crate::Topology::build)
    /// accepted raises none in practice.
    pub fn new(built: &BuiltTopology) -> Result<Self, ProcessorError> {
        let reuse_source_topics = built.list_source_topics().into_iter().collect();
        Self::instantiate(built, built.application_id(), reuse_source_topics, None)
    }

    /// Instantiates the graph of one subtopology, with in-memory stores, and
    /// calls `init` on its processors.
    ///
    /// The graph holds that subtopology's sources, processors and sinks, and
    /// the stores its processors connect to, and nothing of any other
    /// subtopology. This is the graph of a KIP-1071 task `(subtopology_id,
    /// partition)`; [`BuiltTopology::subtopology_ids`] lists the ids and
    /// [`BuiltTopology::source_topics_for`] the topics to pipe.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessorError::UnknownSubtopology`] when the topology has no
    /// subtopology `subtopology_id`, else what [`new`](Self::new) returns.
    pub fn for_subtopology(
        built: &BuiltTopology,
        subtopology_id: &str,
    ) -> Result<Self, ProcessorError> {
        let reuse_source_topics = built.list_source_topics().into_iter().collect();
        Self::instantiate(
            built,
            built.application_id(),
            reuse_source_topics,
            Some(subtopology_id),
        )
    }

    /// Instantiates the graph of `subtopology`, or of every subtopology when it
    /// is `None`, under `application_id`, which sets the changelog topic names,
    /// and with the given reuse-source suppression set.
    ///
    /// [`TopologyTestDriver`](crate::TopologyTestDriver) uses a fixed
    /// application id and an empty set, so a test sees every changelog record.
    pub(crate) fn instantiate(
        built: &BuiltTopology,
        application_id: &str,
        reuse_source_topics: HashSet<String>,
        subtopology: Option<&str>,
    ) -> Result<Self, ProcessorError> {
        let backend = StoreBackend::InMemory;
        // A zero cache budget disables the record cache, so stores emit on every
        // update and the outputs are deterministic. This is the JVM
        // `TopologyTestDriver` default for `statestore.cache.max.bytes`.
        let mut graph = match subtopology {
            Some(id) => pollster::block_on(built.instantiate_subtopology(
                &backend,
                application_id,
                ByteSize::ZERO,
                id,
            )),
            None => pollster::block_on(built.instantiate(&backend, application_id, ByteSize::ZERO)),
        }?;
        // The fully-replicated global stores are shared across tasks in the
        // runtime. Each embedded task owns its own copy, and the embedder fills
        // it through `apply_global`.
        graph.globals = pollster::block_on(GlobalStateManager::build(
            built.global_store_factories(),
            built.global_store_topics(),
            &backend,
            application_id,
        ));
        pollster::block_on(graph.init_processors())?;
        Ok(Self {
            graph,
            reuse_source_topics,
        })
    }

    pub(crate) fn graph(&self) -> &Graph {
        &self.graph
    }

    pub(crate) fn graph_mut(&mut self) -> &mut Graph {
        &mut self.graph
    }

    /// Runs one source record through the graph.
    ///
    /// `partition` and `offset` are the record's provenance, and a processor
    /// reads them through its record context. The record raises the task's
    /// stream-time to its `timestamp` when it is later. A record on a topic no
    /// source reads is ignored. The sink outputs wait in
    /// [`take_output`](Self::take_output) and the store changelog records in
    /// [`drain_changelogs`](Self::drain_changelogs).
    ///
    /// # Errors
    ///
    /// Returns a [`ProcessorError`] when a source serde cannot deserialize the
    /// record, or a sink serde cannot serialize an output.
    pub fn pipe(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        key: Option<&[u8]>,
        value: &[u8],
        timestamp: i64,
    ) -> Result<(), ProcessorError> {
        pollster::block_on(
            self.graph
                .pipe(topic, partition, offset, key, value, timestamp),
        )
    }

    /// Takes the sink records the graph emitted since the last call, in
    /// emission order.
    ///
    /// A record for a repartition topic is in the list too. The embedder
    /// produces it like any other output, and the subtopology that reads the
    /// repartition topic gets it back through [`pipe`](Self::pipe).
    pub fn take_output(&mut self) -> Vec<OutputRecord> {
        self.graph.take_output()
    }

    /// Takes the changelog records the stores logged since the last call.
    ///
    /// A reuse-source store's records are dropped, not returned, because its
    /// changelog topic is the source topic itself. Restore writes log nothing
    /// while logging is off.
    pub fn drain_changelogs(&mut self) -> Vec<ChangelogRecord> {
        self.graph
            .drain_changelogs(&self.reuse_source_topics)
            .into_iter()
            .map(|(topic, key, value, timestamp)| ChangelogRecord {
                topic,
                key,
                value,
                timestamp,
            })
            .collect()
    }

    /// Applies one changelog record to the named store, as a restore does.
    ///
    /// A `value` of `None` is a tombstone. `timestamp` is the changelog record's
    /// timestamp; a versioned store inserts the version at it. Turn logging off
    /// with [`set_logging`](Self::set_logging) for the whole replay, so the
    /// restored writes are not logged again, and turn it back on before the
    /// first [`pipe`](Self::pipe). A store the graph does not hold is ignored.
    pub fn restore_apply(&mut self, store: &str, key: Bytes, value: Option<Bytes>, timestamp: i64) {
        pollster::block_on(self.graph.restore_apply(store, key, value, timestamp));
    }

    /// Applies one record of a global source topic to the named global store.
    ///
    /// A `GlobalKTable` store is fully replicated and has no changelog, so the
    /// embedder replays every partition of the source topic here, before the
    /// first record that joins against it. A `value` of `None` deletes the
    /// entry. [`BuiltTopology::global_store_topics`] maps each store to its
    /// topic.
    pub fn apply_global(&mut self, store: &str, key: Bytes, value: Option<Bytes>) {
        pollster::block_on(self.graph.globals.apply(store, key, value));
    }

    /// Turns changelog logging on or off for every store.
    pub fn set_logging(&mut self, on: bool) {
        self.graph.set_logging(on);
    }

    /// The task's stream-time: the highest record timestamp piped so far, or
    /// `i64::MIN` before the first record.
    #[must_use]
    pub fn stream_time(&self) -> i64 {
        self.graph.stream_time
    }

    /// Raises stream-time to `stream_time` when it is higher, then fires every
    /// due stream-time punctuator once at the current stream-time.
    ///
    /// The runtime fires stream-time punctuators after each fetched batch.
    /// Passing [`stream_time`](Self::stream_time) fires what is due without
    /// moving the clock. What a punctuator forwards waits in
    /// [`take_output`](Self::take_output) and
    /// [`drain_changelogs`](Self::drain_changelogs).
    ///
    /// # Errors
    ///
    /// Returns a [`ProcessorError`] when a sink serde cannot serialize a record
    /// a punctuator forwarded.
    pub fn punctuate_stream_time(&mut self, stream_time: i64) -> Result<(), ProcessorError> {
        pollster::block_on(self.graph.punctuate_stream_time(stream_time))
    }

    /// Sets the wall clock to `now_ms` and fires every due wall-clock
    /// punctuator once at that time.
    ///
    /// The runtime ticks the wall clock between polls; an embedder ticks it
    /// from its own clock. A schedule registered during the call stamps its
    /// first fire from `now_ms`.
    ///
    /// # Errors
    ///
    /// Returns a [`ProcessorError`] when a sink serde cannot serialize a record
    /// a punctuator forwarded.
    pub fn punctuate_wall_clock(&mut self, now_ms: i64) -> Result<(), ProcessorError> {
        pollster::block_on(self.graph.punctuate_wall_clock(now_ms))
    }

    /// Flushes every cached store and forwards its deduplicated changes.
    ///
    /// The runtime flushes before each commit. The embedded graph runs with the
    /// record cache disabled, so no store is cached and the call forwards
    /// nothing; it exists so an embedder can follow the runtime's commit
    /// sequence unchanged.
    ///
    /// # Errors
    ///
    /// Returns a [`ProcessorError`] when a node rejects a forwarded change.
    pub fn flush_caches(&mut self) -> Result<(), ProcessorError> {
        pollster::block_on(self.graph.flush_caches())
    }

    /// Wipes every store, for a clean-slate replay of the changelogs.
    pub fn clear_stores(&mut self) {
        pollster::block_on(self.graph.clear_stores());
    }

    /// Snapshots every store, keyed by store name, for a barrier cut.
    ///
    /// The payloads are the ones [`SnapshotStore`](crate::SnapshotStore) keeps,
    /// and [`restore_store_snapshots`](Self::restore_store_snapshots) takes the
    /// task back to them.
    pub fn snapshot_stores(&mut self) -> TaskSnapshot {
        pollster::block_on(self.graph.snapshot_stores())
    }

    /// Replaces every store with the snapshot taken at a cut.
    ///
    /// A store the snapshot does not name is wiped, so the task holds the state
    /// of the cut and nothing after it.
    ///
    /// # Errors
    ///
    /// Returns [`StreamsClientError::Snapshot`] when a payload does not match
    /// the format its store writes.
    pub fn restore_store_snapshots(
        &mut self,
        snapshot: &TaskSnapshot,
    ) -> Result<(), StreamsClientError> {
        pollster::block_on(self.graph.restore_store_snapshots(snapshot))
    }

    /// Calls `close` on every processor, in graph order, and drops the task.
    pub fn close(mut self) {
        pollster::block_on(self.graph.close_processors());
    }

    /// The names of the task's state stores, in ascending order.
    ///
    /// Global stores are not in the list; they belong to the application, not
    /// to a task.
    #[must_use]
    pub fn store_names(&self) -> Vec<String> {
        let mut names = self.graph.stores.names();
        names.sort();
        names
    }

    /// The changelog topic of a store, or `None` when the task has no such
    /// store or the store has logging off.
    #[must_use]
    pub fn store_changelog_topic(&self, store: &str) -> Option<String> {
        let topic = self.graph.stores.get(store)?.changelog_topic();
        (!topic.is_empty()).then(|| topic.to_string())
    }

    /// The first `limit` entries of a key-value store, as raw key and value
    /// bytes in ascending key order.
    ///
    /// The dump is for an inspector, and it scans the whole store before it
    /// cuts the list. A window, session or versioned store dumps nothing here,
    /// and neither does a store the task does not hold.
    #[must_use]
    pub fn dump_store(&self, store: &str, limit: usize) -> Vec<(Bytes, Bytes)> {
        let Some(query) = self.graph.stores.iq_get(store) else {
            return Vec::new();
        };
        let mut entries = pollster::block_on(query.iq_kv_all());
        entries.truncate(limit);
        entries
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::{assert, check};
    use async_trait::async_trait;
    use krabka_protocol::owned::{
        common::streams_group_heartbeat_request::{key_value::KeyValue, topic_info::TopicInfo},
        streams_group_heartbeat_request::{Subtopology, Topology as WireTopology},
    };

    use super::*;
    use crate::{
        dsl::StreamsBuilder,
        processor::{
            api::{Processor, ProcessorContext},
            punctuation::{PunctuationType, Punctuator},
            record::Record,
            serde::{I64Serde, StringSerde},
        },
        topology::{NodeHandle, Topology},
    };

    struct DropEmpty;
    #[async_trait]
    impl Processor<String, String, String, String> for DropEmpty {
        async fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, String>,
            r: Record<String, String>,
        ) {
            if !r.value.is_empty() {
                ctx.forward(r);
            }
        }
    }

    struct Upper;
    #[async_trait]
    impl Processor<String, String, String, String> for Upper {
        async fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, String>,
            r: Record<String, String>,
        ) {
            ctx.forward(Record::new(r.key, r.value.to_uppercase(), r.timestamp));
        }
    }

    /// Counts each value in the named store.
    struct Count(&'static str);
    #[async_trait]
    impl Processor<String, String, String, i64> for Count {
        async fn process(
            &mut self,
            ctx: &mut ProcessorContext<'_, '_, String, i64>,
            r: Record<String, String>,
        ) {
            let n = {
                let store = ctx.get_state_store::<String, i64>(self.0).unwrap();
                let n = store.get(&r.value).await.unwrap_or(0) + 1;
                store.put(r.value.clone(), n).await;
                n
            };
            ctx.forward(Record::new(Some(r.value), n, r.timestamp));
        }
    }

    /// `in` → drop empty values → uppercase → count per value → `out`, with the
    /// count in the logged store `counts`.
    fn filter_map_count() -> BuiltTopology {
        let mut t = Topology::new();
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let filter = t.add_processor("filter", || DropEmpty, [&src]);
        let map = t.add_processor("map", || Upper, [&filter]);
        let count = t.add_processor("count", || Count("counts"), [&map]);
        t.add_state_store("counts", StringSerde, I64Serde, [count.name()]);
        t.add_sink("out", "out", [&count]);
        t.build("app").unwrap()
    }

    /// Two subtopologies that share no node and both read `in`: `0` uppercases
    /// and counts in `counts-a` into `out-a`; `1` counts as is in `counts-b`
    /// into `out-b`.
    fn two_readers_of_one_topic() -> BuiltTopology {
        let mut t = Topology::new();
        let a: NodeHandle<String, String> = t.add_source("src-a", ["in"]);
        let upper = t.add_processor("upper", || Upper, [&a]);
        let count_a = t.add_processor("count-a", || Count("counts-a"), [&upper]);
        t.add_state_store("counts-a", StringSerde, I64Serde, [count_a.name()]);
        t.add_sink("out-a", "out-a", [&count_a]);
        let b: NodeHandle<String, String> = t.add_source("src-b", ["in"]);
        let count_b = t.add_processor("count-b", || Count("counts-b"), [&b]);
        t.add_state_store("counts-b", StringSerde, I64Serde, [count_b.name()]);
        t.add_sink("out-b", "out-b", [&count_b]);
        t.build("app").unwrap()
    }

    fn i64_bytes(n: i64) -> Bytes {
        Bytes::copy_from_slice(&n.to_be_bytes())
    }

    fn pipe_word(task: &mut EmbeddedTask, offset: i64, word: &str) {
        task.pipe("in", 0, offset, Some(b"k"), word.as_bytes(), offset)
            .unwrap();
    }

    #[test]
    fn pipes_a_record_and_reports_the_output_and_changelog_bytes() {
        let built = filter_map_count();
        let mut task = EmbeddedTask::new(&built).unwrap();

        pipe_word(&mut task, 0, "hello");
        check!(
            task.take_output()
                == vec![OutputRecord {
                    topic: "out".into(),
                    key: Some(Bytes::from_static(b"HELLO")),
                    value: Some(i64_bytes(1)),
                    timestamp: 0,
                }]
        );
        check!(
            task.drain_changelogs()
                == vec![ChangelogRecord {
                    topic: "app-counts-changelog".into(),
                    key: Bytes::from_static(b"HELLO"),
                    value: Some(i64_bytes(1)),
                    timestamp: None,
                }]
        );

        // The filter drops an empty value before it reaches the store.
        pipe_word(&mut task, 1, "");
        check!(task.take_output().is_empty());
        check!(task.drain_changelogs().is_empty());

        // A repeated word bumps the count, and each drain starts empty.
        pipe_word(&mut task, 2, "hello");
        check!(task.take_output()[0].value == Some(i64_bytes(2)));
        check!(task.drain_changelogs().len() == 1);
        check!(task.stream_time() == 2);
        task.close();
    }

    #[test]
    fn unknown_topics_are_ignored() {
        let built = filter_map_count();
        let mut task = EmbeddedTask::new(&built).unwrap();
        task.pipe("elsewhere", 0, 0, None, b"x", 5).unwrap();
        check!(task.take_output().is_empty());
        check!(task.stream_time() == 5);
    }

    #[test]
    fn restore_from_changelog_records_continues_the_count() {
        let built = filter_map_count();
        let mut task = EmbeddedTask::new(&built).unwrap();

        task.set_logging(false);
        task.restore_apply(
            "counts",
            Bytes::from_static(b"HELLO"),
            Some(i64_bytes(2)),
            7,
        );
        task.restore_apply("counts", Bytes::from_static(b"GONE"), Some(i64_bytes(9)), 8);
        task.restore_apply("counts", Bytes::from_static(b"GONE"), None, 9);
        task.set_logging(true);
        // A restore logs nothing back.
        check!(task.drain_changelogs().is_empty());
        check!(task.dump_store("counts", 10) == vec![(Bytes::from_static(b"HELLO"), i64_bytes(2))]);

        pipe_word(&mut task, 0, "hello");
        check!(task.take_output()[0].value == Some(i64_bytes(3)));
        check!(
            task.drain_changelogs()
                == vec![ChangelogRecord {
                    topic: "app-counts-changelog".into(),
                    key: Bytes::from_static(b"HELLO"),
                    value: Some(i64_bytes(3)),
                    timestamp: None,
                }]
        );
    }

    #[test]
    fn dump_store_lists_the_first_entries_in_key_order() {
        let built = filter_map_count();
        let mut task = EmbeddedTask::new(&built).unwrap();
        for (offset, word) in ["b", "a", "c", "a"].into_iter().enumerate() {
            pipe_word(&mut task, i64::try_from(offset).unwrap(), word);
        }
        check!(
            task.dump_store("counts", 2)
                == vec![
                    (Bytes::from_static(b"A"), i64_bytes(2)),
                    (Bytes::from_static(b"B"), i64_bytes(1)),
                ]
        );
        check!(task.dump_store("counts", 10).len() == 3);
        check!(task.dump_store("missing", 10).is_empty());
        check!(task.store_names() == vec!["counts".to_string()]);
        check!(task.store_changelog_topic("counts") == Some("app-counts-changelog".into()));
        check!(task.store_changelog_topic("missing") == None);
    }

    #[test]
    fn snapshots_rewind_the_stores_and_clear_wipes_them() {
        let built = filter_map_count();
        let mut task = EmbeddedTask::new(&built).unwrap();
        pipe_word(&mut task, 0, "a");
        pipe_word(&mut task, 1, "a");
        let at_cut = task.snapshot_stores();
        check!(at_cut.keys().cloned().collect::<Vec<_>>() == vec!["counts".to_string()]);

        pipe_word(&mut task, 2, "a");
        check!(task.dump_store("counts", 10) == vec![(Bytes::from_static(b"A"), i64_bytes(3))]);
        task.restore_store_snapshots(&at_cut).unwrap();
        check!(task.dump_store("counts", 10) == vec![(Bytes::from_static(b"A"), i64_bytes(2))]);

        task.clear_stores();
        check!(task.dump_store("counts", 10).is_empty());

        let mut malformed = TaskSnapshot::new();
        malformed.insert("counts".into(), Bytes::from_static(b"\x00"));
        assert!(let Err(StreamsClientError::Snapshot(_)) = task.restore_store_snapshots(&malformed));
    }

    #[test]
    fn a_reuse_source_changelog_is_not_handed_back() {
        // The optimized build reads `users` straight into a store whose
        // changelog is the source topic itself, so its records must never be
        // produced again.
        let b = StreamsBuilder::new();
        b.table::<String, String>("users", "users-store")
            .to_stream()
            .to("out");
        let built = b.build_optimized("app").unwrap();
        check!(built.changelog_topics() == [("users-store".to_string(), "users".to_string())]);

        let mut task = EmbeddedTask::new(&built).unwrap();
        task.pipe("users", 0, 0, Some(b"u1"), b"alice", 0).unwrap();
        check!(task.take_output().len() == 1);
        check!(task.drain_changelogs().is_empty());
        check!(
            task.dump_store("users-store", 10)
                == vec![(Bytes::from_static(b"u1"), Bytes::from_static(b"alice"))]
        );
    }

    #[test]
    fn apply_global_feeds_a_global_table_join() {
        let b = StreamsBuilder::new();
        let customers = b.global_table::<String, String>("customers", "customers-by-id");
        b.stream::<String, String>(["orders"])
            .left_join_global(
                &customers,
                |_order_id, customer_id| customer_id.clone(),
                |customer_id, customer| format!("{customer_id}|{}", customer.map_or("?", |c| c)),
            )
            .to("enriched");
        drop(customers);
        let built = b.build("app").unwrap();
        check!(
            built.global_store_topics()
                == [("customers-by-id".to_string(), "customers".to_string())]
                    .into_iter()
                    .collect()
        );

        let mut task = EmbeddedTask::new(&built).unwrap();
        task.apply_global(
            "customers-by-id",
            Bytes::from_static(b"c1"),
            Some(Bytes::from_static(b"Alice")),
        );
        task.pipe("orders", 0, 0, Some(b"o1"), b"c1", 0).unwrap();
        task.pipe("orders", 0, 1, Some(b"o2"), b"c2", 1).unwrap();
        let values: Vec<Option<Bytes>> = task.take_output().into_iter().map(|r| r.value).collect();
        check!(
            values
                == vec![
                    Some(Bytes::from_static(b"c1|Alice")),
                    Some(Bytes::from_static(b"c2|?")),
                ]
        );

        task.apply_global("customers-by-id", Bytes::from_static(b"c1"), None);
        task.pipe("orders", 0, 2, Some(b"o3"), b"c1", 2).unwrap();
        check!(task.take_output()[0].value == Some(Bytes::from_static(b"c1|?")));
    }

    struct EmitTs;
    #[async_trait]
    impl Punctuator<String, i64> for EmitTs {
        async fn punctuate(&mut self, ctx: &mut ProcessorContext<'_, '_, String, i64>, ts: i64) {
            ctx.forward(Record::new(None, ts, ts));
        }
    }

    /// Schedules one stream-time and one wall-clock punctuator, 10 ms each,
    /// and drops every record.
    struct Scheduler;
    #[async_trait]
    impl Processor<String, String, String, i64> for Scheduler {
        async fn init(&mut self, ctx: &mut ProcessorContext<'_, '_, String, i64>) {
            ctx.schedule(
                Duration::from_millis(10),
                PunctuationType::StreamTime,
                EmitTs,
            );
            ctx.schedule(
                Duration::from_millis(10),
                PunctuationType::WallClockTime,
                EmitTs,
            );
        }
        async fn process(
            &mut self,
            _ctx: &mut ProcessorContext<'_, '_, String, i64>,
            _r: Record<String, String>,
        ) {
        }
    }

    #[test]
    fn punctuators_fire_on_both_clocks() {
        let mut t = Topology::new();
        let src: NodeHandle<String, String> = t.add_source("src", ["in"]);
        let p = t.add_processor("p", || Scheduler, [&src]);
        t.add_sink("out", "out", [&p]);
        let built = t.build("app").unwrap();
        let mut task = EmbeddedTask::new(&built).unwrap();

        // A stream-time schedule first fires on the first record; firing at the
        // current stream-time moves nothing and emits once.
        task.pipe("in", 0, 0, None, b"v", 5).unwrap();
        task.punctuate_stream_time(task.stream_time()).unwrap();
        check!(
            task.take_output()
                == vec![OutputRecord {
                    topic: "out".into(),
                    key: None,
                    value: Some(i64_bytes(5)),
                    timestamp: 5,
                }]
        );
        task.punctuate_stream_time(task.stream_time()).unwrap();
        check!(task.take_output().is_empty());
        // Raising stream-time past the next boundary fires again, at the new
        // time.
        task.punctuate_stream_time(20).unwrap();
        check!(task.take_output()[0].value == Some(i64_bytes(20)));
        check!(task.stream_time() == 20);

        // A wall-clock schedule first fires one interval after registration,
        // which was at wall clock 0.
        task.punctuate_wall_clock(9).unwrap();
        check!(task.take_output().is_empty());
        task.punctuate_wall_clock(10).unwrap();
        check!(task.take_output()[0].value == Some(i64_bytes(10)));
    }

    #[test]
    fn for_subtopology_runs_only_that_subtopology() {
        let built = two_readers_of_one_topic();
        check!(built.subtopology_ids() == vec!["0".to_string(), "1".to_string()]);
        check!(built.source_topics_for("0") == ["in".to_string()]);
        check!(built.source_topics_for("1") == ["in".to_string()]);

        for (id, out_topic, out_key, store, changelog) in [
            ("0", "out-a", "HELLO", "counts-a", "app-counts-a-changelog"),
            ("1", "out-b", "hello", "counts-b", "app-counts-b-changelog"),
        ] {
            let mut task = EmbeddedTask::for_subtopology(&built, id).unwrap();
            check!(task.store_names() == vec![store.to_string()]);
            pipe_word(&mut task, 0, "hello");
            check!(
                task.take_output()
                    == vec![OutputRecord {
                        topic: out_topic.into(),
                        key: Some(Bytes::copy_from_slice(out_key.as_bytes())),
                        value: Some(i64_bytes(1)),
                        timestamp: 0,
                    }]
            );
            check!(
                task.drain_changelogs()
                    == vec![ChangelogRecord {
                        topic: changelog.into(),
                        key: Bytes::copy_from_slice(out_key.as_bytes()),
                        value: Some(i64_bytes(1)),
                        timestamp: None,
                    }]
            );
        }

        assert!(
            let Err(ProcessorError::UnknownSubtopology { .. }) =
                EmbeddedTask::for_subtopology(&built, "2")
        );
    }

    #[test]
    fn new_runs_every_subtopology() {
        let built = two_readers_of_one_topic();
        let mut task = EmbeddedTask::new(&built).unwrap();
        check!(task.store_names() == vec!["counts-a".to_string(), "counts-b".to_string()]);
        pipe_word(&mut task, 0, "hello");
        let mut outputs: Vec<(String, Option<Bytes>)> = task
            .take_output()
            .into_iter()
            .map(|r| (r.topic, r.key))
            .collect();
        outputs.sort();
        check!(
            outputs
                == vec![
                    ("out-a".to_string(), Some(Bytes::from_static(b"HELLO"))),
                    ("out-b".to_string(), Some(Bytes::from_static(b"hello"))),
                ]
        );
        check!(task.drain_changelogs().len() == 2);
    }

    #[test]
    fn for_subtopology_scopes_a_repartition_chain() {
        let built = two_subtopologies();

        // Subtopology 0 reads `in` and writes the repartition topic; it has no
        // store.
        let mut up = EmbeddedTask::for_subtopology(&built, "0").unwrap();
        check!(up.store_names().is_empty());
        up.pipe("in", 0, 0, Some(b"k"), b"word", 0).unwrap();
        let out = up.take_output();
        check!(out.len() == 1);
        check!(out[0].topic == "app-counts-repartition");
        check!(up.drain_changelogs().is_empty());

        // Subtopology 1 ignores `in`, reads the repartition topic and counts.
        let mut down = EmbeddedTask::for_subtopology(&built, "1").unwrap();
        check!(down.store_names() == vec!["counts".to_string()]);
        down.pipe("in", 0, 0, Some(b"k"), b"word", 0).unwrap();
        check!(down.take_output().is_empty());
        down.pipe(
            &out[0].topic,
            0,
            0,
            out[0].key.as_deref(),
            out[0].value.as_deref().unwrap_or_default(),
            out[0].timestamp,
        )
        .unwrap();
        check!(down.take_output()[0].topic == "out");
        check!(down.drain_changelogs()[0].topic == "app-counts-changelog");
    }

    #[test]
    fn pipe_reports_a_record_the_source_cannot_deserialize() {
        let mut t = Topology::new();
        let src: NodeHandle<String, i64> = t.add_source("src", ["in"]);
        t.add_sink("out", "out", [&src]);
        let built = t.build("app").unwrap();
        let mut task = EmbeddedTask::new(&built).unwrap();
        assert!(let Err(ProcessorError::Serde { .. }) = task.pipe("in", 0, 0, None, b"x", 0));
        check!(task.take_output().is_empty());
    }

    #[test]
    fn flush_caches_forwards_nothing_without_a_record_cache() {
        let built = filter_map_count();
        let mut task = EmbeddedTask::new(&built).unwrap();
        pipe_word(&mut task, 0, "hello");
        task.take_output();
        task.drain_changelogs();
        task.flush_caches().unwrap();
        check!(task.take_output().is_empty());
        check!(task.drain_changelogs().is_empty());
    }

    #[test]
    fn restore_apply_ignores_a_store_the_task_does_not_hold() {
        let built = filter_map_count();
        let mut task = EmbeddedTask::new(&built).unwrap();
        task.restore_apply("missing", Bytes::from_static(b"k"), Some(i64_bytes(1)), 0);
        check!(task.dump_store("missing", 10).is_empty());
        check!(task.drain_changelogs().is_empty());
        check!(task.store_names() == vec!["counts".to_string()]);
    }

    /// `in` → group by key → count → `out`, with a repartition in between, so
    /// the topology has two subtopologies, one repartition topic and one
    /// changelog.
    fn two_subtopologies() -> BuiltTopology {
        let b = StreamsBuilder::new();
        b.stream::<String, String>(["in"])
            .map(|k: &String, v: &String| (v.clone(), k.clone()))
            .group_by_key()
            .count("counts")
            .to_stream()
            .to("out");
        b.build("app").unwrap()
    }

    #[test]
    fn built_topology_lists_its_subtopologies_and_internal_topics() {
        let built = two_subtopologies();
        check!(built.subtopology_ids() == vec!["0".to_string(), "1".to_string()]);
        check!(built.repartition_topics() == vec!["app-counts-repartition".to_string()]);
        check!(
            built.changelog_topics()
                == [("counts".to_string(), "app-counts-changelog".to_string())]
        );
        check!(built.source_topics_for("1") == ["app-counts-repartition".to_string()]);

        let mut task = EmbeddedTask::new(&built).unwrap();
        // Subtopology 0 writes the repartition topic; the embedder produces it
        // and feeds it back to subtopology 1, whose count logs to the changelog.
        task.pipe("in", 0, 0, Some(b"k"), b"word", 0).unwrap();
        let out = task.take_output();
        check!(out.len() == 1);
        check!(out[0].topic == "app-counts-repartition");
        check!(task.drain_changelogs().is_empty());
        task.pipe(
            &out[0].topic,
            0,
            0,
            out[0].key.as_deref(),
            out[0].value.as_deref().unwrap_or_default(),
            out[0].timestamp,
        )
        .unwrap();
        check!(task.take_output()[0].topic == "out");
        check!(task.drain_changelogs()[0].topic == "app-counts-changelog");
    }

    #[test]
    fn wire_request_is_the_byte_exact_join_topology() {
        let built = filter_map_count();
        let expected = WireTopology {
            epoch: 0,
            subtopologies: vec![Subtopology {
                subtopology_id: "0".into(),
                source_topics: vec!["in".into()],
                source_topic_regex: Vec::new(),
                state_changelog_topics: vec![TopicInfo {
                    name: "app-counts-changelog".into(),
                    partitions: 0,
                    replication_factor: -1,
                    topic_configs: vec![
                        KeyValue {
                            key: "cleanup.policy".into(),
                            value: "compact".into(),
                            ..Default::default()
                        },
                        KeyValue {
                            key: "message.timestamp.type".into(),
                            value: "CreateTime".into(),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                repartition_sink_topics: Vec::new(),
                repartition_source_topics: Vec::new(),
                copartition_groups: Vec::new(),
                ..Default::default()
            }],
            ..Default::default()
        };
        check!(built.to_wire_request() == expected);
    }

    #[cfg(not(target_family = "wasm"))]
    #[test]
    fn wire_request_is_what_the_membership_client_sends() {
        let built = two_subtopologies();
        let heartbeat = crate::membership::coordinator::join_heartbeat(
            "app",
            "member",
            "process",
            None,
            Time::from_millis(30_000),
            &built,
        );
        check!(heartbeat.topology == Some(built.to_wire_request()));
    }
}
