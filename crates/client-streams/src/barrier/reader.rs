//! Reads published cuts from the internal `__barrier_state` topic.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use crate::{
    barrier::cut::{BARRIER_STATE_TOPIC, BarrierCut, decode_cut_value, decode_key},
    error::StreamsClientError,
    runtime::io::{FetchedRec, IsolationLevel, RecordFetcher},
};

/// Reads the cuts a barrier coordinator published to `__barrier_state`.
///
/// The reader drives the runtime's own [`RecordFetcher`] with a manual read of
/// every partition of the topic, so it joins no consumer group and needs no new
/// broker RPC. The first read starts at offset `0` of each partition. A later
/// read continues where the previous one stopped, so it costs one fetch per
/// partition of the records the coordinator published since.
///
/// The reader keeps complete cuts only. A partial cut names partitions that
/// never received the epoch's marker, so a task that waited for one would wait
/// forever. That is why the coordinator publishes partial cuts at all: a reader
/// skips the epoch instead of stalling on markers that never arrive.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
///
/// use crabka_client_streams::{barrier::CutReader, runtime::RecordFetcher};
///
/// # async fn read(fetcher: Arc<dyn RecordFetcher>) -> Result<(), Box<dyn std::error::Error>> {
/// let mut reader = CutReader::new(fetcher);
/// if let Some(cut) = reader.latest_complete_cut("transactions").await? {
///     println!("epoch {} at {:?}", cut.epoch, cut.offsets);
/// }
/// # Ok(()) }
/// ```
pub struct CutReader {
    fetcher: Arc<dyn RecordFetcher>,
    topic: String,
    /// The partitions of the topic, discovered on the first read.
    partitions: Option<Vec<i32>>,
    /// The next offset to read in each partition.
    offsets: HashMap<i32, i64>,
    /// Complete cuts, as barrier group to epoch to cut.
    cuts: HashMap<String, BTreeMap<i64, BarrierCut>>,
}

impl CutReader {
    /// Creates a reader over the `__barrier_state` topic.
    #[must_use]
    pub fn new(fetcher: Arc<dyn RecordFetcher>) -> Self {
        Self::with_topic(fetcher, BARRIER_STATE_TOPIC)
    }

    /// Creates a reader over another topic than `__barrier_state`.
    #[must_use]
    pub fn with_topic(fetcher: Arc<dyn RecordFetcher>, topic: impl Into<String>) -> Self {
        Self {
            fetcher,
            topic: topic.into(),
            partitions: None,
            offsets: HashMap::new(),
            cuts: HashMap::new(),
        }
    }

    /// The complete cut of a barrier group with the highest epoch.
    ///
    /// The result is `None` when the topic holds no complete cut of that group.
    ///
    /// # Errors
    ///
    /// Returns the fetcher's error, or
    /// [`StreamsClientError::BarrierFormat`](crate::StreamsClientError::BarrierFormat)
    /// when a record does not match the frozen layout.
    pub async fn latest_complete_cut(
        &mut self,
        group: &str,
    ) -> Result<Option<BarrierCut>, StreamsClientError> {
        self.refresh().await?;
        Ok(self
            .cuts
            .get(group)
            .and_then(|by_epoch| by_epoch.values().next_back())
            .cloned())
    }

    /// The complete cuts of a barrier group above `epoch`, oldest first.
    ///
    /// Pass `-1` to get every complete cut. Partial cuts are left out.
    ///
    /// # Errors
    ///
    /// Returns the fetcher's error, or
    /// [`StreamsClientError::BarrierFormat`](crate::StreamsClientError::BarrierFormat)
    /// when a record does not match the frozen layout.
    pub async fn complete_cuts_after(
        &mut self,
        group: &str,
        epoch: i64,
    ) -> Result<Vec<BarrierCut>, StreamsClientError> {
        self.refresh().await?;
        Ok(self.cuts.get(group).map_or_else(Vec::new, |by_epoch| {
            by_epoch
                .range((epoch + 1)..)
                .map(|(_epoch, cut)| cut.clone())
                .collect()
        }))
    }

    /// The complete cut of one epoch, or `None` when the topic holds no such
    /// cut.
    ///
    /// # Errors
    ///
    /// Returns the fetcher's error, or
    /// [`StreamsClientError::BarrierFormat`](crate::StreamsClientError::BarrierFormat)
    /// when a record does not match the frozen layout.
    pub async fn complete_cut_at(
        &mut self,
        group: &str,
        epoch: i64,
    ) -> Result<Option<BarrierCut>, StreamsClientError> {
        self.refresh().await?;
        Ok(self
            .cuts
            .get(group)
            .and_then(|by_epoch| by_epoch.get(&epoch))
            .cloned())
    }

    /// Reads every partition of the topic up to its end.
    ///
    /// The first call discovers the partitions and starts at offset `0`. A later
    /// call resumes at the offset the previous one left.
    async fn refresh(&mut self) -> Result<(), StreamsClientError> {
        if self.partitions.is_none() {
            let mut partitions = self.fetcher.partitions(&self.topic).await?;
            partitions.sort_unstable();
            self.partitions = Some(partitions);
        }
        let partitions = self.partitions.clone().unwrap_or_default();
        for partition in partitions {
            let mut offset = *self.offsets.entry(partition).or_insert(0);
            loop {
                let batch = self
                    .fetcher
                    .fetch(
                        &self.topic,
                        partition,
                        offset,
                        IsolationLevel::ReadUncommitted,
                    )
                    .await?;
                if batch.records.is_empty() {
                    break;
                }
                let mut advanced = false;
                for record in &batch.records {
                    self.accept(record)?;
                    let next = record.offset + 1;
                    if next > offset {
                        offset = next;
                        advanced = true;
                    }
                }
                // Stop when no record moved the offset, so a fetcher that
                // replays one batch cannot spin here.
                if !advanced {
                    break;
                }
            }
            self.offsets.insert(partition, offset);
        }
        Ok(())
    }

    /// Applies one record of the topic.
    ///
    /// A record of another kind is skipped. A tombstone and a partial cut both
    /// drop the epoch, so neither can be adopted later.
    fn accept(&mut self, record: &FetchedRec) -> Result<(), StreamsClientError> {
        let Some(key) = record.key.as_deref() else {
            return Ok(()); // A record with no key carries no barrier state.
        };
        let key = decode_key(key)?;
        if !key.is_cut() {
            return Ok(());
        }
        let value = record.value.as_deref().filter(|value| !value.is_empty());
        let Some(value) = value else {
            self.forget(&key.group, key.epoch);
            return Ok(());
        };
        let cut = decode_cut_value(&key, value)?;
        if cut.complete() {
            self.cuts
                .entry(key.group)
                .or_default()
                .insert(key.epoch, cut);
        } else {
            self.forget(&key.group, key.epoch);
        }
        Ok(())
    }

    fn forget(&mut self, group: &str, epoch: i64) {
        if let Some(by_epoch) = self.cuts.get_mut(group) {
            by_epoch.remove(&epoch);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex as StdMutex;

    use assert2::{assert, check};
    use bytes::Bytes;

    use super::*;
    use crate::{
        barrier::testing::{CutRecord, cut_key, cut_value, key_of_kind},
        runtime::io::FetchBatch,
    };

    /// A fetcher that serves one scripted batch per `(partition, offset)` of the
    /// barrier state topic and an empty batch for anything else.
    #[derive(Default)]
    struct CutFetcher {
        partitions: Vec<i32>,
        batches: StdMutex<HashMap<(i32, i64), FetchBatch>>,
    }

    impl CutFetcher {
        /// Serves `records` from partition `0` at offset `0`.
        fn one_batch(records: Vec<FetchedRec>) -> Self {
            let mut fetcher = Self {
                partitions: vec![0],
                batches: StdMutex::new(HashMap::new()),
            };
            fetcher.partitions = vec![0];
            fetcher.script(0, 0, records);
            fetcher
        }

        /// Adds the batch that one partition serves at one offset.
        fn script(&self, partition: i32, offset: i64, records: Vec<FetchedRec>) {
            self.batches
                .lock()
                .unwrap()
                .insert((partition, offset), FetchBatch { records });
        }
    }

    #[async_trait::async_trait]
    impl RecordFetcher for CutFetcher {
        async fn fetch(
            &self,
            _topic: &str,
            partition: i32,
            offset: i64,
            _isolation: IsolationLevel,
        ) -> Result<FetchBatch, StreamsClientError> {
            Ok(self
                .batches
                .lock()
                .unwrap()
                .remove(&(partition, offset))
                .unwrap_or_default())
        }

        async fn partitions(&self, _topic: &str) -> Result<Vec<i32>, StreamsClientError> {
            Ok(self.partitions.clone())
        }
    }

    fn record(offset: i64, key: Bytes, value: Option<Bytes>) -> FetchedRec {
        FetchedRec {
            offset,
            key: Some(key),
            value,
            timestamp: -1,
        }
    }

    fn cut_record(offset: i64, cut: &CutRecord) -> FetchedRec {
        record(offset, cut_key(cut), Some(cut_value(cut)))
    }

    #[tokio::test]
    async fn reads_complete_cuts_in_epoch_order() {
        let first = CutRecord::simple("g", 1, &[("in", 0, 10)]);
        let second = CutRecord::simple("g", 2, &[("in", 0, 20)]);
        let other_group = CutRecord::simple("other", 3, &[("in", 0, 30)]);
        let fetcher = Arc::new(CutFetcher::one_batch(vec![
            cut_record(0, &first),
            cut_record(1, &second),
            cut_record(2, &other_group),
        ]));
        let mut reader = CutReader::new(fetcher);

        check!(
            reader.complete_cuts_after("g", -1).await.unwrap()
                == vec![first.expected(), second.expected()]
        );
        check!(reader.complete_cuts_after("g", 1).await.unwrap() == vec![second.expected()]);
        check!(reader.latest_complete_cut("g").await.unwrap() == Some(second.expected()));
        check!(reader.complete_cut_at("g", 1).await.unwrap() == Some(first.expected()));
        check!(reader.complete_cut_at("g", 9).await.unwrap() == None);
        check!(reader.latest_complete_cut("absent").await.unwrap() == None);
    }

    #[tokio::test]
    async fn never_adopts_a_partial_cut() {
        let partial = CutRecord::simple("g", 1, &[("in", 0, 10)]).partial(&[("in", 1)]);
        let complete = CutRecord::simple("g", 2, &[("in", 0, 20), ("in", 1, 7)]);
        let fetcher = Arc::new(CutFetcher::one_batch(vec![
            cut_record(0, &partial),
            cut_record(1, &complete),
        ]));
        let mut reader = CutReader::new(fetcher);

        check!(reader.complete_cuts_after("g", -1).await.unwrap() == vec![complete.expected()]);
        check!(reader.complete_cut_at("g", 1).await.unwrap() == None);
    }

    #[tokio::test]
    async fn a_republished_partial_cut_drops_the_epoch() {
        let complete = CutRecord::simple("g", 1, &[("in", 0, 10)]);
        let partial = CutRecord::simple("g", 1, &[("in", 0, 10)]).partial(&[("in", 1)]);
        let fetcher = Arc::new(CutFetcher::one_batch(vec![
            cut_record(0, &complete),
            cut_record(1, &partial),
        ]));
        let mut reader = CutReader::new(fetcher);
        check!(reader.complete_cuts_after("g", -1).await.unwrap() == vec![]);
    }

    #[tokio::test]
    async fn skips_other_kinds_tombstones_and_keyless_records() {
        let cut = CutRecord::simple("g", 1, &[("in", 0, 10)]);
        let fetcher = Arc::new(CutFetcher::one_batch(vec![
            record(
                0,
                key_of_kind(0, "g", -1),
                Some(Bytes::from_static(b"junk")),
            ),
            record(1, key_of_kind(1, "g", 1), Some(Bytes::from_static(b"junk"))),
            FetchedRec {
                offset: 2,
                key: None,
                value: Some(Bytes::from_static(b"junk")),
                timestamp: -1,
            },
            cut_record(3, &cut),
            record(4, cut_key(&cut), None),
        ]));
        let mut reader = CutReader::new(fetcher);
        // The group record, the injection start, and the keyless record are
        // skipped; the tombstone drops the cut the batch published before it.
        check!(reader.complete_cuts_after("g", -1).await.unwrap() == vec![]);
    }

    #[tokio::test]
    async fn resumes_at_the_offset_the_previous_read_left() {
        let first = CutRecord::simple("g", 1, &[("in", 0, 10)]);
        let second = CutRecord::simple("g", 2, &[("in", 0, 20)]);
        let fetcher = Arc::new(CutFetcher::one_batch(vec![cut_record(0, &first)]));
        let mut reader = CutReader::new(Arc::clone(&fetcher) as Arc<dyn RecordFetcher>);
        check!(reader.complete_cuts_after("g", -1).await.unwrap() == vec![first.expected()]);

        // The coordinator publishes another cut at offset 1. The next read picks
        // it up from there and keeps the cut the first read already saw.
        fetcher.script(0, 1, vec![cut_record(1, &second)]);
        check!(
            reader.complete_cuts_after("g", -1).await.unwrap()
                == vec![first.expected(), second.expected()]
        );
    }

    #[tokio::test]
    async fn reads_every_partition_of_the_topic() {
        let first = CutRecord::simple("g", 1, &[("in", 0, 10)]);
        let second = CutRecord::simple("g", 2, &[("in", 0, 20)]);
        let fetcher = Arc::new(CutFetcher {
            partitions: vec![0, 1],
            batches: StdMutex::new(HashMap::new()),
        });
        fetcher.script(0, 0, vec![cut_record(0, &first)]);
        fetcher.script(1, 0, vec![cut_record(0, &second)]);
        let mut reader = CutReader::new(fetcher);
        check!(
            reader.complete_cuts_after("g", -1).await.unwrap()
                == vec![first.expected(), second.expected()]
        );
    }

    #[tokio::test]
    async fn a_malformed_record_fails_the_read() {
        let fetcher = Arc::new(CutFetcher::one_batch(vec![record(
            0,
            Bytes::from_static(b"\x00\x00"),
            Some(Bytes::from_static(b"x")),
        )]));
        let mut reader = CutReader::new(fetcher);
        assert!(let Err(error) = reader.latest_complete_cut("g").await);
        check!(error.to_string().contains("truncated barrier state key"));
    }
}
