//! Broker-backed execution runtime for built streams topologies.
//!
//! The I/O traits, the global-store manager and the `IQv2` query types build
//! for every target. The stream thread, its tasks, the broker adapters and the
//! `KafkaStreams` supervisor drive Kafka network clients, so they exist only on
//! native targets; on `wasm32-unknown-unknown` an embedder drives a task's graph
//! through [`EmbeddedTask`](crate::EmbeddedTask) instead.

#[cfg(not(target_family = "wasm"))]
mod app;
#[cfg(not(target_family = "wasm"))]
pub(crate) mod clock;
#[cfg(not(target_family = "wasm"))]
pub(crate) mod eos;
pub(crate) mod global;
pub mod io;
#[cfg(not(target_family = "wasm"))]
mod io_broker;
#[cfg(not(target_family = "wasm"))]
pub(crate) mod iq;
#[cfg(not(target_family = "wasm"))]
mod iq_view;
pub mod iqv2;
#[cfg(not(target_family = "wasm"))]
mod task;
#[cfg(not(target_family = "wasm"))]
mod thread;

#[cfg(not(target_family = "wasm"))]
pub use app::{
    DEFAULT_STREAMS_COMMIT_INTERVAL, DEFAULT_STREAMS_INTERACTIVE_QUERY_QUEUE_CAPACITY,
    DEFAULT_STREAMS_POLL_INTERVAL, DEFAULT_STREAMS_STATE_STORE_CACHE_MAX_BYTES, KafkaStreams,
    MAX_STREAMS_STATE_STORE_CACHE_MAX_BYTES, StreamsCommitInterval,
    StreamsInteractiveQueryQueueCapacity, StreamsPollInterval, StreamsStateStoreCacheMaxBytes,
};
pub use io::{FetchBatch, FetchedRec, IsolationLevel, OffsetStore, RecordFetcher, RecordProducer};
#[cfg(not(target_family = "wasm"))]
pub use iq_view::{ReadOnlyKeyValueStore, ReadOnlySessionStore, ReadOnlyWindowStore};
