//! Streams group membership: the `StreamsGroupHeartbeat` lifecycle and the
//! assignments.
//!
//! The heartbeat client is a Kafka network client and exists only on native
//! targets. The assignment value types build everywhere, so an embedder on
//! `wasm32-unknown-unknown` can describe the tasks its own membership logic
//! assigns.

#[cfg(not(target_family = "wasm"))]
mod assignment;
#[cfg(not(target_family = "wasm"))]
mod client;
#[cfg(not(target_family = "wasm"))]
pub(crate) mod coordinator;
#[cfg(not(target_family = "wasm"))]
mod status;
mod types;

#[cfg(not(target_family = "wasm"))]
pub use client::{
    DEFAULT_STREAMS_JOIN_RETRY_BACKOFF, DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT,
    DEFAULT_STREAMS_REBALANCE_TIMEOUT, SchemaPrewarm, StreamsJoinRetryBackoff,
    StreamsLeaveHeartbeatTimeout, StreamsMembership, StreamsRebalanceTimeout,
};
pub use types::{
    StreamsAssignment, StreamsEvent, StreamsStatus, TaskAssignment, TaskOffsetTracker,
    TopicPartition,
};
