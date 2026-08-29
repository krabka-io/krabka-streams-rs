//! Barrier cuts: an exact, reproducible point in every input at once.
//!
//! A krabka broker injects an epoch-stamped marker into every partition of a
//! named barrier group and publishes the resulting **cut**, one marker offset
//! per partition, to the internal topic `__barrier_state`. The marker is a Kafka
//! control record, so no consumer ever receives one. A task aligns on a cut by
//! comparing its consumed offsets against the cut's offsets, never by watching
//! for a marker.
//!
//! # Key Types
//!
//! - [`BarrierCut`] — one epoch's cut, with the marker offset of each partition.
//! - [`CutReader`] — reads published cuts from `__barrier_state`.
//! - [`BarrierAlignment`] — tells a runtime which group to align on, and where
//!   to keep each cut's snapshot.
//! - [`BarrierListener`] — the callback that fires when a barrier is reached.
//!
//! # Examples
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use krabka_client_streams::{barrier::BarrierAlignment, store::snapshot::FileSnapshotStore};
//!
//! # fn build() -> BarrierAlignment {
//! let snapshots = Arc::new(FileSnapshotStore::new("/var/lib/app/state"));
//! BarrierAlignment::on("transactions", snapshots)
//! # }
//! ```

mod align;
mod cut;
mod reader;
#[cfg(test)]
pub(crate) mod testing;

pub use align::{Barrier, BarrierAlignment, BarrierListener};
pub use cut::{BARRIER_STATE_TOPIC, BarrierCut, CutStatus, decode_barrier_cut};
pub use reader::CutReader;
