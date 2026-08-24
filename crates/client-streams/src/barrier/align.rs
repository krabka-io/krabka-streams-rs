//! Barrier configuration, the barrier a runtime reached, and the callback.

use std::sync::Arc;

use crate::{
    barrier::cut::BarrierCut, runtime::iqv2::request::Position, store::snapshot::SnapshotStore,
};

/// The barrier a stream thread reached: the cut, the tasks it snapshotted, and
/// the offsets it committed.
///
/// Every committed offset is a marker offset of the cut, so the committed
/// position of each aligned partition **is** the cut.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Barrier {
    /// The cut the thread aligned on.
    pub cut: BarrierCut,
    /// The tasks whose stores the thread snapshotted, as `<subtopology>-<partition>`
    /// and in ascending order.
    pub tasks: Vec<String>,
    /// The offsets the thread committed, as topic to partition to offset.
    pub offsets: Position,
}

/// Receives a barrier after a stream thread commits at a cut.
///
/// The thread calls the listener once per epoch. The snapshot of every task is
/// already in the snapshot store, and the committed position of every aligned
/// partition is the cut. The call runs on the thread that drives the runtime, so
/// keep the work short.
///
/// Any `Fn(&Barrier)` that is `Send + Sync + 'static` is a listener.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use crabka_client_streams::barrier::{Barrier, BarrierListener};
///
/// let listener: Arc<dyn BarrierListener> =
///     Arc::new(|barrier: &Barrier| println!("aligned on epoch {}", barrier.cut.epoch));
/// ```
pub trait BarrierListener: Send + Sync + 'static {
    /// Reports that every assigned partition reached the cut.
    fn on_barrier(&self, barrier: &Barrier);
}

impl<F> BarrierListener for F
where
    F: Fn(&Barrier) + Send + Sync + 'static,
{
    fn on_barrier(&self, barrier: &Barrier) {
        self(barrier);
    }
}

/// Tells a stream thread which barrier group to align on.
///
/// Pass one to [`StreamsApp`](crate::StreamsApp) or to
/// [`KafkaStreams`](crate::KafkaStreams). The thread then reads the group's cuts
/// from `__barrier_state`, holds back every record at or above each partition's
/// marker offset, and fires the barrier when every assigned partition reaches
/// the cut. At that point it snapshots each task under the cut's epoch, commits
/// the cut offsets, and calls the listener.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use crabka_client_streams::{
///     barrier::{Barrier, BarrierAlignment},
///     store::snapshot::FileSnapshotStore,
/// };
///
/// let snapshots = Arc::new(FileSnapshotStore::new("/var/lib/app/state"));
/// let alignment = BarrierAlignment::on("transactions", snapshots).with_listener(Arc::new(
///     |barrier: &Barrier| {
///         println!("epoch {}", barrier.cut.epoch);
///     },
/// ));
/// ```
#[derive(Clone)]
pub struct BarrierAlignment {
    group: String,
    snapshots: Arc<dyn SnapshotStore>,
    listener: Option<Arc<dyn BarrierListener>>,
}

impl BarrierAlignment {
    /// Aligns on the cuts of `group` and keeps each cut's snapshot in
    /// `snapshots`.
    #[must_use]
    pub fn on(group: impl Into<String>, snapshots: Arc<dyn SnapshotStore>) -> Self {
        Self {
            group: group.into(),
            snapshots,
            listener: None,
        }
    }

    /// Adds the callback the thread calls after each barrier commit.
    #[must_use]
    pub fn with_listener(mut self, listener: Arc<dyn BarrierListener>) -> Self {
        self.listener = Some(listener);
        self
    }

    /// The barrier group this alignment reads.
    #[must_use]
    pub fn group(&self) -> &str {
        &self.group
    }

    pub(crate) fn snapshots(&self) -> &Arc<dyn SnapshotStore> {
        &self.snapshots
    }

    pub(crate) fn listener(&self) -> Option<&Arc<dyn BarrierListener>> {
        self.listener.as_ref()
    }
}

impl std::fmt::Debug for BarrierAlignment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The snapshot store is a trait object with no `Debug` bound.
        f.debug_struct("BarrierAlignment")
            .field("group", &self.group)
            .field("listener", &self.listener.is_some())
            .finish_non_exhaustive()
    }
}
