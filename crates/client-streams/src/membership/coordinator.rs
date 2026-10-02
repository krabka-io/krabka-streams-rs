//! Background `StreamsGroupHeartbeat` loop.
//!
//! This module matches `share/coordinator.rs`. A ticker and a `select!` race
//! each heartbeat against shutdown. The loop adopts the broker's epoch and
//! assignment, then echoes the owned tasks back, which is adopt-and-echo
//! reconciliation. It rejoins from epoch 0 on a fence, and it sends a leave
//! heartbeat with `member_epoch = -1` on shutdown. It emits every meaningful
//! change as a [`StreamsEvent`].
//!
//! The requests follow Kafka's `StreamsGroupHeartbeatRequestManager` and pass
//! the checks of `GroupCoordinatorService.throwIfStreamsGroupHeartbeatRequestIsInvalid`.
//! A join at epoch 0 carries the topology and three empty task lists. A later
//! heartbeat carries all three task lists when the owned tasks differ from the
//! last lists sent, and none of them otherwise.

use std::sync::Arc;

use krabka_client_core::{Client, ClientError};
use krabka_protocol::owned::{
    common::{
        streams_group_heartbeat_request::task_ids::TaskIds as ReqTaskIds,
        streams_group_heartbeat_response::task_ids::TaskIds as RespTaskIds,
    },
    streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
};
use krabka_units::prelude::*;
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use super::{
    assignment::resolve,
    status::map_status,
    types::{StreamsAssignment, StreamsEvent, TaskOffsetTracker},
};
use crate::topology::BuiltTopology;

const FENCED_MEMBER_EPOCH: i16 = 110;
const UNKNOWN_MEMBER_ID: i16 = 25;
const STALE_MEMBER_EPOCH: i16 = 113;

/// `LEAVE_GROUP_MEMBER_EPOCH`: the epoch of a member that leaves the group.
const LEAVE_GROUP_MEMBER_EPOCH: i32 = -1;

/// The heartbeat RPC that the coordinator depends on. The real [`Client`]
/// implements it. Tests inject a fake, so the loop runs without a broker.
#[async_trait::async_trait]
pub(crate) trait HeartbeatTransport: Send + Sync + 'static {
    async fn send_heartbeat(
        &self,
        req: StreamsGroupHeartbeatRequest,
    ) -> Result<StreamsGroupHeartbeatResponse, ClientError>;
}

#[async_trait::async_trait]
impl HeartbeatTransport for Client {
    async fn send_heartbeat(
        &self,
        req: StreamsGroupHeartbeatRequest,
    ) -> Result<StreamsGroupHeartbeatResponse, ClientError> {
        self.send(req).await
    }
}

/// State owned by the heartbeat task.
pub(crate) struct CoordinatorState<T: HeartbeatTransport> {
    pub client: T,
    pub group_id: String,
    pub member_id: String,
    pub process_id: String,
    pub instance_id: Option<String>,
    /// Rebalance deadline advertised to the coordinator. It is rendered as the
    /// raw `StreamsGroupHeartbeat.rebalance_timeout_ms` wire field.
    pub rebalance_timeout: Time,
    pub topology: Arc<BuiltTopology>,
    pub member_epoch: Arc<Mutex<i32>>,
    /// Owned tasks last adopted, echoed back as `active_tasks`.
    pub owned_active: Arc<Mutex<Vec<RespTaskIds>>>,
    /// Owned standby tasks last adopted, echoed back as `standby_tasks`.
    pub owned_standby: Arc<Mutex<Vec<RespTaskIds>>>,
    /// Owned warmup tasks last adopted, echoed back as `warmup_tasks`.
    pub owned_warmup: Arc<Mutex<Vec<RespTaskIds>>>,
    /// The task lists that the last steady-state heartbeat carried. It is
    /// `None` before the first one and after a failed heartbeat, so the next
    /// heartbeat carries the lists again. This is Kafka's
    /// `HeartbeatState.LastSentFields`.
    pub last_sent_tasks: Mutex<Option<TaskLists>>,
    /// Tracker containing current and end offsets of all tasks.
    pub tracker: Arc<Mutex<TaskOffsetTracker>>,
    pub heartbeat_interval: Time,
    pub leave_heartbeat_timeout: Time,
    pub events: mpsc::UnboundedSender<StreamsEvent>,
    /// The last assignment emitted. It suppresses duplicate `Assigned` events,
    /// because the broker re-sends `active_tasks: Some(...)` every heartbeat.
    pub last_assignment: tokio::sync::Mutex<StreamsAssignment>,
}

enum Outcome {
    Ok,
    Rejoin,
    Transient,
}

/// The active, standby, and warmup task lists of one heartbeat.
///
/// Kafka refuses a heartbeat that sends some of the three lists but not all of
/// them ("If one task-type is non-null, all must be non-null."), so they travel
/// as one value.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct TaskLists {
    active: Vec<ReqTaskIds>,
    standby: Vec<ReqTaskIds>,
    warmup: Vec<ReqTaskIds>,
}

impl TaskLists {
    /// The request fields `active_tasks`, `standby_tasks`, and
    /// `warmup_tasks`: all three present, or all three absent.
    fn into_fields(lists: Option<Self>) -> [Option<Vec<ReqTaskIds>>; 3] {
        match lists {
            Some(lists) => [Some(lists.active), Some(lists.standby), Some(lists.warmup)],
            None => [None, None, None],
        }
    }
}

/// The join heartbeat at epoch 0, as Kafka's `StreamsGroupHeartbeatRequestManager`
/// builds it for a member in the `JOINING` state.
///
/// Kafka refuses a join that omits the rebalance timeout or the topology, and
/// one whose task lists are absent or non-empty (`ActiveTasks must be empty
/// when (re-)joining.`), so the join sends three empty lists.
pub(crate) fn join_heartbeat(
    group_id: &str,
    member_id: &str,
    process_id: &str,
    instance_id: Option<String>,
    rebalance_timeout: Time,
    topology: &BuiltTopology,
) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: group_id.to_string(),
        member_id: member_id.to_string(),
        member_epoch: 0,
        process_id: Some(process_id.to_string()),
        instance_id,
        // The generated request field is raw `int32` milliseconds.
        rebalance_timeout_ms: rebalance_timeout.millis_i32(),
        topology: Some(topology.to_wire_request()),
        active_tasks: Some(Vec::new()),
        standby_tasks: Some(Vec::new()),
        warmup_tasks: Some(Vec::new()),
        ..Default::default()
    }
}

/// The leave heartbeat sent on shutdown. Kafka's `StreamsMembershipManager`
/// leaves with `LEAVE_GROUP_MEMBER_EPOCH` and keeps the instance id, and it
/// sends no task lists and no topology.
fn leave_heartbeat<T: HeartbeatTransport>(
    state: &CoordinatorState<T>,
) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: state.group_id.clone(),
        member_id: state.member_id.clone(),
        member_epoch: LEAVE_GROUP_MEMBER_EPOCH,
        instance_id: state.instance_id.clone(),
        ..Default::default()
    }
}

/// Drive the loop until `shutdown` fires, then leave.
#[tracing::instrument(
    name = "streams.coordinator.run",
    level = "info",
    skip_all,
    fields(group_id = %state.group_id, member_id = %state.member_id),
)]
pub(crate) async fn run<T: HeartbeatTransport>(
    state: CoordinatorState<T>,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(state.heartbeat_interval.to_std());
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = ticker.tick() => {}
        }
        tokio::select! {
            () = shutdown.cancelled() => break,
            outcome = heartbeat_once(&state) => match outcome {
                Outcome::Ok | Outcome::Transient => {}
                Outcome::Rejoin => {
                    reset_for_rejoin(&state).await;
                    let _ = state.events.send(StreamsEvent::Fenced);
                }
            },
        }
    }

    let leave = state.client.send_heartbeat(leave_heartbeat(&state));
    let _ = tokio::time::timeout(state.leave_heartbeat_timeout.to_std(), leave).await;
}

/// Drop the epoch, the owned tasks, and the offsets, so the next heartbeat
/// joins again from epoch 0.
async fn reset_for_rejoin<T: HeartbeatTransport>(state: &CoordinatorState<T>) {
    *state.member_epoch.lock().await = 0;
    state.owned_active.lock().await.clear();
    state.owned_standby.lock().await.clear();
    state.owned_warmup.lock().await.clear();
    {
        let mut lock = state.tracker.lock().await;
        lock.task_offsets.clear();
        lock.task_end_offsets.clear();
    }
    *state.last_assignment.lock().await = StreamsAssignment::default();
}

#[tracing::instrument(
    name = "streams.coordinator.heartbeat_once",
    level = "debug",
    skip_all,
    fields(group_id = %state.group_id, member_id = %state.member_id),
)]
async fn heartbeat_once<T: HeartbeatTransport>(state: &CoordinatorState<T>) -> Outcome {
    let epoch = *state.member_epoch.lock().await;

    // Kafka 4.3.1 rejects non-null TaskOffsets and TaskEndOffsets. The runtime's
    // changelog positions stay in the local tracker.
    let req = if epoch == 0 {
        // The first heartbeat after a join reports the owned tasks.
        *state.last_sent_tasks.lock().await = None;
        join_heartbeat(
            &state.group_id,
            &state.member_id,
            &state.process_id,
            state.instance_id.clone(),
            state.rebalance_timeout,
            &state.topology,
        )
    } else {
        let [active_tasks, standby_tasks, warmup_tasks] =
            TaskLists::into_fields(changed_task_lists(state).await);
        StreamsGroupHeartbeatRequest {
            group_id: state.group_id.clone(),
            member_id: state.member_id.clone(),
            member_epoch: epoch,
            process_id: Some(state.process_id.clone()),
            instance_id: state.instance_id.clone(),
            rebalance_timeout_ms: state.rebalance_timeout.millis_i32(),
            active_tasks,
            standby_tasks,
            warmup_tasks,
            ..Default::default()
        }
    };

    let outcome = send_and_adopt(state, req).await;
    if !matches!(outcome, Outcome::Ok) {
        // Kafka's `HeartbeatState.reset` on every failed heartbeat: the next
        // heartbeat reports the owned tasks again.
        *state.last_sent_tasks.lock().await = None;
    }
    outcome
}

/// The owned tasks, when they differ from the task lists that the last
/// steady-state heartbeat carried; `None` when that heartbeat already carried
/// them. Records the lists it returns as sent.
async fn changed_task_lists<T: HeartbeatTransport>(
    state: &CoordinatorState<T>,
) -> Option<TaskLists> {
    let owned = TaskLists {
        active: state
            .owned_active
            .lock()
            .await
            .iter()
            .map(resp_to_req)
            .collect(),
        standby: state
            .owned_standby
            .lock()
            .await
            .iter()
            .map(resp_to_req)
            .collect(),
        warmup: state
            .owned_warmup
            .lock()
            .await
            .iter()
            .map(resp_to_req)
            .collect(),
    };
    let mut last_sent = state.last_sent_tasks.lock().await;
    if last_sent.as_ref() == Some(&owned) {
        None
    } else {
        *last_sent = Some(owned.clone());
        Some(owned)
    }
}

/// Send one heartbeat and adopt the epoch and the assignment of a successful
/// response.
async fn send_and_adopt<T: HeartbeatTransport>(
    state: &CoordinatorState<T>,
    req: StreamsGroupHeartbeatRequest,
) -> Outcome {
    match state.client.send_heartbeat(req).await {
        Ok(r) if r.error_code == 0 => {
            *state.member_epoch.lock().await = r.member_epoch;
            emit_response(state, &r).await;
            Outcome::Ok
        }
        Ok(r)
            if r.error_code == FENCED_MEMBER_EPOCH
                || r.error_code == UNKNOWN_MEMBER_ID
                || r.error_code == STALE_MEMBER_EPOCH =>
        {
            tracing::warn!(
                error_code = r.error_code,
                "streams heartbeat fenced; rejoining"
            );
            Outcome::Rejoin
        }
        Ok(r) => {
            tracing::warn!(
                error_code = r.error_code,
                "unexpected streams heartbeat error"
            );
            Outcome::Transient
        }
        Err(e) => {
            tracing::warn!(error = %e, "streams heartbeat send failed");
            Outcome::Transient
        }
    }
}

/// Emit `NotReady` when a status is present, and emit `Assigned` when tasks are
/// present and have changed since the last emission. Then update the owned-active
/// set for the next echo.
#[tracing::instrument(
    name = "streams.coordinator.emit_response",
    level = "debug",
    skip_all,
    fields(group_id = %state.group_id, member_id = %state.member_id, member_epoch = r.member_epoch),
)]
async fn emit_response<T: HeartbeatTransport>(
    state: &CoordinatorState<T>,
    r: &StreamsGroupHeartbeatResponse,
) {
    if let Some(statuses) = &r.status
        && !statuses.is_empty()
    {
        let mapped = statuses.iter().map(map_status).collect();
        let _ = state.events.send(StreamsEvent::NotReady(mapped));
    }
    if let Some(tasks) = &r.active_tasks {
        *state.owned_active.lock().await = tasks.clone();
    }
    if let Some(tasks) = &r.standby_tasks {
        *state.owned_standby.lock().await = tasks.clone();
    }
    if let Some(tasks) = &r.warmup_tasks {
        *state.owned_warmup.lock().await = tasks.clone();
    }
    let mut last = state.last_assignment.lock().await;
    if let Some(ev) = assignment_event(r, &state.topology, &mut last) {
        let _ = state.events.send(ev);
    }
}

/// Build the assignment from a heartbeat response and decide whether it changed
/// since `last`. Returns the event to emit, or `None` when nothing changed.
/// An omitted role keeps its previous assignment; an explicit empty list clears it.
fn assignment_event(
    r: &StreamsGroupHeartbeatResponse,
    topology: &BuiltTopology,
    last: &mut StreamsAssignment,
) -> Option<StreamsEvent> {
    let mut assignment = last.clone();
    if let Some(tasks) = &r.active_tasks {
        assignment.active = resolve(Some(tasks), topology);
    }
    if let Some(tasks) = &r.standby_tasks {
        assignment.standby = resolve(Some(tasks), topology);
    }
    if let Some(tasks) = &r.warmup_tasks {
        assignment.warmup = resolve(Some(tasks), topology);
    }
    if assignment == *last {
        None
    } else {
        *last = assignment.clone();
        Some(StreamsEvent::Assigned(assignment))
    }
}

fn resp_to_req(t: &RespTaskIds) -> ReqTaskIds {
    ReqTaskIds {
        subtopology_id: t.subtopology_id.clone(),
        partitions: t.partitions.clone(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Mutex as StdMutex, time::Duration};

    use assert2::check;
    use krabka_protocol::owned::common::streams_group_heartbeat_response::task_ids::TaskIds as RespTaskIds2;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{
        membership::DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT,
        topology::{NodeHandle, Topology},
    };

    // ---------------------------------------------------------------------------
    // Fake transport
    // ---------------------------------------------------------------------------

    struct FakeTransport {
        responses: StdMutex<VecDeque<Result<StreamsGroupHeartbeatResponse, ClientError>>>,
        sent: Arc<StdMutex<Vec<StreamsGroupHeartbeatRequest>>>,
    }

    impl FakeTransport {
        fn new(responses: Vec<StreamsGroupHeartbeatResponse>) -> Self {
            Self {
                responses: StdMutex::new(responses.into_iter().map(Ok).collect()),
                sent: Arc::new(StdMutex::new(Vec::new())),
            }
        }

        fn sent_arc(&self) -> Arc<StdMutex<Vec<StreamsGroupHeartbeatRequest>>> {
            Arc::clone(&self.sent)
        }
    }

    #[async_trait::async_trait]
    impl HeartbeatTransport for FakeTransport {
        async fn send_heartbeat(
            &self,
            req: StreamsGroupHeartbeatRequest,
        ) -> Result<StreamsGroupHeartbeatResponse, ClientError> {
            self.sent.lock().unwrap().push(req);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Ok(ok_resp(7, vec![0])))
        }
    }

    // FakeTransport wrapped in Arc also implements the trait (for the run-loop test)
    #[async_trait::async_trait]
    impl HeartbeatTransport for Arc<FakeTransport> {
        async fn send_heartbeat(
            &self,
            req: StreamsGroupHeartbeatRequest,
        ) -> Result<StreamsGroupHeartbeatResponse, ClientError> {
            self.as_ref().send_heartbeat(req).await
        }
    }

    struct HangingLeaveTransport;

    #[async_trait::async_trait]
    impl HeartbeatTransport for HangingLeaveTransport {
        async fn send_heartbeat(
            &self,
            req: StreamsGroupHeartbeatRequest,
        ) -> Result<StreamsGroupHeartbeatResponse, ClientError> {
            if req.member_epoch == -1 {
                std::future::pending().await
            } else {
                Ok(ok_resp(7, vec![]))
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------

    fn built() -> Arc<BuiltTopology> {
        let mut t = Topology::new();
        let src: NodeHandle<bytes::Bytes, bytes::Bytes> = t.add_source("src", ["in"]);
        t.add_sink("snk", "out", [&src]);
        Arc::new(t.build("app").unwrap())
    }

    fn ok_resp(epoch: i32, active: Vec<i32>) -> StreamsGroupHeartbeatResponse {
        StreamsGroupHeartbeatResponse {
            error_code: 0,
            member_epoch: epoch,
            heartbeat_interval_ms: 1,
            active_tasks: Some(vec![RespTaskIds2 {
                subtopology_id: "0".into(),
                partitions: active,
                ..Default::default()
            }]),
            ..Default::default()
        }
    }

    fn err_resp(code: i16) -> StreamsGroupHeartbeatResponse {
        StreamsGroupHeartbeatResponse {
            error_code: code,
            ..Default::default()
        }
    }

    fn state_with<T: HeartbeatTransport>(
        client: T,
    ) -> (CoordinatorState<T>, mpsc::UnboundedReceiver<StreamsEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let st = CoordinatorState {
            client,
            group_id: "g".into(),
            member_id: "m".into(),
            process_id: "p".into(),
            instance_id: None,
            rebalance_timeout: secs(30),
            topology: built(),
            member_epoch: Arc::new(Mutex::new(7)),
            owned_active: Arc::new(Mutex::new(Vec::new())),
            owned_standby: Arc::new(Mutex::new(Vec::new())),
            owned_warmup: Arc::new(Mutex::new(Vec::new())),
            last_sent_tasks: Mutex::new(None),
            tracker: Arc::new(Mutex::new(TaskOffsetTracker::default())),
            heartbeat_interval: millis(1),
            leave_heartbeat_timeout: DEFAULT_STREAMS_LEAVE_HEARTBEAT_TIMEOUT.as_time(),
            events: tx,
            last_assignment: tokio::sync::Mutex::new(StreamsAssignment::default()),
        };
        (st, rx)
    }

    // ---------------------------------------------------------------------------
    // heartbeat_once tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn heartbeat_ok_adopts_epoch_and_emits_assignment() {
        let fake = FakeTransport::new(vec![ok_resp(9, vec![0, 1])]);
        let (st, mut rx) = state_with(fake);
        let outcome = heartbeat_once(&st).await;
        check!(matches!(outcome, Outcome::Ok));
        check!(*st.member_epoch.lock().await == 9);
        check!(matches!(rx.try_recv(), Ok(StreamsEvent::Assigned(_))));
    }

    #[tokio::test]
    async fn heartbeat_uses_configured_rebalance_timeout() {
        let fake = FakeTransport::new(vec![ok_resp(9, vec![0])]);
        let sent = fake.sent_arc();
        let (mut state, _rx) = state_with(fake);
        state.rebalance_timeout = secs(45);

        check!(matches!(heartbeat_once(&state).await, Outcome::Ok));
        check!(sent.lock().unwrap()[0].rebalance_timeout_ms == 45_000);
    }

    #[tokio::test]
    async fn heartbeat_keeps_task_offsets_local() {
        for epoch in [0, 7] {
            let fake = FakeTransport::new(vec![ok_resp(9, vec![0])]);
            let sent = fake.sent_arc();
            let (state, _rx) = state_with(fake);
            *state.member_epoch.lock().await = epoch;
            {
                let mut tracker = state.tracker.lock().await;
                tracker.task_offsets.insert(("0".into(), 0), 5);
                tracker.task_end_offsets.insert(("0".into(), 0), 10);
            }

            check!(matches!(heartbeat_once(&state).await, Outcome::Ok));
            {
                let sent = sent.lock().unwrap();
                check!(sent[0].task_offsets.is_none());
                check!(sent[0].task_end_offsets.is_none());
                check!(kafka_refusal(&sent[0]).is_none());
            }
            let tracker = state.tracker.lock().await;
            check!(tracker.task_offsets.get(&("0".into(), 0)) == Some(&5));
            check!(tracker.task_end_offsets.get(&("0".into(), 0)) == Some(&10));
        }
    }

    #[tokio::test]
    async fn heartbeat_fenced_member_epoch_requests_rejoin() {
        let fake = FakeTransport::new(vec![err_resp(110)]);
        let (st, _rx) = state_with(fake);
        check!(matches!(heartbeat_once(&st).await, Outcome::Rejoin));
    }

    #[tokio::test]
    async fn heartbeat_unknown_member_id_requests_rejoin() {
        let fake = FakeTransport::new(vec![err_resp(25)]);
        let (st, _rx) = state_with(fake);
        check!(matches!(heartbeat_once(&st).await, Outcome::Rejoin));
    }

    #[tokio::test]
    async fn heartbeat_stale_member_epoch_requests_rejoin() {
        let fake = FakeTransport::new(vec![err_resp(113)]);
        let (st, _rx) = state_with(fake);
        check!(matches!(heartbeat_once(&st).await, Outcome::Rejoin));
    }

    #[tokio::test]
    async fn heartbeat_unexpected_code_is_transient() {
        let fake = FakeTransport::new(vec![err_resp(99)]);
        let (st, _rx) = state_with(fake);
        check!(matches!(heartbeat_once(&st).await, Outcome::Transient));
    }

    #[tokio::test]
    async fn heartbeat_transport_error_is_transient() {
        struct ErrTransport;
        #[async_trait::async_trait]
        impl HeartbeatTransport for ErrTransport {
            async fn send_heartbeat(
                &self,
                _req: StreamsGroupHeartbeatRequest,
            ) -> Result<StreamsGroupHeartbeatResponse, ClientError> {
                Err(ClientError::Disconnected)
            }
        }
        let (st, _rx) = state_with(ErrTransport);
        check!(matches!(heartbeat_once(&st).await, Outcome::Transient));
    }

    #[tokio::test]
    async fn heartbeat_sends_topology_when_epoch_zero() {
        let fake = FakeTransport::new(vec![ok_resp(1, vec![])]);
        let sent = fake.sent_arc();
        let (st, _rx) = state_with(fake);
        *st.member_epoch.lock().await = 0;
        let _ = heartbeat_once(&st).await;
        let sent = sent.lock().unwrap();
        check!(sent[0].topology.is_some());
    }

    #[tokio::test]
    async fn heartbeat_echoes_owned_active_tasks() {
        use krabka_protocol::owned::common::streams_group_heartbeat_response::task_ids::TaskIds as RespTids;
        let fake = FakeTransport::new(vec![ok_resp(8, vec![0])]);
        let sent = fake.sent_arc();
        let (st, _rx) = state_with(fake);
        // Pre-populate owned_active
        *st.owned_active.lock().await = vec![RespTids {
            subtopology_id: "0".into(),
            partitions: vec![0, 1],
            ..Default::default()
        }];
        let _ = heartbeat_once(&st).await;
        let sent = sent.lock().unwrap();
        check!(sent[0].active_tasks.is_some());
    }

    // ---------------------------------------------------------------------------
    // Request shapes that Kafka accepts
    // ---------------------------------------------------------------------------

    /// The request checks of Kafka's
    /// `GroupCoordinatorService.throwIfStreamsGroupHeartbeatRequestIsInvalid`
    /// (Apache Kafka 4.1 through 4.3) that the shape of a heartbeat decides,
    /// with Kafka's messages. `None` when Kafka accepts the request.
    fn kafka_refusal(req: &StreamsGroupHeartbeatRequest) -> Option<&'static str> {
        if req.task_offsets.is_some() {
            return Some("TaskOffsets are not supported yet.");
        }
        if req.task_end_offsets.is_some() {
            return Some("TaskEndOffsets are not supported yet.");
        }
        // `throwIfNotEmptyCollection` refuses a null list as well as a
        // non-empty one.
        let not_empty =
            |tasks: &Option<Vec<ReqTaskIds>>| tasks.as_ref().is_none_or(|t| !t.is_empty());
        if req.member_id.trim().is_empty() {
            return Some("MemberId can't be empty.");
        }
        if req.group_id.trim().is_empty() {
            return Some("GroupId can't be empty.");
        }
        if req
            .instance_id
            .as_deref()
            .is_some_and(|id| id.trim().is_empty())
        {
            return Some("InstanceId can't be empty.");
        }
        if req.member_epoch == 0 {
            if req.rebalance_timeout_ms == -1 {
                return Some("RebalanceTimeoutMs must be provided in first request.");
            }
            if not_empty(&req.active_tasks) {
                return Some("ActiveTasks must be empty when (re-)joining.");
            }
            if not_empty(&req.standby_tasks) {
                return Some("StandbyTasks must be empty when (re-)joining.");
            }
            if not_empty(&req.warmup_tasks) {
                return Some("WarmupTasks must be empty when (re-)joining.");
            }
            if req.topology.is_none() {
                return Some("Topology must be non-null when (re-)joining.");
            }
        } else if req.member_epoch == -2 {
            if req.instance_id.is_none() {
                return Some("InstanceId can't be null.");
            }
        } else if req.member_epoch < -2 {
            return Some("MemberEpoch must be greater than or equal to -2.");
        }
        let present = [
            req.active_tasks.is_some(),
            req.standby_tasks.is_some(),
            req.warmup_tasks.is_some(),
        ];
        if present.contains(&true) && present.contains(&false) {
            return Some("If one task-type is non-null, all must be non-null.");
        }
        if req.member_epoch != 0 && req.topology.is_some() {
            return Some("Topology can only be provided when (re-)joining.");
        }
        None
    }

    fn task(subtopology_id: &str, partitions: Vec<i32>) -> ReqTaskIds {
        ReqTaskIds {
            subtopology_id: subtopology_id.into(),
            partitions,
            ..Default::default()
        }
    }

    /// The join that `state_with` sends at epoch 0.
    fn expected_join(instance_id: Option<&str>) -> StreamsGroupHeartbeatRequest {
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m".into(),
            member_epoch: 0,
            process_id: Some("p".into()),
            instance_id: instance_id.map(Into::into),
            rebalance_timeout_ms: 30_000,
            topology: Some(built().to_wire_request()),
            active_tasks: Some(vec![]),
            standby_tasks: Some(vec![]),
            warmup_tasks: Some(vec![]),
            ..Default::default()
        }
    }

    /// A steady-state heartbeat of `state_with` at `epoch`.
    fn expected_steady(epoch: i32, lists: Option<TaskLists>) -> StreamsGroupHeartbeatRequest {
        let [active_tasks, standby_tasks, warmup_tasks] = TaskLists::into_fields(lists);
        StreamsGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m".into(),
            member_epoch: epoch,
            process_id: Some("p".into()),
            rebalance_timeout_ms: 30_000,
            active_tasks,
            standby_tasks,
            warmup_tasks,
            ..Default::default()
        }
    }

    #[test]
    fn kafka_refusal_matches_kafka_on_the_shapes_it_refuses() {
        let cases = [
            (
                "join without task lists",
                StreamsGroupHeartbeatRequest {
                    active_tasks: None,
                    standby_tasks: None,
                    warmup_tasks: None,
                    ..expected_join(None)
                },
                Some("ActiveTasks must be empty when (re-)joining."),
            ),
            (
                "join with owned tasks",
                StreamsGroupHeartbeatRequest {
                    standby_tasks: Some(vec![task("0", vec![1])]),
                    ..expected_join(None)
                },
                Some("StandbyTasks must be empty when (re-)joining."),
            ),
            (
                "join without topology",
                StreamsGroupHeartbeatRequest {
                    topology: None,
                    ..expected_join(None)
                },
                Some("Topology must be non-null when (re-)joining."),
            ),
            (
                "join without rebalance timeout",
                StreamsGroupHeartbeatRequest {
                    rebalance_timeout_ms: -1,
                    ..expected_join(None)
                },
                Some("RebalanceTimeoutMs must be provided in first request."),
            ),
            (
                "heartbeat with only active tasks",
                StreamsGroupHeartbeatRequest {
                    active_tasks: Some(vec![task("0", vec![0])]),
                    ..expected_steady(3, None)
                },
                Some("If one task-type is non-null, all must be non-null."),
            ),
            (
                "heartbeat with topology",
                StreamsGroupHeartbeatRequest {
                    topology: Some(built().to_wire_request()),
                    ..expected_steady(3, None)
                },
                Some("Topology can only be provided when (re-)joining."),
            ),
            (
                "static leave without instance id",
                expected_steady(-2, None),
                Some("InstanceId can't be null."),
            ),
            (
                "heartbeat without member id",
                StreamsGroupHeartbeatRequest {
                    member_id: " ".into(),
                    ..expected_steady(3, None)
                },
                Some("MemberId can't be empty."),
            ),
            ("join", expected_join(None), None),
            (
                "heartbeat without task lists",
                expected_steady(3, None),
                None,
            ),
            (
                "heartbeat with empty task lists",
                expected_steady(3, Some(TaskLists::default())),
                None,
            ),
        ];
        for (name, req, refusal) in cases {
            check!(kafka_refusal(&req) == refusal, "{name}");
        }
    }

    #[test]
    fn kafka_refuses_non_null_task_offsets_even_when_empty() {
        for (req, refusal) in [
            (
                StreamsGroupHeartbeatRequest {
                    task_offsets: Some(vec![]),
                    ..expected_join(None)
                },
                "TaskOffsets are not supported yet.",
            ),
            (
                StreamsGroupHeartbeatRequest {
                    task_end_offsets: Some(vec![]),
                    ..expected_steady(3, None)
                },
                "TaskEndOffsets are not supported yet.",
            ),
        ] {
            check!(kafka_refusal(&req) == Some(refusal));
        }
    }

    #[test]
    fn join_heartbeat_is_the_join_that_kafka_accepts() {
        for instance_id in [None, Some("instance-1")] {
            let req = join_heartbeat(
                "g",
                "m",
                "p",
                instance_id.map(Into::into),
                secs(30),
                &built(),
            );
            check!(req == expected_join(instance_id));
            check!(kafka_refusal(&req) == None);
        }
    }

    #[tokio::test]
    async fn heartbeats_send_all_task_lists_on_join_and_change_and_none_otherwise() {
        let active = |partitions: Vec<i32>| TaskLists {
            active: vec![task("0", partitions)],
            ..TaskLists::default()
        };
        let unchanged = StreamsGroupHeartbeatResponse {
            member_epoch: 2,
            heartbeat_interval_ms: 1,
            ..Default::default()
        };
        // Each step: the response to the heartbeat that the step sends, and
        // that heartbeat.
        let steps = [
            ("join", ok_resp(1, vec![0, 1]), expected_join(None)),
            (
                "first steady heartbeat reports the adopted tasks",
                unchanged.clone(),
                expected_steady(1, Some(active(vec![0, 1]))),
            ),
            (
                "unchanged tasks are not reported",
                ok_resp(3, vec![0]),
                expected_steady(2, None),
            ),
            (
                "changed tasks are reported",
                err_resp(99),
                expected_steady(3, Some(active(vec![0]))),
            ),
            (
                "a failed heartbeat reports the tasks again",
                err_resp(FENCED_MEMBER_EPOCH),
                expected_steady(3, Some(active(vec![0]))),
            ),
            (
                "a fenced member joins again",
                unchanged,
                expected_join(None),
            ),
        ];
        let fake = FakeTransport::new(steps.iter().map(|(_, resp, _)| resp.clone()).collect());
        let sent = fake.sent_arc();
        let (st, _rx) = state_with(fake);
        *st.member_epoch.lock().await = 0;
        for (index, (name, _, expected)) in steps.into_iter().enumerate() {
            if matches!(heartbeat_once(&st).await, Outcome::Rejoin) {
                reset_for_rejoin(&st).await;
            }
            let req = sent.lock().unwrap()[index].clone();
            check!(req == expected, "{name}");
            check!(kafka_refusal(&req) == None, "{name}");
        }
    }

    #[tokio::test]
    async fn leave_heartbeat_carries_the_leave_epoch_and_instance_id() {
        for instance_id in [None, Some("instance-1")] {
            let (mut st, _rx) = state_with(FakeTransport::new(vec![]));
            st.instance_id = instance_id.map(Into::into);
            *st.owned_active.lock().await = vec![RespTaskIds2 {
                subtopology_id: "0".into(),
                partitions: vec![0],
                ..Default::default()
            }];
            let req = leave_heartbeat(&st);
            check!(
                req == StreamsGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: "m".into(),
                    member_epoch: -1,
                    instance_id: instance_id.map(Into::into),
                    ..Default::default()
                }
            );
            check!(kafka_refusal(&req) == None);
        }
    }

    #[tokio::test]
    async fn emit_response_sends_not_ready_for_status() {
        use krabka_protocol::owned::common::streams_group_heartbeat_response::status::Status;
        let fake = FakeTransport::new(vec![]);
        let (st, mut rx) = state_with(fake);
        let resp = StreamsGroupHeartbeatResponse {
            error_code: 0,
            member_epoch: 1,
            status: Some(vec![Status {
                status_code: 0,
                status_detail: "topo-stale".into(),
                ..Default::default()
            }]),
            ..Default::default()
        };
        emit_response(&st, &resp).await;
        check!(matches!(rx.try_recv(), Ok(StreamsEvent::NotReady(_))));
    }

    // ---------------------------------------------------------------------------
    // run-loop tests
    // ---------------------------------------------------------------------------

    #[tokio::test]
    async fn run_loop_heartbeats_then_leaves_on_shutdown() {
        let fake = Arc::new(FakeTransport::new(vec![ok_resp(8, vec![0, 1])]));
        let sent = fake.sent_arc();
        let (st, mut rx) = state_with(Arc::clone(&fake));
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(run(st, shutdown.clone()));
        // Deterministically wait for the Assigned event the first heartbeat
        // response produces, rather than racing a fixed sleep (which flakes
        // under the ~2-3x slowdown of coverage instrumentation).
        let ev = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("an Assigned event within 5s")
            .expect("event channel stays open");
        check!(matches!(ev, StreamsEvent::Assigned(_)));
        shutdown.cancel();
        handle.await.unwrap();
        // A leave heartbeat (member_epoch == -1) must have been sent on shutdown.
        let sent = sent.lock().unwrap();
        check!(sent.iter().any(|r| r.member_epoch == -1));
    }

    #[tokio::test]
    async fn run_loop_fenced_emits_fenced_event_and_resets_epoch() {
        // First response fences, subsequent ones succeed.
        let fake = Arc::new(FakeTransport::new(vec![err_resp(110)]));
        let (st, mut rx) = state_with(Arc::clone(&fake));
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(run(st, shutdown.clone()));
        // Deterministically wait for the Fenced event the error response
        // produces, rather than racing a fixed sleep (which flakes under the
        // ~2-3x slowdown of coverage instrumentation).
        let saw_fenced = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(ev) = rx.recv().await {
                if matches!(ev, StreamsEvent::Fenced) {
                    return true;
                }
            }
            false
        })
        .await
        .expect("a Fenced event within 5s");
        check!(saw_fenced);
        let sent = fake.sent_arc();
        tokio::time::timeout(Duration::from_secs(5), async {
            while sent.lock().unwrap().len() < 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("a rejoin heartbeat within 5s");
        shutdown.cancel();
        handle.await.unwrap();
        let sent = sent.lock().unwrap();
        check!(sent[1] == expected_join(None));
        for req in sent.iter() {
            check!(kafka_refusal(req) == None);
        }
    }

    #[tokio::test]
    async fn run_loop_shutdown_immediately_sends_leave() {
        let fake = Arc::new(FakeTransport::new(vec![]));
        let sent = fake.sent_arc();
        let (st, _rx) = state_with(Arc::clone(&fake));
        let shutdown = CancellationToken::new();
        // Cancel immediately before any tick
        shutdown.cancel();
        run(st, shutdown).await;
        let sent = sent.lock().unwrap();
        // Even with immediate shutdown, leave heartbeat must be sent
        check!(sent.iter().any(|r| r.member_epoch == -1));
    }

    #[tokio::test]
    async fn run_loop_bounds_stalled_leave_with_configured_timeout() {
        let (mut state, _rx) = state_with(HangingLeaveTransport);
        state.leave_heartbeat_timeout = millis(37);
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        tokio::time::timeout(Duration::from_secs(1), run(state, shutdown))
            .await
            .expect("configured leave timeout bounds shutdown");
    }

    // ---------------------------------------------------------------------------
    // assignment_event (existing tests preserved)
    // ---------------------------------------------------------------------------

    fn built_plain() -> BuiltTopology {
        let mut t = Topology::new();
        let src: NodeHandle<bytes::Bytes, bytes::Bytes> = t.add_source("src", ["in"]);
        t.add_sink("snk", "out", [&src]);
        t.build("app").unwrap()
    }

    fn resp_plain(active: Vec<i32>) -> StreamsGroupHeartbeatResponse {
        use krabka_protocol::owned::common::streams_group_heartbeat_response::task_ids::TaskIds;
        StreamsGroupHeartbeatResponse {
            active_tasks: Some(vec![TaskIds {
                subtopology_id: "0".into(),
                partitions: active,
                ..Default::default()
            }]),
            ..Default::default()
        }
    }

    #[test]
    fn identical_assignment_is_not_re_emitted() {
        let topo = built_plain();
        let mut last = StreamsAssignment::default();
        let r = resp_plain(vec![0, 1]);
        check!(assignment_event(&r, &topo, &mut last).is_some());
        check!(assignment_event(&r, &topo, &mut last).is_none());
    }

    #[test]
    fn omitted_assignment_roles_leave_the_previous_assignment_unchanged() {
        let topo = built_plain();
        let active = resp_plain(vec![0]).active_tasks;
        let standby = resp_plain(vec![1]).active_tasks;
        let warmup = resp_plain(vec![2]).active_tasks;
        let mut last = StreamsAssignment::default();
        let initial = StreamsGroupHeartbeatResponse {
            active_tasks: active,
            standby_tasks: standby,
            warmup_tasks: warmup,
            ..Default::default()
        };
        check!(assignment_event(&initial, &topo, &mut last).is_some());
        let previous = last.clone();

        check!(
            assignment_event(&StreamsGroupHeartbeatResponse::default(), &topo, &mut last).is_none()
        );
        check!(last == previous);
    }

    #[test]
    fn explicit_empty_assignment_clears_only_the_role_the_response_names() {
        let topo = built_plain();
        let initial = StreamsGroupHeartbeatResponse {
            active_tasks: resp_plain(vec![0]).active_tasks,
            standby_tasks: resp_plain(vec![1]).active_tasks,
            warmup_tasks: resp_plain(vec![2]).active_tasks,
            ..Default::default()
        };
        let mut previous = StreamsAssignment::default();
        check!(assignment_event(&initial, &topo, &mut previous).is_some());

        for role in 0..3 {
            let mut response = StreamsGroupHeartbeatResponse::default();
            let mut expected = previous.clone();
            match role {
                0 => {
                    response.active_tasks = Some(Vec::new());
                    expected.active.clear();
                }
                1 => {
                    response.standby_tasks = Some(Vec::new());
                    expected.standby.clear();
                }
                _ => {
                    response.warmup_tasks = Some(Vec::new());
                    expected.warmup.clear();
                }
            }
            let mut last = previous.clone();
            check!(
                assignment_event(&response, &topo, &mut last)
                    == Some(StreamsEvent::Assigned(expected.clone()))
            );
            check!(last == expected);
        }
    }

    #[test]
    fn empty_assignment_is_not_emitted_from_default() {
        let topo = built_plain();
        let mut last = StreamsAssignment::default();
        let empty = StreamsGroupHeartbeatResponse {
            active_tasks: Some(vec![]),
            ..Default::default()
        };
        check!(assignment_event(&empty, &topo, &mut last).is_none());
    }

    #[test]
    fn changed_assignment_is_re_emitted() {
        let topo = built_plain();
        let mut last = StreamsAssignment::default();
        check!(assignment_event(&resp_plain(vec![0]), &topo, &mut last).is_some());
        check!(assignment_event(&resp_plain(vec![0, 1]), &topo, &mut last).is_some());
    }
}
