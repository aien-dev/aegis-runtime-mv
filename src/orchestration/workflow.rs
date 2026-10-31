//! Dependency-aware task workflow (DAG) for multi-step agent work (issue #39).
//!
//! This layer sits above the single-run lifecycle in `orchestration::run` and
//! far above AIEN inference scheduling. It decides only WHEN a caller-supplied
//! task may start. It authorizes nothing, caches no decision, retries nothing
//! and never schedules inference: every real effect happens inside the task
//! body through the existing gated paths. State is in memory only and does not
//! survive a restart (see `docs/WORKFLOW-DAG.md`).

use crate::events::{AegisEvent, EventBus, EventEnvelope};
use crate::failure::{
    now_millis, payload_digest, redact_payload_echoes, truncate_bytes, CancelToken,
    EffectCertainty, FailureClass, FailureRecord, ToolFault, MAX_CAUSE_MESSAGE_BYTES,
};
use crate::sessions::id::{RunId, SessionId};
use async_trait::async_trait;
use futures_util::future::BoxFuture;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// Tool name stamped on failure records produced by the DAG itself.
pub const WORKFLOW_TOOL_NAME: &str = "workflow_task";

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(String);

impl TaskId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for TaskId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

/// Build-time errors. A spec that builds is a valid DAG.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowError {
    EmptyTaskId,
    DuplicateTask(TaskId),
    UnknownPredecessor {
        task: TaskId,
        predecessor: TaskId,
    },
    DuplicateEdge {
        task: TaskId,
        predecessor: TaskId,
    },
    SelfDependency(TaskId),
    /// One cycle, listed in dependency order.
    Cycle(Vec<TaskId>),
    InvalidConfig(String),
}

impl fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyTaskId => write!(f, "task id must not be empty"),
            Self::DuplicateTask(t) => write!(f, "duplicate task id: {t}"),
            Self::UnknownPredecessor { task, predecessor } => {
                write!(f, "task {task} depends on unknown task {predecessor}")
            }
            Self::DuplicateEdge { task, predecessor } => {
                write!(f, "task {task} lists predecessor {predecessor} twice")
            }
            Self::SelfDependency(t) => write!(f, "task {t} depends on itself"),
            Self::Cycle(c) => {
                let names: Vec<&str> = c.iter().map(|t| t.as_str()).collect();
                write!(f, "dependency cycle: {}", names.join(" -> "))
            }
            Self::InvalidConfig(m) => write!(f, "invalid workflow config: {m}"),
        }
    }
}

impl std::error::Error for WorkflowError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", content = "detail", rename_all = "snake_case")]
pub enum TaskState {
    Pending,
    Ready,
    Running,
    WaitingForApproval,
    Completed,
    Failed(FailureRecord),
    Cancelled,
    Skipped {
        prerequisite: TaskId,
    },
    /// Started, then interrupted (timeout or cancel) while its effect
    /// certainty was `Uncertain`: not known whether the effect happened. Not
    /// terminal: it must be reconciled against the effect authority before
    /// anything else happens, and its dependents stay Pending behind it
    /// (issue #55). The record says why it was interrupted.
    Uncertain(FailureRecord),
}

impl TaskState {
    /// Terminal means the task will never change state again. `Uncertain` is
    /// not terminal: reconciliation decides its final state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed(_) | Self::Cancelled | Self::Skipped { .. }
        )
    }

    /// Short stable name, used in events.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Running => "running",
            Self::WaitingForApproval => "waiting_for_approval",
            Self::Completed => "completed",
            Self::Failed(_) => "failed",
            Self::Cancelled => "cancelled",
            Self::Skipped { .. } => "skipped",
            Self::Uncertain(_) => "uncertain",
        }
    }
}

/// What a task body reports. Approval pending is not success.
#[derive(Debug, Clone)]
pub enum TaskOutcome {
    Completed(serde_json::Value),
    Failed(ToolFault),
    ApprovalPending,
    Cancelled,
}

/// Handed to the task body when it starts.
#[derive(Clone)]
pub struct TaskContext {
    pub task_id: TaskId,
    pub run_id: RunId,
    pub session_id: SessionId,
    /// Fires when the workflow is cancelled or its total budget is spent.
    pub cancel: CancelToken,
}

/// Caller-supplied task body. It must route every real effect through the
/// existing gate path; the DAG never authorizes on its behalf.
#[async_trait]
pub trait TaskRunner: Send + Sync {
    async fn run(&self, ctx: TaskContext) -> TaskOutcome;

    /// Certainty recorded when the DAG interrupts this task (timeout or
    /// cancellation) before it reports. Default is the safe answer.
    fn certainty_on_interrupt(&self) -> EffectCertainty {
        EffectCertainty::Uncertain
    }
}

/// Wraps an async closure as a `TaskRunner`.
pub struct FnRunner<F> {
    f: F,
    interrupt: EffectCertainty,
}

pub fn runner_fn<F>(f: F) -> Arc<dyn TaskRunner>
where
    F: Fn(TaskContext) -> BoxFuture<'static, TaskOutcome> + Send + Sync + 'static,
{
    Arc::new(FnRunner {
        f,
        interrupt: EffectCertainty::Uncertain,
    })
}

/// Like `runner_fn` with an explicit certainty for interruptions.
pub fn runner_fn_with_certainty<F>(f: F, interrupt: EffectCertainty) -> Arc<dyn TaskRunner>
where
    F: Fn(TaskContext) -> BoxFuture<'static, TaskOutcome> + Send + Sync + 'static,
{
    Arc::new(FnRunner { f, interrupt })
}

#[async_trait]
impl<F> TaskRunner for FnRunner<F>
where
    F: Fn(TaskContext) -> BoxFuture<'static, TaskOutcome> + Send + Sync + 'static,
{
    async fn run(&self, ctx: TaskContext) -> TaskOutcome {
        (self.f)(ctx).await
    }

    fn certainty_on_interrupt(&self) -> EffectCertainty {
        self.interrupt
    }
}

struct TaskNode {
    id: TaskId,
    deps: Vec<TaskId>,
    runner: Arc<dyn TaskRunner>,
}

/// A validated DAG. Insertion order is the deterministic tie break.
pub struct WorkflowSpec {
    tasks: Vec<TaskNode>,
}

impl WorkflowSpec {
    pub fn builder() -> WorkflowBuilder {
        WorkflowBuilder::default()
    }

    pub fn task_ids(&self) -> Vec<TaskId> {
        self.tasks.iter().map(|t| t.id.clone()).collect()
    }

    /// The prerequisites of `id`, in the order they were listed.
    pub fn dependencies(&self, id: &TaskId) -> Option<&[TaskId]> {
        self.tasks
            .iter()
            .find(|t| &t.id == id)
            .map(|t| t.deps.as_slice())
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

#[derive(Default)]
pub struct WorkflowBuilder {
    tasks: Vec<TaskNode>,
}

impl WorkflowBuilder {
    /// Predecessors may be added after their dependents (forward references
    /// are allowed). `build` validates the whole graph: unknown predecessors,
    /// duplicate edges and cycles are all rejected there.
    pub fn add_task(
        mut self,
        id: impl Into<TaskId>,
        deps: &[&str],
        runner: Arc<dyn TaskRunner>,
    ) -> Result<Self, WorkflowError> {
        let id = id.into();
        if id.as_str().is_empty() {
            return Err(WorkflowError::EmptyTaskId);
        }
        if self.tasks.iter().any(|t| t.id == id) {
            return Err(WorkflowError::DuplicateTask(id));
        }
        let mut seen: HashSet<&str> = HashSet::new();
        let mut deps_out = Vec::with_capacity(deps.len());
        for d in deps {
            if *d == id.as_str() {
                return Err(WorkflowError::SelfDependency(id));
            }
            if !seen.insert(d) {
                return Err(WorkflowError::DuplicateEdge {
                    task: id,
                    predecessor: TaskId::new(*d),
                });
            }
            deps_out.push(TaskId::new(*d));
        }
        self.tasks.push(TaskNode {
            id,
            deps: deps_out,
            runner,
        });
        Ok(self)
    }

    /// Validates predecessors and rejects cycles. Predecessors may be added
    /// after their dependents, so this is where unknown ids and cycles surface.
    pub fn build(self) -> Result<WorkflowSpec, WorkflowError> {
        let index: HashMap<&TaskId, usize> = self
            .tasks
            .iter()
            .enumerate()
            .map(|(i, t)| (&t.id, i))
            .collect();
        for t in &self.tasks {
            for d in &t.deps {
                if !index.contains_key(d) {
                    return Err(WorkflowError::UnknownPredecessor {
                        task: t.id.clone(),
                        predecessor: d.clone(),
                    });
                }
            }
        }
        // Kahn: whatever cannot be peeled off sits on or behind a cycle.
        let mut indegree: Vec<usize> = self.tasks.iter().map(|t| t.deps.len()).collect();
        let mut queue: Vec<usize> = (0..self.tasks.len())
            .filter(|i| indegree[*i] == 0)
            .collect();
        let mut peeled = 0;
        while let Some(n) = queue.pop() {
            peeled += 1;
            for (i, t) in self.tasks.iter().enumerate() {
                if t.deps.contains(&self.tasks[n].id) {
                    indegree[i] -= 1;
                    if indegree[i] == 0 {
                        queue.push(i);
                    }
                }
            }
        }
        if peeled != self.tasks.len() {
            return Err(WorkflowError::Cycle(find_cycle(
                &self.tasks,
                &index,
                &indegree,
            )));
        }
        Ok(WorkflowSpec { tasks: self.tasks })
    }
}

/// Walks predecessor links among the unpeeled nodes until one repeats.
fn find_cycle(
    tasks: &[TaskNode],
    index: &HashMap<&TaskId, usize>,
    indegree: &[usize],
) -> Vec<TaskId> {
    let start = (0..tasks.len()).find(|i| indegree[*i] > 0).unwrap_or(0);
    let mut path: Vec<usize> = vec![start];
    let mut cur = start;
    loop {
        let next = tasks[cur]
            .deps
            .iter()
            .map(|d| index[d])
            .find(|i| indegree[*i] > 0)
            .unwrap_or(start);
        if let Some(pos) = path.iter().position(|p| *p == next) {
            let mut cycle: Vec<TaskId> = path[pos..].iter().map(|i| tasks[*i].id.clone()).collect();
            cycle.reverse();
            return cycle;
        }
        path.push(next);
        cur = next;
    }
}

#[derive(Debug, Clone)]
pub struct WorkflowConfig {
    pub run_id: RunId,
    pub session_id: SessionId,
    /// Upper bound on tasks running at once. Must be at least 1.
    pub max_parallel: usize,
    pub per_task_timeout: Option<Duration>,
    /// Wall-clock cap for the whole workflow. When it elapses the workflow is
    /// cancelled like an operator cancel and `budget_exhausted` is set.
    pub total_budget: Option<Duration>,
}

impl WorkflowConfig {
    pub fn new(run_id: RunId, session_id: SessionId, max_parallel: usize) -> Self {
        Self {
            run_id,
            session_id,
            max_parallel,
            per_task_timeout: None,
            total_budget: None,
        }
    }
}

/// Digest of a completed task's result. The value itself is not kept here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultHandle {
    pub digest: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    /// Every task completed.
    Completed,
    /// Some task failed, was skipped or cancelled, and nothing waits on a human.
    Failed,
    /// Cancelled (operator or budget).
    Cancelled,
    /// At least one task waits for approval; the workflow is not finished.
    WaitingForApproval,
}

/// One transition, in workflow order. `seq` starts at 1.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowEvent {
    pub seq: u64,
    pub task_id: Option<TaskId>,
    pub state: String,
}

/// Serializable view of the in-memory workflow state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowSnapshot {
    pub run_id: String,
    pub last_seq: u64,
    pub states: BTreeMap<TaskId, TaskState>,
    pub results: BTreeMap<TaskId, ResultHandle>,
}

#[derive(Debug, Clone)]
pub struct WorkflowReport {
    pub status: WorkflowStatus,
    pub snapshot: WorkflowSnapshot,
    /// Failure records for Failed, Skipped, Cancelled and Uncertain tasks.
    pub failures: BTreeMap<TaskId, FailureRecord>,
    /// Tasks left non-terminal (waiting for approval, uncertain, or blocked
    /// behind either).
    pub unfinished: Vec<TaskId>,
    /// Pending tasks that wait, directly or through other pending tasks, on
    /// an `Uncertain` task, mapped to one uncertain task they wait on.
    /// They were neither run nor skipped.
    pub blocked_behind_uncertain: BTreeMap<TaskId, TaskId>,
    pub events: Vec<WorkflowEvent>,
    pub peak_concurrency: usize,
    pub budget_exhausted: bool,
}

struct Ledger {
    seq: u64,
    states: BTreeMap<TaskId, TaskState>,
    results: BTreeMap<TaskId, ResultHandle>,
    failures: BTreeMap<TaskId, FailureRecord>,
    events: Vec<WorkflowEvent>,
}

/// The one place a state change is recorded. The sequence number is assigned
/// under the lock together with the state change, so replay order equals
/// transition order.
struct Recorder {
    ledger: Mutex<Ledger>,
    bus: Option<EventBus>,
    run_id: RunId,
    session_id: SessionId,
}

impl Recorder {
    fn transition(&self, id: &TaskId, state: TaskState, result: Option<ResultHandle>) -> u64 {
        let mut l = self.ledger.lock();
        l.seq += 1;
        let seq = l.seq;
        let name = state.name();
        let event = self.event_for(seq, id, &state, result.as_ref());
        if let Some(r) = result {
            l.results.insert(id.clone(), r);
        }
        l.states.insert(id.clone(), state);
        l.events.push(WorkflowEvent {
            seq,
            task_id: Some(id.clone()),
            state: name.to_string(),
        });
        // Publish while holding the lock so bus order equals sequence order.
        if let Some(event) = event {
            self.publish(event, seq);
        }
        seq
    }

    fn finish(&self, status: WorkflowStatus) {
        let mut l = self.ledger.lock();
        l.seq += 1;
        let seq = l.seq;
        l.events.push(WorkflowEvent {
            seq,
            task_id: None,
            state: "finished".to_string(),
        });
        let event = AegisEvent::WorkflowFinished {
            run_id: self.run_id.to_string(),
            workflow_seq: seq,
            status: format!("{status:?}").to_lowercase(),
        };
        self.publish(event, seq);
    }

    fn note_failure(&self, id: &TaskId, rec: FailureRecord) {
        self.ledger.lock().failures.insert(id.clone(), rec);
    }

    fn state_of(&self, id: &TaskId) -> TaskState {
        self.ledger.lock().states[id].clone()
    }

    fn next_failure_seq(&self) -> u64 {
        // Failure records share the workflow sequence space; the number is
        // only a stable order key, so the next transition's number is used.
        self.ledger.lock().seq + 1
    }

    fn publish(&self, event: AegisEvent, seq: u64) {
        if let Some(bus) = &self.bus {
            let env = EventEnvelope::new(
                event,
                Some(self.session_id.to_string()),
                Some(self.run_id.to_string()),
                seq,
            );
            // No subscriber is not an error for the workflow.
            let _ = bus.publish(env);
        }
    }

    fn event_for(
        &self,
        seq: u64,
        id: &TaskId,
        state: &TaskState,
        result: Option<&ResultHandle>,
    ) -> Option<AegisEvent> {
        let run_id = self.run_id.to_string();
        let task_id = id.to_string();
        Some(match state {
            // Pending is the initial state and is never recorded as a
            // transition, and the event enum has no variant for it, so no
            // bus event exists (the ledger entry is still written).
            TaskState::Pending => return None,
            TaskState::Ready => AegisEvent::WorkflowTaskReady {
                run_id,
                workflow_seq: seq,
                task_id,
            },
            TaskState::Running => AegisEvent::WorkflowTaskStarted {
                run_id,
                workflow_seq: seq,
                task_id,
            },
            TaskState::Completed => AegisEvent::WorkflowTaskCompleted {
                run_id,
                workflow_seq: seq,
                task_id,
                result_digest: result.map(|r| r.digest.clone()).unwrap_or_default(),
            },
            TaskState::WaitingForApproval => AegisEvent::WorkflowTaskWaiting {
                run_id,
                workflow_seq: seq,
                task_id,
            },
            TaskState::Failed(rec) => AegisEvent::WorkflowTaskFailed {
                run_id,
                workflow_seq: seq,
                task_id,
                failure: rec.clone(),
            },
            TaskState::Cancelled => AegisEvent::WorkflowTaskCancelled {
                run_id,
                workflow_seq: seq,
                task_id,
            },
            TaskState::Skipped { prerequisite } => AegisEvent::WorkflowTaskSkipped {
                run_id,
                workflow_seq: seq,
                task_id,
                prerequisite: prerequisite.to_string(),
            },
            // No bus variant yet (issue #55 item 4). Publishing it as Failed
            // or Cancelled would be false, so the bus stays silent; the
            // ledger event "uncertain" is still written in sequence.
            TaskState::Uncertain(_) => return None,
        })
    }
}

enum Finish {
    Outcome(TaskOutcome),
    TimedOut,
    Interrupted,
}

pub struct WorkflowExecutor {
    config: WorkflowConfig,
    bus: Option<EventBus>,
    cancel: CancelToken,
}

impl WorkflowExecutor {
    pub fn new(config: WorkflowConfig) -> Result<Self, WorkflowError> {
        if config.max_parallel == 0 {
            return Err(WorkflowError::InvalidConfig(
                "max_parallel must be at least 1".into(),
            ));
        }
        Ok(Self {
            config,
            bus: None,
            cancel: CancelToken::new(),
        })
    }

    pub fn with_event_bus(mut self, bus: EventBus) -> Self {
        self.bus = Some(bus);
        self
    }

    pub fn with_cancel_token(mut self, cancel: CancelToken) -> Self {
        self.cancel = cancel;
        self
    }

    fn failure(
        &self,
        rec: &Recorder,
        id: &TaskId,
        class: FailureClass,
        certainty: EffectCertainty,
        message: &str,
    ) -> FailureRecord {
        let now = now_millis();
        FailureRecord::new(
            rec.next_failure_seq(),
            self.config.run_id.to_string(),
            0,
            id.to_string(),
            None,
            WORKFLOW_TOOL_NAME,
            1,
            class,
            certainty,
            now,
            now,
        )
        .with_cause("workflow", &scrub_message(message), None)
    }

    /// Runs the DAG to quiescence. Never blocks a worker thread: the
    /// coordinator awaits, and task bodies run on spawned tasks.
    pub async fn run(&self, spec: WorkflowSpec) -> WorkflowReport {
        let ids = spec.task_ids();
        let rec = Recorder {
            ledger: Mutex::new(Ledger {
                seq: 0,
                states: ids
                    .iter()
                    .map(|i| (i.clone(), TaskState::Pending))
                    .collect(),
                results: BTreeMap::new(),
                failures: BTreeMap::new(),
                events: Vec::new(),
            }),
            bus: self.bus.clone(),
            run_id: self.config.run_id.clone(),
            session_id: self.config.session_id.clone(),
        };
        let nodes: HashMap<TaskId, &TaskNode> =
            spec.tasks.iter().map(|t| (t.id.clone(), t)).collect();
        let permits = Arc::new(Semaphore::new(self.config.max_parallel));
        let mut running: JoinSet<(TaskId, Finish)> = JoinSet::new();
        let mut by_join_id: HashMap<tokio::task::Id, TaskId> = HashMap::new();
        let mut live = 0usize;
        let mut peak = 0usize;
        // The executor never triggers the caller's token. Budget expiry and
        // observed caller cancels both trip this run-local token instead.
        let run_cancel = CancelToken::new();
        let mut cancel_seen = false;
        let mut budget_exhausted = false;
        let deadline = self
            .config
            .total_budget
            .map(|d| tokio::time::Instant::now() + d);

        'outer: loop {
            // Deadline first: no task may start once the budget is spent.
            if !budget_exhausted && deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
                budget_exhausted = true;
                run_cancel.cancel();
            }
            if self.cancel.is_cancelled() {
                run_cancel.cancel();
            }
            if !cancel_seen && run_cancel.is_cancelled() {
                cancel_seen = true;
                self.cancel_open(&rec, &ids, &nodes);
            }

            if !cancel_seen {
                self.propagate(&ids, &nodes, &rec);
                // Start ready tasks in insertion order while permits last.
                for id in &ids {
                    if rec.state_of(id) != TaskState::Ready {
                        continue;
                    }
                    // Re-check right before starting: cancel or an expired
                    // budget restarts the pass, which cancels what is left.
                    if self.cancel.is_cancelled()
                        || run_cancel.is_cancelled()
                        || deadline.is_some_and(|d| tokio::time::Instant::now() >= d)
                    {
                        continue 'outer;
                    }
                    let Ok(permit) = permits.clone().try_acquire_owned() else {
                        break;
                    };
                    rec.transition(id, TaskState::Running, None);
                    live += 1;
                    peak = peak.max(self.config.max_parallel - permits.available_permits());
                    let node = nodes[id];
                    let runner = node.runner.clone();
                    let ctx = TaskContext {
                        task_id: id.clone(),
                        run_id: self.config.run_id.clone(),
                        session_id: self.config.session_id.clone(),
                        cancel: run_cancel.clone(),
                    };
                    let cancel = run_cancel.clone();
                    let timeout = self.config.per_task_timeout;
                    let tid = id.clone();
                    let handle = running.spawn(async move {
                        let _permit = permit;
                        let body = runner.run(ctx);
                        let finish = tokio::select! {
                            biased;
                            _ = cancel.cancelled() => Finish::Interrupted,
                            f = async {
                                match timeout {
                                    Some(d) => match tokio::time::timeout(d, body).await {
                                        Ok(o) => Finish::Outcome(o),
                                        Err(_) => Finish::TimedOut,
                                    },
                                    None => Finish::Outcome(body.await),
                                }
                            } => f,
                        };
                        (tid, finish)
                    });
                    by_join_id.insert(handle.id(), id.clone());
                }
            }

            if live == 0 {
                break;
            }

            let joined = tokio::select! {
                biased;
                _ = self.cancel.cancelled(), if !cancel_seen => {
                    run_cancel.cancel();
                    continue;
                }
                _ = async {
                    match deadline {
                        Some(d) => tokio::time::sleep_until(d).await,
                        None => std::future::pending().await,
                    }
                }, if !budget_exhausted => {
                    budget_exhausted = true;
                    run_cancel.cancel();
                    continue;
                }
                j = running.join_next_with_id() => j,
            };
            let Some(joined) = joined else { break };
            live -= 1;
            let (jid, result) = match joined {
                Ok((jid, r)) => (jid, Ok(r)),
                Err(e) => (e.id(), Err(e)),
            };
            let tid = by_join_id.remove(&jid).expect("tracked join id");
            let runner = nodes[&tid].runner.clone();
            let interrupt = runner.certainty_on_interrupt();
            let finish = match result {
                Ok((_, f)) => f,
                Err(_) => {
                    // A panicking body: the effect may have happened.
                    let f = self.failure(
                        &rec,
                        &tid,
                        FailureClass::Failed,
                        EffectCertainty::Uncertain,
                        "task body panicked",
                    );
                    rec.note_failure(&tid, f.clone());
                    rec.transition(&tid, TaskState::Failed(f), None);
                    continue;
                }
            };
            self.settle(&rec, &tid, finish, interrupt);
        }

        if cancel_seen {
            // A task that reported ApprovalPending after the cancel was seen
            // is settled here, so Cancelled never lists unfinished tasks.
            self.cancel_open(&rec, &ids, &nodes);
        }
        let blocked = blocked_behind_uncertain(&rec, &ids, &nodes);
        self.report(rec, ids, peak, budget_exhausted, cancel_seen, blocked)
    }

    /// Moves every non-terminal, non-running task to Cancelled. A task
    /// waiting for approval is cancelled too: nothing will ever resume it.
    fn cancel_open(&self, rec: &Recorder, ids: &[TaskId], nodes: &HashMap<TaskId, &TaskNode>) {
        for id in ids {
            let (message, certainty) = match rec.state_of(id) {
                TaskState::Pending | TaskState::Ready => {
                    ("workflow cancelled before start", EffectCertainty::NoEffect)
                }
                TaskState::WaitingForApproval => (
                    "workflow cancelled while waiting for approval",
                    nodes[id].runner.certainty_on_interrupt(),
                ),
                _ => continue,
            };
            let f = self.failure(rec, id, FailureClass::Cancelled, certainty, message);
            rec.note_failure(id, f);
            rec.transition(id, TaskState::Cancelled, None);
        }
    }

    /// Unblocks dependents whose prerequisites all completed, and skips
    /// dependents of any prerequisite that ended without success. A task that
    /// waits for approval is not terminal, so its dependents stay Pending.
    fn propagate(&self, ids: &[TaskId], nodes: &HashMap<TaskId, &TaskNode>, rec: &Recorder) {
        loop {
            let mut changed = false;
            for id in ids {
                if rec.state_of(id) != TaskState::Pending {
                    continue;
                }
                let deps = &nodes[id].deps;
                let bad = deps.iter().find(|d| {
                    matches!(
                        rec.state_of(d),
                        TaskState::Failed(_) | TaskState::Cancelled | TaskState::Skipped { .. }
                    )
                });
                if let Some(bad) = bad {
                    let f = self.failure(
                        rec,
                        id,
                        FailureClass::Rejected,
                        EffectCertainty::NoEffect,
                        &format!("prerequisite failed: {bad}"),
                    );
                    rec.note_failure(id, f);
                    rec.transition(
                        id,
                        TaskState::Skipped {
                            prerequisite: bad.clone(),
                        },
                        None,
                    );
                    changed = true;
                } else if deps.iter().all(|d| rec.state_of(d) == TaskState::Completed) {
                    rec.transition(id, TaskState::Ready, None);
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
    }

    fn settle(&self, rec: &Recorder, id: &TaskId, finish: Finish, interrupt: EffectCertainty) {
        match finish {
            Finish::Outcome(TaskOutcome::Completed(v)) => {
                let handle = ResultHandle {
                    digest: payload_digest(&v),
                };
                rec.transition(id, TaskState::Completed, Some(handle));
            }
            Finish::Outcome(TaskOutcome::Failed(fault)) => {
                let f = self.failure(rec, id, fault.class, fault.certainty, &fault.message);
                rec.note_failure(id, f.clone());
                rec.transition(id, TaskState::Failed(f), None);
            }
            Finish::Outcome(TaskOutcome::ApprovalPending) => {
                rec.transition(id, TaskState::WaitingForApproval, None);
            }
            Finish::Outcome(TaskOutcome::Cancelled) | Finish::Interrupted => {
                let f = self.failure(
                    rec,
                    id,
                    FailureClass::Cancelled,
                    interrupt,
                    "task cancelled while running",
                );
                rec.note_failure(id, f.clone());
                if interrupt == EffectCertainty::Uncertain {
                    rec.transition(id, TaskState::Uncertain(f), None);
                } else {
                    rec.transition(id, TaskState::Cancelled, None);
                }
            }
            Finish::TimedOut => {
                let f = self.failure(
                    rec,
                    id,
                    FailureClass::Timeout,
                    interrupt,
                    "per-task timeout elapsed",
                );
                rec.note_failure(id, f.clone());
                if interrupt == EffectCertainty::Uncertain {
                    rec.transition(id, TaskState::Uncertain(f), None);
                } else {
                    rec.transition(id, TaskState::Failed(f), None);
                }
            }
        }
    }

    fn report(
        &self,
        rec: Recorder,
        ids: Vec<TaskId>,
        peak: usize,
        budget_exhausted: bool,
        cancelled: bool,
        blocked_behind_uncertain: BTreeMap<TaskId, TaskId>,
    ) -> WorkflowReport {
        let (unfinished, any_waiting, all_done) = {
            let l = rec.ledger.lock();
            let unfinished: Vec<TaskId> = ids
                .iter()
                .filter(|i| !l.states[*i].is_terminal())
                .cloned()
                .collect();
            let any_waiting = l
                .states
                .values()
                .any(|s| matches!(s, TaskState::WaitingForApproval));
            let all_done = l.states.values().all(|s| matches!(s, TaskState::Completed));
            (unfinished, any_waiting, all_done)
        };
        let status = if cancelled {
            WorkflowStatus::Cancelled
        } else if any_waiting {
            WorkflowStatus::WaitingForApproval
        } else if all_done {
            WorkflowStatus::Completed
        } else {
            WorkflowStatus::Failed
        };
        rec.finish(status);
        let l = rec.ledger.lock();
        WorkflowReport {
            status,
            snapshot: WorkflowSnapshot {
                run_id: self.config.run_id.to_string(),
                last_seq: l.seq,
                states: l.states.clone(),
                results: l.results.clone(),
            },
            failures: l.failures.clone(),
            unfinished,
            blocked_behind_uncertain,
            events: l.events.clone(),
            peak_concurrency: peak,
            budget_exhausted,
        }
    }
}

/// Pending tasks that wait on an `Uncertain` task, directly or through other
/// pending tasks. Each maps to one uncertain task it waits on (several may
/// apply; the first one found is named).
fn blocked_behind_uncertain(
    rec: &Recorder,
    ids: &[TaskId],
    nodes: &HashMap<TaskId, &TaskNode>,
) -> BTreeMap<TaskId, TaskId> {
    let mut blocked: BTreeMap<TaskId, TaskId> = BTreeMap::new();
    loop {
        let mut changed = false;
        for id in ids {
            if blocked.contains_key(id) || rec.state_of(id) != TaskState::Pending {
                continue;
            }
            let root = nodes[id].deps.iter().find_map(|d| match rec.state_of(d) {
                TaskState::Uncertain(_) => Some(d.clone()),
                _ => blocked.get(d).cloned(),
            });
            if let Some(root) = root {
                blocked.insert(id.clone(), root);
                changed = true;
            }
        }
        if !changed {
            return blocked;
        }
    }
}

/// Error text from runners and engines goes into Workflow events and reports,
/// so it is cut to the cause limit before it is recorded. Runners that know
/// their payload should also call `redact_payload_echoes` first (the agent
/// adapter does).
fn scrub_message(message: &str) -> String {
    truncate_bytes(message, MAX_CAUSE_MESSAGE_BYTES)
}

/// Maps a finished `AgentEngine::execute_task` result onto a task outcome.
/// This is the whole adapter seam: the agent already gates every tool call,
/// so the DAG adds no authority here.
pub fn agent_outcome(result: &crate::agent::AgentExecutionResult) -> TaskOutcome {
    if let Some(fault) = crate::agent::terminal_tool_failure(&result.steps) {
        return if fault.class == FailureClass::ApprovalPending {
            TaskOutcome::ApprovalPending
        } else {
            TaskOutcome::Failed(fault)
        };
    }
    if result.completed {
        TaskOutcome::Completed(serde_json::json!({ "final_response": result.final_response }))
    } else {
        // Ran out of turns: tools may have acted, so claim no certainty.
        TaskOutcome::Failed(ToolFault::new(
            FailureClass::Failed,
            EffectCertainty::Uncertain,
            "agent did not complete within its turn budget",
        ))
    }
}

/// Runs one `AgentEngine::execute_task` call as a workflow task.
pub struct AgentTaskRunner {
    pub engine: Arc<crate::agent::AgentEngine>,
    pub goal: String,
    pub max_turns: usize,
}

#[async_trait]
impl TaskRunner for AgentTaskRunner {
    async fn run(&self, _ctx: TaskContext) -> TaskOutcome {
        match self
            .engine
            .execute_task(&self.goal, None, self.max_turns)
            .await
        {
            Ok(r) => agent_outcome(&r),
            Err(e) => TaskOutcome::Failed(ToolFault::new(
                FailureClass::Failed,
                EffectCertainty::Uncertain,
                redact_payload_echoes(&e.to_string(), &serde_json::json!({ "goal": self.goal })),
            )),
        }
    }
}
