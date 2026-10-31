//! Workflow state to PST1 progress messages (issue #58, lane G5).
//!
//! A pure mapping from what the workflow recorded to the message types of
//! PST1 Draft v0 section 5 (aien-dev/aien-protocols-mv#28). Nothing here
//! encodes bytes or touches a network: the first cut stops at typed
//! messages. Status comes only from recorded task state, never from model
//! text, and a message is refused rather than filled with a guess.
//!
//! The type values are copied from the PST1 table here so AEGIS does not
//! depend on the draft `aien-pi-spark-task` crate before it merges. When it
//! does, `PstType` should become that crate's type (item 4, the bytes).
//!
//! Not here yet: `accepted` (the admission answer, owned by the request
//! store), `text_chunk`, `receipt_reference`, the `resume_snapshot` built
//! from `recovery::rebuild` (item 3), and encoding (item 4).

use crate::failure::{EffectCertainty, FailureClass, FailureRecord};
use crate::orchestration::recovery::{EffectClass, StoredPointers};
use crate::orchestration::workflow::{TaskId, TaskState, WorkflowReport, WorkflowSnapshot};
use std::collections::BTreeMap;
use std::fmt;

/// PST1 message types, section 5. The discriminant is the wire value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PstType {
    Request = 1,
    Accepted = 2,
    Queued = 3,
    Running = 4,
    TextChunk = 5,
    WaitingForApproval = 6,
    Completed = 7,
    Failed = 8,
    Cancelled = 9,
    UncertainAfterDisconnect = 10,
    ResumeSnapshot = 11,
    ReceiptReference = 12,
    CancelRequest = 13,
    ResumeRequest = 14,
}

impl PstType {
    /// Every type in wire order.
    pub const ALL: [PstType; 14] = [
        Self::Request,
        Self::Accepted,
        Self::Queued,
        Self::Running,
        Self::TextChunk,
        Self::WaitingForApproval,
        Self::Completed,
        Self::Failed,
        Self::Cancelled,
        Self::UncertainAfterDisconnect,
        Self::ResumeSnapshot,
        Self::ReceiptReference,
        Self::CancelRequest,
        Self::ResumeRequest,
    ];

    pub fn value(self) -> u8 {
        self as u8
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Accepted => "accepted",
            Self::Queued => "queued",
            Self::Running => "running",
            Self::TextChunk => "text_chunk",
            Self::WaitingForApproval => "waiting_for_approval",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::UncertainAfterDisconnect => "uncertain_after_disconnect",
            Self::ResumeSnapshot => "resume_snapshot",
            Self::ReceiptReference => "receipt_reference",
            Self::CancelRequest => "cancel_request",
            Self::ResumeRequest => "resume_request",
        }
    }

    /// The Pi sends `request`, `cancel_request` and `resume_request`; the
    /// Spark sends every other type.
    pub fn sent_by_spark(self) -> bool {
        !matches!(
            self,
            Self::Request | Self::CancelRequest | Self::ResumeRequest
        )
    }
}

/// What the payload will carry once encoded. Digests stay in the
/// `sha256:<64 hex>` form AEGIS records; encoding turns them into 32 bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProgressPayload {
    Empty,
    /// `waiting_for_approval`: the action awaiting approval.
    ActionDigest(String),
    /// `completed`: the result digest from `WorkflowSnapshot.results`,
    /// never the raw output.
    ResultDigest(String),
    /// `failed`: a short reason code and text.
    Failure {
        code: &'static str,
        text: String,
    },
}

/// One progress message, before encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressMessage {
    pub msg_type: PstType,
    /// Copied from the Pi's request; never minted here.
    pub cause_id: String,
    pub origin_run_id: String,
    pub objective_id: String,
    pub task_id: TaskId,
    /// Messages of one task, from 0, rising by one.
    pub ordinal: u32,
    pub effect_class: EffectClass,
    pub payload: ProgressPayload,
}

/// The ids every message carries, read from the stored records.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProgressContext {
    pub run_id: String,
    pub cause_id: String,
    pub objective_id: String,
    pub effect_classes: BTreeMap<TaskId, EffectClass>,
    /// Digest of the action each approval-waiting task asks about. AEGIS
    /// records none yet; the approval desk will (G6, sovereign-core#379).
    pub action_digests: BTreeMap<TaskId, String>,
}

impl ProgressContext {
    /// The ids and effect classes from the G3 stored pointers. Action
    /// digests are not in those pointers and start empty.
    pub fn from_pointers(p: &StoredPointers) -> Self {
        Self {
            run_id: p.run_id.clone(),
            cause_id: p.cause_id.clone(),
            objective_id: p.objective_id.clone(),
            effect_classes: p
                .tasks
                .iter()
                .map(|(id, t)| (id.clone(), t.effect_class))
                .collect(),
            action_digests: BTreeMap::new(),
        }
    }
}

/// Why no message could be built. Nothing is sent in its place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProgressRefusal {
    /// No cause id: one is never minted here (PST1 section 7, `PST_NO_CAUSE`).
    NoCause,
    RunMismatch {
        context: String,
        report: String,
    },
    /// No effect class is recorded for this task.
    UnknownTask {
        task: TaskId,
    },
    UnknownEventState {
        task: TaskId,
        state: String,
    },
    MissingFailureRecord {
        task: TaskId,
    },
    CompletedWithoutResult {
        task: TaskId,
    },
    /// The recorded result is not a `sha256:<64 hex>` digest.
    BadResultDigest {
        task: TaskId,
    },
    WaitingWithoutActionDigest {
        task: TaskId,
    },
}

impl ProgressRefusal {
    pub fn name(&self) -> &'static str {
        match self {
            Self::NoCause => "PROGRESS_NO_CAUSE",
            Self::RunMismatch { .. } => "PROGRESS_RUN_MISMATCH",
            Self::UnknownTask { .. } => "PROGRESS_UNKNOWN_TASK",
            Self::UnknownEventState { .. } => "PROGRESS_UNKNOWN_EVENT_STATE",
            Self::MissingFailureRecord { .. } => "PROGRESS_MISSING_FAILURE_RECORD",
            Self::CompletedWithoutResult { .. } => "PROGRESS_COMPLETED_WITHOUT_RESULT",
            Self::BadResultDigest { .. } => "PROGRESS_BAD_RESULT_DIGEST",
            Self::WaitingWithoutActionDigest { .. } => "PROGRESS_WAITING_WITHOUT_ACTION_DIGEST",
        }
    }
}

impl fmt::Display for ProgressRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {self:?}", self.name())
    }
}

impl std::error::Error for ProgressRefusal {}

/// The one message type a task state is reported as.
///
/// Pending and Ready are both `queued`. Skipped is `failed` with the reason
/// `PREREQUISITE_FAILED` (PST1 has no skipped type). A Failed task whose
/// effect certainty is Uncertain is reported as not known, never as
/// `failed` (PST1 section 8 item 3), even though the workflow keeps it
/// Failed. PST1 names that type `uncertain_after_disconnect`; its meaning,
/// "not known whether it happened", is what is reported.
pub fn message_type(state: &TaskState) -> PstType {
    match state {
        TaskState::Pending | TaskState::Ready => PstType::Queued,
        TaskState::Running => PstType::Running,
        TaskState::WaitingForApproval => PstType::WaitingForApproval,
        TaskState::Completed => PstType::Completed,
        TaskState::Failed(f) if f.effect_certainty == EffectCertainty::Uncertain => {
            PstType::UncertainAfterDisconnect
        }
        TaskState::Failed(_) | TaskState::Skipped { .. } => PstType::Failed,
        TaskState::Cancelled => PstType::Cancelled,
        TaskState::Uncertain(_) => PstType::UncertainAfterDisconnect,
    }
}

/// One message for one task in one state, with the given ordinal.
pub fn message_for_state(
    ctx: &ProgressContext,
    snapshot: &WorkflowSnapshot,
    task: &TaskId,
    state: &TaskState,
    ordinal: u32,
) -> Result<ProgressMessage, ProgressRefusal> {
    check_context(ctx, snapshot)?;
    let effect_class = *ctx
        .effect_classes
        .get(task)
        .ok_or_else(|| ProgressRefusal::UnknownTask { task: task.clone() })?;
    let msg_type = message_type(state);
    let payload = match msg_type {
        PstType::Completed => {
            let r = snapshot
                .results
                .get(task)
                .ok_or_else(|| ProgressRefusal::CompletedWithoutResult { task: task.clone() })?;
            if !is_sha256(&r.digest) {
                return Err(ProgressRefusal::BadResultDigest { task: task.clone() });
            }
            ProgressPayload::ResultDigest(r.digest.clone())
        }
        PstType::WaitingForApproval => {
            let d = ctx.action_digests.get(task).ok_or_else(|| {
                ProgressRefusal::WaitingWithoutActionDigest { task: task.clone() }
            })?;
            ProgressPayload::ActionDigest(d.clone())
        }
        PstType::Failed => match state {
            TaskState::Skipped { prerequisite } => ProgressPayload::Failure {
                code: "PREREQUISITE_FAILED",
                text: format!("prerequisite {prerequisite} did not complete"),
            },
            TaskState::Failed(f) => ProgressPayload::Failure {
                code: failure_code(f.class),
                text: failure_text(f),
            },
            _ => ProgressPayload::Empty,
        },
        _ => ProgressPayload::Empty,
    };
    Ok(ProgressMessage {
        msg_type,
        cause_id: ctx.cause_id.clone(),
        origin_run_id: snapshot.run_id.clone(),
        objective_id: ctx.objective_id.clone(),
        task_id: task.clone(),
        ordinal,
        effect_class,
        payload,
    })
}

/// Every progress message of a finished (or stopped) run, in ledger order.
///
/// Ordinals count each task's messages from 0. A transition that maps to
/// the same type as that task's previous message adds nothing and is not
/// repeated (for example Pending then Ready, both `queued`).
pub fn messages_from_report(
    ctx: &ProgressContext,
    report: &WorkflowReport,
) -> Result<Vec<ProgressMessage>, ProgressRefusal> {
    let snap = &report.snapshot;
    check_context(ctx, snap)?;
    let mut last: BTreeMap<TaskId, (u32, PstType)> = BTreeMap::new();
    let mut out = Vec::new();
    for e in &report.events {
        let Some(task) = &e.task_id else {
            continue;
        };
        let state = state_for_event(report, task, &e.state)?;
        let ty = message_type(&state);
        let ordinal = match last.get(task) {
            Some((_, prev)) if *prev == ty => continue,
            Some((n, _)) => n + 1,
            None => 0,
        };
        out.push(message_for_state(ctx, snap, task, &state, ordinal)?);
        last.insert(task.clone(), (ordinal, ty));
    }
    Ok(out)
}

fn check_context(ctx: &ProgressContext, snap: &WorkflowSnapshot) -> Result<(), ProgressRefusal> {
    if ctx.cause_id.is_empty() {
        return Err(ProgressRefusal::NoCause);
    }
    if ctx.run_id != snap.run_id {
        return Err(ProgressRefusal::RunMismatch {
            context: ctx.run_id.clone(),
            report: snap.run_id.clone(),
        });
    }
    Ok(())
}

/// The ledger keeps a state name per event; the records behind a failed,
/// uncertain or skipped state come from the report.
fn state_for_event(
    report: &WorkflowReport,
    task: &TaskId,
    name: &str,
) -> Result<TaskState, ProgressRefusal> {
    let record = || {
        report
            .failures
            .get(task)
            .cloned()
            .ok_or_else(|| ProgressRefusal::MissingFailureRecord { task: task.clone() })
    };
    Ok(match name {
        "pending" => TaskState::Pending,
        "ready" => TaskState::Ready,
        "running" => TaskState::Running,
        "waiting_for_approval" => TaskState::WaitingForApproval,
        "completed" => TaskState::Completed,
        "cancelled" => TaskState::Cancelled,
        "failed" => TaskState::Failed(record()?),
        "uncertain" => TaskState::Uncertain(record()?),
        "skipped" => match report.snapshot.states.get(task) {
            Some(s @ TaskState::Skipped { .. }) => s.clone(),
            _ => return Err(ProgressRefusal::MissingFailureRecord { task: task.clone() }),
        },
        other => {
            return Err(ProgressRefusal::UnknownEventState {
                task: task.clone(),
                state: other.to_string(),
            })
        }
    })
}

fn failure_code(class: FailureClass) -> &'static str {
    match class {
        FailureClass::Rejected => "REJECTED",
        FailureClass::Unavailable => "UNAVAILABLE",
        FailureClass::Timeout => "TIMEOUT",
        FailureClass::Cancelled => "CANCELLED",
        FailureClass::Uncertain => "UNCERTAIN",
        FailureClass::Failed => "FAILED",
        FailureClass::Malformed => "MALFORMED",
        FailureClass::ApprovalPending => "APPROVAL_PENDING",
    }
}

/// The recorded cause messages (already length capped by the failure
/// record), joined; the class name when there are none.
fn failure_text(f: &FailureRecord) -> String {
    if f.cause.is_empty() {
        return failure_code(f.class).to_lowercase();
    }
    f.cause
        .iter()
        .map(|c| c.message.as_str())
        .collect::<Vec<_>>()
        .join("; ")
}

fn is_sha256(d: &str) -> bool {
    d.strip_prefix("sha256:").is_some_and(|h| {
        h.len() == 64
            && h.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}
