//! G5 (#58): workflow state to PST1 progress message types. Pure mapping,
//! no encoding, no network. The message types are PST1 Draft v0 section 5
//! (aien-dev/aien-protocols-mv#28).

use aegis::orchestration::progress::{
    message_type, messages_from_report, ProgressContext, ProgressMessage, ProgressPayload,
    ProgressRefusal, PstType,
};
use aegis::orchestration::recovery::{EffectClass, StoredPointers, TaskPointers};
use aegis::orchestration::workflow::{runner_fn, TaskContext, TaskOutcome, WorkflowBuilder};
use aegis::{
    EffectCertainty, FailureClass, FailureRecord, RunId, SessionId, TaskId, TaskRunner, TaskState,
    ToolFault, WorkflowConfig, WorkflowExecutor, WorkflowReport, WorkflowSpec,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const RUN: &str = "run_g5";
const CAUSE: &str = "cause_from_the_pi";
const OBJECTIVE: &str = "sha256:objective";
const ACTION: &str = "sha256:action_awaiting_approval";

fn tid(s: &str) -> TaskId {
    TaskId::new(s)
}

fn record(class: FailureClass, cert: EffectCertainty) -> FailureRecord {
    FailureRecord::new(1, RUN, 0, "t", None, "tool", 1, class, cert, 0, 0)
}

fn ok() -> Arc<dyn TaskRunner> {
    runner_fn(|_c: TaskContext| {
        Box::pin(async { TaskOutcome::Completed(serde_json::json!({ "ok": true })) })
    })
}

fn failing(class: FailureClass, cert: EffectCertainty) -> Arc<dyn TaskRunner> {
    runner_fn(move |_c: TaskContext| {
        Box::pin(async move { TaskOutcome::Failed(ToolFault::new(class, cert, "boom")) })
    })
}

fn approval() -> Arc<dyn TaskRunner> {
    runner_fn(|_c| Box::pin(async { TaskOutcome::ApprovalPending }))
}

async fn run(spec: WorkflowSpec) -> WorkflowReport {
    let cfg = WorkflowConfig::new(
        RunId::from_string(RUN),
        SessionId::from_string("sess_g5"),
        1,
    );
    WorkflowExecutor::new(cfg).unwrap().run(spec).await
}

fn ctx(tasks: &[(&str, EffectClass)]) -> ProgressContext {
    ProgressContext {
        run_id: RUN.into(),
        cause_id: CAUSE.into(),
        objective_id: OBJECTIVE.into(),
        effect_classes: tasks.iter().map(|(t, c)| (tid(t), *c)).collect(),
        action_digests: BTreeMap::new(),
    }
}

fn of<'a>(msgs: &'a [ProgressMessage], task: &str) -> Vec<&'a ProgressMessage> {
    msgs.iter().filter(|m| m.task_id == tid(task)).collect()
}

fn types(msgs: &[&ProgressMessage]) -> Vec<PstType> {
    msgs.iter().map(|m| m.msg_type).collect()
}

/// a -> b, plus an independent gate that waits for approval.
fn approval_spec() -> WorkflowSpec {
    WorkflowBuilder::default()
        .add_task("a", &[], ok())
        .unwrap()
        .add_task("b", &["a"], ok())
        .unwrap()
        .add_task("gate", &[], approval())
        .unwrap()
        .build()
        .unwrap()
}

fn approval_ctx() -> ProgressContext {
    let mut c = ctx(&[
        ("a", EffectClass::Compute),
        ("b", EffectClass::Compute),
        ("gate", EffectClass::Effect),
    ]);
    c.action_digests.insert(tid("gate"), ACTION.into());
    c
}

#[test]
fn g5_message_type_values_match_pst1_section_5() {
    let want = [
        (1, "request", false),
        (2, "accepted", true),
        (3, "queued", true),
        (4, "running", true),
        (5, "text_chunk", true),
        (6, "waiting_for_approval", true),
        (7, "completed", true),
        (8, "failed", true),
        (9, "cancelled", true),
        (10, "uncertain_after_disconnect", true),
        (11, "resume_snapshot", true),
        (12, "receipt_reference", true),
        (13, "cancel_request", false),
        (14, "resume_request", false),
    ];
    for (t, (v, n, spark)) in PstType::ALL.iter().zip(want) {
        assert_eq!((t.value(), t.name(), t.sent_by_spark()), (v, n, spark));
    }
}

/// DoD item 4 (first half): every state maps to exactly one message type,
/// always one the Spark sends.
#[test]
fn g5_every_task_state_maps_to_exactly_one_spark_message_type() {
    let table = [
        (TaskState::Pending, PstType::Queued),
        (TaskState::Ready, PstType::Queued),
        (TaskState::Running, PstType::Running),
        (TaskState::WaitingForApproval, PstType::WaitingForApproval),
        (TaskState::Completed, PstType::Completed),
        (
            TaskState::Failed(record(FailureClass::Failed, EffectCertainty::NoEffect)),
            PstType::Failed,
        ),
        (
            TaskState::Failed(record(
                FailureClass::Failed,
                EffectCertainty::EffectOccurred,
            )),
            PstType::Failed,
        ),
        // Failed, but not known whether the effect happened: PST1 section 8
        // item 3 says never report failed when it does not know.
        (
            TaskState::Failed(record(FailureClass::Uncertain, EffectCertainty::Uncertain)),
            PstType::UncertainAfterDisconnect,
        ),
        (TaskState::Cancelled, PstType::Cancelled),
        (
            TaskState::Skipped {
                prerequisite: tid("a"),
            },
            PstType::Failed,
        ),
        (
            TaskState::Uncertain(record(FailureClass::Timeout, EffectCertainty::Uncertain)),
            PstType::UncertainAfterDisconnect,
        ),
    ];
    let names: BTreeSet<&str> = table.iter().map(|(s, _)| s.name()).collect();
    let all: BTreeSet<&str> = [
        "pending",
        "ready",
        "running",
        "waiting_for_approval",
        "completed",
        "failed",
        "cancelled",
        "skipped",
        "uncertain",
    ]
    .into();
    assert_eq!(names, all, "the table must cover every TaskState");
    for (state, want) in &table {
        let got = message_type(state);
        assert_eq!(got, *want, "{}", state.name());
        assert!(got.sent_by_spark(), "{}", state.name());
    }
}

/// DoD items 1 and 2: ids from the records, ordinals from 0 per task, the
/// result digest from the snapshot, never raw output.
#[tokio::test]
async fn g5_messages_from_a_real_run_carry_ids_ordinals_and_digests() {
    let r = run(approval_spec()).await;
    let msgs = messages_from_report(&approval_ctx(), &r).expect("mapping");
    assert!(!msgs.is_empty());
    for m in &msgs {
        assert_eq!(m.cause_id, CAUSE);
        assert_eq!(m.objective_id, OBJECTIVE);
        assert_eq!(m.origin_run_id, r.snapshot.run_id);
        assert!(m.msg_type.sent_by_spark());
    }
    for task in ["a", "b", "gate"] {
        let ms = of(&msgs, task);
        let ords: Vec<u32> = ms.iter().map(|m| m.ordinal).collect();
        assert_eq!(ords, (0..ms.len() as u32).collect::<Vec<_>>(), "{task}");
    }
    let a = of(&msgs, "a");
    assert_eq!(
        types(&a),
        vec![PstType::Queued, PstType::Running, PstType::Completed]
    );
    assert_eq!(a[0].effect_class, EffectClass::Compute);
    assert_eq!(
        a[2].payload,
        ProgressPayload::ResultDigest(r.snapshot.results[&tid("a")].digest.clone())
    );
    let gate = of(&msgs, "gate");
    assert_eq!(gate.last().unwrap().msg_type, PstType::WaitingForApproval);
    assert_eq!(
        gate.last().unwrap().payload,
        ProgressPayload::ActionDigest(ACTION.into())
    );
    assert_eq!(gate[0].effect_class, EffectClass::Effect);
    for m in &msgs {
        if matches!(m.msg_type, PstType::Queued | PstType::Running) {
            assert_eq!(m.payload, ProgressPayload::Empty);
        }
    }
}

#[tokio::test]
async fn g5_same_run_twice_gives_the_same_messages() {
    let one = messages_from_report(&approval_ctx(), &run(approval_spec()).await).unwrap();
    let two = messages_from_report(&approval_ctx(), &run(approval_spec()).await).unwrap();
    assert_eq!(one, two);
}

#[tokio::test]
async fn g5_failed_with_unknown_effect_is_reported_as_not_known() {
    let spec = WorkflowBuilder::default()
        .add_task(
            "w",
            &[],
            failing(FailureClass::Uncertain, EffectCertainty::Uncertain),
        )
        .unwrap()
        .build()
        .unwrap();
    let r = run(spec).await;
    let msgs = messages_from_report(&ctx(&[("w", EffectClass::Effect)]), &r).unwrap();
    let w = of(&msgs, "w");
    assert_eq!(
        w.last().unwrap().msg_type,
        PstType::UncertainAfterDisconnect
    );
    assert!(!types(&w).contains(&PstType::Failed));
}

#[tokio::test]
async fn g5_failure_and_skip_carry_a_reason_code() {
    let spec = WorkflowBuilder::default()
        .add_task(
            "a",
            &[],
            failing(FailureClass::Rejected, EffectCertainty::NoEffect),
        )
        .unwrap()
        .add_task("b", &["a"], ok())
        .unwrap()
        .build()
        .unwrap();
    let r = run(spec).await;
    let c = ctx(&[("a", EffectClass::Compute), ("b", EffectClass::Compute)]);
    let msgs = messages_from_report(&c, &r).unwrap();
    let a = of(&msgs, "a");
    assert_eq!(a.last().unwrap().msg_type, PstType::Failed);
    assert!(matches!(
        &a.last().unwrap().payload,
        ProgressPayload::Failure { code: "REJECTED", text } if text.contains("boom")
    ));
    let b = of(&msgs, "b");
    assert_eq!(types(&b), vec![PstType::Failed]);
    assert!(matches!(
        &b[0].payload,
        ProgressPayload::Failure { code: "PREREQUISITE_FAILED", text } if text.contains('a')
    ));
}

#[tokio::test]
async fn g5_refusals() {
    let r = run(approval_spec()).await;

    let mut c = approval_ctx();
    c.cause_id.clear();
    assert_eq!(
        messages_from_report(&c, &r).unwrap_err(),
        ProgressRefusal::NoCause
    );

    let mut c = approval_ctx();
    c.run_id = "run_other".into();
    assert!(matches!(
        messages_from_report(&c, &r).unwrap_err(),
        ProgressRefusal::RunMismatch { .. }
    ));

    let mut c = approval_ctx();
    c.effect_classes.remove(&tid("b"));
    assert_eq!(
        messages_from_report(&c, &r).unwrap_err(),
        ProgressRefusal::UnknownTask { task: tid("b") }
    );

    let mut c = approval_ctx();
    c.action_digests.clear();
    assert_eq!(
        messages_from_report(&c, &r).unwrap_err(),
        ProgressRefusal::WaitingWithoutActionDigest { task: tid("gate") }
    );

    let mut bad = r.clone();
    bad.snapshot.results.remove(&tid("a"));
    assert_eq!(
        messages_from_report(&approval_ctx(), &bad).unwrap_err(),
        ProgressRefusal::CompletedWithoutResult { task: tid("a") }
    );

    let mut bad = r.clone();
    bad.snapshot.results.get_mut(&tid("a")).unwrap().digest = "raw output".into();
    let e = messages_from_report(&approval_ctx(), &bad).unwrap_err();
    assert_eq!(e, ProgressRefusal::BadResultDigest { task: tid("a") });
    assert_eq!(e.name(), "PROGRESS_BAD_RESULT_DIGEST");
}

#[test]
fn g5_context_comes_from_the_stored_pointers() {
    let mut tasks = BTreeMap::new();
    tasks.insert(tid("a"), TaskPointers::no_effect());
    let mut w = TaskPointers::no_effect();
    w.effect_class = EffectClass::Effect;
    tasks.insert(tid("w"), w);
    let p = StoredPointers {
        run_id: RUN.into(),
        cause_id: CAUSE.into(),
        objective_id: OBJECTIVE.into(),
        spec_digest: "sha256:spec".into(),
        tasks,
    };
    let c = ProgressContext::from_pointers(&p);
    assert_eq!(
        (
            c.run_id.as_str(),
            c.cause_id.as_str(),
            c.objective_id.as_str()
        ),
        (RUN, CAUSE, OBJECTIVE)
    );
    assert_eq!(c.effect_classes[&tid("a")], EffectClass::None);
    assert_eq!(c.effect_classes[&tid("w")], EffectClass::Effect);
    assert!(c.action_digests.is_empty());
}
