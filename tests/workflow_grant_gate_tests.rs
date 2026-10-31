//! G6 (#59): an effectful task starts only under a desk grant bound to its
//! objective and cause, checked again right before the effect. Fake desk;
//! no sovereign-core client.

use aegis::orchestration::grant_gate::{
    check, DeskGrant, EffectBody, GrantBinding, GrantDesk, GrantGate, GrantRefusal,
};
use aegis::orchestration::recovery::{GrantId, GrantStatus, LookupError};
use aegis::orchestration::workflow::{TaskContext, TaskOutcome, WorkflowBuilder};
use aegis::{
    CancelToken, EffectCertainty, FailureClass, RunId, SessionId, TaskId, TaskRunner, TaskState,
    WorkflowConfig, WorkflowExecutor,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const OBJECTIVE: &str = "sha256:objective_mine";
const CAUSE: &str = "cause_mine";

fn gid(s: &str) -> GrantId {
    GrantId::new(s)
}

#[derive(Default)]
struct FakeDesk {
    grants: Mutex<HashMap<GrantId, DeskGrant>>,
    down: Mutex<bool>,
}

impl FakeDesk {
    fn with(id: &str, status: GrantStatus, objective: &str, cause: &str) -> Arc<Self> {
        let d = Arc::new(Self::default());
        d.put(id, status, objective, cause);
        d
    }

    fn put(&self, id: &str, status: GrantStatus, objective: &str, cause: &str) {
        self.grants.lock().unwrap().insert(
            gid(id),
            DeskGrant {
                status,
                objective_id: objective.into(),
                cause_id: cause.into(),
            },
        );
    }

    fn set_status(&self, id: &str, status: GrantStatus) {
        self.grants
            .lock()
            .unwrap()
            .get_mut(&gid(id))
            .unwrap()
            .status = status;
    }
}

#[async_trait]
impl GrantDesk for FakeDesk {
    async fn grant(&self, id: &GrantId) -> Result<Option<DeskGrant>, LookupError> {
        if *self.down.lock().unwrap() {
            return Err(LookupError::new("desk unreachable"));
        }
        Ok(self.grants.lock().unwrap().get(id).cloned())
    }
}

/// Counts preparation and effects. `during_prepare` runs inside prepare,
/// so a test can change the desk between the two checks.
#[derive(Default)]
struct Body {
    prepared: AtomicUsize,
    applied: AtomicUsize,
    applied_with: Mutex<Vec<GrantId>>,
    during_prepare: Mutex<Option<Box<dyn Fn() + Send>>>,
}

#[async_trait]
impl EffectBody for Body {
    async fn prepare(&self, _ctx: &TaskContext) -> Result<(), TaskOutcome> {
        self.prepared.fetch_add(1, Ordering::SeqCst);
        if let Some(f) = self.during_prepare.lock().unwrap().as_ref() {
            f();
        }
        Ok(())
    }

    async fn apply(&self, _ctx: TaskContext, grant: GrantId) -> TaskOutcome {
        self.applied.fetch_add(1, Ordering::SeqCst);
        self.applied_with.lock().unwrap().push(grant);
        TaskOutcome::Completed(serde_json::json!({ "written": true }))
    }
}

fn binding(grant: Option<&str>) -> GrantBinding {
    GrantBinding {
        objective_id: OBJECTIVE.into(),
        cause_id: CAUSE.into(),
        grant: grant.map(gid),
    }
}

fn ctx() -> TaskContext {
    TaskContext {
        task_id: TaskId::new("write"),
        run_id: RunId::from_string("run_g6"),
        session_id: SessionId::from_string("sess_g6"),
        cancel: CancelToken::new(),
    }
}

async fn gate(desk: Arc<FakeDesk>, b: GrantBinding) -> (TaskOutcome, Arc<Body>) {
    let body = Arc::new(Body::default());
    let out = GrantGate::new(desk, b, body.clone()).run(ctx()).await;
    (out, body)
}

/// A refusal is a Rejected, no-effect failure carrying the refusal name.
fn assert_refused(out: &TaskOutcome, body: &Body, name: &str) {
    match out {
        TaskOutcome::Failed(f) => {
            assert!(f.message.starts_with(name), "{}", f.message);
            assert_eq!(f.certainty, EffectCertainty::NoEffect);
        }
        other => panic!("expected refusal {name}, got {other:?}"),
    }
    assert_eq!(body.applied.load(Ordering::SeqCst), 0, "{name}: effect ran");
}

/// Negative control: a good grant runs the effect once, under that grant.
#[tokio::test]
async fn g6_bound_available_grant_runs_the_effect_once() {
    let desk = FakeDesk::with("g1", GrantStatus::Available, OBJECTIVE, CAUSE);
    let (out, body) = gate(desk, binding(Some("g1"))).await;
    assert!(matches!(out, TaskOutcome::Completed(_)), "{out:?}");
    assert_eq!(body.applied.load(Ordering::SeqCst), 1);
    assert_eq!(*body.applied_with.lock().unwrap(), vec![gid("g1")]);
}

/// Refusal 1: no grant. The task asks for approval and nothing runs.
#[tokio::test]
async fn g6_no_grant_waits_for_approval_and_runs_nothing() {
    let desk = Arc::new(FakeDesk::default());
    let (out, body) = gate(desk.clone(), binding(None)).await;
    assert!(matches!(out, TaskOutcome::ApprovalPending), "{out:?}");
    assert_eq!(body.prepared.load(Ordering::SeqCst), 0);
    assert_eq!(body.applied.load(Ordering::SeqCst), 0);
    assert_eq!(
        check(desk.as_ref(), &binding(None)).await,
        Err(GrantRefusal::NoGrant)
    );
}

/// Refusal 2: stale grant (spent, expired or revoked).
#[tokio::test]
async fn g6_stale_grant_is_refused() {
    for status in [
        GrantStatus::Spent,
        GrantStatus::Expired,
        GrantStatus::Revoked,
    ] {
        let desk = FakeDesk::with("g1", status, OBJECTIVE, CAUSE);
        let (out, body) = gate(desk, binding(Some("g1"))).await;
        assert_refused(&out, &body, "GRANT_GATE_STALE");
        assert_eq!(body.prepared.load(Ordering::SeqCst), 0);
    }
}

/// Refusal 3: a grant for another objective.
#[tokio::test]
async fn g6_foreign_objective_grant_is_refused() {
    let desk = FakeDesk::with(
        "g1",
        GrantStatus::Available,
        "sha256:objective_other",
        CAUSE,
    );
    let (out, body) = gate(desk, binding(Some("g1"))).await;
    assert_refused(&out, &body, "GRANT_GATE_FOREIGN_OBJECTIVE");
}

/// Refusal 4: an id the desk never issued.
#[tokio::test]
async fn g6_forged_grant_id_is_refused() {
    let desk = FakeDesk::with("g1", GrantStatus::Available, OBJECTIVE, CAUSE);
    let (out, body) = gate(desk, binding(Some("g1-forged"))).await;
    assert_refused(&out, &body, "GRANT_GATE_FORGED");
}

#[tokio::test]
async fn g6_foreign_cause_grant_is_refused() {
    let desk = FakeDesk::with("g1", GrantStatus::Available, OBJECTIVE, "cause_other");
    let (out, body) = gate(desk, binding(Some("g1"))).await;
    assert_refused(&out, &body, "GRANT_GATE_FOREIGN_CAUSE");
}

#[tokio::test]
async fn g6_unbound_binding_is_refused_even_if_the_desk_matches() {
    let desk = FakeDesk::with("g1", GrantStatus::Available, "", CAUSE);
    let mut b = binding(Some("g1"));
    b.objective_id.clear();
    let (out, body) = gate(desk, b).await;
    assert_refused(&out, &body, "GRANT_GATE_UNBOUND");
}

#[tokio::test]
async fn g6_unreachable_desk_is_unavailable_with_no_effect() {
    let desk = FakeDesk::with("g1", GrantStatus::Available, OBJECTIVE, CAUSE);
    *desk.down.lock().unwrap() = true;
    let (out, body) = gate(desk, binding(Some("g1"))).await;
    match &out {
        TaskOutcome::Failed(f) => {
            assert_eq!(f.class, FailureClass::Unavailable);
            assert_eq!(f.certainty, EffectCertainty::NoEffect);
            assert!(f.message.starts_with("GRANT_GATE_DESK_UNAVAILABLE"));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(body.applied.load(Ordering::SeqCst), 0);
}

/// DoD item 2: the grant is checked again right before the effect. The
/// desk revokes it while the body prepares; the effect must not run.
#[tokio::test]
async fn g6_grant_revoked_during_prepare_stops_the_effect() {
    let desk = FakeDesk::with("g1", GrantStatus::Available, OBJECTIVE, CAUSE);
    let body = Arc::new(Body::default());
    let d = desk.clone();
    *body.during_prepare.lock().unwrap() =
        Some(Box::new(move || d.set_status("g1", GrantStatus::Revoked)));
    let out = GrantGate::new(desk, binding(Some("g1")), body.clone())
        .run(ctx())
        .await;
    assert_eq!(body.prepared.load(Ordering::SeqCst), 1);
    assert_refused(&out, &body, "GRANT_GATE_STALE");
}

/// Through the DAG: no grant leaves the task waiting for approval and its
/// dependent untouched; a forged grant fails it and skips the dependent.
#[tokio::test]
async fn g6_gate_inside_a_workflow() {
    let run = |b: GrantBinding| async move {
        let desk = FakeDesk::with("g1", GrantStatus::Available, OBJECTIVE, CAUSE);
        let body = Arc::new(Body::default());
        let spec = WorkflowBuilder::default()
            .add_task("write", &[], GrantGate::new(desk, b, body.clone()))
            .unwrap()
            .add_task(
                "report",
                &["write"],
                GrantGate::new(
                    Arc::new(FakeDesk::default()),
                    binding(None),
                    Arc::new(Body::default()),
                ),
            )
            .unwrap()
            .build()
            .unwrap();
        let cfg = WorkflowConfig::new(
            RunId::from_string("run_g6"),
            SessionId::from_string("sess_g6"),
            1,
        );
        let r = WorkflowExecutor::new(cfg).unwrap().run(spec).await;
        (r, body.applied.load(Ordering::SeqCst))
    };
    let (r, n) = run(binding(None)).await;
    assert_eq!(
        r.snapshot.states[&TaskId::new("write")],
        TaskState::WaitingForApproval
    );
    assert_eq!(
        r.snapshot.states[&TaskId::new("report")],
        TaskState::Pending
    );
    assert_eq!(n, 0);

    let (r, n) = run(binding(Some("forged"))).await;
    assert!(matches!(
        r.snapshot.states[&TaskId::new("write")],
        TaskState::Failed(_)
    ));
    assert!(matches!(
        r.snapshot.states[&TaskId::new("report")],
        TaskState::Skipped { .. }
    ));
    assert_eq!(n, 0);
}

#[test]
fn g6_refusal_names() {
    let g = gid("g");
    let all = [
        (GrantRefusal::Unbound, "GRANT_GATE_UNBOUND"),
        (GrantRefusal::NoGrant, "GRANT_GATE_NO_GRANT"),
        (
            GrantRefusal::Forged { grant: g.clone() },
            "GRANT_GATE_FORGED",
        ),
        (
            GrantRefusal::Stale {
                grant: g.clone(),
                status: GrantStatus::Spent,
            },
            "GRANT_GATE_STALE",
        ),
        (
            GrantRefusal::ForeignObjective { grant: g.clone() },
            "GRANT_GATE_FOREIGN_OBJECTIVE",
        ),
        (
            GrantRefusal::ForeignCause { grant: g },
            "GRANT_GATE_FOREIGN_CAUSE",
        ),
        (
            GrantRefusal::DeskUnavailable { detail: "x".into() },
            "GRANT_GATE_DESK_UNAVAILABLE",
        ),
    ];
    for (r, n) in all {
        assert_eq!(r.name(), n);
    }
}
