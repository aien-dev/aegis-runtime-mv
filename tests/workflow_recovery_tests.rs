//! Workflow rebuild after a restart (G3, issue #56). The authorities are
//! in-memory fakes: no storage, no sovereign-core client, no network.

use aegis::orchestration::recovery::{
    rebuild, spec_digest, AuthorityLookup, EffectClass, EffectIntentId, EffectState, GrantId,
    GrantRecord, GrantStatus, LookupError, ReceiptDigest, RecoveryRefusal, StoredPointers,
    TaskPointers,
};
use aegis::orchestration::workflow::{runner_fn, WorkflowSpec};
use aegis::{FailureClass, TaskId, TaskRunner, TaskState};
use async_trait::async_trait;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

/// The authorities as one in-memory stand-in. `down` makes every call fail
/// like an unreachable desk or ledger.
#[derive(Default)]
struct FakeAuthority {
    effects: HashMap<EffectIntentId, EffectState>,
    grants: HashMap<GrantId, GrantRecord>,
    receipts: HashSet<ReceiptDigest>,
    down: bool,
}

#[async_trait]
impl AuthorityLookup for FakeAuthority {
    async fn effect_state(
        &self,
        intent: &EffectIntentId,
    ) -> Result<Option<EffectState>, LookupError> {
        if self.down {
            return Err(LookupError::new("effect ledger unreachable"));
        }
        Ok(self.effects.get(intent).copied())
    }

    async fn grant(&self, grant: &GrantId) -> Result<Option<GrantRecord>, LookupError> {
        if self.down {
            return Err(LookupError::new("approval desk unreachable"));
        }
        Ok(self.grants.get(grant).cloned())
    }

    async fn receipt_present(&self, receipt: &ReceiptDigest) -> Result<bool, LookupError> {
        if self.down {
            return Err(LookupError::new("receipt store unreachable"));
        }
        Ok(self.receipts.contains(receipt))
    }
}

fn never() -> Arc<dyn TaskRunner> {
    runner_fn(|_c| Box::pin(async { panic!("rebuild must never run a task body") }))
}

fn tid(s: &str) -> TaskId {
    TaskId::new(s)
}

/// write (effect) -> report (no effect); lint (no effect) independent.
fn spec() -> WorkflowSpec {
    WorkflowSpec::builder()
        .add_task("write", &[], never())
        .unwrap()
        .add_task("report", &["write"], never())
        .unwrap()
        .add_task("lint", &[], never())
        .unwrap()
        .build()
        .unwrap()
}

fn intent(s: &str) -> EffectIntentId {
    EffectIntentId::new(s)
}
fn grant(s: &str) -> GrantId {
    GrantId::new(s)
}
fn receipt(s: &str) -> ReceiptDigest {
    ReceiptDigest::new(s)
}

fn pointers(write: TaskPointers) -> StoredPointers {
    let mut tasks = BTreeMap::new();
    tasks.insert(tid("write"), write);
    tasks.insert(tid("report"), TaskPointers::no_effect());
    tasks.insert(tid("lint"), TaskPointers::no_effect());
    StoredPointers {
        run_id: "run_g3".into(),
        cause_id: "cause-1".into(),
        objective_id: "objective-1".into(),
        spec_digest: spec_digest(&spec()),
        tasks,
    }
}

fn effect_pointers(i: Option<&str>, g: &str, r: Option<&str>) -> TaskPointers {
    TaskPointers {
        effect_class: EffectClass::Effect,
        effect_intent: i.map(intent),
        grant: Some(grant(g)),
        receipt: r.map(receipt),
        result_digest: None,
    }
}

fn spent(i: &str) -> GrantRecord {
    GrantRecord {
        status: GrantStatus::Spent,
        intent: Some(intent(i)),
    }
}

#[tokio::test]
async fn done_with_receipt_is_completed_and_never_runs_again() {
    let mut a = FakeAuthority::default();
    a.effects.insert(intent("i1"), EffectState::Done);
    a.grants.insert(grant("g1"), spent("i1"));
    a.receipts.insert(receipt("r1"));
    let p = pointers(effect_pointers(Some("i1"), "g1", Some("r1")));
    let s = rebuild(&spec(), &p, &a).await.unwrap();
    assert_eq!(s.states[&tid("write")], TaskState::Completed);
    assert!(s.states[&tid("write")].is_terminal());
    // Its no-effect dependent never finished, so it may run.
    assert_eq!(s.states[&tid("report")], TaskState::Ready);
    assert_eq!(s.states[&tid("lint")], TaskState::Ready);
    assert_eq!(s.run_id, "run_g3");
}

#[tokio::test]
async fn open_and_unresolved_are_uncertain_and_dependents_wait() {
    for state in [EffectState::Open, EffectState::Unresolved] {
        let mut a = FakeAuthority::default();
        a.effects.insert(intent("i1"), state);
        a.grants.insert(grant("g1"), spent("i1"));
        let p = pointers(effect_pointers(Some("i1"), "g1", None));
        let s = rebuild(&spec(), &p, &a).await.unwrap();
        match &s.states[&tid("write")] {
            TaskState::Uncertain(f) => assert_eq!(f.class, FailureClass::Uncertain),
            other => panic!("{state:?}: {other:?}"),
        }
        assert_eq!(s.states[&tid("report")], TaskState::Pending, "{state:?}");
    }
}

#[tokio::test]
async fn not_done_runs_again_only_under_a_valid_grant() {
    // Still-valid grant: may run again.
    let mut a = FakeAuthority::default();
    a.effects.insert(intent("i1"), EffectState::NotDone);
    a.grants.insert(
        grant("g1"),
        GrantRecord {
            status: GrantStatus::Available,
            intent: None,
        },
    );
    let p = pointers(effect_pointers(Some("i1"), "g1", None));
    let s = rebuild(&spec(), &p, &a).await.unwrap();
    assert_eq!(s.states[&tid("write")], TaskState::Ready);

    // Spent (or expired, or revoked) grant: needs a newly issued one.
    for status in [
        GrantStatus::Spent,
        GrantStatus::Expired,
        GrantStatus::Revoked,
    ] {
        let mut a = FakeAuthority::default();
        a.effects.insert(intent("i1"), EffectState::NotDone);
        a.grants.insert(
            grant("g1"),
            GrantRecord {
                status,
                intent: Some(intent("i1")),
            },
        );
        let s = rebuild(&spec(), &p, &a).await.unwrap();
        assert_eq!(
            s.states[&tid("write")],
            TaskState::WaitingForApproval,
            "{status:?}"
        );
        assert_eq!(s.states[&tid("report")], TaskState::Pending);
    }
}

#[tokio::test]
async fn spent_grant_without_effect_record_pointer_is_uncertain() {
    // Kill between intent and pointer write: AEGIS never stored the intent
    // id, but the desk says the grant was spent on one that is OPEN.
    let mut a = FakeAuthority::default();
    a.effects.insert(intent("i1"), EffectState::Open);
    a.grants.insert(grant("g1"), spent("i1"));
    let p = pointers(effect_pointers(None, "g1", None));
    let s = rebuild(&spec(), &p, &a).await.unwrap();
    assert!(matches!(s.states[&tid("write")], TaskState::Uncertain(_)));
    // Spent with no intent the desk can name: still not known.
    let mut a = FakeAuthority::default();
    a.grants.insert(
        grant("g1"),
        GrantRecord {
            status: GrantStatus::Spent,
            intent: None,
        },
    );
    let s = rebuild(&spec(), &p, &a).await.unwrap();
    assert!(matches!(s.states[&tid("write")], TaskState::Uncertain(_)));
}

#[tokio::test]
async fn unstarted_effect_task_with_available_grant_is_ready() {
    let mut a = FakeAuthority::default();
    a.grants.insert(
        grant("g1"),
        GrantRecord {
            status: GrantStatus::Available,
            intent: None,
        },
    );
    let p = pointers(effect_pointers(None, "g1", None));
    let s = rebuild(&spec(), &p, &a).await.unwrap();
    assert_eq!(s.states[&tid("write")], TaskState::Ready);
}

#[tokio::test]
async fn no_effect_task_with_result_is_completed() {
    let a = FakeAuthority::default();
    let mut p = pointers(TaskPointers::no_effect());
    p.tasks.get_mut(&tid("lint")).unwrap().result_digest = Some("sha256:ab".into());
    let s = rebuild(&spec(), &p, &a).await.unwrap();
    assert_eq!(s.states[&tid("lint")], TaskState::Completed);
    assert_eq!(s.results[&tid("lint")].digest, "sha256:ab");
    assert_eq!(s.states[&tid("write")], TaskState::Ready);
}

// ---- named refusals, never a silent fresh start ----

#[tokio::test]
async fn missing_pointers_are_refused_by_name() {
    let a = FakeAuthority::default();
    let mut p = pointers(TaskPointers::no_effect());
    p.tasks.remove(&tid("report"));
    let e = rebuild(&spec(), &p, &a).await.unwrap_err();
    assert_eq!(e, RecoveryRefusal::MissingPointers(tid("report")));
    assert_eq!(e.name(), "RECOVERY_MISSING_POINTERS");
}

#[tokio::test]
async fn unknown_task_pointer_is_refused() {
    let a = FakeAuthority::default();
    let mut p = pointers(TaskPointers::no_effect());
    p.tasks.insert(tid("ghost"), TaskPointers::no_effect());
    let e = rebuild(&spec(), &p, &a).await.unwrap_err();
    assert_eq!(e, RecoveryRefusal::UnknownTaskPointer(tid("ghost")));
}

#[tokio::test]
async fn changed_spec_is_refused() {
    let a = FakeAuthority::default();
    let mut p = pointers(TaskPointers::no_effect());
    p.spec_digest = "sha256:00".into();
    let e = rebuild(&spec(), &p, &a).await.unwrap_err();
    assert!(matches!(e, RecoveryRefusal::SpecMismatch { .. }), "{e:?}");
}

#[tokio::test]
async fn unreachable_authority_is_refused() {
    let a = FakeAuthority {
        down: true,
        ..Default::default()
    };
    let p = pointers(effect_pointers(Some("i1"), "g1", None));
    let e = rebuild(&spec(), &p, &a).await.unwrap_err();
    assert!(
        matches!(&e, RecoveryRefusal::LookupUnavailable { task, .. } if *task == tid("write")),
        "{e:?}"
    );
}

#[tokio::test]
async fn effect_task_without_grant_or_unknown_records_is_refused() {
    let a = FakeAuthority::default();
    // A pointer naming an intent the ledger does not hold.
    let mut a2 = FakeAuthority::default();
    a2.grants.insert(grant("g1"), spent("i1"));
    let p = pointers(effect_pointers(Some("i1"), "g1", None));
    let e = rebuild(&spec(), &p, &a2).await.unwrap_err();
    assert!(
        matches!(e, RecoveryRefusal::EffectRecordMissing { .. }),
        "{e:?}"
    );
    // A grant the desk never issued.
    let e = rebuild(&spec(), &p, &a).await.unwrap_err();
    assert!(matches!(e, RecoveryRefusal::GrantUnknown { .. }), "{e:?}");
    // An effect task with no grant pointer at all.
    let mut bare = effect_pointers(None, "g1", None);
    bare.grant = None;
    let e = rebuild(&spec(), &pointers(bare), &a).await.unwrap_err();
    assert_eq!(e, RecoveryRefusal::MissingPointers(tid("write")));
}

#[tokio::test]
async fn pointer_and_desk_disagreeing_on_the_intent_is_refused() {
    let mut a = FakeAuthority::default();
    a.effects.insert(intent("i1"), EffectState::Done);
    a.effects.insert(intent("i2"), EffectState::Done);
    a.grants.insert(grant("g1"), spent("i2"));
    let p = pointers(effect_pointers(Some("i1"), "g1", Some("r1")));
    let e = rebuild(&spec(), &p, &a).await.unwrap_err();
    assert!(matches!(e, RecoveryRefusal::IntentMismatch { .. }), "{e:?}");
}

/// Negative control (#56 item 4): DONE without a receipt is refused, both
/// when no receipt pointer was stored and when the authority does not hold
/// the receipt the pointer names.
#[tokio::test]
async fn negative_control_done_without_receipt_is_refused() {
    let mut a = FakeAuthority::default();
    a.effects.insert(intent("i1"), EffectState::Done);
    a.grants.insert(grant("g1"), spent("i1"));
    for r in [None, Some("r-missing")] {
        let p = pointers(effect_pointers(Some("i1"), "g1", r));
        let e = rebuild(&spec(), &p, &a).await.unwrap_err();
        assert!(
            matches!(&e, RecoveryRefusal::DoneWithoutReceipt { task, .. } if *task == tid("write")),
            "{r:?}: {e:?}"
        );
        assert_eq!(e.name(), "RECOVERY_DONE_WITHOUT_RECEIPT");
    }
}

#[tokio::test]
async fn rebuild_is_a_pure_function_of_the_records() {
    let mut a = FakeAuthority::default();
    a.effects.insert(intent("i1"), EffectState::Unresolved);
    a.grants.insert(grant("g1"), spent("i1"));
    let p = pointers(effect_pointers(Some("i1"), "g1", None));
    let s1 = rebuild(&spec(), &p, &a).await.unwrap();
    let s2 = rebuild(&spec(), &p, &a).await.unwrap();
    assert_eq!(
        serde_json::to_string(&s1).unwrap(),
        serde_json::to_string(&s2).unwrap()
    );
}

#[test]
fn spec_digest_ignores_insertion_order_and_sees_edges() {
    let a = spec();
    let b = WorkflowSpec::builder()
        .add_task("lint", &[], never())
        .unwrap()
        .add_task("report", &["write"], never())
        .unwrap()
        .add_task("write", &[], never())
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(spec_digest(&a), spec_digest(&b));
    let c = WorkflowSpec::builder()
        .add_task("write", &[], never())
        .unwrap()
        .add_task("report", &[], never())
        .unwrap()
        .add_task("lint", &[], never())
        .unwrap()
        .build()
        .unwrap();
    assert_ne!(spec_digest(&a), spec_digest(&c));
}
