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

// ---- G4 (#57): fire drills, rows 1 to 3 of docs/DELEGATED-DAG.md section 5 ----
//
// The stand-in world plays the authorities: a single-use grant desk that
// opens one effect intent per grant, an effect ledger whose reconcile looks
// at the world, receipts, and an effect counter. A kill drops what AEGIS
// holds in memory; the world and the stored pointers survive.

#[derive(Default)]
struct WorldState {
    next: u32,
    grants: HashMap<GrantId, GrantRecord>,
    /// Intent state and whether the effect touched the world.
    intents: HashMap<EffectIntentId, (EffectState, bool)>,
    receipts: HashSet<ReceiptDigest>,
    effects: u32,
}

#[derive(Default)]
struct World(std::sync::Mutex<WorldState>);

impl World {
    fn issue_grant(&self) -> GrantId {
        let mut w = self.0.lock().unwrap();
        w.next += 1;
        let g = grant(&format!("g{}", w.next));
        w.grants.insert(
            g.clone(),
            GrantRecord {
                status: GrantStatus::Available,
                intent: None,
            },
        );
        g
    }

    /// The desk spends the grant and opens one intent, or refuses.
    fn open_intent(&self, g: &GrantId) -> Result<EffectIntentId, String> {
        let mut w = self.0.lock().unwrap();
        match w.grants.get(g).map(|r| r.status) {
            Some(GrantStatus::Available) => {}
            other => return Err(format!("desk refuses {g}: {other:?}")),
        }
        w.next += 1;
        let i = intent(&format!("i{}", w.next));
        w.grants.insert(
            g.clone(),
            GrantRecord {
                status: GrantStatus::Spent,
                intent: Some(i.clone()),
            },
        );
        w.intents.insert(i.clone(), (EffectState::Open, false));
        Ok(i)
    }

    /// The effect itself: the only place the counter moves.
    fn apply(&self, i: &EffectIntentId) {
        let mut w = self.0.lock().unwrap();
        w.effects += 1;
        w.intents.get_mut(i).unwrap().1 = true;
    }

    fn ack(&self, i: &EffectIntentId) -> ReceiptDigest {
        let mut w = self.0.lock().unwrap();
        w.intents.get_mut(i).unwrap().0 = EffectState::Done;
        let r = receipt(&format!("r-{i}"));
        w.receipts.insert(r.clone());
        r
    }

    /// The authority settles an open intent by looking at the world. The
    /// terminal record is the receipt.
    fn reconcile(&self, i: &EffectIntentId) -> Option<ReceiptDigest> {
        let mut w = self.0.lock().unwrap();
        let (state, applied) = w.intents[i];
        if !matches!(state, EffectState::Open | EffectState::Unresolved) {
            return None;
        }
        if applied {
            w.intents.get_mut(i).unwrap().0 = EffectState::Done;
            let r = receipt(&format!("r-{i}"));
            w.receipts.insert(r.clone());
            Some(r)
        } else {
            w.intents.get_mut(i).unwrap().0 = EffectState::NotDone;
            None
        }
    }

    fn effects(&self) -> u32 {
        self.0.lock().unwrap().effects
    }
}

#[async_trait]
impl AuthorityLookup for World {
    async fn effect_state(&self, i: &EffectIntentId) -> Result<Option<EffectState>, LookupError> {
        Ok(self.0.lock().unwrap().intents.get(i).map(|(s, _)| *s))
    }

    async fn grant(&self, g: &GrantId) -> Result<Option<GrantRecord>, LookupError> {
        Ok(self.0.lock().unwrap().grants.get(g).cloned())
    }

    async fn receipt_present(&self, r: &ReceiptDigest) -> Result<bool, LookupError> {
        Ok(self.0.lock().unwrap().receipts.contains(r))
    }
}

fn line(spec: &WorkflowSpec, s: &aegis::orchestration::workflow::WorkflowSnapshot) -> String {
    spec.task_ids()
        .iter()
        .map(|id| format!("{id}={}", s.states[id].name()))
        .collect::<Vec<_>>()
        .join(" ")
}

/// The resume step after a restart: rebuild from the records, run exactly
/// the tasks that came back Ready (one at a time, in spec order), store
/// their pointers, and repeat until nothing is Ready. Uncertain and
/// WaitingForApproval tasks are never run.
async fn resume(
    spec: &WorkflowSpec,
    stored: &mut StoredPointers,
    world: &World,
    trace: &mut Vec<String>,
) -> aegis::orchestration::workflow::WorkflowSnapshot {
    loop {
        let snap = rebuild(spec, stored, world).await.expect("rebuild");
        trace.push(format!("rebuild {}", line(spec, &snap)));
        let ready: Vec<TaskId> = spec
            .task_ids()
            .into_iter()
            .filter(|id| snap.states[id] == TaskState::Ready)
            .collect();
        if ready.is_empty() {
            return snap;
        }
        for id in ready {
            let p = stored.tasks.get_mut(&id).unwrap();
            if p.effect_class == EffectClass::Effect {
                let g = p.grant.clone().expect("grant pointer");
                let i = world.open_intent(&g).expect("desk");
                p.effect_intent = Some(i.clone());
                world.apply(&i);
                p.receipt = Some(world.ack(&i));
            } else {
                p.result_digest = Some(format!("sha256:{id}"));
            }
            trace.push(format!("run {id}"));
        }
    }
}

/// A human approves again only when the rebuilt status asks for it.
fn approve_if_asked(
    snap: &aegis::orchestration::workflow::WorkflowSnapshot,
    stored: &mut StoredPointers,
    world: &World,
    trace: &mut Vec<String>,
) {
    if snap.states[&tid("write")] == TaskState::WaitingForApproval {
        let g = world.issue_grant();
        trace.push(format!("approve {g}"));
        stored.tasks.get_mut(&tid("write")).unwrap().grant = Some(g);
    }
}

fn fresh(world: &World) -> StoredPointers {
    let g = world.issue_grant();
    pointers(effect_pointers(None, g.as_str(), None))
}

/// Row 1: kill between approval and effect. The desk opened the intent,
/// AEGIS had not stored its id yet, and the effect never ran.
async fn drill_row1() -> (Vec<String>, u32) {
    let (w, spec) = (World::default(), spec());
    let mut stored = fresh(&w);
    let g1 = stored.tasks[&tid("write")].grant.clone().unwrap();
    let i1 = w.open_intent(&g1).unwrap();
    let mut t = vec!["kill after intent, before pointer and effect".to_string()];

    let s = resume(&spec, &mut stored, &w, &mut t).await;
    assert!(
        matches!(s.states[&tid("write")], TaskState::Uncertain(_)),
        "{}",
        line(&spec, &s)
    );
    assert_eq!(s.states[&tid("report")], TaskState::Pending);
    approve_if_asked(&s, &mut stored, &w, &mut t);
    assert_eq!(w.effects(), 0);

    assert_eq!(w.reconcile(&i1), None);
    t.push("reconcile NOT_DONE".into());
    let s = resume(&spec, &mut stored, &w, &mut t).await;
    assert_eq!(s.states[&tid("write")], TaskState::WaitingForApproval);
    approve_if_asked(&s, &mut stored, &w, &mut t);

    let s = resume(&spec, &mut stored, &w, &mut t).await;
    for id in ["write", "report", "lint"] {
        assert_eq!(s.states[&tid(id)], TaskState::Completed, "{id}");
    }
    (t, w.effects())
}

/// Row 2: kill after the effect, before acknowledgement.
async fn drill_row2() -> (Vec<String>, u32) {
    let (w, spec) = (World::default(), spec());
    let mut stored = fresh(&w);
    let g1 = stored.tasks[&tid("write")].grant.clone().unwrap();
    let i1 = w.open_intent(&g1).unwrap();
    stored.tasks.get_mut(&tid("write")).unwrap().effect_intent = Some(i1.clone());
    w.apply(&i1);
    let mut t = vec!["kill after effect, before ack".to_string()];

    let s = resume(&spec, &mut stored, &w, &mut t).await;
    assert!(
        matches!(s.states[&tid("write")], TaskState::Uncertain(_)),
        "{}",
        line(&spec, &s)
    );
    assert_eq!(s.states[&tid("report")], TaskState::Pending);
    approve_if_asked(&s, &mut stored, &w, &mut t);

    let r = w.reconcile(&i1).expect("world changed, so DONE");
    stored.tasks.get_mut(&tid("write")).unwrap().receipt = Some(r);
    t.push("reconcile DONE".into());
    let s = resume(&spec, &mut stored, &w, &mut t).await;
    approve_if_asked(&s, &mut stored, &w, &mut t);
    for id in ["write", "report", "lint"] {
        assert_eq!(s.states[&tid(id)], TaskState::Completed, "{id}");
    }
    (t, w.effects())
}

/// Admission stand-in for PST1 section 8 item 2: a repeated request with
/// the same idempotence key is the same request. No request store exists
/// in AEGIS yet, so this map lives here.
#[derive(Default)]
struct Admissions(HashMap<String, StoredPointers>);

impl Admissions {
    /// Returns the stored pointers of an earlier request with this key, or
    /// registers `fresh` as a new run.
    fn admit(&mut self, key: &str, fresh: StoredPointers) -> (StoredPointers, bool) {
        match self.0.get(key) {
            Some(p) => (p.clone(), false),
            None => {
                self.0.insert(key.into(), fresh.clone());
                (fresh, true)
            }
        }
    }
}

/// Row 3: the same request twice with one idempotence key.
async fn drill_row3() -> (Vec<String>, u32) {
    let (w, spec) = (World::default(), spec());
    let mut adm = Admissions::default();
    let mut t = Vec::new();

    let (mut stored, new) = adm.admit("k1", fresh(&w));
    assert!(new);
    let s = resume(&spec, &mut stored, &w, &mut t).await;
    assert_eq!(s.states[&tid("write")], TaskState::Completed);
    adm.0.insert("k1".into(), stored.clone());
    let first_run = s.run_id.clone();
    t.push("request 1 answered".into());

    // The repeat carries the same key; a fresh run would get its own id.
    let mut repeat = fresh(&w);
    repeat.run_id = "run_repeat".into();
    let (mut stored2, new) = adm.admit("k1", repeat);
    let s = resume(&spec, &mut stored2, &w, &mut t).await;
    t.push(format!("request 2 answered new={new}"));
    assert_eq!(s.run_id, first_run, "repeat must map to the first run");
    for id in ["write", "report", "lint"] {
        assert_eq!(s.states[&tid(id)], TaskState::Completed, "{id}");
    }
    (t, w.effects())
}

async fn twice(
    row: fn() -> futures_util::future::BoxFuture<'static, (Vec<String>, u32)>,
) -> Vec<String> {
    let (t1, e1) = row().await;
    let (t2, e2) = row().await;
    assert_eq!(e1, 1, "duplicate or missing final effect: {t1:?}");
    assert_eq!(e2, 1);
    assert_eq!(t1, t2, "event order differs between two runs");
    t1
}

#[tokio::test]
async fn drill_row1_kill_between_approval_and_effect() {
    let t = twice(|| Box::pin(drill_row1())).await;
    assert!(t.contains(&"approve g3".to_string()), "{t:?}");
}

#[tokio::test]
async fn drill_row2_kill_after_effect_before_ack() {
    let t = twice(|| Box::pin(drill_row2())).await;
    assert!(!t.iter().any(|l| l.starts_with("approve")), "{t:?}");
}

#[tokio::test]
async fn drill_row3_duplicate_request_one_idempotence_key() {
    let t = twice(|| Box::pin(drill_row3())).await;
    assert_eq!(t.iter().filter(|l| *l == "run write").count(), 1, "{t:?}");
}
