//! Task DAG tests (issue #39). CPU only, deterministic ordering assertions,
//! sleep-based fake tasks. Timings are printed for information, never asserted.

use aegis::orchestration::workflow::{
    runner_fn, runner_fn_with_certainty, TaskContext, TaskOutcome, WorkflowBuilder,
};
use aegis::{
    AegisEvent, CancelToken, EffectCertainty, EventBus, FailureClass, RunId, SessionId, TaskId,
    TaskRunner, TaskState, ToolFault, WorkflowConfig, WorkflowError, WorkflowExecutor,
    WorkflowSpec, WorkflowStatus,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn cfg(max_parallel: usize) -> WorkflowConfig {
    WorkflowConfig::new(
        RunId::from_string("run_test"),
        SessionId::from_string("sess_test"),
        max_parallel,
    )
}

fn ok(ms: u64) -> Arc<dyn TaskRunner> {
    runner_fn(move |_c: TaskContext| {
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            TaskOutcome::Completed(serde_json::json!({ "ms": ms }))
        })
    })
}

fn failing(class: FailureClass, cert: EffectCertainty) -> Arc<dyn TaskRunner> {
    runner_fn(move |_c: TaskContext| {
        Box::pin(async move { TaskOutcome::Failed(ToolFault::new(class, cert, "boom")) })
    })
}

fn tid(s: &str) -> TaskId {
    TaskId::new(s)
}

fn b() -> WorkflowBuilder {
    WorkflowSpec::builder()
}

/// Records start/finish order and a counter of runs per task.
struct Probe {
    log: Mutex<Vec<String>>,
    runs: Mutex<std::collections::HashMap<String, usize>>,
    live: AtomicUsize,
    peak: AtomicUsize,
}

impl Probe {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            log: Mutex::new(vec![]),
            runs: Mutex::new(Default::default()),
            live: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        })
    }

    fn task(self: &Arc<Self>, name: &'static str, ms: u64) -> Arc<dyn TaskRunner> {
        let p = self.clone();
        runner_fn(move |_c| {
            let p = p.clone();
            Box::pin(async move {
                let now = p.live.fetch_add(1, Ordering::SeqCst) + 1;
                p.peak.fetch_max(now, Ordering::SeqCst);
                p.log.lock().unwrap().push(format!("start:{name}"));
                *p.runs.lock().unwrap().entry(name.into()).or_default() += 1;
                tokio::time::sleep(Duration::from_millis(ms)).await;
                p.log.lock().unwrap().push(format!("end:{name}"));
                p.live.fetch_sub(1, Ordering::SeqCst);
                TaskOutcome::Completed(serde_json::json!(name))
            })
        })
    }

    fn runs(&self, name: &str) -> usize {
        self.runs.lock().unwrap().get(name).copied().unwrap_or(0)
    }

    fn pos(&self, entry: &str) -> usize {
        self.log
            .lock()
            .unwrap()
            .iter()
            .position(|e| e == entry)
            .unwrap_or_else(|| panic!("missing {entry}"))
    }
}

#[tokio::test]
async fn fixture_a_b_independent_c_waits_d_blocked_on_failure() {
    let p = Probe::new();
    let spec = b()
        .add_task("A", &[], p.task("A", 30))
        .unwrap()
        .add_task("B", &[], p.task("B", 30))
        .unwrap()
        .add_task("C", &["A", "B"], p.task("C", 5))
        .unwrap()
        .add_task(
            "F",
            &[],
            failing(FailureClass::Failed, EffectCertainty::EffectOccurred),
        )
        .unwrap()
        .add_task("D", &["F"], p.task("D", 5))
        .unwrap()
        .build()
        .unwrap();
    let r = WorkflowExecutor::new(cfg(4)).unwrap().run(spec).await;

    assert_eq!(p.runs("C"), 1, "C runs exactly once");
    assert!(p.pos("end:A") < p.pos("start:C"));
    assert!(p.pos("end:B") < p.pos("start:C"));
    assert!(p.pos("start:B") < p.pos("end:A"), "A and B overlapped");
    assert_eq!(p.runs("D"), 0, "D never starts");
    assert_eq!(r.snapshot.states[&tid("C")], TaskState::Completed);
    assert!(matches!(r.snapshot.states[&tid("F")], TaskState::Failed(_)));
    assert_eq!(
        r.snapshot.states[&tid("D")],
        TaskState::Skipped {
            prerequisite: tid("F")
        }
    );
    let rec = &r.failures[&tid("D")];
    assert_eq!(rec.class, FailureClass::Rejected);
    assert_eq!(rec.effect_certainty, EffectCertainty::NoEffect);
    assert_eq!(rec.cause[0].message, "prerequisite failed: F");
    assert_eq!(r.status, WorkflowStatus::Failed);
    assert!(r.snapshot.results.contains_key(&tid("C")));
    assert!(!r.snapshot.results.contains_key(&tid("D")));
}

#[tokio::test]
async fn skip_propagates_transitively() {
    let spec = b()
        .add_task(
            "F",
            &[],
            failing(FailureClass::Rejected, EffectCertainty::NoEffect),
        )
        .unwrap()
        .add_task("X", &["F"], ok(1))
        .unwrap()
        .add_task("Y", &["X"], ok(1))
        .unwrap()
        .build()
        .unwrap();
    let r = WorkflowExecutor::new(cfg(2)).unwrap().run(spec).await;
    assert_eq!(
        r.snapshot.states[&tid("X")],
        TaskState::Skipped {
            prerequisite: tid("F")
        }
    );
    assert_eq!(
        r.snapshot.states[&tid("Y")],
        TaskState::Skipped {
            prerequisite: tid("X")
        }
    );
}

#[test]
fn rejects_cycle() {
    let spec = b()
        .add_task("A", &["C"], ok(1))
        .unwrap()
        .add_task("B", &["A"], ok(1))
        .unwrap()
        .add_task("C", &["B"], ok(1))
        .unwrap()
        .add_task("Z", &[], ok(1))
        .unwrap()
        .build();
    match spec {
        Err(WorkflowError::Cycle(c)) => {
            assert_eq!(c.len(), 3);
            assert!(!c.contains(&tid("Z")));
        }
        _ => panic!("expected cycle"),
    }
}

#[test]
fn rejects_self_dependency() {
    let e = b().add_task("A", &["A"], ok(1)).err().unwrap();
    assert_eq!(e, WorkflowError::SelfDependency(tid("A")));
}

#[test]
fn rejects_unknown_predecessor() {
    let e = b()
        .add_task("A", &["ghost"], ok(1))
        .unwrap()
        .build()
        .err()
        .unwrap();
    assert_eq!(
        e,
        WorkflowError::UnknownPredecessor {
            task: tid("A"),
            predecessor: tid("ghost")
        }
    );
}

#[test]
fn rejects_duplicate_task_and_duplicate_edge() {
    let e = b()
        .add_task("A", &[], ok(1))
        .unwrap()
        .add_task("A", &[], ok(1))
        .err()
        .unwrap();
    assert_eq!(e, WorkflowError::DuplicateTask(tid("A")));
    let e = b()
        .add_task("A", &[], ok(1))
        .unwrap()
        .add_task("B", &["A", "A"], ok(1))
        .err()
        .unwrap();
    assert_eq!(
        e,
        WorkflowError::DuplicateEdge {
            task: tid("B"),
            predecessor: tid("A")
        }
    );
}

#[test]
fn rejects_zero_parallelism() {
    assert!(matches!(
        WorkflowExecutor::new(cfg(0)),
        Err(WorkflowError::InvalidConfig(_))
    ));
}

#[tokio::test]
async fn timeout_is_failed_timeout_with_runner_certainty() {
    let slow = |c: EffectCertainty| {
        runner_fn_with_certainty(
            |_c| {
                Box::pin(async {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    TaskOutcome::Completed(serde_json::json!(null))
                })
            },
            c,
        )
    };
    let spec = b()
        .add_task("read_only", &[], slow(EffectCertainty::NoEffect))
        .unwrap()
        .add_task("writer", &[], slow(EffectCertainty::Uncertain))
        .unwrap()
        .add_task("after", &["writer"], ok(1))
        .unwrap()
        .build()
        .unwrap();
    let mut c = cfg(2);
    c.per_task_timeout = Some(Duration::from_millis(40));
    let r = WorkflowExecutor::new(c).unwrap().run(spec).await;
    match &r.snapshot.states[&tid("read_only")] {
        TaskState::Failed(f) => {
            assert_eq!(f.class, FailureClass::Timeout);
            assert_eq!(f.effect_certainty, EffectCertainty::NoEffect);
        }
        other => panic!("read_only: {other:?}"),
    }
    // Since #55 an Uncertain timeout is not reported as Failed, and its
    // dependent waits behind it instead of being skipped.
    match &r.snapshot.states[&tid("writer")] {
        TaskState::Uncertain(f) => {
            assert_eq!(f.class, FailureClass::Timeout);
            assert_eq!(f.effect_certainty, EffectCertainty::Uncertain);
        }
        other => panic!("writer: {other:?}"),
    }
    assert_eq!(r.snapshot.states[&tid("after")], TaskState::Pending);
}

#[tokio::test]
async fn cancellation_cancels_running_and_pending_and_starts_nothing_new() {
    let p = Probe::new();
    let spec = b()
        .add_task(
            "long",
            &[],
            // Waits for the token and touches nothing: NoEffect (#55).
            runner_fn_with_certainty(
                |c: TaskContext| {
                    Box::pin(async move {
                        c.cancel.cancelled().await;
                        TaskOutcome::Cancelled
                    })
                },
                EffectCertainty::NoEffect,
            ),
        )
        .unwrap()
        .add_task("next", &["long"], p.task("next", 1))
        .unwrap()
        .add_task("other", &[], p.task("other", 1))
        .unwrap()
        .build()
        .unwrap();
    let cancel = CancelToken::new();
    let c2 = cancel.clone();
    let exec = WorkflowExecutor::new(cfg(1))
        .unwrap()
        .with_cancel_token(cancel);
    let h = tokio::spawn(async move { exec.run(spec).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    c2.cancel();
    let r = h.await.unwrap();
    assert_eq!(r.status, WorkflowStatus::Cancelled);
    assert_eq!(r.snapshot.states[&tid("long")], TaskState::Cancelled);
    assert_eq!(r.snapshot.states[&tid("next")], TaskState::Cancelled);
    assert_eq!(r.snapshot.states[&tid("other")], TaskState::Cancelled);
    assert_eq!(p.runs("next") + p.runs("other"), 0);
}

#[tokio::test]
async fn total_budget_cancels_the_workflow() {
    let spec = b()
        // A sleeper with no effect, so the interrupt is a plain cancel (#55).
        .add_task("slow", &[], hang(EffectCertainty::NoEffect))
        .unwrap()
        .add_task("n", &["slow"], ok(1))
        .unwrap()
        .build()
        .unwrap();
    let mut c = cfg(2);
    c.total_budget = Some(Duration::from_millis(40));
    let r = WorkflowExecutor::new(c).unwrap().run(spec).await;
    assert!(r.budget_exhausted);
    assert_eq!(r.status, WorkflowStatus::Cancelled);
    assert_eq!(r.snapshot.states[&tid("slow")], TaskState::Cancelled);
    assert_eq!(r.snapshot.states[&tid("n")], TaskState::Cancelled);
}

#[tokio::test]
async fn max_parallel_is_honoured() {
    for limit in [1usize, 2, 3] {
        let p = Probe::new();
        let mut bld = b();
        for n in ["t1", "t2", "t3", "t4", "t5", "t6"] {
            bld = bld.add_task(n, &[], p.task(n, 15)).unwrap();
        }
        let r = WorkflowExecutor::new(cfg(limit))
            .unwrap()
            .run(bld.build().unwrap())
            .await;
        assert_eq!(r.status, WorkflowStatus::Completed);
        assert!(p.peak.load(Ordering::SeqCst) <= limit, "limit {limit}");
        assert_eq!(
            p.peak.load(Ordering::SeqCst),
            limit,
            "limit {limit} is reached"
        );
        assert!(r.peak_concurrency <= limit);
    }
}

#[tokio::test]
async fn approval_pending_blocks_dependents_and_is_not_success() {
    let p = Probe::new();
    let spec = b()
        .add_task(
            "gate",
            &[],
            runner_fn(|_c| Box::pin(async { TaskOutcome::ApprovalPending })),
        )
        .unwrap()
        .add_task("after", &["gate"], p.task("after", 1))
        .unwrap()
        .add_task("later", &["after"], p.task("later", 1))
        .unwrap()
        .add_task("free", &[], p.task("free", 1))
        .unwrap()
        .build()
        .unwrap();
    let r = WorkflowExecutor::new(cfg(2)).unwrap().run(spec).await;
    assert_eq!(r.status, WorkflowStatus::WaitingForApproval);
    assert_eq!(
        r.snapshot.states[&tid("gate")],
        TaskState::WaitingForApproval
    );
    assert_eq!(r.snapshot.states[&tid("after")], TaskState::Pending);
    assert_eq!(r.snapshot.states[&tid("free")], TaskState::Completed);
    assert_eq!(p.runs("after") + p.runs("later"), 0);
    assert_eq!(r.unfinished, vec![tid("gate"), tid("after"), tid("later")]);
}

#[tokio::test]
async fn uncertain_outcome_is_never_rescheduled() {
    let n = Arc::new(AtomicUsize::new(0));
    let n2 = n.clone();
    let spec = b()
        .add_task(
            "write",
            &[],
            runner_fn(move |_c| {
                let n = n2.clone();
                Box::pin(async move {
                    n.fetch_add(1, Ordering::SeqCst);
                    TaskOutcome::Failed(ToolFault::new(
                        FailureClass::Uncertain,
                        EffectCertainty::Uncertain,
                        "lost response",
                    ))
                })
            }),
        )
        .unwrap()
        .add_task("dep", &["write"], ok(1))
        .unwrap()
        .build()
        .unwrap();
    let r = WorkflowExecutor::new(cfg(2)).unwrap().run(spec).await;
    assert_eq!(n.load(Ordering::SeqCst), 1);
    match &r.snapshot.states[&tid("write")] {
        TaskState::Failed(f) => {
            assert_eq!(f.class, FailureClass::Uncertain);
            assert_eq!(f.effect_certainty, EffectCertainty::Uncertain);
        }
        o => panic!("{o:?}"),
    }
    assert!(matches!(
        r.snapshot.states[&tid("dep")],
        TaskState::Skipped { .. }
    ));
}

fn diamond(p: &Arc<Probe>) -> WorkflowSpec {
    b().add_task("A", &[], p.task("A", 5))
        .unwrap()
        .add_task("B", &["A"], p.task("B", 5))
        .unwrap()
        .add_task("C", &["A"], p.task("C", 5))
        .unwrap()
        .add_task("D", &["B", "C"], p.task("D", 5))
        .unwrap()
        .build()
        .unwrap()
}

#[tokio::test]
async fn event_sequence_is_deterministic_and_matches_the_bus() {
    let mut runs = vec![];
    for _ in 0..2 {
        let bus = EventBus::default();
        let mut rx = bus.subscribe();
        let p = Probe::new();
        // max_parallel 1 removes completion races, so the whole order is fixed.
        let r = WorkflowExecutor::new(cfg(1))
            .unwrap()
            .with_event_bus(bus)
            .run(diamond(&p))
            .await;
        let mut bus_seq = vec![];
        while let Ok(env) = rx.try_recv() {
            let name = match env.event {
                AegisEvent::WorkflowTaskReady { task_id, .. } => format!("ready:{task_id}"),
                AegisEvent::WorkflowTaskStarted { task_id, .. } => format!("started:{task_id}"),
                AegisEvent::WorkflowTaskCompleted {
                    task_id,
                    result_digest,
                    ..
                } => {
                    assert!(result_digest.starts_with("sha256:"));
                    format!("completed:{task_id}")
                }
                AegisEvent::WorkflowFinished { .. } => "finished".into(),
                other => panic!("unexpected {other:?}"),
            };
            bus_seq.push((env.sequence, name));
        }
        let from_report: Vec<(u64, String)> = r
            .events
            .iter()
            .map(|e| {
                let n = match &e.task_id {
                    Some(t) => format!("{}:{t}", e.state),
                    None => e.state.clone(),
                };
                (e.seq, n.replace("running", "started"))
            })
            .collect();
        assert_eq!(bus_seq, from_report, "bus order equals recorded order");
        let seqs: Vec<u64> = bus_seq.iter().map(|e| e.0).collect();
        assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
        runs.push(bus_seq);
    }
    assert_eq!(runs[0], runs[1]);
}

#[tokio::test]
async fn parallel_run_sequence_is_gapless_and_orders_dependencies() {
    let p = Probe::new();
    let r = WorkflowExecutor::new(cfg(4))
        .unwrap()
        .run(diamond(&p))
        .await;
    let seqs: Vec<u64> = r.events.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
    let at = |t: &str, s: &str| {
        r.events
            .iter()
            .find(|e| e.task_id.as_ref().map(|x| x.as_str()) == Some(t) && e.state == s)
            .unwrap()
            .seq
    };
    assert!(at("A", "completed") < at("B", "ready"));
    assert!(at("B", "completed") < at("D", "ready"));
    assert!(at("C", "completed") < at("D", "ready"));
}

#[tokio::test]
async fn snapshot_is_serializable_and_deterministic() {
    let p = Probe::new();
    let r = WorkflowExecutor::new(cfg(1))
        .unwrap()
        .run(diamond(&p))
        .await;
    let j1 = serde_json::to_string(&r.snapshot).unwrap();
    let j2 = serde_json::to_string(&r.snapshot.clone()).unwrap();
    assert_eq!(j1, j2);
    let back: aegis::orchestration::workflow::WorkflowSnapshot = serde_json::from_str(&j1).unwrap();
    assert_eq!(back, r.snapshot);
    // A failed task also round trips (it carries a FailureRecord).
    let spec = b()
        .add_task(
            "F",
            &[],
            failing(FailureClass::Failed, EffectCertainty::EffectOccurred),
        )
        .unwrap()
        .build()
        .unwrap();
    let r = WorkflowExecutor::new(cfg(1)).unwrap().run(spec).await;
    let j = serde_json::to_string(&r.snapshot).unwrap();
    let back: aegis::orchestration::workflow::WorkflowSnapshot = serde_json::from_str(&j).unwrap();
    assert_eq!(back, r.snapshot);
}

/// Demo: edit and lint are independent, test needs both, a build task fails
/// and its dependent report is skipped. Same four-plus tasks run with
/// max_parallel 1 (sequential) and 2. Only ordering and the bound are asserted.
#[tokio::test]
async fn demo_code_test_eval_dag_sequential_vs_parallel() {
    async fn run(
        limit: usize,
        p: &Arc<Probe>,
    ) -> (Duration, aegis::orchestration::workflow::WorkflowReport) {
        let spec = b()
            .add_task("edit", &[], p.task("edit", 40))
            .unwrap()
            .add_task("lint", &[], p.task("lint", 40))
            .unwrap()
            .add_task("test", &["edit", "lint"], p.task("test", 20))
            .unwrap()
            .add_task(
                "build_docs",
                &[],
                failing(FailureClass::Failed, EffectCertainty::EffectOccurred),
            )
            .unwrap()
            .add_task("report", &["build_docs"], p.task("report", 20))
            .unwrap()
            .build()
            .unwrap();
        let t = Instant::now();
        let r = WorkflowExecutor::new(cfg(limit)).unwrap().run(spec).await;
        (t.elapsed(), r)
    }
    let p1 = Probe::new();
    let (seq_t, r1) = run(1, &p1).await;
    let p2 = Probe::new();
    let (par_t, r2) = run(2, &p2).await;
    println!("INFO sequential(max_parallel=1) {seq_t:?}  dag(max_parallel=2) {par_t:?}");
    assert!(p1.peak.load(Ordering::SeqCst) <= 1);
    assert!(p2.peak.load(Ordering::SeqCst) <= 2);
    assert!(p2.pos("end:edit") < p2.pos("start:test"));
    assert!(p2.pos("end:lint") < p2.pos("start:test"));
    assert_eq!(p2.runs("report"), 0);
    for r in [&r1, &r2] {
        assert_eq!(r.snapshot.states[&tid("test")], TaskState::Completed);
        assert!(matches!(
            r.snapshot.states[&tid("report")],
            TaskState::Skipped { .. }
        ));
    }
}

fn gapless(r: &aegis::WorkflowReport) {
    let seqs: Vec<u64> = r.events.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
    assert_eq!(r.snapshot.last_seq, seqs.len() as u64);
}

fn agent_result(
    calls: Vec<aegis::ToolExecutionRecord>,
    completed: bool,
) -> aegis::AgentExecutionResult {
    aegis::AgentExecutionResult {
        final_response: "done".into(),
        steps: vec![aegis::AgentStep {
            step_index: 0,
            thought: None,
            tool_calls: calls,
        }],
        turns_taken: 1,
        duration_ms: 0,
        completed,
    }
}

fn call(success: bool) -> aegis::ToolExecutionRecord {
    aegis::ToolExecutionRecord {
        call_id: "c1".into(),
        tool_name: "t".into(),
        arguments: serde_json::json!({}),
        output: String::new(),
        success,
        failure: None,
        retry_failures: vec![],
    }
}

#[test]
fn agent_outcome_unrecorded_failure_is_failed_and_uncertain() {
    use aegis::orchestration::workflow::agent_outcome;
    // success=false with no failure record must never read as success.
    match agent_outcome(&agent_result(vec![call(false)], true)) {
        TaskOutcome::Failed(f) => {
            assert_eq!(f.class, FailureClass::Failed);
            assert_eq!(f.certainty, EffectCertainty::Uncertain);
        }
        other => panic!("expected Failed, got {other:?}"),
    }
    // Control: a clean completed run is still success.
    assert!(matches!(
        agent_outcome(&agent_result(vec![call(true)], true)),
        TaskOutcome::Completed(_)
    ));
    // Incomplete run stays Failed/Uncertain.
    assert!(matches!(
        agent_outcome(&agent_result(vec![call(true)], false)),
        TaskOutcome::Failed(_)
    ));
}

#[tokio::test]
async fn cancel_while_waiting_for_approval_leaves_nothing_unfinished() {
    let p = Probe::new();
    let spec = b()
        .add_task(
            "gate",
            &[],
            runner_fn(|_c| Box::pin(async { TaskOutcome::ApprovalPending })),
        )
        .unwrap()
        .add_task("after", &["gate"], p.task("after", 1))
        .unwrap()
        .add_task(
            "long",
            &[],
            // Waits for the token and touches nothing: NoEffect (#55).
            runner_fn_with_certainty(
                |c: TaskContext| {
                    Box::pin(async move {
                        c.cancel.cancelled().await;
                        TaskOutcome::Cancelled
                    })
                },
                EffectCertainty::NoEffect,
            ),
        )
        .unwrap()
        .build()
        .unwrap();
    let cancel = CancelToken::new();
    let c2 = cancel.clone();
    let exec = WorkflowExecutor::new(cfg(2))
        .unwrap()
        .with_cancel_token(cancel);
    let h = tokio::spawn(async move { exec.run(spec).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    c2.cancel();
    let r = h.await.unwrap();
    assert_eq!(r.status, WorkflowStatus::Cancelled);
    assert_eq!(r.snapshot.states[&tid("gate")], TaskState::Cancelled);
    assert_eq!(r.snapshot.states[&tid("after")], TaskState::Cancelled);
    assert_eq!(r.snapshot.states[&tid("long")], TaskState::Cancelled);
    assert!(r.unfinished.is_empty(), "unfinished: {:?}", r.unfinished);
    assert_eq!(p.runs("after"), 0);
    gapless(&r);
}

#[tokio::test]
async fn budget_exhaustion_does_not_cancel_the_callers_token() {
    let spec = b()
        .add_task("slow", &[], ok(10_000))
        .unwrap()
        .build()
        .unwrap();
    let caller = CancelToken::new();
    let mut c = cfg(1);
    c.total_budget = Some(Duration::from_millis(40));
    let r = WorkflowExecutor::new(c)
        .unwrap()
        .with_cancel_token(caller.clone())
        .run(spec)
        .await;
    assert!(r.budget_exhausted);
    assert_eq!(r.status, WorkflowStatus::Cancelled);
    assert!(!caller.is_cancelled(), "executor must only read the token");
}

#[tokio::test]
async fn caller_cancel_still_stops_the_workflow_through_the_run_token() {
    let spec = b()
        .add_task("slow", &[], ok(10_000))
        .unwrap()
        .build()
        .unwrap();
    let caller = CancelToken::new();
    caller.cancel();
    let r = WorkflowExecutor::new(cfg(1))
        .unwrap()
        .with_cancel_token(caller)
        .run(spec)
        .await;
    assert_eq!(r.status, WorkflowStatus::Cancelled);
    assert_eq!(r.snapshot.states[&tid("slow")], TaskState::Cancelled);
}

#[tokio::test]
async fn no_task_starts_once_the_budget_is_exhausted() {
    let p = Probe::new();
    let spec = b()
        .add_task("a", &[], p.task("a", 1))
        .unwrap()
        .add_task("b", &["a"], p.task("b", 1))
        .unwrap()
        .build()
        .unwrap();
    let mut c = cfg(2);
    c.total_budget = Some(Duration::ZERO);
    let r = WorkflowExecutor::new(c).unwrap().run(spec).await;
    assert!(r.budget_exhausted);
    assert_eq!(r.status, WorkflowStatus::Cancelled);
    assert_eq!(p.runs("a") + p.runs("b"), 0, "nothing may start");
    assert!(r.unfinished.is_empty());
    gapless(&r);
}

#[tokio::test]
async fn dependents_do_not_start_when_the_budget_expires_as_a_task_finishes() {
    let p = Probe::new();
    // "first" outlives the 30 ms budget, so its dependent must never start
    // even though "first" itself completes normally.
    let spec = b()
        .add_task(
            "first",
            &[],
            runner_fn(|_c| {
                Box::pin(async {
                    // Blocks the thread past the deadline, then returns
                    // without yielding to the timer.
                    std::thread::sleep(Duration::from_millis(80));
                    TaskOutcome::Completed(serde_json::json!(1))
                })
            }),
        )
        .unwrap()
        .add_task("second", &["first"], p.task("second", 1))
        .unwrap()
        .build()
        .unwrap();
    let mut c = cfg(1);
    c.total_budget = Some(Duration::from_millis(30));
    let r = WorkflowExecutor::new(c).unwrap().run(spec).await;
    assert_eq!(p.runs("second"), 0, "no task starts after the budget");
    assert!(r.budget_exhausted);
    assert!(r.unfinished.is_empty());
    gapless(&r);
}

#[tokio::test]
async fn panicking_task_is_failed_uncertain_and_releases_its_permit() {
    let p = Probe::new();
    let spec = b()
        .add_task(
            "boom",
            &[],
            runner_fn(|_c| {
                Box::pin(async {
                    if true {
                        panic!("task body panic (expected in this test)");
                    }
                    TaskOutcome::Cancelled
                })
            }),
        )
        .unwrap()
        .add_task("dep", &["boom"], p.task("dep", 1))
        .unwrap()
        .add_task("free", &[], p.task("free", 1))
        .unwrap()
        .build()
        .unwrap();
    // One permit: "free" can only run if the panicking task released it.
    let r = WorkflowExecutor::new(cfg(1)).unwrap().run(spec).await;
    assert!(matches!(
        r.snapshot.states[&tid("boom")],
        TaskState::Failed(_)
    ));
    let rec = &r.failures[&tid("boom")];
    assert_eq!(rec.class, FailureClass::Failed);
    assert_eq!(rec.effect_certainty, EffectCertainty::Uncertain);
    assert_eq!(
        r.snapshot.states[&tid("dep")],
        TaskState::Skipped {
            prerequisite: tid("boom")
        }
    );
    assert_eq!(p.runs("dep"), 0);
    assert_eq!(p.runs("free"), 1, "permit was released");
    assert_eq!(r.snapshot.states[&tid("free")], TaskState::Completed);
    assert_eq!(r.status, WorkflowStatus::Failed);
    assert!(r.unfinished.is_empty());
    assert_eq!(r.events.last().unwrap().state, "finished");
    gapless(&r);
}

#[tokio::test]
async fn runner_error_text_is_truncated_before_it_is_recorded() {
    let long = "x".repeat(5000);
    let spec = b()
        .add_task(
            "noisy",
            &[],
            runner_fn(move |_c| {
                let long = long.clone();
                Box::pin(async move {
                    TaskOutcome::Failed(ToolFault::new(
                        FailureClass::Failed,
                        EffectCertainty::Uncertain,
                        long,
                    ))
                })
            }),
        )
        .unwrap()
        .build()
        .unwrap();
    let r = WorkflowExecutor::new(cfg(1)).unwrap().run(spec).await;
    let msg = &r.failures[&tid("noisy")].cause[0].message;
    assert!(msg.len() <= aegis::failure::MAX_CAUSE_MESSAGE_BYTES);
}

// ---- G2 (#55): "not known whether it happened" is its own state ----

/// A body that never finishes on its own, with an explicit interrupt certainty.
fn hang(cert: EffectCertainty) -> Arc<dyn TaskRunner> {
    runner_fn_with_certainty(
        |_c| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                TaskOutcome::Completed(serde_json::json!(null))
            })
        },
        cert,
    )
}

#[tokio::test]
async fn g2_timeout_on_uncertain_runner_is_uncertain_and_blocks_dependents() {
    let p = Probe::new();
    let spec = b()
        .add_task("writer", &[], hang(EffectCertainty::Uncertain))
        .unwrap()
        .add_task("after", &["writer"], p.task("after", 1))
        .unwrap()
        .add_task("after2", &["after"], p.task("after2", 1))
        .unwrap()
        .add_task("free", &[], p.task("free", 1))
        .unwrap()
        .build()
        .unwrap();
    let mut c = cfg(2);
    c.per_task_timeout = Some(Duration::from_millis(40));
    let r = WorkflowExecutor::new(c).unwrap().run(spec).await;
    match &r.snapshot.states[&tid("writer")] {
        TaskState::Uncertain(f) => {
            assert_eq!(f.class, FailureClass::Timeout);
            assert_eq!(f.effect_certainty, EffectCertainty::Uncertain);
        }
        other => panic!("writer: {other:?}"),
    }
    assert!(!r.snapshot.states[&tid("writer")].is_terminal());
    // Dependents are neither run nor skipped: they wait behind the unknown.
    for d in ["after", "after2"] {
        assert_eq!(r.snapshot.states[&tid(d)], TaskState::Pending, "{d}");
        assert_eq!(p.runs(d), 0, "{d} ran");
        assert_eq!(r.blocked_behind_uncertain[&tid(d)], tid("writer"), "{d}");
    }
    assert_eq!(r.snapshot.states[&tid("free")], TaskState::Completed);
    assert!(!r.blocked_behind_uncertain.contains_key(&tid("free")));
    for t in ["writer", "after", "after2"] {
        assert!(r.unfinished.contains(&tid(t)), "{t} not listed unfinished");
    }
    assert!(r.failures.contains_key(&tid("writer")));
    assert!(r.events.iter().any(|e| e.state == "uncertain"));
    gapless(&r);
}

#[tokio::test]
async fn g2_timeout_on_no_effect_runner_is_still_failed() {
    let spec = b()
        .add_task("read_only", &[], hang(EffectCertainty::NoEffect))
        .unwrap()
        .add_task("after", &["read_only"], ok(1))
        .unwrap()
        .build()
        .unwrap();
    let mut c = cfg(1);
    c.per_task_timeout = Some(Duration::from_millis(40));
    let r = WorkflowExecutor::new(c).unwrap().run(spec).await;
    match &r.snapshot.states[&tid("read_only")] {
        TaskState::Failed(f) => {
            assert_eq!(f.class, FailureClass::Timeout);
            assert_eq!(f.effect_certainty, EffectCertainty::NoEffect);
        }
        other => panic!("read_only: {other:?}"),
    }
    assert_eq!(
        r.snapshot.states[&tid("after")],
        TaskState::Skipped {
            prerequisite: tid("read_only")
        }
    );
    assert!(r.blocked_behind_uncertain.is_empty());
}

#[tokio::test]
async fn g2_cancel_interrupting_uncertain_runner_is_uncertain_not_cancelled() {
    let spec = b()
        .add_task("writer", &[], hang(EffectCertainty::Uncertain))
        .unwrap()
        .add_task("reader", &[], hang(EffectCertainty::NoEffect))
        .unwrap()
        .build()
        .unwrap();
    let cancel = CancelToken::new();
    let c2 = cancel.clone();
    let exec = WorkflowExecutor::new(cfg(2))
        .unwrap()
        .with_cancel_token(cancel);
    let h = tokio::spawn(async move { exec.run(spec).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    c2.cancel();
    let r = h.await.unwrap();
    match &r.snapshot.states[&tid("writer")] {
        TaskState::Uncertain(f) => {
            assert_eq!(f.class, FailureClass::Cancelled);
            assert_eq!(f.effect_certainty, EffectCertainty::Uncertain);
        }
        other => panic!("writer: {other:?}"),
    }
    assert_eq!(r.snapshot.states[&tid("reader")], TaskState::Cancelled);
}

#[tokio::test]
async fn g2_uncertain_state_round_trips_in_the_snapshot() {
    let spec = b()
        .add_task("writer", &[], hang(EffectCertainty::Uncertain))
        .unwrap()
        .build()
        .unwrap();
    let mut c = cfg(1);
    c.per_task_timeout = Some(Duration::from_millis(20));
    let r = WorkflowExecutor::new(c).unwrap().run(spec).await;
    let j = serde_json::to_string(&r.snapshot).unwrap();
    assert!(j.contains("\"state\":\"uncertain\""), "{j}");
    let back: aegis::orchestration::workflow::WorkflowSnapshot = serde_json::from_str(&j).unwrap();
    assert_eq!(back, r.snapshot);
}
