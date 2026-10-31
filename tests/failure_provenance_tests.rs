//! Failure provenance and effect-safe retry tests (issue #38).
//! Deterministic: scripted model turns, scripted tool dispatcher, no network.

use aegis::agent::FailurePolicy;
use aegis::{
    root_cause, AegisEvent, AgentEngine, CancelToken, ChatStream, ChatTurnResponse,
    EffectCertainty, EventBus, FailureClass, FailureRecord, InferenceEngine, RetryPolicy,
    SkillRegistry, ToolCallFunction, ToolCallItem, ToolDispatcher, ToolFault,
};
use async_trait::async_trait;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const SECRET: &str = "sk-live-SEEDED-SECRET-9f3a77";

struct ScriptedModel {
    turns: Mutex<VecDeque<ChatTurnResponse>>,
    asked: AtomicUsize,
    seen: Mutex<Vec<String>>,
}

impl ScriptedModel {
    fn new(turns: Vec<ChatTurnResponse>) -> Arc<Self> {
        Arc::new(Self {
            turns: Mutex::new(turns.into()),
            asked: AtomicUsize::new(0),
            seen: Mutex::new(Vec::new()),
        })
    }
}

fn calls(items: &[(&str, &str, serde_json::Value)]) -> ChatTurnResponse {
    ChatTurnResponse {
        content: None,
        reasoning: None,
        tool_calls: Some(
            items
                .iter()
                .map(|(id, name, args)| ToolCallItem {
                    id: id.to_string(),
                    call_type: "function".to_string(),
                    function: ToolCallFunction {
                        name: name.to_string(),
                        arguments: args.to_string(),
                    },
                })
                .collect(),
        ),
        finish_reason: Some("tool_calls".to_string()),
    }
}

fn done() -> ChatTurnResponse {
    ChatTurnResponse {
        content: Some("finished".to_string()),
        reasoning: None,
        tool_calls: None,
        finish_reason: Some("stop".to_string()),
    }
}

#[async_trait]
impl InferenceEngine for ScriptedModel {
    fn model_id(&self) -> String {
        "scripted".into()
    }
    fn endpoint(&self) -> String {
        "none".into()
    }
    async fn generate_with_tokens(
        &self,
        _p: &str,
        _s: Option<&str>,
        _t: Option<f32>,
        _m: Option<u32>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        Ok(String::new())
    }
    async fn generate_chat(
        &self,
        _m: &[serde_json::Value],
        _t: Option<f32>,
        _x: Option<u32>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        Ok(String::new())
    }
    async fn generate_chat_with_tools(
        &self,
        m: &[serde_json::Value],
        _tools: Option<&[serde_json::Value]>,
        _t: Option<f32>,
        _x: Option<u32>,
    ) -> Result<ChatTurnResponse, Box<dyn std::error::Error + Send + Sync>> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        self.seen
            .lock()
            .unwrap()
            .push(serde_json::to_string(m).unwrap());
        Ok(self.turns.lock().unwrap().pop_front().unwrap_or_else(done))
    }
    async fn stream_chat(
        &self,
        _m: &[serde_json::Value],
        _t: Option<f32>,
        _x: Option<u32>,
    ) -> Result<ChatStream, Box<dyn std::error::Error + Send + Sync>> {
        Err("not used".into())
    }
    async fn check_health(&self) -> bool {
        true
    }
}

#[derive(Clone)]
enum Behavior {
    Ok,
    Fault(ToolFault),
    Hang,
}

/// Scripted tool path: per tool name, a queue of behaviours; empty queue = ok.
struct Script {
    queues: Mutex<HashMap<String, VecDeque<Behavior>>>,
    dispatches: AtomicUsize,
}

impl Script {
    fn new(entries: Vec<(&str, Vec<Behavior>)>) -> Arc<Self> {
        Arc::new(Self {
            queues: Mutex::new(
                entries
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.into()))
                    .collect(),
            ),
            dispatches: AtomicUsize::new(0),
        })
    }
    fn count(&self) -> usize {
        self.dispatches.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ToolDispatcher for Script {
    async fn dispatch(
        &self,
        skill_name: &str,
        _arguments: &serde_json::Value,
    ) -> Result<String, ToolFault> {
        self.dispatches.fetch_add(1, Ordering::SeqCst);
        let next = self
            .queues
            .lock()
            .unwrap()
            .get_mut(skill_name)
            .and_then(|q| q.pop_front());
        match next {
            None | Some(Behavior::Ok) => Ok("ok".to_string()),
            Some(Behavior::Fault(f)) => Err(f),
            Some(Behavior::Hang) => {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok("late".to_string())
            }
        }
    }
}

fn engine(model: Arc<ScriptedModel>, script: Arc<Script>) -> AgentEngine {
    AgentEngine::new(model, Arc::new(SkillRegistry::new()), None)
        .with_dispatcher(script)
        .with_run_id("run_test")
}

fn path() -> serde_json::Value {
    serde_json::json!({"path": "a.txt"})
}

fn retry(n: u32) -> FailurePolicy {
    FailurePolicy {
        retry: RetryPolicy { max_attempts: n },
        tool_timeout: None,
    }
}

fn all_failures(res: &aegis::AgentExecutionResult) -> Vec<FailureRecord> {
    res.steps
        .iter()
        .flat_map(|s| s.tool_calls.iter())
        .flat_map(|t| t.retry_failures.iter().chain(t.failure.iter()))
        .cloned()
        .collect()
}

#[tokio::test]
async fn failing_tool_identifies_step_cause_and_flows_through_event_bus() {
    let model = ScriptedModel::new(vec![calls(&[("c1", "read_file", path())]), done()]);
    let script = Script::new(vec![(
        "read_file",
        vec![Behavior::Fault(ToolFault::new(
            FailureClass::Failed,
            EffectCertainty::NoEffect,
            "disk read error",
        ))],
    )]);
    let bus = EventBus::default();
    let mut rx = bus.subscribe();
    let res = engine(model, script)
        .with_event_bus(bus, Some("sess_1".into()))
        .execute_task("goal", Some("sys"), 5)
        .await
        .unwrap();

    let rec = &res.steps[0].tool_calls[0];
    assert!(!rec.success);
    assert_eq!(rec.output, "disk read error");
    let f = rec.failure.as_ref().unwrap();
    assert_eq!(f.run_id, "run_test");
    assert_eq!(f.step_index, 1);
    assert_eq!(f.tool_call_id, "c1");
    assert_eq!(f.class, FailureClass::Failed);
    assert_eq!(f.cause[0].message, "disk read error");
    assert!(f.failed_at >= f.created_at);
    assert!(!res.completed);

    let env = rx.try_recv().unwrap();
    assert_eq!(env.sequence, 1);
    match env.event {
        AegisEvent::StepFailed { failure } => assert_eq!(&failure, f),
        other => panic!("unexpected event {:?}", other),
    }
}

#[tokio::test]
async fn chained_dependent_failure_resolves_to_original() {
    let model = ScriptedModel::new(vec![
        calls(&[("a", "read_file", path()), ("b", "list_dir", path())]),
        calls(&[("c", "git_status", path())]),
        done(),
    ]);
    let fail = |m: &str| {
        Behavior::Fault(ToolFault::new(
            FailureClass::Failed,
            EffectCertainty::NoEffect,
            m,
        ))
    };
    let script = Script::new(vec![
        ("read_file", vec![fail("root failure")]),
        ("list_dir", vec![fail("downstream one")]),
        ("git_status", vec![fail("downstream two")]),
    ]);
    let res = engine(model, script)
        .execute_task("goal", Some("sys"), 5)
        .await
        .unwrap();
    let all = all_failures(&res);
    assert_eq!(all.len(), 3);
    let seqs: Vec<u64> = all.iter().map(|f| f.sequence).collect();
    assert_eq!(seqs, vec![1, 2, 3]);
    assert_eq!(all[0].parent_call_id, None);
    assert_eq!(all[1].parent_call_id.as_deref(), Some("a"));
    assert_eq!(all[2].parent_call_id.as_deref(), Some("b"));
    let root = root_cause(&all, &all[2]).unwrap();
    assert_eq!(root.tool_call_id, "a");
    assert_eq!(root.step_index, 1);
    assert_eq!(root.cause[0].message, "root failure");
}

#[tokio::test]
async fn success_between_failures_breaks_the_chain() {
    let model = ScriptedModel::new(vec![
        calls(&[
            ("a", "read_file", path()),
            ("b", "list_dir", path()),
            ("c", "git_status", path()),
        ]),
        done(),
    ]);
    let fail = Behavior::Fault(ToolFault::new(
        FailureClass::Failed,
        EffectCertainty::NoEffect,
        "x",
    ));
    let script = Script::new(vec![
        ("read_file", vec![fail.clone()]),
        ("git_status", vec![fail]),
    ]);
    let res = engine(model, script)
        .execute_task("goal", Some("sys"), 5)
        .await
        .unwrap();
    let all = all_failures(&res);
    assert_eq!(all.len(), 2);
    assert_eq!(all[1].tool_call_id, "c");
    assert_eq!(all[1].parent_call_id, None);
}

#[tokio::test]
async fn cancellation_in_flight_write_is_uncertain_and_halts_the_run() {
    let model = ScriptedModel::new(vec![
        calls(&[("w", "write_file", path()), ("never", "read_file", path())]),
        done(),
    ]);
    let script = Script::new(vec![("write_file", vec![Behavior::Hang])]);
    let token = CancelToken::new();
    let t2 = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(40)).await;
        t2.cancel();
    });
    let res = engine(model.clone(), script.clone())
        .with_cancel_token(token)
        .execute_task("goal", Some("sys"), 5)
        .await
        .unwrap();
    let f = res.steps[0].tool_calls[0].failure.as_ref().unwrap();
    assert_eq!(f.class, FailureClass::Cancelled);
    assert_eq!(f.effect_certainty, EffectCertainty::Uncertain);
    assert!(!res.completed);
    assert_eq!(res.steps[0].tool_calls.len(), 1, "later call must not run");
    assert_eq!(script.count(), 1);
    assert_eq!(model.asked.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancellation_of_read_is_no_effect_and_before_dispatch_never_dispatches() {
    let script = Script::new(vec![("read_file", vec![Behavior::Hang])]);
    let token = CancelToken::new();
    let t2 = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(40)).await;
        t2.cancel();
    });
    let res = engine(
        ScriptedModel::new(vec![calls(&[("r", "read_file", path())])]),
        script.clone(),
    )
    .with_cancel_token(token)
    .execute_task("goal", Some("sys"), 5)
    .await
    .unwrap();
    let f = res.steps[0].tool_calls[0].failure.as_ref().unwrap();
    assert_eq!(f.class, FailureClass::Cancelled);
    assert_eq!(f.effect_certainty, EffectCertainty::NoEffect);

    let pre = CancelToken::new();
    pre.cancel();
    let script2 = Script::new(vec![]);
    let res2 = engine(
        ScriptedModel::new(vec![calls(&[("w", "write_file", path())])]),
        script2.clone(),
    )
    .with_cancel_token(pre)
    .execute_task("goal", Some("sys"), 5)
    .await
    .unwrap();
    let f2 = res2.steps[0].tool_calls[0].failure.as_ref().unwrap();
    assert_eq!(f2.class, FailureClass::Cancelled);
    assert_eq!(f2.effect_certainty, EffectCertainty::NoEffect);
    assert_eq!(script2.count(), 0);
}

#[tokio::test]
async fn timeout_certainty_follows_skill_contract_and_is_never_retried() {
    let policy = FailurePolicy {
        retry: RetryPolicy { max_attempts: 4 },
        tool_timeout: Some(Duration::from_millis(30)),
    };
    let script = Script::new(vec![
        ("write_file", vec![Behavior::Hang, Behavior::Hang]),
        ("read_file", vec![Behavior::Hang, Behavior::Hang]),
    ]);
    let res = engine(
        ScriptedModel::new(vec![calls(&[
            ("w", "write_file", path()),
            ("r", "read_file", path()),
        ])]),
        script.clone(),
    )
    .with_failure_policy(policy)
    .execute_task("goal", Some("sys"), 3)
    .await
    .unwrap();
    let w = res.steps[0].tool_calls[0].failure.as_ref().unwrap();
    let r = res.steps[0].tool_calls[1].failure.as_ref().unwrap();
    assert_eq!(w.class, FailureClass::Timeout);
    assert_eq!(w.effect_certainty, EffectCertainty::Uncertain);
    assert_eq!(r.class, FailureClass::Timeout);
    assert_eq!(r.effect_certainty, EffectCertainty::NoEffect);
    assert_eq!(script.count(), 2, "one dispatch each, no timeout retry");
}

#[tokio::test]
async fn unavailable_backend_is_unavailable_with_no_effect() {
    let script = Script::new(vec![(
        "cortex_recall",
        vec![Behavior::Fault(ToolFault::unavailable(
            "Unavailable: backend not reachable",
        ))],
    )]);
    let res = engine(
        ScriptedModel::new(vec![calls(&[("u", "cortex_recall", path())]), done()]),
        script,
    )
    .execute_task("goal", Some("sys"), 3)
    .await
    .unwrap();
    let f = res.steps[0].tool_calls[0].failure.as_ref().unwrap();
    assert_eq!(f.class, FailureClass::Unavailable);
    assert_eq!(f.effect_certainty, EffectCertainty::NoEffect);
}

#[tokio::test]
async fn real_registry_paths_classify_refusal_and_keep_old_output_text() {
    let model = ScriptedModel::new(vec![
        calls(&[("x", "nonexistent_custom_skill", serde_json::json!({}))]),
        done(),
    ]);
    let agent = AgentEngine::new(model, Arc::new(SkillRegistry::new()), None).with_run_id("run_r");
    let res = agent.execute_task("goal", Some("sys"), 3).await.unwrap();
    let rec = &res.steps[0].tool_calls[0];
    assert!(rec.output.contains("not on the dispatch allowlist"));
    let f = rec.failure.as_ref().unwrap();
    assert_eq!(f.class, FailureClass::Rejected);
    assert_eq!(f.effect_certainty, EffectCertainty::NoEffect);

    // Registered but unconnected handler reports Unavailable. The probe gate
    // may refuse first depending on its deterministic verdict, which is also
    // an acceptable no-effect outcome; both must be NoEffect and not Failed.
    let model2 = ScriptedModel::new(vec![
        calls(&[("y", "cortex_recall", serde_json::json!({"query": "q"}))]),
        done(),
    ]);
    let agent2 = AgentEngine::new(model2, Arc::new(SkillRegistry::new()), None);
    let res2 = agent2.execute_task("goal", Some("sys"), 3).await.unwrap();
    let f2 = res2.steps[0].tool_calls[0].failure.as_ref().unwrap();
    assert!(matches!(
        f2.class,
        FailureClass::Unavailable | FailureClass::Rejected
    ));
    assert_eq!(f2.effect_certainty, EffectCertainty::NoEffect);
}

#[tokio::test]
async fn idempotent_read_retries_with_bounded_attempts_and_provenance() {
    let un = || Behavior::Fault(ToolFault::unavailable("Unavailable: warming up"));
    // Succeeds on third attempt.
    let script = Script::new(vec![("read_file", vec![un(), un(), Behavior::Ok])]);
    let res = engine(
        ScriptedModel::new(vec![calls(&[("r", "read_file", path())]), done()]),
        script.clone(),
    )
    .with_failure_policy(retry(3))
    .execute_task("goal", Some("sys"), 3)
    .await
    .unwrap();
    let rec = &res.steps[0].tool_calls[0];
    assert!(rec.success);
    assert!(rec.failure.is_none());
    assert_eq!(script.count(), 3, "each attempt is a new dispatch");
    let attempts: Vec<u32> = rec.retry_failures.iter().map(|f| f.attempt).collect();
    assert_eq!(attempts, vec![1, 2]);
    assert!(rec.retry_failures[0].sequence < rec.retry_failures[1].sequence);

    // Budget exhausted at the policy bound, not beyond it.
    let script2 = Script::new(vec![("read_file", vec![un(), un(), un(), un(), un()])]);
    let res2 = engine(
        ScriptedModel::new(vec![calls(&[("r", "read_file", path())]), done()]),
        script2.clone(),
    )
    .with_failure_policy(retry(2))
    .execute_task("goal", Some("sys"), 3)
    .await
    .unwrap();
    let rec2 = &res2.steps[0].tool_calls[0];
    assert!(!rec2.success);
    assert_eq!(script2.count(), 2);
    assert_eq!(rec2.failure.as_ref().unwrap().attempt, 2);
    assert_eq!(rec2.retry_failures.len(), 1);
}

#[tokio::test]
async fn default_policy_does_not_retry() {
    let script = Script::new(vec![(
        "read_file",
        vec![Behavior::Fault(ToolFault::unavailable("Unavailable: x"))],
    )]);
    let res = engine(
        ScriptedModel::new(vec![calls(&[("r", "read_file", path())]), done()]),
        script.clone(),
    )
    .execute_task("goal", Some("sys"), 3)
    .await
    .unwrap();
    assert_eq!(script.count(), 1);
    assert!(!res.steps[0].tool_calls[0].success);
}

#[tokio::test]
async fn uncertain_or_mutating_failures_are_never_retried() {
    let cases: Vec<(&str, ToolFault)> = vec![
        (
            "write_file",
            ToolFault::new(FailureClass::Uncertain, EffectCertainty::Uncertain, "lost"),
        ),
        // Even a no-effect refusal is not retried for a non idempotent write.
        ("write_file", ToolFault::rejected("refused")),
        (
            "read_file",
            ToolFault::rejected("not on the dispatch allowlist"),
        ),
        (
            "read_file",
            ToolFault::new(FailureClass::Uncertain, EffectCertainty::Uncertain, "lost"),
        ),
        (
            "read_file",
            ToolFault::new(FailureClass::Failed, EffectCertainty::NoEffect, "boom"),
        ),
        (
            "read_file",
            ToolFault::new(
                FailureClass::Malformed,
                EffectCertainty::NoEffect,
                "bad shape",
            ),
        ),
        (
            "read_file",
            ToolFault::new(
                FailureClass::Unavailable,
                EffectCertainty::Uncertain,
                "maybe ran",
            ),
        ),
    ];
    for (tool, fault) in cases {
        let script = Script::new(vec![(tool, vec![Behavior::Fault(fault.clone())])]);
        let res = engine(
            ScriptedModel::new(vec![calls(&[("c", tool, path())]), done()]),
            script.clone(),
        )
        .with_failure_policy(retry(5))
        .execute_task("goal", Some("sys"), 3)
        .await
        .unwrap();
        assert_eq!(script.count(), 1, "{} {:?} was replayed", tool, fault);
        assert!(!res.steps[0].tool_calls[0].success);
    }
}

#[tokio::test]
async fn approval_pending_is_never_auto_continued() {
    let script = Script::new(vec![(
        "read_file",
        vec![Behavior::Fault(ToolFault::new(
            FailureClass::ApprovalPending,
            EffectCertainty::NoEffect,
            "needs a human",
        ))],
    )]);
    let model = ScriptedModel::new(vec![
        calls(&[("p", "read_file", path()), ("q", "list_dir", path())]),
        done(),
    ]);
    let res = engine(model.clone(), script.clone())
        .with_failure_policy(retry(5))
        .execute_task("goal", Some("sys"), 5)
        .await
        .unwrap();
    assert_eq!(script.count(), 1, "no replay and no following call");
    assert_eq!(model.asked.load(Ordering::SeqCst), 1, "model not re-asked");
    assert!(!res.completed);
    assert!(res.final_response.contains("waiting for approval"));
    assert_eq!(
        res.steps[0].tool_calls[0].failure.as_ref().unwrap().class,
        FailureClass::ApprovalPending
    );
}

#[tokio::test]
async fn seeded_secret_never_appears_in_failure_output() {
    let args = serde_json::json!({"path": "a.txt", "api_key": SECRET});
    let script = Script::new(vec![(
        "write_file",
        vec![Behavior::Fault(ToolFault::new(
            FailureClass::Failed,
            EffectCertainty::EffectOccurred,
            format!("handler rejected value {} at offset 3", SECRET),
        ))],
    )]);
    let bus = EventBus::default();
    let mut rx = bus.subscribe();
    let model = ScriptedModel::new(vec![calls(&[("w", "write_file", args)]), done()]);
    let res = engine(model.clone(), script)
        .with_event_bus(bus, None)
        .execute_task("goal", Some("sys"), 3)
        .await
        .unwrap();
    let f = res.steps[0].tool_calls[0].failure.as_ref().unwrap();
    let ser = serde_json::to_string(f).unwrap();
    let dbg = format!("{:?}", f);
    assert!(!ser.contains(SECRET), "serialized: {}", ser);
    assert!(!dbg.contains(SECRET), "debug: {}", dbg);
    assert!(ser.contains("sha256:"));
    // The tool message sent back to the model and the record output are redacted too.
    assert!(!res.steps[0].tool_calls[0].output.contains(SECRET));
    assert!(res.steps[0].tool_calls[0].output.contains("[redacted]"));
    assert!(model.seen.lock().unwrap().len() >= 2);
    // The model's own earlier tool call (its arguments) is history it wrote; the
    // tool result message that we send back must be redacted.
    let mut tool_msgs = 0;
    for seen in model.seen.lock().unwrap().iter() {
        let msgs: Vec<serde_json::Value> = serde_json::from_str(seen).unwrap();
        for m in msgs.iter().filter(|m| m["role"] == "tool") {
            tool_msgs += 1;
            assert!(!m["content"].as_str().unwrap().contains(SECRET));
        }
    }
    assert!(tool_msgs >= 1);
    let ev = rx.try_recv().unwrap();
    assert!(!serde_json::to_string(&ev).unwrap().contains(SECRET));
    assert!(!format!("{:?}", ev).contains(SECRET));
}

#[tokio::test]
async fn cause_message_is_bounded_in_a_real_run() {
    let long = "z".repeat(5000);
    let script = Script::new(vec![(
        "read_file",
        vec![Behavior::Fault(ToolFault::new(
            FailureClass::Failed,
            EffectCertainty::NoEffect,
            long,
        ))],
    )]);
    let res = engine(
        ScriptedModel::new(vec![calls(&[("r", "read_file", path())]), done()]),
        script,
    )
    .execute_task("goal", Some("sys"), 3)
    .await
    .unwrap();
    let f = res.steps[0].tool_calls[0].failure.as_ref().unwrap();
    assert!(f.cause.len() <= aegis::failure::MAX_CAUSE_ENTRIES);
    assert!(f.cause[0].message.len() <= aegis::failure::MAX_CAUSE_MESSAGE_BYTES);
}
