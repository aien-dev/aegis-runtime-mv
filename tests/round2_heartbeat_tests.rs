//! Task interruption, terminal transaction and projection-failure tests, all on private SQLite files.
use aegis::{Database, HeartbeatEngine, InferenceEngine, ChatStream, ChatTurnResponse};
use aegis::persistence::TaskClaim;
use rusqlite::Connection;
use serde_json::Value;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::time::Duration;

fn fixture() -> (tempfile::TempDir, Arc<Database>, Connection) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.sqlite");
    let db = Arc::new(Database::open(&path).unwrap());
    let observer = Connection::open(&path).unwrap();
    (dir, db, observer)
}
fn claim(db: &Database, id: &str) -> TaskClaim { db.claim_task(id).unwrap().unwrap() }
fn state(observer: &Connection, id: &str) -> (String, Option<String>) {
    observer.query_row("SELECT status,result FROM tasks WHERE id=?1", [id], |row| Ok((row.get(0)?,row.get(1)?))).unwrap()
}
fn event_count(observer: &Connection) -> i64 {
    observer.query_row("SELECT count(*) FROM task_terminal_events", [], |row| row.get(0)).unwrap()
}
fn exported_events(dir: &tempfile::TempDir) -> Vec<Value> {
    std::fs::read_to_string(dir.path().join("transcript.jsonl")).unwrap().lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|event| matches!(event["event_type"].as_str(), Some("task_completed" | "task_failed"))).collect()
}

#[tokio::test]
async fn round2_interrupted_claim_reopen_abandons_once_without_replay() {
    let (dir, db, observer) = fixture();
    db.create_task("interrupted", "consolidate_memory", "").unwrap();
    let _claim = claim(&db, "interrupted");
    // An independently opened live owner must not be stolen.
    let live = Database::open(&dir.path().join("tasks.sqlite")).unwrap();
    assert_eq!(state(&observer,"interrupted").0, "running");
    drop(live);
    drop(db);
    let reopened = Arc::new(Database::open(&dir.path().join("tasks.sqlite")).unwrap());
    assert_eq!(state(&observer,"interrupted"), ("failed".into(),Some("abandoned: outcome unknown".into())));
    let receipt = HeartbeatEngine::new(60, reopened.clone()).pulse_once().await;
    assert_eq!(receipt.actions_dispatched, 0);
    assert_eq!(event_count(&observer), 1);
    let events = exported_events(&dir);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["event_type"], "task_failed");
    drop(reopened);
    let _reopened = Database::open(&dir.path().join("tasks.sqlite")).unwrap();
    assert_eq!(event_count(&observer), 1);
    assert_eq!(exported_events(&dir).len(), 1);
}

#[test]
fn round2_ownerless_legacy_claim_requires_quiescence() {
    let (dir, db, observer) = fixture();
    db.create_task("legacy", "system_telemetry", "").unwrap();
    observer.execute("UPDATE tasks SET status='running' WHERE id='legacy'", []).unwrap();
    drop(db);
    let result = Database::open(&dir.path().join("tasks.sqlite"));
    assert!(result.is_err());
    assert!(result.err().unwrap().to_string().contains("quiescent migration"));
}

#[test]
fn round2_claim_identity_and_duplicate_finalization_are_refused() {
    let (_dir, db, observer) = fixture();
    db.create_task("once", "system_telemetry", "").unwrap();
    let valid = claim(&db,"once");
    let mut wrong = valid.clone();
    wrong.token = "wrong-token".into();
    assert!(db.complete_task(&wrong,"wrong").is_err());
    assert_eq!(state(&observer,"once").0, "running");
    assert_eq!(event_count(&observer), 0);
    wrong = valid.clone(); wrong.owner = "wrong-owner".into();
    assert!(db.fail_task(&wrong,"wrong").is_err());
    wrong = valid.clone(); wrong.id = "missing-task".into();
    assert!(db.complete_task(&wrong,"missing").is_err());
    assert_eq!(event_count(&observer), 0);
    db.complete_task(&valid,"finished").unwrap();
    assert!(db.complete_task(&valid,"again").is_err());
    assert!(db.fail_task(&valid,"conflicting").is_err());
    assert_eq!(state(&observer,"once"), ("completed".into(),Some("finished".into())));
    assert_eq!(event_count(&observer), 1);
    assert!(observer.execute("INSERT INTO task_terminal_events(event_id,task_id,claim_id,event_json) VALUES('duplicate','once','other','{}')", []).is_err());
    db.create_task("failed", "unknown", "").unwrap();
    let failed = claim(&db,"failed");
    db.fail_task(&failed,"original failure").unwrap();
    assert!(db.fail_task(&failed,"again").is_err());
    assert!(db.complete_task(&failed,"false completion").is_err());
    assert_eq!(event_count(&observer), 2);
}

async fn finalization_retry(successful: bool) {
    let (_dir, db, observer) = fixture();
    db.create_task("retry", if successful {"consolidate_memory"} else {"unknown"}, "").unwrap();
    observer.execute_batch("CREATE TRIGGER reject_terminal BEFORE INSERT ON task_terminal_events BEGIN SELECT RAISE(FAIL,'injected terminal event failure'); END;").unwrap();
    let engine = HeartbeatEngine::new(60, db.clone());
    let first = engine.pulse_once().await;
    assert_eq!(first.status, "degraded");
    assert_eq!(first.actions_dispatched, 1);
    assert_eq!(state(&observer,"retry").0, "running", "terminal state must roll back with its event");
    assert_eq!(event_count(&observer), 0);
    observer.execute_batch("DROP TRIGGER reject_terminal").unwrap();
    // Retained outcomes belong to the DB owner, not a replaceable heartbeat engine.
    drop(engine);
    let engine = HeartbeatEngine::new(60, db);
    let second = engine.pulse_once().await;
    assert_eq!(second.actions_dispatched, 0);
    assert_eq!(state(&observer,"retry").0, if successful {"completed"} else {"failed"});
    assert!(state(&observer,"retry").1.unwrap().contains(if successful {"Memory crumbs recorded"} else {"Unknown task type"}));
    assert_eq!(event_count(&observer), 1);
    let crumbs: i64 = observer.query_row("SELECT count(*) FROM memory_crumbs", [], |row| row.get(0)).unwrap();
    assert_eq!(crumbs, if successful {1} else {0});
}
#[tokio::test]
async fn round2_successful_outcome_retained_after_failed_finalization() { finalization_retry(true).await; }
#[tokio::test]
async fn round2_failed_outcome_retained_after_failed_finalization() { finalization_retry(false).await; }

#[tokio::test]
async fn round2_unwritable_transcript_keeps_one_durable_event_and_retries_export() {
    let (dir, db, observer) = fixture();
    db.create_task("export", "system_telemetry", "").unwrap();
    let path = dir.path().join("transcript.jsonl");
    let saved = dir.path().join("transcript.saved");
    std::fs::rename(&path,&saved).unwrap();
    std::fs::create_dir(&path).unwrap();
    let engine = HeartbeatEngine::new(60, db);
    let first = engine.pulse_once().await;
    assert_eq!(first.status,"degraded");
    assert_eq!(state(&observer,"export").0,"completed");
    assert_eq!(event_count(&observer),1);
    let (event_id, exported): (String,i64) = observer.query_row("SELECT event_id,exported FROM task_terminal_events",[],|row|Ok((row.get(0)?,row.get(1)?))).unwrap();
    assert_eq!(exported,0);
    std::fs::remove_dir(&path).unwrap();
    std::fs::rename(saved,&path).unwrap();
    // Simulate a torn trailing append; retry must not leave malformed JSONL.
    use std::io::Write;
    std::fs::OpenOptions::new().append(true).open(&path).unwrap().write_all(b"{\"interrupted").unwrap();
    let second = engine.pulse_once().await;
    assert_eq!(second.status,"nominal");
    assert_eq!(second.actions_dispatched,0);
    assert_eq!(event_count(&observer),1);
    let events = exported_events(&dir);
    assert_eq!(events.len(),1);
    assert_eq!(events[0]["event_id"],event_id);
    assert_eq!(events[0]["event_type"],"task_completed");
    observer.execute("UPDATE task_terminal_events SET exported=0",[]).unwrap();
    assert_eq!(engine.pulse_once().await.status,"nominal");
    assert_eq!(exported_events(&dir).len(),1, "append-before-ack retry must deduplicate by stable event identity");
}

#[tokio::test]
async fn round2_receipt_insert_failure_degrades_return_and_broadcast() {
    let (_dir, db, observer) = fixture();
    observer.execute_batch("CREATE TRIGGER reject_heartbeat BEFORE INSERT ON heartbeats BEGIN SELECT RAISE(FAIL,'injected heartbeat failure'); END;").unwrap();
    let engine = HeartbeatEngine::new(60, db);
    let mut rx = engine.subscribe();
    let receipt = engine.pulse_once().await;
    assert_eq!(receipt.status,"degraded");
    assert!(receipt.notes.contains("injected heartbeat failure"));
    assert_eq!(rx.recv().await.unwrap().status,"degraded");
    let count: i64 = observer.query_row("SELECT count(*) FROM heartbeats",[],|row|row.get(0)).unwrap();
    assert_eq!(count,0);
    observer.execute_batch("DROP TRIGGER reject_heartbeat").unwrap();
    assert_eq!(engine.pulse_once().await.status,"nominal");
}

struct HealthProbe { calls: AtomicUsize, hang: bool }
#[async_trait::async_trait]
impl InferenceEngine for HealthProbe {
    fn model_id(&self)->String { "test-health".into() }
    fn endpoint(&self)->String { "test-only".into() }
    async fn generate_with_tokens(&self,_:&str,_:Option<&str>,_:Option<f32>,_:Option<u32>)->Result<String,Box<dyn std::error::Error+Send+Sync>> { unreachable!() }
    async fn generate_chat(&self,_:&[Value],_:Option<f32>,_:Option<u32>)->Result<String,Box<dyn std::error::Error+Send+Sync>> { unreachable!() }
    async fn generate_chat_with_tools(&self,_:&[Value],_:Option<&[Value]>,_:Option<f32>,_:Option<u32>)->Result<ChatTurnResponse,Box<dyn std::error::Error+Send+Sync>> { unreachable!() }
    async fn stream_chat(&self,_:&[Value],_:Option<f32>,_:Option<u32>)->Result<ChatStream,Box<dyn std::error::Error+Send+Sync>> { unreachable!() }
    async fn check_health(&self)->bool {
        self.calls.fetch_add(1,Ordering::SeqCst);
        if self.hang { std::future::pending::<()>().await; }
        panic!("injected health action panic");
    }
}
#[tokio::test]
async fn round2_panicking_health_action_does_not_kill_loop() {
    let (_dir,db,observer)=fixture();
    db.create_task("panic","model_health","").unwrap();
    let health=Arc::new(HealthProbe{calls:AtomicUsize::new(0),hang:false});
    let engine=Arc::new(HeartbeatEngine::with_inference(1,db.clone(),health.clone()));
    let mut rx=engine.subscribe();
    engine.start_loop().await;
    let first=tokio::time::timeout(Duration::from_secs(5),rx.recv()).await.unwrap().unwrap();
    assert_eq!(first.status,"degraded");
    assert_eq!(state(&observer,"panic").0,"failed");
    assert!(state(&observer,"panic").1.unwrap().contains("panicked"));
    db.create_task("after-panic","system_telemetry","").unwrap();
    let next=tokio::time::timeout(Duration::from_secs(5),rx.recv()).await.unwrap().unwrap();
    assert_eq!(next.status,"nominal");
    assert_eq!(state(&observer,"after-panic").0,"completed");
    assert_eq!(health.calls.load(Ordering::SeqCst),1);
}
#[tokio::test]
async fn round2_cancelled_claim_retains_unknown_failure_without_reexecution() {
    let (_dir,db,observer)=fixture();
    db.create_task("cancel","model_health","").unwrap();
    let health=Arc::new(HealthProbe{calls:AtomicUsize::new(0),hang:true});
    let engine=Arc::new(HeartbeatEngine::with_inference(60,db.clone(),health.clone()));
    let running=tokio::spawn(async move { engine.pulse_once().await });
    tokio::time::timeout(Duration::from_secs(5),async { while health.calls.load(Ordering::SeqCst)==0 { tokio::task::yield_now().await; } }).await.unwrap();
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    let receipt=HeartbeatEngine::new(60,db).pulse_once().await;
    assert_eq!(receipt.actions_dispatched,0);
    assert_eq!(state(&observer,"cancel"),("failed".into(),Some("abandoned: outcome unknown".into())));
    assert_eq!(event_count(&observer),1);
    assert_eq!(health.calls.load(Ordering::SeqCst),1);
}
