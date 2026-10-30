//! Host-only regressions for maintenance recovery, explicit migration and bounded health work.
use aegis::{ChatStream, ChatTurnResponse, Database, HeartbeatEngine, InferenceEngine};
use rusqlite::Connection;
use serde_json::{json, Value};
use std::io::Read;
use std::process::{Child, Command, Stdio};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

fn state(conn: &Connection, id: &str) -> (String, Option<String>) {
    conn.query_row("SELECT status,result FROM tasks WHERE id=?1", [id], |r| {
        Ok((r.get(0)?, r.get(1)?))
    })
    .unwrap()
}
fn events(conn: &Connection) -> i64 {
    conn.query_row("SELECT count(*) FROM task_terminal_events", [], |r| {
        r.get(0)
    })
    .unwrap()
}
struct OwnerProcess(Child);
impl Drop for OwnerProcess {
    fn drop(&mut self) {
        if self.0.try_wait().unwrap().is_none() {
            self.0.kill().unwrap();
        }
        self.0.wait().unwrap();
    }
}

#[test]
fn round3_owner_process_helper() {
    let Some(path) = std::env::var_os("AEGIS_TEST_OWNER_DATABASE") else {
        return;
    };
    let db = Database::open(std::path::Path::new(&path)).unwrap();
    db.create_task("peer", "consolidate_memory", "must not replay")
        .unwrap();
    assert!(db.claim_task("peer").unwrap().is_some());
    std::fs::write(
        std::env::var_os("AEGIS_TEST_OWNER_READY").unwrap(),
        b"claimed",
    )
    .unwrap();
    let mut release = [0];
    let _ = std::io::stdin().read_exact(&mut release);
    drop(db);
}

#[tokio::test]
async fn round3_live_peer_death_recovers_without_reopening_observer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("peer.sqlite");
    let ready = dir.path().join("ready");
    let mut child = OwnerProcess(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "round3_owner_process_helper", "--nocapture"])
            .env("AEGIS_TEST_OWNER_DATABASE", &path)
            .env("AEGIS_TEST_OWNER_READY", &ready)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while !ready.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let db = Arc::new(Database::open(&path).unwrap());
    let observer = Connection::open(&path).unwrap();
    let engine = HeartbeatEngine::new(60, db);
    assert_eq!(engine.pulse_once().await.status, "nominal");
    assert_eq!(
        state(&observer, "peer").0,
        "running",
        "live process owns the flock"
    );
    assert_eq!(events(&observer), 0);
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let receipt = engine.pulse_once().await;
    assert_eq!(receipt.status, "degraded");
    assert_eq!(receipt.actions_dispatched, 0);
    assert_eq!(
        state(&observer, "peer"),
        ("failed".into(), Some("abandoned: outcome unknown".into()))
    );
    assert_eq!(events(&observer), 1);
    assert_eq!(engine.pulse_once().await.status, "nominal");
    assert_eq!(events(&observer), 1);
    let crumbs: i64 = observer
        .query_row("SELECT count(*) FROM memory_crumbs", [], |r| r.get(0))
        .unwrap();
    assert_eq!(crumbs, 0);
}

fn legacy_fixture(path: &std::path::Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch("CREATE TABLE tasks(id TEXT PRIMARY KEY,task_type TEXT NOT NULL,payload TEXT NOT NULL,status TEXT NOT NULL,result TEXT,created_at TEXT NOT NULL,completed_at TEXT);
        INSERT INTO tasks VALUES('legacy','consolidate_memory','preserve payload','running',NULL,'preserve timestamp',NULL);").unwrap();
    conn
}
fn migrate(path: &std::path::Path, quiescent: bool) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_aegis"));
    command
        .arg("migrate-legacy-tasks")
        .arg("--db-path")
        .arg(path);
    if quiescent {
        command.arg("--quiescent");
    }
    command.output().unwrap()
}

#[tokio::test]
async fn round3_explicit_legacy_migration_preserves_and_fails_unknown_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.sqlite");
    let observer = legacy_fixture(&path);
    assert!(
        Database::open(&path).is_err(),
        "normal startup must fail closed without migration"
    );
    let refused = migrate(&path, false);
    assert!(
        !refused.status.success(),
        "explicit quiescence acknowledgement is required"
    );
    assert_eq!(state(&observer, "legacy").0, "running");
    assert_eq!(events(&observer), 0);
    let migrated = migrate(&path, true);
    assert!(
        migrated.status.success(),
        "{}",
        String::from_utf8_lossy(&migrated.stderr)
    );
    let db = Arc::new(Database::open(&path).unwrap());
    assert_eq!(
        state(&observer, "legacy"),
        ("failed".into(), Some("abandoned: outcome unknown".into()))
    );
    let (payload, created, claim, owner): (String, String, String, String) = observer
        .query_row(
            "SELECT payload,created_at,claim_id,owner_id FROM tasks WHERE id='legacy'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
    assert_eq!(payload, "preserve payload");
    assert_eq!(created, "preserve timestamp");
    assert!(uuid::Uuid::parse_str(&claim).is_ok());
    assert!(uuid::Uuid::parse_str(&owner).is_ok());
    assert_eq!(events(&observer), 1);
    assert_eq!(
        HeartbeatEngine::new(60, db.clone())
            .pulse_once()
            .await
            .actions_dispatched,
        0
    );
    drop(db);
    assert!(migrate(&path, true).status.success());
    assert_eq!(events(&observer), 1);
}

#[test]
fn round3_legacy_migration_rolls_back_claim_metadata_and_terminal_event_together() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.sqlite");
    let observer = legacy_fixture(&path);
    assert!(Database::open(&path).is_err());
    observer.execute_batch("CREATE TRIGGER reject_migration_event BEFORE INSERT ON task_terminal_events BEGIN SELECT RAISE(FAIL,'injected migration event failure'); END;").unwrap();
    let result = migrate(&path, true);
    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("injected migration event failure"));
    assert_eq!(state(&observer, "legacy").0, "running");
    let metadata: (Option<String>, Option<String>) = observer
        .query_row(
            "SELECT claim_id,owner_id FROM tasks WHERE id='legacy'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(metadata, (None, None));
    assert_eq!(events(&observer), 0);
    observer
        .execute_batch("DROP TRIGGER reject_migration_event")
        .unwrap();
    assert!(migrate(&path, true).status.success());
    assert_eq!(events(&observer), 1);
}

#[test]
fn round3_legacy_migration_refuses_a_live_owner_even_with_acknowledgement() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.sqlite");
    let db = Database::open(&path).unwrap();
    let observer = Connection::open(&path).unwrap();
    observer.execute("INSERT INTO tasks(id,task_type,payload,status,created_at) VALUES('legacy','consolidate_memory','','running','now')",[]).unwrap();
    let refused = migrate(&path, true);
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("live task owner"));
    assert_eq!(state(&observer, "legacy").0, "running");
    drop(db);
    assert!(migrate(&path, true).status.success());
}

#[tokio::test]
async fn round3_receipt_projection_failure_without_tasks_matches_sqlite_and_broadcast() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("receipt.sqlite");
    let db = Arc::new(Database::open(&path).unwrap());
    let observer = Connection::open(&path).unwrap();
    std::fs::create_dir(dir.path().join("transcript.jsonl")).unwrap();
    let engine = HeartbeatEngine::new(60, db);
    let mut rx = engine.subscribe();
    let receipt = engine.pulse_once().await;
    assert_eq!(receipt.status, "degraded");
    let stored: Value = observer.query_row("SELECT tick_id,timestamp,status,tasks_scanned,actions_dispatched,notes FROM heartbeats WHERE tick_id=?1",[receipt.tick_id],|r|Ok(json!({"tick_id":r.get::<_,u64>(0)?,"timestamp":r.get::<_,String>(1)?,"status":r.get::<_,String>(2)?,"tasks_scanned":r.get::<_,usize>(3)?,"actions_dispatched":r.get::<_,usize>(4)?,"notes":r.get::<_,String>(5)?}))).unwrap();
    assert_eq!(stored, serde_json::to_value(&receipt).unwrap());
    assert_eq!(
        serde_json::to_value(rx.recv().await.unwrap()).unwrap(),
        stored
    );
    assert_eq!(events(&observer), 0);
}

struct PendingHealth(AtomicUsize);
#[async_trait::async_trait]
impl InferenceEngine for PendingHealth {
    fn model_id(&self) -> String {
        "pending-health".into()
    }
    fn endpoint(&self) -> String {
        "test-only".into()
    }
    async fn generate_with_tokens(
        &self,
        _: &str,
        _: Option<&str>,
        _: Option<f32>,
        _: Option<u32>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        unreachable!()
    }
    async fn generate_chat(
        &self,
        _: &[Value],
        _: Option<f32>,
        _: Option<u32>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        unreachable!()
    }
    async fn generate_chat_with_tools(
        &self,
        _: &[Value],
        _: Option<&[Value]>,
        _: Option<f32>,
        _: Option<u32>,
    ) -> Result<ChatTurnResponse, Box<dyn std::error::Error + Send + Sync>> {
        unreachable!()
    }
    async fn stream_chat(
        &self,
        _: &[Value],
        _: Option<f32>,
        _: Option<u32>,
    ) -> Result<ChatStream, Box<dyn std::error::Error + Send + Sync>> {
        unreachable!()
    }
    async fn check_health(&self) -> bool {
        self.0.fetch_add(1, Ordering::SeqCst);
        std::future::pending().await
    }
}
#[tokio::test(start_paused = true)]
async fn round3_health_deadline_fails_once_and_later_pulses_continue() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("deadline.sqlite");
    let db = Arc::new(Database::open(&path).unwrap());
    let observer = Connection::open(&path).unwrap();
    db.create_task("never", "model_health", "").unwrap();
    let health = Arc::new(PendingHealth(AtomicUsize::new(0)));
    let engine = Arc::new(HeartbeatEngine::with_inference(
        60,
        db.clone(),
        health.clone(),
    ));
    let mut rx = engine.subscribe();
    let action_engine = engine.clone();
    let action = tokio::spawn(async move { action_engine.pulse_once().await });
    while health.0.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(30)).await;
    tokio::time::resume();
    let receipt = tokio::time::timeout(Duration::from_secs(2), action)
        .await
        .expect("health action must have its own deadline")
        .unwrap();
    assert_eq!(receipt.status, "degraded");
    assert_eq!(rx.recv().await.unwrap().status, "degraded");
    let failed = state(&observer, "never");
    assert_eq!(failed.0, "failed");
    assert!(failed.1.unwrap().contains("deadline"));
    assert_eq!(events(&observer), 1);
    db.create_task("next", "system_telemetry", "").unwrap();
    let next = engine.pulse_once().await;
    assert_eq!(next.status, "nominal");
    assert_eq!(next.actions_dispatched, 1);
    assert_eq!(health.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        events(&observer),
        2,
        "one deadline failure and one successful subsequent action"
    );
}
