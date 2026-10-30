use chrono::Utc;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing::info;

use super::async_db::AsyncDatabase;
use super::sqlite::schema::initialize_schema;
use super::task_lifecycle::{lock_file, TaskOwner};
use crate::heartbeat::PulseReceipt;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TaskRecord {
    pub id: String,
    pub task_type: String,
    pub payload: String,
    pub status: String,
    pub result: Option<String>,
    pub created_at: String,
    pub completed_at: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CrumbRecord {
    pub key: String,
    pub value: String,
    pub namespace: String,
    pub created_at: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TurnRecord {
    pub id: i64,
    pub session_id: String,
    pub role: String,
    pub prompt: String,
    pub response: String,
    pub duration_ms: u64,
    pub created_at: String,
}

pub struct Database {
    pub(super) conn: Arc<Mutex<Connection>>,
    pub(super) transcript_path: Option<PathBuf>,
    pub(super) owner: TaskOwner,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open(path)?;
        let transcript_path = path.parent().map(|p| p.join("transcript.jsonl"));
        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
            transcript_path,
            owner: TaskOwner::new(Some(path))?,
        };
        db.init_tables()?;
        db.recover_abandoned_tasks()?;
        db.export_terminal_events()?;
        Ok(db)
    }

    pub fn open_in_memory() -> Result<Self, rusqlite::Error> {
        let conn = Connection::open_in_memory()?;
        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
            transcript_path: None,
            owner: TaskOwner::new(None)?,
        };
        db.init_tables()?;
        Ok(db)
    }

    pub fn to_async(&self) -> AsyncDatabase {
        AsyncDatabase::from_connection_arc(Arc::clone(&self.conn), self.transcript_path.clone())
    }

    pub fn init_tables(&self) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        initialize_schema(&conn)?;
        info!("SQLite tables initialized");
        Ok(())
    }

    pub fn record_turn(
        &self,
        role: &str,
        prompt: &str,
        response: &str,
        duration_ms: u64,
    ) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO turns (session_id, role, prompt, response, duration_ms, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                "default",
                role,
                prompt,
                response,
                duration_ms as i64,
                Utc::now().to_rfc3339()
            ],
        )?;

        self.record_transcript(
            "chat_turn",
            &serde_json::json!({
                "role": role,
                "prompt": prompt,
                "response": response,
                "duration_ms": duration_ms,
            }),
        )?;

        Ok(())
    }

    pub fn list_recent_turns(&self, limit: usize) -> Result<Vec<TurnRecord>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, session_id, role, prompt, response, duration_ms, created_at
             FROM turns ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |row| {
            Ok(TurnRecord {
                id: row.get(0)?,
                session_id: row.get(1)?,
                role: row.get(2)?,
                prompt: row.get(3)?,
                response: row.get(4)?,
                duration_ms: row.get::<_, i64>(5)? as u64,
                created_at: row.get(6)?,
            })
        })?;

        let mut turns = Vec::new();
        for r in rows {
            turns.push(r?);
        }
        Ok(turns)
    }

    /// Projection failure changes the receipt before its authoritative SQLite transaction commits.
    pub fn record_heartbeat_tick(&self, receipt: &mut PulseReceipt) -> Result<(), rusqlite::Error> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT OR REPLACE INTO heartbeats (tick_id, timestamp, status, tasks_scanned, actions_dispatched, notes)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                receipt.tick_id as i64,
                receipt.timestamp,
                receipt.status,
                receipt.tasks_scanned as i64,
                receipt.actions_dispatched as i64,
                receipt.notes
            ],
        )?;
        if let Err(error) = self.record_transcript("heartbeat_tick", receipt) {
            receipt.status = "degraded";
            receipt
                .notes
                .push_str(&format!("; receipt projection failed: {}", error));
            tracing::warn!("Heartbeat transcript projection failed: {}", error);
            tx.execute(
                "UPDATE heartbeats SET status=?2,notes=?3 WHERE tick_id=?1",
                params![receipt.tick_id as i64, receipt.status, receipt.notes],
            )?;
        }
        tx.commit()
    }

    pub fn create_task(
        &self,
        id: &str,
        task_type: &str,
        payload: &str,
    ) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO tasks (id, task_type, payload, status, created_at)
             VALUES (?1, ?2, ?3, 'pending', ?4)",
            params![id, task_type, payload, now],
        )?;

        self.record_transcript(
            "task_created",
            &serde_json::json!({
                "id": id,
                "task_type": task_type,
                "payload": payload,
            }),
        )?;
        Ok(())
    }

    pub fn list_pending_tasks(&self) -> Result<Vec<TaskRecord>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, task_type, payload, status, result, created_at, completed_at
             FROM tasks WHERE status = 'pending' ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(TaskRecord {
                id: row.get(0)?,
                task_type: row.get(1)?,
                payload: row.get(2)?,
                status: row.get(3)?,
                result: row.get(4)?,
                created_at: row.get(5)?,
                completed_at: row.get(6)?,
            })
        })?;

        let mut tasks = Vec::new();
        for r in rows {
            tasks.push(r?);
        }
        Ok(tasks)
    }

    pub fn store_crumb(
        &self,
        key: &str,
        value: &str,
        namespace: &str,
    ) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "INSERT OR REPLACE INTO memory_crumbs (key, value, namespace, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![key, value, namespace, now],
        )?;

        self.record_transcript(
            "memory_crumb",
            &serde_json::json!({
                "key": key,
                "value": value,
                "namespace": namespace,
            }),
        )?;
        Ok(())
    }

    pub fn get_recent_crumbs(
        &self,
        namespace: &str,
        limit: usize,
    ) -> Result<Vec<CrumbRecord>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT key, value, namespace, created_at
             FROM memory_crumbs WHERE namespace = ?1 ORDER BY created_at DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![namespace, limit as i64], |row| {
            Ok(CrumbRecord {
                key: row.get(0)?,
                value: row.get(1)?,
                namespace: row.get(2)?,
                created_at: row.get(3)?,
            })
        })?;

        let mut crumbs = Vec::new();
        for r in rows {
            crumbs.push(r?);
        }
        Ok(crumbs)
    }

    pub fn record_transcript<T: Serialize>(
        &self,
        event_type: &str,
        data: &T,
    ) -> Result<(), rusqlite::Error> {
        if let Some(path) = &self.transcript_path {
            let event = serde_json::json!({
                "timestamp": Utc::now().to_rfc3339(),
                "event_type": event_type,
                "data": data,
            });
            let write = || -> std::io::Result<()> {
                let _writer = lock_file(&path.with_extension("jsonl.lock"), false)?;
                let mut file = OpenOptions::new().create(true).append(true).open(path)?;
                writeln!(file, "{}", event)?;
                file.sync_all()
            };
            write().map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        }
        Ok(())
    }
}
