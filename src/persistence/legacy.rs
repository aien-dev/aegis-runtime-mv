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
    conn: Arc<Mutex<Connection>>,
    transcript_path: Option<PathBuf>,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open(path)?;
        let transcript_path = path.parent().map(|p| p.join("transcript.jsonl"));
        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
            transcript_path,
        };
        db.init_tables()?;
        Ok(db)
    }

    pub fn open_in_memory() -> Result<Self, rusqlite::Error> {
        let conn = Connection::open_in_memory()?;
        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
            transcript_path: None,
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
        );

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

    pub fn record_heartbeat_tick(&self, receipt: &PulseReceipt) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
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

        self.record_transcript("heartbeat_tick", &receipt);
        Ok(())
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
        );
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

    /// Moves a pending task to `running`. Returns `false` when another caller
    /// already claimed it, so each task runs at most once.
    pub fn claim_task(&self, id: &str) -> Result<bool, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let changed = conn.execute(
            "UPDATE tasks SET status = 'running' WHERE id = ?1 AND status = 'pending'",
            params![id],
        )?;
        Ok(changed == 1)
    }

    pub fn complete_task(&self, id: &str, result: &str) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "UPDATE tasks SET status = 'completed', result = ?1, completed_at = ?2 WHERE id = ?3",
            params![result, now, id],
        )?;

        self.record_transcript(
            "task_completed",
            &serde_json::json!({
                "id": id,
                "result": result,
                "completed_at": now,
            }),
        );
        Ok(())
    }

    pub fn fail_task(&self, id: &str, error: &str) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE tasks SET status = 'failed', result = ?1, completed_at = NULL WHERE id = ?2",
            params![error, id],
        )?;

        self.record_transcript(
            "task_failed",
            &serde_json::json!({
                "id": id,
                "error": error,
                "failed_at": Utc::now().to_rfc3339(),
            }),
        );
        Ok(())
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
        );
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

    pub fn record_transcript<T: Serialize>(&self, event_type: &str, data: &T) {
        if let Some(ref path) = self.transcript_path {
            let event = serde_json::json!({
                "timestamp": Utc::now().to_rfc3339(),
                "event_type": event_type,
                "data": data,
            });
            if let Ok(line) = serde_json::to_string(&event) {
                if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
                    let _ = writeln!(file, "{}", line);
                }
            }
        }
    }
}
