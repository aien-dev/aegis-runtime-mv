//! Local task claims and the durable terminal-event outbox. Retrying persistence never replays work.
use super::legacy::Database;
use chrono::Utc;
use rusqlite::{params, TransactionBehavior};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

type DbResult<T> = Result<T, rusqlite::Error>;

fn io_error(error: std::io::Error) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

fn refused(message: &str) -> rusqlite::Error {
    io_error(std::io::Error::other(message))
}

pub(crate) fn lock_file(path: &Path, nonblocking: bool) -> std::io::Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(path)?;
    let flags = libc::LOCK_EX | if nonblocking { libc::LOCK_NB } else { 0 };
    // The owned descriptor stays alive for the whole protected operation or owner lifetime.
    if unsafe { libc::flock(file.as_raw_fd(), flags) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(file)
}

#[derive(Clone, Debug)]
pub struct TaskClaim {
    pub id: String,
    pub token: String,
    pub owner: String,
}

pub(crate) struct TaskOwner {
    id: String,
    directory: Option<PathBuf>,
    _lock: Option<File>,
    pending: Mutex<Vec<TerminalOutcome>>,
}

impl TaskOwner {
    pub(crate) fn new(path: Option<&Path>) -> DbResult<Self> {
        let id = Uuid::new_v4().to_string();
        let directory = path
            .map(|path| {
                let canonical = std::fs::canonicalize(path)?;
                let mut name = canonical.as_os_str().to_os_string();
                name.push(".task-owners");
                let directory = PathBuf::from(name);
                std::fs::create_dir_all(&directory)?;
                Ok::<_, std::io::Error>(directory)
            })
            .transpose()
            .map_err(io_error)?;
        let lock = directory
            .as_ref()
            .map(|dir| lock_file(&dir.join(&id), true))
            .transpose()
            .map_err(io_error)?;
        Ok(Self {
            id,
            directory,
            _lock: lock,
            pending: Mutex::new(Vec::new()),
        })
    }
}

struct TerminalOutcome {
    claim: TaskClaim,
    success: bool,
    result: String,
    timestamp: String,
    event_id: String,
}

impl TerminalOutcome {
    fn new(claim: TaskClaim, result: Result<String, String>) -> Self {
        let (success, result) = match result {
            Ok(text) => (true, text),
            Err(text) => (false, text),
        };
        Self {
            claim,
            success,
            result,
            timestamp: Utc::now().to_rfc3339(),
            event_id: Uuid::new_v4().to_string(),
        }
    }
}

#[derive(Default)]
pub(crate) struct TaskFlushSummary {
    pub completed: usize,
    pub failed: usize,
}

/// A cancelled pulse cannot silently forget a claimed task while its owner remains alive.
pub(crate) struct TaskExecution<'a> {
    db: &'a Database,
    claim: Option<TaskClaim>,
}

impl TaskExecution<'_> {
    pub(crate) fn finish(mut self, outcome: Result<String, String>) {
        self.db
            .retain_task_outcome(self.claim.take().unwrap(), outcome);
    }
}

impl Drop for TaskExecution<'_> {
    fn drop(&mut self) {
        if let Some(claim) = self.claim.take() {
            self.db
                .retain_task_outcome(claim, Err("abandoned: outcome unknown".into()));
        }
    }
}

impl Database {
    pub fn claim_task(&self, id: &str) -> DbResult<Option<TaskClaim>> {
        let claim = TaskClaim {
            id: id.into(),
            token: Uuid::new_v4().to_string(),
            owner: self.owner.id.clone(),
        };
        let conn = self.conn.lock().unwrap();
        let changed = conn.execute(
            "UPDATE tasks SET status='running', claim_id=?2, owner_id=?3 WHERE id=?1 AND status='pending'",
            params![id, claim.token, claim.owner],
        )?;
        Ok((changed == 1).then_some(claim))
    }

    pub(crate) fn hold_claim(&self, claim: TaskClaim) -> TaskExecution<'_> {
        TaskExecution {
            db: self,
            claim: Some(claim),
        }
    }

    fn retain_task_outcome(&self, claim: TaskClaim, result: Result<String, String>) {
        self.owner
            .pending
            .lock()
            .unwrap()
            .push(TerminalOutcome::new(claim, result));
    }

    /// Retries immutable outcomes before scanning new tasks, even when there is no queued work.
    pub(crate) fn retry_task_outcomes(&self) -> DbResult<TaskFlushSummary> {
        let mut pending = self.owner.pending.lock().unwrap();
        let mut first_error = None;
        let mut summary = TaskFlushSummary::default();
        pending.retain(|outcome| match self.commit_terminal(outcome) {
            Ok(()) => {
                if outcome.success { summary.completed += 1; } else { summary.failed += 1; }
                false
            }
            Err(error) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
                true
            }
        });
        drop(pending);
        let export = self.export_terminal_events();
        if let Some(error) = first_error {
            Err(error)
        } else {
            export.map(|()| summary)
        }
    }

    pub fn complete_task(&self, claim: &TaskClaim, result: &str) -> DbResult<()> {
        self.commit_terminal(&TerminalOutcome::new(claim.clone(), Ok(result.into())))?;
        self.export_terminal_events()
    }

    pub fn fail_task(&self, claim: &TaskClaim, error: &str) -> DbResult<()> {
        self.commit_terminal(&TerminalOutcome::new(claim.clone(), Err(error.into())))?;
        self.export_terminal_events()
    }

    fn commit_terminal(&self, outcome: &TerminalOutcome) -> DbResult<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::write_terminal(&tx, outcome)?;
        tx.commit()
    }

    fn write_terminal(tx: &rusqlite::Transaction<'_>, outcome: &TerminalOutcome) -> DbResult<()> {
        let status = if outcome.success { "completed" } else { "failed" };
        let changed = tx.execute(
            "UPDATE tasks SET status=?1, result=?2, completed_at=?3 WHERE id=?4 AND status='running' AND claim_id=?5 AND owner_id=?6",
            params![
                status,
                outcome.result,
                outcome.success.then_some(&outcome.timestamp),
                outcome.claim.id,
                outcome.claim.token,
                outcome.claim.owner
            ],
        )?;
        if changed != 1 {
            return Err(refused("task finalization refused: stale claim or terminal task"));
        }
        let event_type = if outcome.success { "task_completed" } else { "task_failed" };
        let data = if outcome.success {
            serde_json::json!({
                "id": outcome.claim.id,
                "result": outcome.result,
                "completed_at": outcome.timestamp
            })
        } else {
            serde_json::json!({
                "id": outcome.claim.id,
                "error": outcome.result,
                "failed_at": outcome.timestamp
            })
        };
        let event = serde_json::json!({
            "event_id": outcome.event_id,
            "timestamp": outcome.timestamp,
            "event_type": event_type,
            "claim_id": outcome.claim.token,
            "data": data
        })
        .to_string();
        tx.execute(
            "INSERT INTO task_terminal_events(event_id,task_id,claim_id,event_json,exported) VALUES(?1,?2,?3,?4,0)",
            params![outcome.event_id, outcome.claim.id, outcome.claim.token, event],
        )?;
        Ok(())
    }

    /// A live owner's lock is never stolen. Unknown legacy owners need a quiescent migration.
    pub(crate) fn recover_abandoned_tasks(&self) -> DbResult<usize> {
        let claims: Vec<TaskClaim> = {
            let conn = self.conn.lock().unwrap();
            let unknown: i64 = conn.query_row(
                "SELECT count(*) FROM tasks WHERE status='running' AND (owner_id IS NULL OR claim_id IS NULL)",
                [],
                |row| row.get(0),
            )?;
            if unknown != 0 {
                return Err(refused("ownerless running tasks require a quiescent migration; outcomes unknown"));
            }
            let mut stmt = conn.prepare("SELECT id,claim_id,owner_id FROM tasks WHERE status='running'")?;
            let rows = stmt.query_map([], |row| {
                Ok(TaskClaim {
                    id: row.get(0)?,
                    token: row.get(1)?,
                    owner: row.get(2)?,
                })
            })?;
            rows.collect::<DbResult<_>>()?
        };
        let mut recovered = 0;
        if let Some(directory) = &self.owner.directory {
            for claim in claims {
                if claim.owner == self.owner.id {
                    continue;
                }
                Uuid::parse_str(&claim.owner).map_err(|_| refused("invalid task owner identity"))?;
                let _dead_owner = match lock_file(&directory.join(&claim.owner), true) {
                    Ok(lock) => lock,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => continue,
                    Err(error) => return Err(io_error(error)),
                };
                let still_running: bool = self.conn.lock().unwrap().query_row(
                    "SELECT EXISTS(SELECT 1 FROM tasks WHERE id=?1 AND status='running' AND claim_id=?2 AND owner_id=?3)",
                    params![claim.id, claim.token, claim.owner],
                    |row| row.get(0),
                )?;
                if !still_running {
                    continue;
                }
                self.commit_terminal(&TerminalOutcome::new(
                    claim,
                    Err("abandoned: outcome unknown".into()),
                ))?;
                recovered += 1;
            }
        }
        Ok(recovered)
    }

    /// Operator-only migration. All old and new task writers must be stopped first.
    /// `quiescent` acknowledges that pre-claim binaries cannot be detected with owner locks.
    pub fn migrate_legacy_running_tasks(path: &Path, quiescent: bool) -> DbResult<usize> {
        if !quiescent {
            return Err(refused("legacy migration requires --quiescent after stopping all task writers"));
        }
        // A mistyped path must not create a new database and appear to migrate successfully.
        let conn = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        let db = Self {
            conn: Arc::new(Mutex::new(conn)),
            transcript_path: path.parent().map(|parent| parent.join("transcript.jsonl")),
            owner: TaskOwner::new(Some(path))?,
        };
        let mut owner_locks = Vec::new();
        for entry in std::fs::read_dir(db.owner.directory.as_ref().unwrap()).map_err(io_error)? {
            let entry = entry.map_err(io_error)?;
            let name = entry.file_name();
            let name = name.to_str().ok_or_else(|| refused("invalid task owner identity"))?;
            Uuid::parse_str(name).map_err(|_| refused("invalid task owner identity"))?;
            if name == db.owner.id { continue; }
            match lock_file(&entry.path(), true) {
                Ok(lock) => owner_locks.push(lock),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    return Err(refused("legacy migration refused: live task owner; stop all task writers"));
                }
                Err(error) => return Err(io_error(error)),
            }
        }
        db.init_tables()?;
        let migrated = {
            let mut conn = db.conn.lock().unwrap();
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let ids = {
                let mut stmt = tx.prepare("SELECT id FROM tasks WHERE status='running' AND (owner_id IS NULL OR claim_id IS NULL)")?;
                let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
                rows.collect::<DbResult<Vec<_>>>()?
            };
            for id in &ids {
                let claim = TaskClaim { id: id.clone(), token: Uuid::new_v4().to_string(), owner: db.owner.id.clone() };
                let changed = tx.execute(
                    "UPDATE tasks SET claim_id=?2,owner_id=?3 WHERE id=?1 AND status='running' AND (owner_id IS NULL OR claim_id IS NULL)",
                    params![claim.id, claim.token, claim.owner],
                )?;
                if changed != 1 { return Err(refused("legacy migration refused: task changed")); }
                Self::write_terminal(&tx, &TerminalOutcome::new(claim, Err("abandoned: outcome unknown".into())))?;
            }
            tx.commit()?;
            ids.len()
        };
        db.export_terminal_events()?;
        drop(owner_locks);
        Ok(migrated)
    }

    /// SQLite is authoritative. JSONL is a retryable projection identified by stable event IDs.
    pub(crate) fn export_terminal_events(&self) -> DbResult<()> {
        let Some(path) = &self.transcript_path else {
            return Ok(());
        };
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT event_id,event_json FROM task_terminal_events WHERE exported=0 ORDER BY rowid")?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let events = rows.collect::<DbResult<Vec<_>>>()?;
        if events.is_empty() {
            return Ok(());
        }
        let _writer = lock_file(&path.with_extension("jsonl.lock"), false).map_err(io_error)?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
            .map_err(io_error)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(io_error)?;
        // Discard only an interrupted trailing append; committed complete records are immutable.
        let end = bytes.iter().rposition(|b| *b == b'\n').map_or(0, |pos| pos + 1);
        file.set_len(end as u64).map_err(io_error)?;
        file.seek(SeekFrom::End(0)).map_err(io_error)?;
        let seen: HashSet<String> = bytes[..end]
            .split(|b| *b == b'\n')
            .filter_map(|line| serde_json::from_slice::<serde_json::Value>(line).ok())
            .filter_map(|event| event.get("event_id").and_then(|id| id.as_str()).map(str::to_owned))
            .collect();
        for (id, json) in events {
            if !seen.contains(&id) {
                writeln!(file, "{json}").map_err(io_error)?;
                file.sync_all().map_err(io_error)?;
            }
            conn.execute("UPDATE task_terminal_events SET exported=1 WHERE event_id=?1", params![id])?;
        }
        Ok(())
    }
}
