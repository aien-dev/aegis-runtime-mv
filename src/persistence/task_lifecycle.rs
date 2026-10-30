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
use std::sync::Mutex;
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
    pub(crate) fn retry_task_outcomes(&self) -> DbResult<()> {
        let mut pending = self.owner.pending.lock().unwrap();
        let mut first_error = None;
        pending.retain(|outcome| match self.commit_terminal(outcome) {
            Ok(()) => false,
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
            export
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
        tx.commit()
    }

    /// A live owner's lock is never stolen. Unknown legacy owners need a quiescent migration.
    pub(crate) fn recover_abandoned_tasks(&self) -> DbResult<()> {
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
            }
        }
        Ok(())
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
