//! Durable reply ledger reimplementation inspired by Hermes Agent, Nous Research,
//! MIT License, commit 5480918. No Hermes implementation code is copied.
//!
//! Reply intent is committed before transport. A transport write is not an
//! application acknowledgement, so unresolved sends remain pending on recovery.
use rusqlite::{params, Connection, OptionalExtension};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplyObligation {
    pub id: String,
    pub session: String,
    pub message_ref: String,
    pub reply: String,
    pub may_be_duplicate: bool,
}

#[derive(Clone)]
pub struct DurableRuntime {
    conn: Arc<Mutex<Connection>>,
}

impl DurableRuntime {
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        Self::initialize(conn)
    }

    pub fn open_in_memory() -> rusqlite::Result<Self> {
        Self::initialize(Connection::open_in_memory()?)
    }

    fn initialize(conn: Connection) -> rusqlite::Result<Self> {
        conn.execute_batch(
            "PRAGMA foreign_keys=ON;
             CREATE TABLE IF NOT EXISTS durable_replies (
               id TEXT PRIMARY KEY,
               session TEXT NOT NULL,
               message_ref TEXT NOT NULL,
               reply TEXT NOT NULL,
               state TEXT NOT NULL CHECK(state IN ('pending','acked')),
               UNIQUE(session, message_ref)
             );",
        )?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Persist the complete reply before returning it to a transport caller.
    /// Length-prefixing prevents ambiguity between adjacent UTF-8 fields.
    pub fn record_reply(
        &self,
        session: &str,
        message_ref: &str,
        reply: &str,
    ) -> rusqlite::Result<ReplyObligation> {
        let mut hash = Sha256::new();
        for field in [session.as_bytes(), message_ref.as_bytes(), reply.as_bytes()] {
            hash.update((field.len() as u64).to_be_bytes());
            hash.update(field);
        }
        let id = format!("sha256:{:x}", hash.finalize());
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| rusqlite::Error::InvalidQuery)?;
        let tx = conn.transaction()?;
        let existing: Option<(String, String)> = tx
            .query_row(
                "SELECT id, reply FROM durable_replies WHERE session=?1 AND message_ref=?2",
                params![session, message_ref],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if let Some((existing_id, existing_reply)) = existing {
            if existing_reply != reply || existing_id != id {
                return Err(rusqlite::Error::InvalidQuery);
            }
        } else {
            tx.execute("INSERT INTO durable_replies(id,session,message_ref,reply,state) VALUES (?1,?2,?3,?4,'pending')", params![id, session, message_ref, reply])?;
        }
        tx.commit()?;
        Ok(ReplyObligation {
            id,
            session: session.to_owned(),
            message_ref: message_ref.to_owned(),
            reply: reply.to_owned(),
            may_be_duplicate: false,
        })
    }

    pub fn pending_replies(&self) -> rusqlite::Result<Vec<ReplyObligation>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| rusqlite::Error::InvalidQuery)?;
        let mut stmt = conn.prepare("SELECT id,session,message_ref,reply FROM durable_replies WHERE state='pending' ORDER BY rowid")?;
        let rows = stmt.query_map([], |r| {
            Ok(ReplyObligation {
                id: r.get(0)?,
                session: r.get(1)?,
                message_ref: r.get(2)?,
                reply: r.get(3)?,
                may_be_duplicate: true,
            })
        })?;
        rows.collect()
    }

    /// Acknowledge only after the client confirms application receipt.
    pub fn acknowledge_reply(&self, id: &str) -> rusqlite::Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| rusqlite::Error::InvalidQuery)?;
        Ok(conn.execute(
            "UPDATE durable_replies SET state='acked' WHERE id=?1 AND state='pending'",
            [id],
        )? == 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn identity_uses_unambiguous_field_boundaries() {
        let db = DurableRuntime::open_in_memory().unwrap();
        let a = db.record_reply("ab", "c", "d").unwrap();
        let b = db.record_reply("a", "bc", "d").unwrap();
        assert_ne!(a.id, b.id);
    }
}
