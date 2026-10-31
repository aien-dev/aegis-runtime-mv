use rusqlite::Connection;

pub fn initialize_schema(conn: &Connection) -> Result<(), rusqlite::Error> {
    let _ = conn.pragma_update(None, "journal_mode", "WAL");
    let _ = conn.pragma_update(None, "busy_timeout", 5000);
    let _ = conn.pragma_update(None, "foreign_keys", "ON");

    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY,
            parent_session_id TEXT,
            status TEXT NOT NULL,
            objective TEXT,
            agent_profile TEXT NOT NULL,
            model_profile TEXT NOT NULL,
            policy_profile TEXT NOT NULL,
            budget_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            version INTEGER NOT NULL DEFAULT 1
        );
        CREATE INDEX IF NOT EXISTS idx_sessions_updated ON sessions (updated_at DESC);

        CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            role TEXT NOT NULL,
            data_json TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_messages_session ON messages (session_id, created_at ASC);

        CREATE TABLE IF NOT EXISTS runs (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            state TEXT NOT NULL,
            trigger_json TEXT NOT NULL,
            steps_used INTEGER NOT NULL DEFAULT 0,
            inference_calls_used INTEGER NOT NULL DEFAULT 0,
            actions_used INTEGER NOT NULL DEFAULT 0,
            budget_json TEXT NOT NULL,
            started_at INTEGER NOT NULL,
            completed_at INTEGER,
            termination_json TEXT,
            FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_runs_session ON runs (session_id, started_at DESC);
        CREATE INDEX IF NOT EXISTS idx_runs_active ON runs (session_id, state);

        CREATE TABLE IF NOT EXISTS actions (
            id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            capability TEXT NOT NULL,
            intent TEXT NOT NULL,
            params_json TEXT NOT NULL,
            status TEXT NOT NULL,
            receipt_json TEXT,
            created_at INTEGER NOT NULL,
            completed_at INTEGER,
            FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE,
            FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_actions_run ON actions (run_id, created_at ASC);

        CREATE TABLE IF NOT EXISTS approvals (
            id TEXT PRIMARY KEY,
            action_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            prompt TEXT NOT NULL,
            status TEXT NOT NULL,
            requested_at INTEGER NOT NULL,
            decided_at INTEGER,
            decided_by TEXT,
            FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE,
            FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
            FOREIGN KEY(action_id) REFERENCES actions(id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_approvals_pending ON approvals (status, requested_at ASC);

        CREATE TABLE IF NOT EXISTS audit_events (
            id TEXT PRIMARY KEY,
            sequence INTEGER NOT NULL,
            session_id TEXT,
            run_id TEXT,
            event_type TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            prev_hash TEXT NOT NULL,
            hash TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_audit_sequence ON audit_events (sequence ASC);

        CREATE TABLE IF NOT EXISTS turns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            role TEXT NOT NULL,
            prompt TEXT NOT NULL,
            response TEXT NOT NULL,
            duration_ms INTEGER NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS heartbeats (
            tick_id INTEGER PRIMARY KEY,
            timestamp TEXT NOT NULL,
            status TEXT NOT NULL,
            tasks_scanned INTEGER NOT NULL,
            actions_dispatched INTEGER NOT NULL,
            notes TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tasks (
            id TEXT PRIMARY KEY,
            task_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            status TEXT NOT NULL,
            result TEXT,
            created_at TEXT NOT NULL,
            completed_at TEXT
        );

        CREATE TABLE IF NOT EXISTS task_terminal_events (
            event_id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL UNIQUE,
            claim_id TEXT NOT NULL,
            event_json TEXT NOT NULL,
            exported INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS memory_crumbs (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            namespace TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS skills (
            name TEXT PRIMARY KEY,
            description TEXT NOT NULL,
            command TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        ",
    )?;
    let columns = {
        let mut stmt = conn.prepare("PRAGMA table_info(tasks)")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    for column in ["claim_id", "owner_id"] {
        if !columns.iter().any(|name| name == column) {
            conn.execute_batch(&format!("ALTER TABLE tasks ADD COLUMN {column} TEXT"))?;
        }
    }
    Ok(())
}
