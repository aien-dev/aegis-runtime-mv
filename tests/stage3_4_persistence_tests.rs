use aegis::{
    Action, ActionReceipt, ActionStatus, ActionStore, Approval, ApprovalStatus, ApprovalStore,
    AsyncDatabase, Database, DoctrineDecision, Message, MessageId, MessageStore, PersistenceError,
    Role, Run, RunBudget, RunState, RunStore, Session, SessionStatus, SessionStore,
    SqliteActionStore, SqliteApprovalStore, SqliteMessageStore, SqliteRunStore, SqliteSessionStore,
    SystemMessage, Trigger, UserMessage,
};

#[tokio::test]
async fn test_sqlite_session_crud_and_optimistic_concurrency() {
    let db = AsyncDatabase::open_in_memory().unwrap();
    let store = SqliteSessionStore::new(db);

    let mut session = Session::new(Some("Optimistic test".to_string()));
    assert_eq!(session.version, 1);
    store.create(&session).await.unwrap();

    let fetched = store
        .get(&session.id)
        .await
        .unwrap()
        .expect("session must exist");
    assert_eq!(fetched.id, session.id);
    assert_eq!(fetched.version, 1);
    assert_eq!(fetched.objective.as_deref(), Some("Optimistic test"));

    // First update succeeds and increments version to 2
    session.status = SessionStatus::Suspended;
    store.update(&session).await.unwrap();

    let updated = store
        .get(&session.id)
        .await
        .unwrap()
        .expect("session must exist");
    assert_eq!(updated.version, 2);
    assert_eq!(updated.status, SessionStatus::Suspended);

    // Stale update with version 1 must return ConcurrencyConflict
    let err = store.update(&session).await.unwrap_err();
    match err {
        PersistenceError::ConcurrencyConflict {
            id,
            expected_version,
        } => {
            assert_eq!(id, session.id.to_string());
            assert_eq!(expected_version, 1);
        }
        other => panic!("Expected ConcurrencyConflict, got: {:?}", other),
    }

    // Update with fresh version 2 succeeds and increments to 3
    let mut fresh_session = updated;
    fresh_session.objective = Some("Updated objective".to_string());
    store.update(&fresh_session).await.unwrap();

    let final_session = store
        .get(&session.id)
        .await
        .unwrap()
        .expect("session must exist");
    assert_eq!(final_session.version, 3);
    assert_eq!(
        final_session.objective.as_deref(),
        Some("Updated objective")
    );

    // List sessions
    let list = store.list(10, 0).await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, session.id);
}

#[tokio::test]
async fn test_sqlite_message_store_lifecycle() {
    let db = AsyncDatabase::open_in_memory().unwrap();
    let session_store = SqliteSessionStore::new(db.clone());
    let msg_store = SqliteMessageStore::new(db);

    let session = Session::new(Some("Message lifecycle test".to_string()));
    session_store.create(&session).await.unwrap();

    let msg1 = Message::System(SystemMessage {
        id: MessageId::new(),
        content: "You are Aegis.".to_string(),
        created_at: 100,
    });
    let msg2 = Message::User(UserMessage {
        id: MessageId::new(),
        content: "Run test scan.".to_string(),
        created_at: 101,
    });

    msg_store.append(&session.id, &msg1).await.unwrap();
    msg_store.append(&session.id, &msg2).await.unwrap();

    let messages = msg_store.list(&session.id).await.unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].role(), Role::System);
    assert_eq!(messages[1].role(), Role::User);

    let retrieved = msg_store
        .get(msg1.id())
        .await
        .unwrap()
        .expect("msg1 exists");
    assert_eq!(retrieved.id(), msg1.id());

    // Messages also load with session.get()
    let loaded_session = session_store.get(&session.id).await.unwrap().unwrap();
    assert_eq!(loaded_session.messages.len(), 2);
}

#[tokio::test]
async fn test_sqlite_run_store_single_active_run_constraint() {
    let db = AsyncDatabase::open_in_memory().unwrap();
    let session_store = SqliteSessionStore::new(db.clone());
    let run_store = SqliteRunStore::new(db);

    let session = Session::new(Some("Run constraint test".to_string()));
    session_store.create(&session).await.unwrap();

    let run1 = Run::new(session.id.clone(), Trigger::Heartbeat, RunBudget::default());
    run_store.create(&run1).await.unwrap();

    // Attempting to create another active run on the same session must fail with ActiveRunConflict
    let run2 = Run::new(session.id.clone(), Trigger::Heartbeat, RunBudget::default());
    let err = run_store.create(&run2).await.unwrap_err();
    match err {
        PersistenceError::ActiveRunConflict {
            session_id,
            active_run_id,
        } => {
            assert_eq!(session_id, session.id.to_string());
            assert_eq!(active_run_id, run1.id.to_string());
        }
        other => panic!("Expected ActiveRunConflict, got: {:?}", other),
    }

    // Active run can be retrieved
    let active = run_store
        .get_active_run(&session.id)
        .await
        .unwrap()
        .expect("active run must exist");
    assert_eq!(active.id, run1.id);

    // Once run1 completes, run2 can be created
    let mut finished_run1 = run1.clone();
    finished_run1.state = RunState::Completed;
    finished_run1.completed_at = Some(chrono::Utc::now().timestamp());
    run_store.update(&finished_run1).await.unwrap();

    assert!(run_store
        .get_active_run(&session.id)
        .await
        .unwrap()
        .is_none());

    run_store.create(&run2).await.unwrap();
    let new_active = run_store
        .get_active_run(&session.id)
        .await
        .unwrap()
        .expect("run2 is now active");
    assert_eq!(new_active.id, run2.id);
}

#[tokio::test]
async fn test_sqlite_action_and_approval_store() {
    let db = AsyncDatabase::open_in_memory().unwrap();
    let session_store = SqliteSessionStore::new(db.clone());
    let run_store = SqliteRunStore::new(db.clone());
    let action_store = SqliteActionStore::new(db.clone());
    let approval_store = SqliteApprovalStore::new(db);

    let session = Session::new(Some("Action and approval test".to_string()));
    session_store.create(&session).await.unwrap();

    let run = Run::new(session.id.clone(), Trigger::Heartbeat, RunBudget::default());
    run_store.create(&run).await.unwrap();

    let mut action = Action::new(
        run.id.clone(),
        session.id.clone(),
        "workspace_write",
        "write to file",
        serde_json::json!({"path": "/tmp/test.txt"}),
    );
    action_store.create(&action).await.unwrap();

    let mut approval = Approval::new(
        action.id.clone(),
        run.id.clone(),
        session.id.clone(),
        "Approve writing /tmp/test.txt?",
    );
    approval_store.create(&approval).await.unwrap();

    let pending = approval_store.list_pending().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, approval.id);

    // Approve
    approval.status = ApprovalStatus::Approved;
    approval.decided_at = Some(chrono::Utc::now().timestamp());
    approval.decided_by = Some("operator".to_string());
    approval_store.update(&approval).await.unwrap();

    let pending_after = approval_store.list_pending().await.unwrap();
    assert_eq!(pending_after.len(), 0);

    // Update action with receipt
    action.status = ActionStatus::Completed;
    action.receipt = Some(ActionReceipt {
        action_id: action.id.to_string(),
        session_id: session.id.to_string(),
        run_id: run.id.to_string(),
        capability: "workspace_write".to_string(),
        target: "/tmp/test.txt".to_string(),
        decision: DoctrineDecision::Allow,
        output: "file written successfully".to_string(),
        success: true,
        duration_ms: 42,
    });
    action.completed_at = Some(chrono::Utc::now().timestamp());
    action_store.update(&action).await.unwrap();

    let fetched_action = action_store
        .get(&action.id)
        .await
        .unwrap()
        .expect("action exists");
    assert_eq!(fetched_action.status, ActionStatus::Completed);
    assert!(fetched_action.receipt.is_some());
    assert_eq!(fetched_action.receipt.unwrap().duration_ms, 42);
}

#[test]
fn test_legacy_database_interop() {
    let db = Database::open_in_memory().unwrap();
    db.create_task("task_001", "scan", "run full audit")
        .unwrap();
    let pending = db.list_pending_tasks().unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, "task_001");

    db.store_crumb("key1", "val1", "ns").unwrap();
    let crumbs = db.get_recent_crumbs("ns", 5).unwrap();
    assert_eq!(crumbs.len(), 1);
    assert_eq!(crumbs[0].value, "val1");

    // Interop with async database
    let async_db = db.to_async();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let session_store = SqliteSessionStore::new(async_db);
        let session = Session::new(Some("Interop session".to_string()));
        session_store.create(&session).await.unwrap();
        let fetched = session_store.get(&session.id).await.unwrap().unwrap();
        assert_eq!(fetched.id, session.id);
    });
}

#[test]
fn test_legacy_task_claim_is_exclusive() {
    let db = Database::open_in_memory().unwrap();
    db.create_task("task_claim", "shell_exec", "echo once")
        .unwrap();

    let claim = db.claim_task("task_claim").unwrap().unwrap();
    assert!(db.claim_task("task_claim").unwrap().is_none());
    assert!(db.claim_task("no_such_task").unwrap().is_none());
    assert!(db.list_pending_tasks().unwrap().is_empty());

    db.complete_task(&claim, "done").unwrap();
    assert!(db.claim_task("task_claim").unwrap().is_none());
}
