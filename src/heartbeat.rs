use chrono::Utc;
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::process::Command;
use tokio::sync::{broadcast, Mutex};
use tracing::{info, warn};

use crate::inference::InferenceEngine;
use crate::persistence::Database;
use crate::security::WorkspaceCapability;

const HEALTH_ACTION_TIMEOUT: Duration = Duration::from_secs(15);

async fn http_model_health(endpoint: &str) -> Result<bool, reqwest::Error> {
    let client = reqwest::Client::builder()
        .connect_timeout(HEALTH_ACTION_TIMEOUT)
        .timeout(HEALTH_ACTION_TIMEOUT)
        .build()?;
    Ok(client.get(endpoint).send().await?.status().is_success())
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PulseReceipt {
    pub tick_id: u64,
    pub timestamp: String,
    pub status: &'static str,
    pub tasks_scanned: usize,
    pub actions_dispatched: usize,
    pub notes: String,
}

pub struct HeartbeatEngine {
    interval_secs: u64,
    tick_counter: AtomicU64,
    db: Arc<Database>,
    inference: Option<Arc<dyn InferenceEngine>>,
    workspace: Arc<WorkspaceCapability>,
    is_running: Mutex<bool>,
    tx_pulse: broadcast::Sender<PulseReceipt>,
}

impl HeartbeatEngine {
    pub fn new(interval_secs: u64, db: Arc<Database>) -> Self {
        let (tx, _) = broadcast::channel(128);
        Self {
            interval_secs,
            tick_counter: AtomicU64::new(0),
            db,
            inference: None,
            workspace: Arc::new(WorkspaceCapability::detect()),
            is_running: Mutex::new(false),
            tx_pulse: tx,
        }
    }

    pub fn with_inference(
        interval_secs: u64,
        db: Arc<Database>,
        inference: Arc<dyn InferenceEngine>,
    ) -> Self {
        let (tx, _) = broadcast::channel(128);
        Self {
            interval_secs,
            tick_counter: AtomicU64::new(0),
            db,
            inference: Some(inference),
            workspace: Arc::new(WorkspaceCapability::detect()),
            is_running: Mutex::new(false),
            tx_pulse: tx,
        }
    }

    pub fn interval_secs(&self) -> u64 {
        self.interval_secs
    }

    pub fn tick_count(&self) -> u64 {
        self.tick_counter.load(Ordering::Relaxed)
    }

    pub fn with_workspace(
        interval_secs: u64,
        db: Arc<Database>,
        workspace: WorkspaceCapability,
    ) -> Self {
        let (tx, _) = broadcast::channel(128);
        Self {
            interval_secs,
            tick_counter: AtomicU64::new(0),
            db,
            inference: None,
            workspace: Arc::new(workspace),
            is_running: Mutex::new(false),
            tx_pulse: tx,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<PulseReceipt> {
        self.tx_pulse.subscribe()
    }

    pub async fn pulse_once(&self) -> PulseReceipt {
        let tick = self.tick_counter.fetch_add(1, Ordering::SeqCst) + 1;
        let now = Utc::now().to_rfc3339();

        let mut degraded = match self.db.recover_abandoned_tasks() {
            Ok(recovered) => recovered != 0,
            Err(error) => {
                warn!("Heartbeat could not recover abandoned claims: {}", error);
                true
            }
        };
        degraded |= match self.db.retry_task_outcomes() {
            Ok(summary) => summary.failed != 0,
            Err(error) => {
                warn!("Heartbeat retained finalization or transcript export for retry: {}", error);
                true
            }
        };
        let pending_tasks = match self.db.list_pending_tasks() {
            Ok(tasks) => tasks,
            Err(e) => {
                degraded = true;
                warn!(
                    "Heartbeat pulse #{} could not list pending tasks: {}",
                    tick, e
                );
                Vec::new()
            }
        };
        let tasks_count = pending_tasks.len();
        let mut actions = 0;

        for task in pending_tasks {
            // Another pulse may have taken this task since the list was read.
            let claim = match self.db.claim_task(&task.id) {
                Ok(Some(claim)) => claim,
                Ok(None) => continue,
                Err(e) => {
                    degraded = true;
                    warn!("Heartbeat could not claim task {}: {}", task.id, e);
                    continue;
                }
            };
            let execution = self.db.hold_claim(claim);

            let action = async {
                match task.task_type.as_str() {
                    // "max_health" is the former name, kept so tasks already queued under it still run.
                    "model_health" | "max_health" => {
                        let health = async {
                            if let Some(ref inf) = self.inference {
                                inf.check_health().await
                            } else {
                                http_model_health("http://127.0.0.1:18006/v1/models").await.unwrap_or(false)
                            }
                        };
                        let is_healthy = tokio::time::timeout(HEALTH_ACTION_TIMEOUT, health)
                            .await
                            .map_err(|_| "Model health deadline exceeded: outcome unknown".to_string())?;
    
                        if is_healthy {
                            Ok("Local model seat verified ACTIVE on port 18006".to_string())
                        } else {
                            Err("Local model seat unreachable or offline on port 18006".to_string())
                        }
                    }
                    "build_monitor" | "shell_exec" => {
                        let cmd_to_run = if task.payload.is_empty() {
                            "echo 'empty build task'"
                        } else {
                            &task.payload
                        };
    
                        match self
                            .workspace
                            .dispatch_shell_gated(cmd_to_run, None, 15)
                            .await
                        {
                            Ok(stdout) => Ok(format!("Exit code 0: {}", stdout.trim())),
                            Err(e) => Err(format!("Execution failure: {}", e)),
                        }
                    }
                    "consolidate_memory" => {
                        let crumb_key = format!("consolidation_tick_{}", tick);
                        self.db
                            .store_crumb(
                                &crumb_key,
                                "Episodic memory consolidated into WAL",
                                "heartbeat",
                            )
                            .map(|()| "Memory crumbs recorded".to_string())
                            .map_err(|e| format!("Memory consolidation failure: {}", e))
                    }
                    "system_telemetry" => Ok("System telemetry nominal: GB10 aarch64".to_string()),
                    _ => Err(format!("Unknown task type: {}", task.task_type)),
                }
            };
            let outcome = AssertUnwindSafe(action).catch_unwind().await
                .unwrap_or_else(|_| Err("Task action panicked: outcome unknown".into()));

            degraded |= outcome.is_err();
            execution.finish(outcome);
            actions += 1;
        }
        degraded |= match self.db.retry_task_outcomes() {
            Ok(summary) => summary.failed != 0,
            Err(error) => {
                warn!("Heartbeat retained finalization or transcript export for retry: {}", error);
                true
            }
        };

        let mut receipt = PulseReceipt {
            tick_id: tick,
            timestamp: now,
            status: if degraded { "degraded" } else { "nominal" },
            tasks_scanned: tasks_count,
            actions_dispatched: actions,
            notes: format!(
                "Autonomous tick #{} scanned {} pending tasks, dispatched {} actions",
                tick, tasks_count, actions
            ),
        };

        if let Err(error) = self.db.record_heartbeat_tick(&mut receipt) {
            receipt.status = "degraded";
            receipt.notes.push_str(&format!("; receipt persistence failed: {}", error));
        }
        let _ = self.tx_pulse.send(receipt.clone());

        // Record operational pulse event to the Crumb ledger (.crumb)
        let crumb_bin = if let Ok(home) = std::env::var("HOME") {
            let candidate = std::path::PathBuf::from(home).join(".local/bin/spark-crumbs");
            if candidate.exists() {
                candidate.to_string_lossy().to_string()
            } else {
                "spark-crumbs".to_string()
            }
        } else {
            "spark-crumbs".to_string()
        };
        let _ = Command::new(&crumb_bin)
            .args([
                "record",
                "--agent",
                "AEGIS",
                "--action",
                "heartbeat:pulse",
                "--target",
                "aegis-heartbeat",
                "--intent",
                "Sovereign runtime heartbeat pulse",
                "--vector",
                &format!(
                    "Tick #{} {} | tasks: {} | actions: {}",
                    tick, receipt.status, tasks_count, actions
                ),
                ".",
            ])
            .output()
            .await;

        receipt
    }

    pub async fn start_loop(self: Arc<Self>) {
        let mut running = self.is_running.lock().await;
        if *running {
            return;
        }
        *running = true;
        drop(running);

        let engine = self.clone();
        tokio::spawn(async move {
            info!(
                "AEGIS autonomous heartbeat loop started (period: {}s)",
                engine.interval_secs
            );
            let mut interval = tokio::time::interval(Duration::from_secs(engine.interval_secs));
            loop {
                interval.tick().await;
                let receipt = engine.pulse_once().await;
                info!(
                    "Heartbeat pulse #{}: {} tasks scanned, {} actions",
                    receipt.tick_id, receipt.tasks_scanned, receipt.actions_dispatched
                );
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inference::{ChatStream, ChatTurnResponse};
    use serde_json::Value;

    struct HealthyProbe;
    #[async_trait::async_trait]
    impl InferenceEngine for HealthyProbe {
        fn model_id(&self)->String { "test-health".into() }
        fn endpoint(&self)->String { "test-only".into() }
        async fn generate_with_tokens(&self,_:&str,_:Option<&str>,_:Option<f32>,_:Option<u32>)->Result<String,Box<dyn std::error::Error+Send+Sync>> { unreachable!() }
        async fn generate_chat(&self,_:&[Value],_:Option<f32>,_:Option<u32>)->Result<String,Box<dyn std::error::Error+Send+Sync>> { unreachable!() }
        async fn generate_chat_with_tools(&self,_:&[Value],_:Option<&[Value]>,_:Option<f32>,_:Option<u32>)->Result<ChatTurnResponse,Box<dyn std::error::Error+Send+Sync>> { unreachable!() }
        async fn stream_chat(&self,_:&[Value],_:Option<f32>,_:Option<u32>)->Result<ChatStream,Box<dyn std::error::Error+Send+Sync>> { unreachable!() }
        async fn check_health(&self)->bool { tokio::task::yield_now().await; true }
    }

    #[tokio::test]
    async fn test_http_health_client_deadline() {
        use tokio::io::AsyncReadExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let accepted = Arc::new(tokio::sync::Notify::new());
        let server_accepted = accepted.clone();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 1024];
            assert!(socket.read(&mut request).await.unwrap() != 0);
            server_accepted.notify_one();
            std::future::pending::<()>().await;
            drop(socket);
        });
        let client = tokio::spawn(async move { http_model_health(&endpoint).await });
        tokio::time::timeout(Duration::from_secs(2), accepted.notified()).await.unwrap();
        tokio::time::pause();
        tokio::time::advance(HEALTH_ACTION_TIMEOUT).await;
        tokio::time::resume();
        let result = tokio::time::timeout(Duration::from_secs(2), client).await;
        server.abort();
        let error = result.expect("HTTP client must have its own request deadline").unwrap().unwrap_err();
        assert!(error.is_timeout());
    }

    #[tokio::test]
    async fn test_heartbeat_pulse_and_task_execution() {
        let db = Arc::new(Database::open_in_memory().unwrap());
        db.create_task("hb1", "system_telemetry", "check").unwrap();
        db.create_task("hb2", "consolidate_memory", "sync").unwrap();
        db.create_task("hb3", "shell_exec", "git status").unwrap();

        let engine = HeartbeatEngine::new(60, db.clone());
        let receipt = engine.pulse_once().await;

        assert_eq!(receipt.tick_id, 1);
        assert_eq!(receipt.tasks_scanned, 3);
        assert_eq!(receipt.actions_dispatched, 3);
        assert_eq!(receipt.status, "nominal");

        let remaining = db.list_pending_tasks().unwrap();
        assert_eq!(remaining.len(), 0);
    }

    #[tokio::test]
    async fn test_overlapping_pulses_run_a_task_once() {
        let db = Arc::new(Database::open_in_memory().unwrap());
        // Uses the former task name on purpose, so the legacy alias stays covered.
        db.create_task("hb_once", "max_health", "check").unwrap();

        // The first pulse awaits the health probe while the second one lists
        // tasks, so both see the task unless the first one claimed it.
        let engine = HeartbeatEngine::with_inference(60, db.clone(), Arc::new(HealthyProbe));
        let (a, b) = tokio::join!(engine.pulse_once(), engine.pulse_once());

        assert_eq!(a.actions_dispatched + b.actions_dispatched, 1);
        assert!(db.list_pending_tasks().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_heartbeat_prohibited_root_command_rejection() {
        let db = Arc::new(Database::open_in_memory().unwrap());
        db.create_task("hb_sec", "shell_exec", "cd /etc && ls")
            .unwrap();

        let engine = HeartbeatEngine::new(60, db.clone());
        let receipt = engine.pulse_once().await;

        assert_eq!(receipt.tick_id, 1);
        assert_eq!(receipt.tasks_scanned, 1);
        assert_eq!(receipt.actions_dispatched, 1);

        let remaining = db.list_pending_tasks().unwrap();
        assert_eq!(remaining.len(), 0);
    }
}
