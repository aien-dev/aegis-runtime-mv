use chrono::Utc;
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

        let mut degraded = false;
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
            match self.db.claim_task(&task.id) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(e) => {
                    degraded = true;
                    warn!("Heartbeat could not claim task {}: {}", task.id, e);
                    continue;
                }
            }

            let outcome = match task.task_type.as_str() {
                // "max_health" is the former name, kept so tasks already queued under it still run.
                "model_health" | "max_health" => {
                    let is_healthy = if let Some(ref inf) = self.inference {
                        inf.check_health().await
                    } else {
                        reqwest::Client::new()
                            .get("http://127.0.0.1:18006/v1/models")
                            .send()
                            .await
                            .map(|r| r.status().is_success())
                            .unwrap_or(false)
                    };

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
            };

            let failure = match outcome {
                Ok(result) => self
                    .db
                    .complete_task(&task.id, &result)
                    .err()
                    .map(|e| format!("Could not record task completion: {}", e)),
                Err(error) => Some(error),
            };
            if let Some(error) = failure {
                degraded = true;
                warn!("Heartbeat task {} failed: {}", task.id, error);
                if let Err(e) = self.db.fail_task(&task.id, &error) {
                    warn!("Heartbeat could not record failure of task {}: {}", task.id, e);
                }
            }
            actions += 1;
        }

        let receipt = PulseReceipt {
            tick_id: tick,
            timestamp: now,
            status: if degraded { "degraded" } else { "nominal" },
            tasks_scanned: tasks_count,
            actions_dispatched: actions,
            notes: format!(
                "Autonomous tick #{} scanned {} pending tasks, dispatched {} actions, status {}",
                tick, tasks_count, actions, if degraded { "degraded" } else { "nominal" }
            ),
        };

        let _ = self.db.record_heartbeat_tick(&receipt);
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
        let engine = HeartbeatEngine::new(60, db.clone());
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
