//! Rebuild a workflow's state after a restart by asking the authoritative
//! records (G3, issue #56; authorities named in `docs/DELEGATED-DAG.md`).
//!
//! AEGIS keeps only pointers: per task, the effect intent id, the grant id,
//! the receipt digest and, for a no-effect task, its result digest. The
//! state of each task is derived from what the authorities say now, never
//! from a saved copy. Anything not known stays `Uncertain` and is never run
//! again; a missing pointer or an unreachable authority is a named refusal,
//! never a silent fresh start.
//!
//! First cut: the lookup is a trait with in-memory fakes in tests. There is
//! no storage, no sovereign-core client and no network here. The real
//! adapter reads the sovereign-core effect ledger
//! (`crates/aien-runtime/src/effects.rs`) and needs
//! aien-dev/aien-sovereign-core-mv#379; this module does not reimplement it.

use crate::failure::{EffectCertainty, FailureClass, FailureRecord};
use crate::orchestration::workflow::{
    ResultHandle, TaskId, TaskState, WorkflowSnapshot, WorkflowSpec,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;

/// Tool name stamped on failure records produced by a rebuild.
pub const RECOVERY_TOOL_NAME: &str = "workflow_recovery";

macro_rules! opaque_id {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(id: impl Into<String>) -> Self {
                Self(id.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
    };
}

opaque_id!(
    /// Names one effect intent record in the sovereign-core effect ledger.
    EffectIntentId
);
opaque_id!(
    /// Names one grant at the Spark approval desk. Carries no authority.
    GrantId
);
opaque_id!(
    /// Digest of a receipt held by the authority.
    ReceiptDigest
);

/// Effect state as the sovereign-core effect ledger names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EffectState {
    Open,
    Done,
    NotDone,
    Unresolved,
}

impl EffectState {
    pub fn name(self) -> &'static str {
        match self {
            Self::Open => "OPEN",
            Self::Done => "DONE",
            Self::NotDone => "NOT_DONE",
            Self::Unresolved => "UNRESOLVED",
        }
    }
}

/// What the desk says about a grant now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GrantStatus {
    /// Unspent and still valid.
    Available,
    /// Spent on an effect intent.
    Spent,
    Expired,
    Revoked,
}

/// One grant as the desk reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GrantRecord {
    pub status: GrantStatus,
    /// The effect intent this grant opened, when the desk can name it.
    pub intent: Option<EffectIntentId>,
}

/// The work class of a task, as PST1 `effect_class` names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectClass {
    /// Touches nothing outside; safe to run again.
    None,
    /// Computes only; safe to run again.
    Compute,
    /// Changes something in the world; needs a grant.
    Effect,
}

/// The pointers AEGIS keeps for one task (DELEGATED-DAG.md section 2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskPointers {
    pub effect_class: EffectClass,
    pub effect_intent: Option<EffectIntentId>,
    pub grant: Option<GrantId>,
    pub receipt: Option<ReceiptDigest>,
    /// Digest of a no-effect task's result, once it finished.
    pub result_digest: Option<String>,
}

impl TaskPointers {
    /// A no-effect task that has not finished.
    pub fn no_effect() -> Self {
        Self {
            effect_class: EffectClass::None,
            effect_intent: None,
            grant: None,
            receipt: None,
            result_digest: None,
        }
    }
}

/// Every pointer AEGIS keeps for one workflow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredPointers {
    pub run_id: String,
    /// Copied from the Pi; never minted here.
    pub cause_id: String,
    pub objective_id: String,
    /// `spec_digest` of the spec this workflow was started with.
    pub spec_digest: String,
    pub tasks: BTreeMap<TaskId, TaskPointers>,
}

/// The lookup could not reach its authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupError {
    pub detail: String,
}

impl LookupError {
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

/// Read-only questions to the authorities. Implementations must not
/// change anything they read.
#[async_trait]
pub trait AuthorityLookup: Send + Sync {
    /// `Ok(None)`: the ledger holds no such intent.
    async fn effect_state(
        &self,
        intent: &EffectIntentId,
    ) -> Result<Option<EffectState>, LookupError>;
    /// `Ok(None)`: the desk never issued this grant.
    async fn grant(&self, grant: &GrantId) -> Result<Option<GrantRecord>, LookupError>;
    async fn receipt_present(&self, receipt: &ReceiptDigest) -> Result<bool, LookupError>;
}

/// Why a rebuild was refused. Nothing is started after a refusal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryRefusal {
    /// The spec handed in is not the one the pointers were stored for.
    SpecMismatch {
        stored: String,
        rebuilt: String,
    },
    /// A task of the spec has no pointers, or an effect task has no grant.
    MissingPointers(TaskId),
    /// Pointers name a task the spec does not have.
    UnknownTaskPointer(TaskId),
    LookupUnavailable {
        task: TaskId,
        detail: String,
    },
    /// A pointer names an effect intent the ledger does not hold.
    EffectRecordMissing {
        task: TaskId,
        intent: EffectIntentId,
    },
    /// A pointer names a grant the desk never issued.
    GrantUnknown {
        task: TaskId,
        grant: GrantId,
    },
    /// The stored intent and the desk's intent for the grant differ.
    IntentMismatch {
        task: TaskId,
        stored: EffectIntentId,
        desk: EffectIntentId,
    },
    /// The ledger says DONE but no receipt exists for it.
    DoneWithoutReceipt {
        task: TaskId,
        intent: EffectIntentId,
    },
}

impl RecoveryRefusal {
    pub fn name(&self) -> &'static str {
        match self {
            Self::SpecMismatch { .. } => "RECOVERY_SPEC_MISMATCH",
            Self::MissingPointers(_) => "RECOVERY_MISSING_POINTERS",
            Self::UnknownTaskPointer(_) => "RECOVERY_UNKNOWN_TASK_POINTER",
            Self::LookupUnavailable { .. } => "RECOVERY_LOOKUP_UNAVAILABLE",
            Self::EffectRecordMissing { .. } => "RECOVERY_EFFECT_RECORD_MISSING",
            Self::GrantUnknown { .. } => "RECOVERY_GRANT_UNKNOWN",
            Self::IntentMismatch { .. } => "RECOVERY_INTENT_MISMATCH",
            Self::DoneWithoutReceipt { .. } => "RECOVERY_DONE_WITHOUT_RECEIPT",
        }
    }
}

impl fmt::Display for RecoveryRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {self:?}", self.name())
    }
}

impl std::error::Error for RecoveryRefusal {}

/// SHA-256 over the task ids and their edges, each sorted, so insertion
/// order does not matter and any changed edge does.
pub fn spec_digest(spec: &WorkflowSpec) -> String {
    let mut lines: Vec<String> = spec
        .task_ids()
        .into_iter()
        .map(|id| {
            let mut deps: Vec<&str> = spec
                .dependencies(&id)
                .unwrap_or_default()
                .iter()
                .map(|d| d.as_str())
                .collect();
            deps.sort_unstable();
            format!("{}\t{}\n", id.as_str(), deps.join(","))
        })
        .collect();
    lines.sort_unstable();
    let digest = Sha256::digest(lines.concat().as_bytes());
    let mut out = String::from("sha256:");
    for b in digest {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// What the records alone say about one task, before dependencies.
enum Derived {
    Done,
    /// Never finished and safe to start (no effect, or a valid grant).
    Runnable,
    /// Needs a grant that is still valid or newly issued.
    NeedsGrant,
    Uncertain(String),
}

/// Rebuilds the snapshot from the pointers and the authorities. Pure: the
/// same records give the same snapshot (timestamps in records are 0). No
/// task body is ever called.
pub async fn rebuild(
    spec: &WorkflowSpec,
    pointers: &StoredPointers,
    lookup: &dyn AuthorityLookup,
) -> Result<WorkflowSnapshot, RecoveryRefusal> {
    let rebuilt = spec_digest(spec);
    if rebuilt != pointers.spec_digest {
        return Err(RecoveryRefusal::SpecMismatch {
            stored: pointers.spec_digest.clone(),
            rebuilt,
        });
    }
    let ids = spec.task_ids();
    if let Some(id) = ids.iter().find(|i| !pointers.tasks.contains_key(*i)) {
        return Err(RecoveryRefusal::MissingPointers(id.clone()));
    }
    if let Some(id) = pointers.tasks.keys().find(|k| !ids.contains(k)) {
        return Err(RecoveryRefusal::UnknownTaskPointer(id.clone()));
    }

    let mut derived: BTreeMap<TaskId, Derived> = BTreeMap::new();
    let mut results: BTreeMap<TaskId, ResultHandle> = BTreeMap::new();
    for id in &ids {
        let p = &pointers.tasks[id];
        let d = match p.effect_class {
            EffectClass::None | EffectClass::Compute => match &p.result_digest {
                Some(digest) => {
                    results.insert(
                        id.clone(),
                        ResultHandle {
                            digest: digest.clone(),
                        },
                    );
                    Derived::Done
                }
                None => Derived::Runnable,
            },
            EffectClass::Effect => derive_effect(id, p, lookup).await?,
        };
        derived.insert(id.clone(), d);
    }

    let mut states: BTreeMap<TaskId, TaskState> = BTreeMap::new();
    for id in &ids {
        let deps_done = spec
            .dependencies(id)
            .unwrap_or_default()
            .iter()
            .all(|d| matches!(derived[d], Derived::Done));
        let state = match &derived[id] {
            Derived::Done => TaskState::Completed,
            Derived::Uncertain(why) => {
                TaskState::Uncertain(uncertain_record(&pointers.run_id, id, why))
            }
            Derived::Runnable if deps_done => TaskState::Ready,
            Derived::NeedsGrant if deps_done => TaskState::WaitingForApproval,
            Derived::Runnable | Derived::NeedsGrant => TaskState::Pending,
        };
        states.insert(id.clone(), state);
    }
    Ok(WorkflowSnapshot {
        run_id: pointers.run_id.clone(),
        last_seq: 0,
        states,
        results,
    })
}

async fn derive_effect(
    id: &TaskId,
    p: &TaskPointers,
    lookup: &dyn AuthorityLookup,
) -> Result<Derived, RecoveryRefusal> {
    let unavailable = |e: LookupError| RecoveryRefusal::LookupUnavailable {
        task: id.clone(),
        detail: e.detail,
    };
    let grant_id = p
        .grant
        .as_ref()
        .ok_or_else(|| RecoveryRefusal::MissingPointers(id.clone()))?;
    let grant = lookup
        .grant(grant_id)
        .await
        .map_err(unavailable)?
        .ok_or_else(|| RecoveryRefusal::GrantUnknown {
            task: id.clone(),
            grant: grant_id.clone(),
        })?;
    let intent = match (&p.effect_intent, &grant.intent) {
        (Some(stored), Some(desk)) if stored != desk => {
            return Err(RecoveryRefusal::IntentMismatch {
                task: id.clone(),
                stored: stored.clone(),
                desk: desk.clone(),
            })
        }
        (Some(stored), _) => Some(stored.clone()),
        (None, desk) => desk.clone(),
    };
    let Some(intent) = intent else {
        return Ok(match grant.status {
            GrantStatus::Available => Derived::Runnable,
            // Spent, but nobody can name the effect: it may have happened.
            GrantStatus::Spent => {
                Derived::Uncertain("grant spent; no effect intent is named".into())
            }
            GrantStatus::Expired | GrantStatus::Revoked => Derived::NeedsGrant,
        });
    };
    let state = lookup
        .effect_state(&intent)
        .await
        .map_err(unavailable)?
        .ok_or_else(|| RecoveryRefusal::EffectRecordMissing {
            task: id.clone(),
            intent: intent.clone(),
        })?;
    Ok(match state {
        EffectState::Done => {
            let done_without_receipt = || RecoveryRefusal::DoneWithoutReceipt {
                task: id.clone(),
                intent: intent.clone(),
            };
            let receipt = p.receipt.as_ref().ok_or_else(done_without_receipt)?;
            if !lookup.receipt_present(receipt).await.map_err(unavailable)? {
                return Err(done_without_receipt());
            }
            Derived::Done
        }
        EffectState::Open | EffectState::Unresolved => {
            Derived::Uncertain(format!("effect ledger says {}", state.name()))
        }
        EffectState::NotDone if grant.status == GrantStatus::Available => Derived::Runnable,
        EffectState::NotDone => Derived::NeedsGrant,
    })
}

fn uncertain_record(run_id: &str, id: &TaskId, why: &str) -> FailureRecord {
    FailureRecord::new(
        0,
        run_id,
        0,
        id.to_string(),
        None,
        RECOVERY_TOOL_NAME,
        1,
        FailureClass::Uncertain,
        EffectCertainty::Uncertain,
        0,
        0,
    )
    .with_cause("recovery", why, None)
}
