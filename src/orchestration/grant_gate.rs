//! Grant gate for effectful workflow tasks (issue #59, lane G6).
//!
//! A task that changes the world starts only under a grant from the Spark
//! approval desk that is bound to this request's objective id and cause id.
//! The grant is checked when the task starts and again immediately before
//! the effect, after any preparation. A missing grant asks for approval; a
//! stale, foreign, forged or unbound one is refused with a named reason and
//! no effect.
//!
//! The desk is a trait. The authority is the durable, objective-bound grant
//! in the sovereign-core compose ledger (docs/DELEGATED-DAG.md, proposed
//! decision D3); no client to it exists yet (sovereign-core#379), so tests
//! use a fake. The gate checks; it does not spend. Spending the grant
//! belongs to the effect ledger when it opens the effect intent, so there
//! is one authority for that fact.
//!
//! Not here yet: binding the grant to the INTERPLANE canonical request
//! digest (item 3, after aien-protocols-mv#33) and the real desk adapter
//! (item 4).

use crate::failure::{EffectCertainty, FailureClass, ToolFault};
use crate::orchestration::recovery::{GrantId, GrantStatus, LookupError};
use crate::orchestration::workflow::{TaskContext, TaskOutcome, TaskRunner};
use async_trait::async_trait;
use std::fmt;
use std::sync::Arc;

/// What the desk holds for one grant id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeskGrant {
    pub status: GrantStatus,
    pub objective_id: String,
    pub cause_id: String,
}

/// Read access to the approval desk.
#[async_trait]
pub trait GrantDesk: Send + Sync {
    /// `Ok(None)` means the desk never issued this id.
    async fn grant(&self, id: &GrantId) -> Result<Option<DeskGrant>, LookupError>;
}

/// The request this task belongs to, and the grant id it was handed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantBinding {
    pub objective_id: String,
    /// Copied from the Pi; never minted here.
    pub cause_id: String,
    pub grant: Option<GrantId>,
}

/// Why the gate stopped the task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantRefusal {
    /// The binding has no objective id or no cause id, so nothing can match.
    Unbound,
    /// No grant id was handed over: the task waits for approval.
    NoGrant,
    /// The desk never issued this id.
    Forged {
        grant: GrantId,
    },
    /// Spent, expired or revoked.
    Stale {
        grant: GrantId,
        status: GrantStatus,
    },
    ForeignObjective {
        grant: GrantId,
    },
    ForeignCause {
        grant: GrantId,
    },
    DeskUnavailable {
        detail: String,
    },
}

impl GrantRefusal {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Unbound => "GRANT_GATE_UNBOUND",
            Self::NoGrant => "GRANT_GATE_NO_GRANT",
            Self::Forged { .. } => "GRANT_GATE_FORGED",
            Self::Stale { .. } => "GRANT_GATE_STALE",
            Self::ForeignObjective { .. } => "GRANT_GATE_FOREIGN_OBJECTIVE",
            Self::ForeignCause { .. } => "GRANT_GATE_FOREIGN_CAUSE",
            Self::DeskUnavailable { .. } => "GRANT_GATE_DESK_UNAVAILABLE",
        }
    }

    /// What the workflow records. A missing grant is a wait for a human,
    /// not a failure; every other refusal is a no-effect failure.
    pub fn outcome(&self) -> TaskOutcome {
        let class = match self {
            Self::NoGrant => return TaskOutcome::ApprovalPending,
            Self::DeskUnavailable { .. } => FailureClass::Unavailable,
            _ => FailureClass::Rejected,
        };
        TaskOutcome::Failed(ToolFault::new(
            class,
            EffectCertainty::NoEffect,
            self.to_string(),
        ))
    }
}

impl fmt::Display for GrantRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unbound => write!(f, "{}: binding has no objective or cause", self.name()),
            Self::NoGrant => write!(f, "{}: no grant id", self.name()),
            Self::Forged { grant }
            | Self::ForeignObjective { grant }
            | Self::ForeignCause { grant } => write!(f, "{}: {grant}", self.name()),
            Self::Stale { grant, status } => write!(f, "{}: {grant} is {status:?}", self.name()),
            Self::DeskUnavailable { detail } => write!(f, "{}: {detail}", self.name()),
        }
    }
}

impl std::error::Error for GrantRefusal {}

/// One check against the desk. Returns the grant id the effect may run
/// under.
pub async fn check(desk: &dyn GrantDesk, b: &GrantBinding) -> Result<GrantId, GrantRefusal> {
    if b.objective_id.is_empty() || b.cause_id.is_empty() {
        return Err(GrantRefusal::Unbound);
    }
    let id = b.grant.clone().ok_or(GrantRefusal::NoGrant)?;
    let g = desk
        .grant(&id)
        .await
        .map_err(|e| GrantRefusal::DeskUnavailable { detail: e.detail })?
        .ok_or_else(|| GrantRefusal::Forged { grant: id.clone() })?;
    if g.objective_id != b.objective_id {
        return Err(GrantRefusal::ForeignObjective { grant: id });
    }
    if g.cause_id != b.cause_id {
        return Err(GrantRefusal::ForeignCause { grant: id });
    }
    if g.status != GrantStatus::Available {
        return Err(GrantRefusal::Stale {
            grant: id,
            status: g.status,
        });
    }
    Ok(id)
}

/// An effectful task body, split so the gate can check between the parts.
#[async_trait]
pub trait EffectBody: Send + Sync {
    /// Work that changes nothing outside (building the request, reading).
    /// An `Err` ends the task with that outcome and no effect.
    async fn prepare(&self, _ctx: &TaskContext) -> Result<(), TaskOutcome> {
        Ok(())
    }

    /// The effect, under the grant the gate just checked.
    async fn apply(&self, ctx: TaskContext, grant: GrantId) -> TaskOutcome;

    /// Passed through to the workflow on timeout or cancel.
    fn certainty_on_interrupt(&self) -> EffectCertainty {
        EffectCertainty::Uncertain
    }
}

/// Wraps an effectful body: check, prepare, check again, apply.
pub struct GrantGate {
    desk: Arc<dyn GrantDesk>,
    binding: GrantBinding,
    body: Arc<dyn EffectBody>,
}

impl GrantGate {
    pub fn new(
        desk: Arc<dyn GrantDesk>,
        binding: GrantBinding,
        body: Arc<dyn EffectBody>,
    ) -> Arc<Self> {
        Arc::new(Self {
            desk,
            binding,
            body,
        })
    }
}

#[async_trait]
impl TaskRunner for GrantGate {
    async fn run(&self, ctx: TaskContext) -> TaskOutcome {
        if let Err(r) = check(self.desk.as_ref(), &self.binding).await {
            return r.outcome();
        }
        if let Err(out) = self.body.prepare(&ctx).await {
            return out;
        }
        match check(self.desk.as_ref(), &self.binding).await {
            Ok(grant) => self.body.apply(ctx, grant).await,
            Err(r) => r.outcome(),
        }
    }

    fn certainty_on_interrupt(&self) -> EffectCertainty {
        self.body.certainty_on_interrupt()
    }
}
