pub mod agent;
pub mod auth;
pub mod defense;
pub mod durability;
pub mod events;
pub mod execution;
pub mod failure;
pub mod gateway;
pub mod heartbeat;
pub mod inference;
pub mod orchestration;
pub mod persistence;
pub mod security;
pub mod sessions;
pub mod skills;
pub mod vault;
pub mod vector_math;

pub use agent::{AgentEngine, AgentExecutionResult, AgentStep, FailurePolicy, ToolExecutionRecord};
pub use auth::{resolve_bind_addr, OperatorAuth};
pub use defense::{Approval, ApprovalStatus, ContainmentLevel, DefenseReceipt, DoctrineDecision};
pub use events::{AegisEvent, EventBus, EventEnvelope, EventId, EventSubscriber};
pub use execution::{Action, ActionReceipt, ActionRequest, ActionStatus, ExecutionAuthority};
pub use failure::{
    retry_eligibility, root_cause, skill_effect_profile, AttemptFailure, CancelToken, CauseEntry,
    EffectCertainty, FailureClass, FailureRecord, RetryEligibility, RetryPolicy, ToolDispatcher,
    ToolFault,
};
pub use gateway::{create_router, start_gateway, GatewayState, HealthResponse};
pub use heartbeat::{HeartbeatEngine, PulseReceipt};
pub use inference::{
    ChatStream, ChatTurnResponse, EmbeddedInferenceBackend, HttpInferenceBackend, InferenceEngine,
    ProtocolInferenceBackend, ToolCallFunction, ToolCallItem,
};
pub use orchestration::workflow::{
    TaskId, TaskOutcome, TaskRunner, TaskState, WorkflowConfig, WorkflowError, WorkflowExecutor,
    WorkflowReport, WorkflowSpec, WorkflowStatus,
};
pub use orchestration::{Run, RunBudget, RunState, TerminationReason, Trigger};
pub use persistence::{
    AsyncDatabase, CrumbRecord, Database, PersistenceError, SqliteActionStore, SqliteApprovalStore,
    SqliteMessageStore, SqliteRunStore, SqliteSessionStore, TaskRecord, TurnRecord,
};
pub use security::{SecurityError, WorkspaceCapability};
pub use sessions::{
    ActionId, ActionStore, ApprovalId, ApprovalStore, AssistantMessage, Message, MessageId,
    MessageStore, Role, RunId, RunStore, Session, SessionBudget, SessionId, SessionStatus,
    SessionStore, SystemMessage, ToolResultMessage, UserMessage,
};
pub use skills::{SkillDefinition, SkillExecutionRequest, SkillExecutionResponse, SkillRegistry};
pub use vault::{VaultResolver, REDACTED_MARKER};
pub use vector_math::VectorMath;

// Re-export Canonical AIEN Agent State ABI and Protocol Types
pub use aien_agent_state_abi::{
    AgentEvent, AgentIdentity, AgentState, AgentStateEvent, ContextState, ExecutionState,
    Objective, ResourceBudget, SessionState, StateRef,
};
pub use aien_protocol_types::{AgentId, Digest32, ProtocolVersion, SequenceNumber, Timestamp};

pub mod enforcement;
pub mod policy_guard;
pub use enforcement::{pre_dispatch_check, probe_threshold_from_env};
pub use policy_guard::ProbePolicyGuard;
