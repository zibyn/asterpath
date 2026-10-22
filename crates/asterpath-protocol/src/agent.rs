//! Driver-facing contracts: session spec, `AgentRunner`, live `AgentEvent`
//! stream and the summed `AgentError`.

use asterpath_core::{
    ExecutionError, InputId, LifecycleError, LogIdRange, SessionId, StepId, StorageError,
    ToolCallId, TurnId,
};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;

use crate::config::{
    CompactionConfig, ConfigSnapshot, ModelSpec, Provenance, ReplaySafety, RequestConfig,
    SideEffect, SubagentConfig, ToolDescriptor, TurnConfig,
};
use crate::event::{ToolResultStatus, TurnEndReason};
use crate::ids::{JsonString, ToolName};
use crate::item::{Part, RawProviderData};
use crate::provider::{PartDelta, ProviderError};
use crate::usage::Usage;

/// Session construction input; resolved into a persisted `ConfigSnapshot`.
#[derive(Clone, Debug, PartialEq)]
pub struct SessionSpec {
    pub model: ModelSpec,
    pub system: Vec<Part>,
    pub tools: ToolSelection,
    pub request: RequestConfig,
    pub turn: TurnConfig,
    pub compaction: CompactionConfig,
    pub subagents: SubagentConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ToolSelection {
    All,
    Names(Vec<ToolName>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct UserInput {
    pub id: InputId,
    pub parts: Vec<Part>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunRequest {
    pub session: SessionId,
    pub trigger: RunTrigger,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunTrigger {
    Input(InputId),
    Resume,
}

/// Live UI notifications. Never a substitute for the event log.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum AgentEvent {
    Idle {
        session: SessionId,
    },
    TurnStarted {
        turn: TurnId,
    },
    ModelDelta {
        turn: TurnId,
        step: StepId,
        delta: PartDelta,
    },
    ToolStarted {
        turn: TurnId,
        step: StepId,
        call: ToolCallId,
        tool: ToolName,
    },
    ToolFinished {
        turn: TurnId,
        step: StepId,
        call: ToolCallId,
        status: ToolResultStatus,
    },
    ApprovalRequested(ApprovalRequest),
    TurnCommitted {
        turn: TurnId,
        reason: TurnEndReason,
    },
}

/// One tool call awaiting a host decision.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ApprovalRequest {
    pub session: SessionId,
    pub turn: TurnId,
    pub step: StepId,
    pub tool_call_id: ToolCallId,
    pub tool: ToolName,
    pub args: JsonString,
    pub replay: ReplaySafety,
    pub side_effect: SideEffect,
    pub provenance: Provenance,
}

impl ApprovalRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session: SessionId,
        turn: TurnId,
        step: StepId,
        tool_call_id: ToolCallId,
        tool: ToolName,
        args: JsonString,
        replay: ReplaySafety,
        side_effect: SideEffect,
        provenance: Provenance,
    ) -> Self {
        Self {
            session,
            turn,
            step,
            tool_call_id,
            tool,
            args,
            replay,
            side_effect,
            provenance,
        }
    }
}

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    #[error("session {session:?} is already being driven")]
    Busy { session: SessionId },
    #[error("session {session:?} not found")]
    NotFound { session: SessionId },
    #[error("session {session:?} is corrupt: {detail}")]
    Corrupt { session: SessionId, detail: String },
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Execution(#[from] ExecutionError),
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    #[error(transparent)]
    Compaction(#[from] CompactionError),
}

#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum CompactionError {
    #[error(transparent)]
    Provider(#[from] ProviderError),
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)] // shape fixed by protocol.md §6.1
pub enum CompactResult {
    NotNeeded,
    NothingToDo,
    NotConfigured,
    Applied(CompactionOutcome),
    Insufficient,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CompactionOutcome {
    pub first_kept: asterpath_core::LogId,
    pub summary: Vec<Part>,
    pub replaces: LogIdRange,
    pub model: ModelSpec,
    pub usage: Usage,
    pub raw: Option<RawProviderData>,
}

/// Turn / step driver. The composition root injects one implementation.
pub trait AgentRunner: Send + Sync {
    fn run(&self, req: RunRequest) -> BoxStream<'_, Result<AgentEvent, AgentError>>;
    fn compact_now(&self, session: SessionId) -> BoxFuture<'_, Result<CompactResult, AgentError>>;
}

impl SessionSpec {
    /// Freeze the session configuration into its persisted snapshot. Tool
    /// selection is resolved by the caller (it owns the live registry).
    pub fn snapshot(&self, tools: Vec<ToolDescriptor>) -> ConfigSnapshot {
        ConfigSnapshot {
            model: self.model.clone(),
            system: self.system.clone(),
            tools,
            request: self.request.clone(),
            turn: self.turn.clone(),
            compaction: self.compaction.clone(),
            subagents: self.subagents.clone(),
        }
    }
}
