//! Driver-facing values: session spec, run request, live `AgentEvent`
//! stream and the summed `AgentError`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use crate::{
    ExecutionError, InputId, LogIdRange, SessionId, StepId, StorageError, ToolCallId, TurnId,
};

use crate::config::{
    CompactionConfig, ConfigPatch, ConfigSnapshot, ModelSpec, Provenance, ReplaySafety,
    RequestConfig, SideEffect, SubagentConfig, ToolDescriptor, TurnConfig,
};
use crate::event::{ToolResultStatus, TurnEndReason};
use crate::ids::{JsonString, ToolName};
use crate::item::{Part, RawProviderData};
use crate::provider::{PartDelta, ProviderError};
use crate::tool::CancelToken;
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

/// A host change to a running session's configuration, validated by
/// `SessionHandle::update_config` and recorded as one `ConfigChanged`.
/// Each present field replaces the effective value whole.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConfigUpdate {
    pub model: Option<ModelSpec>,
    pub request: Option<RequestConfig>,
}

impl ConfigUpdate {
    /// The patch recorded for this update.
    pub fn patch(&self) -> ConfigPatch {
        let mut patch = ConfigPatch::default();
        patch.model = self.model.clone();
        patch.request = self.request.clone();
        patch
    }
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
///
/// The JSON shape is a Tier 1 promise: tag `type`, snake_case variant names.
/// Within a step, `(step, output_index)` identifies one streamed block.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
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
        output_index: u32,
        part_index: u32,
        delta: PartDelta,
    },
    ToolStarted {
        turn: TurnId,
        step: StepId,
        output_index: u32,
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
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
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
    pub first_kept: crate::LogId,
    pub summary: Vec<Part>,
    pub replaces: LogIdRange,
    pub model: ModelSpec,
    pub usage: Usage,
    pub raw: Option<RawProviderData>,
}

/// Process-local map from a driven session to its active turn token.
///
/// The facade and the agent share one registry, so `SessionHandle`
/// cancellation reaches the active drive.
#[derive(Default)]
pub struct CancellationRegistry {
    active: Mutex<HashMap<SessionId, CancelToken>>,
}

impl CancellationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a fresh turn token. The returned guard clears the entry when
    /// the drive ends; sessions are single-flight, so entries never stack.
    pub fn register(self: &Arc<Self>, session: SessionId) -> CancelRegistration {
        let token = CancelToken::new();
        self.active
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(session, token.clone());
        CancelRegistration {
            registry: Arc::clone(self),
            session,
            token,
        }
    }

    /// Cancel the active turn, if any. Idle sessions are a no-op.
    pub fn cancel(&self, session: SessionId) {
        if let Some(token) = self
            .active
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&session)
            .cloned()
        {
            token.cancel();
        }
    }
}

/// RAII registration for one driven turn.
pub struct CancelRegistration {
    registry: Arc<CancellationRegistry>,
    session: SessionId,
    token: CancelToken,
}

impl CancelRegistration {
    pub fn token(&self) -> CancelToken {
        self.token.clone()
    }
}

impl Drop for CancelRegistration {
    fn drop(&mut self) {
        self.registry
            .active
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.session);
    }
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
