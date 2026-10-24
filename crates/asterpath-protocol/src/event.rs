//! The complete session event enum and the value enums its payloads carry.

use crate::{LogId, LogIdRange, SessionId, SignalId, SignalName, StepId, ToolCallId, TurnId};
use serde::{Deserialize, Serialize};

use crate::config::{ConfigSnapshot, ContextPolicy, ModelSpec, Provenance, ReplaySafety};
use crate::ids::JsonString;
use crate::ids::JsonValue;
use crate::ids::ToolName;
use crate::item::{Item, Part, RawProviderData};
use crate::provider::{ProviderError, ProviderErrorKind};
use crate::usage::Usage;

/// One committed record in a session log. Only terminal records are logged.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SessionEvent {
    SessionCreated {
        config: ConfigSnapshot,
        parent: Option<ParentRef>,
    },
    ContextForked {
        source: ParentRef,
        source_log: LogId,
        items: Vec<Item>,
    },
    StreamResumed {
        from: LogId,
        reason: ResumeReason,
    },
    InputRecorded {
        input: Vec<Part>,
    },
    TurnStarted {
        turn_id: TurnId,
    },
    StepStarted {
        turn_id: TurnId,
        step_id: StepId,
    },
    AssistantMessage {
        turn_id: TurnId,
        step_id: StepId,
        items: Vec<Item>,
        raw: Option<RawProviderData>,
        model: ModelSpec,
        usage: Usage,
        stop_reason: StopReason,
    },
    ModelCallAborted {
        turn_id: TurnId,
        step_id: StepId,
        attempt: u32,
        kind: AbortKind,
        partial: Option<Vec<Item>>,
        model: ModelSpec,
        /// Actual backoff before the next attempt; 0 when not retried.
        backoff_ms: u64,
        usage: Option<Usage>,
        /// Why the attempt failed; `None` for cancels, crashes and v1 logs.
        error: Option<ProviderErrorInfo>,
    },
    ToolCallRequested {
        turn_id: TurnId,
        step_id: StepId,
        /// Framework-generated identity; drives dedup, approval and open intents.
        tool_call_id: ToolCallId,
        /// The provider id from `ToolCall.call_id`; used to pair back to the item.
        provider_call_id: String,
        name: ToolName,
        raw_args: JsonString,
        replay: ReplaySafety,
        provenance: Provenance,
    },
    ToolResult {
        tool_call_id: ToolCallId,
        status: ToolResultStatus,
        output: Option<Vec<Part>>,
        error: Option<ToolErrorInfo>,
        truncated: bool,
    },
    StepCommitted {
        turn_id: TurnId,
        step_id: StepId,
    },
    TurnCommitted {
        turn_id: TurnId,
        reason: TurnEndReason,
    },
    CompactionStarted {
        trigger: CompactionTrigger,
        replaces_from: LogId,
        caused_by: LogId,
        /// The turn the compaction runs in; `None` for an Idle `compact_now`.
        turn: Option<TurnId>,
    },
    CompactionApplied {
        first_kept: LogId,
        summary: Vec<Part>,
        replaces: LogIdRange,
        model: ModelSpec,
        usage: Usage,
        raw: Option<RawProviderData>,
    },
    CompactionAborted {
        reason: CompactionAbortReason,
    },
    ChildSessionSpawned {
        child: SessionId,
        task: Vec<Part>,
        config: ConfigSnapshot,
        context: ContextPolicy,
        parent_tool_call_id: ToolCallId,
        caused_by: LogId,
        /// Absolute spawn-timeout deadline, written with the spawn record.
        deadline: Option<crate::Timestamp>,
    },
    ChildSessionCompleted {
        child: SessionId,
        outcome: ChildOutcome,
    },
    SignalReceived {
        signal_id: SignalId,
        name: SignalName,
        payload: JsonValue,
    },
}

impl SessionEvent {
    /// The persistent variant name. The storage layer derives its idempotency
    /// discriminator from this, so it must never change once released.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::SessionCreated { .. } => "SessionCreated",
            Self::ContextForked { .. } => "ContextForked",
            Self::StreamResumed { .. } => "StreamResumed",
            Self::InputRecorded { .. } => "InputRecorded",
            Self::TurnStarted { .. } => "TurnStarted",
            Self::StepStarted { .. } => "StepStarted",
            Self::AssistantMessage { .. } => "AssistantMessage",
            Self::ModelCallAborted { .. } => "ModelCallAborted",
            Self::ToolCallRequested { .. } => "ToolCallRequested",
            Self::ToolResult { .. } => "ToolResult",
            Self::StepCommitted { .. } => "StepCommitted",
            Self::TurnCommitted { .. } => "TurnCommitted",
            Self::CompactionStarted { .. } => "CompactionStarted",
            Self::CompactionApplied { .. } => "CompactionApplied",
            Self::CompactionAborted { .. } => "CompactionAborted",
            Self::ChildSessionSpawned { .. } => "ChildSessionSpawned",
            Self::ChildSessionCompleted { .. } => "ChildSessionCompleted",
            Self::SignalReceived { .. } => "SignalReceived",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentRef {
    pub session: SessionId,
    pub turn: TurnId,
    pub tool_call: ToolCallId,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResumeReason {
    Crash,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AbortKind {
    Transport,
    Rejected,
    Cancelled,
    Crash,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionTrigger {
    Threshold,
    Manual,
    ProviderError,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionAbortReason {
    SummaryFailed,
    Insufficient,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolResultStatus {
    Completed,
    Failed,
    Interrupted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolErrorInfo {
    pub kind: ToolErrorKind,
    pub message: String,
}

/// The loggable part of a `ProviderError`; `raw` stays out of the log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderErrorInfo {
    pub kind: ProviderErrorKind,
    pub message: String,
    pub request_id: Option<String>,
}

impl From<&ProviderError> for ProviderErrorInfo {
    fn from(error: &ProviderError) -> Self {
        Self {
            kind: error.kind,
            message: error.message.clone(),
            request_id: error.request_id.clone(),
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ToolErrorKind {
    InvalidArgs,
    Denied,
    Timeout,
    Cancelled,
    Panic,
    Transport,
    Internal,
    NotFound,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TurnEndReason {
    Completed,
    Cancelled,
    Failed,
    Stopped { reason: String },
    MaxStepsExceeded,
    ContextExhausted,
    CompactionFailed,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StopReason {
    EndTurn,
    ToolUse,
    MaxTokens,
    StopSequence,
    PauseTurn,
    ContextWindowExceeded,
    Refusal,
    Other { provider_label: String },
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ChildOutcome {
    Completed {
        summary: Summary,
    },
    Failed {
        reason: ChildFailure,
        summary: Summary,
    },
    Cancelled {
        reason: CancelReason,
    },
    Lost {
        reason: String,
    },
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChildFailure {
    TurnFailed,
    ContextExhausted,
    MaxStepsExceeded,
    CompactionFailed,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CancelReason {
    Parent,
    Host,
    Timeout,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    /// Text/refusal parts of the child's last committed assistant message.
    pub items: Vec<Part>,
}
