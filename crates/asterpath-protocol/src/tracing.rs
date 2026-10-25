//! Native execution tracing contract: the value model of a deterministic
//! projection over one session's event log.
//!
//! A trace is not stored separately. `TraceQuery` folds the injected `SessionStore`
//! on demand; `TraceTap` pushes the same lifecycle from the engine append entry.
//! Content never enters a span: only `Span.log_range` references the log.

use crate::{LogId, LogIdRange, SessionId, StepId, Timestamp, ToolCallId, TurnId};
use serde::{Deserialize, Serialize};

use crate::config::{ModelSpec, Provenance, ReplaySafety, SideEffect};
use crate::event::{
    AbortKind, ChildOutcome, CompactionTrigger, ProviderErrorInfo, ResumeReason, StopReason,
};
use crate::ids::ToolName;
use crate::usage::Usage;

/// One trace per session: `TraceId = SessionId`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TraceId(SessionId);

impl TraceId {
    pub const fn from_session(session: SessionId) -> Self {
        Self(session)
    }

    pub const fn session(self) -> SessionId {
        self.0
    }
}

impl std::fmt::Debug for TraceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TraceId({})", self.0.to_hex())
    }
}

/// Identity derived from the record that opened the span; stable across rebuilds.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SpanId([u8; 8]);

impl SpanId {
    pub const fn from_bytes(bytes: [u8; 8]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 8] {
        &self.0
    }

    pub fn to_hex(self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

impl std::fmt::Debug for SpanId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SpanId({})", self.to_hex())
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SpanKind {
    Session,
    Turn,
    Step,
    ModelCall,
    ToolCall,
    Subagent,
    Wait,
    Compaction,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum WaitKind {
    Approval,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SpanStatus {
    Unset,
    Ok,
    Error,
    Cancelled,
    Interrupted,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ErrorType {
    ProviderRejected,
    Transport,
    ToolInvalidArgs,
    ToolDenied,
    ToolTimeout,
    ToolPanic,
    ToolCancelled,
    ToolNotFound,
    ContextExhausted,
    CompactionFailed,
    MaxSteps,
    Refused,
    Internal,
    Other,
}

/// One logical operation with a start, an optional terminal and a status.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Span {
    pub trace: TraceId,
    pub span: SpanId,
    /// Parent inside the same trace.
    pub parent_span: Option<SpanId>,
    /// Root span of a child session: the parent trace (`TraceId = SessionId`).
    pub parent_trace: Option<TraceId>,
    /// A parent trace's subagent span: the independent child trace.
    pub child_trace: Option<TraceId>,
    pub kind: SpanKind,
    pub name: String,
    pub session: SessionId,
    pub turn: Option<TurnId>,
    pub step: Option<StepId>,
    pub tool_call: Option<ToolCallId>,
    pub started_at: Timestamp,
    /// `None` while the span is open at the frontier.
    pub ended_at: Option<Timestamp>,
    pub status: SpanStatus,
    pub error_type: Option<ErrorType>,
    /// Audit/content reference; never the content itself.
    pub log_range: LogIdRange,
    /// Redone work after a `StreamResumed`.
    pub replay: bool,
    /// Set only when a new span represents the same logical operation.
    pub replay_of: Option<SpanId>,
    pub attributes: SpanAttributes,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SpanAttributes {
    Session(SessionAttrs),
    Turn(TurnAttrs),
    /// A step carries no additional attributes.
    Step,
    ModelCall(ModelCallAttrs),
    ToolCall(ToolCallAttrs),
    Subagent(SubagentAttrs),
    Wait(WaitAttrs),
    Compaction(CompactionAttrs),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionAttrs {
    pub parent_session: Option<SessionId>,
    pub fork_source_log: Option<LogId>,
    /// `StreamResumed` records, in log order.
    pub resumes: Vec<ResumeEvent>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnAttrs {
    pub index: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCallAttrs {
    pub provider: String,
    pub request_model: ModelSpec,
    pub response_model: Option<String>,
    pub response_id: Option<String>,
    pub finish_reasons: Vec<StopReason>,
    /// Only the successful response contributes.
    pub usage: Option<Usage>,
    pub attempt_count: u32,
    pub compacted: bool,
    /// Failed transport attempts folded into this span, in log order.
    pub attempts: Vec<AttemptEvent>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallAttrs {
    pub name: ToolName,
    pub side_effect: SideEffect,
    pub replay: ReplaySafety,
    pub provenance: Provenance,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SubagentAttrs {
    pub child: SessionId,
    pub outcome: Option<ChildOutcome>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaitAttrs {
    pub wait: WaitKind,
    pub caused_by: Option<LogId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionAttrs {
    pub trigger: CompactionTrigger,
    pub replaces: Option<LogIdRange>,
    pub model: Option<ModelSpec>,
    pub usage: Option<Usage>,
}

/// One failed transport attempt folded into its logical model call.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptEvent {
    /// 1-based attempt number.
    pub attempt: u32,
    pub kind: AbortKind,
    pub error_type: ErrorType,
    /// Backoff before this attempt; 0 for the first.
    pub backoff_before_ms: u64,
    /// Diagnostic only; never added to the span's usage total.
    pub partial_usage: Option<Usage>,
    /// This attempt happened after a `StreamResumed`.
    pub replayed: bool,
    /// The provider's error, when the log recorded one.
    pub error: Option<ProviderErrorInfo>,
}

/// A `StreamResumed` record carried as an event on the session span.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeEvent {
    pub from: LogId,
    pub reason: ResumeReason,
    pub at: Timestamp,
}

/// All fields are `Option` and combine with AND. `session` selects the trace.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SpanFilter {
    pub session: Option<SessionId>,
    pub turn: Option<TurnId>,
    pub step: Option<StepId>,
    pub tool_call: Option<ToolCallId>,
    pub kind: Option<SpanKind>,
    pub status: Option<SpanStatus>,
    pub open_only: bool,
    pub limit: Option<usize>,
}

#[non_exhaustive]
#[derive(Clone, Debug, thiserror::Error)]
pub enum TraceError {
    /// The session was retired or never existed.
    #[error("session {session:?} not found")]
    NotFound { session: SessionId },
    #[error("session {session:?} trace is corrupt: {detail}")]
    Corrupt { session: SessionId, detail: String },
    #[error("trace backend error: {0}")]
    Backend(String),
}
