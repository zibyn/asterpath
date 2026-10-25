//! Typed extension hooks (ADR-0025): the only in-process extension point.
//!
//! Hosts register any number of [`Hooks`]; the engine calls them in
//! registration order. The first `EndTurn` / `Deny` short-circuits, rewrites
//! pass from one hook to the next, and there is no `next` wrapping.

use std::sync::Arc;

use crate::{LogId, OwnerToken, SessionId, StepId, Timestamp, ToolCallId, TurnId};
use futures_core::future::BoxFuture;

use crate::config::{Provenance, ReplaySafety};
use crate::event::{SessionEvent, ToolErrorInfo, ToolResultStatus};
use crate::ids::{ToolArgs, ToolName};
use crate::item::Part;
use crate::usage::{UsageError, UsageReport};

/// Lifecycle hooks. Every method has a pass-through default.
pub trait Hooks: Send + Sync {
    /// Before every new step of a turn (a recovered open step is not gated).
    /// `EndTurn` ends the turn `Stopped`; an error or panic ends it `Failed`.
    fn before_step<'a>(&'a self, _ctx: &'a StepCtx) -> BoxFuture<'a, Result<Gate, HookError>> {
        Box::pin(async { Ok(Gate::Continue) })
    }

    /// Before a tool call's intent is recorded; may rewrite `args` or deny.
    /// Not called again for an intent that is already recorded. An error or
    /// panic records the call as failed with `Internal`.
    fn before_tool<'a>(
        &'a self,
        _ctx: &'a ToolCallCtx,
        _args: &'a mut ToolArgs,
    ) -> BoxFuture<'a, Result<ToolGate, HookError>> {
        Box::pin(async { Ok(ToolGate::Allow) })
    }

    /// After a tool ran and its output was truncated, before the result is
    /// recorded. Framework-written results (`Interrupted`, denials) skip it.
    /// An error or panic records the call as failed with `Internal`.
    fn after_tool<'a>(
        &'a self,
        _ctx: &'a ToolCallCtx,
        _result: &'a mut ToolResultView,
    ) -> BoxFuture<'a, Result<(), HookError>> {
        Box::pin(async { Ok(()) })
    }

    /// After every committed record of every session (children included), in
    /// per-session order. Cannot fail or affect the loop; a panic is logged.
    fn on_record(&self, _record: &CommittedRecord) {}

    /// This process lost `owner`'s tree: its lease could not be renewed, or
    /// another process took the tree over (ADR-0036). The tree's drives
    /// here have stopped and the claim is released; owner operations on its
    /// `OwnedSession`s fail with `OwnedElsewhere`. Called once per claim.
    fn on_ownership_lost(&self, _owner: OwnerToken) {}
}

/// `before_step` decision.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Gate {
    #[default]
    Continue,
    EndTurn {
        reason: String,
    },
}

/// `before_tool` decision.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ToolGate {
    #[default]
    Allow,
    Deny {
        reason: Option<String>,
    },
}

/// A hook failure; its message becomes the recorded error where one is written.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct HookError {
    pub message: String,
}

impl HookError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl From<UsageError> for HookError {
    fn from(error: UsageError) -> Self {
        Self::new(error.to_string())
    }
}

/// Engine-supplied fold behind `usage()`: a session's own and subtree usage.
pub type UsageLookup =
    Arc<dyn Fn(SessionId) -> BoxFuture<'static, Result<UsageReport, UsageError>> + Send + Sync>;

/// Usage of a session and its subtree, shared by both contexts.
#[derive(Clone, Default)]
struct UsageSource(Option<UsageLookup>);

impl UsageSource {
    async fn report(&self, session: SessionId) -> Result<UsageReport, UsageError> {
        let Some(lookup) = &self.0 else {
            return Err(UsageError::Backend(
                "usage accounting is not configured".to_owned(),
            ));
        };
        lookup(session).await
    }
}

impl std::fmt::Debug for UsageSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.is_some() { "Some(..)" } else { "None" })
    }
}

/// Data for `before_step`.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct StepCtx {
    pub session: SessionId,
    pub turn: TurnId,
    /// Zero-based index within the turn of the step about to start.
    pub step_index: u32,
    usage: UsageSource,
}

impl StepCtx {
    pub fn new(
        session: SessionId,
        turn: TurnId,
        step_index: u32,
        usage: Option<UsageLookup>,
    ) -> Self {
        Self {
            session,
            turn,
            step_index,
            usage: UsageSource(usage),
        }
    }

    /// Committed usage of this session (`own`) and its subtree (`descendants`).
    pub async fn usage(&self) -> Result<UsageReport, UsageError> {
        self.usage.report(self.session).await
    }
}

/// Data for `before_tool` / `after_tool`.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct ToolCallCtx {
    pub session: SessionId,
    pub turn: TurnId,
    pub step: StepId,
    pub tool_call_id: ToolCallId,
    pub provider_call_id: String,
    pub tool: ToolName,
    /// `Unknown` when the tool is not registered.
    pub provenance: Provenance,
    pub replay: ReplaySafety,
    usage: UsageSource,
}

impl ToolCallCtx {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session: SessionId,
        turn: TurnId,
        step: StepId,
        tool_call_id: ToolCallId,
        provider_call_id: String,
        tool: ToolName,
        provenance: Provenance,
        replay: ReplaySafety,
        usage: Option<UsageLookup>,
    ) -> Self {
        Self {
            session,
            turn,
            step,
            tool_call_id,
            provider_call_id,
            tool,
            provenance,
            replay,
            usage: UsageSource(usage),
        }
    }

    /// Committed usage of this session (`own`) and its subtree (`descendants`).
    pub async fn usage(&self) -> Result<UsageReport, UsageError> {
        self.usage.report(self.session).await
    }
}

/// The rewritable part of a tool result. Only `output` / `truncated` of a
/// completed call and `error` of a failed call are kept; status is fixed.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq)]
pub struct ToolResultView {
    status: ToolResultStatus,
    pub output: Vec<Part>,
    pub error: Option<ToolErrorInfo>,
    pub truncated: bool,
}

impl ToolResultView {
    pub fn new(
        status: ToolResultStatus,
        output: Vec<Part>,
        error: Option<ToolErrorInfo>,
        truncated: bool,
    ) -> Self {
        Self {
            status,
            output,
            error,
            truncated,
        }
    }

    pub fn status(&self) -> ToolResultStatus {
        self.status
    }
}

/// One committed log record, as `on_record` sees it.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq)]
pub struct CommittedRecord {
    pub session: SessionId,
    pub log_id: LogId,
    pub seq: u64,
    /// Diagnostic wall-clock time written by the backend.
    pub recorded_at: Timestamp,
    pub event: SessionEvent,
}

impl CommittedRecord {
    pub fn new(
        session: SessionId,
        log_id: LogId,
        seq: u64,
        recorded_at: Timestamp,
        event: SessionEvent,
    ) -> Self {
        Self {
            session,
            log_id,
            seq,
            recorded_at,
            event,
        }
    }
}
