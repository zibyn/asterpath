//! Tool contract, pipeline waterfall events and approval policy.
//!
//! The pipeline itself lives in the agent module; this crate owns the frozen
//! shapes it hands to tools, hosts and extensions.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};

use asterpath_core::{
    ApprovalGate, Effect, Event, ServiceKey, SessionId, SignalId, SignalName, StepId, ToolCallId,
    TurnId, WaterfallEvent, service_key,
};
use futures_core::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::agent::ApprovalRequest;
use crate::config::{Provenance, ToolDescriptor};
use crate::event::{ToolErrorInfo, ToolErrorKind, ToolResultStatus};
use crate::ids::{ToolArgs, ToolName};
use crate::item::Part;

/// One executable tool. `descriptor` is immutable for the tool's lifetime.
pub trait Tool: Send + Sync + 'static {
    fn descriptor(&self) -> &ToolDescriptor;
    fn invoke(
        &self,
        args: ToolArgs,
        ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>>;
}

/// Live registry of tools, ordered by name. Duplicate names keep the first
/// registration; the name is the tool's identity within one process.
pub trait ToolRegistry: Send + Sync {
    fn register(&self, tool: Arc<dyn Tool>) -> Effect;
    fn get(&self, name: &ToolName) -> Option<Arc<dyn Tool>>;
    fn list(&self) -> Vec<Arc<dyn Tool>>;
}

pub const TOOL_REGISTRY_SERVICE: ServiceKey<dyn ToolRegistry> = service_key("ToolRegistry");

/// A single tool call's view of the world. Timeouts are enforced by the
/// pipeline; tools only observe `cancel`.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct ToolContext {
    pub session: SessionId,
    pub turn: TurnId,
    pub step: StepId,
    pub tool_call_id: ToolCallId,
    pub cancel: CancelToken,
}

impl ToolContext {
    pub fn new(
        session: SessionId,
        turn: TurnId,
        step: StepId,
        tool_call_id: ToolCallId,
        cancel: CancelToken,
    ) -> Self {
        Self {
            session,
            turn,
            step,
            tool_call_id,
            cancel,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ToolOutput {
    pub content: Vec<Part>,
}

impl ToolOutput {
    pub fn new(content: Vec<Part>) -> Self {
        Self { content }
    }
}

/// Tool-reported failure. The pipeline maps each variant to `ToolErrorKind`
/// and persists the message as the model-visible error.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ToolError {
    #[error("invalid arguments: {message}")]
    InvalidArgs { message: String },
    #[error("not found: {message}")]
    NotFound { message: String },
    #[error("denied: {message}")]
    Denied { message: String },
    #[error("cancelled: {message}")]
    Cancelled { message: String },
    #[error("transport error: {message}")]
    Transport { message: String },
    #[error("internal error: {message}")]
    Internal { message: String },
}

impl ToolError {
    pub fn message(&self) -> &str {
        match self {
            Self::InvalidArgs { message }
            | Self::NotFound { message }
            | Self::Denied { message }
            | Self::Cancelled { message }
            | Self::Transport { message }
            | Self::Internal { message } => message,
        }
    }

    pub fn kind(&self) -> ToolErrorKind {
        match self {
            Self::InvalidArgs { .. } => ToolErrorKind::InvalidArgs,
            Self::NotFound { .. } => ToolErrorKind::NotFound,
            Self::Denied { .. } => ToolErrorKind::Denied,
            Self::Cancelled { .. } => ToolErrorKind::Cancelled,
            Self::Transport { .. } => ToolErrorKind::Transport,
            Self::Internal { .. } => ToolErrorKind::Internal,
        }
    }
}

#[derive(Default)]
struct CancelState {
    cancelled: AtomicBool,
    wakers: Mutex<Vec<Waker>>,
    /// Child tokens cancelled with this one (turn token → per-call token).
    children: Mutex<Vec<std::sync::Weak<CancelState>>>,
}

/// Per-call cancellation handle. Clones share one state.
#[derive(Clone, Default)]
pub struct CancelToken {
    state: Arc<CancelState>,
}

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    /// Broadcast cancellation; later calls to `is_cancelled` observe it.
    /// Idempotent, and it propagates to every child token.
    pub fn cancel(&self) {
        if self.state.cancelled.swap(true, Ordering::SeqCst) {
            return;
        }
        let wakers = std::mem::take(
            &mut *self
                .state
                .wakers
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        for waker in wakers {
            waker.wake();
        }
        let children: Vec<CancelToken> = {
            let mut children = self
                .state
                .children
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            children
                .drain(..)
                .filter_map(|weak| weak.upgrade().map(|state| CancelToken { state }))
                .collect()
        };
        for child in children {
            child.cancel();
        }
    }

    /// A token that is cancelled whenever this one is. The child can be
    /// cancelled on its own without affecting the parent (per-call timeouts).
    pub fn child(&self) -> CancelToken {
        let child = CancelToken::default();
        self.state
            .children
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Arc::downgrade(&child.state));
        // Re-check after registration so a concurrent cancel cannot be missed.
        if self.is_cancelled() {
            child.cancel();
        }
        child
    }

    pub fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::SeqCst)
    }

    /// Resolves once cancellation has been broadcast.
    pub fn cancelled(&self) -> BoxFuture<'_, ()> {
        Box::pin(Cancelled {
            token: self.clone(),
        })
    }
}

impl fmt::Debug for CancelToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancelToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

struct Cancelled {
    token: CancelToken,
}

impl Future for Cancelled {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.token.is_cancelled() {
            return Poll::Ready(());
        }
        let mut wakers = self
            .token
            .state
            .wakers
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // Re-check under the lock so a concurrent `cancel` cannot be missed.
        if self.token.is_cancelled() {
            return Poll::Ready(());
        }
        wakers.push(cx.waker().clone());
        Poll::Pending
    }
}

/// Pre-execute waterfall event. Middleware may rewrite `args` and pass the
/// event on, or short-circuit with [`PreExecuteError::Denied`].
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq)]
pub struct PreExecuteEvent {
    pub session: SessionId,
    pub turn: TurnId,
    pub step: StepId,
    pub tool_call_id: ToolCallId,
    pub provider_call_id: String,
    pub tool: ToolName,
    pub args: ToolArgs,
}

impl PreExecuteEvent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session: SessionId,
        turn: TurnId,
        step: StepId,
        tool_call_id: ToolCallId,
        provider_call_id: String,
        tool: ToolName,
        args: ToolArgs,
    ) -> Self {
        Self {
            session,
            turn,
            step,
            tool_call_id,
            provider_call_id,
            tool,
            args,
        }
    }
}

impl Event for PreExecuteEvent {}
impl WaterfallEvent for PreExecuteEvent {
    type Error = PreExecuteError;
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PreExecuteError {
    #[error("tool call denied: {}", reason.as_deref().unwrap_or(""))]
    Denied { reason: Option<String> },
    #[error("pre-execute failed: {message}")]
    Internal { message: String },
}

/// Post-execute waterfall event, run after truncation and before the result is
/// appended. Middleware may rewrite `output` and `error.message`.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq)]
pub struct PostExecuteEvent {
    pub session: SessionId,
    pub turn: TurnId,
    pub step: StepId,
    pub tool_call_id: ToolCallId,
    pub tool: ToolName,
    pub status: ToolResultStatus,
    pub output: Vec<Part>,
    pub error: Option<ToolErrorInfo>,
    pub truncated: bool,
}

impl PostExecuteEvent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        session: SessionId,
        turn: TurnId,
        step: StepId,
        tool_call_id: ToolCallId,
        tool: ToolName,
        status: ToolResultStatus,
        output: Vec<Part>,
        error: Option<ToolErrorInfo>,
        truncated: bool,
    ) -> Self {
        Self {
            session,
            turn,
            step,
            tool_call_id,
            tool,
            status,
            output,
            error,
            truncated,
        }
    }
}

impl Event for PostExecuteEvent {}
impl WaterfallEvent for PostExecuteEvent {
    type Error = PostExecuteError;
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PostExecuteError {
    #[error("post-execute failed: {message}")]
    Internal { message: String },
}

/// Host policy for one tool call. `Ask` is only meaningful when an approval
/// endpoint may be available; the pipeline fails closed otherwise.
pub trait ApprovalPolicy: Send + Sync {
    fn classify(&self, request: &ApprovalRequest) -> ApprovalVerdict;
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApprovalVerdict {
    Allow,
    Ask,
    Deny { reason: Option<String> },
}

/// Host answer to an approval request; also the `SignalReceived` payload.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApprovalDecision {
    Allow,
    Deny { reason: Option<String> },
}

/// Fixed reason used when `Ask` has no endpoint to answer it.
pub const NO_APPROVAL_ENDPOINT: &str = "no approval endpoint is registered";

/// Signal name prefix shared by the pipeline and approval responders.
pub const APPROVAL_SIGNAL_PREFIX: &str = "tool.approval.";

/// One call's approval signal name: `tool.approval.<tool_call_id>`.
pub fn approval_signal_name(call: ToolCallId) -> SignalName {
    SignalName::new(format!("{APPROVAL_SIGNAL_PREFIX}{}", call.to_hex()))
}

/// The approval signal id is the framework `tool_call_id` itself.
pub fn approval_signal_id(call: ToolCallId) -> SignalId {
    SignalId::from_bytes(*call.as_bytes())
}

/// Framework default: in-process tools are allowed; external tools ask while an
/// endpoint is registered and are denied otherwise. `Unknown` is never asked.
pub struct DefaultApprovalPolicy {
    gate: Arc<ApprovalGate>,
}

impl DefaultApprovalPolicy {
    pub fn new(gate: Arc<ApprovalGate>) -> Self {
        Self { gate }
    }
}

impl ApprovalPolicy for DefaultApprovalPolicy {
    fn classify(&self, request: &ApprovalRequest) -> ApprovalVerdict {
        match request.provenance {
            Provenance::InProcess => ApprovalVerdict::Allow,
            Provenance::External { .. } => {
                if self.gate.is_open() {
                    ApprovalVerdict::Ask
                } else {
                    ApprovalVerdict::Deny {
                        reason: Some(NO_APPROVAL_ENDPOINT.to_owned()),
                    }
                }
            }
            // The pipeline writes `NotFound` before approval for Unknown tools.
            Provenance::Unknown => ApprovalVerdict::Deny { reason: None },
        }
    }
}
