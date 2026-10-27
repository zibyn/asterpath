//! Tool contract and approval policy.
//!
//! The pipeline itself lives in the agent module; this crate owns the frozen
//! shapes it hands to tools, applications and extensions.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};

use crate::{SessionId, StepId, ToolCallId, TurnId};
use futures_core::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::agent::ApprovalRequest;
use crate::config::{Provenance, ToolDescriptor};
use crate::event::ToolErrorKind;
use crate::ids::ToolArgs;
use crate::item::Part;
use crate::sandbox::SpawnEnv;

/// One executable tool. `descriptor` is immutable for the tool's lifetime.
pub trait Tool: Send + Sync + 'static {
    fn descriptor(&self) -> &ToolDescriptor;
    fn invoke(
        &self,
        args: ToolArgs,
        ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>>;
}

/// A single tool call's view of the world. Timeouts are enforced by the
/// pipeline; tools only observe `cancel`.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct ToolContext {
    pub session: SessionId,
    /// The root of `session`'s tree, the same as the call's
    /// `ApprovalRequest::root` (ADR-0039); `session` itself for a root session.
    pub root: SessionId,
    pub turn: TurnId,
    pub step: StepId,
    pub tool_call_id: ToolCallId,
    pub cancel: CancelToken,
    /// The session environment's spawn settings (ADR-0037); empty when the
    /// application registered no session environment.
    pub spawn: SpawnEnv,
}

impl ToolContext {
    #[must_use]
    pub fn with_spawn(mut self, spawn: SpawnEnv) -> Self {
        self.spawn = spawn;
        self
    }

    /// The context of a subagent call in the tree rooted at `root`.
    #[must_use]
    pub fn with_root(mut self, root: SessionId) -> Self {
        self.root = root;
        self
    }

    pub fn new(
        session: SessionId,
        turn: TurnId,
        step: StepId,
        tool_call_id: ToolCallId,
        cancel: CancelToken,
    ) -> Self {
        Self {
            session,
            root: session,
            turn,
            step,
            tool_call_id,
            cancel,
            spawn: SpawnEnv::default(),
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
    #[error("timed out: {message}")]
    Timeout { message: String },
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
            | Self::Timeout { message }
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
            Self::Timeout { .. } => ToolErrorKind::Timeout,
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

/// Application policy for one tool call. `Ask` is only meaningful when an approval
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

/// Application answer to an approval request, recorded as `ApprovalDecided`.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApprovalDecision {
    Allow,
    Deny { reason: Option<String> },
}

/// Fixed reason used when `Ask` has no endpoint to answer it.
pub const NO_APPROVAL_ENDPOINT: &str = "no approval endpoint is registered";

/// Framework default: in-process tools are allowed; external tools ask, which
/// the pipeline turns into a deny with [`NO_APPROVAL_ENDPOINT`] when no
/// endpoint attends the session's tree. `Unknown` is never asked.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultApprovalPolicy;

impl ApprovalPolicy for DefaultApprovalPolicy {
    fn classify(&self, request: &ApprovalRequest) -> ApprovalVerdict {
        match request.provenance {
            Provenance::InProcess => ApprovalVerdict::Allow,
            Provenance::External { .. } => ApprovalVerdict::Ask,
            // The pipeline writes `NotFound` before approval for Unknown tools.
            Provenance::Unknown | Provenance::Output => ApprovalVerdict::Deny { reason: None },
        }
    }
}
