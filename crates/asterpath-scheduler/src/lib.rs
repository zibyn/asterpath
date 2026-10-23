//! Sub-agent scheduler: one child session per spawn tool call, durable wait,
//! recorded outcome and the model-visible `spawn_agent` tool.
//!
//! Spawn is deliberately split into `spawn` (R1/R2: record the child and
//! materialize its stream) and `await_completion` (R3 + durable wait). Recovery
//! reuses both through ordinary tool replay; there is no separate `recover`
//! entry point and no background driver.

// `AgentError` / `SpawnError` shapes are fixed by the contract crates.
#![allow(clippy::result_large_err)]

use std::time::Duration;

use asterpath_protocol::{ChildOutcome, ContextPolicy, ModelAlias, ParentRef, Part, ToolName};
use asterpath_protocol::{ExecutionError, LogId, SessionId};
use futures_core::future::BoxFuture;

mod scheduler;
mod tool;

pub use scheduler::Scheduler;
pub use tool::{SPAWN_TOOL_NAME, SpawnTool};

/// Process capability implemented by this module; hosts may use it directly.
pub trait SubagentSpawner: Send + Sync {
    fn spawn(&self, req: SpawnRequest) -> BoxFuture<'_, Result<ChildRef, SpawnError>>;

    fn await_completion(&self, child: &ChildRef) -> BoxFuture<'_, Result<ChildOutcome, WaitError>>;

    fn cancel(&self, child: &ChildRef) -> BoxFuture<'_, Result<(), WaitError>>;
}

/// One spawn request. `caused_by` is the parent `ToolCallRequested` log id.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct SpawnRequest {
    pub parent: ParentRef,
    pub caused_by: LogId,
    pub task: Vec<Part>,
    pub context: ContextPolicy,
    pub model_alias: Option<ModelAlias>,
    pub tool_allowlist: Option<Vec<ToolName>>,
    pub timeout: Option<Duration>,
}

impl SpawnRequest {
    pub fn new(parent: ParentRef, caused_by: LogId, task: Vec<Part>) -> Self {
        Self {
            parent,
            caused_by,
            task,
            context: ContextPolicy::Fresh,
            model_alias: None,
            tool_allowlist: None,
            timeout: None,
        }
    }
}

/// The parent's tool call and the child it owns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildRef {
    pub parent: ParentRef,
    pub child: SessionId,
}

/// `SpawnError` means no child session was created.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    #[error("sub-agent depth exceeded")]
    DepthExceeded,
    #[error("invalid spawn arguments: {message}")]
    InvalidArgs { message: String },
    #[error(transparent)]
    Execution(#[from] ExecutionError),
}

/// The parent-side end of a wait.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum WaitError {
    #[error("sub-agent wait was cancelled")]
    Cancelled,
    #[error("sub-agent wait timed out")]
    Timeout,
    #[error(transparent)]
    Execution(#[from] ExecutionError),
}
