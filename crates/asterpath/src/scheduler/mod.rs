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

use asterpath_protocol::{ContextPolicy, ModelAlias, ParentRef, Part, ToolName};
use asterpath_protocol::{ExecutionError, LogId, SessionId};

mod spawn;
mod tool;

pub(crate) use spawn::Scheduler;
pub(crate) use tool::{SPAWN_TOOL_NAME, SpawnTool};

/// One spawn request. `caused_by` is the parent `ToolCallRequested` log id.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub(crate) struct SpawnRequest {
    pub(crate) parent: ParentRef,
    pub(crate) caused_by: LogId,
    pub(crate) task: Vec<Part>,
    pub(crate) context: ContextPolicy,
    pub(crate) model_alias: Option<ModelAlias>,
    pub(crate) tool_allowlist: Option<Vec<ToolName>>,
    pub(crate) timeout: Option<Duration>,
}

impl SpawnRequest {
    pub(crate) fn new(parent: ParentRef, caused_by: LogId, task: Vec<Part>) -> Self {
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
pub(crate) struct ChildRef {
    pub(crate) parent: ParentRef,
    pub(crate) child: SessionId,
}

/// `SpawnError` means no child session was created.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub(crate) enum SpawnError {
    #[error("max sub-agent depth {max_depth} reached")]
    DepthExceeded { max_depth: u8 },
    #[error("invalid spawn arguments: {message}")]
    InvalidArgs { message: String },
    #[error(transparent)]
    Execution(#[from] ExecutionError),
}

/// The parent-side end of a wait.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub(crate) enum WaitError {
    #[error("sub-agent wait was cancelled")]
    Cancelled,
    #[error("sub-agent wait timed out")]
    Timeout,
    #[error(transparent)]
    Execution(#[from] ExecutionError),
}

#[cfg(test)]
mod tests;
