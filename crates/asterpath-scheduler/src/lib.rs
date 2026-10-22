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

use asterpath_core::{
    ExecutionError, LogId, Module, ModuleCtx, ServiceId, ServiceKey, SessionId, service_key,
};
use asterpath_protocol::{
    ChildOutcome, ContextPolicy, ModelAlias, ParentRef, Part, TOOL_REGISTRY_SERVICE, ToolName,
};
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

pub const SUBAGENT_SPAWNER_SERVICE: ServiceKey<dyn SubagentSpawner> =
    service_key("SubagentSpawner");

/// Provides the spawner service and registers the `spawn_agent` tool.
pub struct SchedulerModule {
    scheduler: std::sync::Arc<Scheduler>,
}

impl SchedulerModule {
    pub fn new(
        log: std::sync::Arc<dyn asterpath_core::EventLog>,
        catalog: std::sync::Arc<dyn asterpath_core::SessionCatalog>,
        executor: std::sync::Arc<dyn asterpath_core::DurableExecutor>,
        model_aliases: std::collections::BTreeMap<ModelAlias, asterpath_protocol::ModelSpec>,
    ) -> Self {
        Self {
            scheduler: std::sync::Arc::new(Scheduler::new(log, catalog, executor, model_aliases)),
        }
    }

    pub fn scheduler(&self) -> std::sync::Arc<Scheduler> {
        std::sync::Arc::clone(&self.scheduler)
    }
}

impl Module for SchedulerModule {
    fn name(&self) -> &'static str {
        "scheduler"
    }

    fn dependencies(&self) -> &[ServiceId] {
        static IDS: [ServiceId; 1] = [TOOL_REGISTRY_SERVICE.id()];
        &IDS
    }

    fn provides(&self) -> &[ServiceId] {
        static IDS: [ServiceId; 1] = [SUBAGENT_SPAWNER_SERVICE.id()];
        &IDS
    }

    fn init<'a>(
        &'a self,
        ctx: &'a ModuleCtx,
    ) -> BoxFuture<'a, Result<(), asterpath_core::LifecycleError>> {
        Box::pin(async move {
            let registry = ctx.require(TOOL_REGISTRY_SERVICE)?;
            self.scheduler.set_event_bus(ctx.clone());
            let spawner: std::sync::Arc<dyn SubagentSpawner> = self.scheduler.clone();
            ctx.provide(SUBAGENT_SPAWNER_SERVICE, spawner)?;
            registry.register(std::sync::Arc::new(SpawnTool::new(self.scheduler.clone())));
            Ok(())
        })
    }
}
