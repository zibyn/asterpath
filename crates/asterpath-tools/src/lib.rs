//! In-process tool registry module.
//!
//! The registry is a live, replaceable set: extensions register tools during
//! `init`, and registration can continue at runtime without the host registry.
//! There are no built-in tools; those belong to an optional module or the CLI.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use asterpath_core::{Effect, LifecycleError, Module, ModuleCtx, ServiceId};
use asterpath_protocol::{TOOL_REGISTRY_SERVICE, Tool, ToolName, ToolRegistry};
use futures_core::future::BoxFuture;

/// Name-keyed registry. Registering a duplicate name keeps the first tool.
#[derive(Default)]
pub struct InMemoryToolRegistry {
    tools: Mutex<BTreeMap<ToolName, Arc<dyn Tool>>>,
}

impl InMemoryToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ToolRegistry for InMemoryToolRegistry {
    fn register(&self, tool: Arc<dyn Tool>) -> Effect {
        let name = tool.descriptor().name.clone();
        self.tools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(name)
            .or_insert(tool);
        Effect
    }

    fn get(&self, name: &ToolName) -> Option<Arc<dyn Tool>> {
        self.tools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .cloned()
    }

    fn list(&self) -> Vec<Arc<dyn Tool>> {
        self.tools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }
}

/// Provides the process-local registry service and keeps its `ModuleCtx` so a
/// runtime pipeline can dispatch pre/post-execute waterfalls on the same bus.
pub struct ToolsModule {
    registry: Arc<InMemoryToolRegistry>,
    context: Mutex<Option<ModuleCtx>>,
}

impl ToolsModule {
    pub fn new() -> Self {
        Self {
            registry: Arc::new(InMemoryToolRegistry::new()),
            context: Mutex::new(None),
        }
    }

    pub fn registry(&self) -> Arc<dyn ToolRegistry> {
        self.registry.clone()
    }

    /// The bus handle captured at `init`; `None` before initialization.
    pub fn context(&self) -> Option<ModuleCtx> {
        self.context
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Default for ToolsModule {
    fn default() -> Self {
        Self::new()
    }
}

impl Module for ToolsModule {
    fn name(&self) -> &'static str {
        "tools"
    }

    fn provides(&self) -> &[ServiceId] {
        static IDS: [ServiceId; 1] = [TOOL_REGISTRY_SERVICE.id()];
        &IDS
    }

    fn init<'a>(&'a self, ctx: &'a ModuleCtx) -> BoxFuture<'a, Result<(), LifecycleError>> {
        *self.context.lock().unwrap_or_else(PoisonError::into_inner) = Some(ctx.clone());
        let registry: Arc<dyn ToolRegistry> = self.registry.clone();
        let result = ctx.provide(TOOL_REGISTRY_SERVICE, registry);
        Box::pin(async move { result.map(|_| ()) })
    }
}
