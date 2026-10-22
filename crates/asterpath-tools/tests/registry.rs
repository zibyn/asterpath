//! Registry and module contract tests for the in-process tool module.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use asterpath_core::{LifecycleError, Module, ModuleCtx, ModuleHost, ServiceId};
use asterpath_protocol::{
    Parallelism, Provenance, ReplaySafety, SideEffect, TOOL_REGISTRY_SERVICE, Tool, ToolArgs,
    ToolContext, ToolDescriptor, ToolError, ToolName, ToolOutput, ToolRegistry,
};
use asterpath_tools::{InMemoryToolRegistry, ToolsModule};
use futures_core::future::BoxFuture;

struct StubTool {
    descriptor: ToolDescriptor,
}

impl StubTool {
    fn tool(name: &str) -> Arc<dyn Tool> {
        Arc::new(Self {
            descriptor: ToolDescriptor {
                name: ToolName::new(name),
                description: String::new(),
                schema: serde_json::json!({"type": "object"}),
                parallelism: Parallelism::Parallel,
                replay: ReplaySafety::Safe,
                side_effect: SideEffect::ReadOnly,
                provenance: Provenance::InProcess,
                timeout: None,
                max_output_bytes: None,
            },
        })
    }
}

impl Tool for StubTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        _args: ToolArgs,
        _ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async { Ok(ToolOutput::new(Vec::new())) })
    }
}

#[test]
fn names_are_unique_and_listing_is_name_ordered() {
    let registry = InMemoryToolRegistry::new();
    let first = StubTool::tool("echo");
    let duplicate = StubTool::tool("echo");
    let other = StubTool::tool("alpha");
    registry.register(Arc::clone(&first));
    registry.register(duplicate);
    registry.register(other);

    let listed: Vec<_> = registry
        .list()
        .iter()
        .map(|tool| tool.descriptor().name.as_str().to_owned())
        .collect();
    assert_eq!(listed, ["alpha", "echo"]);
    assert!(Arc::ptr_eq(
        &registry.get(&ToolName::new("echo")).expect("registered"),
        &first
    ));
    assert!(registry.get(&ToolName::new("missing")).is_none());
}

/// Depends on the provided registry and records what it got.
struct Consumer {
    found: Arc<AtomicBool>,
}

impl Module for Consumer {
    fn name(&self) -> &'static str {
        "consumer"
    }

    fn dependencies(&self) -> &[ServiceId] {
        static IDS: [ServiceId; 1] = [TOOL_REGISTRY_SERVICE.id()];
        &IDS
    }

    fn init<'a>(&'a self, ctx: &'a ModuleCtx) -> BoxFuture<'a, Result<(), LifecycleError>> {
        let found = Arc::clone(&self.found);
        Box::pin(async move {
            let registry = ctx.require(TOOL_REGISTRY_SERVICE)?;
            found.store(registry.list().is_empty(), Ordering::SeqCst);
            Ok(())
        })
    }
}

#[tokio::test]
async fn the_module_provides_the_registry_service() {
    let found = Arc::new(AtomicBool::new(false));
    let module = Arc::new(ToolsModule::new());
    module
        .registry()
        .register(StubTool::tool("registered-directly"));

    let mut host = ModuleHost::new();
    host.register(module.clone());
    host.register(Arc::new(Consumer {
        found: Arc::clone(&found),
    }));
    host.init().await.expect("topological init");

    assert!(
        module.context().is_some(),
        "runtime waterfall dispatch needs the captured bus"
    );
    // `found` stores `is_empty()`; the consumer must have seen a non-empty list.
    assert!(!found.load(Ordering::SeqCst));

    host.shutdown().await;
}
