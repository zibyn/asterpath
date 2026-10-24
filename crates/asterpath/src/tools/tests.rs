//! The tool table: unique names, name-ordered listing.

use std::sync::Arc;

use crate::tools::ToolTable;
use asterpath_protocol::{
    Parallelism, Provenance, ReplaySafety, SideEffect, Tool, ToolArgs, ToolContext, ToolDescriptor,
    ToolError, ToolName, ToolOutput,
};
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
                strict: None,
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
    let registry = ToolTable::new();
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
