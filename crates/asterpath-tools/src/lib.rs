//! In-process tool registry.
//!
//! The registry is a live set: registration can continue at runtime. There are
//! no built-in tools; those belong to an optional crate or the CLI.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use asterpath_protocol::{Tool, ToolName, ToolRegistry};

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
    fn register(&self, tool: Arc<dyn Tool>) {
        let name = tool.descriptor().name.clone();
        self.tools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(name)
            .or_insert(tool);
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
