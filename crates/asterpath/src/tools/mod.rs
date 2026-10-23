//! The engine's tool table.
//!
//! A live set: registration can continue at runtime. There are no built-in
//! tools; those belong to an optional crate or the CLI.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use asterpath_protocol::{Tool, ToolName};

/// Name-keyed, name-ordered tools. Registering a duplicate name keeps the
/// first tool; the name is the tool's identity within one process.
#[derive(Default)]
pub(crate) struct ToolTable {
    tools: Mutex<BTreeMap<ToolName, Arc<dyn Tool>>>,
}

impl ToolTable {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register(&self, tool: Arc<dyn Tool>) {
        let name = tool.descriptor().name.clone();
        self.tools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(name)
            .or_insert(tool);
    }

    pub(crate) fn get(&self, name: &ToolName) -> Option<Arc<dyn Tool>> {
        self.tools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .cloned()
    }

    pub(crate) fn list(&self) -> Vec<Arc<dyn Tool>> {
        self.tools
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests;
