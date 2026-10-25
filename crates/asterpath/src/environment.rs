//! The session environment a drive runs in (ADR-0037). Like the owner
//! token, it reaches the tool pipeline through the poll: a child drive,
//! polled inside its parent's, uses the parent's environment.

use std::cell::RefCell;
use std::sync::Arc;

use asterpath_protocol::{SessionEnvironment, Tool, ToolName};

use crate::tools::ToolTable;

thread_local! {
    static CURRENT: RefCell<Option<Arc<SessionEnvironment>>> = const { RefCell::new(None) };
}

/// The environment of the drive being polled on this thread.
pub(crate) fn current() -> Option<Arc<SessionEnvironment>> {
    CURRENT.with_borrow(Clone::clone)
}

/// Run `f` in `env`; `None` keeps the enclosing environment.
pub(crate) fn within<R>(env: Option<&Arc<SessionEnvironment>>, f: impl FnOnce() -> R) -> R {
    crate::scoped::within(&CURRENT, env.cloned(), f)
}

/// `name` in `env`, else in the builder's table.
pub(crate) fn tool(
    table: &ToolTable,
    env: Option<&SessionEnvironment>,
    name: &ToolName,
) -> Option<Arc<dyn Tool>> {
    env.and_then(|env| {
        env.tools
            .iter()
            .find(|tool| &tool.descriptor().name == name)
            .cloned()
    })
    .or_else(|| table.get(name))
}

/// The builder's tools with `env`'s on top, name-ordered.
pub(crate) fn tools(table: &ToolTable, env: Option<&SessionEnvironment>) -> Vec<Arc<dyn Tool>> {
    let mut all: std::collections::BTreeMap<ToolName, Arc<dyn Tool>> = table
        .list()
        .into_iter()
        .map(|tool| (tool.descriptor().name.clone(), tool))
        .collect();
    for tool in env.into_iter().flat_map(|env| &env.tools) {
        all.insert(tool.descriptor().name.clone(), Arc::clone(tool));
    }
    all.into_values().collect()
}
