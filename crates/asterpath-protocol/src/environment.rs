//! Session environment contract (ADR-0037): the host resolves, per session,
//! the tools it may call and what its spawned processes start with.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures_core::future::BoxFuture;

use crate::sandbox::SpawnEnv;
use crate::tool::Tool;

/// One session's environment, resolved from its host metadata. Not logged:
/// it is resolved again on every open.
#[non_exhaustive]
#[derive(Clone, Default)]
pub struct SessionEnvironment {
    /// Tools of this session only, on top of the builder's; a name here
    /// shadows a builder tool of that name.
    pub tools: Vec<Arc<dyn Tool>>,
    /// Handed to every tool call in `ToolContext::spawn`.
    pub spawn: SpawnEnv,
}

impl SessionEnvironment {
    pub fn new(tools: Vec<Arc<dyn Tool>>, spawn: SpawnEnv) -> Self {
        Self { tools, spawn }
    }
}

impl std::fmt::Debug for SessionEnvironment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let tools: Vec<_> = self
            .tools
            .iter()
            .map(|tool| tool.descriptor().name.as_str())
            .collect();
        f.debug_struct("SessionEnvironment")
            .field("tools", &tools)
            .field("spawn", &self.spawn)
            .finish()
    }
}

/// Host adapter that turns a root session's metadata into its environment.
/// The engine calls it when a session is created, forked or opened; child
/// sessions use their root's. How metadata maps to an environment, and any
/// caching of tool instances, is the host's.
pub trait EnvironmentAdapter: Send + Sync + 'static {
    fn resolve<'a>(
        &'a self,
        metadata: &'a BTreeMap<String, String>,
    ) -> BoxFuture<'a, Result<SessionEnvironment, EnvironmentError>>;
}

/// The host could not resolve a session's environment.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("session environment: {message}")]
pub struct EnvironmentError {
    pub message: String,
}

impl EnvironmentError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}
