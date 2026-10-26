//! Session environment contract (ADR-0037): the application resolves, per session,
//! the tools it may call and what its spawned processes start with.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures_core::future::BoxFuture;

use crate::config::ModelSpec;
use crate::ids::ToolName;
use crate::item::Part;
use crate::sandbox::SpawnEnv;
use crate::tool::Tool;

/// One session's environment, resolved from its session metadata. Not logged:
/// it is resolved again on every open.
#[non_exhaustive]
#[derive(Clone, Default)]
pub struct SessionEnvironment {
    /// Tools of this session only, on top of the builder's; a name here
    /// shadows a builder tool of that name.
    pub tools: Vec<Arc<dyn Tool>>,
    /// Handed to every tool call in `ToolContext::spawn`.
    pub spawn: SpawnEnv,
    /// Agent profiles `spawn_agent` may select by name (ADR-0053).
    pub agents: Vec<AgentProfile>,
    /// The system of a fresh child the root session spawns without a profile
    /// (ADR-0054); `None` copies the parent's system.
    pub child_system: Option<Vec<Part>>,
}

impl SessionEnvironment {
    pub fn new(tools: Vec<Arc<dyn Tool>>, spawn: SpawnEnv) -> Self {
        Self {
            tools,
            spawn,
            agents: Vec::new(),
            child_system: None,
        }
    }
}

/// An application agent profile a child session can be spawned with
/// (ADR-0053): the child's whole system, and optionally a pinned model and a
/// tool list that narrows the parent's tools.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq)]
pub struct AgentProfile {
    pub name: String,
    pub description: String,
    /// Taken as is; the parent's system is not prepended.
    pub system: Vec<Part>,
    /// Must share the parent's provider.
    pub model: Option<ModelSpec>,
    /// Names the parent lacks are ignored; `None` keeps the parent's tools.
    pub tools: Option<Vec<ToolName>>,
}

impl AgentProfile {
    pub fn new(name: impl Into<String>, description: impl Into<String>, system: Vec<Part>) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            system,
            model: None,
            tools: None,
        }
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
            .field("agents", &self.agents)
            .field("child_system", &self.child_system)
            .finish()
    }
}

/// Application adapter that turns a root session's metadata into its environment.
/// The engine calls it when a session is created, forked or opened; child
/// sessions use their root's. How metadata maps to an environment, and any
/// caching of tool instances, is the application's.
pub trait EnvironmentAdapter: Send + Sync + 'static {
    fn resolve<'a>(
        &'a self,
        metadata: &'a BTreeMap<String, String>,
    ) -> BoxFuture<'a, Result<SessionEnvironment, EnvironmentError>>;
}

/// The application could not resolve a session's environment.
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
