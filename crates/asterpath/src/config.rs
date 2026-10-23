//! Composition root configuration and builder.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;

use asterpath_protocol::{ApprovalGate, Clock, SessionStore, SystemClock};
use asterpath_protocol::{
    ApprovalPolicy, Compactor, DefaultApprovalPolicy, Hooks, ModelAlias, ModelSpec, Provider, Tool,
    ToolConfig,
};

use crate::error::AsterError;

/// Assembly-time configuration for [`crate::Asterpath`].
pub struct AsterpathConfig {
    pub database: PathBuf,
    pub providers: Vec<Arc<dyn Provider>>,
    pub tools: Vec<Arc<dyn Tool>>,
    pub tool: ToolConfig,
    /// Optional compaction capability. `None` (the M0 default) makes every
    /// compaction path report `NotConfigured`.
    pub compactor: Option<Arc<dyn Compactor>>,
    /// Overrides the `sqlite` feature. An injected store may keep the
    /// default `Unsupported` retirement.
    pub store: Option<Arc<dyn SessionStore>>,
    pub clock: Arc<dyn Clock>,
    /// Defaults to the built-in policy over [`Self::gate`].
    pub approval: Arc<dyn ApprovalPolicy>,
    /// The framework approval endpoint gate, shared with the default policy.
    pub gate: Arc<ApprovalGate>,
    /// Process-wide cap on live child sessions across all parents.
    pub max_live_children: NonZeroUsize,
    pub model_aliases: BTreeMap<ModelAlias, ModelSpec>,
    /// Extension hooks, called in this order (ADR-0025).
    pub hooks: Vec<Arc<dyn Hooks>>,
}

impl Default for AsterpathConfig {
    fn default() -> Self {
        let gate = Arc::new(ApprovalGate::new());
        Self {
            database: PathBuf::new(),
            providers: Vec::new(),
            tools: Vec::new(),
            tool: ToolConfig::default(),
            compactor: None,
            store: None,
            clock: Arc::new(SystemClock),
            approval: Arc::new(DefaultApprovalPolicy::new(Arc::clone(&gate))),
            gate,
            max_live_children: NonZeroUsize::new(32).expect("non-zero"),
            model_aliases: BTreeMap::new(),
            hooks: Vec::new(),
        }
    }
}

/// Sugar over [`AsterpathConfig`]; `build()` performs the topology init.
pub struct AsterpathBuilder {
    config: AsterpathConfig,
}

impl AsterpathBuilder {
    pub fn new() -> Self {
        Self {
            config: AsterpathConfig::default(),
        }
    }

    pub fn from_config(config: AsterpathConfig) -> Self {
        Self { config }
    }

    #[must_use]
    pub fn database(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.database = path.into();
        self
    }

    #[must_use]
    pub fn provider(mut self, provider: Arc<dyn Provider>) -> Self {
        self.config.providers.push(provider);
        self
    }

    #[must_use]
    pub fn providers(mut self, providers: Vec<Arc<dyn Provider>>) -> Self {
        self.config.providers = providers;
        self
    }

    #[must_use]
    pub fn tools(mut self, tools: Vec<Arc<dyn Tool>>) -> Self {
        self.config.tools = tools;
        self
    }

    #[must_use]
    pub fn tool(mut self, tool: ToolConfig) -> Self {
        self.config.tool = tool;
        self
    }

    /// Register one hook; may be called repeatedly. Hooks run in this order.
    #[must_use]
    pub fn hooks(mut self, hooks: Arc<dyn Hooks>) -> Self {
        self.config.hooks.push(hooks);
        self
    }

    #[must_use]
    pub fn compactor(mut self, compactor: Arc<dyn Compactor>) -> Self {
        self.config.compactor = Some(compactor);
        self
    }

    #[must_use]
    pub fn approval(mut self, approval: Arc<dyn ApprovalPolicy>) -> Self {
        self.config.approval = approval;
        self
    }

    #[must_use]
    pub fn store(mut self, store: Arc<dyn SessionStore>) -> Self {
        self.config.store = Some(store);
        self
    }

    #[must_use]
    pub fn max_live_children(mut self, max_live_children: NonZeroUsize) -> Self {
        self.config.max_live_children = max_live_children;
        self
    }

    #[must_use]
    pub fn model_aliases(mut self, model_aliases: BTreeMap<ModelAlias, ModelSpec>) -> Self {
        self.config.model_aliases = model_aliases;
        self
    }

    #[must_use]
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.config.clock = clock;
        self
    }

    pub async fn build(self) -> Result<crate::Asterpath, AsterError> {
        crate::Asterpath::from_config(self.config).await
    }
}

impl Default for AsterpathBuilder {
    fn default() -> Self {
        Self::new()
    }
}
