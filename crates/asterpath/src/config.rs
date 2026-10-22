//! Composition root configuration and builder.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use asterpath_core::{
    ApprovalGate, Clock, EventLog, SessionCatalog, SessionRetirement, Storage, SystemClock,
};
use asterpath_protocol::{
    AgentRunner, ApprovalPolicy, CompactionConfig, Compactor, DefaultApprovalPolicy, ModelAlias,
    ModelSpec, Provider, SubagentConfig, Tool, ToolConfig, TurnConfig,
};

use crate::error::AsterError;

/// Assembly-time configuration for [`crate::Asterpath`].
pub struct AsterpathConfig {
    pub database: PathBuf,
    pub providers: Vec<Arc<dyn Provider>>,
    pub tools: Vec<Arc<dyn Tool>>,
    pub tool: ToolConfig,
    pub agent: Option<Arc<dyn AgentRunner>>,
    /// Optional compaction capability. `None` (the M0 default) makes every
    /// compaction path report `NotConfigured`.
    pub compactor: Option<Arc<dyn Compactor>>,
    /// Overrides the `sqlite` feature; must be paired with `storage`.
    pub event_log: Option<Arc<dyn EventLog>>,
    pub storage: Option<Arc<dyn Storage>>,
    pub session_catalog: Option<Arc<dyn SessionCatalog>>,
    /// Optional: injected storage may not implement retirement.
    pub session_retirement: Option<Arc<dyn SessionRetirement>>,
    pub clock: Arc<dyn Clock>,
    /// Defaults to the built-in policy over [`Self::gate`].
    pub approval: Arc<dyn ApprovalPolicy>,
    /// The framework approval endpoint gate, shared with the default policy.
    pub gate: Arc<ApprovalGate>,
    pub turn: TurnConfig,
    pub compaction: CompactionConfig,
    pub subagents: SubagentConfig,
    pub model_aliases: BTreeMap<ModelAlias, ModelSpec>,
}

impl Default for AsterpathConfig {
    fn default() -> Self {
        let gate = Arc::new(ApprovalGate::new());
        Self {
            database: PathBuf::new(),
            providers: Vec::new(),
            tools: Vec::new(),
            tool: ToolConfig::default(),
            agent: None,
            compactor: None,
            event_log: None,
            storage: None,
            session_catalog: None,
            session_retirement: None,
            clock: Arc::new(SystemClock),
            approval: Arc::new(DefaultApprovalPolicy::new(Arc::clone(&gate))),
            gate,
            turn: TurnConfig::default(),
            compaction: CompactionConfig::default(),
            subagents: SubagentConfig::default(),
            model_aliases: BTreeMap::new(),
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

    #[must_use]
    pub fn agent(mut self, agent: Arc<dyn AgentRunner>) -> Self {
        self.config.agent = Some(agent);
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
    pub fn event_log(mut self, event_log: Arc<dyn EventLog>) -> Self {
        self.config.event_log = Some(event_log);
        self
    }

    #[must_use]
    pub fn storage(mut self, storage: Arc<dyn Storage>) -> Self {
        self.config.storage = Some(storage);
        self
    }

    #[must_use]
    pub fn session_catalog(mut self, catalog: Arc<dyn SessionCatalog>) -> Self {
        self.config.session_catalog = Some(catalog);
        self
    }

    #[must_use]
    pub fn session_retirement(mut self, retirement: Arc<dyn SessionRetirement>) -> Self {
        self.config.session_retirement = Some(retirement);
        self
    }

    #[must_use]
    pub fn turn(mut self, turn: TurnConfig) -> Self {
        self.config.turn = turn;
        self
    }

    #[must_use]
    pub fn compaction(mut self, compaction: CompactionConfig) -> Self {
        self.config.compaction = compaction;
        self
    }

    #[must_use]
    pub fn subagents(mut self, subagents: SubagentConfig) -> Self {
        self.config.subagents = subagents;
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
