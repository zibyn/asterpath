//! Asterpath facade: the composition root and the stable session API.
//!
//! The facade constructs the engine components directly; applications extend it only
//! through adapter traits and [`protocol::Hooks`] (ADR-0025). There is no
//! background driver: the application claims a session tree and calls
//! [`OwnedSession::drive`].

// The public error enum is fixed by packaging.md §3.5.
#![allow(clippy::result_large_err)]

mod agent;
mod commit;
mod compaction;
pub mod config;
mod environment;
pub mod error;
mod fork;
mod owner;
mod scan;
mod scheduler;
mod scoped;
mod tools;
mod views;
mod waits;
mod watch;

pub use compaction::DefaultCompactor;
pub use watch::WatchEvent;

/// The contract crate: adapter traits, hooks and value types.
pub use asterpath_protocol as protocol;
/// Core id types, so embedders need only this crate.
pub use asterpath_protocol::{InputId, LogId, SessionId, StepId, Timestamp, ToolCallId, TurnId};

/// Re-export of the Anthropic adapter when the `anthropic` feature is enabled.
/// Providers stay application-injected: the facade never assembles one itself.
#[cfg(feature = "anthropic")]
pub use asterpath_provider_anthropic as anthropic;

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::agent::Agent;
use crate::scheduler::{Scheduler, SpawnTool};
use crate::tools::ToolTable;
use crate::views::trace::TracingQuery;
use crate::views::usage::UsageService;
use crate::waits::Waits;
use crate::watch::Watchers;
use asterpath_protocol::scan_all;
use asterpath_protocol::{
    AgentError, AgentEvent, ApprovalDecision, CancellationRegistry, CompactResult, ConfigPatch,
    ForkSource, ProjectedEntry, ProviderCatalog, RunRequest, RunTrigger, SessionEvent,
    SessionFacts, SessionSpec, SessionState, Span, SpanFilter, ToolSelection, TraceError,
    UsageError, UsageFilter, UsageLookup, UsageReport, UsageScope, UserInput, dedup,
};
use asterpath_protocol::{
    ApprovalGate, CatalogEntry, Clock, EnvironmentAdapter, LogQuery, OwnerToken,
    SessionEnvironment, SessionQuery, SessionStore, Tool, root_of, unique_bytes,
};
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, TryStreamExt};

pub use config::{AsterpathBuilder, AsterpathConfig};
pub use error::AsterError;

#[cfg(feature = "openai")]
pub use asterpath_provider_openai as openai;

/// The assembled framework.
pub struct Asterpath {
    /// The single append entry: the backend wrapped in `CommitLog`, so every
    /// append reaches the hooks.
    store: Arc<dyn SessionStore>,
    providers: Arc<ProviderCatalog>,
    tools: Arc<ToolTable>,
    agent: Arc<Agent>,
    usage: Arc<UsageService>,
    waits: Arc<Waits>,
    gate: Arc<ApprovalGate>,
    cancellations: Arc<CancellationRegistry>,
    clock: Arc<dyn Clock>,
    trace: TracingQuery,
    environment: Option<Arc<dyn EnvironmentAdapter>>,
    watchers: Arc<Watchers>,
    ownership: Arc<owner::Ownership>,
}

impl std::fmt::Debug for Asterpath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Asterpath").finish_non_exhaustive()
    }
}

impl Asterpath {
    pub fn builder() -> AsterpathBuilder {
        AsterpathBuilder::new()
    }

    pub async fn build(config: AsterpathConfig) -> Result<Self, AsterError> {
        Self::from_config(config).await
    }

    pub(crate) async fn from_config(config: AsterpathConfig) -> Result<Self, AsterError> {
        check_tool_names(&config.tools)?;

        let providers = Arc::new(ProviderCatalog::new(config.providers).map_err(|error| {
            AsterError::Config {
                message: error.to_string(),
            }
        })?);
        for model in config.model_aliases.values() {
            if providers.get(&model.provider).is_none() {
                return Err(AsterError::Config {
                    message: format!("model alias references unknown provider {}", model.provider),
                });
            }
        }

        let backend: Arc<dyn SessionStore> = match config.store {
            Some(store) => store,
            #[cfg(feature = "sqlite")]
            None => {
                if config.database.as_os_str().is_empty() {
                    return Err(AsterError::Config {
                        message: "database path is required for the sqlite backend".to_owned(),
                    });
                }
                asterpath_storage_sqlite::open(&config.database)?
            }
            #[cfg(not(feature = "sqlite"))]
            None => {
                return Err(AsterError::Config {
                    message: "a store must be injected without the sqlite feature".to_owned(),
                });
            }
        };

        // Projections read the backend; every write goes through the one
        // append entry so hooks see it.
        let trace = TracingQuery::new(Arc::clone(&backend));
        let usage = Arc::new(UsageService::new(Arc::clone(&backend)));
        let watchers = Arc::new(Watchers::default());
        let ownership = Arc::new(owner::Ownership::new(
            Arc::clone(&backend),
            config.hooks.clone(),
        ));
        owner::Ownership::keep_renewed(&ownership, config.lease_renew_interval);
        let store: Arc<dyn SessionStore> = Arc::new(commit::CommitLog::new(
            backend,
            config.hooks.clone(),
            Arc::clone(&watchers),
            Arc::clone(&ownership),
        ));

        let waits = Arc::new(Waits::new(Arc::clone(&store)));
        let cancellations = Arc::new(CancellationRegistry::new());

        // The agent owns the scheduler and passes itself to drive children,
        // so nothing points back at the agent.
        let scheduler = Arc::new(Scheduler::new(
            Arc::clone(&store),
            Arc::clone(&waits),
            config.model_aliases.clone(),
            Arc::clone(&config.clock),
            config.max_live_children,
            Arc::clone(&cancellations),
        ));
        let tools = Arc::new(ToolTable::new());
        if config.spawn_tool {
            tools.register(Arc::new(SpawnTool::new()));
        }
        for tool in config.tools {
            tools.register(tool);
        }

        let gate = Arc::clone(&config.gate);
        let approval = Arc::clone(&config.approval);
        let clock = Arc::clone(&config.clock);

        let lookup_usage = Arc::clone(&usage);
        let usage_lookup: UsageLookup = Arc::new(move |session| {
            let usage = Arc::clone(&lookup_usage);
            Box::pin(async move {
                usage
                    .totals(totals_filter(UsageScope::SessionSubtree(session)))
                    .await
            })
        });
        let agent = Arc::new(
            Agent::new(
                Arc::clone(&store),
                Arc::clone(&providers),
                Arc::clone(&tools),
                config.tool.clone(),
                approval,
                Arc::clone(&gate),
                Arc::clone(&waits),
                Arc::clone(&config.clock),
            )
            .with_hooks(config.hooks, Some(usage_lookup))
            .with_compactor(config.compactor.clone())
            .with_cancellations(Arc::clone(&cancellations))
            .with_scheduler(scheduler)
            .with_watchers(Arc::clone(&watchers)),
        );

        Ok(Self {
            store,
            providers,
            tools,
            agent,
            usage,
            waits,
            gate,
            cancellations,
            clock,
            trace,
            environment: config.environment,
            watchers,
            ownership,
        })
    }

    pub async fn create_session(&self, spec: SessionSpec) -> Result<SessionHandle, AsterError> {
        if self.providers.get(&spec.model.provider).is_none() {
            return Err(AsterError::Config {
                message: format!("unknown provider {}", spec.model.provider),
            });
        }
        if spec.turn.max_steps_per_turn == Some(0) {
            return Err(AsterError::Config {
                message: "max_steps_per_turn must be greater than zero".to_owned(),
            });
        }
        check_system(&spec.system, &spec.system_sections)?;
        check_metadata(&spec.metadata)?;
        let env = self.resolve(&spec.metadata).await?;
        let lookup = |name: &_| environment::tool(&self.tools, env.as_deref(), name);
        agent::output::check(&spec.request, |name| lookup(name).is_some())
            .map_err(|message| AsterError::Config { message })?;
        let tools = match &spec.tools {
            ToolSelection::All => environment::tools(&self.tools, env.as_deref())
                .iter()
                .map(|tool| tool.descriptor().clone())
                .collect(),
            ToolSelection::Names(names) => {
                let mut selected = Vec::with_capacity(names.len());
                for name in names {
                    let tool = lookup(name).ok_or_else(|| AsterError::Config {
                        message: format!("unknown tool {}", name.as_str()),
                    })?;
                    selected.push(tool.descriptor().clone());
                }
                selected
            }
        };

        let session = SessionId::from_bytes(unique_bytes(self.clock.now()));
        let event = SessionEvent::SessionCreated {
            config: spec.snapshot(tools),
            parent: None,
            metadata: spec.metadata,
        };
        let entry = asterpath_protocol::encode_entry(
            session,
            Some(dedup::session_created(session)),
            &event,
        )
        .map_err(|error| AsterError::Config {
            message: error.to_string(),
        })?;
        self.store.append(entry).await?;
        Ok(self.handle(session, env))
    }

    /// Fork `source` into a new, parentless session (ADR-0033). The snapshot
    /// is the source's projection at the end of its last committed turn, so a
    /// turn in progress is not copied and `source` may be driven meanwhile.
    /// The config is the source's effective config there, unnarrowed; the
    /// copied history drops tool changes. `metadata` is the application's, not
    /// inherited. Unknown source = `NotFound`; metadata over the limits =
    /// `Config`.
    pub async fn fork_session(
        &self,
        source: SessionId,
        metadata: BTreeMap<String, String>,
    ) -> Result<SessionHandle, AsterError> {
        check_metadata(&metadata)?;
        let env = self.resolve(&metadata).await?;
        let records = scan::scan_decoded(&*self.store, source)
            .await
            .map_err(AgentError::from)?;
        let not_found = || AsterError::Agent(AgentError::NotFound { session: source });
        let corrupt = |detail: String| {
            AsterError::Agent(AgentError::Corrupt {
                session: source,
                detail,
            })
        };
        let (waterline, prefix) = fork::session_prefix(&records).ok_or_else(not_found)?;
        let snapshot = fork::snapshot(&prefix).map_err(|e| corrupt(e.to_string()))?;
        let mut config =
            asterpath_protocol::effective_config(&records, waterline.seq).ok_or_else(not_found)?;
        config.system = snapshot.system;
        config.system_sections = snapshot.system_sections;
        let items = snapshot.items;

        let session = SessionId::from_bytes(unique_bytes(self.clock.now()));
        // shortcut: two appends, so a crash between them leaves an empty fork that, being the newest, an application "continue the last active session" (CLI `--continue`) picks; needs a batch append in SessionStore
        for (key, event) in [
            (
                dedup::session_created(session),
                SessionEvent::SessionCreated {
                    config,
                    parent: None,
                    metadata,
                },
            ),
            (
                dedup::context_forked(session),
                SessionEvent::ContextForked {
                    source: ForkSource::Session { session: source },
                    source_log: waterline.log_id,
                    items,
                },
            ),
        ] {
            let entry = asterpath_protocol::encode_entry(session, Some(key), &event)
                .map_err(|error| corrupt(error.to_string()))?;
            self.store.append(entry).await?;
        }
        Ok(self.handle(session, env))
    }

    fn handle(&self, session: SessionId, env: Option<Arc<SessionEnvironment>>) -> SessionHandle {
        SessionHandle {
            session,
            env,
            store: Arc::clone(&self.store),
            providers: Arc::clone(&self.providers),
            tools: Arc::clone(&self.tools),
            agent: Arc::clone(&self.agent),
            waits: Arc::clone(&self.waits),
            cancellations: Arc::clone(&self.cancellations),
            watchers: Arc::clone(&self.watchers),
            ownership: Arc::clone(&self.ownership),
        }
    }

    /// A read-only handle; [`SessionHandle::claim`] makes it drivable.
    pub async fn open_session(&self, id: SessionId) -> Result<SessionHandle, AsterError> {
        // Unknown and retired sessions are indistinguishable: both have no
        // committed frontier (retention.md §9).
        if self.store.frontier(id).await?.is_none() {
            return Err(AsterError::Agent(AgentError::NotFound { session: id }));
        }
        // A child uses its root's environment (ADR-0037).
        let env = match &self.environment {
            Some(_) => {
                let root = root_of(&*self.store, id).await?;
                self.resolve(&created_metadata(&*self.store, root).await?)
                    .await?
            }
            None => None,
        };
        Ok(self.handle(id, env))
    }

    /// The session environment for root metadata (ADR-0037); `None` without
    /// an adapter.
    async fn resolve(
        &self,
        metadata: &BTreeMap<String, String>,
    ) -> Result<Option<Arc<SessionEnvironment>>, AsterError> {
        let Some(adapter) = &self.environment else {
            return Ok(None);
        };
        let env = adapter
            .resolve(metadata)
            .await
            .map_err(|error| AsterError::Config {
                message: error.to_string(),
            })?;
        check_tool_names(&env.tools)?;
        Ok(Some(Arc::new(env)))
    }

    /// Release the engine. The only background work is the process lease
    /// renewal, which stops once the engine and its handles are dropped;
    /// the store then clears the lease and frees this process's trees.
    pub async fn shutdown(self) -> Result<(), AsterError> {
        Ok(())
    }

    /// The session store. Appends made here reach the `on_record` hooks;
    /// retirement is `store().retire(...)` (`Unsupported` when the injected
    /// store has none).
    pub fn store(&self) -> Arc<dyn SessionStore> {
        Arc::clone(&self.store)
    }

    /// One page of the session catalog, session metadata included (ADR-0032):
    /// filtered by parent and by metadata equality (ADR-0039), newest
    /// `(created_at, session)` first, resumed after `query.after`.
    pub async fn sessions(&self, query: SessionQuery) -> Result<Vec<CatalogEntry>, AsterError> {
        Ok(self.store.sessions(query).await?)
    }

    /// The live claim of `session`'s tree, held by this or another process;
    /// `None` when nobody owns it (ADR-0036). Applications route cancellation and
    /// other owner operations by it.
    pub async fn owner(&self, session: SessionId) -> Result<Option<OwnerToken>, AsterError> {
        let root = root_of(&*self.store, session).await?;
        Ok(self.store.owner(root).await?)
    }

    /// Unowned sessions with unfinished work (ADR-0023, ADR-0036): a
    /// non-compaction intent is open or an input waits for its step, and no
    /// process, this one included, holds the session's tree. Retired
    /// sessions are never listed. The application decides which to claim and drive.
    pub async fn wakeable(&self) -> Result<Vec<SessionId>, AsterError> {
        // shortcut: folds every session on each call; index inside the sqlite module once session counts grow
        let mut wakeable = Vec::new();
        let catalog = self.store.sessions(SessionQuery::default()).await?;
        let parents: BTreeMap<SessionId, Option<SessionId>> = catalog
            .iter()
            .map(|entry| (entry.session, entry.parent))
            .collect();
        // Id order, as before the catalog listed newest first.
        let mut sessions: Vec<SessionId> = parents.keys().copied().collect();
        sessions.sort();
        let mut owned = BTreeMap::new();
        for session in sessions {
            let mut root = session;
            while let Some(Some(parent)) = parents.get(&root) {
                root = *parent;
            }
            let held = match owned.get(&root) {
                Some(held) => *held,
                None => {
                    let held = self.store.owner(root).await?.is_some();
                    owned.insert(root, held);
                    held
                }
            };
            if held {
                continue;
            }
            let entries = scan_all(&*self.store, session).await?;
            let facts = SessionFacts::fold(&entries).map_err(corrupt(session))?;
            if facts.is_wakeable() {
                wakeable.push(session);
            }
        }
        Ok(wakeable)
    }

    /// Committed usage of `scope`'s session; `SessionSubtree` also reports
    /// its descendants. A pure fold over the event log.
    pub async fn usage(&self, scope: UsageScope) -> Result<UsageReport, UsageError> {
        self.usage.totals(totals_filter(scope)).await
    }

    /// Register an RAII endpoint per session tree (its root id) while the
    /// application can answer that tree's approvals (ADR-0039).
    pub fn approval_gate(&self) -> Arc<ApprovalGate> {
        Arc::clone(&self.gate)
    }

    /// The session's spans: a deterministic fold over its event log. Live
    /// export is the application's own fold over `Hooks::on_record`.
    pub async fn trace(&self, session: SessionId) -> Result<Vec<Span>, TraceError> {
        self.trace
            .spans(SpanFilter {
                session: Some(session),
                ..SpanFilter::default()
            })
            .try_collect()
            .await
    }
}

/// The metadata `session` was created with.
async fn created_metadata(
    store: &dyn SessionStore,
    session: SessionId,
) -> Result<BTreeMap<String, String>, AsterError> {
    let first = store
        .scan(LogQuery {
            session,
            after: None,
            limit: Some(1),
        })
        .next()
        .await
        .transpose()?;
    match first.as_ref().map(asterpath_protocol::decode_entry) {
        Some(Ok(SessionEvent::SessionCreated { metadata, .. })) => Ok(metadata),
        _ => Err(AsterError::Agent(AgentError::Corrupt {
            session,
            detail: "the log does not start with SessionCreated".to_owned(),
        })),
    }
}

/// Names a tool may be registered under; `Unknown` and `Output` are
/// intent-only and never registrable.
fn check_tool_names(tools: &[Arc<dyn Tool>]) -> Result<(), AsterError> {
    for tool in tools {
        let name = tool.descriptor().name.as_str();
        let validated = match tool.descriptor().provenance {
            asterpath_protocol::Provenance::InProcess => {
                asterpath_protocol::validate_tool_name(name)
            }
            asterpath_protocol::Provenance::External { .. } => {
                asterpath_protocol::validate_external_tool_name(name)
            }
            asterpath_protocol::Provenance::Unknown | asterpath_protocol::Provenance::Output => {
                Err(asterpath_protocol::ToolNameError::InvalidToolName)
            }
        };
        validated.map_err(|error| AsterError::Config {
            message: format!("invalid tool name {name}: {error}"),
        })?;
    }
    Ok(())
}

/// Session metadata limits (storage.md §5).
fn check_metadata(metadata: &BTreeMap<String, String>) -> Result<(), AsterError> {
    if metadata.len() > 64
        || metadata
            .iter()
            .any(|(key, value)| key.len() > 128 || value.len() > 4096)
    {
        return Err(AsterError::Config {
            message: "metadata allows at most 64 entries, keys up to 128 bytes and values \
                      up to 4096 bytes"
                .to_owned(),
        });
    }
    Ok(())
}

/// Totals over `scope` with no further narrowing.
fn totals_filter(scope: UsageScope) -> UsageFilter {
    UsageFilter {
        scope,
        bucket: None,
        model: None,
        since: None,
        until: None,
        limit: None,
    }
}

/// One session, read-only (ADR-0036): its log-derived views plus the
/// external writes any process may make (input, approval decisions, config
/// changes). [`claim`](Self::claim) its tree to drive it.
#[derive(Clone)]
pub struct SessionHandle {
    session: SessionId,
    /// Resolved when the handle was made; `None` without an adapter.
    env: Option<Arc<SessionEnvironment>>,
    store: Arc<dyn SessionStore>,
    providers: Arc<ProviderCatalog>,
    tools: Arc<ToolTable>,
    agent: Arc<Agent>,
    waits: Arc<Waits>,
    cancellations: Arc<CancellationRegistry>,
    watchers: Arc<Watchers>,
    ownership: Arc<owner::Ownership>,
}

impl std::fmt::Debug for SessionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionHandle")
            .field("session", &self.session)
            .finish_non_exhaustive()
    }
}

impl SessionHandle {
    pub fn id(&self) -> SessionId {
        self.session
    }

    /// Take ownership of this session's tree (its root and every child).
    /// Claims from this `Asterpath` share the tree; another process holding
    /// it is
    /// [`StorageError::OwnedElsewhere`](asterpath_protocol::StorageError::OwnedElsewhere). Dropping the last
    /// [`OwnedSession`] releases the tree. The claim lives while this
    /// process renews its lease; if it is lost, `Hooks::on_ownership_lost`
    /// is told and owner operations fail with `OwnedElsewhere`.
    pub async fn claim(&self) -> Result<OwnedSession, AsterError> {
        let root = root_of(&*self.store, self.session).await?;
        let held = self.ownership.claim(root).await?;
        let owned = OwnedSession {
            handle: self.clone(),
            held,
        };
        owned.follow_environment().await?;
        Ok(owned)
    }

    /// The live tool of that name for this session.
    fn tool(&self, name: &asterpath_protocol::ToolName) -> Option<Arc<dyn Tool>> {
        environment::tool(&self.tools, self.env.as_deref(), name)
    }

    /// Append `InputRecorded` only. Never starts a turn.
    pub async fn submit_input(&self, input: UserInput) -> Result<InputId, AsterError> {
        let event = SessionEvent::InputRecorded { input: input.parts };
        let entry = asterpath_protocol::encode_entry(
            self.session,
            Some(dedup::input_recorded(input.id)),
            &event,
        )
        .map_err(|error| AsterError::Config {
            message: error.to_string(),
        })?;
        self.store.append(entry).await?;
        Ok(input.id)
    }

    /// Change the session's model or request parameters, append system
    /// instructions, set or remove keyed system sections, or add and remove
    /// tools (ADR-0030, ADR-0057). Validated first: an unregistered provider;
    /// an `append_system`, `system_sections`, `add_tools` or `remove_tools`
    /// without `id`; non-text system parts; an empty or repeated section key; an added tool that
    /// is not registered on this `Asterpath`, differs from the registered
    /// descriptor or is already effective; a removed tool that is not
    /// effective. Each is a `Config` error and nothing is logged. An update
    /// with an `id` is recorded once however often it is retried. Otherwise
    /// one `ConfigChanged` is appended; it may be called mid-turn and takes
    /// effect at the next `StepStarted`, so a step already running keeps its
    /// config.
    pub async fn update_config(&self, patch: ConfigPatch) -> Result<(), AsterError> {
        if let Some(model) = &patch.model
            && self.providers.get(&model.provider).is_none()
        {
            return Err(AsterError::Config {
                message: format!("unknown provider {}", model.provider),
            });
        }
        if patch.is_positional() && patch.id.is_none() {
            return Err(AsterError::Config {
                message: "append_system, system_sections, add_tools and remove_tools \
                          need an update id: they are not idempotent"
                    .to_owned(),
            });
        }
        check_system(&patch.append_system, &patch.system_sections)?;
        if let Some(request) = &patch.request {
            agent::output::check(request, |name| self.tool(name).is_some())
                .map_err(|message| AsterError::Config { message })?;
        }
        let key = patch.id.map(dedup::config_changed);
        if patch.changes_tools() {
            let records = scan::scan_decoded(&*self.store, self.session)
                .await
                .map_err(AgentError::from)?;
            // A retry of a recorded change is a no-op, not a conflict with itself.
            // Dedup keys are scoped by record kind.
            if key.is_some()
                && records.iter().any(|(entry, event)| {
                    matches!(event, SessionEvent::ConfigChanged { .. }) && entry.dedup_key == key
                })
            {
                return Ok(());
            }
            let effective = asterpath_protocol::effective_config(&records, u64::MAX).ok_or(
                AsterError::Agent(AgentError::NotFound {
                    session: self.session,
                }),
            )?;
            self.check_tool_change(&patch, &effective.tools)?;
        }
        // Without an id a retry may land twice; whole-field overrides fold the same.
        let entry = asterpath_protocol::encode_entry(
            self.session,
            key,
            &SessionEvent::ConfigChanged { patch },
        )
        .map_err(|error| AsterError::Config {
            message: error.to_string(),
        })?;
        self.store.append(entry).await?;
        Ok(())
    }

    /// Added tools must be registered with this exact descriptor and not yet
    /// effective; removed tools must be effective.
    fn check_tool_change(
        &self,
        patch: &ConfigPatch,
        effective: &[asterpath_protocol::ToolDescriptor],
    ) -> Result<(), AsterError> {
        let error = |message: String| Err(AsterError::Config { message });
        let is_effective =
            |name: &asterpath_protocol::ToolName| effective.iter().any(|tool| &tool.name == name);
        for (index, added) in patch.add_tools.iter().enumerate() {
            let name = &added.name;
            match self.tool(name) {
                None => return error(format!("unknown tool {name}")),
                Some(tool) if tool.descriptor() != added => {
                    return error(format!(
                        "tool {name} differs from the registered descriptor"
                    ));
                }
                Some(_) => {}
            }
            if is_effective(name) || patch.add_tools[..index].iter().any(|t| &t.name == name) {
                return error(format!("tool {name} is already in the tool set"));
            }
        }
        for name in &patch.remove_tools {
            if !is_effective(name) {
                return error(format!("tool {name} is not in the tool set"));
            }
        }
        Ok(())
    }

    /// Answer a pending `Ask` as the approval endpoint. The decision is a
    /// recorded `ApprovalDecided`, so it survives a crash and is reused.
    pub async fn approve(
        &self,
        call: ToolCallId,
        decision: ApprovalDecision,
    ) -> Result<(), AsterError> {
        self.waits
            .record_approval(self.session, call, decision)
            .await?;
        Ok(())
    }

    /// The session facts folded from the log, not driver memory.
    pub async fn facts(&self) -> Result<SessionFacts, AsterError> {
        let entries = scan_all(&*self.store, self.session).await?;
        Ok(SessionFacts::fold(&entries).map_err(corrupt(self.session))?)
    }

    /// The model-visible projection folded from the log, not driver memory.
    /// Each entry keeps its origin so applications can map items back to log ids.
    pub async fn projection(&self) -> Result<Vec<ProjectedEntry>, AsterError> {
        let entries = scan_all(&*self.store, self.session).await?;
        Ok(asterpath_protocol::project(&entries).map_err(corrupt(self.session))?)
    }

    /// Observe the session without driving it (ADR-0038): every committed
    /// record after `after` (the whole log for `None`), then
    /// [`WatchEvent::Synced`], then each new commit as it lands. A drive in
    /// this process, of this session or of it as a child, also yields its
    /// live events ([`WatchEvent::Live`]: text deltas and the like), which
    /// are never replayed. No claim is needed; from another process only
    /// records arrive, within the store poll interval. The stream never
    /// ends; drop it to stop watching. Resume after the last record's
    /// `log_id` to neither miss nor repeat one.
    pub fn watch(
        &self,
        after: Option<LogId>,
    ) -> BoxStream<'static, Result<WatchEvent, AsterError>> {
        watch::watch(Arc::clone(&self.store), &self.watchers, self.session, after)
    }

    /// State is the log-derived classification, not driver memory.
    pub async fn state(&self) -> Result<SessionState, AsterError> {
        Ok(self.facts().await?.state())
    }
}

/// A claimed session: ownership of its tree for as long as any clone lives
/// (ADR-0036). Driving, manual compaction, cancellation and child scheduling
/// happen only here; everything a [`SessionHandle`] does is available too.
#[derive(Clone)]
pub struct OwnedSession {
    handle: SessionHandle,
    held: Arc<owner::Held>,
}

impl std::fmt::Debug for OwnedSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedSession")
            .field("session", &self.handle.session)
            .field("owner", &self.held.token())
            .finish()
    }
}

impl std::ops::Deref for OwnedSession {
    type Target = SessionHandle;

    fn deref(&self) -> &SessionHandle {
        &self.handle
    }
}

impl OwnedSession {
    /// Bring the effective tool set in line with the resolved environment
    /// (ADR-0037, ADR-0030): a tool whose descriptor changed is replaced,
    /// one that no longer resolves is removed. Tools are never added; that
    /// stays the application's `update_config`.
    async fn follow_environment(&self) -> Result<(), AsterError> {
        if self.env.is_none() {
            return Ok(());
        }
        let records = scan::scan_decoded(&*self.store, self.session)
            .await
            .map_err(AgentError::from)?;
        let effective = asterpath_protocol::effective_config(&records, u64::MAX).ok_or(
            AsterError::Agent(AgentError::NotFound {
                session: self.session,
            }),
        )?;
        let mut patch = ConfigPatch::default();
        for recorded in &effective.tools {
            match self.tool(&recorded.name) {
                None => patch.remove_tools.push(recorded.name.clone()),
                Some(live) if live.descriptor() != recorded => {
                    patch.add_tools.push(live.descriptor().clone());
                }
                Some(_) => {}
            }
        }
        if !patch.changes_tools() {
            return Ok(());
        }
        // Folds the same if a concurrent claim in this process appends it too.
        let entry = asterpath_protocol::encode_entry(
            self.session,
            None,
            &SessionEvent::ConfigChanged { patch },
        )
        .map_err(|error| AsterError::Config {
            message: error.to_string(),
        })?;
        self.store.append(entry).await?;
        Ok(())
    }

    /// The claim this handle holds: the tree's root and its epoch.
    pub fn owner(&self) -> OwnerToken {
        self.held.token()
    }

    /// Stream live `AgentEvent`s until `TurnCommitted` or `Idle`.
    ///
    /// The turn advances only while this stream is polled. Dropping it does
    /// not cancel anything: the next `drive` resumes from the log (ADR-0006).
    /// A second `drive` on a session that is already being driven yields
    /// [`AgentError::Busy`]. If this process loses the tree (ADR-0036) the
    /// stream stops at once and ends with
    /// [`StorageError::OwnedElsewhere`](asterpath_protocol::StorageError::OwnedElsewhere).
    ///
    /// # Application driver pattern
    ///
    /// - **Do not tie the stream to a web request or connection.** When the
    ///   client disconnects the stream is dropped, and the turn stops until
    ///   someone drives again.
    /// - **Drive from a spawned task.** `tokio::spawn` a task that owns a
    ///   cloned `OwnedSession` (so the process keeps the tree) and reads
    ///   every `drive()` stream to the end. Views
    ///   [`watch`](SessionHandle::watch) the session instead: they see the
    ///   same live events and come and go without affecting the turn.
    /// - **Queue through the log, not by retrying `Busy`.** Keep at most one
    ///   driver task per session. New input only needs
    ///   [`submit_input`](SessionHandle::submit_input) plus a wake-up; after each
    ///   drive the task drives again while
    ///   [`SessionFacts::has_pending_input`] holds.
    /// - **Reconnect by watching again** after the last record it saw. An
    ///   `ApprovalRequested` it missed is
    ///   still listed by [`ApprovalGate::pending_in`] for the tree's root (via
    ///   [`Asterpath::approval_gate`]), and a re-drive after a dropped stream
    ///   emits it again.
    ///
    /// ```no_run
    /// use std::sync::Arc;
    ///
    /// use asterpath::{OwnedSession, SessionHandle};
    /// use futures_util::StreamExt;
    /// use tokio::sync::Notify;
    ///
    /// /// One per session. `wake` is notified after every `submit_input`.
    /// fn spawn_driver(handle: OwnedSession, wake: Arc<Notify>) {
    ///     tokio::spawn(async move {
    ///         loop {
    ///             let mut stream = handle.drive();
    ///             while let Some(event) = stream.next().await {
    ///                 // Watchers get the events; the driver only checks errors.
    ///                 if let Err(error) = event {
    ///                     eprintln!("drive failed: {error}");
    ///                     return;
    ///                 }
    ///             }
    ///             let pending = handle
    ///                 .facts()
    ///                 .await
    ///                 .map(|facts| facts.has_pending_input())
    ///                 .unwrap_or(false);
    ///             if !pending {
    ///                 // Notify keeps a permit, so input submitted since the
    ///                 // check is not lost.
    ///                 wake.notified().await;
    ///             }
    ///         }
    ///     });
    /// }
    ///
    /// async fn on_user_message(
    ///     handle: &SessionHandle,
    ///     wake: &Notify,
    ///     input: asterpath::protocol::UserInput,
    /// ) -> Result<(), asterpath::AsterError> {
    ///     handle.submit_input(input).await?;
    ///     wake.notify_one();
    ///     Ok(())
    /// }
    /// ```
    ///
    /// [`AgentError::Busy`]: asterpath_protocol::AgentError::Busy
    /// [`SessionFacts::has_pending_input`]: asterpath_protocol::SessionFacts::has_pending_input
    /// [`ApprovalGate::pending_in`]: asterpath_protocol::ApprovalGate::pending_in
    pub fn drive(&self) -> BoxStream<'_, Result<AgentEvent, AsterError>> {
        let run = self.agent.run_as(
            RunRequest {
                session: self.session,
                trigger: RunTrigger::Resume,
            },
            Some(self.owner()),
            self.env.clone(),
        );
        let mut run = Some(run.map(|event| event.map_err(AsterError::from)));
        let mut lost = Box::pin(self.held.lost());
        let root = self.owner().root;
        // Dropping the run stops its turn, waits and child drives.
        futures_util::stream::poll_fn(move |cx| {
            let Some(active) = run.as_mut() else {
                return std::task::Poll::Ready(None);
            };
            if lost.as_mut().poll(cx).is_ready() {
                run = None;
                return std::task::Poll::Ready(Some(Err(AsterError::Storage(
                    asterpath_protocol::StorageError::OwnedElsewhere { root },
                ))));
            }
            let next = active.poll_next_unpin(cx);
            if matches!(
                next,
                std::task::Poll::Ready(
                    None | Some(Err(AsterError::Storage(
                        asterpath_protocol::StorageError::OwnedElsewhere { .. }
                    )))
                )
            ) {
                run = None;
            }
            next
        })
        .boxed()
    }

    /// Cancel the active turn. Idempotent; a no-op while idle.
    pub async fn cancel(&self) -> Result<(), AsterError> {
        self.cancellations.cancel(self.session);
        Ok(())
    }

    /// Run one manual compaction. Only allowed while idle and undriven.
    pub async fn compact_now(&self) -> Result<CompactResult, AsterError> {
        let compaction = owner::Owned {
            owner: self.owner(),
            future: self.agent.compact_now(self.session),
        };
        Ok(compaction.await?)
    }
}

/// A log of `session` that does not fold, as `AgentError::Corrupt`.
pub(crate) fn corrupt<E: std::fmt::Display>(session: SessionId) -> impl FnOnce(E) -> AgentError {
    move |error| AgentError::Corrupt {
        session,
        detail: error.to_string(),
    }
}

/// System parts must be text; section keys must be non-empty and distinct.
fn check_system(
    system: &[asterpath_protocol::Part],
    sections: &[asterpath_protocol::SystemSection],
) -> Result<(), AsterError> {
    let config = |message: &str| {
        Err(AsterError::Config {
            message: message.to_owned(),
        })
    };
    if !system
        .iter()
        .chain(asterpath_protocol::SystemSection::flatten(sections))
        .all(|part| matches!(part, asterpath_protocol::Part::Text { .. }))
    {
        return config("system may only contain text parts");
    }
    for (index, section) in sections.iter().enumerate() {
        if section.key.is_empty() || sections[..index].iter().any(|s| s.key == section.key) {
            return config("system section keys must be non-empty and distinct");
        }
    }
    Ok(())
}
