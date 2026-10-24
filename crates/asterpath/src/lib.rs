//! Asterpath facade: the composition root and the stable session API.
//!
//! The facade constructs the engine components directly; hosts extend it only
//! through adapter traits and [`protocol::Hooks`] (ADR-0025). There is no
//! background driver: the host calls [`SessionHandle::drive`].

// The public error enum is fixed by packaging.md §3.5.
#![allow(clippy::result_large_err)]

mod agent;
mod commit;
mod compaction;
pub mod config;
pub mod error;
mod scan;
mod scheduler;
mod tools;
mod views;
mod waits;

pub use compaction::DefaultCompactor;

/// The contract crate: adapter traits, hooks and value types.
pub use asterpath_protocol as protocol;
/// Core id types, so embedders need only this crate.
pub use asterpath_protocol::{InputId, LogId, SessionId, StepId, Timestamp, ToolCallId, TurnId};

/// Re-export of the Anthropic adapter when the `anthropic` feature is enabled.
/// Providers stay host-injected: the facade never assembles one itself.
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
use asterpath_protocol::scan_all;
use asterpath_protocol::{
    AgentError, AgentEvent, ApprovalDecision, CancellationRegistry, CompactResult, ConfigPatch,
    ForkSource, Item, ProjectedEntry, ProviderCatalog, RunRequest, RunTrigger, SessionEvent,
    SessionFacts, SessionSpec, SessionState, Span, SpanFilter, ToolSelection, TraceError,
    UsageError, UsageFilter, UsageLookup, UsageReport, UsageScope, UserInput, dedup,
};
use asterpath_protocol::{
    ApprovalGate, CatalogEntry, Clock, SessionQuery, SessionStore, unique_bytes,
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
        for tool in &config.tools {
            let name = tool.descriptor().name.as_str();
            let validated = match tool.descriptor().provenance {
                asterpath_protocol::Provenance::InProcess => {
                    asterpath_protocol::validate_tool_name(name)
                }
                asterpath_protocol::Provenance::External { .. } => {
                    asterpath_protocol::validate_external_tool_name(name)
                }
                // `Unknown` is an intent-only provenance; it is not registrable.
                asterpath_protocol::Provenance::Unknown => {
                    Err(asterpath_protocol::ToolNameError::InvalidToolName)
                }
            };
            validated.map_err(|error| AsterError::Config {
                message: format!("invalid tool name {name}: {error}"),
            })?;
        }

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
        let store: Arc<dyn SessionStore> =
            Arc::new(commit::CommitLog::new(backend, config.hooks.clone()));

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
            .with_scheduler(scheduler),
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
        if !spec
            .system
            .iter()
            .all(|part| matches!(part, asterpath_protocol::Part::Text { .. }))
        {
            return Err(AsterError::Config {
                message: "system may only contain text parts".to_owned(),
            });
        }
        check_metadata(&spec.metadata)?;
        let tools = match &spec.tools {
            ToolSelection::All => self
                .tools
                .list()
                .iter()
                .map(|tool| tool.descriptor().clone())
                .collect(),
            ToolSelection::Names(names) => {
                let mut selected = Vec::with_capacity(names.len());
                for name in names {
                    let tool = self.tools.get(name).ok_or_else(|| AsterError::Config {
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
        Ok(self.handle(session))
    }

    /// Fork `source` into a new, parentless session (ADR-0033). The snapshot
    /// is the source's projection at the end of its last committed turn, so a
    /// turn in progress is not copied and `source` may be driven meanwhile.
    /// The config is the source's effective config there, unnarrowed; the
    /// copied history drops tool changes. `metadata` is the host's, not
    /// inherited. Unknown source = `NotFound`; metadata over the limits =
    /// `Config`.
    pub async fn fork_session(
        &self,
        source: SessionId,
        metadata: BTreeMap<String, String>,
    ) -> Result<SessionHandle, AsterError> {
        check_metadata(&metadata)?;
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
        // The last committed turn's end, or creation when none has committed.
        let waterline = records
            .iter()
            .rev()
            .find(|(_, event)| matches!(event, SessionEvent::TurnCommitted { .. }))
            .or_else(|| records.first())
            .map(|(entry, _)| entry.clone())
            .ok_or_else(not_found)?;
        // Inputs after the last step started belong to the next turn.
        let last_step = records
            .iter()
            .filter(|(entry, event)| {
                entry.seq <= waterline.seq && matches!(event, SessionEvent::StepStarted { .. })
            })
            .map(|(entry, _)| entry.seq)
            .max()
            .unwrap_or(0);
        // Compactions fold regardless of the waterline, as for a spawn fork.
        let prefix: Vec<_> = records
            .iter()
            .filter(|(entry, event)| match event {
                SessionEvent::CompactionApplied { .. } => true,
                SessionEvent::InputRecorded { .. } => entry.seq <= last_step,
                _ => entry.seq <= waterline.seq,
            })
            .map(|(entry, _)| entry.clone())
            .collect();
        let history =
            asterpath_protocol::request_history(&prefix).map_err(|e| corrupt(e.to_string()))?;
        let mut config =
            asterpath_protocol::effective_config(&records, waterline.seq).ok_or_else(not_found)?;
        // The copied history already holds the instructions appended so far.
        config.system = history.system;
        let mut items = history.items;
        items.retain(|item| !matches!(item, Item::ToolsChanged { .. }));

        let session = SessionId::from_bytes(unique_bytes(self.clock.now()));
        // shortcut: two appends, so a crash between them leaves an empty fork; needs a batch append in SessionStore
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
        Ok(self.handle(session))
    }

    fn handle(&self, session: SessionId) -> SessionHandle {
        SessionHandle {
            session,
            store: Arc::clone(&self.store),
            providers: Arc::clone(&self.providers),
            tools: Arc::clone(&self.tools),
            agent: Arc::clone(&self.agent),
            waits: Arc::clone(&self.waits),
            cancellations: Arc::clone(&self.cancellations),
        }
    }

    pub async fn open_session(&self, id: SessionId) -> Result<SessionHandle, AsterError> {
        // Unknown and retired sessions are indistinguishable: both have no
        // committed frontier (retention.md §9).
        if self.store.frontier(id).await?.is_none() {
            return Err(AsterError::Agent(AgentError::NotFound { session: id }));
        }
        Ok(self.handle(id))
    }

    /// Release the engine. Nothing runs in the background, so there is
    /// nothing to await.
    pub async fn shutdown(self) -> Result<(), AsterError> {
        Ok(())
    }

    /// The session store. Appends made here reach the `on_record` hooks;
    /// retirement is `store().retire(...)` (`Unsupported` when the injected
    /// store has none).
    pub fn store(&self) -> Arc<dyn SessionStore> {
        Arc::clone(&self.store)
    }

    /// One page of the session catalog, host metadata included (ADR-0032):
    /// filtered by parent, newest `(created_at, session)` first, resumed
    /// after `query.after`. Sorting or filtering by metadata is the host's.
    pub async fn sessions(&self, query: SessionQuery) -> Result<Vec<CatalogEntry>, AsterError> {
        Ok(self.store.sessions(query).await?)
    }

    /// Sessions with unfinished work (ADR-0023): a non-compaction intent is
    /// open or an input waits for its step. Retired sessions are never
    /// listed. The host decides when to drive them.
    pub async fn wakeable(&self) -> Result<Vec<SessionId>, AsterError> {
        // shortcut: folds every session on each call; index inside the sqlite module once session counts grow
        let mut wakeable = Vec::new();
        let catalog = self.store.sessions(SessionQuery::default()).await?;
        // Id order, as before the catalog listed newest first.
        let mut sessions: Vec<SessionId> = catalog.iter().map(|entry| entry.session).collect();
        sessions.sort();
        for session in sessions {
            let entries = scan_all(&*self.store, session).await?;
            let facts = SessionFacts::fold(&entries).map_err(|error| {
                AsterError::Agent(AgentError::Corrupt {
                    session,
                    detail: error.to_string(),
                })
            })?;
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

    /// Register an RAII endpoint here while the host can answer approvals.
    pub fn approval_gate(&self) -> Arc<ApprovalGate> {
        Arc::clone(&self.gate)
    }

    /// The session's spans: a deterministic fold over its event log. Live
    /// export is the host's own fold over `Hooks::on_record`.
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

/// Host metadata limits (storage.md §5).
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

/// One session: submit inputs, then drive explicitly.
#[derive(Clone)]
pub struct SessionHandle {
    session: SessionId,
    store: Arc<dyn SessionStore>,
    providers: Arc<ProviderCatalog>,
    tools: Arc<ToolTable>,
    agent: Arc<Agent>,
    waits: Arc<Waits>,
    cancellations: Arc<CancellationRegistry>,
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
    /// instructions, or add and remove tools (ADR-0030). Validated first: an
    /// unregistered provider; an `append_system`, `add_tools` or
    /// `remove_tools` without `id`; non-text system parts; an added tool that
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
                message: "append_system, add_tools and remove_tools need an update id: \
                          they are not idempotent"
                    .to_owned(),
            });
        }
        if !patch
            .append_system
            .iter()
            .all(|part| matches!(part, asterpath_protocol::Part::Text { .. }))
        {
            return Err(AsterError::Config {
                message: "system may only contain text parts".to_owned(),
            });
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
            match self.tools.get(name) {
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

    /// Stream live `AgentEvent`s until `TurnCommitted` or `Idle`.
    ///
    /// The turn advances only while this stream is polled. Dropping it does
    /// not cancel anything: the next `drive` resumes from the log (ADR-0006).
    /// A second `drive` on a session that is already being driven yields
    /// [`AgentError::Busy`].
    ///
    /// # Host driver pattern
    ///
    /// - **Do not tie the stream to a web request or connection.** When the
    ///   client disconnects the stream is dropped, and the turn stops until
    ///   someone drives again.
    /// - **Drive from a spawned task.** `tokio::spawn` a task that owns a
    ///   cloned `SessionHandle`, reads every `drive()` stream to the end, and
    ///   fans `AgentEvent`s out over `tokio::sync::broadcast`. Subscribers
    ///   come and go without affecting the turn.
    /// - **Queue through the log, not by retrying `Busy`.** Keep at most one
    ///   driver task per session. New input only needs
    ///   [`submit_input`](Self::submit_input) plus a wake-up; after each
    ///   drive the task drives again while
    ///   [`SessionFacts::has_pending_input`] holds.
    /// - **Reconnect by resubscribing.** A new client calls
    ///   `broadcast::Sender::subscribe`. An `ApprovalRequested` it missed is
    ///   still listed by [`ApprovalGate::pending`] (via
    ///   [`Asterpath::approval_gate`]), and a re-drive after a dropped stream
    ///   emits it again.
    ///
    /// ```no_run
    /// use std::sync::Arc;
    ///
    /// use asterpath::SessionHandle;
    /// use asterpath::protocol::AgentEvent;
    /// use futures_util::StreamExt;
    /// use tokio::sync::{Notify, broadcast};
    ///
    /// /// One per session. `wake` is notified after every `submit_input`.
    /// fn spawn_driver(
    ///     handle: SessionHandle,
    ///     wake: Arc<Notify>,
    /// ) -> broadcast::Sender<AgentEvent> {
    ///     let (events, _) = broadcast::channel(256);
    ///     let fan_out = events.clone();
    ///     tokio::spawn(async move {
    ///         loop {
    ///             let mut stream = handle.drive();
    ///             while let Some(event) = stream.next().await {
    ///                 match event {
    ///                     // No subscribers is fine: the turn keeps going.
    ///                     Ok(event) => drop(fan_out.send(event)),
    ///                     Err(error) => {
    ///                         eprintln!("drive failed: {error}");
    ///                         return;
    ///                     }
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
    ///     events
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
    /// [`ApprovalGate::pending`]: asterpath_protocol::ApprovalGate::pending
    pub fn drive(&self) -> BoxStream<'_, Result<AgentEvent, AsterError>> {
        let run = self.agent.run(RunRequest {
            session: self.session,
            trigger: RunTrigger::Resume,
        });
        run.map(|event| event.map_err(AsterError::from)).boxed()
    }

    /// Cancel the active turn. Idempotent; a no-op while idle.
    pub async fn cancel(&self) -> Result<(), AsterError> {
        self.cancellations.cancel(self.session);
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
        SessionFacts::fold(&entries).map_err(|error| {
            AsterError::Agent(asterpath_protocol::AgentError::Corrupt {
                session: self.session,
                detail: error.to_string(),
            })
        })
    }

    /// The model-visible projection folded from the log, not driver memory.
    /// Each entry keeps its origin so hosts can map items back to log ids.
    pub async fn projection(&self) -> Result<Vec<ProjectedEntry>, AsterError> {
        let entries = scan_all(&*self.store, self.session).await?;
        asterpath_protocol::project(&entries).map_err(|error| {
            AsterError::Agent(AgentError::Corrupt {
                session: self.session,
                detail: error.to_string(),
            })
        })
    }

    /// State is the log-derived classification, not driver memory.
    pub async fn state(&self) -> Result<SessionState, AsterError> {
        Ok(self.facts().await?.state())
    }

    /// Run one manual compaction. Only allowed while idle and undriven.
    pub async fn compact_now(&self) -> Result<CompactResult, AsterError> {
        Ok(self.agent.compact_now(self.session).await?)
    }
}
