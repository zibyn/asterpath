//! Asterpath facade: the composition root and the stable session API.
//!
//! Real modules are assembled through `ModuleHost`; the SQLite backend is the
//! only default module in this walking skeleton. There is no background driver:
//! the host calls [`SessionHandle::drive`].

// The public error enum is fixed by packaging.md §3.5.
#![allow(clippy::result_large_err)]

pub mod config;
pub mod error;

/// Re-export of the Anthropic adapter when the `anthropic` feature is enabled.
/// Providers stay host-injected: the facade never assembles one itself.
#[cfg(feature = "anthropic")]
pub use asterpath_provider_anthropic as anthropic;

use std::sync::Arc;

use asterpath_agent::Agent;
use asterpath_core::{
    ApprovalGate, Clock, DurableExecutor, EventLog, InputId, LogQuery, ModuleCtx, ModuleHost,
    SessionCatalog, SessionId, SessionRetirement, SignalDelivery, Storage, ToolCallId,
    unique_bytes,
};
use asterpath_protocol::{
    AgentError, AgentEvent, AgentRunner, ApprovalDecision, CancellationRegistry, CompactResult,
    OpenIntents, ProviderCatalog, RecordAppended, RunRequest, RunTrigger, SessionEvent,
    SessionSpec, SessionState, ToolRegistry, ToolSelection, TraceQuery, TraceTap, UsageQuery,
    UserInput, approval_signal_id, approval_signal_name, dedup,
};
use asterpath_runtime::{RuntimeExecutor, RuntimeModule};
use asterpath_scheduler::SchedulerModule;
use asterpath_tools::ToolsModule;
use asterpath_tracing::TracingModule;
use asterpath_usage::UsageModule;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;

pub use config::{AsterpathBuilder, AsterpathConfig};
pub use error::AsterError;

#[cfg(feature = "openai")]
pub use asterpath_provider_openai as openai;

#[cfg(feature = "sqlite")]
use asterpath_storage_sqlite::SqliteStorageModule;

/// The assembled framework.
pub struct Asterpath {
    event_log: Arc<dyn EventLog>,
    storage: Arc<dyn Storage>,
    session_catalog: Arc<dyn SessionCatalog>,
    session_retirement: Option<Arc<dyn SessionRetirement>>,
    providers: Arc<ProviderCatalog>,
    tools: Arc<dyn ToolRegistry>,
    agent: Arc<dyn AgentRunner>,
    usage: Arc<dyn UsageQuery>,
    executor: Arc<dyn DurableExecutor>,
    gate: Arc<ApprovalGate>,
    cancellations: Arc<CancellationRegistry>,
    clock: Arc<dyn Clock>,
    trace: Arc<dyn TraceQuery>,
    trace_tap: Arc<dyn TraceTap>,
    bus: Option<ModuleCtx>,
    host: ModuleHost,
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

        let event_log;
        let storage;
        let session_catalog;
        let session_retirement;

        let mut module_host = ModuleHost::new();
        let tools_module = Arc::new(ToolsModule::new());
        module_host.register(tools_module.clone());

        match (config.event_log, config.storage) {
            (Some(log), Some(store)) => {
                event_log = log;
                storage = store;
                session_catalog = config.session_catalog.ok_or_else(|| AsterError::Config {
                    message: "session_catalog is required when event_log and storage are injected"
                        .to_owned(),
                })?;
                session_retirement = config.session_retirement;
            }
            (None, None) => {
                #[cfg(feature = "sqlite")]
                {
                    if config.database.as_os_str().is_empty() {
                        return Err(AsterError::Config {
                            message: "database path is required for the sqlite backend".to_owned(),
                        });
                    }
                    let backend = asterpath_storage_sqlite::open(&config.database)?;
                    module_host.register(Arc::new(SqliteStorageModule::new(backend.clone())));
                    event_log = backend.clone();
                    storage = backend.clone();
                    session_retirement = Some(Arc::clone(&backend) as Arc<dyn SessionRetirement>);
                    session_catalog = backend;
                }
                #[cfg(not(feature = "sqlite"))]
                {
                    return Err(AsterError::Config {
                        message:
                            "event_log and storage must be injected without the sqlite feature"
                                .to_owned(),
                    });
                }
            }
            _ => {
                return Err(AsterError::Config {
                    message: "event_log and storage must be injected together".to_owned(),
                });
            }
        }

        let runtime = Arc::new(RuntimeExecutor::new(
            Arc::clone(&event_log),
            Arc::clone(&storage),
            Arc::clone(&config.clock),
        ));
        module_host.register(Arc::new(RuntimeModule::new(Arc::clone(&runtime))));
        let executor: Arc<dyn DurableExecutor> = runtime;

        let cancellations = Arc::new(CancellationRegistry::new());
        let tracing_module = Arc::new(TracingModule::new(Arc::clone(&event_log)));
        module_host.register(tracing_module.clone());

        // The scheduler is assembled before the agent; the runner is bound
        // afterwards so the two never form an ownership cycle.
        let scheduler_module = Arc::new(SchedulerModule::with_runtime(
            Arc::clone(&event_log),
            Arc::clone(&session_catalog),
            Arc::clone(&executor),
            config.model_aliases.clone(),
            Arc::clone(&config.clock),
            config.max_live_children,
            Arc::clone(&cancellations),
        ));
        module_host.register(scheduler_module.clone());

        let usage_module = Arc::new(UsageModule::new(
            Arc::clone(&event_log),
            Arc::clone(&session_catalog),
        ));
        module_host.register(usage_module.clone());

        module_host.init().await?;
        let usage = usage_module.query();
        let tools = tools_module.registry();
        for tool in config.tools {
            tools.register(tool);
        }
        let hooks = tools_module.context();
        let bus = hooks.clone();

        let gate = Arc::clone(&config.gate);
        let approval = Arc::clone(&config.approval);
        let clock = Arc::clone(&config.clock);

        let agent = config.agent.unwrap_or_else(|| {
            Arc::new(
                Agent::new(
                    Arc::clone(&event_log),
                    Arc::clone(&providers),
                    Arc::clone(&tools),
                    config.tool.clone(),
                    approval,
                    Arc::clone(&gate),
                    Arc::clone(&executor),
                    Arc::clone(&config.clock),
                    hooks,
                )
                .with_compactor(config.compactor.clone())
                .with_cancellations(Arc::clone(&cancellations)),
            )
        });
        scheduler_module.scheduler().set_runner(Arc::clone(&agent));

        Ok(Self {
            event_log,
            storage,
            session_catalog,
            session_retirement,
            providers,
            tools,
            agent,
            usage,
            executor,
            gate,
            cancellations,
            clock,
            trace: tracing_module.query(),
            trace_tap: tracing_module.tap(),
            bus,
            host: module_host,
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
        };
        let entry = asterpath_protocol::encode_entry(
            session,
            Some(dedup::session_created(session)),
            &event,
        )
        .map_err(|error| AsterError::Config {
            message: error.to_string(),
        })?;
        self.event_log.append(entry).await?;
        self.notify(session).await;

        Ok(SessionHandle {
            session,
            event_log: Arc::clone(&self.event_log),
            agent: Arc::clone(&self.agent),
            executor: Arc::clone(&self.executor),
            cancellations: Arc::clone(&self.cancellations),
            bus: self.bus.clone(),
        })
    }

    pub async fn open_session(&self, id: SessionId) -> Result<SessionHandle, AsterError> {
        // Unknown and retired sessions are indistinguishable: both have no
        // committed frontier (retention.md §9).
        if self.event_log.frontier(id).await?.is_none() {
            return Err(AsterError::Agent(AgentError::NotFound { session: id }));
        }
        Ok(SessionHandle {
            session: id,
            event_log: Arc::clone(&self.event_log),
            agent: Arc::clone(&self.agent),
            executor: Arc::clone(&self.executor),
            cancellations: Arc::clone(&self.cancellations),
            bus: self.bus.clone(),
        })
    }

    pub async fn shutdown(mut self) -> Result<(), AsterError> {
        self.host.shutdown().await;
        Ok(())
    }

    pub fn event_log(&self) -> Arc<dyn EventLog> {
        Arc::clone(&self.event_log)
    }

    pub fn storage(&self) -> Arc<dyn Storage> {
        Arc::clone(&self.storage)
    }

    pub fn session_catalog(&self) -> Arc<dyn SessionCatalog> {
        Arc::clone(&self.session_catalog)
    }

    /// Pull-side usage accounting over the assembled event log.
    pub fn usage(&self) -> Arc<dyn UsageQuery> {
        Arc::clone(&self.usage)
    }

    /// `None` when the injected backend does not implement retirement.
    pub fn session_retirement(&self) -> Option<Arc<dyn SessionRetirement>> {
        self.session_retirement.clone()
    }

    /// Register an RAII endpoint here while the host can answer approvals.
    pub fn approval_gate(&self) -> Arc<ApprovalGate> {
        Arc::clone(&self.gate)
    }

    /// Deterministic trace projection over the event log.
    pub fn trace(&self) -> Arc<dyn TraceQuery> {
        Arc::clone(&self.trace)
    }

    /// Real-time span lifecycle driven by the typed event bus.
    pub fn trace_tap(&self) -> Arc<dyn TraceTap> {
        Arc::clone(&self.trace_tap)
    }

    async fn notify(&self, session: SessionId) {
        if let Some(bus) = &self.bus {
            bus.emit(RecordAppended { session }).await;
        }
    }
}

/// One session: submit inputs, then drive explicitly.
#[derive(Clone)]
pub struct SessionHandle {
    session: SessionId,
    event_log: Arc<dyn EventLog>,
    agent: Arc<dyn AgentRunner>,
    executor: Arc<dyn DurableExecutor>,
    cancellations: Arc<CancellationRegistry>,
    bus: Option<ModuleCtx>,
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
        self.event_log.append(entry).await?;
        if let Some(bus) = &self.bus {
            bus.emit(RecordAppended {
                session: self.session,
            })
            .await;
        }
        Ok(input.id)
    }

    /// Stream live `AgentEvent`s until `TurnCommitted` or `Idle`.
    pub fn drive(&self) -> BoxStream<'_, Result<AgentEvent, AsterError>> {
        let run = self.agent.run(RunRequest {
            session: self.session,
            trigger: RunTrigger::Resume,
        });
        run.map(|event| event.map_err(AsterError::from)).boxed()
    }

    /// Cancel the active turn. Idempotent; a no-op while idle. With a custom
    /// runner that does not share the registry this degrades to a no-op.
    pub async fn cancel(&self) -> Result<(), AsterError> {
        self.cancellations.cancel(self.session);
        Ok(())
    }

    /// Answer a pending `Ask` as the approval endpoint. The decision is a
    /// recorded `SignalReceived`, so it survives a crash and is reused.
    pub async fn approve(
        &self,
        call: ToolCallId,
        decision: ApprovalDecision,
    ) -> Result<(), AsterError> {
        let payload = serde_json::to_value(decision).map_err(|error| AsterError::Config {
            message: error.to_string(),
        })?;
        self.executor
            .deliver_signal(SignalDelivery {
                session: self.session,
                signal: approval_signal_id(call),
                name: approval_signal_name(call),
                payload,
            })
            .await?;
        Ok(())
    }

    /// State is the log-derived classification, not driver memory.
    pub async fn state(&self) -> Result<SessionState, AsterError> {
        let mut stream = self.event_log.scan(LogQuery {
            session: self.session,
            after: None,
            limit: None,
        });
        let mut entries = Vec::new();
        while let Some(entry) = stream.next().await {
            entries.push(entry?);
        }
        let intents = OpenIntents::fold(&entries).map_err(|error| {
            AsterError::Agent(asterpath_protocol::AgentError::Corrupt {
                session: self.session,
                detail: error.to_string(),
            })
        })?;
        Ok(intents.state())
    }

    /// Run one manual compaction. Only allowed while idle and undriven.
    pub async fn compact_now(&self) -> Result<CompactResult, AsterError> {
        Ok(self.agent.compact_now(self.session).await?)
    }
}
