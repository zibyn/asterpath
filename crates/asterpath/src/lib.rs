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
mod scheduler;
mod tools;
mod views;
mod waits;

pub use compaction::DefaultCompactor;

/// The contract crate: adapter traits, hooks and value types.
pub use asterpath_protocol as protocol;
/// Core id types, so embedders need only this crate.
pub use asterpath_protocol::{
    InputId, LogId, SessionId, SignalId, StepId, Timestamp, ToolCallId, TurnId,
};

/// Re-export of the Anthropic adapter when the `anthropic` feature is enabled.
/// Providers stay host-injected: the facade never assembles one itself.
#[cfg(feature = "anthropic")]
pub use asterpath_provider_anthropic as anthropic;

use std::sync::Arc;

use crate::agent::Agent;
use crate::scheduler::{Scheduler, SpawnTool};
use crate::tools::InMemoryToolRegistry;
use crate::views::trace::{TracingQuery, TracingTap};
use crate::views::usage::UsageService;
use crate::waits::RuntimeExecutor;
use asterpath_protocol::{
    AgentError, AgentEvent, AgentRunner, ApprovalDecision, CancellationRegistry, CompactResult,
    OpenIntents, ProviderCatalog, RunRequest, RunTrigger, SessionEvent, SessionSpec, SessionState,
    ToolRegistry, ToolSelection, TraceQuery, TraceTap, UsageQuery, UserInput, approval_signal_id,
    approval_signal_name, dedup,
};
use asterpath_protocol::{
    ApprovalGate, Clock, DurableExecutor, EventLog, LogQuery, SessionCatalog, SessionRetirement,
    SignalDelivery, Storage, unique_bytes,
};
use futures_core::stream::BoxStream;
use futures_util::StreamExt;

pub use config::{AsterpathBuilder, AsterpathConfig};
pub use error::AsterError;

#[cfg(feature = "openai")]
pub use asterpath_provider_openai as openai;

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
    scheduler: Arc<Scheduler>,
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

        // Projections read the backend; every write goes through the one
        // append entry so hooks and the trace tap see it.
        let trace_tap = Arc::new(TracingTap::new(Arc::clone(&event_log)));
        let trace: Arc<dyn TraceQuery> = Arc::new(TracingQuery::new(Arc::clone(&event_log)));
        let usage: Arc<dyn UsageQuery> = Arc::new(UsageService::new(
            Arc::clone(&event_log),
            Arc::clone(&session_catalog),
        ));
        let event_log: Arc<dyn EventLog> = Arc::new(commit::CommitLog::new(
            event_log,
            config.hooks.clone(),
            Arc::clone(&trace_tap),
        ));

        let executor: Arc<dyn DurableExecutor> =
            Arc::new(RuntimeExecutor::new(Arc::clone(&event_log)));
        let cancellations = Arc::new(CancellationRegistry::new());

        // The scheduler is built before the agent; the runner is bound
        // afterwards so the two never form an ownership cycle.
        let scheduler = Arc::new(Scheduler::with_runtime(
            Arc::clone(&event_log),
            Arc::clone(&session_catalog),
            Arc::clone(&executor),
            config.model_aliases.clone(),
            Arc::clone(&config.clock),
            config.max_live_children,
            Arc::clone(&cancellations),
        ));
        let tools: Arc<dyn ToolRegistry> = Arc::new(InMemoryToolRegistry::new());
        tools.register(Arc::new(SpawnTool::new(Arc::clone(&scheduler))));
        for tool in config.tools {
            tools.register(tool);
        }

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
                )
                .with_hooks(config.hooks, Some(Arc::clone(&usage)))
                .with_compactor(config.compactor.clone())
                .with_cancellations(Arc::clone(&cancellations)),
            )
        });
        scheduler.set_runner(Arc::clone(&agent));

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
            trace,
            trace_tap,
            scheduler,
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

        Ok(SessionHandle {
            session,
            event_log: Arc::clone(&self.event_log),
            agent: Arc::clone(&self.agent),
            executor: Arc::clone(&self.executor),
            cancellations: Arc::clone(&self.cancellations),
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
        })
    }

    /// Release the engine in a fixed order: the scheduler (and with it the
    /// spawn tool's hold on child sessions) first, then everything else.
    /// Nothing runs in the background, so there is nothing to await.
    pub async fn shutdown(self) -> Result<(), AsterError> {
        let Self { scheduler, .. } = self;
        drop(scheduler);
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

    /// Real-time span lifecycle driven by the append entry.
    pub fn trace_tap(&self) -> Arc<dyn TraceTap> {
        Arc::clone(&self.trace_tap)
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
