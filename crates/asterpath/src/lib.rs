//! Asterpath facade: the composition root and the stable session API.
//!
//! Real modules are assembled through `ModuleHost`; the SQLite backend is the
//! only default module in this walking skeleton. There is no background driver:
//! the host calls [`SessionHandle::drive`].

// The public error enum is fixed by packaging.md §3.5.
#![allow(clippy::result_large_err)]

pub mod config;
pub mod error;

use std::sync::Arc;

use asterpath_agent::Agent;
use asterpath_core::{EventLog, InputId, LogQuery, ModuleHost, SessionCatalog, SessionId, Storage};
use asterpath_protocol::{
    AgentEvent, AgentRunner, CompactResult, OpenIntents, ProviderCatalog, RunRequest, RunTrigger,
    SessionEvent, SessionSpec, SessionState, ToolSelection, UserInput, dedup,
};
use futures_core::stream::BoxStream;
use futures_util::StreamExt;

pub use config::{AsterpathBuilder, AsterpathConfig};
pub use error::AsterError;

#[cfg(feature = "sqlite")]
use asterpath_storage_sqlite::SqliteStorageModule;

/// The assembled framework.
pub struct Asterpath {
    event_log: Arc<dyn EventLog>,
    storage: Arc<dyn Storage>,
    session_catalog: Arc<dyn SessionCatalog>,
    providers: Arc<ProviderCatalog>,
    agent: Arc<dyn AgentRunner>,
    host: Option<ModuleHost>,
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
        if config.turn.max_steps_per_turn == Some(0) {
            return Err(AsterError::Config {
                message: "max_steps_per_turn must be greater than zero".to_owned(),
            });
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
        let host;

        match (config.event_log, config.storage) {
            (Some(log), Some(store)) => {
                event_log = log;
                storage = store;
                session_catalog = config.session_catalog.ok_or_else(|| AsterError::Config {
                    message: "session_catalog is required when event_log and storage are injected"
                        .to_owned(),
                })?;
                host = None;
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
                    let mut module_host = ModuleHost::new();
                    module_host.register(Arc::new(SqliteStorageModule::new(backend.clone())));
                    module_host.init().await?;
                    event_log = backend.clone();
                    storage = backend.clone();
                    session_catalog = backend;
                    host = Some(module_host);
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

        let agent = config.agent.unwrap_or_else(|| {
            Arc::new(Agent::new(Arc::clone(&event_log), Arc::clone(&providers)))
        });

        Ok(Self {
            event_log,
            storage,
            session_catalog,
            providers,
            agent,
            host,
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
            ToolSelection::All => Vec::new(),
            ToolSelection::Names(names) if names.is_empty() => Vec::new(),
            ToolSelection::Names(names) => {
                return Err(AsterError::Config {
                    message: format!(
                        "unknown tool {}",
                        names.first().expect("non-empty").as_str()
                    ),
                });
            }
        };

        let session = SessionId::from_bytes(fresh_id());
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
        })
    }

    pub async fn open_session(&self, id: SessionId) -> Result<SessionHandle, AsterError> {
        Ok(SessionHandle {
            session: id,
            event_log: Arc::clone(&self.event_log),
            agent: Arc::clone(&self.agent),
        })
    }

    pub async fn shutdown(mut self) -> Result<(), AsterError> {
        if let Some(mut host) = self.host.take() {
            host.shutdown().await;
        }
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
}

/// One session: submit inputs, then drive explicitly.
#[derive(Clone)]
pub struct SessionHandle {
    session: SessionId,
    event_log: Arc<dyn EventLog>,
    agent: Arc<dyn AgentRunner>,
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

    /// Placeholder until cancellation is implemented; the session is not left open.
    pub async fn cancel(&self) -> Result<(), AsterError> {
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

    /// Compaction is not wired in this ticket; idle sessions report NotConfigured.
    pub async fn compact_now(&self) -> Result<CompactResult, AsterError> {
        Ok(self.agent.compact_now(self.session).await?)
    }
}

fn fresh_id() -> [u8; 16] {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or_default();
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut bytes = [0u8; 16];
    bytes[..8]
        .copy_from_slice(&(nanos ^ (std::process::id() as u64).rotate_left(17)).to_le_bytes());
    bytes[8..].copy_from_slice(&counter.to_le_bytes());
    bytes
}
