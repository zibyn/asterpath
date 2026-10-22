//! Spawn, materialization, outcome mapping and the durable wait.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};

use asterpath_core::{
    DurableExecutor, EventLog, ExecutionError, LogEntry, LogId, LogQuery, ModuleCtx,
    SessionCatalog, SessionId, ToolCallId, WakeCause,
};
use asterpath_protocol::{
    AgentError, AgentRunner, CancelReason, ChildFailure, ChildOutcome, ConfigSnapshot,
    ContextPolicy, Item, ModelAlias, ModelSpec, ParentRef, Part, RecordAppended, Role, RunRequest,
    RunTrigger, SessionEvent, Summary, TurnEndReason, decode_entry, dedup, encode_entry,
};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;

use crate::{ChildRef, SpawnError, SpawnRequest, SubagentSpawner, WaitError};

/// Consecutive child spawns; used for the depth check.
const MAX_PARENT_CHAIN: u8 = 64;

pub struct Scheduler {
    log: Arc<dyn EventLog>,
    catalog: Arc<dyn SessionCatalog>,
    executor: Arc<dyn DurableExecutor>,
    aliases: BTreeMap<ModelAlias, ModelSpec>,
    /// Set by the composition root after the agent is built. Weak: the agent
    /// reaches the module bus and this scheduler, so an Arc here would leak the
    /// whole composition root.
    runner: OnceLock<Weak<dyn AgentRunner>>,
    bus: Mutex<Option<ModuleCtx>>,
}

impl Scheduler {
    pub fn new(
        log: Arc<dyn EventLog>,
        catalog: Arc<dyn SessionCatalog>,
        executor: Arc<dyn DurableExecutor>,
        aliases: BTreeMap<ModelAlias, ModelSpec>,
    ) -> Self {
        Self {
            log,
            catalog,
            executor,
            aliases,
            runner: OnceLock::new(),
            bus: Mutex::new(None),
        }
    }

    pub(crate) fn set_event_bus(&self, ctx: ModuleCtx) {
        *self.bus.lock().unwrap_or_else(PoisonError::into_inner) = Some(ctx);
    }

    async fn append(
        &self,
        session: SessionId,
        event: SessionEvent,
        key: Option<asterpath_core::DedupKey>,
    ) -> Result<LogId, ExecutionError> {
        let entry = encode_entry(session, key, &event).map_err(backend)?;
        let log_id = self.log.append(entry).await.map_err(backend)?;
        let bus = self
            .bus
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(bus) = bus {
            bus.emit(RecordAppended { session }).await;
        }
        Ok(log_id)
    }

    /// Late-bind the runner so the scheduler and the agent never own each other.
    pub fn set_runner(&self, runner: Arc<dyn AgentRunner>) {
        let _ = self.runner.set(Arc::downgrade(&runner));
    }

    /// The parent `ToolCallRequested` log id for a spawn tool call.
    pub(crate) async fn caused_by(
        &self,
        session: SessionId,
        tool_call: ToolCallId,
    ) -> Result<Option<LogId>, ExecutionError> {
        let events = scan(&*self.log, session).await?;
        Ok(events.iter().find_map(|(entry, event)| match event {
            SessionEvent::ToolCallRequested { tool_call_id, .. } if *tool_call_id == tool_call => {
                Some(entry.log_id)
            }
            _ => None,
        }))
    }

    async fn parent_depth(&self, session: SessionId) -> Result<u8, ExecutionError> {
        let mut depth = 0u8;
        let mut current = session;
        loop {
            match self.catalog.parent(current).await.map_err(backend)? {
                Some(parent) => {
                    depth = depth.saturating_add(1);
                    if depth > MAX_PARENT_CHAIN {
                        return Err(backend("session parent chain is cyclic"));
                    }
                    current = parent;
                }
                None => return Ok(depth),
            }
        }
    }
}

impl SubagentSpawner for Scheduler {
    fn spawn(&self, req: SpawnRequest) -> BoxFuture<'_, Result<ChildRef, SpawnError>> {
        Box::pin(async move {
            let parent_events = scan(&*self.log, req.parent.session)
                .await
                .map_err(SpawnError::Execution)?;
            let parent_config = session_config(&parent_events)
                .ok_or(SpawnError::Execution(ExecutionError::NotFound {
                    session: req.parent.session,
                }))?
                .clone();

            // R1: one child per parent tool call, ever.
            if let Some(record) = find_spawn(&parent_events, req.parent.tool_call) {
                self.materialize(
                    req.parent.clone(),
                    record.child,
                    &record.task,
                    &record.config,
                    &record.context,
                )
                .await
                .map_err(SpawnError::Execution)?;
                return Ok(ChildRef {
                    parent: req.parent,
                    child: record.child,
                });
            }

            let child_depth = self
                .parent_depth(req.parent.session)
                .await
                .map_err(SpawnError::Execution)?
                .saturating_add(1);
            if child_depth > parent_config.subagents.max_depth {
                return Err(SpawnError::DepthExceeded);
            }

            let model = match &req.model_alias {
                Some(alias) => {
                    let model = self
                        .aliases
                        .get(alias)
                        .ok_or_else(|| SpawnError::InvalidArgs {
                            message: format!("unknown model alias {}", alias.as_str()),
                        })?;
                    if model.provider != parent_config.model.provider {
                        return Err(SpawnError::InvalidArgs {
                            message: format!(
                                "model alias {} uses provider {}, parent uses {}",
                                alias.as_str(),
                                model.provider,
                                parent_config.model.provider
                            ),
                        });
                    }
                    model.clone()
                }
                None => parent_config.model.clone(),
            };

            let tools = narrow_tools(&parent_config.tools, req.tool_allowlist.as_deref())?;

            // #49 owns fork materialization; erroring is explicit, never Fresh.
            if matches!(req.context, ContextPolicy::Fork { .. }) {
                return Err(SpawnError::InvalidArgs {
                    message: "fork context is not available yet".to_owned(),
                });
            }

            let config = ConfigSnapshot {
                model,
                tools,
                ..parent_config
            };

            let child = fresh_session_id();
            self.append(
                req.parent.session,
                SessionEvent::ChildSessionSpawned {
                    child,
                    task: req.task.clone(),
                    config: config.clone(),
                    context: req.context.clone(),
                    parent_tool_call_id: req.parent.tool_call,
                    caused_by: req.caused_by,
                },
                Some(dedup::child_session(child)),
            )
            .await
            .map_err(SpawnError::Execution)?;

            self.materialize(req.parent.clone(), child, &req.task, &config, &req.context)
                .await
                .map_err(SpawnError::Execution)?;

            Ok(ChildRef {
                parent: req.parent,
                child,
            })
        })
    }

    fn await_completion(&self, child: &ChildRef) -> BoxFuture<'_, Result<ChildOutcome, WaitError>> {
        let child = child.clone();
        Box::pin(async move {
            let parent_events = scan(&*self.log, child.parent.session)
                .await
                .map_err(WaitError::Execution)?;

            // R3: a recorded outcome wins; the child is never queried again.
            if let Some(outcome) = find_completion(&parent_events, child.child) {
                return wait_outcome(&outcome);
            }

            let Some(record) = find_spawn(&parent_events, child.parent.tool_call) else {
                return Err(WaitError::Execution(backend(
                    "child wait has no ChildSessionSpawned record",
                )));
            };
            if record.child != child.child {
                return Err(WaitError::Execution(backend(
                    "child wait does not own the recorded child session",
                )));
            }

            let mut child_events = match scan(&*self.log, child.child).await {
                Ok(events) => events,
                Err(error) => return self.lost(&child, error.to_string()).await,
            };

            // R2: fill records missing after a crash before waiting or driving.
            self.materialize(
                child.parent.clone(),
                child.child,
                &record.task,
                &record.config,
                &record.context,
            )
            .await
            .map_err(WaitError::Execution)?;

            if let Some(outcome) = child_outcome(&child_events) {
                self.record_completion(&child, outcome.clone()).await?;
                return wait_outcome(&outcome);
            }

            let Some(runner) = self.runner.get().and_then(Weak::upgrade) else {
                return Err(WaitError::Execution(backend(
                    "agent runner is not configured on the scheduler",
                )));
            };

            loop {
                let parent_events = scan(&*self.log, child.parent.session)
                    .await
                    .map_err(WaitError::Execution)?;
                if let Some(outcome) = find_completion(&parent_events, child.child) {
                    return wait_outcome(&outcome);
                }

                child_events = match scan(&*self.log, child.child).await {
                    Ok(events) => events,
                    Err(error) => return self.lost(&child, error.to_string()).await,
                };
                if let Some(outcome) = child_outcome(&child_events) {
                    self.record_completion(&child, outcome.clone()).await?;
                    return wait_outcome(&outcome);
                }

                let drive = drive_child(Arc::clone(&runner), child.child);
                tokio::pin!(drive);
                let wake = self
                    .executor
                    .await_wake(child.parent.session, WakeCause::ChildCompleted(child.child));
                tokio::pin!(wake);

                let driven = tokio::select! {
                    result = &mut drive => Some(result),
                    _ = &mut wake => None,
                };
                match driven {
                    // Another writer closed the wait; re-read the records.
                    None => {}
                    Some(Ok(())) => {
                        if child_outcome(
                            &scan(&*self.log, child.child)
                                .await
                                .map_err(WaitError::Execution)?,
                        )
                        .is_none()
                        {
                            return self
                                .lost(
                                    &child,
                                    "child turn ended without a committed terminal".to_owned(),
                                )
                                .await;
                        }
                    }
                    Some(Err(AgentError::Busy { .. })) => {
                        let _ = self
                            .executor
                            .await_wake(
                                child.parent.session,
                                WakeCause::ChildCompleted(child.child),
                            )
                            .await;
                    }
                    Some(Err(error)) => return self.lost(&child, error.to_string()).await,
                }
            }
        })
    }

    fn cancel(&self, child: &ChildRef) -> BoxFuture<'_, Result<(), WaitError>> {
        let child = child.clone();
        Box::pin(async move {
            let parent_events = scan(&*self.log, child.parent.session)
                .await
                .map_err(WaitError::Execution)?;
            if find_completion(&parent_events, child.child).is_some() {
                return Ok(());
            }
            // #48 adds child-session cancel propagation; this ticket only owns
            // the parent-side closure.
            self.record_completion(
                &child,
                ChildOutcome::Cancelled {
                    reason: CancelReason::Parent,
                },
            )
            .await
        })
    }
}

impl Scheduler {
    /// R2: create the child log's records from the parent record. Every write
    /// is deduplicated by the child id, so replay fills gaps without doubling.
    async fn materialize(
        &self,
        parent: ParentRef,
        child: SessionId,
        task: &[Part],
        config: &ConfigSnapshot,
        context: &ContextPolicy,
    ) -> Result<(), ExecutionError> {
        let events = scan(&*self.log, child).await?;

        match session_config(&events) {
            Some(existing) if existing != config => {
                return Err(backend(
                    "child SessionCreated.config does not match ChildSessionSpawned.config",
                ));
            }
            Some(_) => {}
            None => {
                self.append(
                    child,
                    SessionEvent::SessionCreated {
                        config: config.clone(),
                        parent: Some(parent),
                    },
                    Some(dedup::session_created(child)),
                )
                .await?;
            }
        }

        if matches!(context, ContextPolicy::Fork { .. }) {
            return Err(backend("fork context materialization is not implemented"));
        }

        if !events
            .iter()
            .any(|(_, event)| matches!(event, SessionEvent::InputRecorded { .. }))
        {
            self.append(
                child,
                SessionEvent::InputRecorded {
                    input: task.to_vec(),
                },
                Some(dedup::child_session(child)),
            )
            .await?;
        }

        // TurnStarted is deliberately not written here: the agent loop opens the
        // first turn from the pending input. Writing it first would force
        // recovery mode and prepend StreamResumed to a fresh child (SA1).
        Ok(())
    }

    async fn record_completion(
        &self,
        child: &ChildRef,
        outcome: ChildOutcome,
    ) -> Result<(), WaitError> {
        self.append(
            child.parent.session,
            SessionEvent::ChildSessionCompleted {
                child: child.child,
                outcome,
            },
            Some(dedup::child_session(child.child)),
        )
        .await
        .map_err(WaitError::Execution)?;
        self.executor
            .wake(child.parent.session, WakeCause::ChildCompleted(child.child));
        Ok(())
    }

    /// The child state could not be read. The recorded `Lost` outcome unblocks
    /// any replay; the current call still reports an infrastructure failure.
    async fn lost(&self, child: &ChildRef, reason: String) -> Result<ChildOutcome, WaitError> {
        self.record_completion(
            child,
            ChildOutcome::Lost {
                reason: reason.clone(),
            },
        )
        .await?;
        Err(WaitError::Execution(backend(reason)))
    }
}

struct SpawnRecord {
    child: SessionId,
    task: Vec<Part>,
    config: ConfigSnapshot,
    context: ContextPolicy,
}

fn find_spawn(events: &[(LogEntry, SessionEvent)], tool_call: ToolCallId) -> Option<SpawnRecord> {
    events.iter().find_map(|(_, event)| match event {
        SessionEvent::ChildSessionSpawned {
            child,
            task,
            config,
            context,
            parent_tool_call_id,
            ..
        } if *parent_tool_call_id == tool_call => Some(SpawnRecord {
            child: *child,
            task: task.clone(),
            config: config.clone(),
            context: context.clone(),
        }),
        _ => None,
    })
}

fn find_completion(events: &[(LogEntry, SessionEvent)], child: SessionId) -> Option<ChildOutcome> {
    events.iter().find_map(|(_, event)| match event {
        SessionEvent::ChildSessionCompleted {
            child: done,
            outcome,
        } if *done == child => Some(outcome.clone()),
        _ => None,
    })
}

fn session_config(events: &[(LogEntry, SessionEvent)]) -> Option<&ConfigSnapshot> {
    events.iter().find_map(|(_, event)| match event {
        SessionEvent::SessionCreated { config, .. } => Some(config),
        _ => None,
    })
}

/// Map the child's last committed terminal to the parent-visible outcome.
fn child_outcome(events: &[(LogEntry, SessionEvent)]) -> Option<ChildOutcome> {
    let reason = events.iter().rev().find_map(|(_, event)| match event {
        SessionEvent::TurnCommitted { reason, .. } => Some(reason),
        _ => None,
    })?;
    let summary = Summary {
        items: summary_parts(events),
    };
    Some(match reason {
        TurnEndReason::Completed | TurnEndReason::Stopped { .. } => {
            ChildOutcome::Completed { summary }
        }
        TurnEndReason::Failed => ChildOutcome::Failed {
            reason: ChildFailure::TurnFailed,
            summary,
        },
        TurnEndReason::ContextExhausted => ChildOutcome::Failed {
            reason: ChildFailure::ContextExhausted,
            summary,
        },
        TurnEndReason::MaxStepsExceeded => ChildOutcome::Failed {
            reason: ChildFailure::MaxStepsExceeded,
            summary,
        },
        TurnEndReason::CompactionFailed => ChildOutcome::Failed {
            reason: ChildFailure::CompactionFailed,
            summary,
        },
        // The child was stopped by its own host, not by the parent's wait.
        TurnEndReason::Cancelled => ChildOutcome::Cancelled {
            reason: CancelReason::Host,
        },
        _ => ChildOutcome::Failed {
            reason: ChildFailure::TurnFailed,
            summary,
        },
    })
}

/// Text/refusal parts of the last committed assistant message only.
fn summary_parts(events: &[(LogEntry, SessionEvent)]) -> Vec<Part> {
    let items = events
        .iter()
        .rev()
        .find_map(|(_, event)| match event {
            SessionEvent::AssistantMessage { items, .. } => Some(items),
            _ => None,
        })
        .map(Vec::as_slice)
        .unwrap_or_default();
    items
        .iter()
        .filter_map(|item| match item {
            Item::Message {
                role: Role::Assistant,
                parts,
            } => Some(parts),
            _ => None,
        })
        .flatten()
        .filter(|part| matches!(part, Part::Text { .. } | Part::Refusal { .. }))
        .cloned()
        .collect()
}

fn wait_outcome(outcome: &ChildOutcome) -> Result<ChildOutcome, WaitError> {
    match outcome {
        ChildOutcome::Cancelled {
            reason: CancelReason::Parent,
        } => Err(WaitError::Cancelled),
        ChildOutcome::Cancelled {
            reason: CancelReason::Timeout,
        } => Err(WaitError::Timeout),
        other => Ok(other.clone()),
    }
}

fn narrow_tools(
    parent: &[asterpath_protocol::ToolDescriptor],
    allowlist: Option<&[asterpath_protocol::ToolName]>,
) -> Result<Vec<asterpath_protocol::ToolDescriptor>, SpawnError> {
    let Some(names) = allowlist else {
        return Ok(parent.to_vec());
    };
    let names: HashSet<&asterpath_protocol::ToolName> = names.iter().collect();
    let known: HashSet<&asterpath_protocol::ToolName> =
        parent.iter().map(|tool| &tool.name).collect();
    if let Some(unknown) = names.iter().find(|name| !known.contains(*name)) {
        return Err(SpawnError::InvalidArgs {
            message: format!("tool {} is not in the parent snapshot", unknown.as_str()),
        });
    }
    Ok(parent
        .iter()
        .filter(|tool| names.contains(&tool.name))
        .cloned()
        .collect())
}

async fn drive_child(runner: Arc<dyn AgentRunner>, session: SessionId) -> Result<(), AgentError> {
    let mut stream: BoxStream<'_, Result<asterpath_protocol::AgentEvent, AgentError>> =
        runner.run(RunRequest {
            session,
            trigger: RunTrigger::Resume,
        });
    while let Some(event) = stream.next().await {
        event?;
    }
    Ok(())
}

async fn scan(
    log: &dyn EventLog,
    session: SessionId,
) -> Result<Vec<(LogEntry, SessionEvent)>, ExecutionError> {
    let mut stream = log.scan(LogQuery {
        session,
        after: None,
        limit: None,
    });
    let mut events = Vec::new();
    while let Some(entry) = stream.next().await {
        let entry = entry.map_err(backend)?;
        let event = decode_entry(&entry).map_err(backend)?;
        events.push((entry, event));
    }
    Ok(events)
}

fn backend(error: impl std::fmt::Display) -> ExecutionError {
    ExecutionError::Backend(error.to_string())
}

/// Process-local unique id source; session scoping makes collisions moot.
fn fresh_session_id() -> SessionId {
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
    SessionId::from_bytes(bytes)
}
