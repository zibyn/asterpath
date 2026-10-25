//! Spawn, materialization, outcome mapping and the durable wait.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use asterpath_protocol::{
    AgentError, CancelReason, CancellationRegistry, ChildFailure, ChildOutcome, ConfigSnapshot,
    ContextPolicy, ForkSource, Item, ModelAlias, ModelSpec, ParentRef, Part, Role, RunRequest,
    RunTrigger, SessionEvent, SessionFacts, SubagentConfig, Summary, TurnEndReason, dedup,
    effective_config, encode_entry,
};
use asterpath_protocol::{
    Clock, ExecutionError, LogEntry, LogId, SessionId, SessionStore, Timestamp, ToolCallId,
    unique_bytes,
};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;

use super::tool::SPAWN_TOOL_NAME;
use super::{ChildRef, SpawnError, SpawnRequest, WaitError};
use crate::agent::Agent;
use crate::fork;
use crate::scan::{backend, scan_decoded};
use crate::waits::Waits;

#[cfg(test)]
const DEFAULT_MAX_LIVE_CHILDREN: NonZeroUsize = NonZeroUsize::new(32).expect("non-zero");

/// Consecutive child spawns; used for the depth check.
const MAX_PARENT_CHAIN: u8 = 64;

pub(crate) struct Scheduler {
    log: Arc<dyn SessionStore>,
    waits: Arc<Waits>,
    aliases: BTreeMap<ModelAlias, ModelSpec>,
    clock: Arc<dyn Clock>,
    /// Process-wide cap on live children across all parents.
    max_live_children: NonZeroUsize,
    /// Shared with the agent so child drives see cancellation.
    cancellations: Arc<CancellationRegistry>,
    /// Admission state is memory only; `recovered` marks parents whose open
    /// spawns have been rebuilt from the log.
    admission: Mutex<Admission>,
    notify: tokio::sync::Notify,
}

#[derive(Default)]
struct Admission {
    live: usize,
    parents: HashMap<SessionId, ParentAdmission>,
    recovered: HashSet<SessionId>,
}

#[derive(Default)]
struct ParentAdmission {
    running: usize,
    admitted: HashSet<ToolCallId>,
    /// Open spawn `ToolCallRequested`s by `seq`; the lowest seq is next.
    queue: BTreeMap<u64, ToolCallId>,
}

impl Scheduler {
    #[cfg(test)]
    pub(crate) fn with_defaults(
        log: Arc<dyn SessionStore>,
        waits: Arc<Waits>,
        aliases: BTreeMap<ModelAlias, ModelSpec>,
    ) -> Self {
        Self::new(
            log,
            waits,
            aliases,
            Arc::new(asterpath_protocol::SystemClock),
            DEFAULT_MAX_LIVE_CHILDREN,
            Arc::new(CancellationRegistry::new()),
        )
    }

    /// Full wiring: the facade passes the injected clock, the global live-child
    /// cap and the registry the agent shares.
    pub(crate) fn new(
        log: Arc<dyn SessionStore>,
        waits: Arc<Waits>,
        aliases: BTreeMap<ModelAlias, ModelSpec>,
        clock: Arc<dyn Clock>,
        max_live_children: NonZeroUsize,
        cancellations: Arc<CancellationRegistry>,
    ) -> Self {
        Self {
            log,
            waits,
            aliases,
            clock,
            max_live_children,
            cancellations,
            admission: Mutex::new(Admission::default()),
            notify: tokio::sync::Notify::new(),
        }
    }

    /// The registry child drives must share to see cancellation.
    #[cfg(test)]
    pub(crate) fn cancellations(&self) -> Arc<CancellationRegistry> {
        Arc::clone(&self.cancellations)
    }

    async fn append(
        &self,
        session: SessionId,
        event: SessionEvent,
        key: Option<asterpath_protocol::DedupKey>,
    ) -> Result<LogId, ExecutionError> {
        let entry = encode_entry(session, key, &event).map_err(backend)?;
        self.log.append(entry).await.map_err(backend)
    }

    /// The parent `ToolCallRequested` log id for a spawn tool call.
    pub(crate) async fn caused_by(
        &self,
        session: SessionId,
        tool_call: ToolCallId,
    ) -> Result<Option<LogId>, ExecutionError> {
        let events = scan_decoded(&*self.log, session).await?;
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
            match self.log.parent(current).await.map_err(backend)? {
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

impl Scheduler {
    pub(crate) fn spawn(&self, req: SpawnRequest) -> BoxFuture<'_, Result<ChildRef, SpawnError>> {
        Box::pin(async move {
            let parent_events = scan_decoded(&*self.log, req.parent.session)
                .await
                .map_err(|error| SpawnError::Execution(error.into()))?;
            // The parent's effective config at the spawn step's `StepStarted`;
            // later parent changes never reach the child (ADR-0030).
            let watermark =
                spawn_step_start(&parent_events, req.caused_by).map_or(u64::MAX, |entry| entry.seq);
            let parent_config = effective_config(&parent_events, watermark).ok_or(
                SpawnError::Execution(ExecutionError::NotFound {
                    session: req.parent.session,
                }),
            )?;
            let subagents = parent_config.subagents.clone();
            // Per-call wins; otherwise the inherited default (None by default).
            let timeout = req.timeout.or(subagents.spawn_timeout);
            // The tool leaves context at `Fresh`; a `Fork` snapshot default
            // wins and anchors at this spawn step's `StepStarted`.
            let context = resolve_context(
                &req.context,
                &subagents.default_context,
                &parent_events,
                req.caused_by,
            );

            // Rebuild this parent's queue and Running count from the log first;
            // a result that landed since then also closes its queue entry.
            let parent_facts = SessionFacts::fold_decoded(
                parent_events.iter().map(|(entry, event)| (entry, event)),
            );
            self.recover_parent(req.parent.session, &parent_facts)
                .await
                .map_err(SpawnError::Execution)?;
            self.prune_queue(req.parent.session, &parent_facts);

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

            // Admission is FIFO by ToolCallRequested.seq, per-parent and
            // global. The guard also clears this call's recovered queue entry
            // if validation rejects it.
            let seq = parent_events
                .iter()
                .find_map(|(entry, _)| (entry.log_id == req.caused_by).then_some(entry.seq))
                .ok_or_else(|| {
                    SpawnError::Execution(backend("spawn request has no ToolCallRequested record"))
                })?;
            let mut queue_guard = QueueGuard {
                scheduler: self,
                parent: req.parent.session,
                seq,
                tool_call: req.parent.tool_call,
                armed: true,
            };

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
            let system = match &context {
                ContextPolicy::Fork { at } => {
                    fork_system(&parent_events, *at).map_err(SpawnError::Execution)?
                }
                ContextPolicy::Fresh => parent_config.system.clone(),
            };

            let config = ConfigSnapshot {
                model,
                system,
                tools,
                ..parent_config
            };

            self.admit(
                &mut queue_guard,
                subagents.max_concurrent_children.get(),
                self.max_live_children.get(),
            )
            .await;
            drop(queue_guard);

            let now = self.clock.now();
            let child = SessionId::from_bytes(unique_bytes(now));
            // The timeout starts at the spawn record, not at admission.
            let deadline = timeout.map(|timeout| {
                now.saturating_add(i64::try_from(timeout.as_millis()).unwrap_or(i64::MAX))
            });
            self.append(
                req.parent.session,
                SessionEvent::ChildSessionSpawned {
                    child,
                    task: req.task.clone(),
                    config: config.clone(),
                    context: context.clone(),
                    parent_tool_call_id: req.parent.tool_call,
                    caused_by: req.caused_by,
                    deadline,
                },
                Some(dedup::child_session(child)),
            )
            .await
            .map_err(SpawnError::Execution)?;

            self.materialize(req.parent.clone(), child, &req.task, &config, &context)
                .await
                .map_err(SpawnError::Execution)?;

            Ok(ChildRef {
                parent: req.parent,
                child,
            })
        })
    }

    /// R3 + the durable wait; `agent` drives the child while it is open.
    pub(crate) fn await_completion<'a>(
        &'a self,
        child: &ChildRef,
        agent: &'a Agent,
    ) -> BoxFuture<'a, Result<ChildOutcome, WaitError>> {
        let child = child.clone();
        Box::pin(async move {
            let mut parent_events = scan_decoded(&*self.log, child.parent.session)
                .await
                .map_err(WaitError::from)?;

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

            // An unreadable child becomes a recorded `Lost`, never a hang.
            if let Err(error) = scan_decoded(&*self.log, child.child).await {
                return self.lost(&child, error.to_string()).await;
            }

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
            let mut child_events = scan_decoded(&*self.log, child.child)
                .await
                .map_err(WaitError::from)?;

            if let Some(outcome) = child_outcome(&child_events) {
                self.record_completion(&child, outcome.clone()).await?;
                return wait_outcome(&outcome);
            }

            loop {
                parent_events = scan_decoded(&*self.log, child.parent.session)
                    .await
                    .map_err(WaitError::from)?;
                if let Some(outcome) = find_completion(&parent_events, child.child) {
                    return wait_outcome(&outcome);
                }

                child_events = match scan_decoded(&*self.log, child.child).await {
                    Ok(events) => events,
                    Err(error) => return self.lost(&child, error.to_string()).await,
                };
                // R3 applies to the child's own terminal too: it wins over an
                // expired deadline, including one that passed while stopped.
                if let Some(outcome) = child_outcome(&child_events) {
                    self.record_completion(&child, outcome.clone()).await?;
                    return wait_outcome(&outcome);
                }

                let remaining = record.deadline.map(|deadline| deadline - self.clock.now());
                if remaining.is_some_and(|remaining| remaining <= 0) {
                    return self.timed_out(&child, &record, agent).await;
                }

                let drive = drive_child(agent, child.child);
                tokio::pin!(drive);
                let wake = self.waits.await_child(child.parent.session, child.child);
                tokio::pin!(wake);
                let deadline_wait = async {
                    match remaining {
                        Some(millis) => {
                            tokio::time::sleep(Duration::from_millis(millis as u64)).await
                        }
                        None => std::future::pending().await,
                    }
                };
                tokio::pin!(deadline_wait);

                let driven = tokio::select! {
                    result = &mut drive => Some(result),
                    _ = &mut wake => None,
                    // Re-read first: the child's terminal still wins (R3).
                    _ = &mut deadline_wait => None,
                };
                match driven {
                    // Another writer closed the wait or the deadline passed;
                    // re-read the records.
                    None => {}
                    Some(Ok(())) => {
                        if child_outcome(
                            &scan_decoded(&*self.log, child.child)
                                .await
                                .map_err(WaitError::from)?,
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
                            .waits
                            .await_child(child.parent.session, child.child)
                            .await;
                    }
                    Some(Err(error)) => return self.lost(&child, error.to_string()).await,
                }
            }
        })
    }

    /// Rebuild this parent's queue and Running count from the log. A child
    /// with `ChildSessionSpawned` and no completion is Running and is never
    /// re-queued; open spawns wait in `seq` order.
    async fn recover_parent(
        &self,
        parent: SessionId,
        facts: &SessionFacts,
    ) -> Result<(), ExecutionError> {
        if self
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .recovered
            .contains(&parent)
        {
            return Ok(());
        }

        // Queued: open spawn calls with no child yet. Running: open children.
        let queue: BTreeMap<u64, ToolCallId> = facts
            .open_tools()
            .filter(|(_, tool)| tool.name.as_str() == SPAWN_TOOL_NAME && tool.spawned.is_none())
            .map(|(call, tool)| (tool.seq, call))
            .collect();
        let running: Vec<ToolCallId> = facts
            .open_children()
            .map(|(_, child)| child.parent_tool_call)
            .collect();

        let mut state = self
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !state.recovered.insert(parent) {
            return Ok(());
        }
        let mut started = 0usize;
        {
            let entry = state.parents.entry(parent).or_default();
            for (seq, call) in queue {
                entry.queue.entry(seq).or_insert(call);
            }
            for call in running {
                if entry.admitted.insert(call) {
                    entry.running += 1;
                    started += 1;
                }
            }
        }
        state.live += started;
        Ok(())
    }

    /// Drop queue entries whose tool call already has a `ToolResult`: a spawn
    /// cancelled before admission is closed and must not hold the FIFO head.
    fn prune_queue(&self, parent: SessionId, facts: &SessionFacts) {
        // A queued call at or before the folded seq that is no longer open has
        // its result; a later one was queued after this read and stays.
        let Some((folded, _)) = facts.last() else {
            return;
        };
        let mut state = self
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(entry) = state.parents.get_mut(&parent) {
            entry
                .queue
                .retain(|seq, call| *seq > folded || facts.open_tool(*call).is_some());
        }
    }

    /// Block until this spawn is the lowest-seq queued call and a slot is free.
    /// The guard removes a still-queued entry if the future is dropped.
    async fn admit(&self, guard: &mut QueueGuard<'_>, per_parent: usize, live_max: usize) {
        let parent = guard.parent;
        let seq = guard.seq;
        let tool_call = guard.tool_call;
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let admitted = {
                let mut state = self
                    .admission
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                let live = state.live;
                let entry = state.parents.entry(parent).or_default();
                entry.queue.entry(seq).or_insert(tool_call);
                let front = entry.queue.keys().next().copied() == Some(seq);
                let admitted = front && entry.running < per_parent && live < live_max;
                if admitted {
                    entry.queue.remove(&seq);
                    entry.running += 1;
                    entry.admitted.insert(tool_call);
                    state.live += 1;
                }
                admitted
            };
            if admitted {
                guard.armed = false;
                self.notify.notify_waiters();
                return;
            }
            notified.await;
        }
    }

    /// Free the slot a completed child occupied; re-admission is FIFO.
    fn release_slot(&self, parent: SessionId, tool_call: ToolCallId) {
        let mut state = self
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let released = match state.parents.get_mut(&parent) {
            Some(entry) => {
                if entry.admitted.remove(&tool_call) {
                    entry.running = entry.running.saturating_sub(1);
                    true
                } else {
                    false
                }
            }
            None => false,
        };
        if released {
            state.live = state.live.saturating_sub(1);
        }
        drop(state);
        if released {
            self.notify.notify_waiters();
        }
    }

    /// The spawn deadline passed: cancel the child and report the outcome.
    async fn timed_out(
        &self,
        child: &ChildRef,
        record: &SpawnRecord,
        agent: &Agent,
    ) -> Result<ChildOutcome, WaitError> {
        match self
            .time_out(child, record.config.subagents.cancel_grace, agent)
            .await?
        {
            Some(outcome) => wait_outcome(&outcome),
            None => Err(WaitError::Timeout),
        }
    }

    /// Timeout: cancel the child and give it `grace` to close. R3 is rechecked
    /// after the drain; a terminal that landed in the window wins over
    /// `Cancelled{Timeout}`. Returns that outcome, or `None` for the timeout.
    async fn time_out(
        &self,
        child: &ChildRef,
        grace: Duration,
        agent: &Agent,
    ) -> Result<Option<ChildOutcome>, WaitError> {
        self.drain_child(agent, child.child, grace).await;
        let child_events = scan_decoded(&*self.log, child.child)
            .await
            .map_err(WaitError::from)?;
        if let Some(outcome) = child_outcome(&child_events)
            // A `Cancelled{Host}` is the child reacting to our own drain, not a
            // terminal that beat the timeout.
            && !matches!(outcome, ChildOutcome::Cancelled { .. })
        {
            self.record_completion(child, outcome.clone()).await?;
            return Ok(Some(outcome));
        }
        self.record_completion(
            child,
            ChildOutcome::Cancelled {
                reason: CancelReason::Timeout,
            },
        )
        .await?;
        Ok(None)
    }

    /// Parent cancellation: signal the child through the shared registry, give
    /// it `cancel_grace` to close its stream, then record `Cancelled{Parent}`
    /// unless an outcome is already committed (R3/SA8).
    pub(crate) async fn cancel_wait(
        &self,
        child: ChildRef,
        agent: &Agent,
    ) -> Result<(), WaitError> {
        let parent_events = scan_decoded(&*self.log, child.parent.session)
            .await
            .map_err(WaitError::from)?;
        if find_completion(&parent_events, child.child).is_some() {
            return Ok(());
        }
        let grace = find_spawn(&parent_events, child.parent.tool_call)
            .map(|record| record.config.subagents.cancel_grace)
            // No spawn record violates an invariant; fall back to the default grace.
            .unwrap_or(SubagentConfig::default().cancel_grace);

        self.drain_child(agent, child.child, grace).await;

        let parent_events = scan_decoded(&*self.log, child.parent.session)
            .await
            .map_err(WaitError::from)?;
        // A committed outcome (possibly Completed) is authoritative.
        if find_completion(&parent_events, child.child).is_some() {
            return Ok(());
        }
        self.record_completion(
            &child,
            ChildOutcome::Cancelled {
                reason: CancelReason::Parent,
            },
        )
        .await?;
        Ok(())
    }

    /// The spawn record for this parent tool call, if it was admitted.
    pub(crate) async fn spawned_child(
        &self,
        parent: ParentRef,
    ) -> Result<Option<ChildRef>, ExecutionError> {
        let events = scan_decoded(&*self.log, parent.session).await?;
        Ok(
            find_spawn(&events, parent.tool_call).map(|record| ChildRef {
                parent,
                child: record.child,
            }),
        )
    }

    /// Drive a child after cancellation. `run` registers a fresh token, so the
    /// registry is signalled after the stream is built.
    async fn drain_child(&self, agent: &Agent, session: SessionId, grace: Duration) {
        let mut stream = agent.run(RunRequest {
            session,
            trigger: RunTrigger::Resume,
        });
        self.cancellations.cancel(session);
        let deadline = tokio::time::sleep(grace);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                biased;
                _ = &mut deadline => break,
                event = stream.next() => match event {
                    Some(Ok(_)) => {}
                    _ => break,
                },
            }
        }
    }

    /// Projection of the parent stream at the spawn step's `StepStarted`.
    async fn fork_snapshot(
        &self,
        parent: ParentRef,
        at: LogId,
    ) -> Result<Vec<Item>, ExecutionError> {
        let entries = scan_decoded(&*self.log, parent.session).await?;
        let items = fork_snapshot_at(&entries, at)?.items;
        Ok(items)
    }

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
        let events = scan_decoded(&*self.log, child).await?;

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
                        parent: Some(parent.clone()),
                        // Children never inherit host metadata (ADR-0032).
                        metadata: Default::default(),
                    },
                    Some(dedup::session_created(child)),
                )
                .await?;
            }
        }

        // #49: the fork snapshot is materialized once into the child log; it is
        // never a live reference to the parent stream.
        if let ContextPolicy::Fork { at } = context
            && !events
                .iter()
                .any(|(_, event)| matches!(event, SessionEvent::ContextForked { .. }))
        {
            let items = self.fork_snapshot(parent.clone(), *at).await?;
            self.append(
                child,
                SessionEvent::ContextForked {
                    source: ForkSource::Spawn(parent),
                    source_log: *at,
                    items,
                },
                Some(dedup::context_forked(child)),
            )
            .await?;
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
        self.release_slot(child.parent.session, child.parent.tool_call);
        self.waits.wake_child(child.child);
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

/// Removes a still-queued admission when the spawn future is dropped.
struct QueueGuard<'a> {
    scheduler: &'a Scheduler,
    parent: SessionId,
    seq: u64,
    tool_call: ToolCallId,
    armed: bool,
}

impl Drop for QueueGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut state = self
            .scheduler
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(entry) = state.parents.get_mut(&self.parent)
            && entry.queue.get(&self.seq) == Some(&self.tool_call)
        {
            entry.queue.remove(&self.seq);
        }
    }
}

struct SpawnRecord {
    deadline: Option<Timestamp>,
    child: SessionId,
    task: Vec<Part>,
    config: ConfigSnapshot,
    context: ContextPolicy,
}

/// A `Fork` snapshot default replaces the tool's implicit `Fresh`; an explicit
/// `Fork` request always wins. The default's `at` is a placeholder: the spawn
/// step's `StepStarted` LogId is the watermark.
fn resolve_context(
    requested: &ContextPolicy,
    default: &ContextPolicy,
    events: &[(LogEntry, SessionEvent)],
    caused_by: LogId,
) -> ContextPolicy {
    if !matches!(requested, ContextPolicy::Fresh) {
        return requested.clone();
    }
    let ContextPolicy::Fork { .. } = default else {
        return ContextPolicy::Fresh;
    };
    match spawn_step_start(events, caused_by) {
        Some(entry) => ContextPolicy::Fork { at: entry.log_id },
        None => ContextPolicy::Fresh,
    }
}

/// The parent's fork snapshot at `at`, the spawn step's `StepStarted`.
fn fork_snapshot_at(
    entries: &[(LogEntry, SessionEvent)],
    at: LogId,
) -> Result<fork::Snapshot, ExecutionError> {
    let prefix = fork::spawn_prefix(entries, at).ok_or_else(|| {
        backend(format!(
            "fork point LogId({}) is not in the parent log",
            at.as_i64()
        ))
    })?;
    fork::snapshot(&prefix).map_err(backend)
}

/// A forked child's system: the parent's baseline at the fork point. The fork
/// point is the spawn step's `StepStarted`, so no change falls between it and
/// the config watermark (ADR-0030).
fn fork_system(
    entries: &[(LogEntry, SessionEvent)],
    at: LogId,
) -> Result<Vec<Part>, ExecutionError> {
    Ok(fork_snapshot_at(entries, at)?.system)
}

/// The `StepStarted` of the step whose `ToolCallRequested` is `caused_by`.
fn spawn_step_start(events: &[(LogEntry, SessionEvent)], caused_by: LogId) -> Option<&LogEntry> {
    let caused_at = events
        .iter()
        .position(|(entry, _)| entry.log_id == caused_by)?;
    let SessionEvent::ToolCallRequested {
        turn_id, step_id, ..
    } = &events[caused_at].1
    else {
        return None;
    };
    events[..caused_at]
        .iter()
        .rev()
        .find_map(|(entry, event)| match event {
            SessionEvent::StepStarted {
                turn_id: started_turn,
                step_id: started_step,
            } if started_turn == turn_id && started_step == step_id => Some(entry),
            _ => None,
        })
}

fn find_spawn(events: &[(LogEntry, SessionEvent)], tool_call: ToolCallId) -> Option<SpawnRecord> {
    events.iter().find_map(|(_, event)| match event {
        SessionEvent::ChildSessionSpawned {
            child,
            task,
            config,
            context,
            parent_tool_call_id,
            deadline,
            ..
        } if *parent_tool_call_id == tool_call => Some(SpawnRecord {
            deadline: *deadline,
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
        // The refused output is not the child's answer, and no earlier message is either.
        TurnEndReason::Refused => ChildOutcome::Failed {
            reason: ChildFailure::Refused,
            summary: Summary { items: Vec::new() },
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
                ..
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

async fn drive_child(agent: &Agent, session: SessionId) -> Result<(), AgentError> {
    let mut stream: BoxStream<'_, Result<asterpath_protocol::AgentEvent, AgentError>> =
        agent.run(RunRequest {
            session,
            trigger: RunTrigger::Resume,
        });
    while let Some(event) = stream.next().await {
        event?;
    }
    Ok(())
}
