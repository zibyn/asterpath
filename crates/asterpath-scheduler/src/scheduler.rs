//! Spawn, materialization, outcome mapping and the durable wait.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};
use std::time::Duration;

use asterpath_core::{
    Clock, DurableExecutor, EventLog, ExecutionError, LogEntry, LogId, LogQuery, ModuleCtx,
    SessionCatalog, SessionId, SystemClock, TimerId, TimerRequest, ToolCallId, WakeCause,
    unique_bytes,
};
use asterpath_protocol::{
    AgentError, AgentRunner, CancelReason, CancellationRegistry, ChildFailure, ChildOutcome,
    ConfigSnapshot, ContextPolicy, Item, ModelAlias, ModelSpec, ParentRef, Part, ProviderId,
    RecordAppended, Role, RunRequest, RunTrigger, SessionEvent, SubagentConfig, Summary,
    TurnEndReason, decode_entry, dedup, encode_entry,
};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;

use crate::tool::SPAWN_TOOL_NAME;
use crate::{ChildRef, SpawnError, SpawnRequest, SubagentSpawner, WaitError};

pub(crate) const DEFAULT_MAX_LIVE_CHILDREN: NonZeroUsize = NonZeroUsize::new(32).expect("non-zero");

/// Consecutive child spawns; used for the depth check.
const MAX_PARENT_CHAIN: u8 = 64;

/// The one timer slot a spawn owns; the executor derives the durable id.
const SPAWN_TIMEOUT_SLOT: TimerId = TimerId::from_bytes([0; 16]);

pub struct Scheduler {
    log: Arc<dyn EventLog>,
    catalog: Arc<dyn SessionCatalog>,
    executor: Arc<dyn DurableExecutor>,
    aliases: BTreeMap<ModelAlias, ModelSpec>,
    clock: Arc<dyn Clock>,
    /// Process-wide cap on live children across all parents.
    max_live_children: NonZeroUsize,
    /// Shared with the agent runner so child drives see cancellation.
    cancellations: Arc<CancellationRegistry>,
    /// Set by the composition root after the agent is built. Weak: the agent
    /// reaches the module bus and this scheduler, so an Arc here would leak the
    /// whole composition root.
    runner: OnceLock<Weak<dyn AgentRunner>>,
    bus: Mutex<Option<ModuleCtx>>,
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
    pub fn new(
        log: Arc<dyn EventLog>,
        catalog: Arc<dyn SessionCatalog>,
        executor: Arc<dyn DurableExecutor>,
        aliases: BTreeMap<ModelAlias, ModelSpec>,
    ) -> Self {
        Self::with_runtime(
            log,
            catalog,
            executor,
            aliases,
            Arc::new(SystemClock),
            DEFAULT_MAX_LIVE_CHILDREN,
            Arc::new(CancellationRegistry::new()),
        )
    }

    /// Full wiring: the facade passes the injected clock, the global live-child
    /// cap and the registry the agent shares.
    pub fn with_runtime(
        log: Arc<dyn EventLog>,
        catalog: Arc<dyn SessionCatalog>,
        executor: Arc<dyn DurableExecutor>,
        aliases: BTreeMap<ModelAlias, ModelSpec>,
        clock: Arc<dyn Clock>,
        max_live_children: NonZeroUsize,
        cancellations: Arc<CancellationRegistry>,
    ) -> Self {
        Self {
            log,
            catalog,
            executor,
            aliases,
            clock,
            max_live_children,
            cancellations,
            runner: OnceLock::new(),
            bus: Mutex::new(None),
            admission: Mutex::new(Admission::default()),
            notify: tokio::sync::Notify::new(),
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
            self.recover_parent(req.parent.session, &parent_events)
                .await
                .map_err(SpawnError::Execution)?;
            self.prune_queue(req.parent.session, &parent_events);

            // R1: one child per parent tool call, ever.
            if let Some(record) = find_spawn(&parent_events, req.parent.tool_call) {
                let completed = find_completion(&parent_events, record.child).is_some();
                self.ensure_timeout(req.parent.session, record.log_id, timeout, completed)
                    .await
                    .map_err(SpawnError::Execution)?;
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

            let config = ConfigSnapshot {
                model,
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

            let child = SessionId::from_bytes(unique_bytes(self.clock.now()));
            let spawn_log = self
                .append(
                    req.parent.session,
                    SessionEvent::ChildSessionSpawned {
                        child,
                        task: req.task.clone(),
                        config: config.clone(),
                        context: context.clone(),
                        parent_tool_call_id: req.parent.tool_call,
                        caused_by: req.caused_by,
                    },
                    Some(dedup::child_session(child)),
                )
                .await
                .map_err(SpawnError::Execution)?;

            self.ensure_timeout(req.parent.session, spawn_log, timeout, false)
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

    fn await_completion(&self, child: &ChildRef) -> BoxFuture<'_, Result<ChildOutcome, WaitError>> {
        let child = child.clone();
        Box::pin(async move {
            let mut parent_events = scan(&*self.log, child.parent.session)
                .await
                .map_err(WaitError::Execution)?;

            // R3: a recorded outcome wins; the child is never queried again.
            if let Some(outcome) = find_completion(&parent_events, child.child) {
                self.stop_timeout(&child, &parent_events).await;
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
            let spawn_log = record.log_id;

            // An unreadable child becomes a recorded `Lost`, never a hang.
            if let Err(error) = scan(&*self.log, child.child).await {
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
            let mut child_events = scan(&*self.log, child.child)
                .await
                .map_err(WaitError::Execution)?;

            if let Some(outcome) = child_outcome(&child_events) {
                self.record_completion(&child, outcome.clone()).await?;
                self.stop_timeout(&child, &parent_events).await;
                return wait_outcome(&outcome);
            }

            let Some(runner) = self.runner.get().and_then(Weak::upgrade) else {
                return Err(WaitError::Execution(backend(
                    "agent runner is not configured on the scheduler",
                )));
            };

            loop {
                parent_events = scan(&*self.log, child.parent.session)
                    .await
                    .map_err(WaitError::Execution)?;
                if let Some(outcome) = find_completion(&parent_events, child.child) {
                    self.stop_timeout(&child, &parent_events).await;
                    return wait_outcome(&outcome);
                }

                child_events = match scan(&*self.log, child.child).await {
                    Ok(events) => events,
                    Err(error) => return self.lost(&child, error.to_string()).await,
                };
                // R3 applies to the child's own terminal too: it wins over a
                // due timer and turns `TimerFired` into `TimerCancelled`.
                if let Some(outcome) = child_outcome(&child_events) {
                    self.record_completion(&child, outcome.clone()).await?;
                    self.stop_timeout(&child, &parent_events).await;
                    return wait_outcome(&outcome);
                }

                let timer_sleep = match find_timer(&parent_events, spawn_log) {
                    Some(TimeoutTimer::Pending { timer_id, deadline }) => {
                        if deadline <= self.clock.now() {
                            // The child terminal was checked just above, before
                            // any `TimerFired` is written.
                            match self
                                .executor
                                .await_wake(child.parent.session, WakeCause::Timer(timer_id))
                                .await
                            {
                                Ok(()) => {
                                    match self
                                        .time_out(&child, record.config.subagents.cancel_grace)
                                        .await?
                                    {
                                        Some(outcome) => return wait_outcome(&outcome),
                                        None => return Err(WaitError::Timeout),
                                    }
                                }
                                // Another writer cancelled the timer.
                                Err(ExecutionError::Cancelled) => None,
                                Err(error) => return Err(WaitError::Execution(error)),
                            }
                        } else {
                            Some(Duration::from_millis(
                                (deadline - self.clock.now()).max(0) as u64
                            ))
                        }
                    }
                    Some(TimeoutTimer::Fired) => {
                        return match self
                            .time_out(&child, record.config.subagents.cancel_grace)
                            .await?
                        {
                            Some(outcome) => wait_outcome(&outcome),
                            None => Err(WaitError::Timeout),
                        };
                    }
                    _ => None,
                };

                let drive = drive_child(Arc::clone(&runner), child.child);
                tokio::pin!(drive);
                let wake = self
                    .executor
                    .await_wake(child.parent.session, WakeCause::ChildCompleted(child.child));
                tokio::pin!(wake);
                let timer_wait = async {
                    match timer_sleep {
                        Some(wait) => tokio::time::sleep(wait).await,
                        None => std::future::pending().await,
                    }
                };
                tokio::pin!(timer_wait);

                let driven = tokio::select! {
                    result = &mut drive => Some(result),
                    _ = &mut wake => None,
                    _ = &mut timer_wait => None,
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
        Box::pin(self.cancel_wait(child))
    }
}

impl Scheduler {
    /// Rebuild this parent's queue and Running count from the log. A child
    /// with `ChildSessionSpawned` and no completion is Running and is never
    /// re-queued; open spawns wait in `seq` order.
    async fn recover_parent(
        &self,
        parent: SessionId,
        events: &[(LogEntry, SessionEvent)],
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

        let mut spawned: HashMap<ToolCallId, SessionId> = HashMap::new();
        let mut completed: HashSet<SessionId> = HashSet::new();
        let mut finished: HashSet<ToolCallId> = HashSet::new();
        for (_, event) in events {
            match event {
                SessionEvent::ChildSessionSpawned {
                    parent_tool_call_id,
                    child,
                    ..
                } => {
                    spawned.insert(*parent_tool_call_id, *child);
                }
                SessionEvent::ChildSessionCompleted { child, .. } => {
                    completed.insert(*child);
                }
                SessionEvent::ToolResult { tool_call_id, .. } => {
                    finished.insert(*tool_call_id);
                }
                _ => {}
            }
        }
        let mut queue = BTreeMap::new();
        for (entry, event) in events {
            if let SessionEvent::ToolCallRequested {
                tool_call_id, name, ..
            } = event
                && name.as_str() == SPAWN_TOOL_NAME
                && !spawned.contains_key(tool_call_id)
                && !finished.contains(tool_call_id)
            {
                queue.insert(entry.seq, *tool_call_id);
            }
        }
        let running: Vec<ToolCallId> = spawned
            .iter()
            .filter(|(_, child)| !completed.contains(child))
            .map(|(call, _)| *call)
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
    fn prune_queue(&self, parent: SessionId, events: &[(LogEntry, SessionEvent)]) {
        let finished: HashSet<ToolCallId> = events
            .iter()
            .filter_map(|(_, event)| match event {
                SessionEvent::ToolResult { tool_call_id, .. } => Some(*tool_call_id),
                _ => None,
            })
            .collect();
        if finished.is_empty() {
            return;
        }
        let mut state = self
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(entry) = state.parents.get_mut(&parent) {
            entry.queue.retain(|_, call| !finished.contains(call));
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

    /// Schedule the durable spawn timeout exactly once.
    async fn ensure_timeout(
        &self,
        session: SessionId,
        spawn_log: LogId,
        timeout: Option<Duration>,
        completed: bool,
    ) -> Result<(), ExecutionError> {
        let Some(timeout) = timeout else {
            return Ok(());
        };
        // A completed spawn never opens a wait.
        if completed {
            return Ok(());
        }
        let millis = i64::try_from(timeout.as_millis()).unwrap_or(i64::MAX);
        self.executor
            .schedule_timer(TimerRequest {
                session,
                timer: SPAWN_TIMEOUT_SLOT,
                caused_by: spawn_log,
                deadline: self.clock.now().saturating_add(millis),
            })
            .await?;
        Ok(())
    }

    /// Child completed first: `TimerCancelled` replaces `TimerFired`.
    async fn stop_timeout(&self, child: &ChildRef, parent_events: &[(LogEntry, SessionEvent)]) {
        let Some(record) = find_spawn(parent_events, child.parent.tool_call) else {
            return;
        };
        if let Some(TimeoutTimer::Pending { timer_id, .. }) =
            find_timer(parent_events, record.log_id)
        {
            let _ = self
                .executor
                .cancel_timer(child.parent.session, timer_id)
                .await;
        }
    }

    /// Timeout: cancel the child and give it `grace` to close. R3 is rechecked
    /// after the drain; a terminal that landed in the window wins over
    /// `Cancelled{Timeout}`. Returns that outcome, or `None` for the timeout.
    async fn time_out(
        &self,
        child: &ChildRef,
        grace: Duration,
    ) -> Result<Option<ChildOutcome>, WaitError> {
        self.drain_child(child.child, grace).await;
        let child_events = scan(&*self.log, child.child)
            .await
            .map_err(WaitError::Execution)?;
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
    pub(crate) async fn cancel_wait(&self, child: ChildRef) -> Result<(), WaitError> {
        let parent_events = scan(&*self.log, child.parent.session)
            .await
            .map_err(WaitError::Execution)?;
        if find_completion(&parent_events, child.child).is_some() {
            return Ok(());
        }
        let grace = find_spawn(&parent_events, child.parent.tool_call)
            .map(|record| record.config.subagents.cancel_grace)
            // No spawn record violates an invariant; fall back to the default grace.
            .unwrap_or(SubagentConfig::default().cancel_grace);

        self.drain_child(child.child, grace).await;

        let parent_events = scan(&*self.log, child.parent.session)
            .await
            .map_err(WaitError::Execution)?;
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
        self.stop_timeout(&child, &parent_events).await;
        Ok(())
    }

    /// The spawn record for this parent tool call, if it was admitted.
    pub(crate) async fn spawned_child(
        &self,
        parent: ParentRef,
    ) -> Result<Option<ChildRef>, ExecutionError> {
        let events = scan(&*self.log, parent.session).await?;
        Ok(
            find_spawn(&events, parent.tool_call).map(|record| ChildRef {
                parent,
                child: record.child,
            }),
        )
    }

    /// Drive a child after cancellation. `run` registers a fresh token, so the
    /// registry is signalled after the stream is built.
    async fn drain_child(&self, session: SessionId, grace: Duration) {
        let Some(runner) = self.runner.get().and_then(Weak::upgrade) else {
            return;
        };
        let mut stream = runner.run(RunRequest {
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
    /// `CompactionApplied` folds regardless of the waterline, mirroring the
    /// agent's request projection; nothing after the waterline is visible.
    async fn fork_snapshot(
        &self,
        parent: ParentRef,
        at: LogId,
        provider: &ProviderId,
    ) -> Result<Vec<Item>, ExecutionError> {
        let entries = scan(&*self.log, parent.session).await?;
        let Some(at_seq) = entries
            .iter()
            .find(|(entry, _)| entry.log_id == at)
            .map(|(entry, _)| entry.seq)
        else {
            return Err(backend(format!(
                "fork point LogId({}) is not in the parent log",
                at.as_i64()
            )));
        };
        let prefix: Vec<LogEntry> = entries
            .into_iter()
            .filter(|(entry, event)| {
                entry.seq <= at_seq || matches!(event, SessionEvent::CompactionApplied { .. })
            })
            .map(|(entry, _)| entry)
            .collect();
        let items = asterpath_protocol::history(&prefix).map_err(backend)?;
        Ok(drop_foreign_items(items, provider))
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
                        parent: Some(parent.clone()),
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
            let items = self
                .fork_snapshot(parent.clone(), *at, &config.model.provider)
                .await?;
            self.append(
                child,
                SessionEvent::ContextForked {
                    source: parent,
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
    log_id: LogId,
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
    let Some(caused_at) = events
        .iter()
        .position(|(entry, _)| entry.log_id == caused_by)
    else {
        return ContextPolicy::Fresh;
    };
    let SessionEvent::ToolCallRequested {
        turn_id, step_id, ..
    } = &events[caused_at].1
    else {
        return ContextPolicy::Fresh;
    };
    let at = events[..caused_at]
        .iter()
        .rev()
        .find_map(|(entry, event)| match event {
            SessionEvent::StepStarted {
                turn_id: started_turn,
                step_id: started_step,
            } if started_turn == turn_id && started_step == step_id => Some(entry.log_id),
            _ => None,
        });
    match at {
        Some(at) => ContextPolicy::Fork { at },
        None => ContextPolicy::Fresh,
    }
}

fn find_spawn(events: &[(LogEntry, SessionEvent)], tool_call: ToolCallId) -> Option<SpawnRecord> {
    events.iter().find_map(|(entry, event)| match event {
        SessionEvent::ChildSessionSpawned {
            child,
            task,
            config,
            context,
            parent_tool_call_id,
            ..
        } if *parent_tool_call_id == tool_call => Some(SpawnRecord {
            log_id: entry.log_id,
            child: *child,
            task: task.clone(),
            config: config.clone(),
            context: context.clone(),
        }),
        _ => None,
    })
}

enum TimeoutTimer {
    Pending { timer_id: TimerId, deadline: i64 },
    Fired,
    Cancelled,
}

/// The spawn's timeout timer: scheduled under `caused_by = ChildSessionSpawned`.
fn find_timer(events: &[(LogEntry, SessionEvent)], spawn_log: LogId) -> Option<TimeoutTimer> {
    let (timer_id, deadline) = events.iter().find_map(|(_, event)| match event {
        SessionEvent::TimerScheduled {
            timer_id,
            deadline,
            caused_by,
        } if *caused_by == spawn_log => Some((*timer_id, *deadline)),
        _ => None,
    })?;
    let terminal = events.iter().rev().find_map(|(_, event)| match event {
        SessionEvent::TimerFired { timer_id: fired } if *fired == timer_id => Some(true),
        SessionEvent::TimerCancelled {
            timer_id: cancelled,
        } if *cancelled == timer_id => Some(false),
        _ => None,
    });
    Some(match terminal {
        Some(true) => TimeoutTimer::Fired,
        Some(false) => TimeoutTimer::Cancelled,
        None => TimeoutTimer::Pending { timer_id, deadline },
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

/// Cross-provider rule (#11 §11): opaque items from another provider are not
/// copied into the fork snapshot at all; there is no translation.
fn drop_foreign_items(items: Vec<Item>, provider: &ProviderId) -> Vec<Item> {
    items
        .into_iter()
        .filter_map(|item| match item {
            Item::Reasoning { raw, .. } if &raw.provider != provider => None,
            Item::ProviderOpaque {
                provider: item_provider,
                ..
            }
            | Item::ServerToolCall {
                provider: item_provider,
                ..
            }
            | Item::ServerToolResult {
                provider: item_provider,
                ..
            } if &item_provider != provider => None,
            Item::Message { role, parts } => Some(Item::Message {
                role,
                parts: drop_foreign_parts(parts, provider),
            }),
            Item::ToolResult {
                call_id,
                provider_item_id,
                parts,
                is_error,
                raw,
            } => Some(Item::ToolResult {
                call_id,
                provider_item_id,
                parts: drop_foreign_parts(parts, provider),
                is_error,
                raw,
            }),
            other => Some(other),
        })
        .collect()
}

fn drop_foreign_parts(parts: Vec<Part>, provider: &ProviderId) -> Vec<Part> {
    parts
        .into_iter()
        .filter(|part| {
            !matches!(part, Part::ProviderOpaque { provider: item_provider, .. }
                if item_provider != provider)
        })
        .collect()
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
