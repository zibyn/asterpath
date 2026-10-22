//! Agent loop: multi-step turns, the fixed tool pipeline, log-derived state and
//! crash recovery.
//!
//! One driver per session; model-attempt retries inside one step; the
//! `StepStarted.seq` projection waterline; parallel/exclusive tool scheduling
//! scoped to a step. Recovery reads only the committed prefix and handles open
//! intents; completed history is never replayed from the session head.

// The public error enums are fixed by packaging.md §3.4–3.5.
#![allow(clippy::result_large_err)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use asterpath_core::{
    ApprovalGate, AwaitSignal, Clock, DedupKey, DurableExecutor, EventLog, LogEntry, LogId,
    LogQuery, ModuleCtx, SessionId, SignalIntent, SignalPayload, StepId, TimerId, ToolCallId,
    TurnId,
};
use asterpath_protocol::{
    AbortKind, AgentError, AgentEvent, AgentRunner, ApprovalDecision, ApprovalPolicy,
    ApprovalRequest, ApprovalVerdict, CancelReason, CancelRegistration, CancelToken,
    CancellationRegistry, ChildOutcome, CompactPlan, CompactResult, CompactSummary,
    CompactionAbortReason, CompactionError, CompactionInput, CompactionOutcome, CompactionTrigger,
    Compactor, ConfigSnapshot, Item, ModelRequest, NO_APPROVAL_ENDPOINT, OpenIntents, Parallelism,
    PostExecuteError, PostExecuteEvent, PreExecuteError, PreExecuteEvent, PreStepGateDecision,
    PreStepGateEvent, ProjectedOrigin, Provenance, ProviderCatalog, ProviderError,
    ProviderErrorKind, ReplaySafety, ResumeReason, RunRequest, SessionEvent, SessionState,
    StopReason, StreamEvent, Tool, ToolArgs, ToolConfig, ToolContext, ToolDescriptor, ToolError,
    ToolErrorInfo, ToolErrorKind, ToolName, ToolOutput, ToolRegistry, ToolResultStatus, TurnConfig,
    TurnEndReason, Usage, UsageAnchor, approval_signal_id, approval_signal_name, decode_entry,
    dedup,
};
use futures_core::Stream;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt, stream};

const DENIED_WITHOUT_REASON: &str = "tool call denied";

/// Default turn driver.
pub struct Agent {
    log: Arc<dyn EventLog>,
    providers: Arc<ProviderCatalog>,
    tools: Arc<dyn ToolRegistry>,
    tool_config: ToolConfig,
    approval: Arc<dyn ApprovalPolicy>,
    gate: Arc<ApprovalGate>,
    executor: Arc<dyn DurableExecutor>,
    clock: Arc<dyn Clock>,
    hooks: Option<ModuleCtx>,
    compactor: Option<Arc<dyn Compactor>>,
    busy: Arc<Mutex<HashSet<SessionId>>>,
    cancellations: Arc<CancellationRegistry>,
}

impl Agent {
    /// The composition root supplies policy, gate and durable executor; this
    /// module never constructs them, so approval waits are real.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        log: Arc<dyn EventLog>,
        providers: Arc<ProviderCatalog>,
        tools: Arc<dyn ToolRegistry>,
        tool_config: ToolConfig,
        approval: Arc<dyn ApprovalPolicy>,
        gate: Arc<ApprovalGate>,
        executor: Arc<dyn DurableExecutor>,
        clock: Arc<dyn Clock>,
        hooks: Option<ModuleCtx>,
    ) -> Self {
        Self {
            log,
            providers,
            tools,
            tool_config,
            approval,
            gate,
            executor,
            clock,
            hooks,
            compactor: None,
            busy: Arc::new(Mutex::new(HashSet::new())),
            cancellations: Arc::new(CancellationRegistry::new()),
        }
    }

    /// Inject the optional compaction capability. `None` means compaction is
    /// unavailable everywhere (`NotConfigured`).
    #[must_use]
    pub fn with_compactor(mut self, compactor: Option<Arc<dyn Compactor>>) -> Self {
        self.compactor = compactor;
        self
    }

    /// Share the facade's per-session cancellation registry. Without this the
    /// agent keeps a private one, so direct `AgentRunner` users still work.
    #[must_use]
    pub fn with_cancellations(mut self, cancellations: Arc<CancellationRegistry>) -> Self {
        self.cancellations = cancellations;
        self
    }

    fn acquire(&self, session: SessionId) -> bool {
        self.busy
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(session)
    }

    async fn scan(&self, session: SessionId) -> Result<Vec<LogEntry>, AgentError> {
        let mut stream = self.log.scan(LogQuery {
            session,
            after: None,
            limit: None,
        });
        let mut entries = Vec::new();
        while let Some(entry) = stream.next().await {
            entries.push(entry?);
        }
        Ok(entries)
    }

    async fn append(
        &self,
        session: SessionId,
        event: SessionEvent,
        key: Option<DedupKey>,
    ) -> Result<LogId, AgentError> {
        let entry = asterpath_protocol::encode_entry(session, key, &event).map_err(|error| {
            AgentError::Corrupt {
                session,
                detail: error.to_string(),
            }
        })?;
        Ok(self.log.append(entry).await?)
    }

    async fn execute(&self, session: SessionId, cancel: &CancelToken, events: &EventSink) {
        if let Err(error) = self.execute_inner(session, cancel, events).await {
            events.push(Err(error));
        }
    }

    async fn execute_inner(
        &self,
        session: SessionId,
        cancel: &CancelToken,
        events: &EventSink,
    ) -> Result<(), AgentError> {
        let mut decoded = decode_entries(self.scan(session).await?, session)?;
        let config = decoded
            .iter()
            .find_map(|(_, event)| match event {
                SessionEvent::SessionCreated { config, .. } => Some(config.clone()),
                _ => None,
            })
            .ok_or_else(|| AgentError::Corrupt {
                session,
                detail: "session log has no SessionCreated".to_owned(),
            })?;

        let mut intents = OpenIntents::default();
        for (entry, event) in &decoded {
            intents.apply(event, entry.dedup_key.as_ref());
        }

        // R4: a child whose parent already stopped waiting is an orphan; close
        // its open intents as cancelled and never open another turn.
        if self.parent_recorded_completion(session, &decoded).await? {
            return self
                .close_orphan(session, &decoded, intents.state(), events)
                .await;
        }

        // A dangling compaction intent is redone only when a compactor is
        // assembled; otherwise it stays open and never blocks Idle.
        if self.compactor.is_some()
            && intents.compactions_are_open()
            && let Some(reason) = self
                .resolve_open_compaction(session, &decoded, &config)
                .await?
        {
            decoded = decode_entries(self.scan(session).await?, session)?;
            intents = OpenIntents::default();
            for (entry, event) in &decoded {
                intents.apply(event, entry.dedup_key.as_ref());
            }
            if let SessionState::TurnOpen { turn, step } = intents.state() {
                if step.is_some()
                    && let Some((step_id, _)) = open_step(&decoded, turn)
                {
                    self.commit_step(session, turn, step_id).await?;
                }
                return self.commit_turn(session, turn, reason, events).await;
            }
        }

        match intents.state() {
            SessionState::Idle => {
                if !has_pending_input(&decoded) {
                    events.push(Ok(AgentEvent::Idle { session }));
                    return Ok(());
                }
                let turn = TurnId::from_bytes(fresh_id());
                self.append(session, SessionEvent::TurnStarted { turn_id: turn }, None)
                    .await?;
                events.push(Ok(AgentEvent::TurnStarted { turn }));
                self.run_steps(session, &config, turn, None, cancel, events)
                    .await
            }
            SessionState::TurnOpen { turn, .. } => {
                self.recover(session, &decoded, &config, turn, cancel, events)
                    .await
            }
        }
    }

    /// Resume from the committed prefix. Only open intents are handled; a
    /// completed turn is never replayed from the session head.
    async fn recover(
        &self,
        session: SessionId,
        decoded: &[(LogEntry, SessionEvent)],
        config: &ConfigSnapshot,
        turn: TurnId,
        cancel: &CancelToken,
        events: &EventSink,
    ) -> Result<(), AgentError> {
        let from = decoded
            .last()
            .map(|(entry, _)| entry.log_id)
            .unwrap_or_else(|| LogId::from_i64(0));
        self.append(
            session,
            SessionEvent::StreamResumed {
                from,
                reason: ResumeReason::Crash,
            },
            None,
        )
        .await?;

        self.recover_waits(session, decoded).await?;

        let current = decode_entries(self.scan(session).await?, session)?;
        let Some((step, watermark)) = open_step(&current, turn) else {
            // Between steps: continue with the turn's normal continue rules.
            return self
                .run_steps(session, config, turn, None, cancel, events)
                .await;
        };

        if has_assistant_message(&current, turn, step) {
            // The model call is committed: never call it again.
            match self
                .resume_step(session, config, turn, step, cancel, events)
                .await?
            {
                StepOutcome::Continue => {}
                StepOutcome::Turn(reason) => {
                    return self.commit_turn(session, turn, reason, events).await;
                }
            }
            return self
                .run_steps(session, config, turn, None, cancel, events)
                .await;
        }

        let last = last_abort(&current, turn, step);
        match last.map(|(_, kind)| kind) {
            Some(AbortKind::Cancelled) => {
                self.commit_step(session, turn, step).await?;
                return self
                    .commit_turn(session, turn, TurnEndReason::Cancelled, events)
                    .await;
            }
            Some(AbortKind::Rejected) => {
                self.commit_step(session, turn, step).await?;
                return self
                    .commit_turn(session, turn, TurnEndReason::Failed, events)
                    .await;
            }
            _ => {}
        }

        let start_attempt = match last {
            Some((attempt, _)) => attempt.saturating_add(1),
            None => {
                // No attempt was ever decided: record the crash, then retry.
                self.append(
                    session,
                    SessionEvent::ModelCallAborted {
                        turn_id: turn,
                        step_id: step,
                        attempt: 1,
                        kind: AbortKind::Crash,
                        partial: None,
                        model: config.model.clone(),
                        backoff_ms: 0,
                        usage: None,
                    },
                    None,
                )
                .await?;
                2
            }
        };
        self.run_steps(
            session,
            config,
            turn,
            Some((step, watermark, start_attempt)),
            cancel,
            events,
        )
        .await
    }

    /// R4: the parent stream already carries a terminal for this child. A
    /// missing parent stream (retired) is not an orphan; an undecodable parent
    /// record is a hard error and classification never runs.
    async fn parent_recorded_completion(
        &self,
        session: SessionId,
        decoded: &[(LogEntry, SessionEvent)],
    ) -> Result<bool, AgentError> {
        let Some(parent) = decoded.iter().find_map(|(_, event)| match event {
            SessionEvent::SessionCreated {
                parent: Some(parent),
                ..
            } => Some(parent.session),
            _ => None,
        }) else {
            return Ok(false);
        };
        let parent_decoded = decode_entries(self.scan(parent).await?, parent)?;
        Ok(parent_decoded.iter().any(|(_, event)| {
            matches!(
                event,
                SessionEvent::ChildSessionCompleted { child, .. } if *child == session
            )
        }))
    }

    /// Close an orphan: `StreamResumed`, the still-open intents as cancelled
    /// terminals, then `StepCommitted` + `TurnCommitted(Cancelled)`. An orphan
    /// that already closed its turn only stops; it does not open a new one.
    async fn close_orphan(
        &self,
        session: SessionId,
        decoded: &[(LogEntry, SessionEvent)],
        state: SessionState,
        events: &EventSink,
    ) -> Result<(), AgentError> {
        let SessionState::TurnOpen { turn, .. } = state else {
            events.push(Ok(AgentEvent::Idle { session }));
            return Ok(());
        };
        let from = decoded
            .last()
            .map(|(entry, _)| entry.log_id)
            .unwrap_or_else(|| LogId::from_i64(0));
        self.append(
            session,
            SessionEvent::StreamResumed {
                from,
                reason: ResumeReason::Crash,
            },
            None,
        )
        .await?;

        let mut tools = BTreeSet::new();
        let mut timers = BTreeSet::new();
        let mut children = BTreeSet::new();
        for (_, event) in decoded {
            match event {
                SessionEvent::ToolCallRequested { tool_call_id, .. } => {
                    tools.insert(*tool_call_id);
                }
                SessionEvent::ToolResult { tool_call_id, .. } => {
                    tools.remove(tool_call_id);
                }
                SessionEvent::TimerScheduled { timer_id, .. } => {
                    timers.insert(*timer_id);
                }
                SessionEvent::TimerFired { timer_id }
                | SessionEvent::TimerCancelled { timer_id } => {
                    timers.remove(timer_id);
                }
                SessionEvent::ChildSessionSpawned { child, .. } => {
                    children.insert(*child);
                }
                SessionEvent::ChildSessionCompleted { child, .. } => {
                    children.remove(child);
                }
                _ => {}
            }
        }
        for tool_call_id in tools {
            self.append_tool_result(
                session,
                tool_call_id,
                ToolResultStatus::Interrupted,
                None,
                None,
                false,
            )
            .await?;
        }
        for timer_id in timers {
            // The runtime remains the single writer of timer terminals.
            let _ = self.executor.cancel_timer(session, timer_id).await;
        }
        for child in children {
            self.append(
                session,
                SessionEvent::ChildSessionCompleted {
                    child,
                    outcome: ChildOutcome::Cancelled {
                        reason: CancelReason::Parent,
                    },
                },
                Some(dedup::child_session(child)),
            )
            .await?;
        }
        if let Some((step, _)) = open_step(decoded, turn) {
            self.commit_step(session, turn, step).await?;
        }
        self.commit_turn(session, turn, TurnEndReason::Cancelled, events)
            .await
    }

    /// Close or refresh wait intents. Timers are recovered here; signal waits
    /// stay waiting, and child waits belong to spawn replay (#47).
    async fn recover_waits(
        &self,
        session: SessionId,
        decoded: &[(LogEntry, SessionEvent)],
    ) -> Result<(), AgentError> {
        let mut timers: BTreeMap<TimerId, (asterpath_core::Timestamp, LogId)> = BTreeMap::new();
        for (_, event) in decoded {
            match event {
                SessionEvent::TimerScheduled {
                    timer_id,
                    deadline,
                    caused_by,
                } => {
                    timers.insert(*timer_id, (*deadline, *caused_by));
                }
                SessionEvent::TimerFired { timer_id }
                | SessionEvent::TimerCancelled { timer_id } => {
                    timers.remove(timer_id);
                }
                _ => {}
            }
        }
        for (timer_id, (deadline, caused_by)) in timers {
            // A child-deadline timer is closed by spawn replay (subagents.md §9).
            if caused_by_is_child_spawn(decoded, caused_by) {
                continue;
            }
            if caused_by_is_terminal(decoded, caused_by) {
                self.append(
                    session,
                    SessionEvent::TimerCancelled { timer_id },
                    Some(dedup::timer(timer_id)),
                )
                .await?;
            } else if deadline <= self.clock.now() {
                self.append(
                    session,
                    SessionEvent::TimerFired { timer_id },
                    Some(dedup::timer(timer_id)),
                )
                .await?;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_steps(
        &self,
        session: SessionId,
        config: &ConfigSnapshot,
        turn: TurnId,
        existing_step: Option<(StepId, u64, u32)>,
        cancel: &CancelToken,
        events: &EventSink,
    ) -> Result<(), AgentError> {
        let mut pending = existing_step;
        let mut steps_started = 0u32;
        let mut auto_compactions = 0u32;

        loop {
            let (step, watermark, start_attempt) = match pending.take() {
                Some((step, watermark, attempt)) => (step, watermark, attempt),
                None => {
                    // Between steps: cancellation closes the turn with no step.
                    if cancel.is_cancelled() {
                        return self
                            .commit_turn(session, turn, TurnEndReason::Cancelled, events)
                            .await;
                    }
                    // Automatic threshold compaction (#46), before the host gate.
                    if self.compactor.is_some()
                        && auto_compactions < config.compaction.max_auto_compactions_per_turn
                    {
                        let (input, frontier) = self
                            .compaction_input(
                                session,
                                config,
                                CompactionTrigger::Threshold,
                                u64::MAX,
                            )
                            .await?;
                        match self
                            .perform_compaction(session, input, frontier, None)
                            .await?
                        {
                            CompactionRun::Done(CompactResult::Applied(_)) => {
                                auto_compactions += 1;
                            }
                            CompactionRun::Done(CompactResult::Insufficient) => {
                                return self
                                    .commit_turn(
                                        session,
                                        turn,
                                        TurnEndReason::ContextExhausted,
                                        events,
                                    )
                                    .await;
                            }
                            CompactionRun::Done(_) => {}
                            CompactionRun::Failed(_) => {
                                return self
                                    .commit_turn(
                                        session,
                                        turn,
                                        TurnEndReason::CompactionFailed,
                                        events,
                                    )
                                    .await;
                            }
                        }
                    }
                    match self.pre_step_gate(session, turn).await {
                        GateOutcome::Continue => {}
                        GateOutcome::Stop(reason) => {
                            return self
                                .commit_turn(
                                    session,
                                    turn,
                                    TurnEndReason::Stopped { reason },
                                    events,
                                )
                                .await;
                        }
                        GateOutcome::Failed => {
                            return self
                                .commit_turn(session, turn, TurnEndReason::Failed, events)
                                .await;
                        }
                    }
                    if cancel.is_cancelled() {
                        return self
                            .commit_turn(session, turn, TurnEndReason::Cancelled, events)
                            .await;
                    }
                    if let Some(max) = config.turn.max_steps_per_turn
                        && steps_started >= max
                    {
                        return self
                            .commit_turn(session, turn, TurnEndReason::MaxStepsExceeded, events)
                            .await;
                    }
                    let step = StepId::from_bytes(fresh_id());
                    let log_id = self
                        .append(
                            session,
                            SessionEvent::StepStarted {
                                turn_id: turn,
                                step_id: step,
                            },
                            None,
                        )
                        .await?;
                    let seq = self.seq_of(session, log_id).await?;
                    steps_started += 1;
                    (step, seq, 1)
                }
            };

            match self
                .run_step(
                    session,
                    config,
                    turn,
                    step,
                    watermark,
                    start_attempt,
                    cancel,
                    events,
                )
                .await?
            {
                StepOutcome::Continue => {}
                StepOutcome::Turn(reason) => {
                    return self.commit_turn(session, turn, reason, events).await;
                }
            }
        }
    }

    /// Run the host's typed pre-step waterfall; no hooks means `Continue`.
    async fn pre_step_gate(&self, session: SessionId, turn: TurnId) -> GateOutcome {
        let Some(hooks) = &self.hooks else {
            return GateOutcome::Continue;
        };
        match hooks.waterfall(PreStepGateEvent::new(session, turn)).await {
            Ok(event) => match event.decision {
                PreStepGateDecision::Continue => GateOutcome::Continue,
                PreStepGateDecision::EndTurn { reason } => GateOutcome::Stop(reason),
                // Forward-compatible decisions do not stop the turn.
                _ => GateOutcome::Continue,
            },
            Err(_) => GateOutcome::Failed,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_step(
        &self,
        session: SessionId,
        config: &ConfigSnapshot,
        turn: TurnId,
        step: StepId,
        watermark: u64,
        start_attempt: u32,
        cancel: &CancelToken,
        events: &EventSink,
    ) -> Result<StepOutcome, AgentError> {
        // The projection is computed once per step; retries reuse it (L12).
        // A forced compaction re-projects the same step.
        let mut request = self.build_request(session, config, watermark).await?;
        let max_attempts = config.turn.max_attempts.get();
        let mut attempt = start_attempt;
        let mut force_compacted = false;

        loop {
            // A step still open when cancellation arrives never calls the model.
            if cancel.is_cancelled() {
                self.abort_cancelled(session, config, turn, step, attempt, None)
                    .await?;
                self.commit_step(session, turn, step).await?;
                return Ok(StepOutcome::Turn(TurnEndReason::Cancelled));
            }

            let provider =
                self.providers
                    .get(&config.model.provider)
                    .ok_or_else(|| AgentError::Corrupt {
                        session,
                        detail: format!("provider {} is not registered", config.model.provider),
                    })?;

            let mut stream = provider.stream(request.clone());
            let mut completed: BTreeMap<u32, Item> = BTreeMap::new();
            let mut terminal: Option<(StopReason, Usage)> = None;
            let mut failure: Option<ProviderError> = None;
            let mut cancelled = false;

            let token = cancel.clone();
            let cancel_wait = token.cancelled();
            tokio::pin!(cancel_wait);
            loop {
                tokio::select! {
                    biased;
                    _ = &mut cancel_wait => {
                        cancelled = true;
                        break;
                    }
                    event = stream.next() => match event {
                        Some(Ok(StreamEvent::PartDelta { delta, .. })) => {
                            events.push(Ok(AgentEvent::ModelDelta { turn, step, delta }));
                        }
                        Some(Ok(StreamEvent::ItemCompleted {
                            output_index, item, ..
                        })) => {
                            completed.insert(output_index, item);
                        }
                        Some(Ok(StreamEvent::ResponseEnded {
                            stop_reason, usage, ..
                        })) => {
                            terminal = Some((stop_reason, usage));
                            break;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(error)) => {
                            failure = Some(error);
                            break;
                        }
                        None => break,
                    }
                }
            }
            drop(stream);

            if cancelled {
                // Abort immediately: record the cancelled attempt, commit the
                // step and end the turn without retrying (L3).
                let partial = if completed.is_empty() {
                    None
                } else {
                    Some(completed.into_values().collect())
                };
                self.abort_cancelled(session, config, turn, step, attempt, partial)
                    .await?;
                self.commit_step(session, turn, step).await?;
                return Ok(StepOutcome::Turn(TurnEndReason::Cancelled));
            }

            if let Some((stop_reason, usage)) = terminal {
                let items: Vec<Item> = completed.into_values().collect();
                self.append(
                    session,
                    SessionEvent::AssistantMessage {
                        turn_id: turn,
                        step_id: step,
                        items: items.clone(),
                        raw: None,
                        model: config.model.clone(),
                        usage,
                        stop_reason: stop_reason.clone(),
                    },
                    Some(dedup::assistant_message(turn, step)),
                )
                .await?;

                let calls = tool_calls(&items);
                let had_calls = !calls.is_empty();
                if had_calls {
                    let decoded = decode_entries(self.scan(session).await?, session)?;
                    let existing = existing_step(&decoded, turn, step);
                    let outcome = self
                        .execute_tool_calls(
                            session,
                            turn,
                            step,
                            watermark,
                            &existing,
                            calls,
                            cancel,
                            config.turn.cancel_grace,
                            false,
                            events,
                        )
                        .await?;
                    if outcome == ToolOutcome::Cancelled {
                        self.commit_step(session, turn, step).await?;
                        return Ok(StepOutcome::Turn(TurnEndReason::Cancelled));
                    }
                }

                self.commit_step(session, turn, step).await?;
                if cancel.is_cancelled() {
                    return Ok(StepOutcome::Turn(TurnEndReason::Cancelled));
                }
                let pending_input = self.pending_input(session).await?;
                return Ok(step_outcome(&stop_reason, had_calls, pending_input));
            }

            // A stream that ends without a terminal event violates S2/S8.
            let error = failure.unwrap_or_else(|| {
                ProviderError::new(
                    ProviderErrorKind::StreamProtocol,
                    "model stream ended without ResponseEnded",
                )
            });
            let will_retry = error.retryable && !error.context_exceeded() && attempt < max_attempts;
            let backoff = if will_retry {
                backoff(&config.turn, attempt)
            } else {
                Duration::ZERO
            };
            let partial: Option<Vec<Item>> = if completed.is_empty() {
                None
            } else {
                Some(completed.into_values().collect())
            };

            self.append(
                session,
                SessionEvent::ModelCallAborted {
                    turn_id: turn,
                    step_id: step,
                    attempt,
                    kind: if error.retryable {
                        AbortKind::Transport
                    } else {
                        AbortKind::Rejected
                    },
                    partial,
                    model: config.model.clone(),
                    backoff_ms: backoff.as_millis() as u64,
                    usage: error.partial_usage,
                },
                None,
            )
            .await?;

            // Context exhaustion before any `AssistantMessage`: try one forced
            // compaction inside this same step, then retry the same step.
            if error.context_exceeded() && !force_compacted {
                force_compacted = true;
                let (input, frontier) = self
                    .compaction_input(session, config, CompactionTrigger::ProviderError, watermark)
                    .await?;
                match self
                    .perform_compaction(session, input, frontier, None)
                    .await?
                {
                    CompactionRun::Done(CompactResult::Applied(_)) => {
                        request = self.build_request(session, config, watermark).await?;
                        attempt += 1;
                        continue;
                    }
                    CompactionRun::Done(_) => {
                        self.commit_step(session, turn, step).await?;
                        return Ok(StepOutcome::Turn(TurnEndReason::ContextExhausted));
                    }
                    CompactionRun::Failed(_) => {
                        self.commit_step(session, turn, step).await?;
                        return Ok(StepOutcome::Turn(TurnEndReason::CompactionFailed));
                    }
                }
            }

            if will_retry {
                if !backoff.is_zero() {
                    let token = cancel.clone();
                    let cancel_wait = token.cancelled();
                    tokio::pin!(cancel_wait);
                    tokio::select! {
                        biased;
                        _ = &mut cancel_wait => {
                            // Cancellation interrupts the backoff sleep and is
                            // the last abort the recovery classifier sees.
                            self.abort_cancelled(
                                session, config, turn, step, attempt + 1, None,
                            )
                            .await?;
                            self.commit_step(session, turn, step).await?;
                            return Ok(StepOutcome::Turn(TurnEndReason::Cancelled));
                        }
                        _ = tokio::time::sleep(backoff) => {}
                    }
                }
                if cancel.is_cancelled() {
                    self.abort_cancelled(session, config, turn, step, attempt + 1, None)
                        .await?;
                    self.commit_step(session, turn, step).await?;
                    return Ok(StepOutcome::Turn(TurnEndReason::Cancelled));
                }
                attempt += 1;
                continue;
            }

            self.commit_step(session, turn, step).await?;
            return Ok(StepOutcome::Turn(if cancel.is_cancelled() {
                TurnEndReason::Cancelled
            } else if error.context_exceeded() {
                TurnEndReason::ContextExhausted
            } else {
                TurnEndReason::Failed
            }));
        }
    }

    /// Record one aborted attempt that will not be retried.
    #[allow(clippy::too_many_arguments)]
    async fn abort_cancelled(
        &self,
        session: SessionId,
        config: &ConfigSnapshot,
        turn: TurnId,
        step: StepId,
        attempt: u32,
        partial: Option<Vec<Item>>,
    ) -> Result<(), AgentError> {
        self.append(
            session,
            SessionEvent::ModelCallAborted {
                turn_id: turn,
                step_id: step,
                attempt,
                kind: AbortKind::Cancelled,
                partial,
                model: config.model.clone(),
                backoff_ms: 0,
                usage: None,
            },
            None,
        )
        .await
        .map(|_| ())
    }

    /// Finish an open step whose `AssistantMessage` is already committed: run
    /// every call without a `ToolResult`, reusing recorded intents verbatim.
    #[allow(clippy::too_many_arguments)]
    async fn resume_step(
        &self,
        session: SessionId,
        config: &ConfigSnapshot,
        turn: TurnId,
        step: StepId,
        cancel: &CancelToken,
        events: &EventSink,
    ) -> Result<StepOutcome, AgentError> {
        let decoded = decode_entries(self.scan(session).await?, session)?;
        let (items, stop_reason) = decoded
            .iter()
            .find_map(|(_, event)| match event {
                SessionEvent::AssistantMessage {
                    turn_id,
                    step_id,
                    items,
                    stop_reason,
                    ..
                } if *turn_id == turn && *step_id == step => {
                    Some((items.clone(), stop_reason.clone()))
                }
                _ => None,
            })
            .ok_or_else(|| AgentError::Corrupt {
                session,
                detail: "open step has no AssistantMessage".to_owned(),
            })?;

        let existing = existing_step(&decoded, turn, step);
        let watermark = decoded
            .iter()
            .find_map(|(entry, event)| match event {
                SessionEvent::StepStarted { turn_id, step_id }
                    if *turn_id == turn && *step_id == step =>
                {
                    Some(entry.seq)
                }
                _ => None,
            })
            .unwrap_or(0);
        let calls = tool_calls(&items);
        let had_calls = !calls.is_empty();
        if had_calls {
            let outcome = self
                .execute_tool_calls(
                    session,
                    turn,
                    step,
                    watermark,
                    &existing,
                    calls,
                    cancel,
                    config.turn.cancel_grace,
                    true,
                    events,
                )
                .await?;
            if outcome == ToolOutcome::Cancelled {
                self.commit_step(session, turn, step).await?;
                return Ok(StepOutcome::Turn(TurnEndReason::Cancelled));
            }
        }

        self.commit_step(session, turn, step).await?;
        if cancel.is_cancelled() {
            return Ok(StepOutcome::Turn(TurnEndReason::Cancelled));
        }
        let pending_input = self.pending_input(session).await?;
        Ok(step_outcome(&stop_reason, had_calls, pending_input))
    }

    /// The fixed pipeline: pre-execute, intent, registry, approval, scheduling,
    /// invocation, truncation, post-execute, result.
    #[allow(clippy::too_many_arguments)]
    async fn execute_tool_calls(
        &self,
        session: SessionId,
        turn: TurnId,
        step: StepId,
        step_started_seq: u64,
        existing: &ExistingStep,
        calls: Vec<CallItem>,
        cancel: &CancelToken,
        cancel_grace: Duration,
        recovering: bool,
        events: &EventSink,
    ) -> Result<ToolOutcome, AgentError> {
        let lock = Arc::new(tokio::sync::RwLock::new(()));
        let semaphore = Arc::new(tokio::sync::Semaphore::new(
            self.tool_config.max_parallel_calls.get(),
        ));
        let budget = Arc::new(Mutex::new(self.tool_config.max_output_bytes_per_step));
        let mut runnable: Vec<Runnable> = Vec::new();
        let mut cancelled = cancel.is_cancelled();

        for call in calls {
            if let Some(intent) = existing.intents.get(&call.provider_call_id) {
                if existing.results.contains(&intent.tool_call_id) {
                    continue;
                }
                // A tool that was absent when its intent was recorded never
                // enters approval and is never rerun.
                if intent.provenance == Provenance::Unknown {
                    self.not_found(session, intent.tool_call_id, &intent.name)
                        .await?;
                    continue;
                }
                if cancelled {
                    self.interrupt(session, turn, step, intent.tool_call_id, events)
                        .await?;
                    continue;
                }
                match self.tools.get(&intent.name) {
                    None => {
                        self.not_found(session, intent.tool_call_id, &intent.name)
                            .await?;
                    }
                    Some(tool) => {
                        let descriptor = tool.descriptor().clone();
                        match self
                            .decide_approval(
                                session,
                                turn,
                                step,
                                intent.tool_call_id,
                                &intent.name,
                                &intent.args,
                                &descriptor,
                                intent.log_id,
                                intent.replay,
                                recovering,
                                cancel,
                                events,
                            )
                            .await?
                        {
                            ToolDecision::Allow => {
                                if cancel.is_cancelled() {
                                    cancelled = true;
                                    self.interrupt(
                                        session,
                                        turn,
                                        step,
                                        intent.tool_call_id,
                                        events,
                                    )
                                    .await?;
                                } else {
                                    runnable.push(Runnable {
                                        tool,
                                        descriptor,
                                        tool_call_id: intent.tool_call_id,
                                        name: intent.name.clone(),
                                        args: intent.args.clone(),
                                    });
                                }
                            }
                            ToolDecision::Interrupt => {
                                self.interrupt(session, turn, step, intent.tool_call_id, events)
                                    .await?;
                            }
                            ToolDecision::Deny(reason) => {
                                self.deny(session, intent.tool_call_id, reason).await?
                            }
                            ToolDecision::Cancelled => {
                                cancelled = true;
                                self.interrupt(session, turn, step, intent.tool_call_id, events)
                                    .await?;
                            }
                        }
                    }
                }
                continue;
            }

            // No recorded intent: pre-execute runs for this call only.
            let tool_call_id = ToolCallId::from_bytes(fresh_id());
            let mut args = call.raw_args;
            let failure = self
                .pre_execute(
                    session,
                    turn,
                    step,
                    tool_call_id,
                    &call.provider_call_id,
                    &call.name,
                    &mut args,
                )
                .await?;

            let tool = self.tools.get(&call.name);
            let descriptor = tool.as_ref().map(|tool| tool.descriptor().clone());
            let (replay, provenance) = match &descriptor {
                Some(descriptor) => (descriptor.replay, descriptor.provenance),
                None => (ReplaySafety::Unsafe, Provenance::Unknown),
            };
            let caused_by = self
                .append(
                    session,
                    SessionEvent::ToolCallRequested {
                        turn_id: turn,
                        step_id: step,
                        tool_call_id,
                        provider_call_id: call.provider_call_id,
                        name: call.name.clone(),
                        raw_args: args.clone(),
                        replay,
                        provenance,
                    },
                    Some(dedup::tool_call(tool_call_id)),
                )
                .await?;

            if cancelled {
                self.interrupt(session, turn, step, tool_call_id, events)
                    .await?;
                continue;
            }

            match (failure, descriptor, tool) {
                (Some(PreFailure::Denied(reason)), _, _) => {
                    self.deny(session, tool_call_id, reason).await?;
                }
                (Some(PreFailure::Internal(message)), _, _) => {
                    self.append_tool_result(
                        session,
                        tool_call_id,
                        ToolResultStatus::Failed,
                        None,
                        Some(ToolErrorInfo {
                            kind: ToolErrorKind::Internal,
                            message,
                        }),
                        false,
                    )
                    .await?;
                }
                (None, None, _) => {
                    self.append_tool_result(
                        session,
                        tool_call_id,
                        ToolResultStatus::Failed,
                        None,
                        Some(ToolErrorInfo {
                            kind: ToolErrorKind::NotFound,
                            message: format!("tool {} is not registered", call.name),
                        }),
                        false,
                    )
                    .await?;
                }
                (None, Some(descriptor), Some(tool)) => {
                    match self
                        .decide_approval(
                            session,
                            turn,
                            step,
                            tool_call_id,
                            &call.name,
                            &args,
                            &descriptor,
                            caused_by,
                            descriptor.replay,
                            false,
                            cancel,
                            events,
                        )
                        .await?
                    {
                        ToolDecision::Allow => {
                            if cancel.is_cancelled() {
                                cancelled = true;
                                self.interrupt(session, turn, step, tool_call_id, events)
                                    .await?;
                            } else {
                                runnable.push(Runnable {
                                    tool,
                                    descriptor,
                                    tool_call_id,
                                    name: call.name,
                                    args,
                                });
                            }
                        }
                        ToolDecision::Interrupt => {
                            // Only recovery returns this; a fresh call has no
                            // recorded effect to protect.
                            unreachable!("fresh intents always execute or deny")
                        }
                        ToolDecision::Deny(reason) => {
                            self.deny(session, tool_call_id, reason).await?
                        }
                        ToolDecision::Cancelled => {
                            cancelled = true;
                            self.interrupt(session, turn, step, tool_call_id, events)
                                .await?;
                        }
                    }
                }
                (None, Some(_), None) => unreachable!("descriptor comes from the registry entry"),
            }
        }

        if cancelled {
            // Calls that passed approval but never started are interrupted too.
            let pending: Vec<ToolCallId> = runnable.iter().map(|item| item.tool_call_id).collect();
            self.finish_cancelled_step(session, turn, step, step_started_seq, &pending, events)
                .await?;
            return Ok(ToolOutcome::Cancelled);
        }
        if runnable.is_empty() {
            return Ok(ToolOutcome::Done);
        }

        let pending: Vec<ToolCallId> = runnable.iter().map(|item| item.tool_call_id).collect();
        let mut futures: FuturesUnordered<BoxFuture<'_, Result<(), AgentError>>> =
            FuturesUnordered::new();
        for item in runnable {
            futures.push(self.execute_one(
                session,
                turn,
                step,
                item,
                Arc::clone(&semaphore),
                Arc::clone(&lock),
                Arc::clone(&budget),
                cancel,
                events,
            ));
        }

        let mut failure = None;
        let token = cancel.clone();
        let cancel_wait = token.cancelled();
        tokio::pin!(cancel_wait);
        loop {
            tokio::select! {
                biased;
                _ = &mut cancel_wait => break,
                result = futures.next() => match result {
                    Some(Ok(())) => {}
                    Some(Err(error)) => {
                        failure.get_or_insert(error);
                    }
                    None => return match failure {
                        Some(error) => Err(error),
                        None => Ok(ToolOutcome::Done),
                    },
                }
            }
        }

        // Cancellation grace: in-flight tools may return a real result.
        if !cancel_grace.is_zero() {
            let deadline = tokio::time::sleep(cancel_grace);
            tokio::pin!(deadline);
            loop {
                tokio::select! {
                    biased;
                    _ = &mut deadline => break,
                    result = futures.next() => match result {
                        Some(Ok(())) => {}
                        Some(Err(error)) => {
                            failure.get_or_insert(error);
                        }
                        None => break,
                    }
                }
            }
        }
        drop(futures);

        self.finish_cancelled_step(session, turn, step, step_started_seq, &pending, events)
            .await?;
        Ok(ToolOutcome::Cancelled)
    }

    /// Interrupt every call of this step that has no recorded result, then close
    /// the step's open waits. Reading the log (not driver memory) makes a real
    /// result that landed while its future was dropped still win.
    async fn finish_cancelled_step(
        &self,
        session: SessionId,
        turn: TurnId,
        step: StepId,
        step_started_seq: u64,
        pending: &[ToolCallId],
        events: &EventSink,
    ) -> Result<(), AgentError> {
        let decoded = decode_entries(self.scan(session).await?, session)?;
        let mut recorded = HashSet::new();
        let mut open_timers: BTreeMap<asterpath_core::TimerId, u64> = BTreeMap::new();
        for (entry, event) in &decoded {
            if let SessionEvent::ToolResult { tool_call_id, .. } = event {
                recorded.insert(*tool_call_id);
            }
            if entry.seq <= step_started_seq {
                continue;
            }
            match event {
                SessionEvent::TimerScheduled { timer_id, .. } => {
                    open_timers.insert(*timer_id, entry.seq);
                }
                SessionEvent::TimerFired { timer_id }
                | SessionEvent::TimerCancelled { timer_id } => {
                    open_timers.remove(timer_id);
                }
                // #48: child waits are closed by the scheduler's own terminal.
                _ => {}
            }
        }
        for tool_call_id in pending {
            if !recorded.contains(tool_call_id) {
                self.interrupt(session, turn, step, *tool_call_id, events)
                    .await?;
            }
        }
        for timer in open_timers.into_keys() {
            // A failed close must not turn cancellation into an error; the
            // runtime remains the single writer of timer terminals.
            let _ = self.executor.cancel_timer(session, timer).await;
        }
        Ok(())
    }

    /// One interrupted `ToolResult`; the dedup key makes a racing real result win.
    async fn interrupt(
        &self,
        session: SessionId,
        turn: TurnId,
        step: StepId,
        tool_call_id: ToolCallId,
        events: &EventSink,
    ) -> Result<(), AgentError> {
        self.append_tool_result(
            session,
            tool_call_id,
            ToolResultStatus::Interrupted,
            None,
            None,
            false,
        )
        .await?;
        events.push(Ok(AgentEvent::ToolFinished {
            turn,
            step,
            call: tool_call_id,
            status: ToolResultStatus::Interrupted,
        }));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_one<'a>(
        &'a self,
        session: SessionId,
        turn: TurnId,
        step: StepId,
        item: Runnable,
        semaphore: Arc<tokio::sync::Semaphore>,
        lock: Arc<tokio::sync::RwLock<()>>,
        budget: Arc<Mutex<usize>>,
        turn_cancel: &'a CancelToken,
        events: &'a EventSink,
    ) -> BoxFuture<'a, Result<(), AgentError>> {
        Box::pin(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .expect("the semaphore is never closed");
            let _guard = match item.descriptor.parallelism {
                Parallelism::Parallel => StepLockGuard::Read(lock.read().await),
                Parallelism::Exclusive => StepLockGuard::Write(lock.write().await),
            };

            events.push(Ok(AgentEvent::ToolStarted {
                turn,
                step,
                call: item.tool_call_id,
                tool: item.name.clone(),
            }));

            // A child of the turn token: the turn broadcasts to every tool, and
            // a per-call timeout cancels only this call.
            let cancel = turn_cancel.child();
            let context = ToolContext::new(session, turn, step, item.tool_call_id, cancel.clone());
            let timeout = item
                .descriptor
                .timeout
                .or(match item.descriptor.provenance {
                    Provenance::External { .. } => self.tool_config.external_timeout,
                    _ => None,
                });
            let invocation = invoke_tool(
                item.tool,
                item.args,
                context,
                cancel,
                turn_cancel.clone(),
                timeout,
                self.tool_config.kill_grace,
            )
            .await;

            let (mut status, mut output, mut error, mut truncated) = match invocation {
                Invocation::Returned(Ok(output)) => (
                    ToolResultStatus::Completed,
                    Some(output.content),
                    None,
                    false,
                ),
                Invocation::Returned(Err(error)) => (
                    ToolResultStatus::Failed,
                    None,
                    Some(ToolErrorInfo {
                        kind: error.kind(),
                        message: error.message().to_owned(),
                    }),
                    false,
                ),
                Invocation::Panic => (
                    ToolResultStatus::Failed,
                    None,
                    Some(ToolErrorInfo {
                        kind: ToolErrorKind::Panic,
                        message: "tool panicked".to_owned(),
                    }),
                    false,
                ),
                Invocation::Cancelled => (ToolResultStatus::Interrupted, None, None, false),
                Invocation::Timeout => (
                    ToolResultStatus::Failed,
                    None,
                    Some(ToolErrorInfo {
                        kind: ToolErrorKind::Timeout,
                        message: "tool call timed out".to_owned(),
                    }),
                    false,
                ),
            };

            if status == ToolResultStatus::Completed {
                let limit = item
                    .descriptor
                    .max_output_bytes
                    .unwrap_or(self.tool_config.max_output_bytes);
                let (parts, was_truncated) =
                    truncate_output(output.take().unwrap_or_default(), limit, &budget);
                output = Some(parts);
                truncated = was_truncated;
            }

            if let Some(hooks) = &self.hooks {
                let event = PostExecuteEvent::new(
                    session,
                    turn,
                    step,
                    item.tool_call_id,
                    item.name.clone(),
                    status,
                    output.clone().unwrap_or_default(),
                    error.clone(),
                    truncated,
                );
                match hooks.waterfall(event).await {
                    Ok(event) => {
                        // Status is not a post-execute concern; only content and
                        // the model-visible error message are rewritable.
                        if status == ToolResultStatus::Completed {
                            output = Some(event.output);
                            truncated = event.truncated;
                        }
                        if status == ToolResultStatus::Failed {
                            error = event.error;
                        }
                    }
                    Err(PostExecuteError::Internal { message }) => {
                        status = ToolResultStatus::Failed;
                        output = None;
                        error = Some(ToolErrorInfo {
                            kind: ToolErrorKind::Internal,
                            message,
                        });
                        truncated = false;
                    }
                    Err(_) => {}
                }
            }

            self.append_tool_result(session, item.tool_call_id, status, output, error, truncated)
                .await?;
            events.push(Ok(AgentEvent::ToolFinished {
                turn,
                step,
                call: item.tool_call_id,
                status,
            }));

            drop(_guard);
            Ok(())
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn pre_execute(
        &self,
        session: SessionId,
        turn: TurnId,
        step: StepId,
        tool_call_id: ToolCallId,
        provider_call_id: &str,
        name: &ToolName,
        args: &mut ToolArgs,
    ) -> Result<Option<PreFailure>, AgentError> {
        let Some(hooks) = &self.hooks else {
            return Ok(None);
        };
        let event = PreExecuteEvent::new(
            session,
            turn,
            step,
            tool_call_id,
            provider_call_id.to_owned(),
            name.clone(),
            args.clone(),
        );
        match hooks.waterfall(event).await {
            Ok(event) => {
                *args = event.args;
                Ok(None)
            }
            Err(PreExecuteError::Denied { reason }) => Ok(Some(PreFailure::Denied(
                reason.unwrap_or_else(|| DENIED_WITHOUT_REASON.to_owned()),
            ))),
            Err(PreExecuteError::Internal { message }) => Ok(Some(PreFailure::Internal(message))),
            Err(_) => Ok(Some(PreFailure::Internal("pre-execute failed".to_owned()))),
        }
    }

    /// A recorded approval signal wins over the current policy and is never
    /// re-prompted. `Ask` without a live endpoint fails closed.
    #[allow(clippy::too_many_arguments)]
    async fn decide_approval(
        &self,
        session: SessionId,
        turn: TurnId,
        step: StepId,
        tool_call_id: ToolCallId,
        name: &ToolName,
        args: &ToolArgs,
        descriptor: &ToolDescriptor,
        caused_by: LogId,
        replay: ReplaySafety,
        recovering: bool,
        cancel: &CancelToken,
        events: &EventSink,
    ) -> Result<ToolDecision, AgentError> {
        if let Some(payload) = self.recorded_approval(session, tool_call_id).await? {
            return approval_decision(session, payload);
        }

        let request = ApprovalRequest::new(
            session,
            turn,
            step,
            tool_call_id,
            name.clone(),
            args.clone(),
            descriptor.replay,
            descriptor.side_effect,
            descriptor.provenance,
        );
        match self.approval.classify(&request) {
            // A recorded decision above always wins. Without one, recovery
            // never reruns an unsafe tool.
            ApprovalVerdict::Allow if recovering && replay == ReplaySafety::Unsafe => {
                Ok(ToolDecision::Interrupt)
            }
            ApprovalVerdict::Allow => Ok(ToolDecision::Allow),
            ApprovalVerdict::Deny { reason } => Ok(ToolDecision::Deny(
                reason.unwrap_or_else(|| DENIED_WITHOUT_REASON.to_owned()),
            )),
            ApprovalVerdict::Ask => {
                if !self.gate.is_open() {
                    return Ok(ToolDecision::Deny(NO_APPROVAL_ENDPOINT.to_owned()));
                }
                events.push(Ok(AgentEvent::ApprovalRequested(request)));
                let wait = self.executor.await_signal(AwaitSignal {
                    session,
                    signal: approval_signal_id(tool_call_id),
                    name: approval_signal_name(tool_call_id),
                    caused_by,
                    intent: SignalIntent::AlreadyRecorded,
                });
                tokio::pin!(wait);
                let token = cancel.clone();
                let cancel_wait = token.cancelled();
                tokio::pin!(cancel_wait);
                let payload = tokio::select! {
                    biased;
                    // The interrupted ToolResult closes this call's intent.
                    _ = &mut cancel_wait => return Ok(ToolDecision::Cancelled),
                    payload = &mut wait => payload?,
                };
                approval_decision(session, payload)
            }
            _ => Ok(ToolDecision::Deny(DENIED_WITHOUT_REASON.to_owned())),
        }
    }

    async fn recorded_approval(
        &self,
        session: SessionId,
        tool_call_id: ToolCallId,
    ) -> Result<Option<SignalPayload>, AgentError> {
        let signal = approval_signal_id(tool_call_id);
        let mut stream = self.log.scan(LogQuery {
            session,
            after: None,
            limit: None,
        });
        while let Some(entry) = stream.next().await {
            let event = decode_entry(&entry?).map_err(|error| AgentError::Corrupt {
                session,
                detail: error.to_string(),
            })?;
            if let SessionEvent::SignalReceived {
                signal_id, payload, ..
            } = event
                && signal_id == signal
            {
                return Ok(Some(payload));
            }
        }
        Ok(None)
    }

    async fn deny(
        &self,
        session: SessionId,
        tool_call_id: ToolCallId,
        message: String,
    ) -> Result<(), AgentError> {
        self.append_tool_result(
            session,
            tool_call_id,
            ToolResultStatus::Failed,
            None,
            Some(ToolErrorInfo {
                kind: ToolErrorKind::Denied,
                message,
            }),
            false,
        )
        .await
    }

    /// The model-visible terminal for a tool that is not in the registry.
    async fn not_found(
        &self,
        session: SessionId,
        tool_call_id: ToolCallId,
        name: &ToolName,
    ) -> Result<(), AgentError> {
        self.append_tool_result(
            session,
            tool_call_id,
            ToolResultStatus::Failed,
            None,
            Some(ToolErrorInfo {
                kind: ToolErrorKind::NotFound,
                message: format!("tool {name} is not registered"),
            }),
            false,
        )
        .await
    }

    async fn append_tool_result(
        &self,
        session: SessionId,
        tool_call_id: ToolCallId,
        status: ToolResultStatus,
        output: Option<Vec<asterpath_protocol::Part>>,
        error: Option<ToolErrorInfo>,
        truncated: bool,
    ) -> Result<(), AgentError> {
        self.append(
            session,
            SessionEvent::ToolResult {
                tool_call_id,
                status,
                output,
                error,
                truncated,
            },
            Some(dedup::tool_result(tool_call_id)),
        )
        .await
        .map(|_| ())
    }

    async fn commit_step(
        &self,
        session: SessionId,
        turn: TurnId,
        step: StepId,
    ) -> Result<(), AgentError> {
        self.append(
            session,
            SessionEvent::StepCommitted {
                turn_id: turn,
                step_id: step,
            },
            Some(dedup::step_committed(step)),
        )
        .await
        .map(|_| ())
    }

    async fn pending_input(&self, session: SessionId) -> Result<bool, AgentError> {
        let decoded = decode_entries(self.scan(session).await?, session)?;
        Ok(has_pending_input(&decoded))
    }

    /// Project the log at the `StepStarted.seq` waterline. `CompactionApplied`
    /// is a rewrite operator and is folded regardless of the waterline.
    async fn build_request(
        &self,
        session: SessionId,
        config: &ConfigSnapshot,
        watermark: u64,
    ) -> Result<ModelRequest, AgentError> {
        let entries = self.scan(session).await?;
        let mut visible = Vec::new();
        let mut prefix_epoch = 0u32;
        for entry in entries {
            let event = decode_entry(&entry).map_err(|error| AgentError::Corrupt {
                session,
                detail: error.to_string(),
            })?;
            match event {
                SessionEvent::CompactionApplied { .. } => {
                    prefix_epoch += 1;
                    visible.push(entry);
                }
                _ if entry.seq < watermark => visible.push(entry),
                _ => {}
            }
        }
        let items = asterpath_protocol::history(&visible).map_err(|error| AgentError::Corrupt {
            session,
            detail: error.to_string(),
        })?;
        Ok(ModelRequest::new(
            config.model.clone(),
            prefix_epoch,
            config.system.clone(),
            items,
            config.tools.clone(),
            &config.request,
        ))
    }

    async fn commit_turn(
        &self,
        session: SessionId,
        turn: TurnId,
        reason: TurnEndReason,
        events: &EventSink,
    ) -> Result<(), AgentError> {
        self.append(
            session,
            SessionEvent::TurnCommitted {
                turn_id: turn,
                reason: reason.clone(),
            },
            Some(dedup::turn_committed(turn)),
        )
        .await?;
        events.push(Ok(AgentEvent::TurnCommitted { turn, reason }));
        Ok(())
    }

    async fn seq_of(&self, session: SessionId, log_id: LogId) -> Result<u64, AgentError> {
        self.scan(session)
            .await?
            .into_iter()
            .find(|entry| entry.log_id == log_id)
            .map(|entry| entry.seq)
            .ok_or_else(|| AgentError::Corrupt {
                session,
                detail: "appended record is missing from the log".to_owned(),
            })
    }
}

impl AgentRunner for Agent {
    fn run(&self, req: RunRequest) -> BoxStream<'_, Result<AgentEvent, AgentError>> {
        let session = req.session;
        if !self.acquire(session) {
            return stream::once(async move { Err(AgentError::Busy { session }) }).boxed();
        }
        let guard = BusyGuard {
            busy: Arc::clone(&self.busy),
            session,
            _cancel: self.cancellations.register(session),
        };
        let cancel = guard._cancel.token();
        let sink = Arc::new(EventSink::default());
        let future_sink = Arc::clone(&sink);
        let future: BoxFuture<'_, ()> = Box::pin(async move {
            self.execute(session, &cancel, &future_sink).await;
        });

        Box::pin(DriveStream {
            future,
            sink,
            // Dropping the stream releases single-flight; it does not cancel.
            _guard: guard,
            done: false,
        })
    }

    fn compact_now(&self, session: SessionId) -> BoxFuture<'_, Result<CompactResult, AgentError>> {
        Box::pin(async move {
            let entries = decode_entries(self.scan(session).await?, session)?;
            let mut intents = OpenIntents::default();
            for (entry, event) in &entries {
                intents.apply(event, entry.dedup_key.as_ref());
            }
            if !intents.is_idle() {
                return Err(AgentError::Busy { session });
            }
            // Share the drive single-flight; compact_now is turn-free.
            if !self.acquire(session) {
                return Err(AgentError::Busy { session });
            }
            let _busy = SessionBusy {
                busy: Arc::clone(&self.busy),
                session,
            };
            let config = entries
                .iter()
                .find_map(|(_, event)| match event {
                    SessionEvent::SessionCreated { config, .. } => Some(config.clone()),
                    _ => None,
                })
                .ok_or_else(|| AgentError::Corrupt {
                    session,
                    detail: "session log has no SessionCreated".to_owned(),
                })?;
            let (input, frontier) = self
                .compaction_input(session, &config, CompactionTrigger::Manual, u64::MAX)
                .await?;
            match self
                .perform_compaction(session, input, frontier, None)
                .await?
            {
                CompactionRun::Done(result) => Ok(result),
                CompactionRun::Failed(error) => Err(AgentError::Compaction(error)),
            }
        })
    }
}

/// Compaction machinery shared by all three triggers.
impl Agent {
    /// Build the compactor's planning input from the projection at `watermark`.
    /// `CompactionApplied` is folded regardless of the watermark.
    async fn compaction_input(
        &self,
        session: SessionId,
        config: &ConfigSnapshot,
        trigger: CompactionTrigger,
        watermark: u64,
    ) -> Result<(CompactionInput, LogId), AgentError> {
        let entries = self.scan(session).await?;
        let frontier = entries
            .last()
            .map(|entry| entry.log_id)
            .unwrap_or_else(|| LogId::from_i64(0));
        let mut visible = Vec::new();
        let mut started: Vec<(StepId, LogId)> = Vec::new();
        let mut committed: HashSet<StepId> = HashSet::new();
        for entry in &entries {
            let event = decode_entry(entry).map_err(|error| AgentError::Corrupt {
                session,
                detail: error.to_string(),
            })?;
            match &event {
                SessionEvent::StepStarted { step_id, .. } => {
                    started.push((*step_id, entry.log_id));
                }
                SessionEvent::StepCommitted { step_id, .. } => {
                    committed.insert(*step_id);
                }
                _ => {}
            }
            if matches!(event, SessionEvent::CompactionApplied { .. }) || entry.seq < watermark {
                visible.push(entry.clone());
            }
        }
        let history =
            asterpath_protocol::project(&visible).map_err(|error| AgentError::Corrupt {
                session,
                detail: error.to_string(),
            })?;
        let live: HashSet<LogId> = history
            .iter()
            .filter_map(|entry| match entry.origin {
                ProjectedOrigin::Log(id) => Some(id),
                ProjectedOrigin::Summary(_) => None,
            })
            .collect();
        let last_usage = entries
            .iter()
            .filter_map(|entry| match decode_entry(entry).ok()? {
                SessionEvent::AssistantMessage { usage, .. }
                    if entry.seq < watermark && live.contains(&entry.log_id) =>
                {
                    Some(UsageAnchor {
                        origin: entry.log_id,
                        usage,
                    })
                }
                _ => None,
            })
            .next_back();
        let step_starts = started
            .into_iter()
            .filter(|(step, _)| committed.contains(step))
            .map(|(_, log_id)| log_id)
            .collect();
        Ok((
            CompactionInput {
                trigger,
                model: config.model.clone(),
                config: config.compaction.clone(),
                turn: config.turn.clone(),
                history,
                step_starts,
                last_usage,
            },
            frontier,
        ))
    }

    /// Plan, record the intent, run the summary and write its terminal.
    ///
    /// `existing_key` replays an open `CompactionStarted` from recovery: the
    /// intent is already recorded, so only the terminal is appended.
    async fn perform_compaction(
        &self,
        session: SessionId,
        input: CompactionInput,
        frontier: LogId,
        existing_key: Option<DedupKey>,
    ) -> Result<CompactionRun, AgentError> {
        let Some(compactor) = &self.compactor else {
            return Ok(CompactionRun::Done(CompactResult::NotConfigured));
        };
        let job = match compactor.plan(&input) {
            CompactPlan::Summarize(job) => job,
            CompactPlan::NotNeeded => return Ok(CompactionRun::Done(CompactResult::NotNeeded)),
            CompactPlan::NothingToDo => {
                return Ok(CompactionRun::Done(CompactResult::NothingToDo));
            }
            _ => return Ok(CompactionRun::Done(CompactResult::NotConfigured)),
        };
        let recording_intent = existing_key.is_none();
        let key = existing_key.unwrap_or_else(|| dedup::compaction(frontier));
        if recording_intent {
            self.append(
                session,
                SessionEvent::CompactionStarted {
                    trigger: input.trigger,
                    replaces_from: job.replaces.first,
                    caused_by: frontier,
                },
                Some(key.clone()),
            )
            .await?;
        }
        match compactor.compact(job).await {
            Ok(CompactSummary::Applied(outcome)) => {
                self.append_applied(session, &outcome, key.clone()).await?;
                Ok(CompactionRun::Done(CompactResult::Applied(outcome)))
            }
            Ok(CompactSummary::Insufficient) => {
                self.append_aborted(session, CompactionAbortReason::Insufficient, key)
                    .await?;
                Ok(CompactionRun::Done(CompactResult::Insufficient))
            }
            Err(error) => {
                self.append_aborted(session, CompactionAbortReason::SummaryFailed, key)
                    .await?;
                Ok(CompactionRun::Failed(error))
            }
            Ok(_) => {
                // Forward-compatible summary results are treated as Insufficient.
                self.append_aborted(session, CompactionAbortReason::Insufficient, key)
                    .await?;
                Ok(CompactionRun::Done(CompactResult::Insufficient))
            }
        }
    }

    async fn append_applied(
        &self,
        session: SessionId,
        outcome: &CompactionOutcome,
        key: DedupKey,
    ) -> Result<(), AgentError> {
        self.append(
            session,
            SessionEvent::CompactionApplied {
                first_kept: outcome.first_kept,
                summary: outcome.summary.clone(),
                replaces: outcome.replaces,
                model: outcome.model.clone(),
                usage: outcome.usage,
                raw: outcome.raw.clone(),
            },
            Some(key),
        )
        .await
        .map(|_| ())
    }

    async fn append_aborted(
        &self,
        session: SessionId,
        reason: CompactionAbortReason,
        key: DedupKey,
    ) -> Result<(), AgentError> {
        self.append(
            session,
            SessionEvent::CompactionAborted { reason },
            Some(key),
        )
        .await
        .map(|_| ())
    }

    /// Redo an open `CompactionStarted`. Returns the turn end reason when the
    /// redo produced a terminal that must close the current turn.
    async fn resolve_open_compaction(
        &self,
        session: SessionId,
        decoded: &[(LogEntry, SessionEvent)],
        config: &ConfigSnapshot,
    ) -> Result<Option<TurnEndReason>, AgentError> {
        let mut terminal: HashSet<DedupKey> = HashSet::new();
        for (entry, event) in decoded {
            if matches!(
                event,
                SessionEvent::CompactionApplied { .. } | SessionEvent::CompactionAborted { .. }
            ) && let Some(key) = &entry.dedup_key
            {
                terminal.insert(key.clone());
            }
        }
        let open = decoded.iter().rev().find_map(|(entry, event)| {
            let SessionEvent::CompactionStarted { trigger, .. } = event else {
                return None;
            };
            let key = entry.dedup_key.clone()?;
            (!terminal.contains(&key)).then_some((key, *trigger))
        });
        let Some((key, trigger)) = open else {
            return Ok(None);
        };
        let (input, frontier) = self
            .compaction_input(session, config, trigger, u64::MAX)
            .await?;
        match self
            .perform_compaction(session, input, frontier, Some(key))
            .await?
        {
            CompactionRun::Done(CompactResult::Insufficient) => {
                Ok(Some(TurnEndReason::ContextExhausted))
            }
            CompactionRun::Failed(_) => Ok(Some(TurnEndReason::CompactionFailed)),
            _ => Ok(None),
        }
    }
}

struct CallItem {
    provider_call_id: String,
    name: ToolName,
    raw_args: ToolArgs,
}

struct RecordedIntent {
    tool_call_id: ToolCallId,
    log_id: LogId,
    name: ToolName,
    args: ToolArgs,
    replay: ReplaySafety,
    provenance: Provenance,
}

#[derive(Default)]
struct ExistingStep {
    intents: HashMap<String, RecordedIntent>,
    results: HashSet<ToolCallId>,
}

struct Runnable {
    tool: Arc<dyn Tool>,
    descriptor: ToolDescriptor,
    tool_call_id: ToolCallId,
    name: ToolName,
    args: ToolArgs,
}

enum PreFailure {
    Denied(String),
    Internal(String),
}

enum ToolDecision {
    Allow,
    Deny(String),
    /// Recovery: the intent is recorded but the tool is unsafe to rerun.
    Interrupt,
    Cancelled,
}

fn approval_decision(
    session: SessionId,
    payload: SignalPayload,
) -> Result<ToolDecision, AgentError> {
    match serde_json::from_value::<ApprovalDecision>(payload) {
        Ok(ApprovalDecision::Allow) => Ok(ToolDecision::Allow),
        Ok(ApprovalDecision::Deny { reason }) => Ok(ToolDecision::Deny(
            reason.unwrap_or_else(|| DENIED_WITHOUT_REASON.to_owned()),
        )),
        Ok(_) => Err(AgentError::Corrupt {
            session,
            detail: "unknown approval decision".to_owned(),
        }),
        Err(error) => Err(AgentError::Corrupt {
            session,
            detail: format!("invalid approval decision: {error}"),
        }),
    }
}

enum Invocation {
    Returned(Result<ToolOutput, ToolError>),
    Panic,
    /// The turn's cancel token was already set when the deadline expired.
    Cancelled,
    Timeout,
}

// The guard variants exist only to hold a lock until end of scope.
#[allow(dead_code)]
enum StepLockGuard<'a> {
    Read(tokio::sync::RwLockReadGuard<'a, ()>),
    Write(tokio::sync::RwLockWriteGuard<'a, ()>),
}

enum StepOutcome {
    /// `pause_turn`, tool calls or pending input: continue the same turn.
    Continue,
    Turn(TurnEndReason),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ToolOutcome {
    Done,
    Cancelled,
}

enum GateOutcome {
    Continue,
    Stop(String),
    Failed,
}

struct BusyGuard {
    busy: Arc<Mutex<HashSet<SessionId>>>,
    session: SessionId,
    /// Removes the session's active cancel token when the drive ends.
    _cancel: CancelRegistration,
}

/// Single-flight guard for the turn-free `compact_now` path.
struct SessionBusy {
    busy: Arc<Mutex<HashSet<SessionId>>>,
    session: SessionId,
}

impl Drop for SessionBusy {
    fn drop(&mut self) {
        self.busy
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.session);
    }
}

enum CompactionRun {
    Done(CompactResult),
    Failed(CompactionError),
}

impl Drop for BusyGuard {
    fn drop(&mut self) {
        self.busy
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.session);
    }
}

#[derive(Default)]
struct EventSink {
    queue: Mutex<VecDeque<Result<AgentEvent, AgentError>>>,
}

impl EventSink {
    fn push(&self, event: Result<AgentEvent, AgentError>) {
        self.queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(event);
    }

    fn pop(&self) -> Option<Result<AgentEvent, AgentError>> {
        self.queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front()
    }
}

/// Drives the turn and the stream together: every event is yielded as soon as
/// the loop produces it, without a background task.
struct DriveStream<'a> {
    future: BoxFuture<'a, ()>,
    sink: Arc<EventSink>,
    _guard: BusyGuard,
    done: bool,
}

impl Stream for DriveStream<'_> {
    type Item = Result<AgentEvent, AgentError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(event) = this.sink.pop() {
                return Poll::Ready(Some(event));
            }
            if this.done {
                return Poll::Ready(None);
            }
            match this.future.as_mut().poll(cx) {
                Poll::Ready(()) => this.done = true,
                // The loop already ran and queued events before parking; they
                // must be yielded now, not on some later wake.
                Poll::Pending => {
                    if let Some(event) = this.sink.pop() {
                        return Poll::Ready(Some(event));
                    }
                    return Poll::Pending;
                }
            }
        }
    }
}

/// Invoke one tool with pipeline-owned deadline and panic containment.
async fn invoke_tool(
    tool: Arc<dyn Tool>,
    args: ToolArgs,
    context: ToolContext,
    cancel: CancelToken,
    turn_cancel: CancelToken,
    timeout: Option<Duration>,
    kill_grace: Duration,
) -> Invocation {
    let future = AssertUnwindSafe(tool.invoke(args, context)).catch_unwind();
    tokio::pin!(future);

    let Some(limit) = timeout else {
        return match future.await {
            Ok(result) => Invocation::Returned(result),
            Err(_) => Invocation::Panic,
        };
    };

    let deadline = tokio::time::sleep(limit);
    tokio::pin!(deadline);
    let cancel_wait = turn_cancel.cancelled();
    tokio::pin!(cancel_wait);

    tokio::select! {
        biased;
        // Turn cancellation takes over the deadline: await the tool ourselves;
        // the supervisor's grace drops it if it never settles.
        _ = &mut cancel_wait => match future.as_mut().await {
            Ok(result) => Invocation::Returned(result),
            Err(_) => Invocation::Panic,
        },
        result = &mut future => match result {
            Ok(result) => Invocation::Returned(result),
            Err(_) => Invocation::Panic,
        },
        _ = &mut deadline => {
            let already_cancelled = cancel.is_cancelled();
            cancel.cancel();
            match tokio::time::timeout(kill_grace, future.as_mut()).await {
                Ok(Ok(result)) => Invocation::Returned(result),
                Ok(Err(_)) => Invocation::Panic,
                Err(_) if already_cancelled || turn_cancel.is_cancelled() => {
                    Invocation::Cancelled
                }
                Err(_) => Invocation::Timeout,
            }
        }
    }
}

/// Truncate model-visible content to `call_limit`, bounded by the step budget.
/// Text is cut at a UTF-8 boundary; non-text parts are kept whole or dropped.
fn truncate_output(
    parts: Vec<asterpath_protocol::Part>,
    call_limit: usize,
    budget: &Mutex<usize>,
) -> (Vec<asterpath_protocol::Part>, bool) {
    let mut remaining = budget.lock().unwrap_or_else(PoisonError::into_inner);
    let limit = call_limit.min(*remaining);
    let total: usize = parts.iter().map(part_bytes).sum();
    if total <= limit {
        *remaining = remaining.saturating_sub(total);
        return (parts, false);
    }

    let mut kept = Vec::new();
    let mut used = 0usize;
    let mut full = false;
    for part in parts {
        if full {
            break;
        }
        let size = part_bytes(&part);
        if used + size <= limit {
            used += size;
            kept.push(part);
            continue;
        }
        // A non-text part is never split; it is skipped so later parts that
        // still fit may be kept. A text part fills the remaining room and stops.
        if let asterpath_protocol::Part::Text { text } = part {
            let room = limit.saturating_sub(used);
            let cut = floor_char_boundary(&text, room);
            if cut > 0 {
                let kept_text = text[..cut].to_owned();
                used += kept_text.len();
                kept.push(asterpath_protocol::Part::Text { text: kept_text });
            }
            full = true;
        }
    }

    let omitted = total.saturating_sub(used);
    let suffix = format!("[output truncated: {omitted} bytes omitted]");
    used += suffix.len();
    kept.push(asterpath_protocol::Part::Text { text: suffix });
    *remaining = remaining.saturating_sub(used);
    (kept, true)
}

fn part_bytes(part: &asterpath_protocol::Part) -> usize {
    match part {
        asterpath_protocol::Part::Text { text } => text.len(),
        other => serde_json::to_vec(other)
            .map(|bytes| bytes.len())
            .unwrap_or(0),
    }
}

fn floor_char_boundary(text: &str, mut index: usize) -> usize {
    if index >= text.len() {
        return text.len();
    }
    while !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn tool_calls(items: &[Item]) -> Vec<CallItem> {
    items
        .iter()
        .filter_map(|item| match item {
            Item::ToolCall {
                call_id,
                name,
                raw_args,
                ..
            } => Some(CallItem {
                provider_call_id: call_id.clone(),
                name: name.clone(),
                raw_args: raw_args.clone(),
            }),
            _ => None,
        })
        .collect()
}

fn existing_step(decoded: &[(LogEntry, SessionEvent)], turn: TurnId, step: StepId) -> ExistingStep {
    let mut existing = ExistingStep::default();
    for (entry, event) in decoded {
        match event {
            SessionEvent::ToolCallRequested {
                turn_id,
                step_id,
                tool_call_id,
                provider_call_id,
                name,
                raw_args,
                replay,
                provenance,
            } if *turn_id == turn && *step_id == step => {
                existing.intents.insert(
                    provider_call_id.clone(),
                    RecordedIntent {
                        tool_call_id: *tool_call_id,
                        log_id: entry.log_id,
                        name: name.clone(),
                        args: raw_args.clone(),
                        replay: *replay,
                        provenance: *provenance,
                    },
                );
            }
            SessionEvent::ToolResult { tool_call_id, .. } => {
                existing.results.insert(*tool_call_id);
            }
            _ => {}
        }
    }
    existing
}

fn open_step(decoded: &[(LogEntry, SessionEvent)], turn: TurnId) -> Option<(StepId, u64)> {
    let mut open = None;
    for (entry, event) in decoded {
        match event {
            SessionEvent::StepStarted { turn_id, step_id } if *turn_id == turn => {
                open = Some((*step_id, entry.seq));
            }
            SessionEvent::StepCommitted { step_id, .. }
                if open.is_some_and(|(step, _)| step == *step_id) =>
            {
                open = None;
            }
            _ => {}
        }
    }
    open
}

fn has_assistant_message(decoded: &[(LogEntry, SessionEvent)], turn: TurnId, step: StepId) -> bool {
    decoded.iter().any(|(_, event)| {
        matches!(
            event,
            SessionEvent::AssistantMessage { turn_id, step_id, .. }
                if *turn_id == turn && *step_id == step
        )
    })
}

/// The last decided model attempt for a step, in `seq` order.
fn last_abort(
    decoded: &[(LogEntry, SessionEvent)],
    turn: TurnId,
    step: StepId,
) -> Option<(u32, AbortKind)> {
    decoded.iter().rev().find_map(|(_, event)| match event {
        SessionEvent::ModelCallAborted {
            turn_id,
            step_id,
            attempt,
            kind,
            ..
        } if *turn_id == turn && *step_id == step => Some((*attempt, *kind)),
        _ => None,
    })
}

/// A timer declared by a child spawn is closed by spawn replay (#47).
fn caused_by_is_child_spawn(decoded: &[(LogEntry, SessionEvent)], caused_by: LogId) -> bool {
    decoded.iter().any(|(entry, event)| {
        entry.log_id == caused_by && matches!(event, SessionEvent::ChildSessionSpawned { .. })
    })
}

/// Whether the record a wait points at already reached its own terminal.
fn caused_by_is_terminal(decoded: &[(LogEntry, SessionEvent)], caused_by: LogId) -> bool {
    let Some((entry, event)) = decoded.iter().find(|(entry, _)| entry.log_id == caused_by) else {
        return false;
    };
    match event {
        SessionEvent::ToolCallRequested { tool_call_id, .. } => decoded.iter().any(|(_, event)| {
            matches!(
                event,
                SessionEvent::ToolResult { tool_call_id: done, .. } if done == tool_call_id
            )
        }),
        SessionEvent::ChildSessionSpawned { child, .. } => decoded.iter().any(|(_, event)| {
            matches!(
                event,
                SessionEvent::ChildSessionCompleted { child: done, .. } if done == child
            )
        }),
        SessionEvent::SignalAwaited { signal_id, .. } => decoded.iter().any(|(_, event)| {
            matches!(
                event,
                SessionEvent::SignalReceived { signal_id: done, .. } if done == signal_id
            )
        }),
        SessionEvent::TimerScheduled { timer_id, .. } => decoded.iter().any(|(_, event)| {
            matches!(
                event,
                SessionEvent::TimerFired { timer_id: done }
                    | SessionEvent::TimerCancelled { timer_id: done }
                    if done == timer_id
            )
        }),
        SessionEvent::StepStarted { step_id, .. } => decoded.iter().any(|(_, event)| {
            matches!(
                event,
                SessionEvent::StepCommitted { step_id: done, .. } if done == step_id
            )
        }),
        SessionEvent::TurnStarted { turn_id } => decoded.iter().any(|(_, event)| {
            matches!(
                event,
                SessionEvent::TurnCommitted { turn_id: done, .. } if done == turn_id
            )
        }),
        SessionEvent::CompactionStarted { .. } => {
            let key = entry.dedup_key.as_ref();
            decoded.iter().any(|(entry, event)| {
                matches!(
                    event,
                    SessionEvent::CompactionApplied { .. } | SessionEvent::CompactionAborted { .. }
                ) && entry.dedup_key.as_ref() == key
            })
        }
        _ => false,
    }
}

fn step_outcome(stop_reason: &StopReason, had_calls: bool, pending_input: bool) -> StepOutcome {
    if matches!(stop_reason, StopReason::ContextWindowExceeded) {
        return StepOutcome::Turn(TurnEndReason::ContextExhausted);
    }
    if had_calls || matches!(stop_reason, StopReason::PauseTurn) || pending_input {
        StepOutcome::Continue
    } else {
        StepOutcome::Turn(TurnEndReason::Completed)
    }
}

fn decode_entries(
    entries: Vec<LogEntry>,
    session: SessionId,
) -> Result<Vec<(LogEntry, SessionEvent)>, AgentError> {
    entries
        .into_iter()
        .map(|entry| {
            let event = decode_entry(&entry).map_err(|error| AgentError::Corrupt {
                session,
                detail: error.to_string(),
            })?;
            Ok((entry, event))
        })
        .collect()
}

fn has_pending_input(decoded: &[(LogEntry, SessionEvent)]) -> bool {
    let mut pending = false;
    for (_, event) in decoded {
        match event {
            SessionEvent::InputRecorded { .. } => pending = true,
            SessionEvent::StepStarted { .. } => pending = false,
            _ => {}
        }
    }
    pending
}

/// Exponential backoff with jitter, capped at `max_backoff`.
fn backoff(config: &TurnConfig, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(10);
    let capped = config
        .initial_backoff
        .saturating_mul(1u32 << exponent)
        .min(config.max_backoff);
    let jitter_max = capped / 4;
    if jitter_max.is_zero() {
        return capped;
    }
    let jitter = Duration::from_nanos(random_u64() % (jitter_max.as_nanos() as u64 + 1));
    (capped + jitter).min(config.max_backoff)
}

fn random_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_nanos() as u64 ^ elapsed.as_secs())
        .unwrap_or_default()
}

/// Process-local unique id source; session scoping makes collisions moot.
fn fresh_id() -> [u8; 16] {
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
