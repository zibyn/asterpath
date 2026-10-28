//! Agent loop: multi-step turns, model calls, log-derived state and crash
//! recovery. A step's tool calls go to the `pipeline` module.
//!
//! One driver per session; model-attempt retries inside one step; the
//! `StepStarted.seq` projection waterline. Recovery reads only the committed
//! prefix and handles open intents; completed history is never replayed from
//! the session head.

// The public error enums are fixed by packaging.md §3.4–3.5.
#![allow(clippy::result_large_err)]

mod attempt;
pub(crate) mod output;

use attempt::{Attempts, Failure, Forced, Next};

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use asterpath_protocol::scan_all;
use asterpath_protocol::{
    AbortKind, AgentError, AgentEvent, ApprovalPolicy, CancelReason, CancelRegistration,
    CancelToken, CancellationRegistry, ChildOutcome, CompactPlan, CompactResult, CompactSummary,
    CompactionAbortReason, CompactionError, CompactionInput, CompactionOutcome, CompactionTrigger,
    Compactor, ConfigSnapshot, ForkSource, Gate, HookError, Hooks, Item, JsonString, ModelRequest,
    OpenStep, OpenTool, OutputFormat, ProjectedOrigin, Provenance, ProviderCatalog, ProviderError,
    ProviderErrorInfo, ProviderErrorKind, ResumeReason, RunRequest, SessionEvent, SessionFacts,
    SessionState, StepCtx, StopReason, StorageError, StreamEvent, ToolCallId, ToolConfig, ToolName,
    ToolResultStatus, TurnConfig, TurnEndReason, Usage, UsageAnchor, UsageError, UsageLookup,
    decode_entry, dedup, effective_config,
};
use asterpath_protocol::{
    ApprovalGate, Clock, DedupKey, LogEntry, LogId, LogQuery, OwnerToken, SessionContext,
    SessionEnvironment, SessionId, SessionStore, StepId, Timestamp, TurnId, root_of, unique_bytes,
};
use futures_core::Stream;

use crate::scan::scan_decoded;
use crate::scheduler::Scheduler;
use crate::tools::ToolTable;
use crate::views::usage;
use crate::waits::Waits;
use crate::watch::Watchers;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{FutureExt, StreamExt, stream};
use pipeline::{SpawnFn, ToolOutcome, ToolPipeline, ToolStep};

/// The turn driver.
pub(crate) struct Agent {
    log: Arc<dyn SessionStore>,
    providers: Arc<ProviderCatalog>,
    pipeline: ToolPipeline,
    clock: Arc<dyn Clock>,
    /// Application hooks, called in registration order (ADR-0025). The pipeline
    /// holds the same list for the tool hooks.
    hooks: Vec<Arc<dyn Hooks>>,
    /// Backs `StepCtx::usage`.
    usage: Option<UsageLookup>,
    compactor: Option<Arc<dyn Compactor>>,
    busy: Arc<Mutex<HashSet<SessionId>>>,
    cancellations: Arc<CancellationRegistry>,
    /// Runs `spawn_agent` calls; children are driven by this agent.
    scheduler: Option<Arc<Scheduler>>,
    /// Every drive's events, children's included, reach this process's
    /// watchers (ADR-0038).
    watchers: Arc<Watchers>,
}

impl Agent {
    /// The composition root supplies policy, gate and durable waits; this
    /// module never constructs them, so approval waits are real.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        log: Arc<dyn SessionStore>,
        providers: Arc<ProviderCatalog>,
        tools: Arc<ToolTable>,
        tool_config: ToolConfig,
        approval: Arc<dyn ApprovalPolicy>,
        gate: Arc<ApprovalGate>,
        waits: Arc<Waits>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            pipeline: ToolPipeline::new(
                Arc::clone(&log),
                tools,
                tool_config,
                approval,
                gate,
                waits,
                Arc::clone(&clock),
            ),
            log,
            providers,
            clock,
            hooks: Vec::new(),
            usage: None,
            compactor: None,
            busy: Arc::new(Mutex::new(HashSet::new())),
            cancellations: Arc::new(CancellationRegistry::new()),
            scheduler: None,
            watchers: Arc::default(),
        }
    }

    /// Inject the optional compaction capability. `None` means compaction is
    /// unavailable everywhere (`NotConfigured`).
    #[must_use]
    pub(crate) fn with_compactor(mut self, compactor: Option<Arc<dyn Compactor>>) -> Self {
        self.compactor = compactor;
        self
    }

    /// Register application hooks; `usage` backs their `usage()` lookups.
    #[must_use]
    pub(crate) fn with_hooks(
        mut self,
        hooks: Vec<Arc<dyn Hooks>>,
        usage: Option<UsageLookup>,
    ) -> Self {
        self.pipeline = self.pipeline.with_hooks(hooks.clone(), usage.clone());
        self.hooks = hooks;
        self.usage = usage;
        self
    }

    /// Share the facade's per-session cancellation registry. Without this the
    /// agent keeps a private one.
    #[must_use]
    pub(crate) fn with_cancellations(mut self, cancellations: Arc<CancellationRegistry>) -> Self {
        self.cancellations = cancellations;
        self
    }

    /// Run `spawn_agent` through this scheduler, driving children with this
    /// agent. Without it the spawn tool is an ordinary tool.
    #[must_use]
    pub(crate) fn with_scheduler(mut self, scheduler: Arc<Scheduler>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    /// Share the facade's watchers. Without this the agent keeps private ones.
    #[must_use]
    pub(crate) fn with_watchers(mut self, watchers: Arc<Watchers>) -> Self {
        self.watchers = watchers;
        self
    }

    /// Ids come from the injected clock plus a process counter, so a fixed
    /// `ManualClock` yields a deterministic prefix and unique ids.
    fn fresh_id(&self) -> [u8; 16] {
        unique_bytes(self.clock.now())
    }

    fn acquire(&self, session: SessionId) -> bool {
        self.busy
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(session)
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
        let mut decoded = scan_decoded(&*self.log, session).await?;
        // Only the immutable turn/compaction/subagent fields are read from
        // this; each step folds its own model and request (ADR-0030).
        let config = config_at(session, &decoded, u64::MAX)?;

        let mut facts =
            SessionFacts::fold_decoded(decoded.iter().map(|(entry, event)| (entry, event)));

        // R4: a child whose parent already stopped waiting is an orphan; close
        // its open intents as cancelled and never open another turn.
        if self.parent_recorded_completion(session, &facts).await? {
            return self.close_orphan(session, &decoded, &facts, events).await;
        }

        // A dangling compaction intent is redone only when a compactor is
        // assembled; otherwise it stays open and never blocks Idle.
        if self.compactor.is_some()
            && let Some(reason) = self.resolve_open_compaction(session, &facts).await?
        {
            decoded = scan_decoded(&*self.log, session).await?;
            facts = SessionFacts::fold_decoded(decoded.iter().map(|(entry, event)| (entry, event)));
            if let SessionState::TurnOpen { turn, step } = facts.state() {
                if step.is_some()
                    && let Some(open) = open_step(&facts, turn)
                {
                    self.commit_step(session, turn, open.step).await?;
                }
                return self.commit_turn(session, turn, reason, events).await;
            }
        }

        match facts.state() {
            SessionState::Idle => {
                if !facts.has_pending_input() {
                    events.push(Ok(AgentEvent::Idle { session }));
                    return Ok(());
                }
                let turn = TurnId::from_bytes(self.fresh_id());
                self.append(session, SessionEvent::TurnStarted { turn_id: turn }, None)
                    .await?;
                events.push(Ok(AgentEvent::TurnStarted { turn }));
                self.run_steps(session, &config, turn, None, cancel, events)
                    .await
            }
            SessionState::TurnOpen { turn, .. } => {
                self.recover(session, &facts, &config, turn, cancel, events)
                    .await
            }
        }
    }

    /// Resume from the committed prefix. Only open intents are handled; a
    /// completed turn is never replayed from the session head.
    async fn recover(
        &self,
        session: SessionId,
        facts: &SessionFacts,
        config: &ConfigSnapshot,
        turn: TurnId,
        cancel: &CancelToken,
        events: &EventSink,
    ) -> Result<(), AgentError> {
        self.append(
            session,
            SessionEvent::StreamResumed {
                from: last_log_id(facts),
                reason: ResumeReason::Crash,
            },
            None,
        )
        .await?;

        let current = scan_decoded(&*self.log, session).await?;
        let max_attempts = config.turn.max_attempts.get();
        let Some(OpenStep {
            step,
            seq: watermark,
            ..
        }) = open_step(
            &SessionFacts::fold_decoded(current.iter().map(|(entry, event)| (entry, event))),
            turn,
        )
        else {
            // Between steps: the last committed step decides whether the turn
            // goes on, exactly as it did live.
            if let Some(reason) = committed_step_end(&current, turn, max_attempts, facts) {
                return self.commit_turn(session, turn, reason, events).await;
            }
            return self
                .run_steps(session, config, turn, None, cancel, events)
                .await;
        };

        if assistant_message(&current, turn, step).is_some() {
            // The model call is committed: never call it again.
            match self
                .resume_step(session, watermark, turn, step, cancel, events)
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

        // The attempt policy replays the step's history. D3: a recorded
        // backoff is not waited out; D4: an attempt in flight after a
        // recorded abort is redone under the same number.
        let (history, next) =
            match attempt::replay(max_attempts, step_records(&current, turn, step)) {
                Some(decided) => decided,
                None => {
                    // No attempt was ever decided: record the crash first.
                    self.append(
                        session,
                        SessionEvent::ModelCallAborted {
                            turn_id: turn,
                            step_id: step,
                            attempt: 1,
                            kind: AbortKind::Crash,
                            partial: None,
                            model: config_at(session, &current, watermark)?.model,
                            backoff_ms: 0,
                            usage: None,
                            error: None,
                        },
                        None,
                    )
                    .await?;
                    let history = Attempts::FIRST;
                    (
                        history,
                        attempt::after_failure(max_attempts, history, Failure::Crash),
                    )
                }
            };
        self.run_steps(
            session,
            config,
            turn,
            Some(OpenAttempt {
                step,
                watermark,
                history,
                next,
            }),
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
        facts: &SessionFacts,
    ) -> Result<bool, AgentError> {
        let Some(parent) = facts.parent().map(|parent| parent.session) else {
            return Ok(false);
        };
        let parent_decoded = scan_decoded(&*self.log, parent).await?;
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
        facts: &SessionFacts,
        events: &EventSink,
    ) -> Result<(), AgentError> {
        let SessionState::TurnOpen { turn, .. } = facts.state() else {
            events.push(Ok(AgentEvent::Idle { session }));
            return Ok(());
        };
        self.append(
            session,
            SessionEvent::StreamResumed {
                from: last_log_id(facts),
                reason: ResumeReason::Crash,
            },
            None,
        )
        .await?;

        for (tool_call_id, tool) in facts.open_tools() {
            // Never run in this process: announced, then closed, like any
            // call interrupted before it started.
            let (output_index, args) = recorded_call(decoded, tool, tool_call_id);
            events.push(Ok(AgentEvent::ToolStarted {
                turn: tool.turn,
                step: tool.step,
                output_index,
                call: tool_call_id,
                tool: tool.name.clone(),
                args,
            }));
            self.append(
                session,
                SessionEvent::ToolResult {
                    tool_call_id,
                    status: ToolResultStatus::Interrupted,
                    output: None,
                    error: None,
                    truncated: false,
                },
                Some(dedup::tool_result(tool_call_id)),
            )
            .await?;
            events.push(Ok(AgentEvent::ToolFinished {
                turn: tool.turn,
                step: tool.step,
                call: tool_call_id,
                status: ToolResultStatus::Interrupted,
                output: None,
                error: None,
            }));
        }
        for (child, _) in facts.open_children() {
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
        if let Some(open) = open_step(facts, turn) {
            self.commit_step(session, turn, open.step).await?;
        }
        self.commit_turn(session, turn, TurnEndReason::Cancelled, events)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_steps(
        &self,
        session: SessionId,
        config: &ConfigSnapshot,
        turn: TurnId,
        open: Option<OpenAttempt>,
        cancel: &CancelToken,
        events: &EventSink,
    ) -> Result<(), AgentError> {
        let mut pending = open;
        let mut steps_started = 0u32;
        // Steps of this turn already in the log; only read when hooks need it.
        let prior_steps = if self.hooks.is_empty() {
            0
        } else {
            self.steps_in_turn(session, turn).await?
        };
        let mut auto_compactions = 0u32;

        loop {
            let open = match pending.take() {
                Some(open) => open,
                None => {
                    // Between steps: cancellation closes the turn with no step.
                    if cancel.is_cancelled() {
                        return self
                            .commit_turn(session, turn, TurnEndReason::Cancelled, events)
                            .await;
                    }
                    // Automatic threshold compaction (#46), before the application gate.
                    if self.compactor.is_some()
                        && auto_compactions < config.compaction.max_auto_compactions_per_turn
                    {
                        let (input, frontier) = self
                            .compaction_input(session, CompactionTrigger::Threshold, u64::MAX)
                            .await?;
                        match self
                            .perform_compaction(session, input, frontier, Some(turn), None)
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
                    match self
                        .before_step(session, turn, prior_steps + steps_started)
                        .await
                    {
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
                    let step = StepId::from_bytes(self.fresh_id());
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
                    OpenAttempt {
                        step,
                        watermark: seq,
                        history: Attempts::FIRST,
                        next: Next::Retry {
                            attempt: 1,
                            backoff: false,
                        },
                    }
                }
            };

            match self.run_step(session, turn, open, cancel, events).await? {
                StepOutcome::Continue => {}
                StepOutcome::Turn(reason) => {
                    return self.commit_turn(session, turn, reason, events).await;
                }
            }
        }
    }

    /// Steps of `turn` already recorded.
    async fn steps_in_turn(&self, session: SessionId, turn: TurnId) -> Result<u32, AgentError> {
        let mut count = 0;
        for entry in scan_all(&*self.log, session).await? {
            let event = decode_entry(&entry).map_err(|error| AgentError::Corrupt {
                session,
                detail: error.to_string(),
            })?;
            if matches!(event, SessionEvent::StepStarted { turn_id, .. } if turn_id == turn) {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Application `before_step` hooks in order; the first `EndTurn` wins and an
    /// error or panic fails the turn.
    async fn before_step(&self, session: SessionId, turn: TurnId, step_index: u32) -> GateOutcome {
        if self.hooks.is_empty() {
            return GateOutcome::Continue;
        }
        let ctx = StepCtx::new(session, turn, step_index, self.usage.clone());
        for hook in &self.hooks {
            match caught(|| hook.before_step(&ctx)).await {
                Ok(Gate::Continue) => {}
                Ok(Gate::EndTurn { reason }) => return GateOutcome::Stop(reason),
                // Forward-compatible decisions do not stop the turn.
                Ok(_) => {}
                Err(_) => return GateOutcome::Failed,
            }
        }
        GateOutcome::Continue
    }

    /// Drive one step's model attempts. `open.next` is where the attempt
    /// policy left the step: the first call, a retry or the forced compaction.
    async fn run_step(
        &self,
        session: SessionId,
        turn: TurnId,
        open: OpenAttempt,
        cancel: &CancelToken,
        events: &EventSink,
    ) -> Result<StepOutcome, AgentError> {
        let OpenAttempt {
            step,
            watermark,
            mut history,
            mut next,
        } = open;
        // The step's effective config: changes recorded after its
        // `StepStarted` wait for the next step (ADR-0030).
        let config = &config_at(
            session,
            &scan_decoded(&*self.log, session).await?,
            watermark,
        )?;
        // The projection is computed once per step; retries reuse it (L12).
        // A forced compaction re-projects the same step.
        let (mut request, mut output) = self.build_request(session, config, watermark).await?;
        let max_attempts = config.turn.max_attempts.get();

        loop {
            let attempt = match next {
                Next::End(reason) => {
                    self.commit_step(session, turn, step).await?;
                    return Ok(StepOutcome::Turn(reason));
                }
                Next::ForceCompact => {
                    history.compacted = true;
                    let forced = self.force_compact(session, turn, watermark).await?;
                    if forced == Forced::Applied {
                        (request, output) = self.build_request(session, config, watermark).await?;
                    }
                    next = attempt::after_compaction(history, forced);
                    continue;
                }
                Next::Retry { attempt, .. } => attempt,
            };
            history.attempt = attempt;

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
                        Some(Ok(StreamEvent::PartDelta {
                            output_index, part_index, delta, ..
                        })) => {
                            events.push(Ok(AgentEvent::ModelDelta {
                                turn, step, output_index, part_index, delta,
                            }));
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
                let indexed: Vec<(u32, Item)> = completed.into_iter().collect();
                let items: Vec<Item> = indexed.iter().map(|(_, item)| item.clone()).collect();
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

                let calls = StepCalls {
                    recovering: false,
                    output: output.as_ref(),
                };
                return self
                    .finish_step(
                        session,
                        config,
                        turn,
                        step,
                        &indexed,
                        &stop_reason,
                        calls,
                        cancel,
                        events,
                    )
                    .await;
            }

            // A stream that ends without a terminal event violates S2/S8.
            let error = failure.unwrap_or_else(|| {
                ProviderError::new(
                    ProviderErrorKind::StreamProtocol,
                    "model stream ended without ResponseEnded",
                )
            });
            let failure = if error.context_exceeded() {
                Failure::ContextExceeded
            } else if error.retryable {
                Failure::Retryable
            } else {
                Failure::Rejected
            };
            next = attempt::after_failure(max_attempts, history, failure);
            let backoff = match next {
                Next::Retry { backoff: true, .. } => {
                    backoff(&config.turn, attempt, self.clock.now())
                }
                _ => Duration::ZERO,
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
                    error: Some(ProviderErrorInfo::from(&error)),
                },
                None,
            )
            .await?;

            match &next {
                Next::End(_) if cancel.is_cancelled() => {
                    next = Next::End(TurnEndReason::Cancelled);
                }
                Next::Retry { attempt, .. } if !backoff.is_zero() => {
                    let token = cancel.clone();
                    let cancel_wait = token.cancelled();
                    tokio::pin!(cancel_wait);
                    tokio::select! {
                        biased;
                        _ = &mut cancel_wait => {
                            // Cancellation interrupts the backoff sleep and is
                            // the last abort the recovery classifier sees.
                            self.abort_cancelled(session, config, turn, step, *attempt, None)
                                .await?;
                            self.commit_step(session, turn, step).await?;
                            return Ok(StepOutcome::Turn(TurnEndReason::Cancelled));
                        }
                        _ = tokio::time::sleep(backoff) => {}
                    }
                }
                _ => {}
            }
        }
    }

    /// The step's one forced compaction (C5), at the step's watermark.
    async fn force_compact(
        &self,
        session: SessionId,
        turn: TurnId,
        watermark: u64,
    ) -> Result<Forced, AgentError> {
        let (input, frontier) = self
            .compaction_input(session, CompactionTrigger::ProviderError, watermark)
            .await?;
        Ok(
            match self
                .perform_compaction(session, input, frontier, Some(turn), None)
                .await?
            {
                CompactionRun::Done(CompactResult::Applied(_)) => Forced::Applied,
                CompactionRun::Done(_) => Forced::Exhausted,
                CompactionRun::Failed(_) => Forced::Failed,
            },
        )
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
                error: None,
            },
            None,
        )
        .await
        .map(|_| ())
    }

    /// Finish an open step whose `AssistantMessage` is already committed: run
    /// every call without a `ToolResult`, reusing recorded intents verbatim.
    /// The calls run under the config at the step's `watermark`, as they did live.
    async fn resume_step(
        &self,
        session: SessionId,
        watermark: u64,
        turn: TurnId,
        step: StepId,
        cancel: &CancelToken,
        events: &EventSink,
    ) -> Result<StepOutcome, AgentError> {
        let decoded = scan_decoded(&*self.log, session).await?;
        let config = &config_at(session, &decoded, watermark)?;
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

        // shortcut: the log keeps no output_index, so a resumed step reports
        // item positions; this step's live deltas went to an earlier process.
        // Upgrade by recording output_index on AssistantMessage items if applications
        // need to correlate resumed ToolStarted with pre-crash deltas.
        let indexed: Vec<(u32, Item)> = (0..).zip(items.iter().cloned()).collect();
        // Recorded output intents are known by provenance; this only names
        // the output tool for calls whose intent is not recorded.
        let output = self
            .output_tool(config)
            .map(|format| ToolName::new(&format.name));
        let calls = StepCalls {
            recovering: true,
            output: output.as_ref(),
        };
        self.finish_step(
            session,
            config,
            turn,
            step,
            &indexed,
            &stop_reason,
            calls,
            cancel,
            events,
        )
        .await
    }

    /// Finish a step whose `AssistantMessage` is committed, live or resumed:
    /// run its tool calls, commit it, and decide whether the turn goes on.
    #[allow(clippy::too_many_arguments)]
    async fn finish_step(
        &self,
        session: SessionId,
        config: &ConfigSnapshot,
        turn: TurnId,
        step: StepId,
        items: &[(u32, Item)],
        stop_reason: &StopReason,
        calls: StepCalls<'_>,
        cancel: &CancelToken,
        events: &EventSink,
    ) -> Result<StepOutcome, AgentError> {
        // A refused step's calls never run (I9).
        let had_calls = *stop_reason != StopReason::Refusal
            && items
                .iter()
                .any(|(_, item)| matches!(item, Item::ToolCall { .. }));
        let mut delivered = false;
        if had_calls {
            let outcome = self
                .run_tools(session, config, turn, step, items, calls, cancel, events)
                .await?;
            if outcome == ToolOutcome::Cancelled {
                self.commit_step(session, turn, step).await?;
                return Ok(StepOutcome::Turn(TurnEndReason::Cancelled));
            }
            delivered = outcome == ToolOutcome::Delivered;
        }

        self.commit_step(session, turn, step).await?;
        if cancel.is_cancelled() {
            return Ok(StepOutcome::Turn(TurnEndReason::Cancelled));
        }
        if delivered {
            return Ok(StepOutcome::Turn(TurnEndReason::Completed));
        }
        let pending_input = self.pending_input(session).await?;
        Ok(step_outcome(stop_reason, had_calls, pending_input))
    }

    /// Hand this step's tool calls to the pipeline; `spawn_agent` calls are
    /// routed to the scheduler, which drives children with this agent.
    #[allow(clippy::too_many_arguments)]
    async fn run_tools(
        &self,
        session: SessionId,
        config: &ConfigSnapshot,
        turn: TurnId,
        step: StepId,
        items: &[(u32, Item)],
        calls: StepCalls<'_>,
        cancel: &CancelToken,
        events: &EventSink,
    ) -> Result<ToolOutcome, AgentError> {
        let spawn;
        let spawn: Option<&SpawnFn<'_>> = match &self.scheduler {
            Some(scheduler) => {
                spawn = move |args, ctx| scheduler.run_spawn_tool(self, args, ctx);
                Some(&spawn)
            }
            None => None,
        };
        let step = ToolStep {
            session,
            turn,
            step,
            recovering: calls.recovering,
            cancel_grace: config.turn.cancel_grace,
            tools: &config.tools,
            output: calls.output,
            events,
            spawn,
        };
        self.pipeline.run(&step, items, cancel).await
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
        let decoded = scan_decoded(&*self.log, session).await?;
        Ok(
            SessionFacts::fold_decoded(decoded.iter().map(|(entry, event)| (entry, event)))
                .has_pending_input(),
        )
    }

    /// The format `config` delivers through the output tool for this model,
    /// or `None` for the native constraint (ADR-0034).
    fn output_tool<'a>(&self, config: &'a ConfigSnapshot) -> Option<&'a OutputFormat> {
        let provider = self.providers.get(&config.model.provider)?;
        output::tool(&config.request, &provider.capabilities(&config.model))
    }

    /// Project the log at the `StepStarted.seq` waterline. `CompactionApplied`
    /// is a rewrite operator and is folded regardless of the waterline. Also
    /// returns the output tool the request carries.
    async fn build_request(
        &self,
        session: SessionId,
        config: &ConfigSnapshot,
        watermark: u64,
    ) -> Result<(ModelRequest, Option<ToolName>), AgentError> {
        let entries = scan_all(&*self.log, session).await?;
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
        // The baseline system, not `config.system`: appended instructions sit
        // in `items` at their place until a compaction rebases them (ADR-0030).
        let history =
            asterpath_protocol::request_history(&visible).map_err(|error| AgentError::Corrupt {
                session,
                detail: error.to_string(),
            })?;
        let mut system = history.system;
        system
            .extend(asterpath_protocol::SystemSection::flatten(&history.system_sections).cloned());
        let mut request = ModelRequest::new(
            config.model.clone(),
            prefix_epoch,
            system,
            history.items,
            config.tools.clone(),
            history.tools,
            &config.request,
        );
        request.session = Some(self.session_context(session).await?);
        let format = self.output_tool(config);
        if let Some(format) = format {
            output::apply(&mut request, format);
        }
        Ok((request, format.map(|format| ToolName::new(&format.name))))
    }

    /// Who a model request of `session` is for (ADR-0039): its tree's root
    /// and the root's metadata.
    async fn session_context(&self, session: SessionId) -> Result<SessionContext, AgentError> {
        let root = root_of(&*self.log, session).await?;
        let (metadata, mut source) = self.root_head(root).await?;
        let mut cache_root = root;
        // A fork chain is bounded by the forks the application made; the bound
        // only stops a corrupt cycle.
        for _ in 0..64 {
            let Some(from) = source else { break };
            cache_root = root_of(&*self.log, from).await?;
            source = self.root_head(cache_root).await?.1;
        }
        Ok(SessionContext {
            session,
            root,
            cache_root,
            metadata,
        })
    }

    /// A root's metadata and, for a forked root, the session it was forked
    /// from: `SessionCreated` is every session's first record and
    /// `ContextForked` a fork's second.
    async fn root_head(
        &self,
        root: SessionId,
    ) -> Result<(BTreeMap<String, String>, Option<SessionId>), AgentError> {
        let head: Vec<_> = self
            .log
            .scan(LogQuery {
                session: root,
                after: None,
                limit: Some(2),
            })
            .collect()
            .await;
        let mut metadata = BTreeMap::new();
        let mut source = None;
        for entry in head {
            match decode_entry(&entry?) {
                Ok(SessionEvent::SessionCreated { metadata: m, .. }) => metadata = m,
                Ok(SessionEvent::ContextForked {
                    source: ForkSource::Session { session },
                    ..
                }) => source = Some(session),
                _ => {}
            }
        }
        Ok((metadata, source))
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
        let usage = self.turn_usage(session, turn).await?;
        events.push(Ok(AgentEvent::TurnCommitted {
            turn,
            reason,
            usage,
        }));
        Ok(())
    }

    /// Model calls plus compactions of one turn, folded from its own records.
    async fn turn_usage(&self, session: SessionId, turn: TurnId) -> Result<Usage, AgentError> {
        let entries = scan_all(&*self.log, session).await?;
        let start = entries
            .iter()
            .rposition(|entry| {
                matches!(decode_entry(entry), Ok(SessionEvent::TurnStarted { turn_id }) if turn_id == turn)
            })
            .unwrap_or(0);
        usage::turn_usage(session, turn, &entries[start..]).map_err(|error| match error {
            UsageError::NotFound { session } => AgentError::NotFound { session },
            UsageError::Corrupt { session, detail } => AgentError::Corrupt { session, detail },
            other => AgentError::Storage(StorageError::Backend(other.to_string())),
        })
    }

    async fn seq_of(&self, session: SessionId, log_id: LogId) -> Result<u64, AgentError> {
        scan_all(&*self.log, session)
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

impl Agent {
    pub(crate) fn run(&self, req: RunRequest) -> BoxStream<'_, Result<AgentEvent, AgentError>> {
        self.run_as(req, None, None)
    }

    /// Drive under `owner`, a held claim of the session's tree; every append
    /// the drive makes carries it (ADR-0036). `None` keeps the claim of the
    /// drive this one is polled in: a child drive inside its parent's. The
    /// same holds for `env`, the session environment (ADR-0037).
    pub(crate) fn run_as(
        &self,
        req: RunRequest,
        owner: Option<OwnerToken>,
        env: Option<Arc<SessionEnvironment>>,
    ) -> BoxStream<'_, Result<AgentEvent, AgentError>> {
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
            owner,
            env,
            watchers: Arc::clone(&self.watchers),
            done: false,
        })
    }

    pub(crate) fn compact_now(
        &self,
        session: SessionId,
    ) -> BoxFuture<'_, Result<CompactResult, AgentError>> {
        Box::pin(async move {
            let entries = scan_decoded(&*self.log, session).await?;
            if !SessionFacts::fold_decoded(entries.iter().map(|(entry, event)| (entry, event)))
                .is_idle()
            {
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
            let (input, frontier) = self
                .compaction_input(session, CompactionTrigger::Manual, u64::MAX)
                .await?;
            match self
                .perform_compaction(session, input, frontier, None, None)
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
        trigger: CompactionTrigger,
        watermark: u64,
    ) -> Result<(CompactionInput, LogId), AgentError> {
        let decoded = scan_decoded(&*self.log, session).await?;
        // The effective model when the compaction runs, at the same watermark.
        let config = config_at(session, &decoded, watermark)?;
        let entries: Vec<LogEntry> = decoded.into_iter().map(|(entry, _)| entry).collect();
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
                model: config.model,
                config: config.compaction,
                turn: config.turn,
                history,
                step_starts,
                last_usage,
            },
            frontier,
        ))
    }

    /// Plan, record the intent, run the summary and write its terminal.
    ///
    /// `turn` is the turn the compaction runs in (`None` when Idle).
    /// `existing_key` replays an open `CompactionStarted` from recovery: the
    /// intent is already recorded, so only the terminal is appended.
    async fn perform_compaction(
        &self,
        session: SessionId,
        input: CompactionInput,
        frontier: LogId,
        turn: Option<TurnId>,
        existing_key: Option<DedupKey>,
    ) -> Result<CompactionRun, AgentError> {
        let Some(compactor) = &self.compactor else {
            return Ok(CompactionRun::Done(CompactResult::NotConfigured));
        };
        let mut job = match compactor.plan(&input) {
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
                    turn,
                },
                Some(key.clone()),
            )
            .await?;
        }
        job.session = Some(self.session_context(session).await?);
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
        facts: &SessionFacts,
    ) -> Result<Option<TurnEndReason>, AgentError> {
        let Some(open) = facts.open_compactions().max_by_key(|open| open.seq) else {
            return Ok(None);
        };
        let (key, trigger) = (open.key.clone(), open.trigger);
        // D5: a forced (C5) compaction is redone at the open step's watermark,
        // as live; threshold and manual compactions see the whole log.
        let watermark = match (trigger, facts.open_step()) {
            (CompactionTrigger::ProviderError, Some(step)) => step.seq,
            _ => u64::MAX,
        };
        let (input, frontier) = self.compaction_input(session, trigger, watermark).await?;
        match self
            .perform_compaction(session, input, frontier, None, Some(key))
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

enum StepOutcome {
    /// `pause_turn`, tool calls or pending input: continue the same turn.
    Continue,
    Turn(TurnEndReason),
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
    owner: Option<OwnerToken>,
    env: Option<Arc<SessionEnvironment>>,
    watchers: Arc<Watchers>,
    done: bool,
}

impl DriveStream<'_> {
    /// The next queued event, shown to this process's watchers too.
    fn pop(&self) -> Option<Result<AgentEvent, AgentError>> {
        let event = self.sink.pop()?;
        if let Ok(event) = &event {
            self.watchers.publish(self._guard.session, event);
        }
        Some(event)
    }
}

impl Stream for DriveStream<'_> {
    type Item = Result<AgentEvent, AgentError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(event) = this.pop() {
                return Poll::Ready(Some(event));
            }
            if this.done {
                return Poll::Ready(None);
            }
            let future = &mut this.future;
            let polled = crate::owner::within(this.owner, || {
                crate::environment::within(this.env.as_ref(), || future.as_mut().poll(cx))
            });
            match polled {
                Poll::Ready(()) => this.done = true,
                // The loop already ran and queued events before parking; they
                // must be yielded now, not on some later wake.
                Poll::Pending => {
                    if let Some(event) = this.pop() {
                        return Poll::Ready(Some(event));
                    }
                    return Poll::Pending;
                }
            }
        }
    }
}

/// Run one hook call; a panic, while building or polling its future, counts
/// as that hook's error.
async fn caught<'a, T>(
    call: impl FnOnce() -> BoxFuture<'a, Result<T, HookError>>,
) -> Result<T, HookError> {
    let panicked = || HookError::new("hook panicked");
    match std::panic::catch_unwind(AssertUnwindSafe(call)) {
        Ok(hook) => AssertUnwindSafe(hook)
            .catch_unwind()
            .await
            .unwrap_or_else(|_| Err(panicked())),
        Err(_) => Err(panicked()),
    }
}

/// The effective config at `watermark`, via `effective_config`.
fn config_at(
    session: SessionId,
    decoded: &[(LogEntry, SessionEvent)],
    watermark: u64,
) -> Result<ConfigSnapshot, AgentError> {
    effective_config(decoded, watermark).ok_or_else(|| AgentError::Corrupt {
        session,
        detail: "session log has no SessionCreated".to_owned(),
    })
}

/// The open step of `turn`, with its `StepStarted.seq` waterline.
fn open_step(facts: &SessionFacts, turn: TurnId) -> Option<OpenStep> {
    facts.open_step().filter(|open| open.turn == turn)
}

/// `StreamResumed.from`: the last record before recovery appends.
fn last_log_id(facts: &SessionFacts) -> LogId {
    facts
        .last()
        .map(|(_, log_id)| log_id)
        .unwrap_or_else(|| LogId::from_i64(0))
}

/// How the pipeline treats one step's calls: as a resumed step's, and with
/// the output tool the step's request carried.
#[derive(Clone, Copy)]
struct StepCalls<'a> {
    recovering: bool,
    output: Option<&'a ToolName>,
}

/// Where the attempt policy left a step: its watermark, attempt history and
/// the next action.
struct OpenAttempt {
    step: StepId,
    watermark: u64,
    history: Attempts,
    next: Next,
}

/// A recorded call's `ToolStarted` fields: its position in the step's
/// `AssistantMessage` (as a resumed step reports it; 0 when none was
/// committed) and its recorded arguments.
fn recorded_call(
    decoded: &[(LogEntry, SessionEvent)],
    tool: &OpenTool,
    tool_call_id: ToolCallId,
) -> (u32, JsonString) {
    let Some((provider_call_id, args)) = decoded.iter().find_map(|(_, event)| match event {
        SessionEvent::ToolCallRequested {
            tool_call_id: id,
            provider_call_id,
            raw_args,
            ..
        } if *id == tool_call_id => Some((provider_call_id, raw_args.clone())),
        _ => None,
    }) else {
        return (0, JsonString::new("{}"));
    };
    let output_index = assistant_message(decoded, tool.turn, tool.step)
        .and_then(|(items, _)| {
            items.iter().position(
                |item| matches!(item, Item::ToolCall { call_id, .. } if call_id == provider_call_id),
            )
        })
        .map_or(0, |index| index as u32);
    (output_index, args)
}

/// The step's committed `AssistantMessage`: its items and stop reason.
fn assistant_message(
    decoded: &[(LogEntry, SessionEvent)],
    turn: TurnId,
    step: StepId,
) -> Option<(&[Item], &StopReason)> {
    decoded.iter().find_map(|(_, event)| match event {
        SessionEvent::AssistantMessage {
            turn_id,
            step_id,
            items,
            stop_reason,
            ..
        } if *turn_id == turn && *step_id == step => Some((items.as_slice(), stop_reason)),
        _ => None,
    })
}

/// The step's attempt history as the attempt policy reads it: its aborts and
/// its forced compaction, in `seq` order. Only a forced compaction runs
/// between a step's `StepStarted` and `StepCommitted`.
fn step_records(
    decoded: &[(LogEntry, SessionEvent)],
    turn: TurnId,
    step: StepId,
) -> Vec<attempt::Record> {
    let mut records = Vec::new();
    let mut in_step = false;
    let mut forced_open = false;
    for (_, event) in decoded {
        match event {
            SessionEvent::StepStarted { step_id, .. } if *step_id == step => in_step = true,
            SessionEvent::StepCommitted { step_id, .. } if *step_id == step => break,
            SessionEvent::ModelCallAborted {
                turn_id,
                step_id,
                attempt,
                kind,
                ..
            } if *turn_id == turn && *step_id == step => {
                records.push(attempt::Record::Aborted {
                    attempt: *attempt,
                    kind: *kind,
                });
            }
            SessionEvent::CompactionStarted {
                trigger: CompactionTrigger::ProviderError,
                ..
            } if in_step => {
                forced_open = true;
                records.push(attempt::Record::ForcedStarted);
            }
            SessionEvent::CompactionApplied { .. } if forced_open => {
                forced_open = false;
                records.push(attempt::Record::ForcedApplied);
            }
            SessionEvent::CompactionAborted { reason } if forced_open => {
                forced_open = false;
                records.push(attempt::Record::ForcedAborted(*reason));
            }
            _ => {}
        }
    }
    records
}

/// Between steps of an open turn: the end reason the turn's last committed
/// step decided, or `None` when the turn goes on.
fn committed_step_end(
    decoded: &[(LogEntry, SessionEvent)],
    turn: TurnId,
    max_attempts: u32,
    facts: &SessionFacts,
) -> Option<TurnEndReason> {
    let step = decoded.iter().rev().find_map(|(_, event)| match event {
        SessionEvent::StepStarted { turn_id, step_id } if *turn_id == turn => Some(*step_id),
        _ => None,
    })?;
    if let Some((items, stop_reason)) = assistant_message(decoded, turn, step) {
        if delivered(decoded, turn, step) {
            return Some(TurnEndReason::Completed);
        }
        let had_calls = has_tool_calls(items);
        return match step_outcome(stop_reason, had_calls, facts.has_pending_input()) {
            StepOutcome::Continue => None,
            StepOutcome::Turn(reason) => Some(reason),
        };
    }
    match attempt::replay(max_attempts, step_records(decoded, turn, step)) {
        Some((_, Next::End(reason))) => Some(reason),
        _ => None,
    }
}

/// The step called the output tool: its turn is over (ADR-0034).
fn delivered(decoded: &[(LogEntry, SessionEvent)], turn: TurnId, step: StepId) -> bool {
    decoded.iter().any(|(_, event)| {
        matches!(
            event,
            SessionEvent::ToolCallRequested {
                turn_id,
                step_id,
                provenance: Provenance::Output,
                ..
            } if *turn_id == turn && *step_id == step
        )
    })
}

fn has_tool_calls(items: &[Item]) -> bool {
    items
        .iter()
        .any(|item| matches!(item, Item::ToolCall { .. }))
}

fn step_outcome(stop_reason: &StopReason, had_calls: bool, pending_input: bool) -> StepOutcome {
    match stop_reason {
        StopReason::ContextWindowExceeded => {
            return StepOutcome::Turn(TurnEndReason::ContextExhausted);
        }
        StopReason::Refusal => return StepOutcome::Turn(TurnEndReason::Refused),
        _ => {}
    }
    if had_calls || matches!(stop_reason, StopReason::PauseTurn) || pending_input {
        StepOutcome::Continue
    } else {
        StepOutcome::Turn(TurnEndReason::Completed)
    }
}

/// Exponential backoff with jitter, capped at `max_backoff`. The jitter is
/// derived from the injected clock and the attempt, never wall time.
fn backoff(config: &TurnConfig, attempt: u32, now: Timestamp) -> Duration {
    let exponent = attempt.saturating_sub(1).min(10);
    let capped = config
        .initial_backoff
        .saturating_mul(1u32 << exponent)
        .min(config.max_backoff);
    let jitter_max = capped / 4;
    if jitter_max.is_zero() {
        return capped;
    }
    let seed = now as u64 ^ u64::from(attempt).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let jitter = Duration::from_nanos(seed % (jitter_max.as_nanos() as u64 + 1));
    (capped + jitter).min(config.max_backoff)
}

mod pipeline;

#[cfg(test)]
mod tests;
