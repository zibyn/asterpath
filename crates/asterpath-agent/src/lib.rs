//! Agent loop: multi-step turns, the fixed tool pipeline and log-derived state.
//!
//! One driver per session; model-attempt retries inside one step; the
//! `StepStarted.seq` projection waterline; parallel/exclusive tool scheduling
//! scoped to a step. Steering, cancellation and recovery are later tickets.

// The public error enums are fixed by packaging.md §3.4–3.5.
#![allow(clippy::result_large_err)]

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use asterpath_core::{
    ApprovalGate, AwaitSignal, DedupKey, DurableExecutor, EventLog, LogEntry, LogId, LogQuery,
    ModuleCtx, SessionId, SignalIntent, SignalPayload, StepId, ToolCallId, TurnId,
};
use asterpath_protocol::{
    AbortKind, AgentError, AgentEvent, AgentRunner, ApprovalDecision, ApprovalPolicy,
    ApprovalRequest, ApprovalVerdict, CancelToken, CompactResult, ConfigSnapshot, Item,
    ModelRequest, NO_APPROVAL_ENDPOINT, OpenIntents, Parallelism, PostExecuteError,
    PostExecuteEvent, PreExecuteError, PreExecuteEvent, Provenance, ProviderCatalog, ProviderError,
    ProviderErrorKind, ReplaySafety, RunRequest, SessionEvent, SessionState, StopReason,
    StreamEvent, Tool, ToolArgs, ToolConfig, ToolContext, ToolDescriptor, ToolError, ToolErrorInfo,
    ToolErrorKind, ToolName, ToolOutput, ToolRegistry, ToolResultStatus, TurnConfig, TurnEndReason,
    Usage, approval_signal_id, approval_signal_name, decode_entry, dedup,
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
    hooks: Option<ModuleCtx>,
    busy: Arc<Mutex<HashSet<SessionId>>>,
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
            hooks,
            busy: Arc::new(Mutex::new(HashSet::new())),
        }
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

    async fn execute(&self, session: SessionId, events: &EventSink) {
        if let Err(error) = self.execute_inner(session, events).await {
            events.push(Err(error));
        }
    }

    async fn execute_inner(
        &self,
        session: SessionId,
        events: &EventSink,
    ) -> Result<(), AgentError> {
        let decoded = decode_entries(self.scan(session).await?, session)?;
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
                self.run_steps(session, &config, turn, None, events).await
            }
            SessionState::TurnOpen { turn, .. } => {
                // Resume from the log: never repeat TurnStarted.
                let open_step = open_step(&decoded, turn);
                if let Some((step, _)) = open_step
                    && has_assistant_message(&decoded, turn, step)
                {
                    // The model call already committed; finish its tools, commit,
                    // then continue the turn exactly like a normal step.
                    match self.resume_step(session, turn, step, events).await? {
                        StepOutcome::Continue => {}
                        StepOutcome::Turn(reason) => {
                            return self.commit_turn(session, turn, reason, events).await;
                        }
                    }
                    return self.run_steps(session, &config, turn, None, events).await;
                }

                self.run_steps(session, &config, turn, open_step, events)
                    .await
            }
        }
    }

    async fn run_steps(
        &self,
        session: SessionId,
        config: &ConfigSnapshot,
        turn: TurnId,
        existing_step: Option<(StepId, u64)>,
        events: &EventSink,
    ) -> Result<(), AgentError> {
        let mut pending = existing_step;
        let mut steps_started = 0u32;

        loop {
            let (step, watermark) = match pending.take() {
                Some(step) => step,
                None => {
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
                    (step, seq)
                }
            };

            match self
                .run_step(session, config, turn, step, watermark, events)
                .await?
            {
                StepOutcome::Continue => {}
                StepOutcome::Turn(reason) => {
                    return self.commit_turn(session, turn, reason, events).await;
                }
            }
        }
    }

    async fn run_step(
        &self,
        session: SessionId,
        config: &ConfigSnapshot,
        turn: TurnId,
        step: StepId,
        watermark: u64,
        events: &EventSink,
    ) -> Result<StepOutcome, AgentError> {
        // The projection is computed once per step; retries reuse it (L12).
        let request = self.build_request(session, config, watermark).await?;
        let max_attempts = config.turn.max_attempts.get();
        let mut attempt = 1u32;

        loop {
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

            while let Some(event) = stream.next().await {
                match event {
                    Ok(StreamEvent::PartDelta { delta, .. }) => {
                        events.push(Ok(AgentEvent::ModelDelta { turn, step, delta }));
                    }
                    Ok(StreamEvent::ItemCompleted {
                        output_index, item, ..
                    }) => {
                        completed.insert(output_index, item);
                    }
                    Ok(StreamEvent::ResponseEnded {
                        stop_reason, usage, ..
                    }) => {
                        terminal = Some((stop_reason, usage));
                        break;
                    }
                    Ok(_) => {}
                    Err(error) => {
                        failure = Some(error);
                        break;
                    }
                }
            }
            drop(stream);

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
                    self.execute_tool_calls(session, turn, step, &existing, calls, events)
                        .await?;
                }

                self.commit_step(session, turn, step).await?;
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

            if will_retry {
                if !backoff.is_zero() {
                    tokio::time::sleep(backoff).await;
                }
                attempt += 1;
                continue;
            }

            self.commit_step(session, turn, step).await?;
            return Ok(StepOutcome::Turn(if error.context_exceeded() {
                TurnEndReason::ContextExhausted
            } else {
                TurnEndReason::Failed
            }));
        }
    }

    /// Finish an open step whose `AssistantMessage` is already committed: run
    /// every call without a `ToolResult`, reusing recorded intents verbatim.
    async fn resume_step(
        &self,
        session: SessionId,
        turn: TurnId,
        step: StepId,
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
        let calls = tool_calls(&items);
        let had_calls = !calls.is_empty();
        if had_calls {
            self.execute_tool_calls(session, turn, step, &existing, calls, events)
                .await?;
        }

        self.commit_step(session, turn, step).await?;
        let pending_input = self.pending_input(session).await?;
        Ok(step_outcome(&stop_reason, had_calls, pending_input))
    }

    /// The fixed pipeline: pre-execute, intent, registry, approval, scheduling,
    /// invocation, truncation, post-execute, result.
    async fn execute_tool_calls(
        &self,
        session: SessionId,
        turn: TurnId,
        step: StepId,
        existing: &ExistingStep,
        calls: Vec<CallItem>,
        events: &EventSink,
    ) -> Result<(), AgentError> {
        let lock = Arc::new(tokio::sync::RwLock::new(()));
        let semaphore = Arc::new(tokio::sync::Semaphore::new(
            self.tool_config.max_parallel_calls.get(),
        ));
        let budget = Arc::new(Mutex::new(self.tool_config.max_output_bytes_per_step));
        let mut runnable: Vec<Runnable> = Vec::new();

        for call in calls {
            if let Some(intent) = existing.intents.get(&call.provider_call_id) {
                if existing.results.contains(&intent.tool_call_id) {
                    continue;
                }
                match self.tools.get(&intent.name) {
                    None => {
                        self.append_tool_result(
                            session,
                            intent.tool_call_id,
                            ToolResultStatus::Failed,
                            None,
                            Some(ToolErrorInfo {
                                kind: ToolErrorKind::NotFound,
                                message: format!("tool {} is not registered", intent.name),
                            }),
                            false,
                        )
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
                                events,
                            )
                            .await?
                        {
                            ToolDecision::Allow => runnable.push(Runnable {
                                tool,
                                descriptor,
                                tool_call_id: intent.tool_call_id,
                                name: intent.name.clone(),
                                args: intent.args.clone(),
                            }),
                            ToolDecision::Deny(reason) => {
                                self.deny(session, intent.tool_call_id, reason).await?
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
                            events,
                        )
                        .await?
                    {
                        ToolDecision::Allow => runnable.push(Runnable {
                            tool,
                            descriptor,
                            tool_call_id,
                            name: call.name,
                            args,
                        }),
                        ToolDecision::Deny(reason) => {
                            self.deny(session, tool_call_id, reason).await?
                        }
                    }
                }
                (None, Some(_), None) => unreachable!("descriptor comes from the registry entry"),
            }
        }

        if runnable.is_empty() {
            return Ok(());
        }

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
                events,
            ));
        }

        let mut failure = None;
        while let Some(result) = futures.next().await {
            if let Err(error) = result {
                failure.get_or_insert(error);
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
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

            let cancel = CancelToken::new();
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
            ApprovalVerdict::Allow => Ok(ToolDecision::Allow),
            ApprovalVerdict::Deny { reason } => Ok(ToolDecision::Deny(
                reason.unwrap_or_else(|| DENIED_WITHOUT_REASON.to_owned()),
            )),
            ApprovalVerdict::Ask => {
                if !self.gate.is_open() {
                    return Ok(ToolDecision::Deny(NO_APPROVAL_ENDPOINT.to_owned()));
                }
                events.push(Ok(AgentEvent::ApprovalRequested(request)));
                let payload = self
                    .executor
                    .await_signal(AwaitSignal {
                        session,
                        signal: approval_signal_id(tool_call_id),
                        name: approval_signal_name(tool_call_id),
                        caused_by,
                        intent: SignalIntent::AlreadyRecorded,
                    })
                    .await?;
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
        };
        let sink = Arc::new(EventSink::default());
        let future_sink = Arc::clone(&sink);
        let future: BoxFuture<'_, ()> = Box::pin(async move {
            self.execute(session, &future_sink).await;
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
            if !intents.is_idle()
                || self
                    .busy
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .contains(&session)
            {
                return Err(AgentError::Busy { session });
            }
            Ok(CompactResult::NotConfigured)
        })
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

struct BusyGuard {
    busy: Arc<Mutex<HashSet<SessionId>>>,
    session: SessionId,
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
    timeout: Option<Duration>,
    kill_grace: Duration,
) -> Invocation {
    let future = AssertUnwindSafe(tool.invoke(args, context)).catch_unwind();
    tokio::pin!(future);

    match timeout {
        None => match future.await {
            Ok(result) => Invocation::Returned(result),
            Err(_) => Invocation::Panic,
        },
        Some(limit) => match tokio::time::timeout(limit, future.as_mut()).await {
            Ok(Ok(result)) => Invocation::Returned(result),
            Ok(Err(_)) => Invocation::Panic,
            Err(_) => {
                // Cancellation took the deadline first: the cancel path owns
                // the terminal, so no `Timeout` may be written.
                let already_cancelled = cancel.is_cancelled();
                cancel.cancel();
                match tokio::time::timeout(kill_grace, future.as_mut()).await {
                    Ok(Ok(result)) => Invocation::Returned(result),
                    Ok(Err(_)) => Invocation::Panic,
                    Err(_) if already_cancelled => Invocation::Cancelled,
                    Err(_) => Invocation::Timeout,
                }
            }
        },
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
                ..
            } if *turn_id == turn && *step_id == step => {
                existing.intents.insert(
                    provider_call_id.clone(),
                    RecordedIntent {
                        tool_call_id: *tool_call_id,
                        log_id: entry.log_id,
                        name: name.clone(),
                        args: raw_args.clone(),
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
