//! Tool pipeline: every tool call of one step, from the model's request to a
//! recorded `ToolResult` (GLOSSARY「工具管线」).
//!
//! One operation, [`ToolPipeline::run`]. Admission (before_tool, intent,
//! registry, approval) runs serially in model order; admitted calls then run
//! under the step's Parallel/Exclusive lock and concurrency cap, with per-call
//! and per-step truncation and after_tool. A cancelled step is closed here too:
//! cancel grace, interrupted results and the step's open child waits.

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use asterpath_protocol::{
    AgentError, AgentEvent, ApprovalDecision, ApprovalGate, ApprovalPolicy, ApprovalRequest,
    ApprovalVerdict, CancelReason, CancelToken, ChildOutcome, Clock, DedupKey, HookError, Hooks,
    Item, LogEntry, NO_APPROVAL_ENDPOINT, Parallelism, Part, Provenance, ReplaySafety,
    SessionEvent, SessionFacts, SessionId, SessionStore, StepId, Tool, ToolArgs, ToolCallCtx,
    ToolCallId, ToolConfig, ToolContext, ToolDescriptor, ToolError, ToolErrorInfo, ToolErrorKind,
    ToolGate, ToolName, ToolOutput, ToolResultStatus, ToolResultView, TurnId, UsageLookup, dedup,
    root_of, unique_bytes,
};
use futures_core::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};

use super::{EventSink, caught};
use crate::scan::scan_decoded;
use crate::scheduler::SPAWN_TOOL_NAME;
use crate::tools::ToolTable;
use crate::waits::Waits;

const DENIED_WITHOUT_REASON: &str = "tool call denied";

/// Runs a `spawn_agent` call. The agent supplies it per step so the scheduler
/// drives children with that agent and never owns it.
pub(crate) type SpawnFn<'a> = dyn Fn(ToolArgs, ToolContext) -> BoxFuture<'a, Result<ToolOutput, ToolError>>
    + Send
    + Sync
    + 'a;

/// The step a batch of calls belongs to.
pub(crate) struct ToolStep<'a> {
    pub(crate) session: SessionId,
    pub(crate) turn: TurnId,
    pub(crate) step: StepId,
    /// The step's `AssistantMessage` was committed before a crash: a recorded
    /// unsafe intent without a recorded approval is never rerun.
    pub(crate) recovering: bool,
    pub(crate) cancel_grace: Duration,
    /// The step's effective tool set: a fresh call to any other name is
    /// `NotFound`, even if the registry holds it (ADR-0030).
    pub(crate) tools: &'a [ToolDescriptor],
    /// The output tool this step's request carried (ADR-0034).
    pub(crate) output: Option<&'a ToolName>,
    pub(crate) events: &'a EventSink,
    pub(crate) spawn: Option<&'a SpawnFn<'a>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ToolOutcome {
    /// Every call has a result.
    Done,
    /// Every call has a result and the output tool was called: the turn is over.
    Delivered,
    /// The step was cancelled; every call still has a result.
    Cancelled,
}

pub(crate) struct ToolPipeline {
    log: Arc<dyn SessionStore>,
    tools: Arc<ToolTable>,
    config: ToolConfig,
    approval: Arc<dyn ApprovalPolicy>,
    gate: Arc<ApprovalGate>,
    waits: Arc<Waits>,
    clock: Arc<dyn Clock>,
    /// Application hooks, called in registration order (ADR-0025).
    hooks: Vec<Arc<dyn Hooks>>,
    /// Backs `ToolCallCtx::usage`.
    usage: Option<UsageLookup>,
}

impl ToolPipeline {
    pub(crate) fn new(
        log: Arc<dyn SessionStore>,
        tools: Arc<ToolTable>,
        config: ToolConfig,
        approval: Arc<dyn ApprovalPolicy>,
        gate: Arc<ApprovalGate>,
        waits: Arc<Waits>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            log,
            tools,
            config,
            approval,
            gate,
            waits,
            clock,
            hooks: Vec::new(),
            usage: None,
        }
    }

    #[must_use]
    pub(crate) fn with_hooks(
        mut self,
        hooks: Vec<Arc<dyn Hooks>>,
        usage: Option<UsageLookup>,
    ) -> Self {
        self.hooks = hooks;
        self.usage = usage;
        self
    }

    /// Give every tool call in `items` a recorded result. Calls of this step
    /// that already have an intent reuse it verbatim; calls with a result are
    /// skipped. Each item is paired with its stream `output_index`.
    pub(crate) async fn run(
        &self,
        step: &ToolStep<'_>,
        items: &[(u32, Item)],
        cancel: &CancelToken,
    ) -> Result<ToolOutcome, AgentError> {
        let recorded = self.recorded(step).await?;
        let lock = Arc::new(tokio::sync::RwLock::new(()));
        let semaphore = Arc::new(tokio::sync::Semaphore::new(
            self.config.max_parallel_calls.get(),
        ));
        let budget = Arc::new(Mutex::new(self.config.max_output_bytes_per_step));
        let mut runnable: Vec<Runnable> = Vec::new();
        let mut cancelled = cancel.is_cancelled();
        let mut done = ToolOutcome::Done;

        for call in tool_calls(items) {
            // An output call never enters the pipeline; a recorded intent is
            // known by its provenance, whatever the config says now.
            let is_output_call = match recorded.intents.get(&call.provider_call_id) {
                Some(intent) => intent.provenance == Provenance::Output,
                None => step.output == Some(&call.name),
            };
            if is_output_call {
                done = ToolOutcome::Delivered;
                self.deliver(step, &recorded, call).await?;
                continue;
            }
            let Some(admitted) = self.resolve(step, &recorded, call).await? else {
                continue;
            };
            // Shared from here on: recorded and fresh intents differ only above.
            let info = admitted.info();
            if cancelled {
                self.interrupt(step, admitted.tool_call_id, Some(&info))
                    .await?;
                continue;
            }
            match admitted.pre_failure {
                Some(PreFailure::Denied(reason)) => {
                    self.deny(step, &info, reason).await?;
                    continue;
                }
                Some(PreFailure::Internal(message)) => {
                    self.fail(step, &info, ToolErrorKind::Internal, message)
                        .await?;
                    continue;
                }
                None => {}
            }
            let Some(tool) = admitted.tool.clone() else {
                self.not_found(step, &info).await?;
                continue;
            };
            let descriptor = tool.descriptor().clone();
            match self
                .decide_approval(step, &admitted, &descriptor, cancel)
                .await?
            {
                ToolDecision::Allow if cancel.is_cancelled() => {
                    cancelled = true;
                    self.interrupt(step, admitted.tool_call_id, Some(&info))
                        .await?;
                }
                ToolDecision::Allow => runnable.push(Runnable {
                    tool,
                    descriptor,
                    tool_call_id: admitted.tool_call_id,
                    output_index: admitted.output_index,
                    provider_call_id: admitted.provider_call_id,
                    name: admitted.name,
                    args: admitted.args,
                }),
                ToolDecision::Interrupt => {
                    self.interrupt(step, admitted.tool_call_id, Some(&info))
                        .await?;
                }
                ToolDecision::Deny(reason) => {
                    self.deny(step, &info, reason).await?;
                }
                ToolDecision::Cancelled => {
                    cancelled = true;
                    self.interrupt(step, admitted.tool_call_id, Some(&info))
                        .await?;
                }
            }
        }

        let pending: Vec<CallInfo> = runnable.iter().map(Runnable::info).collect();
        // Calls whose `ToolStarted` went out; the rest get it when they finish.
        let started = Arc::new(Mutex::new(HashSet::new()));
        if cancelled {
            // Calls that passed approval but never started are interrupted too.
            self.finish_cancelled(step, &pending, &started).await?;
            return Ok(ToolOutcome::Cancelled);
        }
        if runnable.is_empty() {
            return Ok(done);
        }

        let mut futures: FuturesUnordered<BoxFuture<'_, Result<(), AgentError>>> =
            FuturesUnordered::new();
        for item in runnable {
            futures.push(self.execute(
                step,
                item,
                Arc::clone(&semaphore),
                Arc::clone(&lock),
                Arc::clone(&budget),
                Arc::clone(&started),
                cancel,
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
                        None => Ok(done),
                    },
                }
            }
        }

        // Cancellation grace: in-flight tools may return a real result.
        if !step.cancel_grace.is_zero() {
            let deadline = tokio::time::sleep(step.cancel_grace);
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

        self.finish_cancelled(step, &pending, &started).await?;
        Ok(ToolOutcome::Cancelled)
    }

    /// Close an output tool call with the fixed result: no hooks, approval or
    /// execution. A fresh call records its intent first.
    async fn deliver(
        &self,
        step: &ToolStep<'_>,
        recorded: &Recorded,
        call: CallItem,
    ) -> Result<(), AgentError> {
        let output_index = call.output_index;
        let (tool_call_id, name, args) = match recorded.intents.get(&call.provider_call_id) {
            Some(intent) if recorded.closed.contains(&intent.tool_call_id) => return Ok(()),
            Some(intent) => (
                intent.tool_call_id,
                intent.name.clone(),
                intent.args.clone(),
            ),
            None => {
                let tool_call_id = ToolCallId::from_bytes(unique_bytes(self.clock.now()));
                self.append(
                    step.session,
                    SessionEvent::ToolCallRequested {
                        turn_id: step.turn,
                        step_id: step.step,
                        tool_call_id,
                        provider_call_id: call.provider_call_id,
                        name: call.name.clone(),
                        raw_args: call.raw_args.clone(),
                        replay: ReplaySafety::Safe,
                        provenance: Provenance::Output,
                    },
                    Some(dedup::tool_call(tool_call_id)),
                )
                .await?;
                (tool_call_id, call.name, call.raw_args)
            }
        };
        self.announce(
            step,
            &CallInfo {
                tool_call_id,
                output_index,
                name,
                args,
            },
        );
        let output = vec![Part::Text {
            text: super::output::DELIVERED.to_owned(),
        }];
        self.finish(
            step,
            tool_call_id,
            ToolResultStatus::Completed,
            Some(output),
            None,
            false,
        )
        .await
    }

    /// The live tool of that name in the drive's session environment.
    fn tool(&self, name: &ToolName) -> Option<Arc<dyn Tool>> {
        crate::environment::tool(&self.tools, crate::environment::current().as_deref(), name)
    }

    /// One read of the log per step: this step's recorded intents and which
    /// of them are closed.
    async fn recorded(&self, step: &ToolStep<'_>) -> Result<Recorded, AgentError> {
        let decoded = scan_decoded(&*self.log, step.session).await?;
        Ok(Recorded::of(&decoded, step.turn, step.step))
    }

    /// Intent resolution: a recorded intent is reused (an `Unknown` one is
    /// closed `NotFound` at once); a fresh call runs `before_tool` and records
    /// its intent. `None` means the call needs nothing more.
    async fn resolve(
        &self,
        step: &ToolStep<'_>,
        recorded: &Recorded,
        call: CallItem,
    ) -> Result<Option<Admitted>, AgentError> {
        if let Some(intent) = recorded.intents.get(&call.provider_call_id) {
            if recorded.closed.contains(&intent.tool_call_id) {
                return Ok(None);
            }
            // A tool that was absent when its intent was recorded never
            // enters approval and is never rerun.
            if intent.provenance == Provenance::Unknown {
                let info = CallInfo {
                    tool_call_id: intent.tool_call_id,
                    output_index: call.output_index,
                    name: intent.name.clone(),
                    args: intent.args.clone(),
                };
                self.not_found(step, &info).await?;
                return Ok(None);
            }
            return Ok(Some(Admitted {
                tool_call_id: intent.tool_call_id,
                output_index: call.output_index,
                provider_call_id: call.provider_call_id,
                tool: self.tool(&intent.name),
                name: intent.name.clone(),
                args: intent.args.clone(),
                replay: intent.replay,
                recorded: true,
                pre_failure: None,
            }));
        }

        let tool_call_id = ToolCallId::from_bytes(unique_bytes(self.clock.now()));
        let mut args = call.raw_args;
        let tool = step
            .tools
            .iter()
            .any(|tool| tool.name == call.name)
            .then(|| self.tool(&call.name))
            .flatten();
        let (replay, provenance) = match &tool {
            Some(tool) => (tool.descriptor().replay, tool.descriptor().provenance),
            None => (ReplaySafety::Unsafe, Provenance::Unknown),
        };
        let ctx = self.call_ctx(
            step,
            tool_call_id,
            &call.provider_call_id,
            &call.name,
            provenance,
            replay,
        );
        let pre_failure = self.before_tool(&ctx, &mut args).await;
        self.append(
            step.session,
            SessionEvent::ToolCallRequested {
                turn_id: step.turn,
                step_id: step.step,
                tool_call_id,
                provider_call_id: call.provider_call_id.clone(),
                name: call.name.clone(),
                raw_args: args.clone(),
                replay,
                provenance,
            },
            Some(dedup::tool_call(tool_call_id)),
        )
        .await?;
        Ok(Some(Admitted {
            tool_call_id,
            output_index: call.output_index,
            provider_call_id: call.provider_call_id,
            tool,
            name: call.name,
            args,
            replay,
            recorded: false,
            pre_failure,
        }))
    }

    fn call_ctx(
        &self,
        step: &ToolStep<'_>,
        tool_call_id: ToolCallId,
        provider_call_id: &str,
        name: &ToolName,
        provenance: Provenance,
        replay: ReplaySafety,
    ) -> ToolCallCtx {
        ToolCallCtx::new(
            step.session,
            step.turn,
            step.step,
            tool_call_id,
            provider_call_id.to_owned(),
            name.clone(),
            provenance,
            replay,
            self.usage.clone(),
        )
    }

    /// Interrupt every pending call of this step that has no recorded result,
    /// then close the step's open child waits. Reading the log (not driver
    /// memory) makes a real result that landed while its future was dropped
    /// still win.
    async fn finish_cancelled(
        &self,
        step: &ToolStep<'_>,
        pending: &[CallInfo],
        started: &Mutex<HashSet<ToolCallId>>,
    ) -> Result<(), AgentError> {
        let decoded = scan_decoded(&*self.log, step.session).await?;
        let facts = SessionFacts::fold_decoded(decoded.iter().map(|(entry, event)| (entry, event)));
        let step_calls: HashSet<ToolCallId> = decoded
            .iter()
            .filter_map(|(_, event)| match event {
                SessionEvent::ToolCallRequested {
                    turn_id,
                    step_id,
                    tool_call_id,
                    ..
                } if *turn_id == step.turn && *step_id == step.step => Some(*tool_call_id),
                _ => None,
            })
            .collect();
        // #41/#48: a spawn owned by this step is a wait to close even when its
        // tool future was dropped before the scheduler wrote the terminal.
        // Dedup by child id makes a racing write a no-op.
        let owned_children = facts
            .open_children()
            .filter(|(_, child)| step_calls.contains(&child.parent_tool_call));
        for (child, _) in owned_children {
            self.append(
                step.session,
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
        // Every pending call has a recorded intent; an open one has no result.
        for call in pending {
            if facts.open_tool(call.tool_call_id).is_some() {
                let unstarted = !started
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .contains(&call.tool_call_id);
                self.interrupt(step, call.tool_call_id, unstarted.then_some(call))
                    .await?;
            }
        }
        Ok(())
    }

    /// One interrupted `ToolResult`; the dedup key makes a racing real result
    /// win. `unstarted` is a call whose `ToolStarted` has not gone out yet.
    async fn interrupt(
        &self,
        step: &ToolStep<'_>,
        tool_call_id: ToolCallId,
        unstarted: Option<&CallInfo>,
    ) -> Result<(), AgentError> {
        if let Some(call) = unstarted {
            self.announce(step, call);
        }
        self.finish(
            step,
            tool_call_id,
            ToolResultStatus::Interrupted,
            None,
            None,
            false,
        )
        .await
    }

    /// `ToolStarted`: every `ToolFinished` follows one, so a call that never
    /// runs is announced right before it is closed.
    fn announce(&self, step: &ToolStep<'_>, call: &CallInfo) {
        step.events.push(Ok(AgentEvent::ToolStarted {
            turn: step.turn,
            step: step.step,
            output_index: call.output_index,
            call: call.tool_call_id,
            tool: call.name.clone(),
            args: call.args.clone(),
        }));
    }

    /// Record a call's `ToolResult` and emit the matching `ToolFinished`.
    async fn finish(
        &self,
        step: &ToolStep<'_>,
        tool_call_id: ToolCallId,
        status: ToolResultStatus,
        output: Option<Vec<Part>>,
        error: Option<ToolErrorInfo>,
        truncated: bool,
    ) -> Result<(), AgentError> {
        self.append_result(
            step.session,
            tool_call_id,
            status,
            output.clone(),
            error.clone(),
            truncated,
        )
        .await?;
        step.events.push(Ok(AgentEvent::ToolFinished {
            turn: step.turn,
            step: step.step,
            call: tool_call_id,
            status,
            output,
            error,
        }));
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn execute<'a, 'b: 'a>(
        &'a self,
        step: &'a ToolStep<'b>,
        item: Runnable,
        semaphore: Arc<tokio::sync::Semaphore>,
        lock: Arc<tokio::sync::RwLock<()>>,
        budget: Arc<Mutex<usize>>,
        started: Arc<Mutex<HashSet<ToolCallId>>>,
        turn_cancel: &'a CancelToken,
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

            started
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(item.tool_call_id);
            self.announce(step, &item.info());

            // A child of the turn token: the turn broadcasts to every tool, and
            // a per-call timeout cancels only this call.
            let cancel = turn_cancel.child();
            let context = ToolContext::new(
                step.session,
                step.turn,
                step.step,
                item.tool_call_id,
                cancel.clone(),
            )
            .with_root(root_of(&*self.log, step.session).await?)
            .with_spawn(
                crate::environment::current()
                    .map(|env| env.spawn.clone())
                    .unwrap_or_default(),
            );
            let timeout = item
                .descriptor
                .timeout
                .or(match item.descriptor.provenance {
                    Provenance::External { .. } => self.config.external_timeout,
                    _ => None,
                });
            let call = match step.spawn {
                Some(spawn) if item.descriptor.name.as_str() == SPAWN_TOOL_NAME => {
                    spawn(item.args, context)
                }
                _ => item.tool.invoke(item.args, context),
            };
            let invocation = invoke_tool(
                call,
                cancel,
                turn_cancel.clone(),
                timeout,
                self.config.kill_grace,
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
                    .unwrap_or(self.config.max_output_bytes);
                let (parts, was_truncated) =
                    truncate_output(output.take().unwrap_or_default(), limit, &budget);
                output = Some(parts);
                truncated = was_truncated;
            }

            if !self.hooks.is_empty() {
                let ctx = self.call_ctx(
                    step,
                    item.tool_call_id,
                    &item.provider_call_id,
                    &item.name,
                    item.descriptor.provenance,
                    item.descriptor.replay,
                );
                let mut view = ToolResultView::new(
                    status,
                    output.clone().unwrap_or_default(),
                    error.clone(),
                    truncated,
                );
                match self.after_tool(&ctx, &mut view).await {
                    Ok(()) => {
                        // Status is not an after_tool concern; only content and
                        // the model-visible error message are rewritable.
                        if status == ToolResultStatus::Completed {
                            output = Some(view.output);
                            truncated = view.truncated;
                        }
                        if status == ToolResultStatus::Failed {
                            error = view.error;
                        }
                    }
                    Err(failure) => {
                        status = ToolResultStatus::Failed;
                        output = None;
                        error = Some(ToolErrorInfo {
                            kind: ToolErrorKind::Internal,
                            message: failure.message,
                        });
                        truncated = false;
                    }
                }
            }

            self.finish(step, item.tool_call_id, status, output, error, truncated)
                .await?;

            drop(_guard);
            Ok(())
        })
    }

    /// Application `before_tool` hooks in order: rewrites pass along, the first
    /// `Deny` short-circuits, and an error or panic fails the call.
    async fn before_tool(&self, ctx: &ToolCallCtx, args: &mut ToolArgs) -> Option<PreFailure> {
        for hook in &self.hooks {
            match caught(|| hook.before_tool(ctx, args)).await {
                Ok(ToolGate::Deny { reason }) => {
                    return Some(PreFailure::Denied(
                        reason.unwrap_or_else(|| DENIED_WITHOUT_REASON.to_owned()),
                    ));
                }
                Ok(_) => {}
                Err(error) => return Some(PreFailure::Internal(error.message)),
            }
        }
        None
    }

    /// Application `after_tool` hooks in order; the first error or panic wins.
    async fn after_tool(
        &self,
        ctx: &ToolCallCtx,
        view: &mut ToolResultView,
    ) -> Result<(), HookError> {
        for hook in &self.hooks {
            caught(|| hook.after_tool(ctx, view)).await?;
        }
        Ok(())
    }

    /// A recorded approval signal wins over the current policy and is never
    /// re-prompted. `Ask` without a live endpoint fails closed.
    async fn decide_approval(
        &self,
        step: &ToolStep<'_>,
        call: &Admitted,
        descriptor: &ToolDescriptor,
        cancel: &CancelToken,
    ) -> Result<ToolDecision, AgentError> {
        // Read right before deciding: a decision recorded while an earlier
        // call of this step waited for a human still wins.
        if let Some(decision) = self
            .waits
            .find_approval(step.session, call.tool_call_id)
            .await?
        {
            return approval_decision(step.session, decision);
        }

        // Endpoints and policies attend a whole tree (ADR-0039).
        let root = root_of(&*self.log, step.session).await?;
        let request = ApprovalRequest::new(
            step.session,
            step.turn,
            step.step,
            call.tool_call_id,
            call.name.clone(),
            call.args.clone(),
            descriptor.replay,
            descriptor.side_effect,
            descriptor.provenance,
        )
        .with_root(root);
        match self.approval.classify(&request) {
            // A recorded decision above always wins. Without one, recovery
            // never reruns an unsafe tool.
            ApprovalVerdict::Allow
                if call.recorded && step.recovering && call.replay == ReplaySafety::Unsafe =>
            {
                Ok(ToolDecision::Interrupt)
            }
            ApprovalVerdict::Allow => Ok(ToolDecision::Allow),
            ApprovalVerdict::Deny { reason } => Ok(ToolDecision::Deny(
                reason.unwrap_or_else(|| DENIED_WITHOUT_REASON.to_owned()),
            )),
            ApprovalVerdict::Ask => {
                if !self.gate.is_open(root) {
                    return Ok(ToolDecision::Deny(NO_APPROVAL_ENDPOINT.to_owned()));
                }
                // Listed until this wait ends for any reason, including drop.
                let _pending = self.gate.wait_for(root, request.clone());
                step.events.push(Ok(AgentEvent::ApprovalRequested(request)));
                // A decision recorded after this step's read is still returned here.
                let wait = self.waits.await_approval(step.session, call.tool_call_id);
                tokio::pin!(wait);
                let token = cancel.clone();
                let cancel_wait = token.cancelled();
                tokio::pin!(cancel_wait);
                let decision = tokio::select! {
                    biased;
                    // The interrupted ToolResult closes this call's intent.
                    _ = &mut cancel_wait => return Ok(ToolDecision::Cancelled),
                    decision = &mut wait => decision?,
                };
                approval_decision(step.session, decision)
            }
            _ => Ok(ToolDecision::Deny(DENIED_WITHOUT_REASON.to_owned())),
        }
    }

    async fn deny(
        &self,
        step: &ToolStep<'_>,
        call: &CallInfo,
        message: String,
    ) -> Result<(), AgentError> {
        self.fail(step, call, ToolErrorKind::Denied, message).await
    }

    /// The model-visible terminal for a tool that is not in the registry.
    async fn not_found(&self, step: &ToolStep<'_>, call: &CallInfo) -> Result<(), AgentError> {
        self.fail(
            step,
            call,
            ToolErrorKind::NotFound,
            format!("tool {} is not registered", call.name),
        )
        .await
    }

    /// A call that fails before it starts.
    async fn fail(
        &self,
        step: &ToolStep<'_>,
        call: &CallInfo,
        kind: ToolErrorKind,
        message: String,
    ) -> Result<(), AgentError> {
        self.announce(step, call);
        self.finish(
            step,
            call.tool_call_id,
            ToolResultStatus::Failed,
            None,
            Some(ToolErrorInfo { kind, message }),
            false,
        )
        .await
    }

    async fn append_result(
        &self,
        session: SessionId,
        tool_call_id: ToolCallId,
        status: ToolResultStatus,
        output: Option<Vec<Part>>,
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
    }

    async fn append(
        &self,
        session: SessionId,
        event: SessionEvent,
        key: Option<DedupKey>,
    ) -> Result<(), AgentError> {
        let entry = asterpath_protocol::encode_entry(session, key, &event).map_err(|error| {
            AgentError::Corrupt {
                session,
                detail: error.to_string(),
            }
        })?;
        self.log.append(entry).await?;
        Ok(())
    }
}

struct CallItem {
    output_index: u32,
    provider_call_id: String,
    name: ToolName,
    raw_args: ToolArgs,
}

struct RecordedIntent {
    tool_call_id: ToolCallId,
    name: ToolName,
    args: ToolArgs,
    replay: ReplaySafety,
    provenance: Provenance,
}

/// What the log already says about this step's calls.
#[derive(Default)]
struct Recorded {
    /// This step's intents by provider call id.
    intents: HashMap<String, RecordedIntent>,
    /// Intents of this step that already have a result.
    closed: HashSet<ToolCallId>,
}

impl Recorded {
    fn of(decoded: &[(LogEntry, SessionEvent)], turn: TurnId, step: StepId) -> Self {
        let mut recorded = Self::default();
        for (_, event) in decoded {
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
                    recorded.intents.insert(
                        provider_call_id.clone(),
                        RecordedIntent {
                            tool_call_id: *tool_call_id,
                            name: name.clone(),
                            args: raw_args.clone(),
                            replay: *replay,
                            provenance: *provenance,
                        },
                    );
                }
                _ => {}
            }
        }
        let facts = SessionFacts::fold_decoded(decoded.iter().map(|(entry, event)| (entry, event)));
        recorded.closed = recorded
            .intents
            .values()
            .map(|intent| intent.tool_call_id)
            .filter(|id| facts.open_tool(*id).is_none())
            .collect();
        recorded
    }
}

/// A call whose intent is recorded, before approval.
struct Admitted {
    tool_call_id: ToolCallId,
    output_index: u32,
    provider_call_id: String,
    tool: Option<Arc<dyn Tool>>,
    name: ToolName,
    args: ToolArgs,
    /// The recorded replay safety for a reused intent.
    replay: ReplaySafety,
    /// The intent was in the log before this run.
    recorded: bool,
    pre_failure: Option<PreFailure>,
}

struct Runnable {
    provider_call_id: String,
    tool: Arc<dyn Tool>,
    descriptor: ToolDescriptor,
    tool_call_id: ToolCallId,
    output_index: u32,
    name: ToolName,
    args: ToolArgs,
}

impl Admitted {
    fn info(&self) -> CallInfo {
        CallInfo {
            tool_call_id: self.tool_call_id,
            output_index: self.output_index,
            name: self.name.clone(),
            args: self.args.clone(),
        }
    }
}

impl Runnable {
    fn info(&self) -> CallInfo {
        CallInfo {
            tool_call_id: self.tool_call_id,
            output_index: self.output_index,
            name: self.name.clone(),
            args: self.args.clone(),
        }
    }
}

/// What `ToolStarted` reports about a call.
struct CallInfo {
    tool_call_id: ToolCallId,
    output_index: u32,
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
    decision: ApprovalDecision,
) -> Result<ToolDecision, AgentError> {
    match decision {
        ApprovalDecision::Allow => Ok(ToolDecision::Allow),
        ApprovalDecision::Deny { reason } => Ok(ToolDecision::Deny(
            reason.unwrap_or_else(|| DENIED_WITHOUT_REASON.to_owned()),
        )),
        _ => Err(AgentError::Corrupt {
            session,
            detail: "unknown approval decision".to_owned(),
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

/// Invoke one tool with pipeline-owned deadline and panic containment.
async fn invoke_tool(
    call: BoxFuture<'_, Result<ToolOutput, ToolError>>,
    cancel: CancelToken,
    turn_cancel: CancelToken,
    timeout: Option<Duration>,
    kill_grace: Duration,
) -> Invocation {
    let future = AssertUnwindSafe(call).catch_unwind();
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
            let already_cancelled = cancel.is_cancelled() || turn_cancel.is_cancelled();
            cancel.cancel();
            match tokio::time::timeout(kill_grace, future.as_mut()).await {
                // The tool only saw a cancel; the engine knows it was the deadline
                // unless the turn was cancelled during the grace too.
                Ok(Ok(Err(ToolError::Cancelled { message })))
                    if !already_cancelled && !turn_cancel.is_cancelled() =>
                {
                    Invocation::Returned(Err(ToolError::Timeout { message }))
                }
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
    parts: Vec<Part>,
    call_limit: usize,
    budget: &Mutex<usize>,
) -> (Vec<Part>, bool) {
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
        if let Part::Text { text } = part {
            let room = limit.saturating_sub(used);
            let cut = floor_char_boundary(&text, room);
            if cut > 0 {
                let kept_text = text[..cut].to_owned();
                used += kept_text.len();
                kept.push(Part::Text { text: kept_text });
            }
            full = true;
        }
    }

    let omitted = total.saturating_sub(used);
    let suffix = format!("[output truncated: {omitted} bytes omitted]");
    used += suffix.len();
    kept.push(Part::Text { text: suffix });
    *remaining = remaining.saturating_sub(used);
    (kept, true)
}

fn part_bytes(part: &Part) -> usize {
    match part {
        Part::Text { text } => text.len(),
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

fn tool_calls(items: &[(u32, Item)]) -> Vec<CallItem> {
    items
        .iter()
        .filter_map(|&(output_index, ref item)| match item {
            Item::ToolCall {
                call_id,
                name,
                raw_args,
                ..
            } => Some(CallItem {
                output_index,
                provider_call_id: call_id.clone(),
                name: name.clone(),
                raw_args: raw_args.clone(),
            }),
            _ => None,
        })
        .collect()
}
