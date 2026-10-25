//! The tool pipeline driven directly: no provider, no turn loop.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::support::{SESSION, append, decoded, descriptor, text, tool_call, tool_table};
use crate::agent::EventSink;
use crate::agent::pipeline::{ToolOutcome, ToolPipeline, ToolStep};
use crate::waits::Waits;
use asterpath_protocol::{
    AgentEvent, ApprovalDecision, ApprovalPolicy, ApprovalRequest, ApprovalVerdict, CancelReason,
    CancelToken, ChildOutcome, ContextPolicy, DefaultApprovalPolicy, HookError, Hooks, Item,
    JsonString, NO_APPROVAL_ENDPOINT, Part, Provenance, ReplaySafety, SessionEvent, Tool, ToolArgs,
    ToolCallCtx, ToolConfig, ToolDescriptor, ToolErrorInfo, ToolErrorKind, ToolGate, ToolName,
    ToolOutput, ToolResultStatus, ToolResultView, dedup,
};
use asterpath_protocol::{
    ApprovalGate, LogId, SessionId, SessionStore, StepId, ToolCallId, TurnId,
};
use asterpath_testing::{InMemoryEventLog, ManualClock, TestTool};
use futures_core::future::BoxFuture;

const TURN: TurnId = TurnId::from_bytes([1; 16]);
const STEP: StepId = StepId::from_bytes([2; 16]);
const RECORDED: ToolCallId = ToolCallId::from_bytes([3; 16]);

struct Harness {
    log: Arc<InMemoryEventLog>,
    gate: Arc<ApprovalGate>,
    waits: Arc<Waits>,
    tools: Vec<Arc<dyn Tool>>,
    policy: Option<Arc<dyn ApprovalPolicy>>,
    hooks: Vec<Arc<dyn Hooks>>,
    config: ToolConfig,
    cancel_grace: Duration,
}

impl Harness {
    /// A log with this step open and no tool records yet.
    async fn new(tools: Vec<Arc<dyn Tool>>) -> Self {
        let log = Arc::new(InMemoryEventLog::new());
        append(&log, SessionEvent::TurnStarted { turn_id: TURN }, None).await;
        append(
            &log,
            SessionEvent::StepStarted {
                turn_id: TURN,
                step_id: STEP,
            },
            None,
        )
        .await;
        let waits = Arc::new(Waits::new(log.clone()));
        Self {
            log,
            gate: Arc::new(ApprovalGate::new()),
            waits,
            tools,
            policy: None,
            hooks: Vec::new(),
            config: {
                let mut config = ToolConfig::default();
                config.kill_grace = Duration::ZERO;
                config
            },
            cancel_grace: Duration::ZERO,
        }
    }

    fn hook(mut self, hook: impl Hooks + 'static) -> Self {
        self.hooks.push(Arc::new(hook));
        self
    }

    fn policy(mut self, policy: impl ApprovalPolicy + 'static) -> Self {
        self.policy = Some(Arc::new(policy));
        self
    }

    /// Record an intent for `provider_call_id`, as a crash would leave it.
    async fn intent(
        &self,
        id: ToolCallId,
        provider_call_id: &str,
        name: &str,
        provenance: Provenance,
    ) {
        let replay = self
            .tools
            .iter()
            .find(|tool| tool.descriptor().name.as_str() == name)
            .map_or(ReplaySafety::Unsafe, |tool| tool.descriptor().replay);
        append(
            &self.log,
            SessionEvent::ToolCallRequested {
                turn_id: TURN,
                step_id: STEP,
                tool_call_id: id,
                provider_call_id: provider_call_id.to_owned(),
                name: ToolName::new(name),
                raw_args: JsonString::new("{}"),
                replay,
                provenance,
            },
            Some(dedup::tool_call(id)),
        )
        .await;
    }

    async fn deliver(&self, call: ToolCallId, decision: ApprovalDecision) {
        self.waits
            .record_approval(SESSION, call, decision)
            .await
            .expect("record");
    }

    fn pipeline(&self) -> ToolPipeline {
        let log: Arc<dyn SessionStore> = self.log.clone();
        let policy = self
            .policy
            .clone()
            .unwrap_or_else(|| Arc::new(DefaultApprovalPolicy));
        ToolPipeline::new(
            log,
            tool_table(self.tools.clone()),
            self.config.clone(),
            policy,
            Arc::clone(&self.gate),
            Arc::clone(&self.waits),
            Arc::new(ManualClock::new()),
        )
        .with_hooks(self.hooks.clone(), None)
    }

    /// Run one step's calls; `recovering` marks a resumed open step.
    async fn run_with(
        &self,
        items: &[Item],
        recovering: bool,
        cancel: &CancelToken,
    ) -> (ToolOutcome, Vec<AgentEvent>) {
        let events = EventSink::default();
        let tools: Vec<_> = self
            .tools
            .iter()
            .map(|tool| tool.descriptor().clone())
            .collect();
        let step = ToolStep {
            session: SESSION,
            turn: TURN,
            step: STEP,
            recovering,
            cancel_grace: self.cancel_grace,
            tools: &tools,
            output: None,
            events: &events,
            spawn: None,
        };
        let indexed: Vec<(u32, Item)> = (0..).zip(items.iter().cloned()).collect();
        let outcome = self
            .pipeline()
            .run(&step, &indexed, cancel)
            .await
            .expect("pipeline");
        let mut live = Vec::new();
        while let Some(event) = events.pop() {
            live.push(event.expect("event"));
        }
        (outcome, live)
    }

    async fn run(&self, items: &[Item]) -> (ToolOutcome, Vec<AgentEvent>) {
        self.run_with(items, false, &CancelToken::new()).await
    }

    async fn events(&self) -> Vec<SessionEvent> {
        decoded(&self.log).await
    }

    /// Recorded intents in log order: (id, provider call id, args, provenance).
    async fn intents(&self) -> Vec<(ToolCallId, String, String, Provenance)> {
        self.events()
            .await
            .into_iter()
            .filter_map(|event| match event {
                SessionEvent::ToolCallRequested {
                    tool_call_id,
                    provider_call_id,
                    raw_args,
                    provenance,
                    ..
                } => Some((
                    tool_call_id,
                    provider_call_id,
                    raw_args.as_str().to_owned(),
                    provenance,
                )),
                _ => None,
            })
            .collect()
    }

    /// The single recorded result of `provider_call_id`'s intent.
    async fn result(&self, provider_call_id: &str) -> Recorded {
        let call = self
            .intents()
            .await
            .into_iter()
            .find(|(_, id, _, _)| id == provider_call_id)
            .map(|(call, ..)| call)
            .expect("intent");
        let mut results = self
            .events()
            .await
            .into_iter()
            .filter_map(|event| match event {
                SessionEvent::ToolResult {
                    tool_call_id,
                    status,
                    output,
                    error,
                    truncated,
                } if tool_call_id == call => Some(Recorded {
                    status,
                    output: output.unwrap_or_default(),
                    error,
                    truncated,
                }),
                _ => None,
            });
        let result = results.next().expect("a result");
        assert!(results.next().is_none(), "exactly one result");
        result
    }
}

#[derive(Debug)]
struct Recorded {
    status: ToolResultStatus,
    output: Vec<Part>,
    error: Option<ToolErrorInfo>,
    truncated: bool,
}

impl Recorded {
    fn error_kind(&self) -> Option<ToolErrorKind> {
        self.error.as_ref().map(|error| error.kind)
    }

    fn text(&self) -> String {
        self.output
            .iter()
            .map(|part| match part {
                Part::Text { text } => text.as_str(),
                _ => "",
            })
            .collect()
    }
}

fn started(live: &[AgentEvent]) -> usize {
    live.iter()
        .filter(|event| matches!(event, AgentEvent::ToolStarted { .. }))
        .count()
}

/// Every call has exactly one `ToolStarted`, then exactly one `ToolFinished`.
fn assert_paired(live: &[AgentEvent], calls: usize) {
    let mut open = HashSet::new();
    let mut finished = HashSet::new();
    for event in live {
        match event {
            AgentEvent::ToolStarted { call, .. } => assert!(open.insert(*call), "{live:?}"),
            AgentEvent::ToolFinished { call, .. } => {
                assert!(open.contains(call), "finished before started: {live:?}");
                assert!(finished.insert(*call), "{live:?}");
            }
            _ => {}
        }
    }
    assert_eq!((open.len(), finished.len()), (calls, calls), "{live:?}");
}

fn asked(live: &[AgentEvent]) -> usize {
    live.iter()
        .filter(|event| matches!(event, AgentEvent::ApprovalRequested(_)))
        .count()
}

// --- hooks and policies ---------------------------------------------------

struct RewriteArgs(&'static str, Arc<AtomicUsize>);

impl Hooks for RewriteArgs {
    fn before_tool<'a>(
        &'a self,
        _ctx: &'a ToolCallCtx,
        args: &'a mut ToolArgs,
    ) -> BoxFuture<'a, Result<ToolGate, HookError>> {
        self.1.fetch_add(1, Ordering::SeqCst);
        *args = JsonString::new(self.0);
        Box::pin(async { Ok(ToolGate::Allow) })
    }
}

fn rewrite(args: &'static str) -> (RewriteArgs, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    (RewriteArgs(args, Arc::clone(&count)), count)
}

struct DenyBeforeTool;

impl Hooks for DenyBeforeTool {
    fn before_tool<'a>(
        &'a self,
        _ctx: &'a ToolCallCtx,
        _args: &'a mut ToolArgs,
    ) -> BoxFuture<'a, Result<ToolGate, HookError>> {
        Box::pin(async {
            Ok(ToolGate::Deny {
                reason: Some("blocked".to_owned()),
            })
        })
    }
}

struct Redact;

impl Hooks for Redact {
    fn after_tool<'a>(
        &'a self,
        _ctx: &'a ToolCallCtx,
        result: &'a mut ToolResultView,
    ) -> BoxFuture<'a, Result<(), HookError>> {
        result.output = vec![text("[redacted]")];
        Box::pin(async { Ok(()) })
    }
}

struct FailAfterTool;

impl Hooks for FailAfterTool {
    fn after_tool<'a>(
        &'a self,
        _ctx: &'a ToolCallCtx,
        _result: &'a mut ToolResultView,
    ) -> BoxFuture<'a, Result<(), HookError>> {
        Box::pin(async { Err(HookError::new("after boom")) })
    }
}

/// A fixed verdict that counts how often it was consulted.
struct Verdict(ApprovalVerdict, Arc<AtomicUsize>);

impl ApprovalPolicy for Verdict {
    fn classify(&self, _request: &ApprovalRequest) -> ApprovalVerdict {
        self.1.fetch_add(1, Ordering::SeqCst);
        self.0.clone()
    }
}

fn verdict(verdict: ApprovalVerdict) -> (Verdict, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    (Verdict(verdict, Arc::clone(&count)), count)
}

fn tool_with(
    descriptor: ToolDescriptor,
    invoke: impl Fn() -> BoxFuture<'static, Result<ToolOutput, asterpath_protocol::ToolError>>
    + Send
    + Sync
    + 'static,
) -> Arc<dyn Tool> {
    TestTool::new(descriptor, Arc::default(), move |_args, _ctx| invoke())
}

fn echo(name: &str) -> Arc<dyn Tool> {
    TestTool::echo(descriptor(name)).0
}

// --- before_tool ----------------------------------------------------------

#[tokio::test]
async fn before_tool_rewrites_the_recorded_and_executed_args() {
    let (echo, calls) = TestTool::echo(descriptor("echo"));
    let (hook, _) = rewrite(r#"{"n":2}"#);
    let harness = Harness::new(vec![echo]).await.hook(hook);

    let (outcome, _) = harness
        .run(&[tool_call("call-1", "echo", r#"{"n":1}"#)])
        .await;

    assert_eq!(outcome, ToolOutcome::Done);
    assert_eq!(*calls.lock().unwrap(), [JsonString::new(r#"{"n":2}"#)]);
    assert_eq!(harness.intents().await[0].2, r#"{"n":2}"#);
    assert_eq!(harness.result("call-1").await.text(), r#"{"n":2}"#);
}

#[tokio::test]
async fn before_tool_deny_records_the_intent_and_a_denied_result() {
    let (echo, calls) = TestTool::echo(descriptor("echo"));
    let harness = Harness::new(vec![echo]).await.hook(DenyBeforeTool);

    let (outcome, live) = harness.run(&[tool_call("call-1", "echo", "{}")]).await;

    assert_eq!(outcome, ToolOutcome::Done);
    assert!(calls.lock().unwrap().is_empty());
    assert_eq!(started(&live), 1, "announced, then closed");
    let result = harness.result("call-1").await;
    assert_eq!(result.status, ToolResultStatus::Failed);
    assert_eq!(result.error_kind(), Some(ToolErrorKind::Denied));
    assert_eq!(result.error.unwrap().message, "blocked");
}

// --- registry -------------------------------------------------------------

#[tokio::test]
async fn an_unregistered_tool_is_recorded_unknown_and_not_found() {
    let (hook, before) = rewrite("{}");
    let harness = Harness::new(Vec::new()).await.hook(hook);

    let (outcome, live) = harness.run(&[tool_call("call-1", "ghost", "{}")]).await;

    assert_eq!(outcome, ToolOutcome::Done);
    assert_eq!(before.load(Ordering::SeqCst), 1, "before_tool still runs");
    assert_eq!(started(&live), 1, "announced, then closed");
    assert!(harness.events().await.iter().any(|event| matches!(
        event,
        SessionEvent::ToolCallRequested {
            replay: ReplaySafety::Unsafe,
            provenance: Provenance::Unknown,
            ..
        }
    )));
    assert_eq!(
        harness.result("call-1").await.error_kind(),
        Some(ToolErrorKind::NotFound)
    );
}

// --- approval -------------------------------------------------------------

#[tokio::test]
async fn policy_allow_executes_and_deny_records_the_reason() {
    let (policy, _) = verdict(ApprovalVerdict::Allow);
    let harness = Harness::new(vec![echo("echo")]).await.policy(policy);
    harness.run(&[tool_call("call-1", "echo", "{}")]).await;
    assert_eq!(
        harness.result("call-1").await.status,
        ToolResultStatus::Completed
    );

    let (policy, _) = verdict(ApprovalVerdict::Deny {
        reason: Some("no".to_owned()),
    });
    let harness = Harness::new(vec![echo("echo")]).await.policy(policy);
    let (_, live) = harness.run(&[tool_call("call-1", "echo", "{}")]).await;
    assert_eq!(started(&live), 1, "announced, then closed");
    let result = harness.result("call-1").await;
    assert_eq!(result.error_kind(), Some(ToolErrorKind::Denied));
    assert_eq!(result.error.unwrap().message, "no");
}

#[tokio::test]
async fn ask_with_a_closed_gate_denies_without_asking() {
    let (policy, _) = verdict(ApprovalVerdict::Ask);
    let harness = Harness::new(vec![echo("echo")]).await.policy(policy);

    let (_, live) = harness.run(&[tool_call("call-1", "echo", "{}")]).await;

    assert_eq!(asked(&live), 0);
    let result = harness.result("call-1").await;
    assert_eq!(result.error_kind(), Some(ToolErrorKind::Denied));
    assert_eq!(result.error.unwrap().message, NO_APPROVAL_ENDPOINT);
}

#[tokio::test]
async fn ask_with_an_open_gate_waits_for_the_recorded_decision() {
    let (policy, _) = verdict(ApprovalVerdict::Ask);
    let harness = Harness::new(vec![echo("echo")]).await.policy(policy);
    let _endpoint = harness.gate.register(SESSION);

    let approve = async {
        // The decision arrives once the intent it answers is recorded.
        let call = loop {
            if let Some((call, ..)) = harness.intents().await.into_iter().next() {
                break call;
            }
            tokio::task::yield_now().await;
        };
        harness.deliver(call, ApprovalDecision::Allow).await;
    };
    let calls = [tool_call("call-1", "echo", "{}")];
    let ((outcome, live), ()) = tokio::join!(harness.run(&calls), approve);

    assert_eq!(outcome, ToolOutcome::Done);
    assert_eq!(asked(&live), 1);
    assert_eq!(
        harness.result("call-1").await.status,
        ToolResultStatus::Completed
    );
}

#[tokio::test]
async fn a_recorded_approval_wins_over_the_policy() {
    let (policy, consulted) = verdict(ApprovalVerdict::Allow);
    let harness = Harness::new(vec![echo("echo")]).await.policy(policy);
    harness
        .intent(RECORDED, "call-1", "echo", Provenance::InProcess)
        .await;
    harness
        .deliver(
            RECORDED,
            ApprovalDecision::Deny {
                reason: Some("host said no".to_owned()),
            },
        )
        .await;

    let (_, live) = harness
        .run_with(
            &[tool_call("call-1", "echo", "{}")],
            true,
            &CancelToken::new(),
        )
        .await;

    assert_eq!(consulted.load(Ordering::SeqCst), 0);
    assert_eq!(started(&live), 1, "announced, then closed");
    let result = harness.result("call-1").await;
    assert_eq!(result.error_kind(), Some(ToolErrorKind::Denied));
    assert_eq!(result.error.unwrap().message, "host said no");
}

#[tokio::test]
async fn recovery_never_reruns_an_unsafe_recorded_intent() {
    let (danger, calls) = TestTool::echo(descriptor("danger").with_replay(ReplaySafety::Unsafe));
    let harness = Harness::new(vec![danger]).await;
    harness
        .intent(RECORDED, "call-1", "danger", Provenance::InProcess)
        .await;

    let (outcome, _) = harness
        .run_with(
            &[tool_call("call-1", "danger", "{}")],
            true,
            &CancelToken::new(),
        )
        .await;

    assert_eq!(outcome, ToolOutcome::Done);
    assert!(calls.lock().unwrap().is_empty());
    assert_eq!(
        harness.result("call-1").await.status,
        ToolResultStatus::Interrupted
    );
}

#[tokio::test]
async fn recovery_reuses_a_recorded_intent_without_before_tool() {
    let (echo, calls) = TestTool::echo(descriptor("echo"));
    let (hook, before) = rewrite(r#"{"fresh":true}"#);
    let harness = Harness::new(vec![echo]).await.hook(hook);
    harness
        .intent(RECORDED, "call-1", "echo", Provenance::InProcess)
        .await;

    harness
        .run_with(
            &[
                tool_call("call-1", "echo", r#"{"model":1}"#),
                tool_call("call-2", "echo", r#"{"model":2}"#),
            ],
            true,
            &CancelToken::new(),
        )
        .await;

    assert_eq!(before.load(Ordering::SeqCst), 1, "only the fresh call");
    assert_eq!(
        *calls.lock().unwrap(),
        [JsonString::new("{}"), JsonString::new(r#"{"fresh":true}"#)]
    );
    assert_eq!(harness.result("call-1").await.text(), "{}");
}

// --- invocation -----------------------------------------------------------

#[tokio::test]
async fn a_tool_that_ignores_its_deadline_fails_timeout() {
    let slow = tool_with(
        descriptor("slow").with_timeout(Some(Duration::from_millis(10))),
        || Box::pin(std::future::pending()),
    );
    let harness = Harness::new(vec![slow]).await;

    harness.run(&[tool_call("call-1", "slow", "{}")]).await;

    assert_eq!(
        harness.result("call-1").await.error_kind(),
        Some(ToolErrorKind::Timeout)
    );
}

/// Returns `Cancelled` as soon as its call is cancelled.
fn gives_up_on_cancel(timeout: Duration, turn: Option<CancelToken>) -> Arc<dyn Tool> {
    TestTool::new(
        descriptor("quitter").with_timeout(Some(timeout)),
        Arc::default(),
        move |_args, ctx| {
            let turn = turn.clone();
            Box::pin(async move {
                if let Some(turn) = turn {
                    turn.cancel();
                }
                ctx.cancel.cancelled().await;
                Err(asterpath_protocol::ToolError::Cancelled {
                    message: "gave up after partial work".to_owned(),
                })
            })
        },
    )
}

#[tokio::test]
async fn a_tool_that_reports_cancelled_at_its_deadline_fails_timeout() {
    let mut harness = Harness::new(vec![gives_up_on_cancel(Duration::from_millis(10), None)]).await;
    harness.config.kill_grace = Duration::from_secs(5);

    harness.run(&[tool_call("call-1", "quitter", "{}")]).await;

    let result = harness.result("call-1").await;
    assert_eq!(result.status, ToolResultStatus::Failed);
    let error = result.error.expect("error");
    assert_eq!(error.kind, ToolErrorKind::Timeout);
    assert_eq!(error.message, "gave up after partial work");
}

#[tokio::test]
async fn a_tool_that_reports_cancelled_under_turn_cancel_stays_cancelled() {
    let cancel = CancelToken::new();
    let mut harness = Harness::new(vec![gives_up_on_cancel(
        Duration::from_secs(5),
        Some(cancel.clone()),
    )])
    .await;
    harness.cancel_grace = Duration::from_secs(5);

    harness
        .run_with(&[tool_call("call-1", "quitter", "{}")], false, &cancel)
        .await;

    assert_eq!(
        harness.result("call-1").await.error_kind(),
        Some(ToolErrorKind::Cancelled)
    );
}

#[tokio::test]
async fn a_turn_cancel_during_the_kill_grace_keeps_cancelled() {
    let cancel = CancelToken::new();
    let turn = cancel.clone();
    let quitter = TestTool::new(
        descriptor("quitter").with_timeout(Some(Duration::from_millis(10))),
        Arc::default(),
        move |_args, ctx| {
            let turn = turn.clone();
            Box::pin(async move {
                // Deadline fires first; the turn is cancelled inside the grace.
                ctx.cancel.cancelled().await;
                turn.cancel();
                tokio::task::yield_now().await;
                Err(asterpath_protocol::ToolError::Cancelled {
                    message: "stopped".to_owned(),
                })
            })
        },
    );
    let mut harness = Harness::new(vec![quitter]).await;
    harness.config.kill_grace = Duration::from_secs(5);
    harness.cancel_grace = Duration::from_secs(5);

    harness
        .run_with(&[tool_call("call-1", "quitter", "{}")], false, &cancel)
        .await;

    assert_eq!(
        harness.result("call-1").await.error_kind(),
        Some(ToolErrorKind::Cancelled)
    );
}

#[tokio::test]
async fn a_panicking_tool_fails_alone() {
    let boom = tool_with(descriptor("boom"), || Box::pin(async { panic!("boom") }));
    let harness = Harness::new(vec![boom, echo("ok")]).await;

    let (outcome, _) = harness
        .run(&[
            tool_call("call-1", "boom", "{}"),
            tool_call("call-2", "ok", "{}"),
        ])
        .await;

    assert_eq!(outcome, ToolOutcome::Done);
    assert_eq!(
        harness.result("call-1").await.error_kind(),
        Some(ToolErrorKind::Panic)
    );
    assert_eq!(
        harness.result("call-2").await.status,
        ToolResultStatus::Completed
    );
}

// --- truncation -----------------------------------------------------------

#[tokio::test]
async fn output_over_the_call_limit_is_cut_at_a_char_boundary() {
    let (long, _) = TestTool::text(descriptor("long").with_max_output_bytes(Some(5)), "ééééé");
    let harness = Harness::new(vec![long]).await;

    harness.run(&[tool_call("call-1", "long", "{}")]).await;

    let result = harness.result("call-1").await;
    assert!(result.truncated);
    assert_eq!(
        result.output,
        [text("éé"), text("[output truncated: 6 bytes omitted]")]
    );
}

#[tokio::test]
async fn the_step_budget_truncates_the_call_that_exceeds_it() {
    let (fixed, _) = TestTool::text(descriptor("fixed"), &"a".repeat(100));
    let mut harness = Harness::new(vec![fixed]).await;
    harness.config.max_output_bytes = 1000;
    harness.config.max_output_bytes_per_step = 150;
    // One at a time, so the budget is spent in call order.
    harness.config.max_parallel_calls = std::num::NonZeroUsize::MIN;

    harness
        .run(&[
            tool_call("call-1", "fixed", "{}"),
            tool_call("call-2", "fixed", "{}"),
        ])
        .await;

    let first = harness.result("call-1").await;
    assert!(!first.truncated);
    let second = harness.result("call-2").await;
    assert!(second.truncated);
    assert_eq!(
        second.output,
        [
            text(&"a".repeat(50)),
            text("[output truncated: 50 bytes omitted]")
        ]
    );
}

// --- after_tool -----------------------------------------------------------

#[tokio::test]
async fn after_tool_rewrites_the_logged_content() {
    let harness = Harness::new(vec![echo("echo")]).await.hook(Redact);

    harness.run(&[tool_call("call-1", "echo", "secret")]).await;

    assert_eq!(harness.result("call-1").await.output, [text("[redacted]")]);
}

#[tokio::test]
async fn a_failing_after_tool_fails_the_call_as_internal() {
    let harness = Harness::new(vec![echo("echo")]).await.hook(FailAfterTool);

    harness.run(&[tool_call("call-1", "echo", "{}")]).await;

    let result = harness.result("call-1").await;
    assert_eq!(result.status, ToolResultStatus::Failed);
    assert_eq!(result.error_kind(), Some(ToolErrorKind::Internal));
    assert_eq!(result.error.unwrap().message, "after boom");
    assert!(result.output.is_empty());
}

// --- cancellation ---------------------------------------------------------

#[tokio::test]
async fn cancel_grace_lets_a_real_result_win_and_interrupts_the_rest() {
    let cancel = CancelToken::new();
    let token = cancel.clone();
    // Sees the cancel, then still returns a real result inside the grace.
    let polite = TestTool::new(descriptor("polite"), Arc::default(), move |_args, ctx| {
        let token = token.clone();
        Box::pin(async move {
            token.cancel();
            ctx.cancel.cancelled().await;
            Ok(ToolOutput::new(vec![text("real")]))
        })
    });
    let stuck = tool_with(descriptor("stuck"), || Box::pin(std::future::pending()));
    let mut harness = Harness::new(vec![polite, stuck]).await;
    harness.cancel_grace = Duration::from_millis(50);

    let (outcome, live) = harness
        .run_with(
            &[
                tool_call("call-1", "polite", "{}"),
                tool_call("call-2", "stuck", "{}"),
            ],
            false,
            &cancel,
        )
        .await;

    assert_eq!(outcome, ToolOutcome::Cancelled);
    assert_paired(&live, 2);
    let real = harness.result("call-1").await;
    assert_eq!(
        (real.status, real.text()),
        (ToolResultStatus::Completed, "real".to_owned())
    );
    assert_eq!(
        harness.result("call-2").await.status,
        ToolResultStatus::Interrupted
    );
}

/// A call still queued for a permit when the step is cancelled never ran,
/// and is announced only as it is interrupted.
#[tokio::test]
async fn a_queued_call_cancelled_before_it_starts_is_still_announced() {
    let cancel = CancelToken::new();
    let token = cancel.clone();
    let blocker = TestTool::new(descriptor("blocker"), Arc::default(), move |_args, ctx| {
        let token = token.clone();
        Box::pin(async move {
            token.cancel();
            ctx.cancel.cancelled().await;
            Err(asterpath_protocol::ToolError::Cancelled {
                message: "stopped".to_owned(),
            })
        })
    });
    let (queued, calls) = TestTool::echo(descriptor("queued"));
    let mut harness = Harness::new(vec![blocker, queued]).await;
    harness.config.max_parallel_calls = std::num::NonZeroUsize::MIN;

    let (outcome, live) = harness
        .run_with(
            &[
                tool_call("call-1", "blocker", "{}"),
                tool_call("call-2", "queued", "{}"),
            ],
            false,
            &cancel,
        )
        .await;

    assert_eq!(outcome, ToolOutcome::Cancelled);
    assert!(
        calls.lock().unwrap().is_empty(),
        "the queued call never ran"
    );
    assert_paired(&live, 2);
    assert_eq!(
        harness.result("call-2").await.status,
        ToolResultStatus::Interrupted
    );
}

#[tokio::test]
async fn cancelling_closes_the_child_sessions_this_step_spawned() {
    const CHILD: SessionId = SessionId::from_bytes([7; 16]);
    let cancel = CancelToken::new();
    let harness = Harness::new(vec![echo("spawn")]).await;
    harness
        .intent(RECORDED, "call-1", "spawn", Provenance::InProcess)
        .await;
    append(
        &harness.log,
        SessionEvent::ChildSessionSpawned {
            child: CHILD,
            task: vec![text("task")],
            config: super::support::config(Vec::new()),
            context: ContextPolicy::Fresh,
            parent_tool_call_id: RECORDED,
            caused_by: LogId::from_i64(0),
            deadline: Some(5),
        },
        None,
    )
    .await;
    cancel.cancel();

    let (outcome, _) = harness
        .run_with(&[tool_call("call-1", "spawn", "{}")], true, &cancel)
        .await;

    assert_eq!(outcome, ToolOutcome::Cancelled);
    assert!(harness.events().await.iter().any(|event| matches!(
        event,
        SessionEvent::ChildSessionCompleted {
            child: CHILD,
            outcome: ChildOutcome::Cancelled {
                reason: CancelReason::Parent
            },
        }
    )));
    assert_eq!(
        harness.result("call-1").await.status,
        ToolResultStatus::Interrupted
    );
}

// --- decision 6: the two admission paths check cancellation differently ----

#[tokio::test]
async fn a_fresh_call_records_its_intent_before_it_sees_the_cancel() {
    let (hook, before) = rewrite("{}");
    let (policy, consulted) = verdict(ApprovalVerdict::Allow);
    // An unregistered tool shows the order: Interrupted, not NotFound.
    let harness = Harness::new(Vec::new()).await.hook(hook).policy(policy);
    let cancel = CancelToken::new();
    cancel.cancel();

    let (outcome, live) = harness
        .run_with(&[tool_call("call-1", "ghost", "{}")], false, &cancel)
        .await;

    assert_eq!(outcome, ToolOutcome::Cancelled);
    assert_paired(&live, 1);
    assert_eq!(before.load(Ordering::SeqCst), 1);
    assert_eq!(harness.intents().await.len(), 1);
    assert_eq!(consulted.load(Ordering::SeqCst), 0);
    assert_eq!(
        harness.result("call-1").await.status,
        ToolResultStatus::Interrupted
    );
}

#[tokio::test]
async fn a_recorded_intent_sees_the_cancel_before_approval() {
    let (policy, consulted) = verdict(ApprovalVerdict::Ask);
    let harness = Harness::new(vec![echo("echo")]).await.policy(policy);
    let _endpoint = harness.gate.register(SESSION);
    harness
        .intent(RECORDED, "call-1", "echo", Provenance::InProcess)
        .await;
    harness
        .intent(
            ToolCallId::from_bytes([4; 16]),
            "call-0",
            "ghost",
            Provenance::Unknown,
        )
        .await;
    let cancel = CancelToken::new();
    cancel.cancel();

    let (outcome, live) = harness
        .run_with(
            &[
                tool_call("call-1", "echo", "{}"),
                tool_call("call-0", "ghost", "{}"),
            ],
            true,
            &cancel,
        )
        .await;

    assert_eq!(outcome, ToolOutcome::Cancelled);
    assert_eq!(consulted.load(Ordering::SeqCst), 0);
    assert_eq!(asked(&live), 0);
    assert_eq!(
        harness.result("call-1").await.status,
        ToolResultStatus::Interrupted
    );
    // A recorded `Unknown` intent is closed before the cancel check.
    assert_eq!(
        harness.result("call-0").await.error_kind(),
        Some(ToolErrorKind::NotFound)
    );
}
