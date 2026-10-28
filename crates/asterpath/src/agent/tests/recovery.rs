//! Recovery from a committed prefix: open intents only, no full replay.
//!
//! Each test fabricates the exact log a `SIGKILL` would leave behind, opens a
//! fresh agent over that log and asserts the terminal records it appends.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::support::{
    self, SESSION, append, config, descriptor, drive, model, scan, text, tool_call, tool_table,
};
use crate::agent::Agent;
use crate::{Asterpath, AsterpathConfig};
use asterpath_protocol::{
    AbortKind, AgentError, AgentEvent, ApprovalDecision, ApprovalPolicy, ApprovalRequest,
    ApprovalVerdict, CancelReason, ChildOutcome, ConfigSnapshot, ContextPolicy,
    DefaultApprovalPolicy, HookError, Hooks, Item, JsonString, ParentRef, Provenance, ReplaySafety,
    Role, RunRequest, RunTrigger, SessionEvent, StopReason, Tool, ToolArgs, ToolCallCtx,
    ToolDescriptor, ToolGate, ToolName, ToolResultStatus, TurnEndReason, Usage, decode_entry,
    dedup, encode_entry,
};
use asterpath_protocol::{
    ApprovalGate, CatalogEntry, DedupKey, InputId, LogEntry, LogId, LogQuery, SessionId,
    SessionQuery, SessionStore, StepId, StorageError, ToolCallId, TurnId,
};
use asterpath_testing::TestTool;
use asterpath_testing::{InMemoryEventLog, ScriptedProvider, ToolCalls};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;

fn assistant(call_id: &str, name: &str) -> SessionEvent {
    SessionEvent::AssistantMessage {
        turn_id: TURN,
        step_id: STEP,
        items: vec![tool_call(call_id, name, "{}")],
        raw: None,
        model: model(),
        usage: Usage::default(),
        stop_reason: StopReason::ToolUse,
    }
}

const TURN: TurnId = TurnId::from_bytes([1; 16]);
const STEP: StepId = StepId::from_bytes([2; 16]);
const CALL: ToolCallId = ToolCallId::from_bytes([3; 16]);

async fn seed(config: ConfigSnapshot) -> Arc<InMemoryEventLog> {
    let log = Arc::new(InMemoryEventLog::new());
    append(
        &log,
        SessionEvent::SessionCreated {
            config,
            parent: None,
            metadata: Default::default(),
        },
        Some(dedup::session_created(SESSION)),
    )
    .await;
    log
}

async fn open_turn(log: &Arc<InMemoryEventLog>) {
    append(log, SessionEvent::TurnStarted { turn_id: TURN }, None).await;
    append(
        log,
        SessionEvent::StepStarted {
            turn_id: TURN,
            step_id: STEP,
        },
        None,
    )
    .await;
}

fn intent(
    call: ToolCallId,
    provider_call_id: &str,
    name: &str,
    replay: ReplaySafety,
) -> SessionEvent {
    SessionEvent::ToolCallRequested {
        turn_id: TURN,
        step_id: STEP,
        tool_call_id: call,
        provider_call_id: provider_call_id.to_owned(),
        name: ToolName::new(name),
        raw_args: JsonString::new("{}"),
        replay,
        provenance: Provenance::InProcess,
    }
}

fn echo(name: &str, replay: ReplaySafety) -> (Arc<dyn Tool>, ToolCalls) {
    let (tool, calls) = TestTool::text(described(name, replay), "tool done");
    (tool, calls)
}
struct AlwaysAsk;

impl ApprovalPolicy for AlwaysAsk {
    fn classify(&self, _request: &ApprovalRequest) -> ApprovalVerdict {
        ApprovalVerdict::Ask
    }
}

fn described(name: &str, replay: ReplaySafety) -> ToolDescriptor {
    descriptor(name).with_replay(replay)
}

/// The engine with the default tool config and an optional hook.
fn engine<L: SessionStore + 'static>(
    log: Arc<L>,
    provider: Arc<ScriptedProvider>,
    tools: Vec<Arc<dyn Tool>>,
    approval: Arc<dyn ApprovalPolicy>,
    gate: Arc<ApprovalGate>,
    hooks: Option<Arc<dyn Hooks>>,
) -> Asterpath {
    let config = AsterpathConfig {
        approval,
        gate,
        hooks: hooks.into_iter().collect(),
        ..AsterpathConfig::default()
    };
    support::engine(log, provider, tool_table(tools), config)
}

fn agent_with<L: SessionStore + 'static>(
    log: Arc<L>,
    provider: Arc<ScriptedProvider>,
    tools: Vec<Arc<dyn Tool>>,
    approval: Arc<dyn ApprovalPolicy>,
    gate: Arc<ApprovalGate>,
    hooks: Option<Arc<dyn Hooks>>,
) -> Arc<Agent> {
    Arc::clone(engine(log, provider, tools, approval, gate, hooks).agent())
}

struct Harness {
    agent: Arc<Agent>,
}

fn harness(
    log: Arc<InMemoryEventLog>,
    provider: Arc<ScriptedProvider>,
    tools: Vec<Arc<dyn Tool>>,
) -> Harness {
    let gate = Arc::new(ApprovalGate::new());
    let agent = agent_with(
        Arc::clone(&log),
        Arc::clone(&provider),
        tools,
        Arc::new(DefaultApprovalPolicy),
        gate,
        None,
    );
    Harness { agent }
}

fn events(entries: &[LogEntry]) -> Vec<SessionEvent> {
    entries
        .iter()
        .map(|entry| decode_entry(entry).expect("decodes"))
        .collect()
}

fn kinds(entries: &[LogEntry]) -> Vec<&'static str> {
    events(entries).iter().map(SessionEvent::kind).collect()
}

fn count(entries: &[LogEntry], kind: &str) -> usize {
    kinds(entries)
        .iter()
        .filter(|value| **value == kind)
        .count()
}

fn results(entries: &[LogEntry]) -> Vec<(ToolCallId, ToolResultStatus, Option<&'static str>)> {
    events(entries)
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::ToolResult {
                tool_call_id,
                status,
                error,
                ..
            } => Some((
                tool_call_id,
                status,
                error.map(|error| match error.kind {
                    asterpath_protocol::ToolErrorKind::NotFound => "NotFound",
                    asterpath_protocol::ToolErrorKind::Denied => "Denied",
                    _ => "other",
                }),
            )),
            _ => None,
        })
        .collect()
}

fn turn_reasons(entries: &[LogEntry]) -> Vec<TurnEndReason> {
    events(entries)
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::TurnCommitted { reason, .. } => Some(reason),
            _ => None,
        })
        .collect()
}

fn aborts(entries: &[LogEntry]) -> Vec<(u32, AbortKind)> {
    events(entries)
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::ModelCallAborted { attempt, kind, .. } => Some((attempt, kind)),
            _ => None,
        })
        .collect()
}

fn completed() -> Vec<Item> {
    vec![Item::Message {
        role: Role::Assistant,
        parts: vec![text("done")],
        raw: None,
    }]
}

// --- S10 / L11: AssistantMessage committed, step and tools open -------------

#[tokio::test]
async fn assistant_message_prefix_skips_the_model_and_finishes_a_safe_tool() {
    let (tool, calls) = echo("echo", ReplaySafety::Safe);
    let log = seed(config(vec![described("echo", ReplaySafety::Safe)])).await;
    open_turn(&log).await;
    append(
        &log,
        assistant("c1", "echo"),
        Some(dedup::assistant_message(TURN, STEP)),
    )
    .await;
    append(
        &log,
        intent(CALL, "c1", "echo", ReplaySafety::Safe),
        Some(dedup::tool_call(CALL)),
    )
    .await;

    let provider = Arc::new(ScriptedProvider::new("p").respond(
        completed(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let harness = harness(Arc::clone(&log), Arc::clone(&provider), vec![tool]);
    let events = drive(&harness.agent).await;
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::ToolFinished {
            status: ToolResultStatus::Completed,
            ..
        }
    )));

    assert_eq!(calls.lock().unwrap().len(), 1, "the safe tool runs once");
    let entries = scan(&log).await;
    assert_eq!(count(&entries, "StreamResumed"), 1);
    assert_eq!(
        count(&entries, "AssistantMessage"),
        2,
        "the committed message is reused; only the next step calls the model"
    );
    assert_eq!(
        count(&entries, "StepCommitted"),
        2,
        "resumed step + next step"
    );
    assert_eq!(results(&entries)[0].1, ToolResultStatus::Completed);
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Completed]);
    assert_eq!(
        provider.requests().len(),
        1,
        "only the following step calls the model"
    );
}

#[tokio::test]
async fn assistant_message_prefix_does_not_rerun_an_unsafe_tool() {
    let (tool, calls) = echo("side_effect", ReplaySafety::Unsafe);
    let log = seed(config(vec![described("side_effect", ReplaySafety::Unsafe)])).await;
    open_turn(&log).await;
    append(
        &log,
        assistant("c1", "side_effect"),
        Some(dedup::assistant_message(TURN, STEP)),
    )
    .await;
    append(
        &log,
        intent(CALL, "c1", "side_effect", ReplaySafety::Unsafe),
        Some(dedup::tool_call(CALL)),
    )
    .await;

    let provider = Arc::new(ScriptedProvider::new("p").respond(
        completed(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let harness = harness(Arc::clone(&log), Arc::clone(&provider), vec![tool]);
    let _ = drive(&harness.agent).await;

    assert!(calls.lock().unwrap().is_empty(), "unsafe tools never rerun");
    let entries = scan(&log).await;
    assert_eq!(
        results(&entries),
        vec![(CALL, ToolResultStatus::Interrupted, None)]
    );
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Completed]);
}

#[tokio::test]
async fn usage_survives_the_recovered_step() {
    let log = seed(config(Vec::new())).await;
    open_turn(&log).await;
    let usage = Usage {
        input: asterpath_protocol::InputTokens {
            uncached: 11,
            cache_read: 2,
            cache_write: 0,
            cache_write_1h: 0,
        },
        output: asterpath_protocol::OutputTokens {
            total: 7,
            reasoning: 0,
        },
    };
    append(
        &log,
        SessionEvent::AssistantMessage {
            turn_id: TURN,
            step_id: STEP,
            items: vec![Item::Message {
                role: Role::Assistant,
                parts: vec![text("answer")],
                raw: None,
            }],
            raw: None,
            model: model(),
            usage,
            stop_reason: StopReason::EndTurn,
        },
        Some(dedup::assistant_message(TURN, STEP)),
    )
    .await;

    let provider = Arc::new(ScriptedProvider::new("p"));
    let harness = harness(Arc::clone(&log), Arc::clone(&provider), Vec::new());
    let _ = drive(&harness.agent).await;

    let entries = scan(&log).await;
    let usage_after = events(&entries)
        .into_iter()
        .find_map(|event| match event {
            SessionEvent::AssistantMessage { usage, .. } => Some(usage),
            _ => None,
        })
        .expect("assistant message");
    assert_eq!(usage_after, usage);
    assert!(provider.requests().is_empty());
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Completed]);
}

// --- S3: model stream crash before any attempt terminal --------------------

#[tokio::test]
async fn step_without_an_abort_gets_a_crash_abort_and_retries() {
    let log = seed(config(Vec::new())).await;
    open_turn(&log).await;
    // A crash during the stream leaves no abort and no AssistantMessage.

    let provider = Arc::new(ScriptedProvider::new("p").respond(
        completed(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let harness = harness(Arc::clone(&log), Arc::clone(&provider), Vec::new());
    let _ = drive(&harness.agent).await;

    let entries = scan(&log).await;
    assert_eq!(aborts(&entries), vec![(1, AbortKind::Crash)]);
    assert_eq!(count(&entries, "AssistantMessage"), 1);
    assert_eq!(count(&entries, "StepCommitted"), 1);
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Completed]);
    assert_eq!(count(&entries, "StreamResumed"), 1);
    assert_eq!(
        provider.requests().len(),
        1,
        "the same step is retried once"
    );
}

#[tokio::test]
async fn transport_abort_retries_without_adding_a_crash_abort() {
    let log = seed(config(Vec::new())).await;
    open_turn(&log).await;
    append(
        &log,
        SessionEvent::ModelCallAborted {
            turn_id: TURN,
            step_id: STEP,
            attempt: 1,
            kind: AbortKind::Transport,
            partial: None,
            model: model(),
            backoff_ms: 0,
            usage: None,
            error: None,
        },
        None,
    )
    .await;

    let provider = Arc::new(ScriptedProvider::new("p").respond(
        completed(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let harness = harness(Arc::clone(&log), Arc::clone(&provider), Vec::new());
    let _ = drive(&harness.agent).await;

    let entries = scan(&log).await;
    assert_eq!(aborts(&entries), vec![(1, AbortKind::Transport)]);
    assert_eq!(count(&entries, "AssistantMessage"), 1);
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Completed]);
}

#[tokio::test]
async fn recovered_transport_exhaustion_fails_without_another_call() {
    let log = seed(config(Vec::new())).await;
    open_turn(&log).await;
    // The last committed attempt already consumed every retry.
    let max = config(Vec::new()).turn.max_attempts.get();
    append(
        &log,
        SessionEvent::ModelCallAborted {
            turn_id: TURN,
            step_id: STEP,
            attempt: max,
            kind: AbortKind::Transport,
            partial: None,
            model: model(),
            backoff_ms: 0,
            usage: None,
            error: None,
        },
        None,
    )
    .await;

    let provider = Arc::new(ScriptedProvider::new("p").respond(
        completed(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let harness = harness(Arc::clone(&log), Arc::clone(&provider), Vec::new());
    let _ = drive(&harness.agent).await;

    let entries = scan(&log).await;
    assert!(
        provider.requests().is_empty(),
        "exhausted recovery never calls the model again"
    );
    assert_eq!(aborts(&entries), vec![(max, AbortKind::Transport)]);
    assert_eq!(count(&entries, "StepCommitted"), 1);
    assert_eq!(count(&entries, "AssistantMessage"), 0);
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Failed]);
}

#[tokio::test]
async fn recovered_forced_compaction_retries_the_same_step() {
    let log = seed(config(Vec::new())).await;
    open_turn(&log).await;
    append(
        &log,
        SessionEvent::ModelCallAborted {
            turn_id: TURN,
            step_id: STEP,
            attempt: 1,
            kind: AbortKind::Rejected,
            partial: None,
            model: model(),
            backoff_ms: 0,
            usage: None,
            error: None,
        },
        None,
    )
    .await;
    // The forced compaction landed before the crash.
    let caused_by = LogId::from_i64(4);
    append(
        &log,
        SessionEvent::CompactionStarted {
            trigger: asterpath_protocol::CompactionTrigger::ProviderError,
            replaces_from: LogId::from_i64(1),
            caused_by,
            turn: Some(TURN),
        },
        Some(dedup::compaction(caused_by)),
    )
    .await;
    append(
        &log,
        SessionEvent::CompactionApplied {
            first_kept: LogId::from_i64(2),
            summary: vec![text("compacted")],
            replaces: asterpath_protocol::LogIdRange {
                first: LogId::from_i64(1),
                last: LogId::from_i64(2),
            },
            model: model(),
            usage: Usage::default(),
            raw: None,
        },
        None,
    )
    .await;

    let provider = Arc::new(ScriptedProvider::new("p").respond(
        completed(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let harness = harness(Arc::clone(&log), Arc::clone(&provider), Vec::new());
    let _ = drive(&harness.agent).await;

    let entries = scan(&log).await;
    assert_eq!(
        provider.requests().len(),
        1,
        "the same step is retried once, without a second compaction"
    );
    assert_eq!(count(&entries, "CompactionApplied"), 1);
    assert_eq!(count(&entries, "AssistantMessage"), 1);
    assert_eq!(count(&entries, "StepStarted"), 1);
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Completed]);
}

#[tokio::test]
async fn cancelled_abort_finishes_the_turn_without_calling_the_model() {
    let log = seed(config(Vec::new())).await;
    open_turn(&log).await;
    append(
        &log,
        SessionEvent::ModelCallAborted {
            turn_id: TURN,
            step_id: STEP,
            attempt: 1,
            kind: AbortKind::Cancelled,
            partial: None,
            model: model(),
            backoff_ms: 0,
            usage: None,
            error: None,
        },
        None,
    )
    .await;

    let provider = Arc::new(ScriptedProvider::new("p"));
    let harness = harness(Arc::clone(&log), Arc::clone(&provider), Vec::new());
    let _ = drive(&harness.agent).await;

    let entries = scan(&log).await;
    assert_eq!(aborts(&entries), vec![(1, AbortKind::Cancelled)]);
    assert_eq!(count(&entries, "StepCommitted"), 1);
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Cancelled]);
    assert!(provider.requests().is_empty());
    assert_eq!(count(&entries, "StreamResumed"), 1);
}

#[tokio::test]
async fn rejected_abort_finishes_the_turn_failed() {
    let log = seed(config(Vec::new())).await;
    open_turn(&log).await;
    append(
        &log,
        SessionEvent::ModelCallAborted {
            turn_id: TURN,
            step_id: STEP,
            attempt: 2,
            kind: AbortKind::Rejected,
            partial: None,
            model: model(),
            backoff_ms: 0,
            usage: None,
            error: None,
        },
        None,
    )
    .await;

    let provider = Arc::new(ScriptedProvider::new("p"));
    let harness = harness(Arc::clone(&log), Arc::clone(&provider), Vec::new());
    let _ = drive(&harness.agent).await;

    let entries = scan(&log).await;
    assert_eq!(count(&entries, "StepCommitted"), 1);
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Failed]);
    assert!(provider.requests().is_empty());
}

// --- Idle and StreamResumed boundaries -------------------------------------

#[tokio::test]
async fn idle_session_writes_no_resume_record() {
    let log = seed(config(Vec::new())).await;
    let provider = Arc::new(ScriptedProvider::new("p"));
    let harness = harness(Arc::clone(&log), Arc::clone(&provider), Vec::new());
    let events = drive(&harness.agent).await;
    assert!(matches!(events.as_slice(), [AgentEvent::Idle { .. }]));
    assert_eq!(count(&scan(&log).await, "StreamResumed"), 0);
}

#[tokio::test]
async fn stream_resumed_points_at_the_last_committed_record() {
    let log = seed(config(Vec::new())).await;
    open_turn(&log).await;
    // AssistantMessage with no tool calls only needs the step terminal.
    append(
        &log,
        SessionEvent::AssistantMessage {
            turn_id: TURN,
            step_id: STEP,
            items: completed(),
            raw: None,
            model: model(),
            usage: Usage::default(),
            stop_reason: StopReason::EndTurn,
        },
        Some(dedup::assistant_message(TURN, STEP)),
    )
    .await;
    let frontier = log.frontier(SESSION).await.unwrap().unwrap();

    let provider = Arc::new(ScriptedProvider::new("p"));
    let harness = harness(Arc::clone(&log), Arc::clone(&provider), Vec::new());
    let _ = drive(&harness.agent).await;

    let entries = scan(&log).await;
    let resumed = events(&entries)
        .into_iter()
        .find_map(|event| match event {
            SessionEvent::StreamResumed { from, .. } => Some(from),
            _ => None,
        })
        .expect("resume record");
    assert_eq!(resumed, frontier);
}

// --- S17 / T14: unknown tool intent ---------------------------------------

#[tokio::test]
async fn unknown_tool_intent_recovers_as_not_found_without_approval_or_rerun() {
    let log = seed(config(Vec::new())).await;
    open_turn(&log).await;
    append(
        &log,
        assistant("c1", "ghost"),
        Some(dedup::assistant_message(TURN, STEP)),
    )
    .await;
    append(
        &log,
        SessionEvent::ToolCallRequested {
            turn_id: TURN,
            step_id: STEP,
            tool_call_id: CALL,
            provider_call_id: "c1".to_owned(),
            name: ToolName::new("ghost"),
            raw_args: JsonString::new("{}"),
            replay: ReplaySafety::Unsafe,
            provenance: Provenance::Unknown,
        },
        Some(dedup::tool_call(CALL)),
    )
    .await;

    let gate = Arc::new(ApprovalGate::new());
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        completed(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let agent = agent_with(
        Arc::clone(&log),
        Arc::clone(&provider),
        Vec::new(),
        Arc::new(AlwaysAsk),
        Arc::clone(&gate),
        None,
    );
    let events = drive(&agent).await;

    assert!(
        !events
            .iter()
            .any(|event| matches!(event, AgentEvent::ApprovalRequested(_))),
        "unknown provenance never enters approval"
    );
    let entries = scan(&log).await;
    assert_eq!(
        results(&entries),
        vec![(CALL, ToolResultStatus::Failed, Some("NotFound"))]
    );
    assert_eq!(count(&entries, "ApprovalDecided"), 0);
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Completed]);
}

// --- S15: batch intent window ---------------------------------------------

#[tokio::test]
async fn missing_batch_intent_is_filled_by_provider_call_id_without_crossing() {
    let (alpha, alpha_calls) = echo("alpha", ReplaySafety::Safe);
    let (beta, beta_calls) = echo("beta", ReplaySafety::Safe);
    let log = seed(config(vec![
        described("alpha", ReplaySafety::Safe),
        described("beta", ReplaySafety::Safe),
    ]))
    .await;
    open_turn(&log).await;
    let items = vec![
        tool_call("c1", "alpha", "{}"),
        tool_call("c2", "beta", "{}"),
    ];
    append(
        &log,
        SessionEvent::AssistantMessage {
            turn_id: TURN,
            step_id: STEP,
            items,
            raw: None,
            model: model(),
            usage: Usage::default(),
            stop_reason: StopReason::ToolUse,
        },
        Some(dedup::assistant_message(TURN, STEP)),
    )
    .await;
    let first = ToolCallId::from_bytes([10; 16]);
    append(
        &log,
        intent(first, "c1", "alpha", ReplaySafety::Safe),
        Some(dedup::tool_call(first)),
    )
    .await;

    let provider = Arc::new(ScriptedProvider::new("p").respond(
        completed(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let harness = harness(Arc::clone(&log), Arc::clone(&provider), vec![alpha, beta]);
    let _ = drive(&harness.agent).await;

    assert_eq!(alpha_calls.lock().unwrap().len(), 1);
    assert_eq!(beta_calls.lock().unwrap().len(), 1);

    let entries = scan(&log).await;
    let intents: Vec<_> = events(&entries)
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::ToolCallRequested {
                tool_call_id,
                provider_call_id,
                name,
                ..
            } => Some((tool_call_id, provider_call_id, name.to_string())),
            _ => None,
        })
        .collect();
    assert_eq!(intents.len(), 2, "one intent per call, no duplicates");
    assert_eq!(intents[0], (first, "c1".to_owned(), "alpha".to_owned()));
    let second = intents
        .iter()
        .find(|(call, _, _)| *call != first)
        .expect("filled sibling");
    assert_eq!(second.1, "c2");
    assert_eq!(second.2, "beta");

    let results = results(&entries);
    assert_eq!(results.len(), 2);
    assert!(
        results
            .iter()
            .all(|(_, status, _)| *status == ToolResultStatus::Completed)
    );
    assert_eq!(
        results.iter().map(|(call, _, _)| *call).collect::<Vec<_>>(),
        vec![first, second.0],
        "each result binds its own call, in intent order"
    );
}

// --- S16: pre-execute window ----------------------------------------------

struct RewriteAndCount {
    count: Arc<AtomicUsize>,
}

impl Hooks for RewriteAndCount {
    fn before_tool<'a>(
        &'a self,
        _ctx: &'a ToolCallCtx,
        args: &'a mut ToolArgs,
    ) -> BoxFuture<'a, Result<ToolGate, HookError>> {
        let hit = self.count.fetch_add(1, Ordering::SeqCst);
        *args = JsonString::new(format!(r#"{{"pre":{hit}}}"#));
        Box::pin(async { Ok(ToolGate::Allow) })
    }
}

#[tokio::test]
async fn pre_execute_runs_only_for_calls_without_recorded_intents() {
    let (alpha, alpha_calls) = echo("alpha", ReplaySafety::Safe);
    let (beta, beta_calls) = echo("beta", ReplaySafety::Safe);
    let log = seed(config(vec![
        described("alpha", ReplaySafety::Safe),
        described("beta", ReplaySafety::Safe),
    ]))
    .await;
    open_turn(&log).await;
    append(
        &log,
        SessionEvent::AssistantMessage {
            turn_id: TURN,
            step_id: STEP,
            items: vec![
                tool_call("c1", "alpha", "{}"),
                tool_call("c2", "beta", "{}"),
            ],
            raw: None,
            model: model(),
            usage: Usage::default(),
            stop_reason: StopReason::ToolUse,
        },
        Some(dedup::assistant_message(TURN, STEP)),
    )
    .await;
    // The recorded intent keeps its effective args; pre-execute must not rerun.
    let first = ToolCallId::from_bytes([10; 16]);
    append(
        &log,
        SessionEvent::ToolCallRequested {
            turn_id: TURN,
            step_id: STEP,
            tool_call_id: first,
            provider_call_id: "c1".to_owned(),
            name: ToolName::new("alpha"),
            raw_args: JsonString::new(r#"{"recorded":true}"#),
            replay: ReplaySafety::Safe,
            provenance: Provenance::InProcess,
        },
        Some(dedup::tool_call(first)),
    )
    .await;

    let count = Arc::new(AtomicUsize::new(0));
    let middleware_count = Arc::clone(&count);
    let hooks: Arc<dyn Hooks> = Arc::new(RewriteAndCount {
        count: Arc::clone(&middleware_count),
    });

    let gate = Arc::new(ApprovalGate::new());
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        completed(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let agent = agent_with(
        Arc::clone(&log),
        Arc::clone(&provider),
        vec![alpha, beta],
        Arc::new(DefaultApprovalPolicy),
        gate,
        Some(hooks),
    );
    let _ = drive(&agent).await;

    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "only the call without an intent"
    );
    let alpha_args = alpha_calls.lock().unwrap().clone();
    assert_eq!(
        alpha_args[0].as_str(),
        r#"{"recorded":true}"#,
        "recorded effective args are authoritative"
    );
    let beta_args = beta_calls.lock().unwrap().clone();
    assert_eq!(
        beta_args[0].as_str(),
        r#"{"pre":0}"#,
        "missing intent reruns pre-execute"
    );
}

// --- T3 / T4: approval across a crash -------------------------------------

#[tokio::test]
async fn recorded_allow_executes_without_reprompting_on_recovery() {
    let (tool, calls) = echo("guarded", ReplaySafety::Unsafe);
    let log = seed(config(vec![described("guarded", ReplaySafety::Unsafe)])).await;
    open_turn(&log).await;
    append(
        &log,
        assistant("c1", "guarded"),
        Some(dedup::assistant_message(TURN, STEP)),
    )
    .await;
    append(
        &log,
        SessionEvent::ToolCallRequested {
            turn_id: TURN,
            step_id: STEP,
            tool_call_id: CALL,
            provider_call_id: "c1".to_owned(),
            name: ToolName::new("guarded"),
            raw_args: JsonString::new("{}"),
            replay: ReplaySafety::Unsafe,
            provenance: Provenance::External {
                transport: asterpath_protocol::TransportKind::Stdio,
            },
        },
        Some(dedup::tool_call(CALL)),
    )
    .await;
    append(
        &log,
        SessionEvent::ApprovalDecided {
            tool_call_id: CALL,
            decision: ApprovalDecision::Allow,
        },
        Some(dedup::approval_decided(CALL)),
    )
    .await;

    let gate = Arc::new(ApprovalGate::new());
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        completed(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let agent = agent_with(
        Arc::clone(&log),
        Arc::clone(&provider),
        vec![tool],
        Arc::new(AlwaysAsk),
        gate,
        None,
    );
    let events = drive(&agent).await;

    assert!(
        !events
            .iter()
            .any(|event| matches!(event, AgentEvent::ApprovalRequested(_)))
    );
    assert_eq!(
        calls.lock().unwrap().len(),
        1,
        "a recorded allow executes an unsafe tool"
    );
    let entries = scan(&log).await;
    assert_eq!(results(&entries)[0].1, ToolResultStatus::Completed);
}

#[tokio::test]
async fn undecided_ask_resuspends_then_executes_after_delivery() {
    let (tool, calls) = echo("guarded", ReplaySafety::Safe);
    let log = seed(config(vec![described("guarded", ReplaySafety::Safe)])).await;
    open_turn(&log).await;
    append(
        &log,
        assistant("c1", "guarded"),
        Some(dedup::assistant_message(TURN, STEP)),
    )
    .await;
    append(
        &log,
        SessionEvent::ToolCallRequested {
            turn_id: TURN,
            step_id: STEP,
            tool_call_id: CALL,
            provider_call_id: "c1".to_owned(),
            name: ToolName::new("guarded"),
            raw_args: JsonString::new("{}"),
            replay: ReplaySafety::Safe,
            provenance: Provenance::External {
                transport: asterpath_protocol::TransportKind::Stdio,
            },
        },
        Some(dedup::tool_call(CALL)),
    )
    .await;

    let gate = Arc::new(ApprovalGate::new());
    let _endpoint = gate.register(SESSION);
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        completed(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let engine = engine(
        Arc::clone(&log),
        Arc::clone(&provider),
        vec![tool],
        Arc::new(AlwaysAsk),
        Arc::clone(&gate),
        None,
    );
    let (agent, waits) = (engine.agent(), engine.waits());

    let mut stream = agent.run(RunRequest {
        session: SESSION,
        trigger: RunTrigger::Resume,
    });
    loop {
        let event = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("approval requested")
            .expect("event")
            .expect("no error");
        if matches!(event, AgentEvent::ApprovalRequested(_)) {
            break;
        }
    }
    assert!(calls.lock().unwrap().is_empty());
    assert_eq!(
        results(&scan(&log).await).len(),
        0,
        "no terminal while waiting"
    );

    waits
        .record_approval(SESSION, CALL, ApprovalDecision::Allow)
        .await
        .expect("record");
    while let Some(event) = stream.next().await {
        event.expect("drive");
    }

    assert_eq!(calls.lock().unwrap().len(), 1);
    let entries = scan(&log).await;
    assert_eq!(results(&entries)[0].1, ToolResultStatus::Completed);
}

#[tokio::test]
async fn undecided_ask_with_a_closed_gate_denies() {
    let (tool, calls) = echo("guarded", ReplaySafety::Safe);
    let log = seed(config(vec![described("guarded", ReplaySafety::Safe)])).await;
    open_turn(&log).await;
    append(
        &log,
        assistant("c1", "guarded"),
        Some(dedup::assistant_message(TURN, STEP)),
    )
    .await;
    append(
        &log,
        intent(CALL, "c1", "guarded", ReplaySafety::Safe),
        Some(dedup::tool_call(CALL)),
    )
    .await;

    let gate = Arc::new(ApprovalGate::new());
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        completed(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let agent = agent_with(
        Arc::clone(&log),
        Arc::clone(&provider),
        vec![tool],
        Arc::new(AlwaysAsk),
        gate,
        None,
    );
    let _ = drive(&agent).await;

    assert!(calls.lock().unwrap().is_empty());
    let entries = scan(&log).await;
    assert_eq!(
        results(&entries),
        vec![(CALL, ToolResultStatus::Failed, Some("Denied"))]
    );
}

// --- waits ----------------------------------------------------------------

#[tokio::test]
async fn unclosed_compaction_keeps_the_session_idle_and_untouched() {
    let log = seed(config(Vec::new())).await;
    let frontier = log.frontier(SESSION).await.unwrap().unwrap();
    append(
        &log,
        SessionEvent::CompactionStarted {
            trigger: asterpath_protocol::CompactionTrigger::Manual,
            replaces_from: frontier,
            caused_by: frontier,
            turn: None,
        },
        Some(dedup::compaction(frontier)),
    )
    .await;

    let provider = Arc::new(ScriptedProvider::new("p"));
    let harness = harness(Arc::clone(&log), Arc::clone(&provider), Vec::new());
    let events = drive(&harness.agent).await;
    assert!(matches!(events.as_slice(), [AgentEvent::Idle { .. }]));

    let entries = scan(&log).await;
    assert_eq!(count(&entries, "CompactionApplied"), 0);
    assert_eq!(count(&entries, "CompactionAborted"), 0);
    assert_eq!(count(&entries, "StreamResumed"), 0);
    let intents = asterpath_protocol::SessionFacts::fold(&entries).expect("fold");
    assert!(intents.is_idle());
    assert!(intents.open_compactions().next().is_some());
}

#[tokio::test]
async fn no_tool_call_after_assistant_message_commits_without_model_or_rerun() {
    // Sanity: an open step whose tools are all terminal simply commits.
    let log = seed(config(Vec::new())).await;
    open_turn(&log).await;
    append(
        &log,
        SessionEvent::AssistantMessage {
            turn_id: TURN,
            step_id: STEP,
            items: completed(),
            raw: None,
            model: model(),
            usage: Usage::default(),
            stop_reason: StopReason::EndTurn,
        },
        Some(dedup::assistant_message(TURN, STEP)),
    )
    .await;

    let provider = Arc::new(ScriptedProvider::new("p"));
    let harness = harness(Arc::clone(&log), Arc::clone(&provider), Vec::new());
    let _ = drive(&harness.agent).await;
    let entries = scan(&log).await;
    assert_eq!(count(&entries, "StepCommitted"), 1);
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Completed]);
    assert!(provider.requests().is_empty());
}

// --- #49: fork first request and R4 orphan recovery -----------------------

const PARENT: SessionId = SessionId::from_bytes([8; 16]);
const GRANDCHILD: SessionId = SessionId::from_bytes([7; 16]);
const CHILD_INPUT: InputId = InputId::from_bytes([0x44; 16]);

fn parent_ref() -> ParentRef {
    ParentRef {
        session: PARENT,
        turn: TURN,
        tool_call: CALL,
    }
}

async fn append_to(
    log: &Arc<InMemoryEventLog>,
    session: SessionId,
    event: SessionEvent,
    key: Option<DedupKey>,
) -> LogId {
    let entry = encode_entry(session, key, &event).expect("encode");
    log.append(entry).await.expect("append")
}

async fn scan_session(log: &Arc<InMemoryEventLog>, session: SessionId) -> Vec<LogEntry> {
    let mut stream = log.scan(LogQuery {
        session,
        after: None,
        limit: None,
    });
    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await {
        entries.push(entry.expect("scan"));
    }
    entries
}

async fn seed_child(log: &Arc<InMemoryEventLog>, parent: Option<ParentRef>) {
    append_to(
        log,
        SESSION,
        SessionEvent::SessionCreated {
            config: config(Vec::new()),
            parent,
            metadata: Default::default(),
        },
        Some(dedup::session_created(SESSION)),
    )
    .await;
}

async fn seed_parent_waiting(log: &Arc<InMemoryEventLog>) {
    append_to(
        log,
        PARENT,
        SessionEvent::SessionCreated {
            config: config(Vec::new()),
            parent: None,
            metadata: Default::default(),
        },
        Some(dedup::session_created(PARENT)),
    )
    .await;
    append_to(
        log,
        PARENT,
        SessionEvent::ChildSessionSpawned {
            child: SESSION,
            task: vec![text("do the task")],
            config: config(Vec::new()),
            context: ContextPolicy::Fresh,
            parent_tool_call_id: CALL,
            caused_by: LogId::from_i64(1),
            deadline: None,
        },
        Some(dedup::child_session(SESSION)),
    )
    .await;
}

async fn complete_parent(log: &Arc<InMemoryEventLog>, outcome: ChildOutcome) {
    append_to(
        log,
        PARENT,
        SessionEvent::ChildSessionCompleted {
            child: SESSION,
            outcome,
        },
        Some(dedup::child_session(SESSION)),
    )
    .await;
}

#[tokio::test]
async fn fork_child_first_request_is_the_snapshot_plus_the_task() {
    let log = Arc::new(InMemoryEventLog::new());
    // The parent still waits: no orphan classification on the first drive.
    seed_parent_waiting(&log).await;

    // Exactly the stream the scheduler materializes for `context: fork`.
    seed_child(&log, Some(parent_ref())).await;
    append_to(
        &log,
        SESSION,
        SessionEvent::ContextForked {
            source: asterpath_protocol::ForkSource::Spawn(parent_ref()),
            source_log: LogId::from_i64(2),
            items: vec![Item::Message {
                role: Role::User,
                parts: vec![text("parent context")],
                raw: None,
            }],
        },
        Some(dedup::context_forked(SESSION)),
    )
    .await;
    append_to(
        &log,
        SESSION,
        SessionEvent::InputRecorded {
            input: vec![text("do the task")],
        },
        Some(dedup::input_recorded(CHILD_INPUT)),
    )
    .await;

    let provider = Arc::new(ScriptedProvider::new("p").respond(
        completed(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let harness = harness(Arc::clone(&log), Arc::clone(&provider), Vec::new());
    let events = drive(&harness.agent).await;
    assert!(
        matches!(events.last(), Some(AgentEvent::TurnCommitted { .. })),
        "the child runs its first turn"
    );

    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].items,
        vec![
            Item::Message {
                role: Role::User,
                parts: vec![text("parent context")],
                raw: None,
            },
            Item::Message {
                role: Role::User,
                parts: vec![text("do the task")],
                raw: None,
            },
        ],
        "the first request is the fork snapshot plus the task, nothing else"
    );
}

#[tokio::test]
async fn orphan_child_closes_open_intents_as_cancelled_without_a_new_turn() {
    let log = Arc::new(InMemoryEventLog::new());
    seed_parent_waiting(&log).await;
    seed_child(&log, Some(parent_ref())).await;
    append_to(
        &log,
        SESSION,
        SessionEvent::ContextForked {
            source: asterpath_protocol::ForkSource::Spawn(parent_ref()),
            source_log: LogId::from_i64(2),
            items: vec![Item::Message {
                role: Role::User,
                parts: vec![text("parent context")],
                raw: None,
            }],
        },
        Some(dedup::context_forked(SESSION)),
    )
    .await;
    append_to(
        &log,
        SESSION,
        SessionEvent::InputRecorded {
            input: vec![text("do the task")],
        },
        Some(dedup::input_recorded(CHILD_INPUT)),
    )
    .await;
    open_turn(&log).await;
    append(
        &log,
        intent(CALL, "c1", "echo", ReplaySafety::Unsafe),
        Some(dedup::tool_call(CALL)),
    )
    .await;
    append(
        &log,
        SessionEvent::ChildSessionSpawned {
            child: GRANDCHILD,
            task: vec![text("grandchild task")],
            config: config(Vec::new()),
            context: ContextPolicy::Fresh,
            parent_tool_call_id: ToolCallId::from_bytes([0x55; 16]),
            caused_by: LogId::from_i64(1),
            deadline: None,
        },
        Some(dedup::child_session(GRANDCHILD)),
    )
    .await;
    // Window: parent recorded the cancellation but the child never closed.
    complete_parent(
        &log,
        ChildOutcome::Cancelled {
            reason: CancelReason::Parent,
        },
    )
    .await;

    let provider = Arc::new(ScriptedProvider::new("p"));
    let gate = Arc::new(ApprovalGate::new());
    let agent = agent_with(
        Arc::clone(&log),
        Arc::clone(&provider),
        Vec::new(),
        Arc::new(DefaultApprovalPolicy),
        gate,
        None,
    );
    let agent_events = drive(&agent).await;

    assert!(provider.requests().is_empty(), "no new model request");
    assert!(
        matches!(
            agent_events.last(),
            Some(AgentEvent::TurnCommitted {
                reason: TurnEndReason::Cancelled,
                ..
            })
        ),
        "the orphan turn closes as cancelled"
    );
    // The closed intent is announced, then finished, like any interrupted call.
    let tool_events: Vec<_> = agent_events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::ToolStarted {
                call, tool, args, ..
            } => Some((
                *call,
                Some((tool.as_str().to_owned(), args.as_str().to_owned())),
            )),
            AgentEvent::ToolFinished {
                call,
                status: ToolResultStatus::Interrupted,
                ..
            } => Some((*call, None)),
            _ => None,
        })
        .collect();
    assert_eq!(
        tool_events,
        [
            (CALL, Some(("echo".to_owned(), "{}".to_owned()))),
            (CALL, None)
        ]
    );

    let entries = scan(&log).await;
    assert_eq!(
        kinds(&entries),
        [
            "SessionCreated",
            "ContextForked",
            "InputRecorded",
            "TurnStarted",
            "StepStarted",
            "ToolCallRequested",
            "ChildSessionSpawned",
            "StreamResumed",
            "ToolResult",
            "ChildSessionCompleted",
            "StepCommitted",
            "TurnCommitted",
        ]
    );
    let results = results(&entries);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, ToolResultStatus::Interrupted);
    assert_eq!(count(&entries, "TurnStarted"), 1, "no new turn is opened");
    assert_eq!(count(&entries, "TurnCommitted"), 1);
    let grandchild = events(&entries)
        .into_iter()
        .find_map(|event| match event {
            SessionEvent::ChildSessionCompleted { child, outcome } => Some((child, outcome)),
            _ => None,
        })
        .expect("own child is closed");
    assert_eq!(grandchild.0, GRANDCHILD);
    assert_eq!(
        grandchild.1,
        ChildOutcome::Cancelled {
            reason: CancelReason::Parent
        }
    );
}

#[tokio::test]
async fn orphan_child_with_a_committed_turn_stays_idle() {
    let log = seed(config(Vec::new())).await;
    // The child already finished its turn; only the parent completion is new.
    open_turn(&log).await;
    append(
        &log,
        SessionEvent::AssistantMessage {
            turn_id: TURN,
            step_id: STEP,
            items: completed(),
            raw: None,
            model: model(),
            usage: Usage::default(),
            stop_reason: StopReason::EndTurn,
        },
        Some(dedup::assistant_message(TURN, STEP)),
    )
    .await;
    append(
        &log,
        SessionEvent::StepCommitted {
            turn_id: TURN,
            step_id: STEP,
        },
        Some(dedup::step_committed(STEP)),
    )
    .await;
    append(
        &log,
        SessionEvent::TurnCommitted {
            turn_id: TURN,
            reason: TurnEndReason::Completed,
        },
        Some(dedup::turn_committed(TURN)),
    )
    .await;
    complete_parent(
        &log,
        ChildOutcome::Completed {
            summary: asterpath_protocol::Summary {
                items: vec![text("done")],
            },
        },
    )
    .await;

    let provider = Arc::new(ScriptedProvider::new("p"));
    let harness = harness(Arc::clone(&log), Arc::clone(&provider), Vec::new());
    let events = drive(&harness.agent).await;
    assert!(matches!(events.as_slice(), [AgentEvent::Idle { .. }]));
    assert!(provider.requests().is_empty());
    let entries = scan(&log).await;
    assert_eq!(count(&entries, "TurnStarted"), 1);
    assert_eq!(count(&entries, "TurnCommitted"), 1);
}

#[tokio::test]
async fn missing_parent_continues_as_an_independent_session() {
    let log = Arc::new(InMemoryEventLog::new());
    // No parent stream at all: the parent was retired.
    seed_child(&log, Some(parent_ref())).await;
    append_to(
        &log,
        SESSION,
        SessionEvent::InputRecorded {
            input: vec![text("continue alone")],
        },
        Some(dedup::input_recorded(CHILD_INPUT)),
    )
    .await;

    let provider = Arc::new(ScriptedProvider::new("p").respond(
        completed(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let harness = harness(Arc::clone(&log), Arc::clone(&provider), Vec::new());
    let _ = drive(&harness.agent).await;

    assert_eq!(provider.requests().len(), 1);
    let entries = scan(&log).await;
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Completed]);
}

#[tokio::test]
async fn an_open_parent_wait_does_not_orphan_the_child() {
    // Window: the parent's cancellation was requested but no terminal record
    // landed; #6 semantics resume the turn.
    let log = Arc::new(InMemoryEventLog::new());
    seed_parent_waiting(&log).await;
    seed_child(&log, Some(parent_ref())).await;
    append_to(
        &log,
        SESSION,
        SessionEvent::InputRecorded {
            input: vec![text("do the task")],
        },
        Some(dedup::input_recorded(CHILD_INPUT)),
    )
    .await;
    open_turn(&log).await;

    let provider = Arc::new(ScriptedProvider::new("p").respond(
        completed(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let harness = harness(Arc::clone(&log), Arc::clone(&provider), Vec::new());
    let _ = drive(&harness.agent).await;

    assert_eq!(provider.requests().len(), 1, "the turn continues");
    let entries = scan(&log).await;
    assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Completed]);
    assert_eq!(count(&entries, "StreamResumed"), 1);
}

#[tokio::test]
async fn a_corrupt_parent_log_is_a_hard_error_not_an_orphan_check() {
    let inner = Arc::new(InMemoryEventLog::new());
    seed_child(&inner, Some(parent_ref())).await;
    append_to(
        &inner,
        SESSION,
        SessionEvent::InputRecorded {
            input: vec![text("do the task")],
        },
        Some(dedup::input_recorded(CHILD_INPUT)),
    )
    .await;
    let corrupt = LogEntry {
        log_id: LogId::from_i64(1),
        session: PARENT,
        seq: 1,
        recorded_at: 0,
        schema_version: 1,
        dedup_key: None,
        payload: serde_json::json!({"NoSuchEvent": {}}),
    };
    let log = Arc::new(CorruptParentLog {
        inner: Arc::clone(&inner),
        corrupt,
    });

    let provider = Arc::new(ScriptedProvider::new("p"));
    let gate = Arc::new(ApprovalGate::new());
    let agent = agent_with(
        log,
        provider,
        Vec::new(),
        Arc::new(DefaultApprovalPolicy),
        gate,
        None,
    );
    let mut stream = agent.run(RunRequest {
        session: SESSION,
        trigger: RunTrigger::Resume,
    });
    let error = stream
        .next()
        .await
        .expect("one event")
        .expect_err("the corrupt parent is a hard error");
    assert!(
        matches!(error, AgentError::Corrupt { session, .. } if session == PARENT),
        "got {error:?}"
    );
    // Orphan classification never ran, so no turn was opened or closed.
    let entries = scan_session(&inner, SESSION).await;
    assert_eq!(count(&entries, "TurnStarted"), 0);
    assert_eq!(count(&entries, "StreamResumed"), 0);
}

/// Delegating event log with one undecodable record on the parent stream.
struct CorruptParentLog {
    inner: Arc<InMemoryEventLog>,
    corrupt: LogEntry,
}

impl SessionStore for CorruptParentLog {
    fn append(
        &self,
        entry: asterpath_protocol::NewEntry,
    ) -> BoxFuture<'_, Result<LogId, StorageError>> {
        self.inner.append(entry)
    }

    fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>> {
        if query.session == PARENT {
            return futures_util::stream::once(async move { Ok(self.corrupt.clone()) }).boxed();
        }
        self.inner.scan(query)
    }

    fn frontier(&self, session: SessionId) -> BoxFuture<'_, Result<Option<LogId>, StorageError>> {
        self.inner.frontier(session)
    }

    fn sessions(
        &self,
        query: SessionQuery,
    ) -> BoxFuture<'_, Result<Vec<CatalogEntry>, StorageError>> {
        self.inner.sessions(query)
    }

    fn parent(&self, session: SessionId) -> BoxFuture<'_, Result<Option<SessionId>, StorageError>> {
        self.inner.parent(session)
    }

    fn claim(
        &self,
        root: SessionId,
    ) -> BoxFuture<'_, Result<asterpath_protocol::Claim, StorageError>> {
        self.inner.claim(root)
    }

    fn owner(
        &self,
        root: SessionId,
    ) -> BoxFuture<'_, Result<Option<asterpath_protocol::OwnerToken>, StorageError>> {
        self.inner.owner(root)
    }

    fn renew(&self) -> BoxFuture<'_, Result<Vec<asterpath_protocol::OwnerToken>, StorageError>> {
        self.inner.renew()
    }
}

/// Crash after a refused step's `AssistantMessage`, with the step open or
/// committed: recovery runs none of its calls and ends the turn `Refused`.
#[tokio::test]
async fn a_recovered_refusal_runs_no_tools_and_ends_refused() {
    for step_committed in [false, true] {
        let (tool, calls) = echo("echo", ReplaySafety::Safe);
        let log = seed(config(vec![described("echo", ReplaySafety::Safe)])).await;
        open_turn(&log).await;
        append(
            &log,
            SessionEvent::AssistantMessage {
                turn_id: TURN,
                step_id: STEP,
                items: vec![tool_call("c1", "echo", "{}")],
                raw: None,
                model: model(),
                usage: Usage::default(),
                stop_reason: StopReason::Refusal,
            },
            Some(dedup::assistant_message(TURN, STEP)),
        )
        .await;
        if step_committed {
            append(
                &log,
                SessionEvent::StepCommitted {
                    turn_id: TURN,
                    step_id: STEP,
                },
                None,
            )
            .await;
        }

        let provider = Arc::new(ScriptedProvider::new("p"));
        let harness = harness(Arc::clone(&log), Arc::clone(&provider), vec![tool]);
        let events = drive(&harness.agent).await;

        assert!(
            !events
                .iter()
                .any(|event| matches!(event, AgentEvent::ToolStarted { .. })),
            "step_committed={step_committed}"
        );
        assert!(calls.lock().unwrap().is_empty());
        let entries = scan(&log).await;
        assert_eq!(count(&entries, "ToolCallRequested"), 0);
        assert_eq!(turn_reasons(&entries), vec![TurnEndReason::Refused]);
        assert!(provider.requests().is_empty());
    }
}
