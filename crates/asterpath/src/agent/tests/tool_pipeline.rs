//! Tool pipeline behaviour through the public `AgentRunner` seam.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use asterpath_protocol::{
    AgentEvent, AgentRunner, CompactionConfig, ConfigSnapshot, DefaultApprovalPolicy, Gate,
    HookError, Hooks, Item, JsonString, ModelId, ModelSpec, Parallelism, Part, Provenance,
    Provider, ProviderCatalog, ProviderId, ReplaySafety, RequestConfig, Role, RunRequest,
    RunTrigger, SessionEvent, SideEffect, StepCtx, StopReason, SubagentConfig, Tool, ToolArgs,
    ToolCallCtx, ToolConfig, ToolContext, ToolDescriptor, ToolError, ToolGate, ToolName,
    ToolOutput, ToolRegistry, ToolResultStatus, ToolResultView, TurnConfig, TurnEndReason, Usage,
    decode_entry, encode_entry,
};
use asterpath_protocol::{
    ApprovalGate, AwaitSignal, DurableExecutor, EventLog, ExecutionError, LogQuery, SessionId,
    SignalDelivery, SignalPayload, ToolCallId, TurnId, WakeCause,
};
use asterpath_testing::{InMemoryEventLog, ScriptedProvider};
use futures_core::future::BoxFuture;
use futures_util::StreamExt;

const SESSION: SessionId = SessionId::from_bytes([9; 16]);

// --- host hooks -----------------------------------------------------------

struct RewriteArgs {
    args: JsonString,
    count: Arc<AtomicUsize>,
}

impl Hooks for RewriteArgs {
    fn before_tool<'a>(
        &'a self,
        _ctx: &'a ToolCallCtx,
        args: &'a mut ToolArgs,
    ) -> BoxFuture<'a, Result<ToolGate, HookError>> {
        self.count.fetch_add(1, Ordering::SeqCst);
        *args = self.args.clone();
        Box::pin(async { Ok(ToolGate::Allow) })
    }
}

struct DenyPreExecute {
    reason: String,
}

impl Hooks for DenyPreExecute {
    fn before_tool<'a>(
        &'a self,
        _ctx: &'a ToolCallCtx,
        _args: &'a mut ToolArgs,
    ) -> BoxFuture<'a, Result<ToolGate, HookError>> {
        let reason = self.reason.clone();
        Box::pin(async move {
            Ok(ToolGate::Deny {
                reason: Some(reason),
            })
        })
    }
}

struct RedactOutput;

impl Hooks for RedactOutput {
    fn after_tool<'a>(
        &'a self,
        _ctx: &'a ToolCallCtx,
        result: &'a mut ToolResultView,
    ) -> BoxFuture<'a, Result<(), HookError>> {
        for part in &mut result.output {
            if let Part::Text { text } = part {
                *text = "[redacted]".to_owned();
            }
        }
        Box::pin(async { Ok(()) })
    }
}

// --- tools and registry ---------------------------------------------------

type InvokeFn = Arc<
    dyn Fn(ToolArgs, ToolContext) -> BoxFuture<'static, Result<ToolOutput, ToolError>>
        + Send
        + Sync,
>;

struct TestTool {
    descriptor: ToolDescriptor,
    calls: Arc<Mutex<Vec<ToolArgs>>>,
    invoke: InvokeFn,
}

impl TestTool {
    fn new(
        descriptor: ToolDescriptor,
        calls: Arc<Mutex<Vec<ToolArgs>>>,
        invoke: impl Fn(ToolArgs, ToolContext) -> BoxFuture<'static, Result<ToolOutput, ToolError>>
        + Send
        + Sync
        + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            descriptor,
            calls,
            invoke: Arc::new(invoke),
        })
    }

    fn echo(descriptor: ToolDescriptor) -> (Arc<Self>, Arc<Mutex<Vec<ToolArgs>>>) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let tool = Self::new(descriptor, Arc::clone(&calls), |args, _ctx| {
            Box::pin(async move { Ok(ToolOutput::new(vec![text(args.as_str())])) })
        });
        (tool, calls)
    }

    fn text(descriptor: ToolDescriptor, value: &str) -> Arc<Self> {
        let value = value.to_owned();
        Self::new(
            descriptor,
            Arc::new(Mutex::new(Vec::new())),
            move |_args, _ctx| {
                let value = value.clone();
                Box::pin(async move { Ok(ToolOutput::new(vec![text(&value)])) })
            },
        )
    }
}

impl Tool for TestTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        args: ToolArgs,
        ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        self.calls.lock().unwrap().push(args.clone());
        (self.invoke)(args, ctx)
    }
}

#[derive(Default)]
struct TestRegistry {
    tools: Mutex<HashMap<ToolName, Arc<dyn Tool>>>,
}

impl TestRegistry {
    fn with(tools: Vec<Arc<dyn Tool>>) -> Arc<Self> {
        let registry = Arc::new(Self::default());
        for tool in tools {
            registry.register(tool);
        }
        registry
    }
}

impl ToolRegistry for TestRegistry {
    fn register(&self, tool: Arc<dyn Tool>) {
        self.tools
            .lock()
            .unwrap()
            .insert(tool.descriptor().name.clone(), tool);
    }

    fn get(&self, name: &ToolName) -> Option<Arc<dyn Tool>> {
        self.tools.lock().unwrap().get(name).cloned()
    }

    fn list(&self) -> Vec<Arc<dyn Tool>> {
        let mut tools: Vec<_> = self.tools.lock().unwrap().values().cloned().collect();
        tools.sort_by(|a, b| a.descriptor().name.cmp(&b.descriptor().name));
        tools
    }
}

// --- fixtures -------------------------------------------------------------

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn descriptor(name: &str) -> ToolDescriptor {
    ToolDescriptor {
        name: ToolName::new(name),
        description: String::new(),
        schema: serde_json::json!({"type": "object"}),
        parallelism: Parallelism::Parallel,
        replay: ReplaySafety::Safe,
        side_effect: SideEffect::ReadOnly,
        provenance: Provenance::InProcess,
        timeout: None,
        max_output_bytes: None,
    }
}

fn tool_config() -> ToolConfig {
    ToolConfig {
        // Timeout tests must not sit in the default 5s kill grace.
        kill_grace: Duration::ZERO,
        ..ToolConfig::default()
    }
}

fn config(tools: Vec<ToolDescriptor>) -> ConfigSnapshot {
    ConfigSnapshot {
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("m"),
            context_window: None,
        },
        system: Vec::new(),
        tools,
        request: RequestConfig::default(),
        turn: TurnConfig {
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            ..TurnConfig::default()
        },
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
    }
}

fn tool_call(call_id: &str, name: &str, args: &str) -> Item {
    Item::ToolCall {
        call_id: call_id.to_owned(),
        provider_item_id: None,
        name: ToolName::new(name),
        raw_args: JsonString::new(args),
        raw: None,
    }
}

fn assistant_text(value: &str) -> Vec<Item> {
    vec![Item::Message {
        role: Role::Assistant,
        parts: vec![text(value)],
    }]
}

async fn append(log: &InMemoryEventLog, event: SessionEvent) {
    let entry = encode_entry(SESSION, None, &event).expect("encodes");
    log.append(entry).await.expect("appends");
}

async fn new_log(config: ConfigSnapshot) -> Arc<InMemoryEventLog> {
    let log = Arc::new(InMemoryEventLog::new());
    append(
        &log,
        SessionEvent::SessionCreated {
            config,
            parent: None,
        },
    )
    .await;
    append(
        &log,
        SessionEvent::InputRecorded {
            input: vec![text("go")],
        },
    )
    .await;
    log
}

fn provider(first: Vec<Item>, stop: StopReason, second: Vec<Item>) -> Arc<ScriptedProvider> {
    Arc::new(
        ScriptedProvider::new("p")
            .respond(first, stop, Usage::default())
            .respond(second, StopReason::EndTurn, Usage::default()),
    )
}

fn agent(
    log: Arc<InMemoryEventLog>,
    provider: Arc<ScriptedProvider>,
    registry: Arc<dyn ToolRegistry>,
    tool_config: ToolConfig,
    hooks: Vec<Arc<dyn Hooks>>,
) -> impl AgentRunner {
    let provider: Arc<dyn Provider> = provider;
    let gate = Arc::new(ApprovalGate::new());
    crate::agent::Agent::new(
        log,
        Arc::new(ProviderCatalog::new(vec![provider]).expect("catalog")),
        registry,
        tool_config,
        Arc::new(DefaultApprovalPolicy::new(gate.clone())),
        gate,
        Arc::new(NoopExecutor),
        Arc::new(asterpath_testing::ManualClock::new()),
    )
    .with_hooks(hooks, None)
}

/// Pipeline tests never wait: approval is `Allow` for their in-process tools.
struct NoopExecutor;

impl DurableExecutor for NoopExecutor {
    fn await_signal(
        &self,
        _req: AwaitSignal,
    ) -> BoxFuture<'_, Result<SignalPayload, ExecutionError>> {
        Box::pin(async { Err(ExecutionError::Backend("unused".to_owned())) })
    }

    fn deliver_signal(
        &self,
        _delivery: SignalDelivery,
    ) -> BoxFuture<'_, Result<(), ExecutionError>> {
        Box::pin(async { Err(ExecutionError::Backend("unused".to_owned())) })
    }

    fn await_wake(
        &self,
        _session: SessionId,
        _cause: WakeCause,
    ) -> BoxFuture<'_, Result<(), ExecutionError>> {
        Box::pin(async { Err(ExecutionError::Backend("unused".to_owned())) })
    }

    fn wake(&self, _session: SessionId, _cause: WakeCause) {}
}

async fn drive(agent: &impl AgentRunner) -> Vec<AgentEvent> {
    let mut stream = agent.run(RunRequest {
        session: SESSION,
        trigger: RunTrigger::Resume,
    });
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.expect("drive"));
    }
    events
}

async fn decoded(log: &InMemoryEventLog) -> Vec<SessionEvent> {
    let mut stream = log.scan(LogQuery {
        session: SESSION,
        after: None,
        limit: None,
    });
    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await {
        entries.push(entry.expect("scan"));
    }
    entries
        .iter()
        .map(|entry| decode_entry(entry).expect("decodes"))
        .collect()
}

fn intents(events: &[SessionEvent]) -> Vec<(ToolCallId, String, JsonString, Provenance)> {
    events
        .iter()
        .filter_map(|event| match event {
            SessionEvent::ToolCallRequested {
                tool_call_id,
                provider_call_id,
                raw_args,
                provenance,
                ..
            } => Some((
                *tool_call_id,
                provider_call_id.clone(),
                raw_args.clone(),
                *provenance,
            )),
            _ => None,
        })
        .collect()
}

fn results(events: &[SessionEvent]) -> Vec<(ToolCallId, ToolResultStatus, bool)> {
    events
        .iter()
        .filter_map(|event| match event {
            SessionEvent::ToolResult {
                tool_call_id,
                status,
                truncated,
                ..
            } => Some((*tool_call_id, *status, *truncated)),
            _ => None,
        })
        .collect()
}

fn error_kind(
    events: &[SessionEvent],
    call: ToolCallId,
) -> Option<asterpath_protocol::ToolErrorKind> {
    events.iter().find_map(|event| match event {
        SessionEvent::ToolResult {
            tool_call_id,
            error,
            ..
        } if *tool_call_id == call => error.as_ref().map(|info| info.kind),
        _ => None,
    })
}

fn result_output(events: &[SessionEvent], call: ToolCallId) -> Option<Vec<Part>> {
    events.iter().find_map(|event| match event {
        SessionEvent::ToolResult {
            tool_call_id,
            output,
            ..
        } if *tool_call_id == call => output.clone(),
        _ => None,
    })
}

// --- T1 / T2: pre-execute -------------------------------------------------

#[tokio::test]
async fn pre_execute_rewrites_args_before_the_intent_is_recorded() {
    let (tool, calls) = TestTool::echo(descriptor("echo"));
    let registry = TestRegistry::with(vec![tool]);
    let log = new_log(config(vec![descriptor("echo")])).await;
    let provider = provider(
        vec![tool_call("call-1", "echo", r#"{"n":1}"#)],
        StopReason::ToolUse,
        assistant_text("done"),
    );
    let count = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&count);
    let hooks: Vec<Arc<dyn Hooks>> = vec![Arc::new(RewriteArgs {
        args: JsonString::new(r#"{"n":2}"#),
        count: Arc::clone(&counter),
    }) as Arc<dyn Hooks>];
    let agent = agent(log.clone(), provider, registry, tool_config(), hooks);
    drive(&agent).await;

    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(*calls.lock().unwrap(), vec![JsonString::new(r#"{"n":2}"#)]);

    let events = decoded(&log).await;
    let intent = intents(&events);
    assert_eq!(intent.len(), 1);
    assert_eq!(intent[0].2, JsonString::new(r#"{"n":2}"#));

    // The assistant message keeps the model's original arguments.
    let raw = events.iter().find_map(|event| match event {
        SessionEvent::AssistantMessage { items, .. } => items.iter().find_map(|item| match item {
            Item::ToolCall { raw_args, .. } => Some(raw_args.clone()),
            _ => None,
        }),
        _ => None,
    });
    assert_eq!(raw, Some(JsonString::new(r#"{"n":1}"#)));
}

#[tokio::test]
async fn pre_execute_deny_records_a_failed_intent_without_executing() {
    let (tool, calls) = TestTool::echo(descriptor("echo"));
    let registry = TestRegistry::with(vec![tool]);
    let log = new_log(config(vec![descriptor("echo")])).await;
    let provider = provider(
        vec![tool_call("call-1", "echo", "{}")],
        StopReason::ToolUse,
        assistant_text("done"),
    );
    let hooks: Vec<Arc<dyn Hooks>> = vec![Arc::new(DenyPreExecute {
        reason: "blocked by policy".to_owned(),
    }) as Arc<dyn Hooks>];
    let agent = agent(log.clone(), provider, registry, tool_config(), hooks);
    let live = drive(&agent).await;

    assert!(
        calls.lock().unwrap().is_empty(),
        "denied calls do not execute"
    );
    let events = decoded(&log).await;
    let intent = intents(&events);
    assert_eq!(intent.len(), 1, "deny still records the intent");
    let result = results(&events);
    assert_eq!(result, vec![(intent[0].0, ToolResultStatus::Failed, false)]);
    assert_eq!(
        error_kind(&events, intent[0].0),
        Some(asterpath_protocol::ToolErrorKind::Denied)
    );
    assert!(
        !live
            .iter()
            .any(|event| matches!(event, AgentEvent::ToolStarted { .. })),
        "no execution event for a denied call"
    );
}

struct PanicBeforeTool;

impl Hooks for PanicBeforeTool {
    fn before_tool<'a>(
        &'a self,
        _ctx: &'a ToolCallCtx,
        _args: &'a mut ToolArgs,
    ) -> BoxFuture<'a, Result<ToolGate, HookError>> {
        panic!("hook bug");
    }
}

#[tokio::test]
async fn a_panicking_before_tool_hook_fails_the_call_as_internal() {
    let (tool, calls) = TestTool::echo(descriptor("echo"));
    let registry = TestRegistry::with(vec![tool]);
    let log = new_log(config(vec![descriptor("echo")])).await;
    let provider = provider(
        vec![tool_call("call-1", "echo", "{}")],
        StopReason::ToolUse,
        assistant_text("done"),
    );
    let hooks: Vec<Arc<dyn Hooks>> = vec![Arc::new(PanicBeforeTool)];
    let agent = agent(log.clone(), provider, registry, tool_config(), hooks);
    drive(&agent).await;

    assert!(calls.lock().unwrap().is_empty());
    let events = decoded(&log).await;
    let intent = intents(&events);
    assert_eq!(
        error_kind(&events, intent[0].0),
        Some(asterpath_protocol::ToolErrorKind::Internal)
    );
    assert_eq!(events.last().map(SessionEvent::kind), Some("TurnCommitted"));
}

// --- T7: panic ------------------------------------------------------------

#[tokio::test]
async fn a_panicking_tool_fails_alone_and_the_step_commits() {
    let boom = TestTool::new(
        descriptor("boom"),
        Arc::new(Mutex::new(Vec::new())),
        |_args, _ctx| Box::pin(async { panic!("boom") }),
    );
    let (ok, _) = TestTool::echo(descriptor("ok"));
    let registry = TestRegistry::with(vec![boom, ok]);
    let log = new_log(config(vec![descriptor("boom"), descriptor("ok")])).await;
    let provider = provider(
        vec![
            tool_call("call-1", "boom", "{}"),
            tool_call("call-2", "ok", "{}"),
        ],
        StopReason::ToolUse,
        assistant_text("done"),
    );
    let agent = agent(log.clone(), provider, registry, tool_config(), Vec::new());
    drive(&agent).await;

    let events = decoded(&log).await;
    let intent = intents(&events);
    assert_eq!(intent.len(), 2);
    let by_provider: HashMap<_, _> = intent
        .iter()
        .map(|(call, provider, _, _)| (provider.as_str(), *call))
        .collect();
    let panicked = by_provider["call-1"];
    let sibling = by_provider["call-2"];

    assert_eq!(
        error_kind(&events, panicked),
        Some(asterpath_protocol::ToolErrorKind::Panic)
    );
    let observed = results(&events);
    assert_eq!(observed.len(), 2);
    assert!(observed.contains(&(panicked, ToolResultStatus::Failed, false)));
    assert!(observed.contains(&(sibling, ToolResultStatus::Completed, false)));
    assert!(
        events
            .iter()
            .any(|event| matches!(event, SessionEvent::StepCommitted { .. }))
    );
    assert!(matches!(
        events.last(),
        Some(SessionEvent::TurnCommitted {
            reason: TurnEndReason::Completed,
            ..
        })
    ));
}

// --- T10: truncation ------------------------------------------------------

#[tokio::test]
async fn oversized_output_is_truncated_at_a_utf8_boundary() {
    let long = "é".repeat(500);
    let mut descriptor = descriptor("long");
    descriptor.max_output_bytes = Some(10);
    let tool = TestTool::text(descriptor.clone(), &long);
    let registry = TestRegistry::with(vec![tool]);
    let log = new_log(config(vec![descriptor])).await;
    let provider = provider(
        vec![tool_call("call-1", "long", "{}")],
        StopReason::ToolUse,
        assistant_text("done"),
    );
    let agent = agent(log.clone(), provider, registry, tool_config(), Vec::new());
    drive(&agent).await;

    let events = decoded(&log).await;
    let call = intents(&events)[0].0;
    assert!(results(&events)[0].2, "truncated flag is set");
    let output = result_output(&events, call).expect("completed output");
    let first = match &output[0] {
        Part::Text { text } => text,
        other => panic!("expected text, got {other:?}"),
    };
    assert_eq!(first.len(), 10);
    assert!(first.is_char_boundary(first.len()));
    let suffix = match output.last() {
        Some(Part::Text { text }) => text,
        other => panic!("expected suffix, got {other:?}"),
    };
    assert!(
        suffix.starts_with("[output truncated: ") && suffix.ends_with(" bytes omitted]"),
        "suffix is {suffix:?}"
    );
    assert!(
        !output
            .iter()
            .any(|part| matches!(part, Part::Text { text } if text.contains(&long))),
        "the full output never reaches the log"
    );
}

#[tokio::test]
async fn the_step_cap_truncates_later_completions() {
    let tool = TestTool::text(descriptor("fixed"), &"a".repeat(100));
    let registry = TestRegistry::with(vec![tool]);
    let log = new_log(config(vec![descriptor("fixed")])).await;
    let provider = provider(
        vec![
            tool_call("call-1", "fixed", "{}"),
            tool_call("call-2", "fixed", "{}"),
        ],
        StopReason::ToolUse,
        assistant_text("done"),
    );
    let config = ToolConfig {
        max_output_bytes: 1000,
        max_output_bytes_per_step: 150,
        ..tool_config()
    };
    let agent = agent(log.clone(), provider, registry, config, Vec::new());
    drive(&agent).await;

    let events = decoded(&log).await;
    let truncated = results(&events).iter().filter(|(_, _, t)| *t).count();
    assert_eq!(truncated, 1, "the step budget truncates exactly one call");
    let written: usize = intents(&events)
        .iter()
        .filter_map(|(call, _, _, _)| result_output(&events, *call))
        .flat_map(|parts| parts.into_iter())
        .map(|part| match part {
            Part::Text { text } => text.len(),
            _ => 0,
        })
        .sum();
    assert!(
        written < 150 + 2 * 64,
        "step cap bounds the visible bytes: {written}"
    );
}

#[tokio::test]
async fn post_execute_can_rewrite_content_before_the_result_is_logged() {
    let (tool, _) = TestTool::echo(descriptor("echo"));
    let registry = TestRegistry::with(vec![tool]);
    let log = new_log(config(vec![descriptor("echo")])).await;
    let provider = provider(
        vec![tool_call("call-1", "echo", r#"{"secret":1}"#)],
        StopReason::ToolUse,
        assistant_text("done"),
    );
    let hooks: Vec<Arc<dyn Hooks>> = vec![Arc::new(RedactOutput) as Arc<dyn Hooks>];
    let agent = agent(log.clone(), provider, registry, tool_config(), hooks);
    drive(&agent).await;

    let events = decoded(&log).await;
    let call = intents(&events)[0].0;
    assert_eq!(result_output(&events, call), Some(vec![text("[redacted]")]));
}

// --- T14: unknown tool ----------------------------------------------------

#[tokio::test]
async fn an_unknown_tool_records_intent_and_not_found_without_execution() {
    let registry = TestRegistry::with(Vec::new());
    let log = new_log(config(Vec::new())).await;
    let provider = provider(
        vec![tool_call("call-1", "ghost", "{}")],
        StopReason::ToolUse,
        assistant_text("done"),
    );
    let count = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&count);
    let hooks: Vec<Arc<dyn Hooks>> = vec![Arc::new(RewriteArgs {
        args: JsonString::new("{}"),
        count: Arc::clone(&counter),
    }) as Arc<dyn Hooks>];
    let agent = agent(log.clone(), provider, registry, tool_config(), hooks);
    let live = drive(&agent).await;

    assert_eq!(count.load(Ordering::SeqCst), 1, "pre-execute still runs");
    assert!(
        !live
            .iter()
            .any(|event| matches!(event, AgentEvent::ToolStarted { .. })),
        "nothing executes"
    );

    let events = decoded(&log).await;
    let intent = intents(&events);
    assert_eq!(intent.len(), 1);
    assert_eq!(intent[0].3, Provenance::Unknown);
    let mismatch = events.iter().find_map(|event| match event {
        SessionEvent::ToolCallRequested {
            replay, provenance, ..
        } => Some((*replay, *provenance)),
        _ => None,
    });
    assert_eq!(mismatch, Some((ReplaySafety::Unsafe, Provenance::Unknown)));
    assert_eq!(
        error_kind(&events, intent[0].0),
        Some(asterpath_protocol::ToolErrorKind::NotFound)
    );
    assert_eq!(results(&events)[0].1, ToolResultStatus::Failed);
}

// --- T15: same name and args in parallel ----------------------------------

#[tokio::test]
async fn same_name_and_args_parallel_calls_get_distinct_identities() {
    let (tool, _) = TestTool::echo(descriptor("echo"));
    let registry = TestRegistry::with(vec![tool]);
    let log = new_log(config(vec![descriptor("echo")])).await;
    let provider = provider(
        vec![
            tool_call("provider-a", "echo", r#"{"n":1}"#),
            tool_call("provider-b", "echo", r#"{"n":1}"#),
        ],
        StopReason::ToolUse,
        assistant_text("done"),
    );
    let agent = agent(
        log.clone(),
        provider.clone(),
        registry,
        tool_config(),
        Vec::new(),
    );
    drive(&agent).await;

    let events = decoded(&log).await;
    let intent = intents(&events);
    assert_eq!(intent.len(), 2);
    let ids: HashSet<_> = intent.iter().map(|(call, _, _, _)| *call).collect();
    assert_eq!(ids.len(), 2, "framework identities are distinct");
    let provider_ids: HashSet<_> = intent.iter().map(|(_, id, _, _)| id.clone()).collect();
    assert_eq!(
        provider_ids,
        HashSet::from(["provider-a".into(), "provider-b".into()])
    );

    let results = results(&events);
    assert_eq!(results.len(), 2);
    let result_ids: HashSet<_> = results.iter().map(|(call, _, _)| *call).collect();
    assert_eq!(result_ids, ids, "each result binds its own call");

    // The next request pairs the results back by provider call id, in call order.
    let second = &provider.requests()[1];
    let call_ids: Vec<&str> = second
        .items
        .iter()
        .filter_map(|item| match item {
            Item::ToolResult { call_id, .. } => Some(call_id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(call_ids, ["provider-a", "provider-b"]);
}

// --- locking and concurrency ---------------------------------------------

#[derive(Default)]
struct Probe {
    active: AtomicUsize,
    max_active: AtomicUsize,
    write_saw_others: AtomicBool,
}

#[tokio::test]
async fn an_exclusive_call_excludes_parallel_siblings_for_the_step() {
    let probe = Arc::new(Probe::default());
    let read_probe = Arc::clone(&probe);
    let read = TestTool::new(
        ToolDescriptor {
            parallelism: Parallelism::Parallel,
            ..descriptor("read")
        },
        Arc::new(Mutex::new(Vec::new())),
        move |_args, _ctx| {
            let probe = Arc::clone(&read_probe);
            Box::pin(async move {
                let active = probe.active.fetch_add(1, Ordering::SeqCst) + 1;
                probe.max_active.fetch_max(active, Ordering::SeqCst);
                for _ in 0..5 {
                    tokio::task::yield_now().await;
                }
                probe.active.fetch_sub(1, Ordering::SeqCst);
                Ok(ToolOutput::new(Vec::new()))
            })
        },
    );
    let write_probe = Arc::clone(&probe);
    let write = TestTool::new(
        ToolDescriptor {
            parallelism: Parallelism::Exclusive,
            ..descriptor("write")
        },
        Arc::new(Mutex::new(Vec::new())),
        move |_args, _ctx| {
            let probe = Arc::clone(&write_probe);
            Box::pin(async move {
                let active = probe.active.fetch_add(1, Ordering::SeqCst) + 1;
                probe.max_active.fetch_max(active, Ordering::SeqCst);
                if active > 1 {
                    probe.write_saw_others.store(true, Ordering::SeqCst);
                }
                tokio::task::yield_now().await;
                probe.active.fetch_sub(1, Ordering::SeqCst);
                Ok(ToolOutput::new(Vec::new()))
            })
        },
    );
    let registry = TestRegistry::with(vec![read, write]);
    let log = new_log(config(vec![
        descriptor("read"),
        ToolDescriptor {
            parallelism: Parallelism::Exclusive,
            ..descriptor("write")
        },
    ]))
    .await;
    let provider = provider(
        vec![
            tool_call("call-1", "read", "{}"),
            tool_call("call-2", "write", "{}"),
        ],
        StopReason::ToolUse,
        assistant_text("done"),
    );
    let agent = agent(log, provider, registry, tool_config(), Vec::new());
    drive(&agent).await;

    assert!(!probe.write_saw_others.load(Ordering::SeqCst));
    assert_eq!(probe.max_active.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn max_parallel_calls_queues_excess_tools() {
    struct ParallelProbe {
        started: AtomicUsize,
        active: AtomicUsize,
        max_active: AtomicUsize,
    }

    let probe = Arc::new(ParallelProbe {
        started: AtomicUsize::new(0),
        active: AtomicUsize::new(0),
        max_active: AtomicUsize::new(0),
    });
    let notify = Arc::new(tokio::sync::Notify::new());
    let target = 2;

    let mut tools: Vec<Arc<dyn Tool>> = Vec::new();
    for name in ["p1", "p2", "p3"] {
        let probe = Arc::clone(&probe);
        let notify = Arc::clone(&notify);
        tools.push(TestTool::new(
            descriptor(name),
            Arc::new(Mutex::new(Vec::new())),
            move |_args, _ctx| {
                let probe = Arc::clone(&probe);
                let notify = Arc::clone(&notify);
                Box::pin(async move {
                    let notified = notify.notified();
                    let started = probe.started.fetch_add(1, Ordering::SeqCst) + 1;
                    let active = probe.active.fetch_add(1, Ordering::SeqCst) + 1;
                    probe.max_active.fetch_max(active, Ordering::SeqCst);
                    if started < target {
                        notified.await;
                    } else {
                        notify.notify_waiters();
                    }
                    probe.active.fetch_sub(1, Ordering::SeqCst);
                    Ok(ToolOutput::new(Vec::new()))
                })
            },
        ));
    }
    let registry = TestRegistry::with(tools);
    let log = new_log(config(vec![
        descriptor("p1"),
        descriptor("p2"),
        descriptor("p3"),
    ]))
    .await;
    let provider = provider(
        vec![
            tool_call("call-1", "p1", "{}"),
            tool_call("call-2", "p2", "{}"),
            tool_call("call-3", "p3", "{}"),
        ],
        StopReason::ToolUse,
        assistant_text("done"),
    );
    let config = ToolConfig {
        max_parallel_calls: std::num::NonZeroUsize::new(2).expect("non-zero"),
        ..tool_config()
    };
    let agent = agent(log, provider, registry, config, Vec::new());
    drive(&agent).await;

    assert_eq!(probe.started.load(Ordering::SeqCst), 3);
    assert_eq!(probe.max_active.load(Ordering::SeqCst), 2);
}

// --- timeout --------------------------------------------------------------

#[tokio::test]
async fn a_tool_that_ignores_the_deadline_fails_timeout() {
    let mut descriptor = descriptor("slow");
    descriptor.timeout = Some(Duration::from_millis(20));
    let tool = TestTool::new(
        descriptor.clone(),
        Arc::new(Mutex::new(Vec::new())),
        |_args, _ctx| Box::pin(std::future::pending()),
    );
    let registry = TestRegistry::with(vec![tool]);
    let log = new_log(config(vec![descriptor])).await;
    let provider = provider(
        vec![tool_call("call-1", "slow", "{}")],
        StopReason::ToolUse,
        assistant_text("done"),
    );
    let agent = agent(log.clone(), provider, registry, tool_config(), Vec::new());
    drive(&agent).await;

    let events = decoded(&log).await;
    let call = intents(&events)[0].0;
    assert_eq!(
        error_kind(&events, call),
        Some(asterpath_protocol::ToolErrorKind::Timeout)
    );
}

// --- T16: recovery seam ---------------------------------------------------

#[tokio::test]
async fn resume_does_not_rerun_pre_execute_for_recorded_intents() {
    let turn = TurnId::from_bytes([2; 16]);
    let step = asterpath_protocol::StepId::from_bytes([3; 16]);
    let recorded = ToolCallId::from_bytes([4; 16]);
    let log = new_log(config(vec![descriptor("echo")])).await;
    append(&log, SessionEvent::TurnStarted { turn_id: turn }).await;
    append(
        &log,
        SessionEvent::StepStarted {
            turn_id: turn,
            step_id: step,
        },
    )
    .await;
    append(
        &log,
        SessionEvent::AssistantMessage {
            turn_id: turn,
            step_id: step,
            items: vec![
                tool_call("provider-a", "echo", r#"{"n":1}"#),
                tool_call("provider-b", "echo", r#"{"n":1}"#),
            ],
            raw: None,
            model: config(Vec::new()).model,
            usage: Usage::default(),
            stop_reason: StopReason::ToolUse,
        },
    )
    .await;
    append(
        &log,
        SessionEvent::ToolCallRequested {
            turn_id: turn,
            step_id: step,
            tool_call_id: recorded,
            provider_call_id: "provider-a".to_owned(),
            name: ToolName::new("echo"),
            raw_args: JsonString::new(r#"{"n":2}"#),
            replay: ReplaySafety::Safe,
            provenance: Provenance::InProcess,
        },
    )
    .await;

    let (tool, calls) = TestTool::echo(descriptor("echo"));
    let registry = TestRegistry::with(vec![tool]);
    // Only the next step's model response is scripted: the open step must not
    // call the model again.
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        assistant_text("done"),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let count = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&count);
    let hooks: Vec<Arc<dyn Hooks>> = vec![Arc::new(RewriteArgs {
        args: JsonString::new(r#"{"n":99}"#),
        count: Arc::clone(&counter),
    }) as Arc<dyn Hooks>];
    let agent = agent(
        log.clone(),
        provider.clone(),
        registry,
        tool_config(),
        hooks,
    );
    drive(&agent).await;

    assert_eq!(
        count.load(Ordering::SeqCst),
        1,
        "only the call without an intent re-runs pre-execute"
    );
    let received: HashSet<String> = calls
        .lock()
        .unwrap()
        .iter()
        .map(|args| args.as_str().to_owned())
        .collect();
    assert_eq!(
        received,
        HashSet::from([r#"{"n":2}"#.to_owned(), r#"{"n":99}"#.to_owned()])
    );

    let events = decoded(&log).await;
    let intent = intents(&events);
    assert_eq!(intent.len(), 2);
    assert!(
        intent
            .iter()
            .any(|(call, _, args, _)| *call == recorded && args.as_str() == r#"{"n":2}"#),
        "the recorded intent and its effective args are authoritative"
    );
    assert_eq!(results(&events).len(), 2);
    assert_eq!(
        provider.requests().len(),
        1,
        "the open step is not re-asked"
    );
}

// --- pre-step gate --------------------------------------------------------

struct StopGate {
    reason: String,
}

impl Hooks for StopGate {
    fn before_step<'a>(&'a self, _ctx: &'a StepCtx) -> BoxFuture<'a, Result<Gate, HookError>> {
        let reason = self.reason.clone();
        Box::pin(async move { Ok(Gate::EndTurn { reason }) })
    }
}

struct StopAfter {
    calls: Arc<AtomicUsize>,
    skip: usize,
    reason: String,
}

impl Hooks for StopAfter {
    fn before_step<'a>(&'a self, ctx: &'a StepCtx) -> BoxFuture<'a, Result<Gate, HookError>> {
        let index = self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            ctx.step_index as usize, index,
            "step_index counts the turn's steps"
        );
        let gate = if index >= self.skip {
            Gate::EndTurn {
                reason: self.reason.clone(),
            }
        } else {
            Gate::Continue
        };
        Box::pin(async move { Ok(gate) })
    }
}

struct FailGate;

impl Hooks for FailGate {
    fn before_step<'a>(&'a self, _ctx: &'a StepCtx) -> BoxFuture<'a, Result<Gate, HookError>> {
        Box::pin(async { Err(HookError::new("gate boom")) })
    }
}

#[tokio::test]
async fn a_gate_stop_before_the_first_step_ends_a_zero_step_turn() {
    let log = new_log(config(Vec::new())).await;
    let provider = Arc::new(ScriptedProvider::new("p"));
    let registry = TestRegistry::with(Vec::new());
    let hooks: Vec<Arc<dyn Hooks>> = vec![Arc::new(StopGate {
        reason: "budget".to_owned(),
    }) as Arc<dyn Hooks>];
    let agent = agent(
        log.clone(),
        provider.clone(),
        registry,
        tool_config(),
        hooks,
    );

    let events = drive(&agent).await;
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::TurnCommitted {
            reason: TurnEndReason::Stopped { reason },
            ..
        } if reason == "budget"
    )));

    let events = decoded(&log).await;
    let kinds: Vec<_> = events.iter().map(SessionEvent::kind).collect();
    assert_eq!(
        kinds,
        [
            "SessionCreated",
            "InputRecorded",
            "TurnStarted",
            "TurnCommitted",
        ]
    );
    assert_eq!(
        provider.requests().len(),
        0,
        "the gate runs before the model"
    );
}

#[tokio::test]
async fn a_gate_error_ends_the_turn_failed_with_no_step() {
    let log = new_log(config(Vec::new())).await;
    let provider = Arc::new(ScriptedProvider::new("p"));
    let registry = TestRegistry::with(Vec::new());
    let hooks: Vec<Arc<dyn Hooks>> = vec![Arc::new(FailGate) as Arc<dyn Hooks>];
    let agent = agent(
        log.clone(),
        provider.clone(),
        registry,
        tool_config(),
        hooks,
    );

    let events = drive(&agent).await;
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::TurnCommitted {
            reason: TurnEndReason::Failed,
            ..
        }
    )));
    let events = decoded(&log).await;
    assert_eq!(events.last().map(SessionEvent::kind), Some("TurnCommitted"));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, SessionEvent::StepStarted { .. }))
    );
}

#[tokio::test]
async fn the_gate_is_consulted_before_every_step() {
    let log = new_log(config(Vec::new())).await;
    let provider = provider(
        assistant_text("one"),
        StopReason::PauseTurn,
        assistant_text("two"),
    );
    let registry = TestRegistry::with(Vec::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let hooks: Vec<Arc<dyn Hooks>> = vec![Arc::new(StopAfter {
        calls: Arc::clone(&counter),
        skip: 1,
        reason: "enough".to_owned(),
    }) as Arc<dyn Hooks>];
    let agent = agent(
        log.clone(),
        provider.clone(),
        registry,
        tool_config(),
        hooks,
    );

    let events = drive(&agent).await;
    assert!(events.iter().any(|event| matches!(
        event,
        AgentEvent::TurnCommitted {
            reason: TurnEndReason::Stopped { reason },
            ..
        } if reason == "enough"
    )));
    let events = decoded(&log).await;
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, SessionEvent::StepStarted { .. }))
            .count(),
        1,
        "the gate stops the turn before the second step"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(provider.requests().len(), 1);
}

// --- hook call sites on the recovery path ----------------------------------

struct CountPre(Arc<AtomicUsize>);

impl Hooks for CountPre {
    fn before_tool<'a>(
        &'a self,
        _ctx: &'a ToolCallCtx,
        _args: &'a mut ToolArgs,
    ) -> BoxFuture<'a, Result<ToolGate, HookError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(ToolGate::Allow) })
    }
}

struct CountPost(Arc<Mutex<Vec<ToolResultStatus>>>);

impl Hooks for CountPost {
    fn after_tool<'a>(
        &'a self,
        _ctx: &'a ToolCallCtx,
        result: &'a mut ToolResultView,
    ) -> BoxFuture<'a, Result<(), HookError>> {
        self.0.lock().unwrap().push(result.status());
        Box::pin(async { Ok(()) })
    }
}

struct CountGate(Arc<AtomicUsize>);

impl Hooks for CountGate {
    fn before_step<'a>(&'a self, _ctx: &'a StepCtx) -> BoxFuture<'a, Result<Gate, HookError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(Gate::Continue) })
    }
}

/// Recovery of an open step: the recorded unsafe intent gets a framework
/// `Interrupted` that skips both tool hooks; only the fresh call runs them, and
/// the step gate is consulted before new steps only.
#[tokio::test]
async fn recovery_runs_hooks_only_for_fresh_calls_and_new_steps() {
    let turn = TurnId::from_bytes([2; 16]);
    let step = asterpath_protocol::StepId::from_bytes([3; 16]);
    let recorded = ToolCallId::from_bytes([4; 16]);
    let danger = ToolDescriptor {
        replay: ReplaySafety::Unsafe,
        ..descriptor("danger")
    };
    let log = new_log(config(vec![descriptor("echo"), danger.clone()])).await;
    append(&log, SessionEvent::TurnStarted { turn_id: turn }).await;
    append(
        &log,
        SessionEvent::StepStarted {
            turn_id: turn,
            step_id: step,
        },
    )
    .await;
    append(
        &log,
        SessionEvent::AssistantMessage {
            turn_id: turn,
            step_id: step,
            items: vec![
                tool_call("provider-a", "danger", "{}"),
                tool_call("provider-b", "echo", "{}"),
            ],
            raw: None,
            model: config(Vec::new()).model,
            usage: Usage::default(),
            stop_reason: StopReason::ToolUse,
        },
    )
    .await;
    append(
        &log,
        SessionEvent::ToolCallRequested {
            turn_id: turn,
            step_id: step,
            tool_call_id: recorded,
            provider_call_id: "provider-a".to_owned(),
            name: ToolName::new("danger"),
            raw_args: JsonString::new("{}"),
            replay: ReplaySafety::Unsafe,
            provenance: Provenance::InProcess,
        },
    )
    .await;

    let (echo, _) = TestTool::echo(descriptor("echo"));
    let (danger_tool, danger_calls) = TestTool::echo(danger);
    let registry = TestRegistry::with(vec![echo, danger_tool]);
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        assistant_text("done"),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let pre = Arc::new(AtomicUsize::new(0));
    let post = Arc::new(Mutex::new(Vec::new()));
    let gate = Arc::new(AtomicUsize::new(0));
    let (pre_c, post_c, gate_c) = (Arc::clone(&pre), Arc::clone(&post), Arc::clone(&gate));
    let hooks: Vec<Arc<dyn Hooks>> = vec![
        Arc::new(CountPre(Arc::clone(&pre_c))) as Arc<dyn Hooks>,
        Arc::new(CountPost(Arc::clone(&post_c))) as Arc<dyn Hooks>,
        Arc::new(CountGate(Arc::clone(&gate_c))) as Arc<dyn Hooks>,
    ];
    let agent = agent(log.clone(), provider, registry, tool_config(), hooks);
    drive(&agent).await;

    let events = decoded(&log).await;
    assert!(
        results(&events)
            .iter()
            .any(|(call, status, _)| *call == recorded && *status == ToolResultStatus::Interrupted),
        "the recorded unsafe intent is interrupted by the framework"
    );
    assert!(danger_calls.lock().unwrap().is_empty());
    assert_eq!(pre.load(Ordering::SeqCst), 1, "only the fresh call");
    assert_eq!(
        *post.lock().unwrap(),
        [ToolResultStatus::Completed],
        "the framework-written Interrupted skips post-execute"
    );
    assert_eq!(
        gate.load(Ordering::SeqCst),
        1,
        "the recovered open step is not gated; the next step is"
    );
}
