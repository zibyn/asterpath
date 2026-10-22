//! Tool pipeline behaviour through the public `AgentRunner` seam.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use asterpath_core::{
    ApprovalGate, AwaitSignal, DurableExecutor, Effect, EventLog, ExecutionError, LifecycleError,
    LogQuery, Middleware, Module, ModuleCtx, Next, SessionId, SignalDelivery, SignalPayload,
    TimerId, TimerRequest, ToolCallId, TurnId, WakeCause,
};
use asterpath_protocol::{
    AgentEvent, AgentRunner, CompactionConfig, ConfigSnapshot, DefaultApprovalPolicy, Item,
    JsonString, ModelId, ModelSpec, Parallelism, Part, PostExecuteError, PostExecuteEvent,
    PreExecuteError, PreExecuteEvent, Provenance, Provider, ProviderCatalog, ProviderId,
    ReplaySafety, RequestConfig, Role, RunRequest, RunTrigger, SessionEvent, SideEffect,
    StopReason, SubagentConfig, Tool, ToolArgs, ToolConfig, ToolContext, ToolDescriptor, ToolError,
    ToolName, ToolOutput, ToolRegistry, ToolResultStatus, TurnConfig, TurnEndReason, Usage,
    decode_entry, encode_entry,
};
use asterpath_testing::{InMemoryEventLog, ScriptedProvider};
use futures_core::future::BoxFuture;
use futures_util::StreamExt;

const SESSION: SessionId = SessionId::from_bytes([9; 16]);

// --- host hooks -----------------------------------------------------------

struct InitModule(Arc<dyn Fn(&ModuleCtx) + Send + Sync>);

impl Module for InitModule {
    fn name(&self) -> &'static str {
        "test-hooks"
    }

    fn init<'a>(&'a self, ctx: &'a ModuleCtx) -> BoxFuture<'a, Result<(), LifecycleError>> {
        (self.0)(ctx);
        Box::pin(async { Ok(()) })
    }
}

/// Run `init` on a real module bus and return that bus's context for runtime
/// waterfall dispatch.
async fn hooks_with(init: impl Fn(&ModuleCtx) + Send + Sync + 'static) -> ModuleCtx {
    let slot: Arc<Mutex<Option<ModuleCtx>>> = Arc::new(Mutex::new(None));
    let capture = Arc::clone(&slot);
    let mut host = asterpath_core::ModuleHost::new();
    host.register(Arc::new(InitModule(Arc::new(move |ctx: &ModuleCtx| {
        *capture.lock().unwrap() = Some(ctx.clone());
        init(ctx);
    }))));
    host.init().await.expect("module init");
    slot.lock().unwrap().clone().expect("ctx captured")
}

struct RewriteArgs {
    args: JsonString,
    count: Arc<AtomicUsize>,
}

impl Middleware<PreExecuteEvent> for RewriteArgs {
    fn handle<'a>(
        &'a self,
        mut event: PreExecuteEvent,
        next: Next<'a, PreExecuteEvent>,
    ) -> BoxFuture<'a, Result<PreExecuteEvent, PreExecuteError>> {
        self.count.fetch_add(1, Ordering::SeqCst);
        event.args = self.args.clone();
        next.run(event)
    }
}

struct DenyPreExecute {
    reason: String,
}

impl Middleware<PreExecuteEvent> for DenyPreExecute {
    fn handle<'a>(
        &'a self,
        _event: PreExecuteEvent,
        _next: Next<'a, PreExecuteEvent>,
    ) -> BoxFuture<'a, Result<PreExecuteEvent, PreExecuteError>> {
        let reason = self.reason.clone();
        Box::pin(async move {
            Err(PreExecuteError::Denied {
                reason: Some(reason),
            })
        })
    }
}

struct RedactOutput;

impl Middleware<PostExecuteEvent> for RedactOutput {
    fn handle<'a>(
        &'a self,
        mut event: PostExecuteEvent,
        next: Next<'a, PostExecuteEvent>,
    ) -> BoxFuture<'a, Result<PostExecuteEvent, PostExecuteError>> {
        for part in &mut event.output {
            if let Part::Text { text } = part {
                *text = "[redacted]".to_owned();
            }
        }
        next.run(event)
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
    fn register(&self, tool: Arc<dyn Tool>) -> Effect {
        self.tools
            .lock()
            .unwrap()
            .insert(tool.descriptor().name.clone(), tool);
        Effect
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
    hooks: Option<ModuleCtx>,
) -> impl AgentRunner {
    let provider: Arc<dyn Provider> = provider;
    let gate = Arc::new(ApprovalGate::new());
    asterpath_agent::Agent::new(
        log,
        Arc::new(ProviderCatalog::new(vec![provider]).expect("catalog")),
        registry,
        tool_config,
        Arc::new(DefaultApprovalPolicy::new(gate.clone())),
        gate,
        Arc::new(NoopExecutor),
        hooks,
    )
}

/// Pipeline tests never wait: approval is `Allow` for their in-process tools.
struct NoopExecutor;

impl DurableExecutor for NoopExecutor {
    fn schedule_timer(&self, _req: TimerRequest) -> BoxFuture<'_, Result<TimerId, ExecutionError>> {
        Box::pin(async { Err(ExecutionError::Backend("unused".to_owned())) })
    }

    fn cancel_timer(
        &self,
        _session: SessionId,
        _timer: TimerId,
    ) -> BoxFuture<'_, Result<(), ExecutionError>> {
        Box::pin(async { Err(ExecutionError::Backend("unused".to_owned())) })
    }

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
    let hooks = hooks_with(move |ctx| {
        ctx.intercept(RewriteArgs {
            args: JsonString::new(r#"{"n":2}"#),
            count: Arc::clone(&counter),
        });
    })
    .await;
    let agent = agent(log.clone(), provider, registry, tool_config(), Some(hooks));
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
    let hooks = hooks_with(|ctx| {
        ctx.intercept(DenyPreExecute {
            reason: "blocked by policy".to_owned(),
        });
    })
    .await;
    let agent = agent(log.clone(), provider, registry, tool_config(), Some(hooks));
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
    let agent = agent(log.clone(), provider, registry, tool_config(), None);
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
    let agent = agent(log.clone(), provider, registry, tool_config(), None);
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
    let agent = agent(log.clone(), provider, registry, config, None);
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
    let hooks = hooks_with(|ctx| {
        ctx.intercept(RedactOutput);
    })
    .await;
    let agent = agent(log.clone(), provider, registry, tool_config(), Some(hooks));
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
    let hooks = hooks_with(move |ctx| {
        ctx.intercept(RewriteArgs {
            args: JsonString::new("{}"),
            count: Arc::clone(&counter),
        });
    })
    .await;
    let agent = agent(log.clone(), provider, registry, tool_config(), Some(hooks));
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
    let agent = agent(log.clone(), provider.clone(), registry, tool_config(), None);
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
    let agent = agent(log, provider, registry, tool_config(), None);
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
    let agent = agent(log, provider, registry, config, None);
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
    let agent = agent(log.clone(), provider, registry, tool_config(), None);
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
    let step = asterpath_core::StepId::from_bytes([3; 16]);
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
    let hooks = hooks_with(move |ctx| {
        ctx.intercept(RewriteArgs {
            args: JsonString::new(r#"{"n":99}"#),
            count: Arc::clone(&counter),
        });
    })
    .await;
    let agent = agent(
        log.clone(),
        provider.clone(),
        registry,
        tool_config(),
        Some(hooks),
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
