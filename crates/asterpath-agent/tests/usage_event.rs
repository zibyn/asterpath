//! U11: `UsageRecorded` is emitted exactly once per usage terminal append by
//! the loop, is visible to the next pre-step gate, and is never replayed.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use asterpath_core::{
    ApprovalGate, AwaitSignal, DedupKey, DurableExecutor, Effect, EventLog, ExecutionError,
    Handler, LifecycleError, LogId, Middleware, Module, ModuleCtx, Next, SessionId, SignalDelivery,
    SignalPayload, StepId, TimerId, TimerRequest, TurnId, WakeCause,
};
use asterpath_protocol::{
    AgentEvent, AgentRunner, CompactPlan, CompactSummary, CompactionConfig, CompactionError,
    CompactionJob, CompactionOutcome, ConfigSnapshot, DefaultApprovalPolicy, InputTokens, Item,
    JsonString, ModelId, ModelSpec, OutputTokens, Part, PreStepGateEvent, Provider,
    ProviderCatalog, ProviderError, ProviderErrorKind, ProviderId, RequestConfig, Role, RunRequest,
    RunTrigger, SessionEvent, StopReason, SubagentConfig, Tool, ToolArgs, ToolConfig, ToolContext,
    ToolDescriptor, ToolError, ToolName, ToolOutput, ToolRegistry, TurnConfig, Usage, UsageBucket,
    UsageRecorded, decode_entry, dedup, encode_entry,
};
use asterpath_testing::{InMemoryEventLog, ManualClock, ScriptedProvider};
use futures_core::future::BoxFuture;
use futures_util::StreamExt;

const SESSION: SessionId = SessionId::from_bytes([9; 16]);
const TURN: TurnId = TurnId::from_bytes([1; 16]);
const STEP: StepId = StepId::from_bytes([2; 16]);

fn usage(input: u64, output: u64) -> Usage {
    Usage {
        input: InputTokens {
            uncached: input,
            cache_read: 0,
            cache_write: 0,
        },
        output: OutputTokens {
            total: output,
            reasoning: 0,
        },
    }
}

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn model() -> ModelSpec {
    ModelSpec {
        provider: ProviderId::new("p"),
        model: ModelId::new("model-1"),
        context_window: Some(200_000),
    }
}

fn config(tools: Vec<ToolDescriptor>) -> ConfigSnapshot {
    ConfigSnapshot {
        model: model(),
        system: vec![text("be helpful")],
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

fn assistant(text_value: &str) -> Item {
    Item::Message {
        role: Role::Assistant,
        parts: vec![text(text_value)],
    }
}

fn tool_call() -> Item {
    Item::ToolCall {
        call_id: "call-1".to_owned(),
        provider_item_id: None,
        name: ToolName::new("echo"),
        raw_args: JsonString::new("{}"),
        raw: None,
    }
}

// --- host hooks -----------------------------------------------------------

struct InitModule(Arc<dyn Fn(&ModuleCtx) + Send + Sync>);

impl Module for InitModule {
    fn name(&self) -> &'static str {
        "usage-event-test-hooks"
    }

    fn init<'a>(&'a self, ctx: &'a ModuleCtx) -> BoxFuture<'a, Result<(), LifecycleError>> {
        (self.0)(ctx);
        Box::pin(async { Ok(()) })
    }
}

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

struct CaptureUsage(Arc<Mutex<Vec<UsageRecorded>>>);

impl Handler<UsageRecorded> for CaptureUsage {
    fn handle<'a>(&'a self, event: &'a UsageRecorded) -> BoxFuture<'a, ()> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(event.clone());
        Box::pin(async {})
    }
}

struct GateProbe {
    recorded: Arc<Mutex<Vec<UsageRecorded>>>,
    sizes: Arc<Mutex<Vec<usize>>>,
}

impl Middleware<PreStepGateEvent> for GateProbe {
    fn handle<'a>(
        &'a self,
        event: PreStepGateEvent,
        next: Next<'a, PreStepGateEvent>,
    ) -> BoxFuture<'a, Result<PreStepGateEvent, asterpath_protocol::PreStepGateError>> {
        let seen = self
            .recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len();
        self.sizes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(seen);
        Box::pin(async move { next.run(event).await })
    }
}

/// Hooks that record every `UsageRecorded` and snapshot the count at each
/// pre-step gate.
async fn observing_hooks() -> (
    ModuleCtx,
    Arc<Mutex<Vec<UsageRecorded>>>,
    Arc<Mutex<Vec<usize>>>,
) {
    let recorded = Arc::new(Mutex::new(Vec::new()));
    let gate_sizes = Arc::new(Mutex::new(Vec::new()));
    let recorded_for_handler = Arc::clone(&recorded);
    let recorded_for_gate = Arc::clone(&recorded);
    let gate_sizes_for_gate = Arc::clone(&gate_sizes);
    let ctx = hooks_with(move |ctx| {
        ctx.on(CaptureUsage(Arc::clone(&recorded_for_handler)));
        ctx.intercept(GateProbe {
            recorded: Arc::clone(&recorded_for_gate),
            sizes: Arc::clone(&gate_sizes_for_gate),
        });
    })
    .await;
    (ctx, recorded, gate_sizes)
}

// --- tools and executor ---------------------------------------------------

struct Echo(ToolDescriptor);

impl Tool for Echo {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.0
    }

    fn invoke(
        &self,
        _args: ToolArgs,
        _ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async { Ok(ToolOutput::new(vec![text("done")])) })
    }
}

struct Registry(Mutex<HashMap<ToolName, Arc<dyn Tool>>>);

impl ToolRegistry for Registry {
    fn register(&self, tool: Arc<dyn Tool>) -> Effect {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(tool.descriptor().name.clone(), tool);
        Effect
    }

    fn get(&self, name: &ToolName) -> Option<Arc<dyn Tool>> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .cloned()
    }

    fn list(&self) -> Vec<Arc<dyn Tool>> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }
}

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

struct AlwaysCompact;

impl asterpath_protocol::Compactor for AlwaysCompact {
    fn plan(&self, input: &asterpath_protocol::CompactionInput) -> CompactPlan {
        CompactPlan::Summarize(CompactionJob {
            first_kept: input
                .step_starts
                .first()
                .copied()
                .unwrap_or(LogId::from_i64(0)),
            replaces: asterpath_core::LogIdRange {
                first: LogId::from_i64(0),
                last: LogId::from_i64(0),
            },
            model: input.model.clone(),
            summary_input: Vec::new(),
            max_summary_tokens: 128,
            usable_window: 1000,
            retained_tokens: 0,
            turn: input.turn.clone(),
        })
    }

    fn compact<'a>(
        &'a self,
        job: CompactionJob,
    ) -> BoxFuture<'a, Result<CompactSummary, CompactionError>> {
        Box::pin(async move {
            Ok(CompactSummary::Applied(CompactionOutcome {
                first_kept: job.first_kept,
                summary: vec![text("summary")],
                replaces: job.replaces,
                model: job.model,
                usage: usage(50, 5),
                raw: None,
            }))
        })
    }
}

// --- harness --------------------------------------------------------------

fn agent(
    log: Arc<InMemoryEventLog>,
    provider: Arc<ScriptedProvider>,
    tools: Vec<Arc<dyn Tool>>,
    hooks: Option<ModuleCtx>,
    compactor: Option<Arc<dyn asterpath_protocol::Compactor>>,
) -> Box<dyn AgentRunner> {
    let registry = Arc::new(Registry(Mutex::new(HashMap::new())));
    for tool in tools {
        registry.register(tool);
    }
    let gate = Arc::new(ApprovalGate::new());
    let provider: Arc<dyn Provider> = provider;
    Box::new(
        asterpath_agent::Agent::new(
            log,
            Arc::new(ProviderCatalog::new(vec![provider]).expect("catalog")),
            registry,
            ToolConfig::default(),
            Arc::new(DefaultApprovalPolicy::new(Arc::clone(&gate))),
            gate,
            Arc::new(NoopExecutor),
            Arc::new(ManualClock::new()),
            hooks,
        )
        .with_compactor(compactor),
    )
}

async fn seed(log: &Arc<InMemoryEventLog>, config: ConfigSnapshot) {
    let entry = encode_entry(
        SESSION,
        Some(dedup::session_created(SESSION)),
        &SessionEvent::SessionCreated {
            config,
            parent: None,
        },
    )
    .expect("encode");
    log.append(entry).await.expect("append");
}

async fn submit_input(log: &Arc<InMemoryEventLog>) {
    let input = asterpath_core::InputId::from_bytes([4; 16]);
    append(
        log,
        SessionEvent::InputRecorded {
            input: vec![text("go")],
        },
        Some(dedup::input_recorded(input)),
    )
    .await;
}

async fn append(log: &Arc<InMemoryEventLog>, event: SessionEvent, key: Option<DedupKey>) -> LogId {
    let entry = encode_entry(SESSION, key, &event).expect("encode");
    log.append(entry).await.expect("append")
}

async fn drive(agent: &dyn AgentRunner) -> Vec<AgentEvent> {
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

#[tokio::test]
async fn usage_recorded_once_per_terminal_and_visible_to_next_gate() {
    let log = Arc::new(InMemoryEventLog::new());
    let descriptor = ToolDescriptor {
        name: ToolName::new("echo"),
        description: String::new(),
        schema: serde_json::json!({"type": "object"}),
        parallelism: asterpath_protocol::Parallelism::Parallel,
        replay: asterpath_protocol::ReplaySafety::Safe,
        side_effect: asterpath_protocol::SideEffect::ReadOnly,
        provenance: asterpath_protocol::Provenance::InProcess,
        timeout: None,
        max_output_bytes: None,
    };
    seed(&log, config(vec![descriptor.clone()])).await;
    submit_input(&log).await;
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(vec![tool_call()], StopReason::ToolUse, usage(10, 2))
            .respond(vec![assistant("done")], StopReason::EndTurn, usage(20, 3)),
    );
    let (hooks, recorded, gate_sizes) = observing_hooks().await;
    let agent = agent(
        Arc::clone(&log),
        Arc::clone(&provider),
        vec![Arc::new(Echo(descriptor))],
        Some(hooks),
        None,
    );

    drive(agent.as_ref()).await;

    let recorded = recorded.lock().unwrap().clone();
    assert_eq!(recorded.len(), 2, "one event per AssistantMessage");
    assert!(
        recorded
            .iter()
            .all(|event| event.bucket == UsageBucket::ModelCall)
    );
    assert_eq!(recorded[0].usage, usage(10, 2));
    assert!(recorded[0].turn.is_some());
    assert!(recorded[0].step.is_some());
    assert_eq!(recorded[1].usage, usage(20, 3));

    // Gate 1 runs before step 1; gate 2 runs after step 1's terminal landed.
    assert_eq!(*gate_sizes.lock().unwrap(), vec![0, 1]);
}

#[tokio::test]
async fn abort_with_usage_emits_partial_once() {
    let log = Arc::new(InMemoryEventLog::new());
    seed(&log, config(Vec::new())).await;
    submit_input(&log).await;
    let provider = Arc::new(ScriptedProvider::new("p").fail(
        ProviderError::new(ProviderErrorKind::Server, "boom").with_partial_usage(usage(7, 0)),
    ));
    let (hooks, recorded, _) = observing_hooks().await;
    let agent = agent(
        Arc::clone(&log),
        Arc::clone(&provider),
        Vec::new(),
        Some(hooks),
        None,
    );

    drive(agent.as_ref()).await;

    let recorded = recorded.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].bucket, UsageBucket::Partial);
    assert_eq!(recorded[0].usage, usage(7, 0));
    assert!(recorded[0].step.is_some());
}

#[tokio::test]
async fn abort_without_usage_emits_nothing() {
    let log = Arc::new(InMemoryEventLog::new());
    seed(&log, config(Vec::new())).await;
    submit_input(&log).await;
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .fail(ProviderError::new(ProviderErrorKind::InvalidRequest, "bad")),
    );
    let (hooks, recorded, _) = observing_hooks().await;
    let agent = agent(
        Arc::clone(&log),
        Arc::clone(&provider),
        Vec::new(),
        Some(hooks),
        None,
    );

    drive(agent.as_ref()).await;
    assert!(recorded.lock().unwrap().is_empty());
}

#[tokio::test]
async fn replay_does_not_emit_historical_usage() {
    let log = Arc::new(InMemoryEventLog::new());
    seed(&log, config(Vec::new())).await;
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
    append(
        &log,
        SessionEvent::AssistantMessage {
            turn_id: TURN,
            step_id: STEP,
            items: vec![assistant("done")],
            raw: None,
            model: model(),
            usage: usage(10, 2),
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
            reason: asterpath_protocol::TurnEndReason::Completed,
        },
        Some(dedup::turn_committed(TURN)),
    )
    .await;

    let provider = Arc::new(ScriptedProvider::new("p"));
    let (hooks, recorded, _) = observing_hooks().await;
    let agent = agent(
        Arc::clone(&log),
        Arc::clone(&provider),
        Vec::new(),
        Some(hooks),
        None,
    );

    drive(agent.as_ref()).await;

    assert!(
        recorded.lock().unwrap().is_empty(),
        "replay must not re-emit committed usage"
    );
    assert!(
        provider.requests().is_empty(),
        "idle session is not re-driven"
    );
}

#[tokio::test]
async fn crash_before_assistant_message_recalls_and_emits_once() {
    let log = Arc::new(InMemoryEventLog::new());
    seed(&log, config(Vec::new())).await;
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

    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![assistant("done")],
        StopReason::EndTurn,
        usage(20, 3),
    ));
    let (hooks, recorded, _) = observing_hooks().await;
    let agent = agent(
        Arc::clone(&log),
        Arc::clone(&provider),
        Vec::new(),
        Some(hooks),
        None,
    );

    drive(agent.as_ref()).await;

    let recorded = recorded.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].bucket, UsageBucket::ModelCall);
    assert_eq!(recorded[0].usage, usage(20, 3));
    assert_eq!(
        provider.requests().len(),
        1,
        "the lost call is retried once"
    );
}

#[tokio::test]
async fn idle_compaction_emits_once_with_no_turn() {
    let log = Arc::new(InMemoryEventLog::new());
    seed(&log, config(Vec::new())).await;
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
    append(
        &log,
        SessionEvent::AssistantMessage {
            turn_id: TURN,
            step_id: STEP,
            items: vec![assistant("done")],
            raw: None,
            model: model(),
            usage: usage(10, 2),
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
            reason: asterpath_protocol::TurnEndReason::Completed,
        },
        Some(dedup::turn_committed(TURN)),
    )
    .await;

    let (hooks, recorded, _) = observing_hooks().await;
    let agent = agent(
        Arc::clone(&log),
        Arc::new(ScriptedProvider::new("p")),
        Vec::new(),
        Some(hooks),
        Some(Arc::new(AlwaysCompact)),
    );

    agent.compact_now(SESSION).await.expect("compaction");

    let recorded = recorded.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].bucket, UsageBucket::Compaction);
    assert_eq!(recorded[0].usage, usage(50, 5));
    assert_eq!(recorded[0].turn, None, "idle compaction has no turn");
    assert_eq!(recorded[0].step, None);
}

#[tokio::test]
async fn in_turn_compaction_emits_with_its_turn() {
    let log = Arc::new(InMemoryEventLog::new());
    let mut seeded = config(Vec::new());
    seeded.compaction.max_auto_compactions_per_turn = 0;
    seed(&log, seeded).await;
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
    append(
        &log,
        SessionEvent::AssistantMessage {
            turn_id: TURN,
            step_id: STEP,
            items: vec![assistant("done")],
            raw: None,
            model: model(),
            usage: usage(10, 2),
            stop_reason: StopReason::EndTurn,
        },
        Some(dedup::assistant_message(TURN, STEP)),
    )
    .await;
    let frontier = append(
        &log,
        SessionEvent::StepCommitted {
            turn_id: TURN,
            step_id: STEP,
        },
        Some(dedup::step_committed(STEP)),
    )
    .await;
    // A crash left the compaction intent open; recovery redoes it.
    append(
        &log,
        SessionEvent::CompactionStarted {
            trigger: asterpath_protocol::CompactionTrigger::Threshold,
            replaces_from: frontier,
            caused_by: frontier,
        },
        Some(dedup::compaction(frontier)),
    )
    .await;

    let (hooks, recorded, _) = observing_hooks().await;
    let agent = agent(
        Arc::clone(&log),
        Arc::new(ScriptedProvider::new("p")),
        Vec::new(),
        Some(hooks),
        Some(Arc::new(AlwaysCompact)),
    );

    drive(agent.as_ref()).await;

    let recorded = recorded.lock().unwrap().clone();
    let compaction: Vec<_> = recorded
        .iter()
        .filter(|event| event.bucket == UsageBucket::Compaction)
        .collect();
    assert_eq!(compaction.len(), 1);
    assert_eq!(compaction[0].turn, Some(TURN));
    assert_eq!(compaction[0].step, None);

    // The terminal really landed once in the log.
    let mut entries = log.scan(asterpath_core::LogQuery {
        session: SESSION,
        after: None,
        limit: None,
    });
    let mut applied = 0;
    while let Some(entry) = entries.next().await {
        let entry = entry.expect("scan");
        if matches!(
            decode_entry(&entry).expect("decode"),
            SessionEvent::CompactionApplied { .. }
        ) {
            applied += 1;
        }
    }
    assert_eq!(applied, 1);
}
