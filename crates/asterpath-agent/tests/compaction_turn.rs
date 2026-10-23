//! `CompactionStarted.turn` (#74): in-turn compaction records its turn, idle
//! compaction records none. Live usage observation is `Hooks::on_record`
//! (ADR-0025), covered by the facade tests.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use asterpath_core::{
    ApprovalGate, AwaitSignal, DedupKey, DurableExecutor, EventLog, ExecutionError, LogId,
    SessionId, SignalDelivery, SignalPayload, StepId, TurnId, WakeCause,
};
use asterpath_protocol::{
    AgentEvent, AgentRunner, CompactPlan, CompactSummary, CompactionConfig, CompactionError,
    CompactionJob, CompactionOutcome, ConfigSnapshot, DefaultApprovalPolicy, InputTokens, Item,
    ModelId, ModelSpec, OutputTokens, Part, Provider, ProviderCatalog, ProviderId, RequestConfig,
    Role, RunRequest, RunTrigger, SessionEvent, StopReason, SubagentConfig, Tool, ToolConfig,
    ToolDescriptor, ToolName, ToolRegistry, TurnConfig, Usage, decode_entry, dedup, encode_entry,
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

// --- tools and executor ---------------------------------------------------

struct Registry(Mutex<HashMap<ToolName, Arc<dyn Tool>>>);

impl ToolRegistry for Registry {
    fn register(&self, tool: Arc<dyn Tool>) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(tool.descriptor().name.clone(), tool);
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
async fn idle_compaction_intent_records_no_turn() {
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

    let agent = agent(
        Arc::clone(&log),
        Arc::new(ScriptedProvider::new("p")),
        Vec::new(),
        Some(Arc::new(AlwaysCompact)),
    );

    agent.compact_now(SESSION).await.expect("compaction");

    assert_eq!(
        started_turns(&log).await,
        vec![None],
        "intent records no turn"
    );
}

/// `turn` of every `CompactionStarted`, in log order.
async fn started_turns(log: &Arc<InMemoryEventLog>) -> Vec<Option<TurnId>> {
    let mut entries = log.scan(asterpath_core::LogQuery {
        session: SESSION,
        after: None,
        limit: None,
    });
    let mut turns = Vec::new();
    while let Some(entry) = entries.next().await {
        if let SessionEvent::CompactionStarted { turn, .. } =
            decode_entry(&entry.expect("scan")).expect("decode")
        {
            turns.push(turn);
        }
    }
    turns
}

#[tokio::test]
async fn threshold_compaction_intent_records_its_turn() {
    let log = Arc::new(InMemoryEventLog::new());
    seed(&log, config(Vec::new())).await;
    submit_input(&log).await;
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![assistant("done")],
        StopReason::EndTurn,
        usage(10, 2),
    ));
    let agent = agent(
        Arc::clone(&log),
        provider,
        Vec::new(),
        Some(Arc::new(AlwaysCompact)),
    );

    drive(agent.as_ref()).await;

    let mut entries = log.scan(asterpath_core::LogQuery {
        session: SESSION,
        after: None,
        limit: None,
    });
    let mut opened = None;
    while let Some(entry) = entries.next().await {
        if let SessionEvent::TurnStarted { turn_id } =
            decode_entry(&entry.expect("scan")).expect("decode")
        {
            opened = Some(turn_id);
        }
    }
    assert!(opened.is_some());
    assert_eq!(started_turns(&log).await, vec![opened]);
}
