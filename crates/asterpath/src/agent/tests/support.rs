//! Fixtures shared by the agent and tool pipeline tests.

use std::sync::Arc;
use std::time::Duration;

use crate::agent::Agent;
use crate::tools::ToolTable;
use crate::waits::Waits;
use asterpath_protocol::{
    AgentEvent, ApprovalPolicy, CompactionConfig, ConfigSnapshot, DefaultApprovalPolicy, Hooks,
    Item, JsonString, ModelId, ModelSpec, Parallelism, Part, Provenance, Provider, ProviderCatalog,
    ProviderId, ReplaySafety, RequestConfig, Role, RunRequest, RunTrigger, SessionEvent,
    SideEffect, SubagentConfig, Tool, ToolConfig, ToolDescriptor, ToolName, TurnConfig,
    decode_entry, encode_entry,
};
use asterpath_protocol::{
    ApprovalGate, DedupKey, LogEntry, LogId, SessionId, SessionStore, scan_all,
};
use asterpath_testing::{InMemoryEventLog, ManualClock, ScriptedProvider};
use futures_util::StreamExt;

pub(super) const SESSION: SessionId = SessionId::from_bytes([9; 16]);

pub(super) fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

pub(super) fn model() -> ModelSpec {
    ModelSpec {
        provider: ProviderId::new("p"),
        model: ModelId::new("model-1"),
        context_window: None,
    }
}

pub(super) fn config(tools: Vec<ToolDescriptor>) -> ConfigSnapshot {
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

/// A safe, parallel, read-only in-process tool.
pub(super) fn descriptor(name: &str) -> ToolDescriptor {
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
        strict: None,
    }
}

pub(super) fn tool_call(call_id: &str, name: &str, args: &str) -> Item {
    Item::ToolCall {
        call_id: call_id.to_owned(),
        provider_item_id: None,
        name: ToolName::new(name),
        raw_args: JsonString::new(args),
        raw: None,
    }
}

pub(super) fn assistant_text(value: &str) -> Vec<Item> {
    vec![Item::Message {
        role: Role::Assistant,
        parts: vec![text(value)],
        raw: None,
    }]
}

pub(super) fn tool_table(tools: Vec<Arc<dyn Tool>>) -> Arc<ToolTable> {
    let table = Arc::new(ToolTable::new());
    for tool in tools {
        table.register(tool);
    }
    table
}

pub(super) async fn append(
    log: &InMemoryEventLog,
    event: SessionEvent,
    key: Option<DedupKey>,
) -> LogId {
    let entry = encode_entry(SESSION, key, &event).expect("encode");
    log.append(entry).await.expect("append")
}

pub(super) async fn scan(log: &InMemoryEventLog) -> Vec<LogEntry> {
    scan_all(log, SESSION).await.expect("scan")
}

pub(super) async fn decoded(log: &InMemoryEventLog) -> Vec<SessionEvent> {
    scan(log)
        .await
        .iter()
        .map(|entry| decode_entry(entry).expect("decodes"))
        .collect()
}

/// An agent with the default approval policy over a closed gate.
pub(super) fn agent(
    log: Arc<dyn SessionStore>,
    provider: Arc<ScriptedProvider>,
    tools: Arc<ToolTable>,
    tool_config: ToolConfig,
    hooks: Vec<Arc<dyn Hooks>>,
) -> Agent {
    let gate = Arc::new(ApprovalGate::new());
    let policy = Arc::new(DefaultApprovalPolicy::new(Arc::clone(&gate)));
    agent_with(log, provider, tools, tool_config, policy, gate, None).with_hooks(hooks, None)
}

pub(super) fn agent_with(
    log: Arc<dyn SessionStore>,
    provider: Arc<ScriptedProvider>,
    tools: Arc<ToolTable>,
    tool_config: ToolConfig,
    approval: Arc<dyn ApprovalPolicy>,
    gate: Arc<ApprovalGate>,
    waits: Option<Arc<Waits>>,
) -> Agent {
    let waits = waits.unwrap_or_else(|| Arc::new(Waits::new(Arc::clone(&log))));
    let provider: Arc<dyn Provider> = provider;
    Agent::new(
        log,
        Arc::new(ProviderCatalog::new(vec![provider]).expect("catalog")),
        tools,
        tool_config,
        approval,
        gate,
        waits,
        Arc::new(ManualClock::new()),
    )
}

pub(super) async fn drive(agent: &Agent) -> Vec<AgentEvent> {
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
