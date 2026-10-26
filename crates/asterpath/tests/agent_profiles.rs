//! Sub-agents select application agent profiles by name (ADR-0053): the
//! session environment lists them, `spawn_agent`'s `agent` picks one.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use asterpath::protocol::{
    AgentProfile, CompactionConfig, ConfigSnapshot, ContextPolicy, EnvironmentAdapter,
    EnvironmentError, InputId, Item, JsonString, LogId, ModelAlias, ModelId, ModelSpec, Part,
    Provider, ProviderId, RequestConfig, Role, SessionEnvironment, SessionEvent, SessionId,
    SessionSpec, SpawnEnv, StopReason, SubagentConfig, Tool, ToolArgs, ToolContext, ToolDescriptor,
    ToolError, ToolErrorKind, ToolName, ToolOutput, ToolSelection, TurnConfig, Usage, UserInput,
    decode_entry, scan_all,
};
use asterpath::{Asterpath, OwnedSession};
use asterpath_testing::{InMemoryEventLog, ScriptedProvider};
use futures_core::future::BoxFuture;
use futures_util::StreamExt;

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

struct Noop(ToolDescriptor);

impl Tool for Noop {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.0
    }

    fn invoke(
        &self,
        _args: ToolArgs,
        _ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async { Ok(ToolOutput::new(vec![text("ok")])) })
    }
}

fn noop(name: &str) -> Arc<dyn Tool> {
    Arc::new(Noop(ToolDescriptor::new(
        ToolName::new(name),
        String::new(),
        serde_json::json!({"type": "object"}),
    )))
}

/// Tools `read` and `write`, and whatever profiles are set at resolve time.
struct Profiles(Mutex<Vec<AgentProfile>>);

impl EnvironmentAdapter for Profiles {
    fn resolve<'a>(
        &'a self,
        _metadata: &'a BTreeMap<String, String>,
    ) -> BoxFuture<'a, Result<SessionEnvironment, EnvironmentError>> {
        let agents = self.0.lock().unwrap().clone();
        Box::pin(async move {
            let mut env =
                SessionEnvironment::new(vec![noop("read"), noop("write")], SpawnEnv::default());
            env.agents = agents;
            Ok(env)
        })
    }
}

fn model(provider: &str, model: &str, context_window: Option<u32>) -> ModelSpec {
    ModelSpec {
        provider: ProviderId::new(provider),
        model: ModelId::new(model),
        context_window,
    }
}

fn profile(name: &str) -> AgentProfile {
    AgentProfile::new(
        name,
        format!("the {name} agent"),
        vec![text(&format!("{name} system"))],
    )
}

fn spec(subagents: SubagentConfig) -> SessionSpec {
    SessionSpec {
        model: model("p", "model-1", Some(1000)),
        system: vec![text("parent system")],
        tools: ToolSelection::All,
        request: RequestConfig::default(),
        turn: {
            let mut config = TurnConfig::default();
            config.initial_backoff = Duration::ZERO;
            config.max_backoff = Duration::ZERO;
            config
        },
        compaction: CompactionConfig::default(),
        subagents,
        metadata: BTreeMap::new(),
    }
}

fn spawn(args: serde_json::Value) -> Vec<Item> {
    vec![Item::ToolCall {
        call_id: "call-1".to_owned(),
        provider_item_id: None,
        name: ToolName::new("spawn_agent"),
        raw_args: JsonString::new(args.to_string()),
        raw: None,
    }]
}

fn say(value: &str) -> Vec<Item> {
    vec![Item::Message {
        role: Role::Assistant,
        parts: vec![text(value)],
        raw: None,
    }]
}

/// A parent that calls `spawn_agent` with `args`; the child (if any) and the
/// parent then end their turns.
fn script(args: serde_json::Value) -> ScriptedProvider {
    ScriptedProvider::new("p")
        .respond(spawn(args), StopReason::ToolUse, Usage::default())
        .respond(say("child done"), StopReason::EndTurn, Usage::default())
        .respond(say("parent done"), StopReason::EndTurn, Usage::default())
}

async fn build(
    provider: ScriptedProvider,
    store: Arc<InMemoryEventLog>,
    adapter: Arc<Profiles>,
) -> Asterpath {
    Asterpath::builder()
        .store(store)
        .provider(Arc::new(provider) as Arc<dyn Provider>)
        .model_aliases(BTreeMap::from([(
            ModelAlias::new("fast"),
            model("p", "fast-model", None),
        )]))
        .environment(adapter)
        .build()
        .await
        .expect("facade builds")
}

async fn run(handle: &OwnedSession) {
    handle
        .submit_input(UserInput {
            id: InputId::from_bytes([1; 16]),
            parts: vec![text("go")],
        })
        .await
        .unwrap();
    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        event.expect("drive succeeds");
    }
}

async fn events(asterpath: &Asterpath, session: SessionId) -> Vec<SessionEvent> {
    scan_all(&*asterpath.store(), session)
        .await
        .unwrap()
        .iter()
        .map(|entry| decode_entry(entry).unwrap())
        .collect()
}

#[allow(clippy::large_enum_variant)] // test-only result type
enum Spawned {
    Child {
        spawned: ConfigSnapshot,
        created: ConfigSnapshot,
        child: Vec<SessionEvent>,
    },
    Rejected(String),
}

/// Runs a parent whose environment offers `agents` and returns what its one
/// `spawn_agent` call did.
async fn spawn_with(
    agents: Vec<AgentProfile>,
    subagents: SubagentConfig,
    args: serde_json::Value,
) -> Spawned {
    let adapter = Arc::new(Profiles(Mutex::new(agents)));
    let asterpath = build(script(args), Arc::new(InMemoryEventLog::new()), adapter).await;
    let parent = asterpath.create_session(spec(subagents)).await.unwrap();
    run(&parent.claim().await.unwrap()).await;
    let parent = events(&asterpath, parent.id()).await;
    let spawned = parent.iter().find_map(|event| match event {
        SessionEvent::ChildSessionSpawned { child, config, .. } => Some((*child, config.clone())),
        _ => None,
    });
    let Some((child, spawned)) = spawned else {
        let error = parent
            .iter()
            .find_map(|event| match event {
                SessionEvent::ToolResult {
                    error: Some(error), ..
                } => Some(error.clone()),
                _ => None,
            })
            .expect("a rejected spawn has a failed tool result");
        assert_eq!(error.kind, ToolErrorKind::InvalidArgs);
        return Spawned::Rejected(error.message);
    };
    let child = events(&asterpath, child).await;
    let SessionEvent::SessionCreated { config, .. } = &child[0] else {
        panic!("the child starts with SessionCreated");
    };
    Spawned::Child {
        spawned,
        created: config.clone(),
        child,
    }
}

fn child_config(spawned: Spawned) -> ConfigSnapshot {
    match spawned {
        Spawned::Child {
            spawned, created, ..
        } => {
            assert_eq!(created, spawned);
            created
        }
        Spawned::Rejected(message) => panic!("spawn rejected: {message}"),
    }
}

fn rejected(spawned: Spawned) -> String {
    match spawned {
        Spawned::Rejected(message) => message,
        Spawned::Child { .. } => panic!("spawn created a child"),
    }
}

fn names(config: &ConfigSnapshot) -> Vec<&str> {
    config.tools.iter().map(|tool| tool.name.as_str()).collect()
}

#[tokio::test]
async fn a_selected_profile_replaces_the_system_and_narrows_the_tools() {
    let mut reviewer = profile("reviewer");
    reviewer.tools = Some(vec![ToolName::new("read"), ToolName::new("ghost")]);
    let config = child_config(
        spawn_with(
            vec![reviewer],
            SubagentConfig::default(),
            serde_json::json!({"task": "review", "agent": "reviewer"}),
        )
        .await,
    );

    assert_eq!(config.system, vec![text("reviewer system")]);
    assert_eq!(names(&config), ["read"]);
    assert_eq!(config.model, model("p", "model-1", Some(1000)));
}

#[tokio::test]
async fn an_unknown_profile_is_invalid_and_creates_no_child() {
    let message = rejected(
        spawn_with(
            vec![profile("reviewer")],
            SubagentConfig::default(),
            serde_json::json!({"task": "review", "agent": "writer"}),
        )
        .await,
    );
    assert!(message.contains("writer"), "{message}");
}

#[tokio::test]
async fn a_profile_model_must_share_the_parents_provider() {
    let mut other = profile("other");
    other.model = Some(model("q", "model-1", None));
    let message = rejected(
        spawn_with(
            vec![other],
            SubagentConfig::default(),
            serde_json::json!({"task": "t", "agent": "other"}),
        )
        .await,
    );
    assert!(message.contains("provider q"), "{message}");

    let mut fast = profile("fast");
    fast.model = Some(model("p", "model-2", None));
    let config = child_config(
        spawn_with(
            vec![fast],
            SubagentConfig::default(),
            serde_json::json!({"task": "t", "agent": "fast"}),
        )
        .await,
    );
    assert_eq!(config.model, model("p", "model-2", None));
}

#[tokio::test]
async fn a_profile_with_a_model_takes_no_model_alias() {
    let mut pinned = profile("pinned");
    pinned.model = Some(model("p", "model-2", None));
    let message = rejected(
        spawn_with(
            vec![pinned],
            SubagentConfig::default(),
            serde_json::json!({"task": "t", "agent": "pinned", "model_alias": "fast"}),
        )
        .await,
    );
    assert!(message.contains("model_alias"), "{message}");

    let config = child_config(
        spawn_with(
            vec![profile("free")],
            SubagentConfig::default(),
            serde_json::json!({"task": "t", "agent": "free", "model_alias": "fast"}),
        )
        .await,
    );
    assert_eq!(config.model, model("p", "fast-model", None));
}

#[tokio::test]
async fn the_allowlist_still_narrows_a_profile_and_rejects_unknown_names() {
    let mut reviewer = profile("reviewer");
    reviewer.tools = Some(vec![ToolName::new("read"), ToolName::new("write")]);
    let config = child_config(
        spawn_with(
            vec![reviewer.clone()],
            SubagentConfig::default(),
            serde_json::json!({"task": "t", "agent": "reviewer", "tool_allowlist": ["write", "spawn_agent"]}),
        )
        .await,
    );
    assert_eq!(names(&config), ["write"]);

    let message = rejected(
        spawn_with(
            vec![reviewer],
            SubagentConfig::default(),
            serde_json::json!({"task": "t", "agent": "reviewer", "tool_allowlist": ["ghost"]}),
        )
        .await,
    );
    assert!(message.contains("ghost"), "{message}");
}

#[tokio::test]
async fn a_profile_child_is_fresh_under_a_fork_default() {
    let mut subagents = SubagentConfig::default();
    subagents.default_context = ContextPolicy::Fork {
        at: LogId::from_i64(0),
    };
    let forked = |events: &[SessionEvent]| {
        events
            .iter()
            .any(|event| matches!(event, SessionEvent::ContextForked { .. }))
    };

    let Spawned::Child { created, child, .. } = spawn_with(
        vec![profile("reviewer")],
        subagents.clone(),
        serde_json::json!({"task": "t", "agent": "reviewer"}),
    )
    .await
    else {
        panic!("profile spawn rejected");
    };
    assert!(!forked(&child));
    assert_eq!(created.system, vec![text("reviewer system")]);

    let Spawned::Child { child, .. } = spawn_with(
        vec![profile("reviewer")],
        subagents,
        serde_json::json!({"task": "t"}),
    )
    .await
    else {
        panic!("plain spawn rejected");
    };
    assert!(forked(&child));
}

#[tokio::test]
async fn without_a_profile_the_child_inherits_the_parent_system() {
    let config = child_config(
        spawn_with(
            vec![profile("reviewer")],
            SubagentConfig::default(),
            serde_json::json!({"task": "t"}),
        )
        .await,
    );
    assert_eq!(config.system, vec![text("parent system")]);
}

#[tokio::test]
async fn spawn_agent_has_one_description_whatever_the_profiles() {
    let descriptor = |config: &ConfigSnapshot| {
        config
            .tools
            .iter()
            .find(|tool| tool.name.as_str() == "spawn_agent")
            .cloned()
            .expect("spawn_agent is offered")
    };
    let mut seen = Vec::new();
    for agents in [vec![], vec![profile("reviewer"), profile("writer")]] {
        let adapter = Arc::new(Profiles(Mutex::new(agents)));
        let asterpath = build(
            ScriptedProvider::new("p"),
            Arc::new(InMemoryEventLog::new()),
            adapter,
        )
        .await;
        let session = asterpath
            .create_session(spec(SubagentConfig::default()))
            .await
            .unwrap();
        let SessionEvent::SessionCreated { config, .. } =
            &events(&asterpath, session.id()).await[0]
        else {
            panic!("a session starts with SessionCreated");
        };
        seen.push(descriptor(config));
    }
    assert_eq!(seen[0], seen[1]);
    assert!(seen[0].schema["properties"]["agent"].is_object());
}
