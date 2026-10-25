//! Session environments (ADR-0037): tools and spawn settings resolved per
//! session from session metadata; children use their root's.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use asterpath::protocol::{
    CompactionConfig, EnvironmentAdapter, EnvironmentError, InputId, Item, JsonString, ModelId,
    ModelSpec, Part, Provider, ProviderId, RequestConfig, Role, SessionEnvironment, SessionEvent,
    SessionId, SessionSpec, SpawnEnv, StopReason, SubagentConfig, Tool, ToolArgs, ToolContext,
    ToolDescriptor, ToolError, ToolName, ToolOutput, ToolSelection, TurnConfig, Usage, UserInput,
    decode_entry, effective_config, scan_all,
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

/// Reports the root it was built for and the session's `GREETING`.
struct WhereTool {
    root: String,
    descriptor: ToolDescriptor,
}

impl Tool for WhereTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        _args: ToolArgs,
        ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        let greeting = ctx
            .spawn
            .vars
            .get(std::ffi::OsStr::new("GREETING"))
            .map(|value| value.to_string_lossy().into_owned())
            .unwrap_or_default();
        let answer = format!("{} {greeting}", self.root);
        Box::pin(async move { Ok(ToolOutput::new(vec![text(&answer)])) })
    }
}

/// `root` metadata names the root; `description` is what the tool says
/// about itself on the next resolution.
struct ByRoot {
    description: Mutex<String>,
}

impl EnvironmentAdapter for ByRoot {
    fn resolve<'a>(
        &'a self,
        metadata: &'a BTreeMap<String, String>,
    ) -> BoxFuture<'a, Result<SessionEnvironment, EnvironmentError>> {
        let root = metadata.get("root").cloned();
        let description = self.description.lock().unwrap().clone();
        Box::pin(async move {
            let root = root.ok_or_else(|| EnvironmentError::new("no root"))?;
            let tool = WhereTool {
                descriptor: ToolDescriptor::new(
                    ToolName::new("where"),
                    description,
                    serde_json::json!({"type": "object"}),
                ),
                root: root.clone(),
            };
            let mut spawn = SpawnEnv::default();
            spawn.vars.insert("GREETING".into(), root.into());
            Ok(SessionEnvironment::new(vec![Arc::new(tool)], spawn))
        })
    }
}

fn spec(root: &str) -> SessionSpec {
    SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("model-1"),
            context_window: None,
        },
        system: Vec::new(),
        tools: ToolSelection::All,
        request: RequestConfig::default(),
        turn: {
            let mut config = TurnConfig::default();
            config.initial_backoff = Duration::ZERO;
            config.max_backoff = Duration::ZERO;
            config
        },
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
        metadata: BTreeMap::from([("root".to_owned(), root.to_owned())]),
    }
}

fn call(name: &str, args: &str) -> Vec<Item> {
    vec![Item::ToolCall {
        call_id: "call-1".to_owned(),
        provider_item_id: None,
        name: ToolName::new(name),
        raw_args: JsonString::new(args),
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

async fn build(provider: ScriptedProvider, adapter: Arc<ByRoot>) -> Asterpath {
    Asterpath::builder()
        .store(Arc::new(InMemoryEventLog::new()))
        .provider(Arc::new(provider) as Arc<dyn Provider>)
        .environment(adapter)
        .build()
        .await
        .expect("facade builds")
}

fn adapter(description: &str) -> Arc<ByRoot> {
    Arc::new(ByRoot {
        description: Mutex::new(description.to_owned()),
    })
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

/// Every `where` result in the session's log.
async fn answers(asterpath: &Asterpath, session: SessionId) -> Vec<String> {
    events(asterpath, session)
        .await
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::ToolResult {
                output: Some(parts),
                ..
            } => match &parts[..] {
                [Part::Text { text }] => Some(text.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn sessions_of_one_asterpath_use_their_own_roots() {
    let provider = ScriptedProvider::new("p")
        .respond(call("where", "{}"), StopReason::ToolUse, Usage::default())
        .respond(say("a done"), StopReason::EndTurn, Usage::default())
        .respond(call("where", "{}"), StopReason::ToolUse, Usage::default())
        .respond(say("b done"), StopReason::EndTurn, Usage::default());
    let asterpath = build(provider, adapter("v1")).await;

    let a = asterpath.create_session(spec("/a")).await.unwrap();
    let b = asterpath.create_session(spec("/b")).await.unwrap();
    run(&a.claim().await.unwrap()).await;
    run(&b.claim().await.unwrap()).await;

    assert_eq!(answers(&asterpath, a.id()).await, ["/a /a"]);
    assert_eq!(answers(&asterpath, b.id()).await, ["/b /b"]);
}

#[tokio::test]
async fn a_child_session_uses_its_parents_environment() {
    let provider = ScriptedProvider::new("p")
        .respond(
            call("spawn_agent", r#"{"task":"look around"}"#),
            StopReason::ToolUse,
            Usage::default(),
        )
        .respond(call("where", "{}"), StopReason::ToolUse, Usage::default())
        .respond(say("child done"), StopReason::EndTurn, Usage::default())
        .respond(say("parent done"), StopReason::EndTurn, Usage::default());
    let asterpath = build(provider, adapter("v1")).await;
    let parent = asterpath.create_session(spec("/a")).await.unwrap();
    run(&parent.claim().await.unwrap()).await;

    let child = events(&asterpath, parent.id())
        .await
        .into_iter()
        .find_map(|event| match event {
            SessionEvent::ChildSessionSpawned { child, .. } => Some(child),
            _ => None,
        })
        .expect("a child was spawned");
    assert_eq!(answers(&asterpath, child).await, ["/a /a"]);
}

#[tokio::test]
async fn a_changed_tool_on_reopen_is_a_tool_change() {
    let adapter = adapter("v1");
    let store = Arc::new(InMemoryEventLog::new());
    let asterpath = Asterpath::builder()
        .store(store.clone())
        .provider(Arc::new(ScriptedProvider::new("p")) as Arc<dyn Provider>)
        .environment(adapter.clone())
        .build()
        .await
        .unwrap();
    let session = asterpath.create_session(spec("/a")).await.unwrap().id();
    drop(asterpath);

    *adapter.description.lock().unwrap() = "v2".to_owned();
    let asterpath = Asterpath::builder()
        .store(store)
        .provider(Arc::new(ScriptedProvider::new("p")) as Arc<dyn Provider>)
        .environment(adapter)
        .build()
        .await
        .unwrap();
    let owned = asterpath
        .open_session(session)
        .await
        .unwrap()
        .claim()
        .await
        .unwrap();

    let entries = scan_all(&*asterpath.store(), owned.id()).await.unwrap();
    let decoded: Vec<_> = entries
        .into_iter()
        .map(|entry| {
            let event = decode_entry(&entry).unwrap();
            (entry, event)
        })
        .collect();
    let config = effective_config(&decoded, u64::MAX).unwrap();
    let descriptions: Vec<_> = config
        .tools
        .iter()
        .filter(|tool| tool.name.as_str() == "where")
        .map(|tool| tool.description.as_str())
        .collect();
    assert_eq!(descriptions, ["v2"]);
}
