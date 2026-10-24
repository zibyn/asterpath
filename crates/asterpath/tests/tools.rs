//! End-to-end tool pipeline through the facade: two-step turn (L1), tool
//! selection resolution and descriptors in the session snapshot.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use asterpath::protocol::{
    AgentEvent, CompactionConfig, Item, JsonString, ModelId, ModelSpec, Parallelism, Part,
    PartDelta, Provider, ProviderId, ReplaySafety, RequestConfig, Role, SessionEvent, SessionSpec,
    SideEffect, StopReason, StreamEvent, SubagentConfig, Tool, ToolArgs, ToolContext,
    ToolDescriptor, ToolError, ToolName, ToolOutput, ToolResultStatus, ToolSelection, TurnConfig,
    TurnEndReason, Usage, UserInput, decode_entry,
};
use asterpath::protocol::{InputId, LogEntry, LogQuery, SessionId, SessionStore};
use asterpath::{AsterError, Asterpath, SessionHandle};
use asterpath_testing::ScriptedProvider;
use futures_core::future::BoxFuture;
use futures_util::StreamExt;

static NEXT_DB: AtomicU64 = AtomicU64::new(0);

fn temp_db(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "asterpath-tools-{tag}-{}-{}.db",
        std::process::id(),
        NEXT_DB.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_file(&path);
    path
}

fn cleanup(path: &Path) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}-wal", path.display()));
    let _ = fs::remove_file(format!("{}-shm", path.display()));
}

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
        provenance: asterpath::protocol::Provenance::InProcess,
        timeout: None,
        max_output_bytes: None,
    }
}

struct EchoTool {
    descriptor: ToolDescriptor,
    calls: Arc<Mutex<Vec<ToolArgs>>>,
}

impl EchoTool {
    fn new(name: &str) -> Arc<Self> {
        Arc::new(Self {
            descriptor: descriptor(name),
            calls: Arc::new(Mutex::new(Vec::new())),
        })
    }
}

impl Tool for EchoTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        args: ToolArgs,
        _ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        self.calls.lock().unwrap().push(args);
        Box::pin(async { Ok(ToolOutput::new(vec![text("tool says hi")])) })
    }
}

fn spec(tools: ToolSelection) -> SessionSpec {
    SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("model-1"),
            context_window: None,
        },
        system: vec![text("be helpful")],
        tools,
        request: RequestConfig::default(),
        turn: TurnConfig {
            initial_backoff: std::time::Duration::ZERO,
            max_backoff: std::time::Duration::ZERO,
            ..TurnConfig::default()
        },
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
    }
}

fn tool_call(name: &str) -> Item {
    Item::ToolCall {
        call_id: "call-1".to_owned(),
        provider_item_id: None,
        name: ToolName::new(name),
        raw_args: JsonString::new(r#"{"q":"hi"}"#),
        raw: None,
    }
}

async fn scan(log: Arc<dyn SessionStore>, session: SessionId) -> Vec<LogEntry> {
    let mut stream = log.scan(LogQuery {
        session,
        after: None,
        limit: None,
    });
    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await {
        entries.push(entry.expect("scan succeeds"));
    }
    entries
}

fn kinds(entries: &[LogEntry]) -> Vec<&'static str> {
    entries
        .iter()
        .map(|entry| decode_entry(entry).expect("decodable").kind())
        .collect()
}

async fn drive_all(handle: &SessionHandle) -> Vec<AgentEvent> {
    let mut stream = handle.drive();
    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event.expect("drive succeeds"));
    }
    events
}

fn input(message: &str) -> UserInput {
    UserInput {
        id: InputId::from_bytes([1; 16]),
        parts: vec![text(message)],
    }
}

#[tokio::test]
async fn two_step_turn_with_a_tool_call_matches_l1_and_projects_the_result() {
    let db = temp_db("l1");
    let tool = EchoTool::new("echo");
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("echo")],
                StopReason::ToolUse,
                Usage::default(),
            )
            .respond(
                vec![Item::Message {
                    role: Role::Assistant,
                    parts: vec![text("done")],
                }],
                StopReason::EndTurn,
                Usage::default(),
            ),
    );
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![provider.clone() as Arc<dyn Provider>])
        .tools(vec![tool.clone() as Arc<dyn Tool>])
        .build()
        .await
        .expect("facade builds");
    let handle = asterpath
        .create_session(spec(ToolSelection::Names(vec![ToolName::new("echo")])))
        .await
        .unwrap();
    handle.submit_input(input("go")).await.unwrap();
    let live = drive_all(&handle).await;

    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(
        kinds(&entries),
        [
            "SessionCreated",
            "InputRecorded",
            "TurnStarted",
            "StepStarted",
            "AssistantMessage",
            "ToolCallRequested",
            "ToolResult",
            "StepCommitted",
            "StepStarted",
            "AssistantMessage",
            "StepCommitted",
            "TurnCommitted",
        ]
    );

    assert_eq!(
        *tool.calls.lock().unwrap(),
        vec![JsonString::new(r#"{"q":"hi"}"#)]
    );
    assert!(live.iter().any(|event| matches!(
        event,
        AgentEvent::ToolStarted { tool, .. } if tool.as_str() == "echo"
    )));
    assert!(live.iter().any(|event| matches!(
        event,
        AgentEvent::ToolFinished {
            status: ToolResultStatus::Completed,
            ..
        }
    )));
    assert_eq!(
        live.iter()
            .filter(|event| matches!(
                event,
                AgentEvent::TurnCommitted {
                    reason: TurnEndReason::Completed,
                    ..
                }
            ))
            .count(),
        1
    );

    // The session snapshot froze the resolved descriptor.
    let snapshot = entries
        .first()
        .map(|entry| decode_entry(entry).unwrap())
        .and_then(|event| match event {
            SessionEvent::SessionCreated { config, .. } => Some(config),
            _ => None,
        })
        .expect("snapshot");
    assert_eq!(snapshot.tools, vec![descriptor("echo")]);

    // The second request sees the projected ToolResult alongside the call.
    let second = &provider.requests()[1];
    let results: Vec<_> = second
        .items
        .iter()
        .filter_map(|item| match item {
            Item::ToolResult {
                call_id,
                parts,
                is_error,
                ..
            } => Some((call_id.as_str(), parts.clone(), *is_error)),
            _ => None,
        })
        .collect();
    assert_eq!(results, vec![("call-1", vec![text("tool says hi")], false)]);
    assert!(second.items.iter().any(|item| matches!(
        item,
        Item::ToolCall { call_id, .. } if call_id == "call-1"
    )));

    // Exactly one terminal result, bound to its call.
    let results: Vec<_> = entries
        .iter()
        .map(|entry| decode_entry(entry).unwrap())
        .filter(|event| matches!(event, SessionEvent::ToolResult { .. }))
        .collect();
    assert_eq!(results.len(), 1);

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn tool_selection_resolves_names_and_rejects_unknown_tools() {
    let db = temp_db("selection");
    let provider = Arc::new(ScriptedProvider::new("p"));
    let first = EchoTool::new("alpha");
    let second = EchoTool::new("beta");
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![provider as Arc<dyn Provider>])
        .tools(vec![first as Arc<dyn Tool>, second as Arc<dyn Tool>])
        .build()
        .await
        .expect("facade builds");

    let handle = asterpath
        .create_session(spec(ToolSelection::Names(vec![ToolName::new("beta")])))
        .await
        .unwrap();
    let entries = scan(asterpath.store(), handle.id()).await;
    let snapshot = entries
        .first()
        .map(|entry| decode_entry(entry).unwrap())
        .and_then(|event| match event {
            SessionEvent::SessionCreated { config, .. } => Some(config),
            _ => None,
        })
        .expect("snapshot");
    assert_eq!(snapshot.tools, vec![descriptor("beta")]);

    let all = asterpath
        .create_session(spec(ToolSelection::All))
        .await
        .unwrap();
    let entries = scan(asterpath.store(), all.id()).await;
    let snapshot = entries
        .first()
        .map(|entry| decode_entry(entry).unwrap())
        .and_then(|event| match event {
            SessionEvent::SessionCreated { config, .. } => Some(config),
            _ => None,
        })
        .expect("snapshot");
    // `All` also picks up the scheduler's built-in spawn tool.
    let names: Vec<_> = snapshot
        .tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect();
    assert_eq!(names, ["alpha", "beta", "spawn_agent"]);
    assert_eq!(
        &snapshot.tools[..2],
        &[descriptor("alpha"), descriptor("beta")]
    );

    let error = asterpath
        .create_session(spec(ToolSelection::Names(vec![ToolName::new("missing")])))
        .await
        .unwrap_err();
    assert!(matches!(error, AsterError::Config { .. }));

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn interleaved_json_args_fragments_reassemble_by_step_and_output_index() {
    let call = |id: &str, args: &str| Item::ToolCall {
        call_id: id.to_owned(),
        provider_item_id: None,
        name: ToolName::new("echo"),
        raw_args: JsonString::new(args),
        raw: None,
    };
    let fragment = |sequence, output_index, fragment: &str| StreamEvent::PartDelta {
        sequence,
        output_index,
        part_index: output_index + 10,
        delta: PartDelta::JsonArgs {
            fragment: fragment.to_owned(),
        },
    };
    // Sparse indices completed out of order: the agent must report
    // `output_index`, not the item's position in the step.
    let db = temp_db("interleaved");
    let tool = EchoTool::new("echo");
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .stream_events(vec![
                StreamEvent::ResponseStarted {
                    sequence: 0,
                    request_id: None,
                },
                StreamEvent::ItemStarted {
                    sequence: 1,
                    output_index: 3,
                },
                StreamEvent::ItemStarted {
                    sequence: 2,
                    output_index: 7,
                },
                fragment(3, 7, r#"{"b":"#),
                fragment(4, 3, r#"{"a":"#),
                fragment(5, 3, "1}"),
                fragment(6, 7, "2}"),
                StreamEvent::ItemCompleted {
                    sequence: 7,
                    output_index: 7,
                    item: call("call-b", r#"{"b":2}"#),
                },
                StreamEvent::ItemCompleted {
                    sequence: 8,
                    output_index: 3,
                    item: call("call-a", r#"{"a":1}"#),
                },
                StreamEvent::ResponseEnded {
                    sequence: 9,
                    stop_reason: StopReason::ToolUse,
                    usage: Usage::default(),
                },
            ])
            .respond(
                vec![Item::Message {
                    role: Role::Assistant,
                    parts: vec![text("done")],
                }],
                StopReason::EndTurn,
                Usage::default(),
            ),
    );
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![provider as Arc<dyn Provider>])
        .tools(vec![tool as Arc<dyn Tool>])
        .build()
        .await
        .expect("facade builds");
    let handle = asterpath
        .create_session(spec(ToolSelection::Names(vec![ToolName::new("echo")])))
        .await
        .unwrap();
    handle.submit_input(input("go")).await.unwrap();
    let live = drive_all(&handle).await;

    let mut fragments: HashMap<_, String> = HashMap::new();
    let mut indices = Vec::new();
    let mut started = Vec::new();
    for event in &live {
        match event {
            AgentEvent::ModelDelta {
                step,
                output_index,
                part_index,
                delta: PartDelta::JsonArgs { fragment },
                ..
            } => {
                indices.push((*output_index, *part_index));
                fragments
                    .entry((*step, *output_index))
                    .or_default()
                    .push_str(fragment);
            }
            AgentEvent::ToolStarted {
                step,
                output_index,
                call,
                ..
            } => started.push(((*step, *output_index), *call)),
            _ => {}
        }
    }
    // Indices pass through from the provider's PartDelta unchanged.
    assert_eq!(indices, [(7, 17), (3, 13), (3, 13), (7, 17)]);
    assert_eq!(started.len(), 2);

    // Each call's recorded arguments, by its kernel id.
    let entries = scan(asterpath.store(), handle.id()).await;
    let args: HashMap<_, _> = entries
        .iter()
        .filter_map(|entry| match decode_entry(entry).unwrap() {
            SessionEvent::ToolCallRequested {
                tool_call_id,
                raw_args,
                ..
            } => Some((tool_call_id, raw_args)),
            _ => None,
        })
        .collect();
    for (key, call) in started {
        assert_eq!(fragments[&key], args[&call].as_str());
    }

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}
