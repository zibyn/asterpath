//! `asterpath-tools` through the engine: one tool set per project root, one
//! `Asterpath` per project (#83).
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use asterpath::{Asterpath, SessionHandle};
use asterpath_protocol::{
    CompactionConfig, Item, JsonString, LogEntry, ModelId, ModelSpec, Part, Provider, ProviderId,
    RequestConfig, Role, SandboxPolicy, SessionEvent, SessionSpec, StopReason, SubagentConfig,
    Tool, ToolName, ToolResultStatus, ToolSelection, TurnConfig, Usage, UserInput, decode_entry,
    scan_all,
};
use asterpath_protocol::{InputId, SessionId};
use asterpath_testing::{Failpoint, FailpointTool, Point, ScriptedProvider};
use futures_util::StreamExt;

struct Project {
    _dir: tempfile::TempDir,
    root: PathBuf,
    db: PathBuf,
}

impl Project {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let root = dir.path().join("project");
        std::fs::create_dir(&root).expect("root");
        let db = dir.path().join("sessions.sqlite");
        Self {
            _dir: dir,
            root,
            db,
        }
    }

    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        asterpath_tools::coding(self.root.clone(), None, SandboxPolicy::FullAccess)
            .expect("absolute root")
    }

    async fn open(&self, provider: Arc<ScriptedProvider>, tools: Vec<Arc<dyn Tool>>) -> Asterpath {
        Asterpath::builder()
            .database(&self.db)
            .providers(vec![provider as Arc<dyn Provider>])
            .tools(tools)
            .build()
            .await
            .expect("build")
    }
}

fn spec() -> SessionSpec {
    SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("model-1"),
            context_window: None,
        },
        system: Vec::new(),
        tools: ToolSelection::All,
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

fn call(id: &str, name: &str, args: serde_json::Value) -> Vec<Item> {
    vec![Item::ToolCall {
        call_id: id.to_owned(),
        provider_item_id: None,
        name: ToolName::new(name),
        raw_args: JsonString::new(args.to_string()),
        raw: None,
    }]
}

fn done() -> Vec<Item> {
    vec![Item::Message {
        role: Role::Assistant,
        parts: vec![Part::Text {
            text: "done".to_owned(),
        }],
        raw: None,
    }]
}

fn script(steps: Vec<Vec<Item>>) -> Arc<ScriptedProvider> {
    let mut provider = ScriptedProvider::new("p");
    for items in steps {
        let stop = if matches!(items[0], Item::ToolCall { .. }) {
            StopReason::ToolUse
        } else {
            StopReason::EndTurn
        };
        provider = provider.respond(items, stop, Usage::default());
    }
    Arc::new(provider)
}

async fn start(asterpath: &Asterpath, input: u8) -> SessionHandle {
    let handle = asterpath.create_session(spec()).await.expect("session");
    handle
        .submit_input(UserInput {
            id: InputId::from_bytes([input; 16]),
            parts: vec![Part::Text {
                text: "go".to_owned(),
            }],
        })
        .await
        .expect("input");
    handle
}

async fn drive_all(handle: &SessionHandle) {
    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        event.expect("drive");
    }
}

async fn entries(asterpath: &Asterpath, session: SessionId) -> Vec<LogEntry> {
    scan_all(&*asterpath.store(), session).await.expect("scan")
}

/// `(status, text output)` of every `ToolResult` in log order.
fn tool_results(entries: &[LogEntry]) -> Vec<(ToolResultStatus, String)> {
    entries
        .iter()
        .filter_map(|entry| match decode_entry(entry).ok()? {
            SessionEvent::ToolResult { status, output, .. } => Some((
                status,
                output
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|part| match part {
                        Part::Text { text } => Some(text.clone()),
                        _ => None,
                    })
                    .collect(),
            )),
            _ => None,
        })
        .collect()
}

fn read(root: &Path, name: &str) -> Option<String> {
    std::fs::read_to_string(root.join(name)).ok()
}

#[tokio::test]
async fn read_edit_read_round_trips_through_the_log() {
    let project = Project::new();
    std::fs::write(project.root.join("notes.txt"), "hello world\n").unwrap();
    let provider = script(vec![
        call(
            "c1",
            "read_file",
            serde_json::json!({ "path": "notes.txt" }),
        ),
        call(
            "c2",
            "edit_file",
            serde_json::json!({ "path": "notes.txt", "old_string": "world", "new_string": "tools" }),
        ),
        call(
            "c3",
            "read_file",
            serde_json::json!({ "path": "notes.txt" }),
        ),
        done(),
    ]);
    let asterpath = project.open(provider, project.tools()).await;
    let handle = start(&asterpath, 1).await;

    drive_all(&handle).await;

    let results = tool_results(&entries(&asterpath, handle.id()).await);
    assert_eq!(
        results,
        vec![
            (ToolResultStatus::Completed, "hello world\n".to_owned()),
            (ToolResultStatus::Completed, "edited notes.txt".to_owned()),
            (ToolResultStatus::Completed, "hello tools\n".to_owned()),
        ]
    );
    asterpath.shutdown().await.unwrap();
}

#[tokio::test]
async fn each_instance_only_touches_its_own_root_and_children_share_the_parent_root() {
    let a = Project::new();
    let b = Project::new();
    // Parent spawns a child; the child writes; the child ends; the parent ends.
    let provider_a = script(vec![
        call(
            "s1",
            "spawn_agent",
            serde_json::json!({ "task": "write it" }),
        ),
        call(
            "w1",
            "write_file",
            serde_json::json!({ "path": "child.txt", "content": "from child" }),
        ),
        done(),
        done(),
    ]);
    let provider_b = script(vec![
        call(
            "w2",
            "write_file",
            serde_json::json!({ "path": "b.txt", "content": "from b" }),
        ),
        done(),
    ]);
    let instance_a = a.open(provider_a, a.tools()).await;
    let instance_b = b.open(provider_b, b.tools()).await;
    let session_a = start(&instance_a, 1).await;
    let session_b = start(&instance_b, 2).await;

    drive_all(&session_a).await;
    drive_all(&session_b).await;

    assert_eq!(read(&a.root, "child.txt").as_deref(), Some("from child"));
    assert_eq!(read(&b.root, "b.txt").as_deref(), Some("from b"));
    assert_eq!(read(&a.root, "b.txt"), None);
    assert_eq!(read(&b.root, "child.txt"), None);
    let parent = tool_results(&entries(&instance_a, session_a.id()).await);
    assert_eq!(parent.len(), 1, "only spawn_agent ran in the parent");
    assert_eq!(parent[0].0, ToolResultStatus::Completed);

    instance_a.shutdown().await.unwrap();
    instance_b.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_unsafe_tool_interrupted_by_a_crash_is_not_rerun() {
    let project = Project::new();
    std::fs::write(project.root.join("count.txt"), "a").unwrap();
    let edit = serde_json::json!({ "path": "count.txt", "old_string": "a", "new_string": "ab" });

    // First process: the edit lands, then the process hangs before reporting.
    let tools: Vec<Arc<dyn Tool>> = project
        .tools()
        .into_iter()
        .map(|tool| {
            if tool.descriptor().name.as_str() == "edit_file" {
                Arc::new(FailpointTool::new(
                    tool,
                    Failpoint::halt(Point::ToolAfterEffect),
                )) as Arc<dyn Tool>
            } else {
                tool
            }
        })
        .collect();
    let asterpath = project
        .open(script(vec![call("e1", "edit_file", edit)]), tools)
        .await;
    let handle = start(&asterpath, 1).await;
    let session = handle.id();
    let driver = tokio::spawn({
        let handle = handle.clone();
        async move { drive_all(&handle).await }
    });
    while read(&project.root, "count.txt").as_deref() != Some("ab") {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    driver.abort();
    let _ = driver.await;
    drop(handle);
    asterpath.shutdown().await.unwrap();

    // Second process: the provider can only answer the follow-up step.
    let reopened = project.open(script(vec![done()]), project.tools()).await;
    let handle = reopened.open_session(session).await.unwrap();
    drive_all(&handle).await;

    assert_eq!(read(&project.root, "count.txt").as_deref(), Some("ab"));
    let results = tool_results(&entries(&reopened, session).await);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, ToolResultStatus::Interrupted);
    reopened.shutdown().await.unwrap();
}
