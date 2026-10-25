//! Assembling a session from a work root without the CLI (ADR-0040).

use std::collections::BTreeMap;
use std::sync::Arc;

use asterpath::Asterpath;
use asterpath::protocol::{
    CompactionConfig, InputId, Item, JsonString, ModelId, ModelSpec, Part, Provider, ProviderId,
    RequestConfig, Role, SessionEvent, SessionSpec, StopReason, SubagentConfig, ToolName,
    ToolSelection, TurnConfig, Usage, UserInput, decode_entry, scan_all,
};
use asterpath_host::{CWD, NAME, SandboxMode, WorkRoot, Workspace, new_session_spec};
use asterpath_testing::{InMemoryEventLog, ScriptedProvider};
use futures_util::StreamExt;

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn base_spec() -> SessionSpec {
    SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("model-1"),
            context_window: None,
        },
        system: Vec::new(),
        tools: ToolSelection::All,
        request: RequestConfig::default(),
        turn: TurnConfig::default(),
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
        metadata: BTreeMap::new(),
    }
}

fn workspace() -> Workspace {
    Workspace {
        sandbox: SandboxMode::FullAccess,
        skills_enabled: false,
        ..Workspace::default()
    }
}

fn tool_names(environment: &asterpath::protocol::SessionEnvironment) -> Vec<String> {
    environment
        .tools
        .iter()
        .map(|tool| tool.descriptor().name.as_str().to_owned())
        .collect()
}

#[tokio::test]
async fn a_new_session_reads_files_of_its_work_root_through_the_facade() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().canonicalize().unwrap();
    std::fs::write(root.join("note.txt"), "from the root").unwrap();
    std::fs::write(root.join("AGENTS.md"), "be brief").unwrap();

    let mut spec = base_spec();
    let diagnostics = new_session_spec(&workspace(), &root, Some("fix"), &mut spec).unwrap();
    assert!(diagnostics.is_empty());
    assert_eq!(spec.system, [text("be brief")]);
    assert_eq!(
        spec.metadata,
        BTreeMap::from([
            (CWD.to_owned(), root.display().to_string()),
            (NAME.to_owned(), "fix".to_owned()),
        ])
    );

    let provider = ScriptedProvider::new("p")
        .respond(
            vec![Item::ToolCall {
                call_id: "call-1".to_owned(),
                provider_item_id: None,
                name: ToolName::new("read_file"),
                raw_args: JsonString::new(r#"{"path":"note.txt"}"#),
                raw: None,
            }],
            StopReason::ToolUse,
            Usage::default(),
        )
        .respond(
            vec![Item::Message {
                role: Role::Assistant,
                parts: vec![text("done")],
                raw: None,
            }],
            StopReason::EndTurn,
            Usage::default(),
        );
    let asterpath = Asterpath::builder()
        .store(Arc::new(InMemoryEventLog::new()))
        .provider(Arc::new(provider) as Arc<dyn Provider>)
        .environment(Arc::new(WorkRoot {
            workspace: workspace(),
            new: true,
            pinned: None,
        }))
        .build()
        .await
        .unwrap();
    let owned = asterpath
        .create_session(spec)
        .await
        .unwrap()
        .claim()
        .await
        .unwrap();
    owned
        .submit_input(UserInput {
            id: InputId::from_bytes([1; 16]),
            parts: vec![text("read it")],
        })
        .await
        .unwrap();
    let mut stream = owned.drive();
    while let Some(event) = stream.next().await {
        event.unwrap();
    }

    let read = scan_all(&*asterpath.store(), owned.id())
        .await
        .unwrap()
        .iter()
        .filter_map(|entry| match decode_entry(entry).unwrap() {
            SessionEvent::ToolResult {
                output: Some(parts),
                ..
            } => Some(parts),
            _ => None,
        })
        .flatten()
        .any(|part| matches!(&part, Part::Text { text } if text.contains("from the root")));
    assert!(read, "read_file ran in the session's work root");
}

#[tokio::test]
async fn new_sessions_get_the_selected_tools_and_continued_ones_every_tool_and_the_skill_loader() {
    let dir = tempfile::tempdir().unwrap();
    let metadata = BTreeMap::from([(CWD.to_owned(), dir.path().display().to_string())]);
    let selected = Workspace {
        tools: Some(vec!["read_file".to_owned()]),
        ..workspace()
    };

    let new = WorkRoot {
        workspace: selected.clone(),
        new: true,
        pinned: None,
    };
    let resolved = asterpath::protocol::EnvironmentAdapter::resolve(&new, &metadata)
        .await
        .unwrap();
    assert_eq!(tool_names(&resolved), ["read_file"]);

    let continued = WorkRoot {
        workspace: selected,
        new: false,
        pinned: None,
    };
    let resolved = asterpath::protocol::EnvironmentAdapter::resolve(&continued, &metadata)
        .await
        .unwrap();
    let names = tool_names(&resolved);
    assert!(names.contains(&"bash".to_owned()), "{names:?}");
    assert!(names.contains(&"load_skill".to_owned()), "{names:?}");
}

#[tokio::test]
async fn a_session_without_cwd_resolves_only_when_pinned() {
    let dir = tempfile::tempdir().unwrap();
    let unpinned = WorkRoot {
        workspace: workspace(),
        new: false,
        pinned: None,
    };
    assert!(
        asterpath::protocol::EnvironmentAdapter::resolve(&unpinned, &BTreeMap::new())
            .await
            .is_err()
    );
    let pinned = WorkRoot {
        pinned: Some(dir.path().to_path_buf()),
        ..unpinned
    };
    assert!(
        asterpath::protocol::EnvironmentAdapter::resolve(&pinned, &BTreeMap::new())
            .await
            .is_ok()
    );
}
