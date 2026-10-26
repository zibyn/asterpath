//! Assembling a session from a work root with the library alone (ADR-0049).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use asterpath::Asterpath;
use asterpath::protocol::{
    CompactionConfig, InputId, Item, JsonString, ModelId, ModelSpec, Part, Provider, ProviderId,
    RequestConfig, Role, SessionEvent, SessionSpec, StopReason, SubagentConfig, ToolName,
    ToolSelection, TurnConfig, Usage, UserInput, decode_entry, scan_all,
};
use asterpath_app::workspace::{
    AGENT, CWD, NAME, SandboxMode, WorkRoot, Workspace, WorkspaceError, agent_profiles,
    new_session_spec,
};
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
    let diagnostics = new_session_spec(&workspace(), None, &root, Some("fix"), &mut spec).unwrap();
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
            new_session: true,
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
        new_session: true,
        pinned: None,
    };
    let resolved = asterpath::protocol::EnvironmentAdapter::resolve(&new, &metadata)
        .await
        .unwrap();
    assert_eq!(tool_names(&resolved), ["read_file"]);

    let continued = WorkRoot {
        workspace: selected,
        new_session: false,
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
        new_session: false,
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

fn write(path: &Path, content: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

/// A work root beside builtin and user agent directories (ADR-0051).
struct Agents {
    _dir: tempfile::TempDir,
    root: PathBuf,
    builtin: PathBuf,
    user: PathBuf,
}

impl Agents {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().canonicalize().unwrap();
        let root = base.join("root");
        std::fs::create_dir_all(&root).unwrap();
        Self {
            root,
            builtin: base.join("builtin"),
            user: base.join("user"),
            _dir: dir,
        }
    }

    fn workspace(&self, agent: Option<&str>) -> Workspace {
        Workspace {
            agent: agent.map(str::to_owned),
            user_agents: Some(self.user.clone()),
            ..workspace()
        }
    }

    fn project(&self) -> PathBuf {
        self.root.join(".asterpath/agents")
    }

    fn spec(&self, agent: &str) -> Result<SessionSpec, WorkspaceError> {
        let mut spec = base_spec();
        new_session_spec(
            &self.workspace(Some(agent)),
            Some(&self.builtin),
            &self.root,
            None,
            &mut spec,
        )?;
        Ok(spec)
    }
}

#[test]
fn the_agent_profile_comes_first_and_project_overrides_user_overrides_builtin() {
    let agents = Agents::new();
    write(&agents.root.join("AGENTS.md"), "be brief");
    write(&agents.builtin.join("code.md"), "builtin code");
    write(&agents.builtin.join("docs.md"), "builtin docs");
    write(&agents.builtin.join("review.md"), "builtin review");
    write(&agents.user.join("code.md"), "user code");
    write(&agents.user.join("review.md"), "user review");
    write(&agents.project().join("review.md"), "project review");
    write(&agents.project().join("notes.txt"), "not a profile");

    let profiles =
        agent_profiles(&agents.workspace(None), Some(&agents.builtin), &agents.root).unwrap();
    let sources: Vec<_> = profiles
        .iter()
        .map(|file| (file.name.as_str(), file.path.clone()))
        .collect();
    assert_eq!(
        sources,
        [
            ("code", agents.user.join("code.md")),
            ("docs", agents.builtin.join("docs.md")),
            ("review", agents.project().join("review.md")),
        ]
    );

    for (agent, prompt) in [
        ("code", "user code"),
        ("docs", "builtin docs"),
        ("review", "project review"),
    ] {
        let spec = agents.spec(agent).unwrap();
        assert_eq!(spec.system, [text(prompt), text("be brief")]);
        assert_eq!(spec.metadata.get(AGENT).map(String::as_str), Some(agent));
    }
}

#[test]
fn an_agent_profile_sets_the_model_and_narrows_the_tools() {
    let agents = Agents::new();
    write(
        &agents.user.join("review.md"),
        "---\ndescription: read-only review\nmodel: q/model-2\ntools: [read_file, grep]\n---\n\nreview only\n",
    );
    write(
        &agents.user.join("plain.md"),
        "---\ndescription: plain\n---\nplain",
    );

    let spec = agents.spec("review").unwrap();
    assert_eq!(spec.system, [text("review only")]);
    assert_eq!(
        spec.model,
        ModelSpec {
            provider: ProviderId::new("q"),
            model: ModelId::new("model-2"),
            context_window: None,
        }
    );
    assert_eq!(
        spec.tools,
        ToolSelection::Names(vec![ToolName::new("read_file"), ToolName::new("grep")])
    );

    let spec = agents.spec("plain").unwrap();
    assert_eq!(spec.model, base_spec().model);
    assert_eq!(spec.tools, ToolSelection::All);
}

#[test]
fn an_unknown_agent_or_frontmatter_field_is_a_config_error() {
    let agents = Agents::new();
    write(
        &agents.user.join("bad.md"),
        "---\ntemperature: hot\n---\nbody",
    );
    assert!(matches!(
        agents.spec("missing"),
        Err(WorkspaceError::Config(message)) if message.contains("unknown agent missing")
    ));
    assert!(matches!(
        agents.spec("bad"),
        Err(WorkspaceError::Config(message)) if message.contains("unknown field `temperature`")
    ));
}

#[test]
fn the_agent_flag_wins_over_default_agent() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    std::fs::write(&config, "default_agent = \"code\"\n").unwrap();
    let mut settings = asterpath_app::config::load(Some(config), &[]).unwrap();
    assert_eq!(settings.workspace().unwrap().agent.as_deref(), Some("code"));
    settings.set_agent("review".to_owned());
    assert_eq!(
        settings.workspace().unwrap().agent.as_deref(),
        Some("review")
    );
}
