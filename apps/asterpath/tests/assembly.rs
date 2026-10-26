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
    AGENT, CWD, NAME, STYLE, SandboxMode, WorkRoot, Workspace, WorkspaceError, agent_profiles,
    new_session_spec, style_files, subagent_profiles,
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

/// The profile index that ends a session's system, with `lines` listing
/// the profiles.
fn index(lines: &str) -> Part {
    text(&format!(
        "## Sub-agents\n\nTo hand a task to one of these sub-agents, call `spawn_agent` with `agent` set to its name.\n\n{lines}"
    ))
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
            builtin_agents: None,
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
        builtin_agents: None,
    };
    let resolved = asterpath::protocol::EnvironmentAdapter::resolve(&new, &metadata)
        .await
        .unwrap();
    assert_eq!(tool_names(&resolved), ["read_file"]);

    let continued = WorkRoot {
        workspace: selected,
        new_session: false,
        pinned: None,
        builtin_agents: None,
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
        builtin_agents: None,
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
        assert_eq!(
            spec.system,
            [
                text(prompt),
                text("be brief"),
                index("- code\n- docs\n- review\n")
            ]
        );
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
    assert_eq!(
        spec.system,
        [
            text("review only"),
            index("- plain: plain\n- review: read-only review\n")
        ]
    );
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

#[test]
fn without_a_configured_agent_the_default_profile_is_used_from_any_layer() {
    let agents = Agents::new();
    write(&agents.root.join("AGENTS.md"), "be brief");
    write(
        &agents.builtin.join("default.md"),
        "---\ndescription: builtin\n---\nbuiltin default",
    );
    let unconfigured = |agents: &Agents| {
        let mut spec = base_spec();
        new_session_spec(
            &agents.workspace(None),
            Some(&agents.builtin),
            &agents.root,
            None,
            &mut spec,
        )
        .unwrap();
        spec
    };

    let spec = unconfigured(&agents);
    assert_eq!(
        spec.system,
        [
            text("builtin default"),
            text("be brief"),
            index("- default: builtin\n")
        ]
    );
    assert_eq!(
        spec.metadata.get(AGENT).map(String::as_str),
        Some("default")
    );

    write(&agents.user.join("default.md"), "user default");
    let spec = unconfigured(&agents);
    assert_eq!(
        spec.system,
        [text("user default"), text("be brief"), index("- default\n")]
    );
}

#[test]
fn without_any_default_profile_or_builtin_directory_a_session_has_no_base_prompt() {
    let agents = Agents::new();
    write(&agents.root.join("AGENTS.md"), "be brief");
    write(&agents.builtin.join("code.md"), "builtin code");
    let missing = agents.root.join("no-such-dir");
    for builtin in [
        None,
        Some(missing.as_path()),
        Some(agents.builtin.as_path()),
    ] {
        let mut spec = base_spec();
        new_session_spec(
            &agents.workspace(None),
            builtin,
            &agents.root,
            None,
            &mut spec,
        )
        .unwrap();
        let mut expected = vec![text("be brief")];
        if builtin == Some(agents.builtin.as_path()) {
            expected.push(index("- code\n"));
        }
        assert_eq!(spec.system, expected);
        assert_eq!(spec.metadata.get(AGENT), None);
        assert_eq!(spec.metadata.get(STYLE), None);
    }
}

/// The bundled style directory sits beside the bundled agent directory.
fn builtin_styles(agents: &Agents) -> PathBuf {
    agents.builtin.parent().unwrap().join("styles")
}

fn styled(
    agents: &Agents,
    agent: Option<&str>,
    style: &str,
) -> Result<SessionSpec, WorkspaceError> {
    let mut spec = base_spec();
    new_session_spec(
        &Workspace {
            style: Some(style.to_owned()),
            user_styles: Some(agents.user.join("styles")),
            ..agents.workspace(agent)
        },
        Some(&agents.builtin),
        &agents.root,
        None,
        &mut spec,
    )?;
    Ok(spec)
}

fn framed(body: &str) -> Part {
    text(&format!(
        "The output style below takes precedence over any output requirements above.\n\n{body}"
    ))
}

#[tokio::test]
async fn an_output_style_follows_the_profile_and_project_overrides_user_overrides_builtin() {
    let agents = Agents::new();
    write(&agents.root.join("AGENTS.md"), "be brief");
    write(&agents.user.join("code.md"), "user code");
    let builtin = builtin_styles(&agents);
    let user = agents.user.join("styles");
    let project = agents.root.join(".asterpath/styles");
    write(&builtin.join("terse.md"), "builtin terse");
    write(&builtin.join("warm.md"), "builtin warm");
    write(&builtin.join("teach.md"), "builtin teach");
    write(
        &user.join("warm.md"),
        "---\ndescription: friendly\n---\nuser warm\n",
    );
    write(&user.join("teach.md"), "user teach");
    write(&project.join("teach.md"), "project teach");

    let files = style_files(
        &Workspace {
            user_styles: Some(user.clone()),
            ..workspace()
        },
        Some(&agents.builtin),
        &agents.root,
    )
    .unwrap();
    let sources: Vec<_> = files
        .iter()
        .map(|file| (file.name.as_str(), file.path.clone()))
        .collect();
    assert_eq!(
        sources,
        [
            ("teach", project.join("teach.md")),
            ("terse", builtin.join("terse.md")),
            ("warm", user.join("warm.md")),
        ]
    );

    for (style, body) in [
        ("terse", "builtin terse"),
        ("warm", "user warm"),
        ("teach", "project teach"),
    ] {
        let spec = styled(&agents, Some("code"), style).unwrap();
        assert_eq!(
            spec.system,
            [
                text("user code"),
                framed(body),
                text("be brief"),
                index("- code\n")
            ]
        );
        assert_eq!(spec.metadata.get(STYLE).map(String::as_str), Some(style));
        assert_eq!(spec.metadata.get(AGENT).map(String::as_str), Some("code"));
    }

    // The environment's profiles never take the style (ADR-0054).
    let environment = WorkRoot {
        workspace: Workspace {
            style: Some("terse".to_owned()),
            user_styles: Some(user.clone()),
            ..agents.workspace(Some("code"))
        },
        new_session: false,
        pinned: None,
        builtin_agents: Some(agents.builtin.clone()),
    };
    let metadata = BTreeMap::from([(CWD.to_owned(), agents.root.display().to_string())]);
    let resolved = asterpath::protocol::EnvironmentAdapter::resolve(&environment, &metadata)
        .await
        .unwrap();
    let systems: Vec<_> = resolved
        .agents
        .iter()
        .map(|profile| profile.system.clone())
        .collect();
    assert_eq!(
        systems,
        [vec![text("user code"), text("be brief"), index("- code\n")]]
    );
}

#[test]
fn a_style_works_without_a_profile_and_an_empty_style_adds_nothing() {
    let agents = Agents::new();
    write(&agents.root.join("AGENTS.md"), "be brief");
    let user = agents.user.join("styles");
    write(&user.join("terse.md"), "terse");
    write(&user.join("empty.md"), "---\ndescription: nothing\n---\n\n");

    let spec = styled(&agents, None, "terse").unwrap();
    assert_eq!(spec.system, [framed("terse"), text("be brief")]);
    assert_eq!(spec.metadata.get(AGENT), None);

    let spec = styled(&agents, None, "empty").unwrap();
    assert_eq!(spec.system, [text("be brief")]);
    assert_eq!(spec.metadata.get(STYLE).map(String::as_str), Some("empty"));
}

#[test]
fn an_unknown_style_or_a_field_other_than_description_is_a_config_error() {
    let agents = Agents::new();
    let user = agents.user.join("styles");
    write(&user.join("fast.md"), "---\nmodel: q/fast\n---\nbody");
    write(&user.join("narrow.md"), "---\ntools: [grep]\n---\nbody");
    assert!(matches!(
        styled(&agents, None, "missing"),
        Err(WorkspaceError::Config(message)) if message.contains("unknown style missing")
    ));
    for (style, field) in [("fast", "model"), ("narrow", "tools")] {
        let error = styled(&agents, None, style).unwrap_err().to_string();
        assert!(
            error.contains(&format!("unknown field `{field}`")),
            "{error}"
        );
        assert!(
            error.contains(&user.join(format!("{style}.md")).display().to_string()),
            "{error}"
        );
    }
}

/// The bundled `default` profile and `explanatory` style as shipped.
#[test]
fn the_shipped_explanatory_style_follows_the_shipped_default_profile() {
    let builtin = Path::new(env!("CARGO_MANIFEST_DIR")).join("agents");
    let root = tempfile::tempdir().unwrap();
    let mut spec = base_spec();
    new_session_spec(
        &Workspace {
            style: Some("explanatory".to_owned()),
            ..workspace()
        },
        Some(&builtin),
        root.path(),
        None,
        &mut spec,
    )
    .unwrap();
    let texts: Vec<&str> = spec
        .system
        .iter()
        .map(|part| match part {
            Part::Text { text } => text.as_str(),
            other => panic!("{other:?}"),
        })
        .collect();
    let [profile, style, _index] = texts[..] else {
        panic!("{texts:?}");
    };

    assert!(profile.starts_with("You are Asterpath"), "{profile}");
    let headings: Vec<_> = profile.lines().filter(|l| l.starts_with("# ")).collect();
    assert_eq!(
        headings,
        ["# How to work", "# Tool use", "# Safety and confirmation"]
    );
    let how_to_work = &profile[..profile.find("# Tool use").unwrap()];
    assert!(
        how_to_work.contains("Reply in the user's language"),
        "{profile}"
    );
    assert!(how_to_work.contains("After making changes, summarize what changed"));
    assert!(!profile.contains("Be concise"), "{profile}");
    assert!(!profile.contains("Markdown"), "{profile}");

    assert!(
        style.starts_with(
            "The output style below takes precedence over any output requirements above.\n\n"
        ),
        "{style}"
    );
    assert!(style.contains("> **Insight**"), "{style}");
    assert!(!style.contains('★'), "{style}");
}

#[test]
fn the_style_flag_wins_over_output_style() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config.toml");
    std::fs::write(&config, "output_style = \"terse\"\n").unwrap();
    let mut settings = asterpath_app::config::load(Some(config), &[]).unwrap();
    assert_eq!(
        settings.workspace().unwrap().style.as_deref(),
        Some("terse")
    );
    settings.set_style(Some("warm".to_owned()));
    assert_eq!(settings.workspace().unwrap().style.as_deref(), Some("warm"));
    settings.set_style(None);
    assert_eq!(settings.workspace().unwrap().style, None);
}

/// Builtin `code` and `docs`, a user `docs` with model and tools, a project
/// `code` and a project profile with a broken frontmatter.
fn layered_agents() -> Agents {
    let agents = Agents::new();
    write(&agents.root.join("AGENTS.md"), "be brief");
    write(&agents.builtin.join("code.md"), "builtin code");
    write(&agents.builtin.join("docs.md"), "builtin docs");
    write(
        &agents.user.join("docs.md"),
        "---\ndescription: writes docs\nmodel: q/model-2\ntools: [read_file]\n---\nuser docs",
    );
    write(
        &agents.project().join("code.md"),
        "---\ndescription: writes code\n---\nproject code",
    );
    write(
        &agents.project().join("broken.md"),
        "---\nmood: grumpy\n---\nbody",
    );
    agents
}

#[tokio::test]
async fn the_environment_offers_every_loadable_profile_assembled_like_a_root_session() {
    let agents = layered_agents();
    let environment = WorkRoot {
        workspace: agents.workspace(None),
        new_session: false,
        pinned: None,
        builtin_agents: Some(agents.builtin.clone()),
    };
    let metadata = BTreeMap::from([(CWD.to_owned(), agents.root.display().to_string())]);
    let resolved = asterpath::protocol::EnvironmentAdapter::resolve(&environment, &metadata)
        .await
        .unwrap();

    let listed = index("- code: writes code\n- docs: writes docs\n");
    let profiles: Vec<_> = resolved
        .agents
        .iter()
        .map(|profile| {
            (
                profile.name.as_str(),
                profile.description.as_str(),
                profile.system.clone(),
                profile.model.clone(),
                profile.tools.clone(),
            )
        })
        .collect();
    assert_eq!(
        profiles,
        [
            (
                "code",
                "writes code",
                vec![text("project code"), text("be brief"), listed.clone()],
                None,
                None,
            ),
            (
                "docs",
                "writes docs",
                vec![text("user docs"), text("be brief"), listed],
                Some(ModelSpec {
                    provider: ProviderId::new("q"),
                    model: ModelId::new("model-2"),
                    context_window: None,
                }),
                Some(vec![ToolName::new("read_file")]),
            ),
        ]
    );
}

#[test]
fn a_profile_that_fails_to_load_is_skipped_with_a_diagnostic() {
    let agents = layered_agents();
    let (profiles, diagnostics) =
        subagent_profiles(&agents.workspace(None), Some(&agents.builtin), &agents.root).unwrap();
    let names: Vec<_> = profiles
        .iter()
        .map(|profile| profile.name.as_str())
        .collect();
    assert_eq!(names, ["code", "docs"]);
    assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
    assert!(
        diagnostics[0].contains("broken.md") && diagnostics[0].contains("unknown field `mood`"),
        "{diagnostics:?}"
    );

    // The new session's index leaves it out too; selecting it is still an error.
    let spec = agents.spec("code").unwrap();
    assert_eq!(
        spec.system.last(),
        Some(&index("- code: writes code\n- docs: writes docs\n"))
    );
    assert!(matches!(
        agents.spec("broken"),
        Err(WorkspaceError::Config(message)) if message.contains("unknown field `mood`")
    ));
}

async fn resolve_child_system(agents: &Agents, metadata: &[(&str, &str)]) -> Option<Vec<Part>> {
    let environment = WorkRoot {
        workspace: agents.workspace(None),
        new_session: false,
        pinned: None,
        builtin_agents: Some(agents.builtin.clone()),
    };
    let mut metadata: BTreeMap<String, String> = metadata
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect();
    metadata.insert(CWD.to_owned(), agents.root.display().to_string());
    asterpath::protocol::EnvironmentAdapter::resolve(&environment, &metadata)
        .await
        .unwrap()
        .child_system
}

#[tokio::test]
async fn a_styled_root_offers_its_unstyled_system_to_its_children() {
    let agents = Agents::new();
    write(&agents.root.join("AGENTS.md"), "be brief");
    write(&agents.user.join("code.md"), "user code");

    assert_eq!(
        resolve_child_system(&agents, &[(STYLE, "terse"), (AGENT, "code")]).await,
        Some(vec![text("user code"), text("be brief"), index("- code\n")])
    );
    assert_eq!(
        resolve_child_system(&agents, &[(STYLE, "terse")]).await,
        Some(vec![text("be brief"), index("- code\n")])
    );
    assert_eq!(
        resolve_child_system(&agents, &[(AGENT, "code")]).await,
        None
    );
}

#[tokio::test]
async fn a_styled_roots_fresh_child_is_created_without_the_style() {
    let agents = Agents::new();
    write(&agents.root.join("AGENTS.md"), "be brief");
    let user_styles = agents.user.join("styles");
    write(&user_styles.join("terse.md"), "terse");
    let workspace = Workspace {
        style: Some("terse".to_owned()),
        user_styles: Some(user_styles),
        ..agents.workspace(None)
    };
    let mut spec = base_spec();
    new_session_spec(
        &workspace,
        Some(&agents.builtin),
        &agents.root,
        None,
        &mut spec,
    )
    .unwrap();
    assert_eq!(spec.system, [framed("terse"), text("be brief")]);

    let say = |value: &str| {
        vec![Item::Message {
            role: Role::Assistant,
            parts: vec![text(value)],
            raw: None,
        }]
    };
    let provider = ScriptedProvider::new("p")
        .respond(
            vec![Item::ToolCall {
                call_id: "call-1".to_owned(),
                provider_item_id: None,
                name: ToolName::new("spawn_agent"),
                raw_args: JsonString::new(r#"{"task":"t"}"#),
                raw: None,
            }],
            StopReason::ToolUse,
            Usage::default(),
        )
        .respond(say("child done"), StopReason::EndTurn, Usage::default())
        .respond(say("parent done"), StopReason::EndTurn, Usage::default());
    let asterpath = Asterpath::builder()
        .store(Arc::new(InMemoryEventLog::new()))
        .provider(Arc::new(provider) as Arc<dyn Provider>)
        .environment(Arc::new(WorkRoot {
            workspace,
            new_session: false,
            pinned: None,
            builtin_agents: Some(agents.builtin.clone()),
        }))
        .build()
        .await
        .unwrap();
    let root = asterpath.create_session(spec).await.unwrap();
    drive(&root.claim().await.unwrap()).await;
    let (child, spawned) = events(&asterpath, root.id())
        .await
        .into_iter()
        .find_map(|event| match event {
            SessionEvent::ChildSessionSpawned { child, config, .. } => Some((child, config)),
            _ => None,
        })
        .expect("the root spawns a child");
    let SessionEvent::SessionCreated {
        config: created, ..
    } = &events(&asterpath, child).await[0]
    else {
        panic!("the child starts with SessionCreated");
    };
    let unstyled = vec![text("be brief")];
    assert_eq!(created.system, unstyled);
    assert_eq!(spawned.system, unstyled);
}

async fn drive(owned: &asterpath::OwnedSession) {
    owned
        .submit_input(UserInput {
            id: InputId::from_bytes([1; 16]),
            parts: vec![text("go")],
        })
        .await
        .unwrap();
    let mut stream = owned.drive();
    while let Some(event) = stream.next().await {
        event.unwrap();
    }
}

async fn events(
    asterpath: &Asterpath,
    session: asterpath::protocol::SessionId,
) -> Vec<SessionEvent> {
    scan_all(&*asterpath.store(), session)
        .await
        .unwrap()
        .iter()
        .map(|entry| decode_entry(entry).unwrap())
        .collect()
}
