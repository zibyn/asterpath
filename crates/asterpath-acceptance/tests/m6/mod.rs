//! M6 end-to-end gate: host-resolved `SKILL.md` packages, the frozen session
//! index and the on-demand `load_skill` tool, including crash recovery,
//! deletion and sub-agent inheritance.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use asterpath::{Asterpath, SessionHandle};
use asterpath_protocol::{
    AgentEvent, ConfigSnapshot, Item, JsonString, ModelId, ModelSpec, Part, Provider, ProviderId,
    RequestConfig, Role, SessionEvent, SessionSpec, SideEffect, StopReason, SubagentConfig,
    ToolErrorKind, ToolName, ToolResultStatus, ToolSelection, TurnConfig, TurnEndReason, Usage,
    UserInput,
};
use asterpath_protocol::{InputId, LogQuery, SessionId, SessionStore};
use asterpath_skills::{
    DEFAULT_INDEX_BUDGET, LoadSkillTool, SkillRoot, SkillRootLabel, SkillSummary, discover,
    render_index,
};
use asterpath_testing::{Failpoint, FailpointTool, Point, ScriptedProvider};
use futures_util::StreamExt;

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn assistant(value: &str) -> Vec<Item> {
    vec![Item::Message {
        role: Role::Assistant,
        parts: vec![text(value)],
        raw: None,
    }]
}

fn tool_call(name: &str, args: &str) -> Item {
    Item::ToolCall {
        call_id: format!("call-{name}"),
        provider_item_id: None,
        name: ToolName::new(name),
        raw_args: JsonString::new(args),
        raw: None,
    }
}

fn spec(system: Vec<Part>) -> SessionSpec {
    SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("model-1"),
            context_window: None,
        },
        system,
        tools: ToolSelection::All,
        request: RequestConfig::default(),
        turn: {
            let mut config = TurnConfig::default();
            config.initial_backoff = Duration::ZERO;
            config.max_backoff = Duration::ZERO;
            config
        },
        compaction: asterpath_protocol::CompactionConfig::default(),
        subagents: SubagentConfig::default(),
    }
}

fn write_skill(root: &Path, name: &str, frontmatter: &str, body: &str) {
    let dir = root.join(name);
    std::fs::create_dir_all(&dir).expect("skill dir");
    std::fs::write(
        dir.join("SKILL.md"),
        format!("---\n{frontmatter}\n---\n{body}"),
    )
    .expect("skill file");
}

fn roots(root: &Path) -> Vec<SkillRoot> {
    vec![SkillRoot {
        path: root.to_path_buf(),
        label: SkillRootLabel::ProjectNative,
    }]
}

/// The host's own system followed by the frozen index.
fn session_system(roots: &[SkillRoot]) -> (Vec<Part>, Vec<SkillSummary>) {
    let discovery = discover(roots);
    let (index, _) = render_index(&discovery.skills, DEFAULT_INDEX_BUDGET);
    let mut system = vec![text("host system")];
    system.extend(index);
    (system, discovery.skills)
}

async fn build(
    db: &Path,
    provider: Arc<ScriptedProvider>,
    tool: Arc<dyn asterpath_protocol::Tool>,
) -> Asterpath {
    Asterpath::builder()
        .database(db)
        .providers(vec![provider as Arc<dyn Provider>])
        .tools(vec![tool])
        .build()
        .await
        .expect("build")
}

async fn drive(handle: &SessionHandle) -> Vec<AgentEvent> {
    let mut events = Vec::new();
    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        events.push(event.expect("drive event"));
    }
    events
}

fn reasons(events: &[AgentEvent]) -> Vec<TurnEndReason> {
    events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::TurnCommitted { reason, .. } => Some(reason.clone()),
            _ => None,
        })
        .collect()
}

fn context_config(session: &[SessionEvent]) -> ConfigSnapshot {
    session
        .iter()
        .find_map(|event| match event {
            SessionEvent::SessionCreated { config, .. } => Some(config.clone()),
            _ => None,
        })
        .expect("SessionCreated")
}

async fn log(db: &Path, session: SessionId) -> Vec<SessionEvent> {
    let backend = asterpath_storage_sqlite::open(db).expect("open");
    let mut stream = backend.scan(LogQuery {
        session,
        after: None,
        limit: None,
    });
    let mut events = Vec::new();
    while let Some(entry) = stream.next().await {
        events.push(asterpath_protocol::decode_entry(&entry.expect("entry")).expect("decode"));
    }
    events
}

fn tool_results(
    events: &[SessionEvent],
) -> Vec<(ToolResultStatus, Option<ToolErrorKind>, Option<String>)> {
    events
        .iter()
        .filter_map(|event| match event {
            SessionEvent::ToolResult {
                status,
                output,
                error,
                ..
            } => Some((
                *status,
                error.as_ref().map(|error| error.kind),
                output.as_ref().map(|parts| {
                    parts
                        .iter()
                        .filter_map(|part| match part {
                            Part::Text { text } => Some(text.as_str()),
                            _ => None,
                        })
                        .collect()
                }),
            )),
            _ => None,
        })
        .collect()
}

async fn finish(asterpath: Asterpath, handle: SessionHandle) -> SessionId {
    let session = handle.id();
    drop(handle);
    asterpath.shutdown().await.expect("shutdown");
    session
}

#[tokio::test]
async fn s3_the_index_is_frozen_in_the_snapshot_and_the_tool_set() {
    let dir = tempfile::tempdir().expect("temp");
    let db = dir.path().join("s3.db");
    let skills = dir.path().join("skills");
    write_skill(
        &skills,
        "demo",
        "description: old description",
        "# Old body\n",
    );
    let roots = roots(&skills);
    let (system, _) = session_system(&roots);

    let provider = Arc::new(ScriptedProvider::new("p").respond(
        assistant("done"),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let loader = Arc::new(LoadSkillTool::new(roots.clone()));
    let asterpath = build(&db, Arc::clone(&provider), loader).await;
    let handle = asterpath
        .create_session(spec(system.clone()))
        .await
        .expect("create");
    handle
        .submit_input(UserInput {
            id: InputId::from_bytes([1; 16]),
            parts: vec![text("go")],
        })
        .await
        .expect("input");

    // The file changes after creation; the session must not observe it.
    write_skill(
        &skills,
        "demo",
        "description: new description",
        "# New body\n",
    );
    let events = drive(&handle).await;
    assert_eq!(reasons(&events), vec![TurnEndReason::Completed]);

    let session = finish(asterpath, handle).await;
    let created = context_config(&log(&db, session).await);
    assert_eq!(created.system, system);
    assert!(
        created
            .tools
            .iter()
            .any(|tool| tool.name.as_str() == "load_skill"),
        "{:?}",
        created.tools
    );
    let request = &provider.requests()[0];
    assert_eq!(request.system, system, "the request uses the frozen index");
    assert!(
        !request
            .system
            .iter()
            .any(|part| matches!(part, Part::Text { text } if text.contains("new description"))),
        "a changed file must not rewrite the session's index"
    );
}

#[tokio::test]
async fn s4_load_skill_returns_the_body_and_needs_no_approval() {
    let dir = tempfile::tempdir().expect("temp");
    let db = dir.path().join("s4.db");
    let skills = dir.path().join("skills");
    write_skill(&skills, "demo", "description: Demo.", "# Demo\n\nBody.\n");
    let roots = roots(&skills);
    let (system, _) = session_system(&roots);

    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("load_skill", r#"{"name":"demo"}"#)],
                StopReason::ToolUse,
                Usage::default(),
            )
            .respond(assistant("done"), StopReason::EndTurn, Usage::default()),
    );
    let asterpath = build(
        &db,
        Arc::clone(&provider),
        Arc::new(LoadSkillTool::new(roots)),
    )
    .await;
    let handle = asterpath
        .create_session(spec(system))
        .await
        .expect("create");
    handle
        .submit_input(UserInput {
            id: InputId::from_bytes([2; 16]),
            parts: vec![text("go")],
        })
        .await
        .expect("input");
    let events = drive(&handle).await;

    assert_eq!(reasons(&events), vec![TurnEndReason::Completed]);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, AgentEvent::ApprovalRequested(_))),
        "an in-process ReadOnly tool must not ask"
    );

    let session = finish(asterpath, handle).await;
    let events = log(&db, session).await;
    let created = context_config(&events);
    let loader = created
        .tools
        .iter()
        .find(|tool| tool.name.as_str() == "load_skill")
        .expect("load_skill descriptor");
    assert_eq!(loader.side_effect, SideEffect::ReadOnly);

    let requested = events
        .iter()
        .find_map(|event| match event {
            SessionEvent::ToolCallRequested { name, replay, .. }
                if name.as_str() == "load_skill" =>
            {
                Some(*replay)
            }
            _ => None,
        })
        .expect("ToolCallRequested");
    assert_eq!(requested, asterpath_protocol::ReplaySafety::Safe);
    assert!(
        events
            .iter()
            .any(|event| matches!(event, SessionEvent::ToolResult { status, output: Some(parts), .. }
                if *status == ToolResultStatus::Completed
                    && parts.iter().any(|part| matches!(part, Part::Text { text } if text.contains("Body."))))),
        "{:?}",
        tool_results(&events)
    );
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, SessionEvent::ApprovalDecided { .. })),
        "no approval signal is written"
    );
}

#[tokio::test]
async fn s5_a_logged_body_is_authoritative_after_the_file_changes() {
    let dir = tempfile::tempdir().expect("temp");
    let db = dir.path().join("s5a.db");
    let skills = dir.path().join("skills");
    write_skill(&skills, "demo", "description: Demo.", "# Old body\n");
    let roots = roots(&skills);
    let (system, _) = session_system(&roots);

    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("load_skill", r#"{"name":"demo"}"#)],
                StopReason::ToolUse,
                Usage::default(),
            )
            .respond(
                assistant("first done"),
                StopReason::EndTurn,
                Usage::default(),
            ),
    );
    let asterpath = build(
        &db,
        Arc::clone(&provider),
        Arc::new(LoadSkillTool::new(roots.clone())),
    )
    .await;
    let handle = asterpath
        .create_session(spec(system.clone()))
        .await
        .expect("create");
    handle
        .submit_input(UserInput {
            id: InputId::from_bytes([3; 16]),
            parts: vec![text("go")],
        })
        .await
        .expect("input");
    drive(&handle).await;
    let session = finish(asterpath, handle).await;

    write_skill(&skills, "demo", "description: Demo.", "# New body\n");

    let provider = Arc::new(ScriptedProvider::new("p").respond(
        assistant("second done"),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let asterpath = build(
        &db,
        Arc::clone(&provider),
        Arc::new(LoadSkillTool::new(roots)),
    )
    .await;
    let handle = asterpath.open_session(session).await.expect("open");
    handle
        .submit_input(UserInput {
            id: InputId::from_bytes([4; 16]),
            parts: vec![text("again")],
        })
        .await
        .expect("input");
    drive(&handle).await;
    finish(asterpath, handle).await;

    let request = &provider.requests()[0];
    let replayed: String = request
        .items
        .iter()
        .filter_map(|item| match item {
            Item::ToolResult { parts, .. } => Some(
                parts
                    .iter()
                    .filter_map(|part| match part {
                        Part::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<String>(),
            ),
            _ => None,
        })
        .collect();
    assert!(replayed.contains("Old body"), "{replayed}");
    assert!(!replayed.contains("New body"), "{replayed}");

    let results = tool_results(&log(&db, session).await);
    assert_eq!(results.len(), 1, "the logged result is the only load");
}

#[tokio::test]
async fn s5_an_unclosed_safe_call_is_rerun_after_resume() {
    let dir = tempfile::tempdir().expect("temp");
    let db = dir.path().join("s5b.db");
    let skills = dir.path().join("skills");
    write_skill(&skills, "demo", "description: Demo.", "# Body\n");
    let roots = roots(&skills);
    let (system, _) = session_system(&roots);

    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![tool_call("load_skill", r#"{"name":"demo"}"#)],
        StopReason::ToolUse,
        Usage::default(),
    ));
    let halted = Arc::new(FailpointTool::new(
        Arc::new(LoadSkillTool::new(roots.clone())),
        Failpoint::halt(Point::ToolBeforeEffect),
    ));
    let asterpath = build(&db, provider, halted).await;
    let handle = asterpath
        .create_session(spec(system))
        .await
        .expect("create");
    handle
        .submit_input(UserInput {
            id: InputId::from_bytes([5; 16]),
            parts: vec![text("go")],
        })
        .await
        .expect("input");
    let crashed = tokio::time::timeout(Duration::from_secs(1), drive(&handle)).await;
    assert!(crashed.is_err(), "the failpoint halts before the effect");
    let session = finish(asterpath, handle).await;

    let provider = Arc::new(ScriptedProvider::new("p").respond(
        assistant("recovered"),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let asterpath = build(
        &db,
        Arc::clone(&provider),
        Arc::new(LoadSkillTool::new(roots)),
    )
    .await;
    let handle = asterpath.open_session(session).await.expect("open");
    let events = drive(&handle).await;
    assert_eq!(reasons(&events), vec![TurnEndReason::Completed]);
    finish(asterpath, handle).await;

    let results = tool_results(&log(&db, session).await);
    assert_eq!(results.len(), 1, "{results:?}");
    assert_eq!(results[0].0, ToolResultStatus::Completed);
    assert!(
        results[0]
            .2
            .as_deref()
            .is_some_and(|text| text.contains("Body"))
    );
    assert!(
        provider.requests()[0]
            .items
            .iter()
            .any(|item| matches!(item, Item::ToolResult { .. })),
        "the rerun result reaches the next model call"
    );
}

#[tokio::test]
async fn s6_a_deleted_skill_is_not_found_and_the_session_continues() {
    let dir = tempfile::tempdir().expect("temp");
    let db = dir.path().join("s6.db");
    let skills = dir.path().join("skills");
    write_skill(&skills, "demo", "description: Demo.", "# Body\n");
    let roots = roots(&skills);
    let (system, _) = session_system(&roots);

    let asterpath = build(
        &db,
        Arc::new(ScriptedProvider::new("p")),
        Arc::new(LoadSkillTool::new(roots.clone())),
    )
    .await;
    let handle = asterpath
        .create_session(spec(system))
        .await
        .expect("create");
    let session = finish(asterpath, handle).await;

    std::fs::remove_dir_all(skills.join("demo")).expect("remove skill");

    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("load_skill", r#"{"name":"demo"}"#)],
                StopReason::ToolUse,
                Usage::default(),
            )
            .respond(assistant("done"), StopReason::EndTurn, Usage::default()),
    );
    let asterpath = build(
        &db,
        Arc::clone(&provider),
        Arc::new(LoadSkillTool::new(roots)),
    )
    .await;
    let handle = asterpath.open_session(session).await.expect("open");
    handle
        .submit_input(UserInput {
            id: InputId::from_bytes([6; 16]),
            parts: vec![text("go")],
        })
        .await
        .expect("input");
    let events = drive(&handle).await;
    assert_eq!(reasons(&events), vec![TurnEndReason::Completed]);
    finish(asterpath, handle).await;

    let results = tool_results(&log(&db, session).await);
    assert_eq!(results.len(), 1, "{results:?}");
    assert_eq!(results[0].0, ToolResultStatus::Failed);
    assert_eq!(results[0].1, Some(ToolErrorKind::NotFound));
}

#[tokio::test]
async fn s8_fresh_children_inherit_the_index_and_loader_and_can_narrow_them_away() {
    let dir = tempfile::tempdir().expect("temp");
    let skills = dir.path().join("skills");
    write_skill(&skills, "demo", "description: Demo.", "# Body\n");
    let roots = roots(&skills);
    let (system, _) = session_system(&roots);

    // Inherited: the child sees the same system and can load the skill.
    let db = dir.path().join("s8-inherit.db");
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call("spawn_agent", r#"{"task":"read the skill"}"#)],
                StopReason::ToolUse,
                Usage::default(),
            )
            .respond(
                vec![tool_call("load_skill", r#"{"name":"demo"}"#)],
                StopReason::ToolUse,
                Usage::default(),
            )
            .respond(
                assistant("child done"),
                StopReason::EndTurn,
                Usage::default(),
            )
            .respond(
                assistant("parent done"),
                StopReason::EndTurn,
                Usage::default(),
            ),
    );
    let asterpath = build(
        &db,
        Arc::clone(&provider),
        Arc::new(LoadSkillTool::new(roots.clone())),
    )
    .await;
    let handle = asterpath
        .create_session(spec(system.clone()))
        .await
        .expect("create");
    handle
        .submit_input(UserInput {
            id: InputId::from_bytes([8; 16]),
            parts: vec![text("go")],
        })
        .await
        .expect("input");
    assert_eq!(
        reasons(&drive(&handle).await),
        vec![TurnEndReason::Completed]
    );
    let parent = finish(asterpath, handle).await;

    let parent_events = log(&db, parent).await;
    let (child, child_config) = parent_events
        .iter()
        .find_map(|event| match event {
            SessionEvent::ChildSessionSpawned { child, config, .. } => {
                Some((*child, config.clone()))
            }
            _ => None,
        })
        .expect("child spawned");
    assert_eq!(child_config.system, system);
    assert!(
        child_config
            .tools
            .iter()
            .any(|tool| tool.name.as_str() == "load_skill")
    );
    let results = tool_results(&log(&db, child).await);
    assert_eq!(results.len(), 1, "{results:?}");
    assert_eq!(results[0].0, ToolResultStatus::Completed);
    assert!(
        results[0]
            .2
            .as_deref()
            .is_some_and(|text| text.contains("Body"))
    );

    // Narrowed: the host removed every tool, so the child cannot see the
    // loader. The inherited index may remain; the live registry still knows
    // the tool, but a real model is never offered it.
    let db = dir.path().join("s8-narrow.db");
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(
                vec![tool_call(
                    "spawn_agent",
                    r#"{"task":"read the skill","tool_allowlist":[]}"#,
                )],
                StopReason::ToolUse,
                Usage::default(),
            )
            .respond(
                assistant("child done"),
                StopReason::EndTurn,
                Usage::default(),
            )
            .respond(
                assistant("parent done"),
                StopReason::EndTurn,
                Usage::default(),
            ),
    );
    let asterpath = build(
        &db,
        Arc::clone(&provider),
        Arc::new(LoadSkillTool::new(roots)),
    )
    .await;
    let handle = asterpath
        .create_session(spec(system))
        .await
        .expect("create");
    handle
        .submit_input(UserInput {
            id: InputId::from_bytes([9; 16]),
            parts: vec![text("go")],
        })
        .await
        .expect("input");
    assert_eq!(
        reasons(&drive(&handle).await),
        vec![TurnEndReason::Completed]
    );
    let parent = finish(asterpath, handle).await;

    let parent_events = log(&db, parent).await;
    let (child, child_config) = parent_events
        .iter()
        .find_map(|event| match event {
            SessionEvent::ChildSessionSpawned { child, config, .. } => {
                Some((*child, config.clone()))
            }
            _ => None,
        })
        .expect("child spawned");
    assert!(child_config.tools.is_empty(), "{:?}", child_config.tools);
    assert_eq!(tool_results(&log(&db, child).await), Vec::new());
}
