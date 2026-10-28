//! Application session fork (ADR-0033, ADR-0045): a new, parentless session
//! whose initial history is the source's projection at a committed turn, the
//! last one by default.

use std::collections::BTreeMap;
use std::sync::Arc;

use asterpath::protocol::{
    CompactResult, CompactionConfig, ForkSource, InputId, Item, JsonString, ModelId, ModelSpec,
    Parallelism, Part, Provider, ProviderId, ReplaySafety, RequestConfig, Role, SessionEvent,
    SessionSpec, SideEffect, StopReason, SubagentConfig, Tool, ToolDescriptor, ToolName,
    ToolOutput, ToolSelection, TurnConfig, TurnEndReason, Usage, UserInput, decode_entry,
    encode_entry, scan_all,
};
use asterpath::{Asterpath, DefaultCompactor, OwnedSession};
use asterpath::{StepId, TurnId};
use asterpath_testing::{InMemoryEventLog, ScriptedProvider, TestTool, ToolCalls};
use futures_util::StreamExt;
use tokio::sync::Notify;

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn user(value: &str) -> Item {
    Item::Message {
        role: Role::User,
        parts: vec![text(value)],
        raw: None,
    }
}

fn assistant(value: &str) -> Item {
    Item::Message {
        role: Role::Assistant,
        parts: vec![text(value)],
        raw: None,
    }
}

fn spec() -> SessionSpec {
    SessionSpec {
        system_sections: Vec::new(),
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("model-1"),
            context_window: Some(100_000),
        },
        system: vec![text("be helpful")],
        tools: ToolSelection::All,
        request: RequestConfig::default(),
        turn: TurnConfig::default(),
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
        metadata: BTreeMap::from([("cwd".to_owned(), "/src".to_owned())]),
    }
}

fn input(id: u8, value: &str) -> UserInput {
    UserInput {
        id: InputId::from_bytes([id; 16]),
        parts: vec![text(value)],
    }
}

fn metadata(name: &str) -> BTreeMap<String, String> {
    BTreeMap::from([("name".to_owned(), name.to_owned())])
}

fn look() -> ToolDescriptor {
    ToolDescriptor::new(
        ToolName::new("look"),
        String::new(),
        serde_json::json!({"type": "object"}),
    )
    .with_parallelism(Parallelism::Parallel)
    .with_replay(ReplaySafety::Safe)
    .with_side_effect(SideEffect::ReadOnly)
}

async fn build(provider: &Arc<ScriptedProvider>) -> Asterpath {
    let (tool, _) = TestTool::text(look(), "seen");
    build_with(provider, tool).await
}

async fn build_with(provider: &Arc<ScriptedProvider>, tool: Arc<TestTool>) -> Asterpath {
    let catalog =
        asterpath::protocol::ProviderCatalog::new(vec![Arc::clone(provider) as Arc<dyn Provider>])
            .expect("catalog");
    Asterpath::builder()
        .store(Arc::new(InMemoryEventLog::new()))
        .providers(vec![Arc::clone(provider) as Arc<dyn Provider>])
        .tools(vec![tool as Arc<dyn Tool>])
        .compactor(Arc::new(DefaultCompactor::new(Arc::new(catalog))))
        .build()
        .await
        .expect("build")
}

async fn turn(handle: &OwnedSession, id: u8, value: &str) {
    handle.submit_input(input(id, value)).await.expect("input");
    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        event.expect("drive");
    }
}

async fn history(handle: &OwnedSession) -> Vec<Item> {
    handle
        .projection()
        .await
        .expect("projection")
        .into_iter()
        .flat_map(|entry| entry.items)
        .collect()
}

async fn events(asterpath: &Asterpath, handle: &OwnedSession) -> Vec<SessionEvent> {
    scan_all(&*asterpath.store(), handle.id())
        .await
        .expect("scan")
        .iter()
        .map(|entry| decode_entry(entry).expect("decode"))
        .collect()
}

#[tokio::test]
async fn forked_sessions_continue_independently() {
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(vec![assistant("a")], StopReason::EndTurn, Usage::default())
            .respond(vec![assistant("b")], StopReason::EndTurn, Usage::default())
            .respond(vec![assistant("c")], StopReason::EndTurn, Usage::default()),
    );
    let asterpath = build(&provider).await;
    let source = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    turn(&source, 1, "one").await;

    let fork = asterpath
        .fork_session(source.id(), metadata("copy"))
        .await
        .expect("fork")
        .claim()
        .await
        .unwrap();
    assert_ne!(fork.id(), source.id());
    turn(&source, 2, "two").await;
    turn(&fork, 3, "three").await;

    assert_eq!(
        history(&source).await,
        [user("one"), assistant("a"), user("two"), assistant("b")]
    );
    assert_eq!(
        history(&fork).await,
        [user("one"), assistant("a"), user("three"), assistant("c")]
    );
    // The fork's model call saw the snapshot and its own input only.
    let last = provider.requests().pop().expect("request");
    assert_eq!(
        last.items,
        [user("one"), assistant("a"), user("three")],
        "fork request"
    );
    assert_eq!(last.system, [text("be helpful")]);
    let context = last.session.expect("a session's request");
    assert_eq!(
        (context.session, context.root, context.cache_root),
        (fork.id(), fork.id(), source.id()),
        "a fork is its own root but shares its source's prompt cache"
    );

    let fork_events = events(&asterpath, &fork).await;
    let SessionEvent::SessionCreated {
        parent, metadata, ..
    } = &fork_events[0]
    else {
        panic!("first record is SessionCreated: {fork_events:?}");
    };
    assert_eq!(*parent, None, "a fork has no parent session");
    assert_eq!(
        *metadata,
        self::metadata("copy"),
        "metadata is the application's"
    );
    let SessionEvent::ContextForked { source: from, .. } = &fork_events[1] else {
        panic!("second record is ContextForked: {fork_events:?}");
    };
    assert_eq!(
        *from,
        ForkSource::Session {
            session: source.id()
        }
    );
}

#[tokio::test]
async fn compacting_the_source_after_a_fork_leaves_the_fork_unchanged() {
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(vec![assistant("a")], StopReason::EndTurn, Usage::default())
            .respond(vec![assistant("b")], StopReason::EndTurn, Usage::default())
            .respond(
                vec![assistant("the summary")],
                StopReason::EndTurn,
                Usage::default(),
            ),
    );
    let asterpath = build(&provider).await;
    let source = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    turn(&source, 1, "one").await;
    turn(&source, 2, "two").await;
    let fork = asterpath
        .fork_session(source.id(), BTreeMap::new())
        .await
        .expect("fork")
        .claim()
        .await
        .unwrap();
    let before = history(&fork).await;
    assert_eq!(
        before,
        [user("one"), assistant("a"), user("two"), assistant("b")]
    );

    let result = source.compact_now().await.expect("compact");
    assert!(matches!(result, CompactResult::Applied(_)), "{result:?}");
    assert_ne!(history(&source).await, before, "the source was compacted");
    assert_eq!(history(&fork).await, before);
}

#[tokio::test]
async fn a_turn_in_progress_is_not_copied() {
    let call = Item::ToolCall {
        call_id: "c1".to_owned(),
        provider_item_id: None,
        name: ToolName::new("look"),
        raw_args: JsonString::new("{}"),
        raw: None,
    };
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(vec![assistant("a")], StopReason::EndTurn, Usage::default())
            .respond(vec![call], StopReason::ToolUse, Usage::default())
            .respond(vec![assistant("b")], StopReason::EndTurn, Usage::default()),
    );
    // The tool parks the source mid-turn: its tool call is committed, the
    // turn is not.
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let tool = TestTool::new(look(), ToolCalls::default(), {
        let entered = Arc::clone(&entered);
        let release = Arc::clone(&release);
        move |_args, _ctx| {
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                Ok(ToolOutput::new(vec![text("seen")]))
            })
        }
    });
    let asterpath = build_with(&provider, tool).await;
    let source = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    turn(&source, 1, "one").await;

    let driven = source.clone();
    let driver = tokio::spawn(async move { turn(&driven, 2, "two").await });
    entered.notified().await;
    let fork = asterpath
        .fork_session(source.id(), BTreeMap::new())
        .await
        .expect("fork while driven")
        .claim()
        .await
        .unwrap();
    release.notify_one();
    driver.await.expect("source turn");

    assert_eq!(history(&fork).await, [user("one"), assistant("a")]);
    assert_eq!(history(&source).await.len(), 6, "the source finished");
}

#[tokio::test]
async fn an_input_waiting_for_the_next_turn_is_not_copied() {
    let provider = Arc::new(ScriptedProvider::new("p"));
    let asterpath = build(&provider).await;
    let source = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    let turn_id = TurnId::from_bytes([1; 16]);
    source.submit_input(input(1, "one")).await.expect("input");
    for event in [
        SessionEvent::TurnStarted { turn_id },
        SessionEvent::StepStarted {
            turn_id,
            step_id: StepId::from_bytes([1; 16]),
        },
    ] {
        let entry = encode_entry(source.id(), None, &event).expect("encode");
        asterpath.store().append(entry).await.expect("append");
    }
    // Recorded after the turn's last step started: the next turn consumes it.
    source.submit_input(input(2, "late")).await.expect("input");
    let committed = SessionEvent::TurnCommitted {
        turn_id,
        reason: TurnEndReason::Completed,
    };
    let entry = encode_entry(source.id(), None, &committed).expect("encode");
    asterpath.store().append(entry).await.expect("append");

    let fork = asterpath
        .fork_session(source.id(), BTreeMap::new())
        .await
        .expect("fork")
        .claim()
        .await
        .unwrap();

    assert_eq!(history(&fork).await, [user("one")]);
}

#[tokio::test]
async fn a_compaction_inside_the_turn_in_progress_is_not_copied() {
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![assistant("a")],
        StopReason::EndTurn,
        Usage::default(),
    ));
    let asterpath = build(&provider).await;
    let source = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    turn(&source, 1, "one").await;

    // A second turn starts and compacts everything so far, then is still
    // running when the application forks.
    let turn_id = TurnId::from_bytes([2; 16]);
    source.submit_input(input(2, "two")).await.expect("input");
    for event in [
        SessionEvent::TurnStarted { turn_id },
        SessionEvent::StepStarted {
            turn_id,
            step_id: StepId::from_bytes([2; 16]),
        },
    ] {
        let entry = encode_entry(source.id(), None, &event).expect("encode");
        asterpath.store().append(entry).await.expect("append");
    }
    let log = scan_all(&*asterpath.store(), source.id())
        .await
        .expect("scan");
    let compaction = SessionEvent::CompactionApplied {
        first_kept: log.last().expect("entry").log_id,
        summary: vec![text("in-progress summary")],
        replaces: asterpath::protocol::LogIdRange {
            first: log[1].log_id,
            last: log[log.len() - 2].log_id,
        },
        model: spec().model,
        usage: Usage::default(),
        raw: None,
    };
    let entry = encode_entry(source.id(), None, &compaction).expect("encode");
    asterpath.store().append(entry).await.expect("append");

    let fork = asterpath
        .fork_session(source.id(), BTreeMap::new())
        .await
        .expect("fork")
        .claim()
        .await
        .unwrap();

    assert_eq!(history(&fork).await, [user("one"), assistant("a")]);
}

/// The source's committed turns, oldest first.
async fn committed(asterpath: &Asterpath, handle: &OwnedSession) -> Vec<TurnId> {
    events(asterpath, handle)
        .await
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::TurnCommitted { turn_id, .. } => Some(turn_id),
            _ => None,
        })
        .collect()
}

async fn fork_at(asterpath: &Asterpath, source: &OwnedSession, at: TurnId) -> OwnedSession {
    asterpath
        .fork_session_at(source.id(), BTreeMap::new(), at)
        .await
        .expect("fork")
        .claim()
        .await
        .unwrap()
}

fn created_model(events: &[SessionEvent]) -> ModelId {
    let SessionEvent::SessionCreated { config, .. } = &events[0] else {
        panic!("first record is SessionCreated: {events:?}");
    };
    config.model.model.clone()
}

#[tokio::test]
async fn a_fork_at_an_earlier_turn_copies_and_configures_only_up_to_it() {
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(vec![assistant("a")], StopReason::EndTurn, Usage::default())
            .respond(vec![assistant("b")], StopReason::EndTurn, Usage::default())
            .respond(vec![assistant("c")], StopReason::EndTurn, Usage::default()),
    );
    let asterpath = build(&provider).await;
    let source = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    turn(&source, 1, "one").await;
    turn(&source, 2, "two").await;
    let mut patch = asterpath::protocol::ConfigPatch::default();
    patch.model = Some(ModelSpec {
        model: ModelId::new("model-2"),
        ..spec().model
    });
    source.update_config(patch).await.expect("update");
    turn(&source, 3, "three").await;
    let turns = committed(&asterpath, &source).await;
    assert_eq!(turns.len(), 3);
    let frontier = asterpath
        .store()
        .frontier(source.id())
        .await
        .expect("frontier");

    let first = fork_at(&asterpath, &source, turns[0]).await;
    assert_eq!(history(&first).await, [user("one"), assistant("a")]);
    assert_eq!(
        created_model(&events(&asterpath, &first).await),
        ModelId::new("model-1"),
        "the config before the later update"
    );
    let third = fork_at(&asterpath, &source, turns[2]).await;
    assert_eq!(history(&third).await.len(), 6);
    assert_eq!(
        created_model(&events(&asterpath, &third).await),
        ModelId::new("model-2")
    );
    assert_eq!(
        asterpath
            .store()
            .frontier(source.id())
            .await
            .expect("frontier"),
        frontier,
        "forking never writes the source"
    );
}

#[tokio::test]
async fn a_fork_before_a_later_compaction_sees_the_uncompacted_history() {
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(vec![assistant("a")], StopReason::EndTurn, Usage::default())
            .respond(vec![assistant("b")], StopReason::EndTurn, Usage::default())
            .respond(
                vec![assistant("the summary")],
                StopReason::EndTurn,
                Usage::default(),
            ),
    );
    let asterpath = build(&provider).await;
    let source = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    turn(&source, 1, "one").await;
    turn(&source, 2, "two").await;
    let result = source.compact_now().await.expect("compact");
    assert!(matches!(result, CompactResult::Applied(_)), "{result:?}");
    let turns = committed(&asterpath, &source).await;

    let fork = fork_at(&asterpath, &source, turns[0]).await;
    assert_eq!(history(&fork).await, [user("one"), assistant("a")]);
}

#[tokio::test]
async fn a_fork_at_an_uncommitted_or_foreign_turn_is_refused() {
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(vec![assistant("a")], StopReason::EndTurn, Usage::default())
            .respond(vec![assistant("b")], StopReason::EndTurn, Usage::default()),
    );
    let asterpath = build(&provider).await;
    let source = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    let other = asterpath
        .create_session(spec())
        .await
        .expect("create")
        .claim()
        .await
        .unwrap();
    turn(&source, 1, "one").await;
    turn(&other, 2, "two").await;
    let foreign = committed(&asterpath, &other).await[0];
    // A turn that started but has not committed.
    let open = TurnId::from_bytes([9; 16]);
    let entry = encode_entry(
        source.id(),
        None,
        &SessionEvent::TurnStarted { turn_id: open },
    )
    .expect("encode");
    asterpath.store().append(entry).await.expect("append");
    let before = asterpath
        .sessions(Default::default())
        .await
        .expect("catalog")
        .len();

    for at in [open, foreign] {
        let error = asterpath
            .fork_session_at(source.id(), BTreeMap::new(), at)
            .await
            .expect_err("refused");
        assert!(
            matches!(error, asterpath::AsterError::InvalidForkPoint { session, turn }
                if session == source.id() && turn == at),
            "{error:?}"
        );
    }
    assert_eq!(
        asterpath
            .sessions(Default::default())
            .await
            .expect("catalog")
            .len(),
        before,
        "no session was created"
    );
}
