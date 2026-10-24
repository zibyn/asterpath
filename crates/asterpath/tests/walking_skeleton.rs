//! Public-behaviour tests for the walking skeleton: create a session, submit an
//! input, drive one turn with a scripted provider, and read the result back from
//! the event log.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use asterpath::protocol::{
    AbortKind, AgentError, AgentEvent, CompactionConfig, InputTokens, Item, ModelId, ModelSpec,
    OutputTokens, Part, Provider, ProviderCapabilities, ProviderError, ProviderErrorKind,
    ProviderId, RequestConfig, Role, SamplingParams, SessionEvent, SessionSpec, SessionState,
    StopReason, SubagentConfig, ToolSelection, TurnConfig, TurnEndReason, Usage, UserInput,
    decode_entry,
};
use asterpath::protocol::{
    Clock, InputId, LogEntry, LogQuery, ParentFilter, SessionId, SessionQuery, SessionStore,
};
use asterpath::{AsterError, Asterpath, SessionHandle};
use asterpath_testing::{ManualClock, ScriptedProvider};
use futures_util::StreamExt;
use std::collections::BTreeMap;

static NEXT_DB: AtomicU64 = AtomicU64::new(0);

fn temp_db(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "asterpath-{tag}-{}-{}.db",
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
    let _ = fs::remove_file(format!("{}.lock", path.display()));
}

fn text(text: &str) -> Part {
    Part::Text {
        text: text.to_owned(),
    }
}

fn assistant_items(text_value: &str) -> Vec<Item> {
    vec![Item::Message {
        role: Role::Assistant,
        parts: vec![text(text_value)],
        raw: None,
    }]
}

fn usage(input: u64, output: u64) -> Usage {
    Usage {
        input: InputTokens {
            uncached: input,
            cache_read: 0,
            cache_write: 0,
        },
        output: OutputTokens {
            total: output,
            reasoning: 0,
        },
    }
}

fn test_turn_config() -> TurnConfig {
    let mut config = TurnConfig::default();
    config.initial_backoff = Duration::ZERO;
    config.max_backoff = Duration::ZERO;
    config
}

fn spec(provider: &str) -> SessionSpec {
    SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new(provider),
            model: ModelId::new("model-1"),
            context_window: Some(200_000),
        },
        system: vec![text("be helpful")],
        // This skeleton exercises the loop without tools; the facade still
        // registers the built-in spawn tool for sessions that ask for tools.
        tools: ToolSelection::Names(Vec::new()),
        request: RequestConfig::default(),
        turn: test_turn_config(),
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
        metadata: Default::default(),
    }
}

async fn open(db: &Path, providers: Vec<Arc<dyn Provider>>) -> Asterpath {
    Asterpath::builder()
        .database(db)
        .providers(providers)
        .build()
        .await
        .expect("facade builds")
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

fn decoded(entries: &[LogEntry]) -> Vec<SessionEvent> {
    entries
        .iter()
        .map(|entry| decode_entry(entry).expect("decodable"))
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

fn input(id: u8, message: &str) -> UserInput {
    UserInput {
        id: InputId::from_bytes([id; 16]),
        parts: vec![text(message)],
    }
}

#[tokio::test]
async fn one_turn_without_tools_commits_the_expected_sequence() {
    let db = temp_db("sequence");
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        assistant_items("hello"),
        StopReason::EndTurn,
        usage(7, 3),
    ));
    let asterpath = open(&db, vec![provider.clone()]).await;
    let handle = asterpath.create_session(spec("p")).await.unwrap();
    handle
        .submit_input(input(1, "hi"))
        .await
        .expect("input recorded");

    let events = drive_all(&handle).await;
    assert!(matches!(
        events.first(),
        Some(AgentEvent::TurnStarted { .. })
    ));
    assert!(matches!(
        events.last(),
        Some(AgentEvent::TurnCommitted {
            reason: TurnEndReason::Completed,
            ..
        })
    ));
    assert!(matches!(handle.state().await.unwrap(), SessionState::Idle));

    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(
        kinds(&entries),
        [
            "SessionCreated",
            "InputRecorded",
            "TurnStarted",
            "StepStarted",
            "AssistantMessage",
            "StepCommitted",
            "TurnCommitted",
        ]
    );

    let snapshot = entries
        .first()
        .map(|entry| decode_entry(entry).unwrap())
        .and_then(|event| match event {
            SessionEvent::SessionCreated { config, parent, .. } => {
                assert!(parent.is_none());
                Some(config)
            }
            _ => None,
        })
        .expect("config snapshot");
    assert_eq!(snapshot.model, spec("p").model);
    assert_eq!(snapshot.system, vec![text("be helpful")]);
    assert!(snapshot.tools.is_empty());
    assert_eq!(snapshot.request, RequestConfig::default());

    let messages: Vec<_> = decoded(&entries)
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::AssistantMessage {
                items,
                usage,
                stop_reason,
                ..
            } => Some((items, usage, stop_reason)),
            _ => None,
        })
        .collect();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].0, assistant_items("hello"));
    assert_eq!(messages[0].1, usage(7, 3));
    assert_eq!(messages[0].2, StopReason::EndTurn);

    let requests = provider.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].spec, spec("p").model);
    assert_eq!(requests[0].system, vec![text("be helpful")]);
    assert!(requests[0].tools.is_empty());
    assert_eq!(
        requests[0].items,
        vec![Item::Message {
            role: Role::User,
            parts: vec![text("hi")],
            raw: None,
        }]
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, AgentEvent::ModelDelta { .. })),
        "deltas are forwarded on the live stream"
    );

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn reopening_the_same_database_finds_an_idle_session() {
    let db = temp_db("reopen");
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        assistant_items("done"),
        StopReason::EndTurn,
        usage(1, 1),
    ));
    let asterpath = open(&db, vec![provider.clone()]).await;
    let handle = asterpath.create_session(spec("p")).await.unwrap();
    let session = handle.id();
    handle.submit_input(input(1, "hi")).await.unwrap();
    drive_all(&handle).await;
    asterpath.shutdown().await.unwrap();
    drop(handle);

    let idle_provider = Arc::new(ScriptedProvider::new("p"));
    let reopened = open(&db, vec![idle_provider.clone()]).await;
    let handle = reopened.open_session(session).await.unwrap();
    assert!(matches!(handle.state().await.unwrap(), SessionState::Idle));
    let events = drive_all(&handle).await;
    assert_eq!(events, vec![AgentEvent::Idle { session }]);
    assert!(idle_provider.requests().is_empty());
    reopened.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn drive_is_single_flight_across_handles() {
    let db = temp_db("busy");
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        assistant_items("done"),
        StopReason::EndTurn,
        usage(1, 1),
    ));
    let asterpath = open(&db, vec![provider.clone()]).await;
    let handle = asterpath.create_session(spec("p")).await.unwrap();
    handle.submit_input(input(1, "hi")).await.unwrap();

    let mut first = handle.drive();
    let mut second = handle.drive();
    match second.next().await {
        Some(Err(AsterError::Agent(AgentError::Busy { session }))) => {
            assert_eq!(session, handle.id());
        }
        other => panic!("expected Busy, got {other:?}"),
    }
    drop(second);

    let mut events = Vec::new();
    while let Some(event) = first.next().await {
        events.push(event.unwrap());
    }
    assert!(matches!(
        events.last(),
        Some(AgentEvent::TurnCommitted {
            reason: TurnEndReason::Completed,
            ..
        })
    ));

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn dropping_the_stream_does_not_cancel_or_close_the_session() {
    let db = temp_db("drop");
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        assistant_items("done"),
        StopReason::EndTurn,
        usage(1, 1),
    ));
    let asterpath = open(&db, vec![provider.clone()]).await;
    let handle = asterpath.create_session(spec("p")).await.unwrap();
    handle.submit_input(input(1, "hi")).await.unwrap();

    let mut stream = handle.drive();
    let first = stream.next().await.expect("first event").unwrap();
    assert!(matches!(first, AgentEvent::TurnStarted { .. }));
    drop(stream);

    // The turn only advances while polled (ADR-0006), and appends yield
    // (#97), so the dropped stream leaves the turn open; a re-drive resumes
    // it from the log instead of starting over.
    let events = drive_all(&handle).await;
    assert!(matches!(
        events.last(),
        Some(AgentEvent::TurnCommitted { .. })
    ));
    assert!(matches!(handle.state().await.unwrap(), SessionState::Idle));
    let entries = scan(asterpath.store(), handle.id()).await;
    assert!(matches!(
        decoded(&entries).last(),
        Some(SessionEvent::TurnCommitted {
            reason: TurnEndReason::Completed,
            ..
        })
    ));
    let turns = decoded(&entries)
        .iter()
        .filter(|event| matches!(event, SessionEvent::TurnStarted { .. }))
        .count();
    assert_eq!(turns, 1, "the dropped turn is resumed, not restarted");
    // The session is still drivable: a later drive sees an idle session.
    let events = drive_all(&handle).await;
    assert_eq!(
        events,
        vec![AgentEvent::Idle {
            session: handle.id()
        }]
    );
    assert_eq!(provider.requests().len(), 1);

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn retryable_failures_retry_the_same_step_with_partial_usage_kept_diagnostic() {
    let db = temp_db("retry");
    let partial = usage(3, 0);
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .fail(
                ProviderError::new(ProviderErrorKind::Transport, "reset")
                    .with_partial_usage(partial),
            )
            .fail(
                ProviderError::new(ProviderErrorKind::Transport, "reset again")
                    .with_partial_usage(partial),
            )
            .respond(
                assistant_items("finally"),
                StopReason::EndTurn,
                usage(11, 5),
            ),
    );
    let asterpath = open(&db, vec![provider.clone()]).await;
    let handle = asterpath.create_session(spec("p")).await.unwrap();
    handle.submit_input(input(1, "hi")).await.unwrap();
    drive_all(&handle).await;

    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(
        kinds(&entries),
        [
            "SessionCreated",
            "InputRecorded",
            "TurnStarted",
            "StepStarted",
            "ModelCallAborted",
            "ModelCallAborted",
            "AssistantMessage",
            "StepCommitted",
            "TurnCommitted",
        ]
    );

    let mut aborts = Vec::new();
    let mut assistant = None;
    for event in decoded(&entries) {
        match event {
            SessionEvent::ModelCallAborted {
                attempt,
                kind,
                backoff_ms,
                usage,
                partial: failed_partial,
                error,
                ..
            } => aborts.push((attempt, kind, backoff_ms, usage, failed_partial, error)),
            SessionEvent::AssistantMessage { usage, .. } => assistant = Some(usage),
            _ => {}
        }
    }
    assert_eq!(aborts.len(), 2);
    assert_eq!(aborts[0].0, 1);
    assert_eq!(aborts[1].0, 2);
    assert_eq!(aborts[0].1, AbortKind::Transport);
    assert_eq!(aborts[0].2, 0, "test backoff is zero");
    assert_eq!(aborts[0].3, Some(partial), "partial usage is diagnostic");
    assert!(aborts[0].4.is_none());
    let messages: Vec<_> = aborts
        .iter()
        .map(|abort| {
            abort
                .5
                .as_ref()
                .map(|error| (error.kind, error.message.as_str()))
        })
        .collect();
    assert_eq!(
        messages,
        [
            Some((ProviderErrorKind::Transport, "reset")),
            Some((ProviderErrorKind::Transport, "reset again")),
        ],
        "every attempt keeps its own error"
    );
    assert_eq!(
        assistant,
        Some(usage(11, 5)),
        "total excludes failed attempts"
    );
    assert_eq!(provider.requests().len(), 3);

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn non_retryable_rejection_ends_the_turn_failed() {
    let db = temp_db("rejected");
    let provider = Arc::new(ScriptedProvider::new("p").fail(ProviderError::new(
        ProviderErrorKind::InvalidRequest,
        "bad request",
    )));
    let asterpath = open(&db, vec![provider.clone()]).await;
    let handle = asterpath.create_session(spec("p")).await.unwrap();
    handle.submit_input(input(1, "hi")).await.unwrap();
    drive_all(&handle).await;

    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(
        kinds(&entries),
        [
            "SessionCreated",
            "InputRecorded",
            "TurnStarted",
            "StepStarted",
            "ModelCallAborted",
            "StepCommitted",
            "TurnCommitted",
        ]
    );
    let last = decoded(&entries).pop().unwrap();
    assert!(matches!(
        last,
        SessionEvent::TurnCommitted {
            reason: TurnEndReason::Failed,
            ..
        }
    ));
    assert_eq!(provider.requests().len(), 1);

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn refusal_empty_and_max_tokens_still_complete_with_stop_reason_preserved() {
    for (tag, items, stop_reason) in [
        (
            "refusal",
            vec![Item::Message {
                role: Role::Assistant,
                parts: vec![Part::Refusal {
                    text: "no".to_owned(),
                }],
                raw: None,
            }],
            StopReason::Refusal,
        ),
        ("empty", Vec::new(), StopReason::EndTurn),
        ("max-tokens", assistant_items("cut"), StopReason::MaxTokens),
    ] {
        let db = temp_db(tag);
        let provider =
            Arc::new(ScriptedProvider::new("p").respond(items, stop_reason.clone(), usage(1, 1)));
        let asterpath = open(&db, vec![provider.clone()]).await;
        let handle = asterpath.create_session(spec("p")).await.unwrap();
        handle.submit_input(input(1, "hi")).await.unwrap();
        drive_all(&handle).await;

        let entries = scan(asterpath.store(), handle.id()).await;
        let all = decoded(&entries);
        assert!(matches!(
            all.last(),
            Some(SessionEvent::TurnCommitted {
                reason: TurnEndReason::Completed,
                ..
            })
        ));
        let message = all
            .iter()
            .find_map(|event| match event {
                SessionEvent::AssistantMessage { stop_reason, .. } => Some(stop_reason.clone()),
                _ => None,
            })
            .expect("assistant message");
        assert_eq!(message, stop_reason);

        asterpath.shutdown().await.unwrap();
        cleanup(&db);
    }
}

#[tokio::test]
async fn pause_turn_continues_the_turn_and_other_stop_reasons_complete() {
    let db = temp_db("pause");
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(assistant_items("first"), StopReason::PauseTurn, usage(1, 1))
            .respond(assistant_items("second"), StopReason::EndTurn, usage(1, 1)),
    );
    let asterpath = open(&db, vec![provider.clone()]).await;
    let handle = asterpath.create_session(spec("p")).await.unwrap();
    handle.submit_input(input(1, "hi")).await.unwrap();
    drive_all(&handle).await;

    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(
        kinds(&entries),
        [
            "SessionCreated",
            "InputRecorded",
            "TurnStarted",
            "StepStarted",
            "AssistantMessage",
            "StepCommitted",
            "StepStarted",
            "AssistantMessage",
            "StepCommitted",
            "TurnCommitted",
        ]
    );
    assert!(matches!(
        decoded(&entries).last(),
        Some(SessionEvent::TurnCommitted {
            reason: TurnEndReason::Completed,
            ..
        })
    ));
    asterpath.shutdown().await.unwrap();
    cleanup(&db);

    let db = temp_db("other");
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        assistant_items("x"),
        StopReason::Other {
            provider_label: "mystery".to_owned(),
        },
        usage(1, 1),
    ));
    let asterpath = open(&db, vec![provider.clone()]).await;
    let handle = asterpath.create_session(spec("p")).await.unwrap();
    handle.submit_input(input(1, "hi")).await.unwrap();
    drive_all(&handle).await;
    let entries = scan(asterpath.store(), handle.id()).await;
    assert!(matches!(
        decoded(&entries).last(),
        Some(SessionEvent::TurnCommitted {
            reason: TurnEndReason::Completed,
            ..
        })
    ));
    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn pause_turn_continuation_keeps_the_declared_server_tools() {
    // A paused turn may end on a server tool call that has not run yet; the
    // continuation must declare the same server tools (ADR-0031).
    let db = temp_db("pause-server-tools");
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(assistant_items("first"), StopReason::PauseTurn, usage(1, 1))
            .respond(assistant_items("second"), StopReason::EndTurn, usage(1, 1))
            .with_capabilities({
                let mut capabilities = ProviderCapabilities::default();
                capabilities.server_tools = true;
                capabilities
            }),
    );
    let asterpath = open(&db, vec![provider.clone()]).await;
    let mut session_spec = spec("p");
    let declared = vec![asterpath::protocol::JsonString::new(
        r#"{"type":"web_search_20250305","name":"web_search"}"#,
    )];
    session_spec
        .request
        .server_tools
        .insert(ProviderId::new("p"), declared.clone());
    let handle = asterpath.create_session(session_spec).await.unwrap();
    handle.submit_input(input(1, "hi")).await.unwrap();
    drive_all(&handle).await;

    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert_eq!(
            request.server_tools.get(&ProviderId::new("p")),
            Some(&declared)
        );
    }
    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

fn named(name: &str) -> SessionSpec {
    let mut session_spec = spec("p");
    session_spec.metadata = BTreeMap::from([
        ("cwd".to_owned(), "/work".to_owned()),
        ("name".to_owned(), name.to_owned()),
    ]);
    session_spec
}

#[tokio::test]
async fn sessions_lists_host_metadata_across_a_reopen() {
    let clock = Arc::new(ManualClock::new());
    let db = temp_db("catalog");
    let build = |db: PathBuf| {
        let clock = clock.clone();
        async move {
            Asterpath::builder()
                .database(db)
                .providers(vec![Arc::new(ScriptedProvider::new("p"))])
                .clock(clock)
                .build()
                .await
                .expect("facade builds")
        }
    };
    let asterpath = build(db.clone()).await;
    let first = asterpath.create_session(named("first")).await.unwrap().id();
    clock.advance(1);
    let second = asterpath
        .create_session(named("second"))
        .await
        .unwrap()
        .id();
    let created = decoded(&scan(asterpath.store(), first).await).remove(0);
    let SessionEvent::SessionCreated { metadata, .. } = created else {
        panic!("log starts with SessionCreated");
    };
    assert_eq!(metadata, named("first").metadata);
    asterpath.shutdown().await.unwrap();

    let reopened = build(db.clone()).await;
    let page = |after| SessionQuery {
        parent: ParentFilter::Roots,
        after,
        limit: Some(1),
    };
    let newest = reopened.sessions(page(None)).await.unwrap();
    assert_eq!(newest.len(), 1);
    assert_eq!(newest[0].session, second);
    assert_eq!(newest[0].metadata, named("second").metadata);
    let older = reopened
        .sessions(page(Some(newest[0].cursor())))
        .await
        .unwrap();
    assert_eq!(older.len(), 1);
    assert_eq!(older[0].session, first);
    assert_eq!(older[0].metadata["name"], "first");
    reopened.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn oversized_host_metadata_is_a_configuration_error() {
    let db = temp_db("metadata-limits");
    let asterpath = open(&db, vec![Arc::new(ScriptedProvider::new("p"))]).await;
    let too_many = (0..=64).map(|i| (format!("k{i}"), String::new())).collect();
    let long_key = BTreeMap::from([("k".repeat(129), String::new())]);
    let long_value = BTreeMap::from([("k".to_owned(), "v".repeat(4097))]);
    for metadata in [too_many, long_key, long_value] {
        let mut session_spec = spec("p");
        session_spec.metadata = metadata;
        assert!(matches!(
            asterpath.create_session(session_spec).await.unwrap_err(),
            AsterError::Config { .. }
        ));
    }
    assert!(
        asterpath
            .sessions(SessionQuery::default())
            .await
            .unwrap()
            .is_empty(),
        "a rejected spec writes nothing"
    );
    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn zero_max_steps_per_turn_is_a_configuration_error() {
    let db = temp_db("zero-steps");
    let provider = Arc::new(ScriptedProvider::new("p"));
    let asterpath = open(&db, vec![provider]).await;
    let mut session_spec = spec("p");
    session_spec.turn.max_steps_per_turn = Some(0);
    assert!(matches!(
        asterpath.create_session(session_spec).await.unwrap_err(),
        AsterError::Config { .. }
    ));
    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn duplicate_provider_ids_are_a_configuration_error() {
    let db = temp_db("duplicate");
    let error = Asterpath::builder()
        .database(&db)
        .providers(vec![
            Arc::new(ScriptedProvider::new("dup")) as Arc<dyn Provider>,
            Arc::new(ScriptedProvider::new("dup")) as Arc<dyn Provider>,
        ])
        .build()
        .await
        .unwrap_err();
    assert!(matches!(error, AsterError::Config { .. }));
    cleanup(&db);
}

#[tokio::test]
async fn capability_mismatch_is_rejected_before_the_response_is_used() {
    let db = temp_db("unsupported");
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .with_capabilities(ProviderCapabilities::default())
            .respond(assistant_items("never"), StopReason::EndTurn, usage(1, 1)),
    );
    let asterpath = open(&db, vec![provider.clone()]).await;
    let mut session_spec = spec("p");
    let mut sampling = SamplingParams::default();
    sampling.temperature = Some(0.4);
    session_spec.request.sampling = Some(sampling);
    let handle = asterpath.create_session(session_spec).await.unwrap();
    handle.submit_input(input(1, "hi")).await.unwrap();
    drive_all(&handle).await;

    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(
        kinds(&entries),
        [
            "SessionCreated",
            "InputRecorded",
            "TurnStarted",
            "StepStarted",
            "ModelCallAborted",
            "StepCommitted",
            "TurnCommitted",
        ]
    );
    let abort = decoded(&entries).into_iter().find_map(|event| match event {
        SessionEvent::ModelCallAborted { kind, error, .. } => Some((kind, error)),
        _ => None,
    });
    let (kind, error) = abort.expect("an abort");
    assert_eq!(kind, AbortKind::Rejected);
    let error = error.expect("the abort records the provider error");
    assert_eq!(error.kind, ProviderErrorKind::Unsupported);
    assert!(error.message.contains("sampling"), "{}", error.message);
    assert_eq!(provider.remaining(), 1, "no scripted response was consumed");
    let last = decoded(&entries).pop().unwrap();
    assert!(matches!(
        last,
        SessionEvent::TurnCommitted {
            reason: TurnEndReason::Failed,
            ..
        }
    ));

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn submit_input_records_but_never_starts_a_turn() {
    let db = temp_db("submit");
    let provider = Arc::new(ScriptedProvider::new("p"));
    let asterpath = open(&db, vec![provider.clone()]).await;
    let handle = asterpath.create_session(spec("p")).await.unwrap();

    let first = handle.submit_input(input(7, "hi")).await.unwrap();
    let duplicate = handle.submit_input(input(7, "hi")).await.unwrap();
    assert_eq!(first, duplicate);
    let entries = scan(asterpath.store(), handle.id()).await;
    assert_eq!(kinds(&entries), ["SessionCreated", "InputRecorded"]);
    assert!(matches!(handle.state().await.unwrap(), SessionState::Idle));
    assert!(provider.requests().is_empty());

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}

#[tokio::test]
async fn manual_clock_ids_are_deterministic_and_unique() {
    let db = temp_db("manual-clock-ids");
    let clock = Arc::new(ManualClock::new());
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        assistant_items("hello"),
        StopReason::EndTurn,
        usage(1, 1),
    ));
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![provider as Arc<dyn Provider>])
        .clock(clock.clone())
        .build()
        .await
        .expect("facade builds");
    let first = asterpath.create_session(spec("p")).await.unwrap();
    let second = asterpath.create_session(spec("p")).await.unwrap();
    first
        .submit_input(input(1, "hi"))
        .await
        .expect("input recorded");
    drive_all(&first).await;

    // The clock-derived half is the deterministic prefix: every id starts with
    // the injected timestamp, and the counter half keeps them unique.
    let stamp = (clock.now() as u64) ^ u64::from(std::process::id()).rotate_left(17);
    let expected = stamp.to_le_bytes();
    let events = decoded(&scan(asterpath.store(), first.id()).await);
    let turn = events
        .iter()
        .find_map(|event| match event {
            SessionEvent::TurnStarted { turn_id } => Some(*turn_id),
            _ => None,
        })
        .expect("turn");
    let step = events
        .iter()
        .find_map(|event| match event {
            SessionEvent::StepStarted { step_id, .. } => Some(*step_id),
            _ => None,
        })
        .expect("step");
    for id in [
        first.id().as_bytes().as_slice(),
        second.id().as_bytes(),
        turn.as_bytes(),
        step.as_bytes(),
    ] {
        assert_eq!(&id[..8], &expected[..]);
    }
    let unique: std::collections::HashSet<_> = [
        first.id().to_hex(),
        second.id().to_hex(),
        turn.to_hex(),
        step.to_hex(),
    ]
    .into();
    assert_eq!(unique.len(), 4, "ids are unique under a fixed clock");

    asterpath.shutdown().await.unwrap();
    cleanup(&db);
}
