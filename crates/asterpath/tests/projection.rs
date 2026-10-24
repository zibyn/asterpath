//! `SessionHandle::projection` reads the model-visible projection from the
//! committed log, on both the in-memory and the sqlite backend.

use std::sync::Arc;
use std::time::Duration;

use asterpath::DefaultCompactor;
use asterpath::protocol::{
    AgentError, CompactResult, CompactionConfig, Compactor, InputTokens, Item, ModelId, ModelSpec,
    OutputTokens, Part, ProjectedOrigin, Provider, ProviderCatalog, ProviderId, RequestConfig,
    Role, SessionEvent, SessionSpec, SessionStore, StopReason, SubagentConfig, ToolResultStatus,
    ToolSelection, TurnConfig, Usage, UserInput, encode_entry, history, scan_all,
};
use asterpath::{AsterError, Asterpath, AsterpathBuilder, InputId, SessionHandle, ToolCallId};
use asterpath_testing::{InMemoryEventLog, ScriptedProvider};
use futures_util::StreamExt;

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn assistant(value: &str) -> Item {
    Item::Message {
        role: Role::Assistant,
        parts: vec![text(value)],
        raw: None,
    }
}

fn usage() -> Usage {
    Usage {
        input: InputTokens {
            uncached: 5,
            cache_read: 0,
            cache_write: 0,
        },
        output: OutputTokens {
            total: 5,
            reasoning: 0,
        },
    }
}

fn spec() -> SessionSpec {
    SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("model-1"),
            context_window: Some(1000),
        },
        system: vec![text("be helpful")],
        tools: ToolSelection::All,
        request: RequestConfig::default(),
        turn: {
            let mut config = TurnConfig::default();
            config.initial_backoff = Duration::ZERO;
            config.max_backoff = Duration::ZERO;
            config
        },
        compaction: {
            let mut config = CompactionConfig::default();
            config.max_auto_compactions_per_turn = 0;
            config
        },
        subagents: SubagentConfig::default(),
        metadata: Default::default(),
    }
}

fn input(id: u8, message: &str) -> UserInput {
    UserInput {
        id: InputId::from_bytes([id; 16]),
        parts: vec![text(message)],
    }
}

async fn facade(builder: AsterpathBuilder, provider: Arc<ScriptedProvider>) -> Asterpath {
    let compactor: Arc<dyn Compactor> = Arc::new(DefaultCompactor::new(Arc::new(
        ProviderCatalog::new(vec![Arc::clone(&provider) as Arc<dyn Provider>]).expect("catalog"),
    )));
    builder
        .providers(vec![provider as Arc<dyn Provider>])
        .compactor(compactor)
        .build()
        .await
        .expect("facade builds")
}

async fn turn(handle: &SessionHandle, id: u8) {
    handle
        .submit_input(input(id, &format!("message {id}")))
        .await
        .expect("input");
    let mut stream = handle.drive();
    while let Some(event) = stream.next().await {
        event.expect("drive succeeds");
    }
}

async fn assert_matches_history(
    asterpath: &Asterpath,
    handle: &SessionHandle,
) -> Vec<ProjectedOrigin> {
    let projection = handle.projection().await.expect("projection");
    let entries = scan_all(&*asterpath.store(), handle.id())
        .await
        .expect("scan");
    let flattened: Vec<Item> = projection
        .iter()
        .flat_map(|entry| entry.items.clone())
        .collect();
    assert_eq!(flattened, history(&entries).expect("history"));
    projection.into_iter().map(|entry| entry.origin).collect()
}

async fn projection_follows_the_log(builder: AsterpathBuilder) {
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(vec![assistant("one")], StopReason::EndTurn, usage())
            .respond(vec![assistant("two")], StopReason::EndTurn, usage())
            .respond(vec![assistant("the summary")], StopReason::EndTurn, usage())
            .respond(vec![assistant("three")], StopReason::EndTurn, usage()),
    );
    let asterpath = facade(builder, provider).await;
    let handle = asterpath.create_session(spec()).await.expect("session");
    turn(&handle, 1).await;
    turn(&handle, 2).await;

    let origins = assert_matches_history(&asterpath, &handle).await;
    assert_eq!(origins.len(), 4, "{origins:?}");
    assert!(
        origins
            .iter()
            .all(|origin| matches!(origin, ProjectedOrigin::Log(_)))
    );

    let result = handle.compact_now().await.expect("compact_now");
    assert!(matches!(result, CompactResult::Applied(_)), "{result:?}");
    turn(&handle, 3).await;

    let origins = assert_matches_history(&asterpath, &handle).await;
    assert!(
        origins
            .iter()
            .any(|origin| matches!(origin, ProjectedOrigin::Summary(_))),
        "{origins:?}"
    );
}

#[tokio::test]
async fn projection_matches_history_on_the_memory_backend() {
    let builder = Asterpath::builder().store(Arc::new(InMemoryEventLog::new()));
    projection_follows_the_log(builder).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn projection_matches_history_on_the_sqlite_backend() {
    let path = std::env::temp_dir().join(format!("asterpath-projection-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    projection_follows_the_log(Asterpath::builder().database(&path)).await;
    for suffix in ["", "-wal", "-shm", ".lock"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

#[tokio::test]
async fn a_corrupt_log_is_reported_as_corrupt() {
    let log = Arc::new(InMemoryEventLog::new());
    let provider = Arc::new(ScriptedProvider::new("p"));
    let asterpath = facade(
        Asterpath::builder().store(Arc::clone(&log) as Arc<dyn SessionStore>),
        provider,
    )
    .await;
    let handle = asterpath.create_session(spec()).await.expect("session");
    // Decodable, but the result refers to a tool call the log never requested.
    let orphan = SessionEvent::ToolResult {
        tool_call_id: ToolCallId::from_bytes([7; 16]),
        status: ToolResultStatus::Completed,
        output: Some(vec![text("orphan")]),
        error: None,
        truncated: false,
    };
    log.append(encode_entry(handle.id(), None, &orphan).expect("encode"))
        .await
        .expect("append");

    let error = handle.projection().await.expect_err("corrupt");
    assert!(
        matches!(
            error,
            AsterError::Agent(AgentError::Corrupt { session, .. }) if session == handle.id()
        ),
        "{error:?}"
    );
}
