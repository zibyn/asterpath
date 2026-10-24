//! Tracing through the facade: `Asterpath::trace(session)` folds the session's
//! log on demand and never writes to it. There is no push channel (ADR-0026).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use asterpath::Asterpath;
use asterpath::protocol::{
    CompactionConfig, Item, ModelId, ModelSpec, Part, Provider, ProviderId, RequestConfig, Role,
    SessionSpec, SpanKind, SpanStatus, StopReason, SubagentConfig, ToolSelection, TraceError,
    TurnConfig, Usage, UserInput,
};
use asterpath::protocol::{InputId, LogEntry, LogQuery, SessionId, SessionStore};
use asterpath_testing::ScriptedProvider;
use futures_util::StreamExt;

static NEXT_DB: AtomicU64 = AtomicU64::new(0);

fn temp_db(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "asterpath-tracing-{tag}-{}-{}.db",
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

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn spec() -> SessionSpec {
    SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("model-1"),
            context_window: None,
        },
        system: vec![],
        tools: ToolSelection::Names(Vec::new()),
        request: RequestConfig::default(),
        turn: TurnConfig::default(),
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
        metadata: Default::default(),
    }
}

fn response() -> Vec<Item> {
    vec![Item::Message {
        role: Role::Assistant,
        parts: vec![text("hello")],
        raw: None,
    }]
}

async fn scan(log: &Arc<dyn SessionStore>, session: SessionId) -> Vec<LogEntry> {
    log.scan(LogQuery {
        session,
        after: None,
        limit: None,
    })
    .map(|entry| entry.expect("scan"))
    .collect()
    .await
}

#[tokio::test]
async fn trace_folds_the_session_without_touching_the_log() {
    let db = temp_db("pull");
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        response(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let framework = Asterpath::builder()
        .database(&db)
        .provider(provider as Arc<dyn Provider>)
        .build()
        .await
        .expect("facade builds");

    let handle = framework.create_session(spec()).await.expect("session");
    let session = handle.id();
    handle
        .submit_input(UserInput {
            id: InputId::from_bytes([0x77; 16]),
            parts: vec![text("hi")],
        })
        .await
        .expect("input");
    let mut drive = handle.drive();
    while let Some(event) = drive.next().await {
        event.expect("drive");
    }
    drop(drive);

    let log = framework.store();
    let before = scan(&log, session).await;
    let spans = framework.trace(session).await.expect("trace");
    assert_eq!(
        before,
        scan(&log, session).await,
        "tracing never changes the log"
    );

    assert!(spans.len() >= 4, "session, turn, step and model call");
    assert_eq!(
        spans
            .iter()
            .filter(|span| span.kind == SpanKind::Session)
            .count(),
        1
    );
    for span in spans.iter().filter(|span| span.kind != SpanKind::Session) {
        assert!(span.ended_at.is_some(), "{:?} is closed", span.kind);
        assert_eq!(span.status, SpanStatus::Ok);
    }

    framework.shutdown().await.expect("shutdown");
    cleanup(&db);
}

#[tokio::test]
async fn retired_sessions_disappear_from_the_trace() {
    let db = temp_db("retired");
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        response(),
        StopReason::EndTurn,
        Usage::default(),
    ));
    let framework = Asterpath::builder()
        .database(&db)
        .provider(provider as Arc<dyn Provider>)
        .build()
        .await
        .expect("facade builds");

    let handle = framework.create_session(spec()).await.expect("session");
    let session = handle.id();
    handle
        .submit_input(UserInput {
            id: InputId::from_bytes([0x78; 16]),
            parts: vec![text("hi")],
        })
        .await
        .expect("input");
    let mut drive = handle.drive();
    while let Some(event) = drive.next().await {
        event.expect("drive");
    }
    drop(drive);

    let log = framework.store();
    let frontier = log.frontier(session).await.expect("frontier").expect("log");
    framework
        .store()
        .retire(session, frontier)
        .await
        .expect("retire");

    let error = framework.trace(session).await.expect_err("retired");
    assert!(matches!(error, TraceError::NotFound { session: s } if s == session));

    framework.shutdown().await.expect("shutdown");
    cleanup(&db);
}
