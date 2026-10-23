//! Real-time tracing through the facade: `TraceTap` pairs Started/Ended off the
//! engine append entry and never writes to the log (T14).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use asterpath::Asterpath;
use asterpath_core::{EventLog, InputId, LogEntry, LogQuery, SessionId};
use asterpath_protocol::{
    CompactionConfig, Item, ModelId, ModelSpec, Part, Provider, ProviderId, RequestConfig, Role,
    SessionSpec, Span, SpanFilter, SpanKind, SpanStatus, SpanUpdate, StopReason, SubagentConfig,
    ToolSelection, TraceError, TurnConfig, Usage, UserInput,
};
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
    }
}

fn response() -> Vec<Item> {
    vec![Item::Message {
        role: Role::Assistant,
        parts: vec![text("hello")],
    }]
}

async fn scan(log: &Arc<dyn EventLog>, session: SessionId) -> Vec<LogEntry> {
    log.scan(LogQuery {
        session,
        after: None,
        limit: None,
    })
    .map(|entry| entry.expect("scan"))
    .collect()
    .await
}

async fn query_spans(framework: &Asterpath, session: SessionId) -> Vec<Span> {
    let query = framework.trace();
    let mut stream = query.spans(SpanFilter {
        session: Some(session),
        ..SpanFilter::default()
    });
    let mut spans = Vec::new();
    while let Some(span) = stream.next().await {
        spans.push(span.expect("span"));
    }
    spans
}

#[tokio::test]
async fn t14_tap_pairs_started_and_ended_without_touching_the_log() {
    let db = temp_db("tap");
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

    let tap = framework.trace_tap();
    let mut updates = tap.subscribe();

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

    let log = framework.event_log();
    let before = scan(&log, session).await;
    let expected = query_spans(&framework, session).await;
    assert!(expected.len() >= 4, "session, turn, step and model call");

    let closed = expected
        .iter()
        .filter(|span| span.kind != SpanKind::Session)
        .count();
    let needed = expected.len() + closed;

    let mut received: Vec<SpanUpdate> = Vec::new();
    let collected = tokio::time::timeout(Duration::from_secs(2), async {
        while received.len() < needed {
            match updates.next().await {
                Some(update) => received.push(update.expect("update")),
                None => break,
            }
        }
        received
    })
    .await
    .expect("tap settles without wall-clock races");

    let after = scan(&log, session).await;
    assert_eq!(before, after, "tracing never changes the log");

    let started: Vec<&Span> = collected
        .iter()
        .filter_map(|update| match update {
            SpanUpdate::Started(span) => Some(span),
            _ => None,
        })
        .collect();
    let ended: Vec<&Span> = collected
        .iter()
        .filter_map(|update| match update {
            SpanUpdate::Ended(span) => Some(span),
            _ => None,
        })
        .collect();

    let session_span = expected
        .iter()
        .find(|span| span.kind == SpanKind::Session)
        .expect("session span");
    let session_starts = started
        .iter()
        .filter(|span| span.span == session_span.span)
        .count();
    assert_eq!(session_starts, 1, "session span starts exactly once");
    assert!(
        ended.iter().all(|span| span.span != session_span.span),
        "session spans never end"
    );

    for span in &expected {
        if span.kind == SpanKind::Session {
            continue;
        }
        let starts = started.iter().filter(|seen| seen.span == span.span).count();
        let ends = ended.iter().filter(|seen| seen.span == span.span).count();
        assert_eq!(starts, 1, "one Started for {:?}", span.kind);
        assert_eq!(ends, 1, "one Ended for {:?}", span.kind);
        let start_index = collected
            .iter()
            .position(
                |update| matches!(update, SpanUpdate::Started(seen) if seen.span == span.span),
            )
            .unwrap();
        let end_index = collected
            .iter()
            .position(|update| matches!(update, SpanUpdate::Ended(seen) if seen.span == span.span))
            .unwrap();
        assert!(start_index < end_index, "Started precedes Ended");
        assert_eq!(span.status, SpanStatus::Ok);
    }

    // Started order follows the log-derived span order.
    let started_ids: Vec<_> = started.iter().map(|span| span.span).collect();
    let expected_ids: Vec<_> = expected.iter().map(|span| span.span).collect();
    assert_eq!(started_ids, expected_ids);

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

    let log = framework.event_log();
    let frontier = log.frontier(session).await.expect("frontier").expect("log");
    framework
        .session_retirement()
        .expect("sqlite retirement")
        .retire(session, frontier)
        .await
        .expect("retire");

    let query = framework.trace();
    let mut stream = query.spans(SpanFilter {
        session: Some(session),
        ..SpanFilter::default()
    });
    let error = stream
        .next()
        .await
        .expect("one error")
        .expect_err("retired");
    assert!(matches!(error, TraceError::NotFound { session: s } if s == session));

    framework.shutdown().await.expect("shutdown");
    cleanup(&db);
}
