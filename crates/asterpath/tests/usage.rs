//! Facade assembly: `Asterpath::usage()` folds the assembled log, exposed as
//! the stable `UsageQuery` contract.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use asterpath::{Asterpath, SessionHandle};
use asterpath_protocol::SessionId;
use asterpath_protocol::{
    AgentEvent, CompactionConfig, InputTokens, ModelId, ModelSpec, OutputTokens, Part, ProviderId,
    RequestConfig, SessionSpec, StopReason, SubagentConfig, ToolSelection, TurnConfig, Usage,
    UsageError, UsageFilter, UsageScope, UserInput,
};
use asterpath_testing::ScriptedProvider;
use futures_util::StreamExt;

static NEXT_DB: AtomicU64 = AtomicU64::new(0);

fn temp_db(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "asterpath-usage-{tag}-{}-{}.db",
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

fn assistant(value: &str) -> asterpath_protocol::Item {
    asterpath_protocol::Item::Message {
        role: asterpath_protocol::Role::Assistant,
        parts: vec![text(value)],
    }
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

fn spec() -> SessionSpec {
    SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("model-1"),
            context_window: Some(200_000),
        },
        system: vec![text("be helpful")],
        tools: ToolSelection::Names(Vec::new()),
        request: RequestConfig::default(),
        turn: TurnConfig::default(),
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
    }
}

fn filter(scope: UsageScope) -> UsageFilter {
    UsageFilter {
        scope,
        bucket: None,
        model: None,
        since: None,
        until: None,
        limit: None,
    }
}

async fn drive(session: &SessionHandle) {
    let mut stream = session.drive();
    while let Some(event) = stream.next().await {
        let event = event.expect("drive");
        if matches!(
            event,
            AgentEvent::Idle { .. } | AgentEvent::TurnCommitted { .. }
        ) {
            break;
        }
    }
}

#[tokio::test]
async fn facade_usage_query_folds_live_turns() {
    let db = temp_db("facade");
    let provider = Arc::new(
        ScriptedProvider::new("p")
            .respond(vec![assistant("one")], StopReason::EndTurn, usage(10, 2))
            .respond(vec![assistant("two")], StopReason::EndTurn, usage(20, 3)),
    );
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![provider])
        .build()
        .await
        .expect("build");
    let session = asterpath
        .create_session(spec())
        .await
        .expect("create session");

    session
        .submit_input(UserInput {
            id: asterpath_protocol::InputId::from_bytes([1; 16]),
            parts: vec![text("first")],
        })
        .await
        .expect("input");
    drive(&session).await;
    session
        .submit_input(UserInput {
            id: asterpath_protocol::InputId::from_bytes([2; 16]),
            parts: vec![text("second")],
        })
        .await
        .expect("input");
    drive(&session).await;

    let query = asterpath.usage();
    let report = query
        .totals(filter(UsageScope::Session(session.id())))
        .await
        .expect("totals");
    assert_eq!(report.own.model_calls, usage(30, 5));
    assert_eq!(report.own.total(), 35);
    assert_eq!(report.descendants, Default::default());

    let records: Vec<_> = query
        .records(filter(UsageScope::Session(session.id())))
        .collect()
        .await;
    assert_eq!(records.len(), 2);
    assert!(records.iter().all(|record| record.is_ok()));

    let missing = SessionId::from_bytes([200; 16]);
    match query.totals(filter(UsageScope::Session(missing))).await {
        Err(UsageError::NotFound { session }) => assert_eq!(session, missing),
        other => panic!("expected NotFound, got {other:?}"),
    }

    asterpath.shutdown().await.expect("shutdown");
    cleanup(&db);
}
