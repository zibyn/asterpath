//! Facade wiring for the optional retirement seam (retention.md §3, §9).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use asterpath::{AsterError, Asterpath};
use asterpath_protocol::{
    AgentError, CompactionConfig, ModelId, ModelSpec, Part, ProviderId, RequestConfig, SessionSpec,
    SubagentConfig, ToolSelection, TurnConfig,
};
use asterpath_protocol::{LogQuery, SessionCatalog, SessionId, StorageError};
use asterpath_testing::{InMemoryEventLog, InMemoryStorage, ScriptedProvider};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};

static NEXT_DB: AtomicU64 = AtomicU64::new(0);

fn temp_db(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "asterpath-retirement-{tag}-{}-{}.db",
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

fn spec(provider: &str) -> SessionSpec {
    SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new(provider),
            model: ModelId::new("model-1"),
            context_window: Some(2_000),
        },
        system: vec![Part::Text {
            text: "be helpful".to_owned(),
        }],
        tools: ToolSelection::All,
        request: RequestConfig::default(),
        turn: TurnConfig::default(),
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
    }
}

async fn scan_len(asterpath: &Asterpath, session: SessionId) -> usize {
    let log = asterpath.event_log();
    let mut entries = log.scan(LogQuery {
        session,
        after: None,
        limit: None,
    });
    let mut count = 0;
    while let Some(entry) = entries.next().await {
        entry.expect("scan succeeds");
        count += 1;
    }
    count
}

#[tokio::test]
async fn sqlite_exposes_retirement_and_hides_the_retired_session() {
    let db = temp_db("sqlite");
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![Arc::new(ScriptedProvider::new("p"))])
        .build()
        .await
        .expect("facade builds");
    let handle = asterpath
        .create_session(spec("p"))
        .await
        .expect("create session");
    let session = handle.id();

    let retirement = asterpath
        .session_retirement()
        .expect("the sqlite backend provides retirement");
    let frontier = asterpath
        .event_log()
        .frontier(session)
        .await
        .expect("frontier")
        .expect("session exists");
    retirement.retire(session, frontier).await.expect("retire");

    assert!(
        asterpath
            .event_log()
            .frontier(session)
            .await
            .expect("frontier")
            .is_none()
    );
    assert_eq!(scan_len(&asterpath, session).await, 0);
    match asterpath.open_session(session).await {
        Err(AsterError::Agent(AgentError::NotFound { session: found })) => {
            assert_eq!(found, session);
        }
        other => panic!("expected NotFound after retirement, got {other:?}"),
    }

    asterpath.shutdown().await.expect("shutdown");
    cleanup(&db);
}

#[tokio::test]
async fn open_session_reports_unknown_sessions_as_not_found() {
    let db = temp_db("unknown");
    let asterpath = Asterpath::builder()
        .database(&db)
        .providers(vec![Arc::new(ScriptedProvider::new("p"))])
        .build()
        .await
        .expect("facade builds");

    let unknown = SessionId::from_bytes([0xee; 16]);
    match asterpath.open_session(unknown).await {
        Err(AsterError::Agent(AgentError::NotFound { session })) => {
            assert_eq!(session, unknown);
        }
        other => panic!("expected NotFound, got {other:?}"),
    }

    asterpath.shutdown().await.expect("shutdown");
    cleanup(&db);
}

struct DummyCatalog;

impl SessionCatalog for DummyCatalog {
    fn parent(
        &self,
        _session: SessionId,
    ) -> BoxFuture<'_, Result<Option<SessionId>, StorageError>> {
        Box::pin(async { Ok(None) })
    }

    fn children(&self, _session: SessionId) -> BoxStream<'_, Result<SessionId, StorageError>> {
        stream::empty().boxed()
    }
}

#[tokio::test]
async fn injected_backend_without_retirement_returns_none() {
    let log = Arc::new(InMemoryEventLog::new());
    let storage = Arc::new(InMemoryStorage::new(Arc::clone(&log)));
    let asterpath = Asterpath::builder()
        .providers(vec![Arc::new(ScriptedProvider::new("p"))])
        .event_log(log)
        .storage(storage)
        .session_catalog(Arc::new(DummyCatalog))
        .build()
        .await
        .expect("facade builds with injected storage");

    assert!(
        asterpath.session_retirement().is_none(),
        "custom storage may legitimately not implement retirement"
    );

    asterpath.shutdown().await.expect("shutdown");
}
