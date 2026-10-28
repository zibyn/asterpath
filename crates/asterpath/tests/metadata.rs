//! ADR-0044: session metadata is patched key by key through any handle,
//! claimed or not, and the catalog lists the merged result.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use asterpath::protocol::{
    AgentError, CompactionConfig, MetadataPatch, ModelId, ModelSpec, Provider, ProviderId,
    RequestConfig, SessionId, SessionQuery, SessionSpec, SubagentConfig, ToolSelection, TurnConfig,
};
use asterpath::{AsterError, Asterpath};
use asterpath_testing::{InMemoryEventLog, ScriptedProvider};

fn spec() -> SessionSpec {
    SessionSpec {
        system_sections: Vec::new(),
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
        metadata: BTreeMap::from([
            ("cwd".to_owned(), "/src".to_owned()),
            ("name".to_owned(), "draft".to_owned()),
        ]),
    }
}

fn patch(set: &[(&str, &str)], remove: &[&str]) -> MetadataPatch {
    MetadataPatch {
        set: set
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect(),
        remove: remove.iter().map(|k| (*k).to_owned()).collect(),
    }
}

async fn memory_facade() -> Asterpath {
    let provider = Arc::new(ScriptedProvider::new("p"));
    Asterpath::builder()
        .store(Arc::new(InMemoryEventLog::new()))
        .providers(vec![provider as Arc<dyn Provider>])
        .build()
        .await
        .expect("build")
}

static NEXT_DB: AtomicU64 = AtomicU64::new(0);

fn temp_db(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "asterpath-metadata-{tag}-{}-{}.db",
        std::process::id(),
        NEXT_DB.fetch_add(1, Ordering::Relaxed)
    ));
    cleanup(&path);
    path
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_file(path);
    for suffix in ["-wal", "-shm", ".lock"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

/// One "process": its own engine over the shared database file.
async fn process(db: &Path) -> Asterpath {
    Asterpath::builder()
        .database(db)
        .providers(vec![
            Arc::new(ScriptedProvider::new("p")) as Arc<dyn Provider>
        ])
        .build()
        .await
        .expect("build")
}

async fn metadata_of(asterpath: &Asterpath, session: SessionId) -> BTreeMap<String, String> {
    asterpath
        .sessions(SessionQuery::default())
        .await
        .expect("sessions")
        .into_iter()
        .find(|entry| entry.session == session)
        .expect("listed")
        .metadata
}

#[tokio::test]
async fn any_handle_patches_metadata_and_the_catalog_lists_the_merge() {
    let asterpath = memory_facade().await;
    let id = asterpath.create_session(spec()).await.expect("create").id();
    let handle = asterpath.open_session(id).await.expect("open");

    handle
        .update_metadata(patch(&[("name", "renamed"), ("archived", "1")], &[]))
        .await
        .expect("rename and archive");
    handle
        .update_metadata(patch(&[], &["archived"]))
        .await
        .expect("unarchive");
    let listed = asterpath
        .sessions(SessionQuery::default())
        .await
        .expect("sessions");
    assert_eq!(
        listed[0].metadata,
        BTreeMap::from([
            ("cwd".to_owned(), "/src".to_owned()),
            ("name".to_owned(), "renamed".to_owned()),
        ])
    );

    let long = "v".repeat(4097);
    let error = handle
        .update_metadata(patch(&[("name", &long)], &[]))
        .await
        .expect_err("over the limits");
    assert!(matches!(error, AsterError::Config { .. }), "{error:?}");
}

#[tokio::test]
async fn a_retired_session_reports_not_found() {
    let asterpath = memory_facade().await;
    let stale = asterpath.create_session(spec()).await.expect("create");
    // A handle opened before retirement outlives it.
    let opened = asterpath.open_session(stale.id()).await.expect("open");
    stale
        .claim()
        .await
        .expect("claim")
        .retire()
        .await
        .expect("retire");
    let error = opened
        .update_metadata(patch(&[("name", "x")], &[]))
        .await
        .expect_err("retired");
    assert!(
        matches!(&error, AsterError::Agent(AgentError::NotFound { session }) if *session == stale.id()),
        "{error:?}"
    );
}

#[tokio::test]
async fn metadata_changes_leave_projection_and_config_alone() {
    let asterpath = memory_facade().await;
    let handle = asterpath.create_session(spec()).await.expect("create");
    let (projection, config) = (
        handle.projection().await.expect("projection"),
        handle.config().await.expect("config"),
    );
    handle
        .update_metadata(patch(&[("title", "x")], &["name"]))
        .await
        .expect("update");
    assert_eq!(handle.projection().await.expect("projection"), projection);
    assert_eq!(handle.config().await.expect("config"), config);
}

#[tokio::test]
async fn sqlite_catalog_metadata_survives_a_reopen() {
    let db = temp_db("reopen");
    let asterpath = process(&db).await;
    let id = asterpath.create_session(spec()).await.expect("create").id();
    asterpath
        .open_session(id)
        .await
        .expect("open")
        .update_metadata(patch(&[("title", "x")], &["name"]))
        .await
        .expect("update");
    let before = metadata_of(&asterpath, id).await;
    asterpath.shutdown().await.expect("shutdown");

    let reopened = process(&db).await;
    assert_eq!(metadata_of(&reopened, id).await, before);
    assert_eq!(
        before,
        BTreeMap::from([
            ("cwd".to_owned(), "/src".to_owned()),
            ("title".to_owned(), "x".to_owned()),
        ])
    );
    reopened.shutdown().await.expect("shutdown");
    cleanup(&db);
}

#[tokio::test]
async fn two_processes_changing_different_keys_keep_both() {
    let db = temp_db("two");
    let (first, second) = (process(&db).await, process(&db).await);
    let id = first.create_session(spec()).await.expect("create").id();
    let (a, b) = (
        first.open_session(id).await.expect("open"),
        second.open_session(id).await.expect("open"),
    );
    let (left, right) = tokio::join!(
        a.update_metadata(patch(&[("title", "x")], &[])),
        b.update_metadata(patch(&[("archived", "1")], &[])),
    );
    left.expect("first");
    right.expect("second");

    let merged = metadata_of(&second, id).await;
    assert_eq!(merged.get("title").map(String::as_str), Some("x"));
    assert_eq!(merged.get("archived").map(String::as_str), Some("1"));
    first.shutdown().await.expect("shutdown");
    second.shutdown().await.expect("shutdown");
    cleanup(&db);
}
