//! Read-only handle queries: the effective config and context usage are
//! folds of the log, readable from any process without a claim.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use asterpath::protocol::{
    AgentEvent, CompactionConfig, ConfigPatch, InputId, InputTokens, Item, ModelId, ModelSpec,
    OutputTokens, Part, Provider, ProviderId, RequestConfig, Role, SessionSpec, StopReason,
    SubagentConfig, ToolSelection, TurnConfig, Usage, UserInput,
};
use asterpath::{Asterpath, OwnedSession};
use asterpath_testing::ScriptedProvider;
use futures_util::StreamExt;

static NEXT_DB: AtomicU64 = AtomicU64::new(0);

struct TempDb(PathBuf);

impl TempDb {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "asterpath-config-query-{tag}-{}-{}.db",
            std::process::id(),
            NEXT_DB.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_file(&path);
        Self(path)
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let path = &self.0;
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}-wal", path.display()));
        let _ = fs::remove_file(format!("{}-shm", path.display()));
        let _ = fs::remove_file(format!("{}.lock", path.display()));
    }
}

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn model(name: &str, context_window: Option<u32>) -> ModelSpec {
    ModelSpec {
        provider: ProviderId::new("p"),
        model: ModelId::new(name),
        context_window,
    }
}

fn spec(context_window: Option<u32>) -> SessionSpec {
    SessionSpec {
        system_sections: Vec::new(),
        model: model("model-1", context_window),
        system: vec![text("be helpful")],
        tools: ToolSelection::Names(Vec::new()),
        request: RequestConfig::default(),
        turn: TurnConfig::default(),
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
        metadata: Default::default(),
    }
}

fn usage(uncached: u64, cache_read: u64, cache_write: u64) -> Usage {
    Usage {
        input: InputTokens {
            uncached,
            cache_read,
            cache_write,
            cache_write_1h: 0,
        },
        output: OutputTokens {
            total: 7,
            reasoning: 0,
        },
    }
}

fn reply(provider: ScriptedProvider, usage: Usage) -> ScriptedProvider {
    provider.respond(
        vec![Item::Message {
            role: Role::Assistant,
            parts: vec![text("ok")],
            raw: None,
        }],
        StopReason::EndTurn,
        usage,
    )
}

/// One "process": its own engine over the shared database file.
async fn process(db: &Path, provider: ScriptedProvider) -> Asterpath {
    Asterpath::builder()
        .database(db)
        .providers(vec![Arc::new(provider) as Arc<dyn Provider>])
        .build()
        .await
        .expect("build")
}

async fn turn(session: &OwnedSession, id: u8) {
    session
        .submit_input(UserInput {
            id: InputId::from_bytes([id; 16]),
            parts: vec![text("hi")],
        })
        .await
        .expect("input");
    let mut stream = session.drive();
    while let Some(event) = stream.next().await {
        if matches!(
            event.expect("drive"),
            AgentEvent::Idle { .. } | AgentEvent::TurnCommitted { .. }
        ) {
            break;
        }
    }
}

#[tokio::test]
async fn config_is_the_fold_of_the_log_in_any_process() {
    let db = TempDb::new("config");
    let here = process(&db.0, ScriptedProvider::new("p")).await;
    let elsewhere = process(&db.0, ScriptedProvider::new("p")).await;
    let session = here.create_session(spec(Some(200_000))).await.unwrap();
    assert_eq!(
        session.config().await.unwrap().model,
        model("model-1", Some(200_000))
    );

    let mut patch = ConfigPatch::default();
    patch.model = Some(model("model-2", Some(100_000)));
    session.update_config(patch).await.unwrap();
    assert_eq!(
        session.config().await.unwrap().model,
        model("model-2", Some(100_000))
    );

    let mut patch = ConfigPatch::default();
    patch.model = Some(model("model-3", None));
    elsewhere
        .open_session(session.id())
        .await
        .unwrap()
        .update_config(patch)
        .await
        .unwrap();
    assert_eq!(
        session.config().await.unwrap().model,
        model("model-3", None)
    );
}

#[tokio::test]
async fn context_usage_is_the_latest_request_input_against_the_window() {
    let db = TempDb::new("usage");
    let provider = reply(
        reply(ScriptedProvider::new("p"), usage(100, 20, 3)),
        usage(200, 50, 5),
    );
    let asterpath = process(&db.0, provider).await;
    let session = asterpath
        .create_session(spec(Some(200_000)))
        .await
        .unwrap()
        .claim()
        .await
        .unwrap();

    let before = session.context_usage().await.unwrap();
    assert_eq!(before.used, None);
    assert_eq!(before.window, Some(200_000));

    turn(&session, 1).await;
    assert_eq!(session.context_usage().await.unwrap().used, Some(123));

    turn(&session, 2).await;
    assert_eq!(session.context_usage().await.unwrap().used, Some(255));
}

#[tokio::test]
async fn context_window_is_none_when_not_configured() {
    let db = TempDb::new("window");
    let asterpath = process(&db.0, ScriptedProvider::new("p")).await;
    let session = asterpath.create_session(spec(None)).await.unwrap();
    assert_eq!(session.context_usage().await.unwrap().window, None);
}
