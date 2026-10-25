//! Finding a work root's sessions in the catalog (ADR-0039 metadata filter).

use std::path::Path;
use std::sync::Arc;

use asterpath::Asterpath;
use asterpath::protocol::{
    CompactionConfig, ModelId, ModelSpec, Provider, ProviderId, RequestConfig, SessionId,
    SessionSpec, SubagentConfig, ToolSelection, TurnConfig,
};
use asterpath_host::{HostError, by_id_or_name, in_root, metadata};
use asterpath_testing::{InMemoryEventLog, ManualClock, ScriptedProvider};

fn spec(root: &str, name: Option<&str>) -> SessionSpec {
    SessionSpec {
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
        metadata: metadata(Path::new(root), name),
    }
}

/// Sessions created in order, one second apart, so the last is the most
/// recently active.
async fn catalog(sessions: &[(&str, Option<&str>)]) -> (Asterpath, Vec<SessionId>) {
    let clock = Arc::new(ManualClock::new());
    let asterpath = Asterpath::builder()
        .store(Arc::new(InMemoryEventLog::with_clock(clock.clone())))
        .clock(clock.clone())
        .providers(vec![
            Arc::new(ScriptedProvider::new("p")) as Arc<dyn Provider>
        ])
        .build()
        .await
        .expect("build");
    let mut ids = Vec::new();
    for (root, name) in sessions {
        clock.advance(1000);
        let handle = asterpath.create_session(spec(root, *name)).await.unwrap();
        ids.push(handle.id());
    }
    (asterpath, ids)
}

#[tokio::test]
async fn in_root_keeps_the_root_and_puts_the_last_active_first() {
    let (asterpath, ids) =
        catalog(&[("/a", None), ("/a", None), ("/b", None), ("/a/c", None)]).await;
    let here: Vec<SessionId> = in_root(&asterpath, Path::new("/a"))
        .await
        .unwrap()
        .iter()
        .map(|entry| entry.session)
        .collect();
    assert_eq!(here, [ids[1], ids[0]]);
}

#[tokio::test]
async fn a_name_resolves_to_its_last_active_session_in_the_root_and_an_id_to_itself() {
    let (asterpath, ids) = catalog(&[
        ("/a", Some("fix")),
        ("/a", Some("fix")),
        ("/a", Some("other")),
        ("/b", Some("fix")),
    ])
    .await;
    let root = Path::new("/a");
    assert_eq!(
        by_id_or_name(&asterpath, root, "fix")
            .await
            .unwrap()
            .session,
        ids[1]
    );
    assert_eq!(
        by_id_or_name(&asterpath, root, &ids[3].to_hex())
            .await
            .unwrap()
            .session,
        ids[3],
        "an id resolves outside the root"
    );
    assert!(matches!(
        by_id_or_name(&asterpath, root, "nope").await,
        Err(HostError::NotFound(_))
    ));
    assert!(
        by_id_or_name(&asterpath, Path::new("/c"), "fix")
            .await
            .is_err(),
        "a name resolves only in the root"
    );
}
