//! ADR-0044: session metadata is patched key by key through any handle,
//! claimed or not, and the catalog lists the merged result.

use std::collections::BTreeMap;
use std::sync::Arc;

use asterpath::protocol::{
    CompactionConfig, ModelId, ModelSpec, Provider, ProviderId, RequestConfig, SessionQuery,
    SessionSpec, SubagentConfig, ToolSelection, TurnConfig,
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

fn patch(pairs: &[(&str, Option<&str>)]) -> BTreeMap<String, Option<String>> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.map(str::to_owned)))
        .collect()
}

#[tokio::test]
async fn any_handle_patches_metadata_and_the_catalog_lists_the_merge() {
    let provider = Arc::new(ScriptedProvider::new("p"));
    let asterpath = Asterpath::builder()
        .store(Arc::new(InMemoryEventLog::new()))
        .providers(vec![provider as Arc<dyn Provider>])
        .build()
        .await
        .expect("build");
    let id = asterpath.create_session(spec()).await.expect("create").id();
    let handle = asterpath.open_session(id).await.expect("open");

    handle
        .update_metadata(patch(&[("name", Some("renamed")), ("archived", Some("1"))]))
        .await
        .expect("rename and archive");
    handle
        .update_metadata(patch(&[("archived", None)]))
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
        .update_metadata(patch(&[("name", Some(&long))]))
        .await
        .expect_err("over the limits");
    assert!(matches!(error, AsterError::Config { .. }), "{error:?}");
}
