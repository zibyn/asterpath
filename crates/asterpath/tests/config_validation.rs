//! Facade configuration validation (`packaging.md` §3.2).

use std::collections::BTreeMap;
use std::sync::Arc;

use asterpath::{AsterError, Asterpath};
use asterpath_protocol::{
    ModelAlias, ModelId, ModelSpec, Parallelism, Provenance, Provider, ProviderId, ReplaySafety,
    SideEffect, Tool, ToolArgs, ToolContext, ToolDescriptor, ToolError, ToolName, ToolOutput,
    TransportKind,
};
use asterpath_testing::{InMemoryEventLog, ScriptedProvider};
use futures_core::future::BoxFuture;

struct DummyTool(ToolDescriptor);

impl Tool for DummyTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.0
    }

    fn invoke(
        &self,
        _args: ToolArgs,
        _ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async { Ok(ToolOutput::new(Vec::new())) })
    }
}

fn alias(provider: &str) -> BTreeMap<ModelAlias, ModelSpec> {
    let mut aliases = BTreeMap::new();
    aliases.insert(
        ModelAlias::new("fast"),
        ModelSpec {
            provider: ProviderId::new(provider),
            model: ModelId::new("small"),
            context_window: None,
        },
    );
    aliases
}

#[tokio::test]
async fn an_injected_store_needs_no_database() {
    Asterpath::builder()
        .store(Arc::new(InMemoryEventLog::new()))
        .build()
        .await
        .expect("an injected store replaces the sqlite backend");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn database_is_required_only_when_no_backend_is_injected() {
    let error = Asterpath::builder().build().await.unwrap_err();
    assert!(matches!(error, AsterError::Config { .. }), "{error:?}");
}

#[tokio::test]
async fn p8_in_process_tool_names_must_not_contain_the_external_separator() {
    let tool = Arc::new(DummyTool(ToolDescriptor {
        name: ToolName::new("a__b"),
        description: String::new(),
        schema: serde_json::json!({"type": "object"}),
        parallelism: Parallelism::Parallel,
        replay: ReplaySafety::Safe,
        side_effect: SideEffect::ReadOnly,
        provenance: Provenance::InProcess,
        timeout: None,
        max_output_bytes: None,
    }));
    let error = Asterpath::builder()
        .tools(vec![tool])
        .build()
        .await
        .unwrap_err();
    assert!(matches!(error, AsterError::Config { .. }), "{error:?}");
    assert!(error.to_string().contains("a__b"), "{error}");
}

#[tokio::test]
async fn p2_external_tool_names_are_validated_at_the_facade() {
    let descriptor = |name: &str, provenance: Provenance| ToolDescriptor {
        name: ToolName::new(name),
        description: String::new(),
        schema: serde_json::json!({"type": "object"}),
        parallelism: Parallelism::Parallel,
        replay: ReplaySafety::Safe,
        side_effect: SideEffect::ReadOnly,
        provenance,
        timeout: None,
        max_output_bytes: None,
    };
    let external = Provenance::External {
        transport: TransportKind::Stdio,
    };
    let error = Asterpath::builder()
        .tools(vec![Arc::new(DummyTool(descriptor("bad.name", external)))])
        .build()
        .await
        .unwrap_err();
    assert!(matches!(error, AsterError::Config { .. }), "{error:?}");
    assert!(error.to_string().contains("bad.name"), "{error}");

    let path = std::env::temp_dir().join(format!(
        "asterpath-external-name-{}.sqlite",
        std::process::id()
    ));
    let asterpath = Asterpath::builder()
        .database(&path)
        .tools(vec![Arc::new(DummyTool(descriptor(
            "plugin__tool",
            external,
        )))])
        .build()
        .await
        .expect("an external composed name is registrable");
    asterpath.shutdown().await.expect("shutdown");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn a_model_alias_must_reference_an_injected_provider() {
    let provider = Arc::new(ScriptedProvider::new("anthropic")) as Arc<dyn Provider>;
    let error = Asterpath::builder()
        .provider(provider)
        .model_aliases(alias("missing"))
        .build()
        .await
        .unwrap_err();
    assert!(matches!(error, AsterError::Config { .. }), "{error:?}");
}
