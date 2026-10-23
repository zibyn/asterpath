//! Facade configuration validation (`packaging.md` §3.2).

use std::collections::BTreeMap;
use std::sync::Arc;

use asterpath::{AsterError, Asterpath};
use asterpath_protocol::{
    ModelAlias, ModelId, ModelSpec, Parallelism, Provenance, Provider, ProviderId, ReplaySafety,
    SideEffect, Tool, ToolArgs, ToolContext, ToolDescriptor, ToolError, ToolName, ToolOutput,
    TurnConfig,
};
use asterpath_testing::{BackendFactory, BackendHandle, MemoryBackend, ScriptedProvider};
use futures_core::future::BoxFuture;

fn backend() -> BackendHandle {
    MemoryBackend::new().open()
}

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
async fn event_log_without_storage_is_a_config_error() {
    let backend = backend();
    let error = Asterpath::builder()
        .event_log(backend.log)
        .build()
        .await
        .unwrap_err();
    assert!(matches!(error, AsterError::Config { .. }), "{error:?}");
}

#[tokio::test]
async fn storage_without_event_log_is_a_config_error() {
    let backend = backend();
    let error = Asterpath::builder()
        .storage(backend.storage)
        .build()
        .await
        .unwrap_err();
    assert!(matches!(error, AsterError::Config { .. }), "{error:?}");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn database_is_required_only_when_no_backend_is_injected() {
    let error = Asterpath::builder().build().await.unwrap_err();
    assert!(matches!(error, AsterError::Config { .. }), "{error:?}");
}

#[tokio::test]
async fn zero_max_steps_is_a_build_time_config_error() {
    let error = Asterpath::builder()
        .turn(TurnConfig {
            max_steps_per_turn: Some(0),
            ..TurnConfig::default()
        })
        .build()
        .await
        .unwrap_err();
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
