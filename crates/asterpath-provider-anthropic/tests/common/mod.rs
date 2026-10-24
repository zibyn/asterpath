//! Anthropic wire fixtures over the shared loopback mock server.

#![allow(dead_code)]

use asterpath_protocol::{
    ModelId, ModelRequest, ModelSpec, Parallelism, Part, Provenance, Provider, ProviderError,
    ProviderId, ReplaySafety, RequestConfig, SideEffect, StreamEvent, ToolChoice, ToolDescriptor,
    ToolName,
};
use asterpath_provider_anthropic::AnthropicProvider;
pub use asterpath_testing::http_mock::{MockServer, Reply};
use futures_util::StreamExt;

pub fn sse_body(events: &[(&str, &str)]) -> String {
    let mut body = String::new();
    for (event, data) in events {
        body.push_str(&format!("event: {event}\ndata: {data}\n\n"));
    }
    body
}

/// A valid, empty response (no content blocks, `end_turn`).
pub fn empty_sse() -> String {
    sse_body(&[
        (
            "message_start",
            r#"{"type":"message_start","message":{"id":"msg_empty","usage":{"input_tokens":1,"output_tokens":0}}}"#,
        ),
        (
            "message_delta",
            r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":0}}"#,
        ),
        ("message_stop", r#"{"type":"message_stop"}"#),
    ])
}

pub fn provider(server: &MockServer) -> AnthropicProvider {
    AnthropicProvider::new("test-key").with_base_url(server.base_url())
}

pub fn model() -> ModelSpec {
    ModelSpec {
        provider: ProviderId::new("anthropic"),
        model: ModelId::new("claude-test"),
        context_window: Some(200_000),
    }
}

pub fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

pub fn base_request() -> ModelRequest {
    ModelRequest::new(
        model(),
        0,
        vec![text("be helpful")],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        &RequestConfig::default(),
    )
}

pub fn descriptor(name: &str) -> ToolDescriptor {
    ToolDescriptor {
        name: ToolName::new(name),
        description: format!("{name} tool"),
        schema: serde_json::json!({
            "type": "object",
            "properties": {"q": {"type": "string"}},
            "required": ["q"]
        }),
        parallelism: Parallelism::Parallel,
        replay: ReplaySafety::Safe,
        side_effect: SideEffect::ReadOnly,
        provenance: Provenance::InProcess,
        timeout: None,
        max_output_bytes: None,
    }
}

pub fn tool_choice(request: &mut ModelRequest, choice: ToolChoice) {
    request.tool_choice = choice;
}

pub async fn drain(
    provider: &AnthropicProvider,
    request: ModelRequest,
) -> Vec<Result<StreamEvent, ProviderError>> {
    provider.stream(request).collect().await
}
