//! A9 (protocol.md §13): a request using a capability the model lacks fails
//! with `Unsupported` before any HTTP, on both adapters. Covers server tools,
//! structured output and file input; unrecognized model ids have none of them
//! (protocol.md §5: unknown means false).

use asterpath::anthropic::AnthropicProvider;
use asterpath::openai::OpenAiProvider;
use asterpath_protocol::{
    FileSource, Item, JsonString, ModelId, ModelRequest, ModelSpec, OutputFormat, Part, Provider,
    ProviderErrorKind, ProviderId, RequestConfig, Role,
};
use asterpath_testing::http_mock::MockServer;
use futures_util::StreamExt;
use serde_json::json;

fn requests(provider: &str) -> Vec<(&'static str, ModelRequest)> {
    let spec = ModelSpec {
        provider: ProviderId::new(provider),
        model: ModelId::new("deepseek-v4-pro"),
        context_window: None,
    };
    let user = |parts| {
        vec![Item::Message {
            role: Role::User,
            parts,
            raw: None,
        }]
    };
    let text = || {
        user(vec![Part::Text {
            text: "hi".to_owned(),
        }])
    };
    let build = |items, config: RequestConfig| {
        ModelRequest::new(
            spec.clone(),
            0,
            Vec::new(),
            items,
            Vec::new(),
            Vec::new(),
            &config,
        )
    };

    let mut server_tools = RequestConfig::default();
    server_tools.server_tools.insert(
        ProviderId::new(provider),
        vec![JsonString::new(r#"{"type":"web_search"}"#)],
    );
    let mut structured = RequestConfig::default();
    structured.output_format = Some(OutputFormat::new("answer", json!({"type": "object"})));
    let file = user(vec![Part::File {
        media_type: "application/pdf".to_owned(),
        filename: None,
        source: FileSource::Base64 {
            data: "JVBE".to_owned(),
        },
    }]);
    vec![
        ("server tools", build(text(), server_tools)),
        ("structured output", build(text(), structured)),
        ("file input", build(file, RequestConfig::default())),
    ]
}

async fn assert_unsupported_before_http(provider: &dyn Provider, server: &MockServer) {
    let id = provider.id();
    for (feature, request) in requests(id.as_str()) {
        let events: Vec<_> = provider.stream(request).collect().await;
        assert!(
            matches!(&events[..], [Err(error)]
                if error.kind == ProviderErrorKind::Unsupported && !error.retryable),
            "{id} {feature}: {events:?}"
        );
    }
    assert!(server.requests().is_empty(), "{id}: no HTTP");
}

#[tokio::test]
async fn a9_unsupported_capabilities_fail_before_http() {
    let server = MockServer::start(Vec::new());
    let anthropic = AnthropicProvider::new("test-key").with_base_url(server.base_url());
    assert_unsupported_before_http(&anthropic, &server).await;

    let openai = OpenAiProvider::new("test-key").with_base_url(format!("{}/v1", server.base_url()));
    assert_unsupported_before_http(&openai, &server).await;
}
