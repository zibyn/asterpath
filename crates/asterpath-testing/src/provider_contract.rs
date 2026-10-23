//! Provider adapter contract suite: SSE framing, truncation, and generic HTTP
//! failure classification over a loopback server.
//!
//! The suite owns the scenarios; each adapter supplies a [`ProviderWire`] that
//! renders the canonical response in its own wire format. Provider-specific
//! error bodies and usage normalization stay in each adapter's own tests.

use std::time::Duration;

use asterpath_protocol::{
    Item, ModelId, ModelRequest, ModelSpec, Part, Provider, ProviderError, ProviderErrorKind,
    RequestConfig, Role, StreamEvent,
};
use futures_util::StreamExt;
use serde_json::Value;

use crate::http_mock::{MockServer, Reply};

pub trait ProviderWire {
    /// The adapter under test, pointed at the mock server root (`http://host:port`).
    fn provider(&self, base_url: &str) -> Box<dyn Provider>;

    /// The SSE `data` payloads of one successful response whose only output is
    /// an assistant message with `text`. Each payload's `type` is also its
    /// `event:` name, and the last payload is the terminal event.
    fn text_response(&self, text: &str) -> Vec<Value>;
}

/// Run the shared provider contract against one adapter.
pub async fn provider_contract(wire: &impl ProviderWire) {
    framing_variants_decode_identically(wire).await;
    multibyte_text_split_across_tcp_chunks_is_byte_exact(wire).await;
    stream_without_terminal_event_is_transport(wire).await;
    generic_status_classes(wire).await;
    retry_after_and_request_id_are_extracted(wire).await;
}

fn render(events: &[Value], newline: &str, multiline_data: bool) -> String {
    let mut body = String::new();
    for data in events {
        let kind = data["type"].as_str().expect("event payload has a type");
        let data = data.to_string();
        // `{` alone on the first `data:` line still joins to valid JSON.
        let data = if multiline_data {
            data.replacen('{', &format!("{{{newline}data: "), 1)
        } else {
            data
        };
        body.push_str(&format!(
            ": keep-alive{newline}event: {kind}{newline}data: {data}{newline}{newline}"
        ));
    }
    body
}

fn request(wire_provider: &dyn Provider) -> ModelRequest {
    ModelRequest::new(
        ModelSpec {
            provider: wire_provider.id(),
            model: ModelId::new("contract-model"),
            context_window: None,
        },
        0,
        Vec::new(),
        vec![Item::Message {
            role: Role::User,
            parts: vec![Part::Text {
                text: "hi".to_owned(),
            }],
        }],
        Vec::new(),
        &RequestConfig::default(),
    )
}

async fn run(wire: &impl ProviderWire, reply: Reply) -> Vec<Result<StreamEvent, ProviderError>> {
    let server = MockServer::start(vec![reply]);
    let provider = wire.provider(&server.base_url());
    provider.stream(request(provider.as_ref())).collect().await
}

/// The assistant text of a successful stream; panics on any failure.
fn completed_text(events: &[Result<StreamEvent, ProviderError>]) -> String {
    let mut text = String::new();
    let mut ended = 0;
    for event in events {
        match event.as_ref().expect("stream succeeds") {
            StreamEvent::ItemCompleted {
                item: Item::Message { parts, .. },
                ..
            } => {
                for part in parts {
                    if let Part::Text { text: value } = part {
                        text.push_str(value);
                    }
                }
            }
            StreamEvent::ResponseEnded { .. } => ended += 1,
            _ => {}
        }
    }
    assert_eq!(ended, 1, "exactly one ResponseEnded: {events:?}");
    text
}

fn only_error(events: &[Result<StreamEvent, ProviderError>]) -> &ProviderError {
    match events {
        [Err(error)] => error,
        other => panic!("expected a single error, got {other:?}"),
    }
}

/// LF / CRLF line endings, comment keep-alives and multi-line `data`.
async fn framing_variants_decode_identically(wire: &impl ProviderWire) {
    let events = wire.text_response("Hello");
    for (newline, multiline) in [("\n", false), ("\r\n", false), ("\n", true), ("\r\n", true)] {
        let body = render(&events, newline, multiline);
        let text = completed_text(&run(wire, Reply::sse(body)).await);
        assert_eq!(
            text, "Hello",
            "newline {newline:?}, multi-line data {multiline}"
        );
    }
}

async fn multibyte_text_split_across_tcp_chunks_is_byte_exact(wire: &impl ProviderWire) {
    let text = "你好🙂世界";
    let body = render(&wire.text_response(text), "\n", false);
    let cjk = body.find('好').expect("cjk in body");
    let emoji = body.find('🙂').expect("emoji in body");
    let reply = Reply::sse(body).split_at(&[cjk + 1, emoji + 2]);
    assert_eq!(completed_text(&run(wire, reply).await), text);
}

async fn stream_without_terminal_event_is_transport(wire: &impl ProviderWire) {
    let mut events = wire.text_response("cut");
    events.pop();
    let outcome = run(wire, Reply::sse(render(&events, "\n", false))).await;
    let last = outcome.last().expect("at least one event");
    let error = last.as_ref().expect_err("truncated stream fails");
    assert_eq!(error.kind, ProviderErrorKind::Transport, "{outcome:?}");
    assert!(error.retryable);
    assert!(
        !outcome
            .iter()
            .any(|event| matches!(event, Ok(StreamEvent::ResponseEnded { .. }))),
        "{outcome:?}"
    );
}

const ERROR_BODY: &str = r#"{"error":{"type":"contract_error","message":"boom"}}"#;

async fn generic_status_classes(wire: &impl ProviderWire) {
    for (status, kind) in [
        (401, ProviderErrorKind::Auth),
        (403, ProviderErrorKind::Auth),
        (429, ProviderErrorKind::RateLimited),
        (500, ProviderErrorKind::Server),
        (503, ProviderErrorKind::Overloaded),
    ] {
        let events = run(wire, Reply::json(status, ERROR_BODY)).await;
        assert_eq!(only_error(&events).kind, kind, "HTTP {status}");
    }
}

async fn retry_after_and_request_id_are_extracted(wire: &impl ProviderWire) {
    let reply = Reply::json(429, ERROR_BODY)
        .header("retry-after", "3")
        .header("request-id", "req_contract");
    let events = run(wire, reply).await;
    let error = only_error(&events);
    assert_eq!(error.retry_after, Some(Duration::from_secs(3)));
    assert_eq!(error.request_id.as_deref(), Some("req_contract"));
}
