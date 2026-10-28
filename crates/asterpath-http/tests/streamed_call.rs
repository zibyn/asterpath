//! Streamed call (GLOSSARY): the HTTP + SSE lifecycle, driven with a fake
//! normalizer over the loopback mock server.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use asterpath_http::{ErrorEnvelope, SseEvent, SseNormalizer, streamed_call};
use asterpath_protocol::{
    ModelId, OutputTokens, ProviderError, ProviderErrorKind, ProviderId, StopReason, StreamEvent,
    Usage,
};
use asterpath_testing::http_mock::{MockServer, Reply};
use futures_util::StreamExt;
use serde_json::Value;

/// `start` (with `id`), `delta`, `end`: one output token per delta.
#[derive(Default)]
struct Fake {
    request_id: Option<String>,
    deltas: u64,
    ended: bool,
}

impl SseNormalizer for Fake {
    fn connected(&mut self, _header_request_id: Option<String>) {}

    fn event(&mut self, event: SseEvent) -> Vec<Result<StreamEvent, ProviderError>> {
        let value: Value = serde_json::from_str(&event.data).expect("fake event is JSON");
        match value["type"].as_str() {
            Some("start") => {
                self.request_id = value["id"].as_str().map(str::to_owned);
                vec![Ok(StreamEvent::ResponseStarted {
                    sequence: 0,
                    request_id: self.request_id.clone(),
                })]
            }
            Some("delta") => {
                self.deltas += 1;
                vec![Ok(StreamEvent::ItemStarted {
                    sequence: self.deltas,
                    output_index: 0,
                })]
            }
            Some("end") => {
                self.ended = true;
                vec![Ok(StreamEvent::ResponseEnded {
                    sequence: 99,
                    stop_reason: StopReason::EndTurn,
                    usage: self.usage(),
                })]
            }
            Some("fail") => vec![Err(ProviderError::new(ProviderErrorKind::Server, "fail"))],
            _ => Vec::new(),
        }
    }

    fn terminal(&self) -> bool {
        self.ended
    }

    fn partial_usage(&self) -> Option<Usage> {
        Some(self.usage())
    }

    fn request_id(&self) -> Option<String> {
        self.request_id.clone()
    }
}

impl Fake {
    fn usage(&self) -> Usage {
        Usage {
            output: OutputTokens {
                total: self.deltas,
                reasoning: 0,
            },
            ..Usage::default()
        }
    }
}

fn envelope() -> ErrorEnvelope {
    ErrorEnvelope {
        provider: ProviderId::new("fake"),
        model: ModelId::new("fake-model"),
        raw_kind: "fake.error",
        // Proves the call takes the adapter's answer, not its own status table.
        classify: |status, body, message| match (status, body, message) {
            (429, Some(_), "boom") => (ProviderErrorKind::RateLimited, false),
            (status, _, _) => {
                let kind = asterpath_http::classify_status(status);
                (kind, kind.retryable_by_default())
            }
        },
    }
}

fn sse(events: &[&str]) -> String {
    events
        .iter()
        .map(|data| format!("data: {data}\n\n"))
        .collect()
}

const START: &str = r#"{"type":"start","id":"msg_1"}"#;
const DELTA: &str = r#"{"type":"delta"}"#;
const END: &str = r#"{"type":"end"}"#;

async fn call_url(url: String, client: reqwest::Client) -> Vec<Result<StreamEvent, ProviderError>> {
    streamed_call(client.post(url).body("{}"), Fake::default(), envelope())
        .collect()
        .await
}

async fn call(reply: Reply) -> Vec<Result<StreamEvent, ProviderError>> {
    let server = MockServer::start(vec![reply]);
    call_url(server.base_url(), reqwest::Client::new()).await
}

fn last_error(outcome: &[Result<StreamEvent, ProviderError>]) -> &ProviderError {
    outcome
        .last()
        .expect("at least one event")
        .as_ref()
        .expect_err("call fails")
}

#[tokio::test]
async fn complete_stream_yields_normalized_events() {
    let outcome = call(Reply::sse(sse(&[START, DELTA, END]))).await;
    assert_eq!(outcome.len(), 3, "{outcome:?}");
    assert!(outcome.iter().all(Result::is_ok), "{outcome:?}");
    assert!(matches!(
        outcome.last(),
        Some(Ok(StreamEvent::ResponseEnded { .. }))
    ));
}

#[tokio::test]
async fn stream_without_terminal_event_is_truncation() {
    let outcome = call(Reply::sse(sse(&[START, DELTA]))).await;
    assert_eq!(outcome.len(), 3, "{outcome:?}");
    let error = last_error(&outcome);
    assert_eq!(error.kind, ProviderErrorKind::Transport);
    assert!(error.retryable);
    assert_eq!(error.request_id.as_deref(), Some("msg_1"));
    assert_eq!(error.partial_usage.map(|usage| usage.output.total), Some(1));
}

#[tokio::test]
async fn last_frame_without_trailing_blank_line_still_counts() {
    let body = sse(&[START, END]);
    let body = body.strip_suffix("\n\n").expect("framed body");
    let outcome = call(Reply::sse(body)).await;
    assert!(outcome.iter().all(Result::is_ok), "{outcome:?}");
    assert!(matches!(
        outcome.last(),
        Some(Ok(StreamEvent::ResponseEnded { .. }))
    ));

    let body = sse(&[START, DELTA]);
    let body = body.strip_suffix('\n').expect("framed body");
    let outcome = call(Reply::sse(body)).await;
    assert_eq!(last_error(&outcome).kind, ProviderErrorKind::Transport);
}

/// Serves `body` under a larger `content-length`, then holds the connection open.
fn stalling_server(body: String, headers: &str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("address"));
    let head = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\n{headers}\r\n",
        body.len() + 1000
    );
    thread::spawn(move || {
        let (mut socket, _) = listener.accept().expect("accept");
        let _ = socket.read(&mut [0u8; 8192]);
        let _ = socket.write_all(head.as_bytes());
        let _ = socket.write_all(body.as_bytes());
        let _ = socket.flush();
        thread::sleep(Duration::from_secs(5));
    });
    url
}

fn client_with_timeout(millis: u64) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_millis(millis))
        .build()
        .expect("client")
}

#[tokio::test]
async fn read_failure_mid_stream_is_timeout_with_request_id_and_usage() {
    let url = stalling_server(sse(&[START, DELTA]), "request-id: req_header\r\n");
    let outcome = call_url(url, client_with_timeout(300)).await;
    let error = last_error(&outcome);
    assert_eq!(error.kind, ProviderErrorKind::Timeout, "{outcome:?}");
    assert_eq!(error.request_id.as_deref(), Some("msg_1"));
    assert_eq!(error.partial_usage.map(|usage| usage.output.total), Some(1));

    let url = stalling_server(String::new(), "request-id: req_header\r\n");
    let outcome = call_url(url, client_with_timeout(300)).await;
    let error = last_error(&outcome);
    assert_eq!(error.kind, ProviderErrorKind::Timeout, "{outcome:?}");
    assert_eq!(error.request_id.as_deref(), Some("req_header"));
}

#[tokio::test]
async fn reading_stops_at_the_terminal_event() {
    // Events after the terminal one are never normalized, and the open
    // connection is not waited on.
    let url = stalling_server(sse(&[START, END, DELTA, r#"{"type":"fail"}"#]), "");
    let outcome = call_url(url, client_with_timeout(3000)).await;
    assert_eq!(outcome.len(), 2, "{outcome:?}");
    assert!(matches!(
        outcome.last(),
        Some(Ok(StreamEvent::ResponseEnded { .. }))
    ));
}

#[tokio::test]
async fn normalizer_error_ends_the_call() {
    let outcome = call(Reply::sse(sse(&[START, r#"{"type":"fail"}"#, END]))).await;
    assert_eq!(outcome.len(), 2, "{outcome:?}");
    assert_eq!(last_error(&outcome).message, "fail");
}

#[tokio::test]
async fn non_success_status_builds_the_error_envelope() {
    let body = r#"{"error":{"type":"x","message":"boom"}}"#;
    let reply = Reply::json(429, body)
        .header("retry-after", "4")
        .header("x-request-id", "req_err");
    let outcome = call(reply).await;
    assert_eq!(outcome.len(), 1, "{outcome:?}");
    let error = last_error(&outcome);
    assert_eq!(error.kind, ProviderErrorKind::RateLimited);
    assert!(!error.retryable, "the adapter's classification wins");
    assert_eq!(error.message, "boom");
    assert_eq!(error.retry_after, Some(Duration::from_secs(4)));
    assert_eq!(error.request_id.as_deref(), Some("req_err"));
    let raw = error.raw.as_ref().expect("raw body");
    assert_eq!(raw.provider, ProviderId::new("fake"));
    assert_eq!(raw.model, Some(ModelId::new("fake-model")));
    assert_eq!(raw.kind, "fake.error");
    assert_eq!(raw.json.as_str(), body);
}

#[tokio::test]
async fn error_message_falls_back_to_body_then_status() {
    let outcome = call(Reply::new(502, "text/plain", "bad gateway")).await;
    let error = last_error(&outcome);
    assert_eq!(error.kind, ProviderErrorKind::Server);
    assert_eq!(error.message, "bad gateway");

    let outcome = call(Reply::json(500, r#"{"error":{"message":""}}"#)).await;
    assert_eq!(last_error(&outcome).message, r#"{"error":{"message":""}}"#);

    let outcome = call(Reply::json(500, "")).await;
    let error = last_error(&outcome);
    assert!(error.message.contains("500"), "{error:?}");
    assert!(error.raw.is_none());
}

#[tokio::test]
async fn connection_failure_is_transport() {
    let outcome = call(Reply::close()).await;
    assert_eq!(outcome.len(), 1, "{outcome:?}");
    assert_eq!(last_error(&outcome).kind, ProviderErrorKind::Transport);
}
