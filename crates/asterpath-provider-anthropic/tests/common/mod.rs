//! Minimal loopback HTTP/1.1 server for replaying Anthropic wire responses.
//!
//! One reply is consumed per connection (responses send `Connection: close`), so
//! requests and replies stay in FIFO order without any matcher DSL.

#![allow(dead_code)]

use asterpath_protocol::{
    ModelId, ModelRequest, ModelSpec, Parallelism, Part, Provenance, Provider, ProviderError,
    ProviderId, ReplaySafety, RequestConfig, SideEffect, StreamEvent, ToolChoice, ToolDescriptor,
    ToolName,
};
use asterpath_provider_anthropic::AnthropicProvider;
use futures_util::StreamExt;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

#[derive(Clone, Copy)]
pub enum Mode {
    Complete,
    /// Accept the connection, then close it without writing a response.
    Close,
    /// Write `Content-Length` for the full body, send `n` bytes, then close.
    Truncate(usize),
}

#[derive(Clone)]
pub struct Reply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
    pub mode: Mode,
}

impl Reply {
    pub fn json(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            headers: vec![("content-type".to_owned(), "application/json".to_owned())],
            body: body.into(),
            mode: Mode::Complete,
        }
    }

    pub fn sse(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            headers: vec![("content-type".to_owned(), "text/event-stream".to_owned())],
            body: body.into(),
            mode: Mode::Complete,
        }
    }

    pub fn truncated_sse(body: impl Into<String>, cut: usize) -> Self {
        Self {
            status: 200,
            headers: vec![("content-type".to_owned(), "text/event-stream".to_owned())],
            body: body.into(),
            mode: Mode::Truncate(cut),
        }
    }

    pub fn close() -> Self {
        Self {
            status: 0,
            headers: Vec::new(),
            body: String::new(),
            mode: Mode::Close,
        }
    }

    #[must_use]
    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }
}

#[derive(Clone, Debug)]
pub struct Recorded {
    pub head: String,
    pub body: String,
}

impl Recorded {
    pub fn json_body(&self) -> serde_json::Value {
        serde_json::from_str(&self.body).expect("recorded body is JSON")
    }
}

pub struct MockServer {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<Recorded>>>,
    _join: JoinHandle<()>,
}

impl MockServer {
    pub async fn start(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let queue = Arc::new(Mutex::new(replies));
        let recorded = Arc::clone(&requests);
        let join = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                let reply = {
                    let mut queue = queue.lock().expect("reply queue");
                    if queue.is_empty() {
                        Reply::json(
                            500,
                            r#"{"error":{"type":"api_error","message":"no reply scripted"}}"#,
                        )
                    } else {
                        queue.remove(0)
                    }
                };
                let recorded = Arc::clone(&recorded);
                tokio::spawn(async move {
                    serve(socket, reply, recorded).await;
                });
            }
        });
        Self {
            addr,
            requests,
            _join: join,
        }
    }

    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().expect("requests").clone()
    }
}

async fn serve(mut socket: TcpStream, reply: Reply, requests: Arc<Mutex<Vec<Recorded>>>) {
    let mut data = Vec::new();
    let mut chunk = [0u8; 8192];
    let completed = loop {
        let Ok(n) = socket.read(&mut chunk).await else {
            return;
        };
        if n == 0 {
            break false;
        }
        data.extend_from_slice(&chunk[..n]);
        if let Some(head_end) = find(&data, b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&data[..head_end]).to_string();
            let length = content_length(&head).unwrap_or(0);
            if data.len() >= head_end + 4 + length {
                requests.lock().expect("requests").push(Recorded {
                    head,
                    body: String::from_utf8_lossy(&data[head_end + 4..]).to_string(),
                });
                break true;
            }
        }
    };

    match reply.mode {
        Mode::Close => {
            let _ = socket.shutdown().await;
        }
        Mode::Complete => {
            if completed {
                write_response(&mut socket, &reply, reply.body.as_bytes()).await;
            }
        }
        Mode::Truncate(cut) => {
            if completed {
                write_head(&mut socket, &reply, reply.body.len()).await;
                let cut = cut.min(reply.body.len());
                let _ = socket.write_all(&reply.body.as_bytes()[..cut]).await;
                let _ = socket.flush().await;
            }
            let _ = socket.shutdown().await;
        }
    }
}

async fn write_response(socket: &mut TcpStream, reply: &Reply, body: &[u8]) {
    write_head(socket, reply, body.len()).await;
    let _ = socket.write_all(body).await;
    let _ = socket.shutdown().await;
}

async fn write_head(socket: &mut TcpStream, reply: &Reply, body_len: usize) {
    let mut head = format!("HTTP/1.1 {} X\r\n", reply.status);
    head.push_str(&format!("content-length: {body_len}\r\n"));
    head.push_str("connection: close\r\n");
    for (name, value) in &reply.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    let _ = socket.write_all(head.as_bytes()).await;
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn content_length(head: &str) -> Option<usize> {
    head.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if name.eq_ignore_ascii_case("content-length") {
            value.trim().parse().ok()
        } else {
            None
        }
    })
}

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
