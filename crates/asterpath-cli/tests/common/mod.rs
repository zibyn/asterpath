//! A tiny blocking loopback HTTP server for the Anthropic adapter.
//!
//! One canned SSE body is served per connection (or the next body in a
//! scripted sequence). Requests are recorded so a test can assert which model
//! the CLI actually sent.

#![allow(dead_code)]

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::thread;

use assert_cmd::Command;

/// The CLI binary with host environment removed and a dummy key in place.
pub fn bin() -> Command {
    let mut command = Command::cargo_bin("asterpath").expect("asterpath binary");
    for name in [
        "ASTERPATH_CONFIG",
        "ASTERPATH_DB",
        "ASTERPATH_PROVIDER",
        "ASTERPATH_MODEL",
        "ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
    ] {
        command.env_remove(name);
    }
    command.env("ANTHROPIC_API_KEY", "test-key");
    command
}

pub struct MockServer {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<String>>>,
}

impl MockServer {
    pub fn start(body: impl Into<String>) -> Self {
        Self::start_sequence(vec![body.into()])
    }

    /// Serve each body to the next model request; after the script runs out,
    /// repeat the last one.
    pub fn start_sequence(bodies: Vec<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let addr = listener.local_addr().expect("local addr");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        let bodies = Arc::new(Mutex::new(VecDeque::from(bodies)));
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                let body = {
                    let mut bodies = bodies.lock().expect("bodies");
                    bodies
                        .pop_front()
                        .or_else(|| bodies.back().cloned())
                        .unwrap_or_default()
                };
                let recorded = Arc::clone(&recorded);
                thread::spawn(move || serve(&mut stream, &body, &recorded));
            }
        });
        Self { addr, requests }
    }

    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn requests(&self) -> Vec<String> {
        self.requests.lock().expect("requests").clone()
    }
}

fn serve(stream: &mut std::net::TcpStream, body: &str, recorded: &Mutex<Vec<String>>) {
    let mut data = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let Ok(read) = stream.read(&mut chunk) else {
            return;
        };
        if read == 0 {
            return;
        }
        data.extend_from_slice(&chunk[..read]);
        let Some(head_end) = find(&data, b"\r\n\r\n") else {
            continue;
        };
        let head = String::from_utf8_lossy(&data[..head_end]).to_string();
        let length = content_length(&head).unwrap_or(0);
        if data.len() >= head_end + 4 + length {
            recorded
                .lock()
                .expect("requests")
                .push(String::from_utf8_lossy(&data).to_string());
            break;
        }
    }
    let head = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body.as_bytes());
    let _ = stream.flush();
    let _ = stream.shutdown(std::net::Shutdown::Both);
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

/// A complete Anthropic text response ending the turn.
pub fn text_sse(text: &str) -> String {
    sse("end_turn", text)
}

/// A response the loop classifies as a non-completed turn.
pub fn context_exceeded_sse(text: &str) -> String {
    sse("model_context_window_exceeded", text)
}

/// A complete Anthropic tool-use response. `args` is raw JSON text.
pub fn tool_use_sse(name: &str, args: &str) -> String {
    let args = serde_json::to_string(args).expect("json string");
    format!(
        concat!(
            "event: message_start\ndata: {{\"type\":\"message_start\",\"message\":{{\"id\":\"msg_1\",",
            "\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"claude-test\",",
            "\"stop_reason\":null,\"usage\":{{\"input_tokens\":5,\"output_tokens\":1}}}}}}\n\n",
            "event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0,",
            "\"content_block\":{{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"{name}\",\"input\":{{}}}}}}\n\n",
            "event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,",
            "\"delta\":{{\"type\":\"input_json_delta\",\"partial_json\":{args}}}}}\n\n",
            "event: content_block_stop\ndata: {{\"type\":\"content_block_stop\",\"index\":0}}\n\n",
            "event: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"tool_use\",",
            "\"stop_sequence\":null}},\"usage\":{{\"output_tokens\":2}}}}\n\n",
            "event: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n",
        ),
        name = name,
        args = args,
    )
}

fn sse(stop_reason: &str, text: &str) -> String {
    format!(
        concat!(
            "event: message_start\ndata: {{\"type\":\"message_start\",\"message\":{{\"id\":\"msg_1\",",
            "\"type\":\"message\",\"role\":\"assistant\",\"content\":[],\"model\":\"claude-test\",",
            "\"stop_reason\":null,\"usage\":{{\"input_tokens\":5,\"output_tokens\":1}}}}}}\n\n",
            "event: content_block_start\ndata: {{\"type\":\"content_block_start\",\"index\":0,",
            "\"content_block\":{{\"type\":\"text\",\"text\":\"\"}}}}\n\n",
            "event: content_block_delta\ndata: {{\"type\":\"content_block_delta\",\"index\":0,",
            "\"delta\":{{\"type\":\"text_delta\",\"text\":\"{text}\"}}}}\n\n",
            "event: content_block_stop\ndata: {{\"type\":\"content_block_stop\",\"index\":0}}\n\n",
            "event: message_delta\ndata: {{\"type\":\"message_delta\",\"delta\":{{\"stop_reason\":\"{stop_reason}\",",
            "\"stop_sequence\":null}},\"usage\":{{\"output_tokens\":2}}}}\n\n",
            "event: message_stop\ndata: {{\"type\":\"message_stop\"}}\n\n",
        ),
        text = text,
        stop_reason = stop_reason,
    )
}
