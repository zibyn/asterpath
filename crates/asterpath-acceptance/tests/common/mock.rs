//! Loopback HTTP servers that replay each provider's wire protocol.
//!
//! Both builders consume the same [`Fixture`] (the single source for scenario
//! (b)) and render its canonical items into Anthropic Messages / OpenAI
//! Responses SSE. The server is a one-reply-per-connection FIFO: the scenario
//! makes exactly one model request per scripted step.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;

use asterpath_protocol::{Item, Part, StopReason};
use asterpath_testing::Fixture;
use serde_json::{Value, json};

pub struct MockServer {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<String>>>,
}

impl MockServer {
    pub fn start(replies: Vec<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let addr = listener.local_addr().expect("mock address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        thread::spawn(move || {
            for reply in replies {
                let Ok((mut socket, _)) = listener.accept() else {
                    return;
                };
                let raw = read_request(&mut socket);
                captured
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(String::from_utf8_lossy(&raw).into_owned());
                write_response(&mut socket, &reply);
            }
        });
        Self { addr, requests }
    }

    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Requests in arrival order, request line plus body.
    pub fn requests(&self) -> Vec<String> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Request-targets in arrival order (the first token of the request line).
    pub fn paths(&self) -> Vec<String> {
        self.requests()
            .iter()
            .filter_map(|raw| raw.lines().next())
            .filter_map(|line| line.split_whitespace().nth(1))
            .map(str::to_owned)
            .collect()
    }

    pub fn bodies(&self) -> Vec<Value> {
        self.requests()
            .iter()
            .map(|raw| {
                raw.split_once("\r\n\r\n")
                    .map(|(_, body)| body)
                    .unwrap_or("")
            })
            .map(|body| serde_json::from_str(body).unwrap_or(Value::Null))
            .collect()
    }
}

fn read_request(socket: &mut TcpStream) -> Vec<u8> {
    let mut data = Vec::new();
    let mut buf = [0u8; 8192];
    let mut body_start = None;
    while let Ok(read) = socket.read(&mut buf) {
        if read == 0 {
            break;
        }
        data.extend_from_slice(&buf[..read]);
        if body_start.is_none() {
            body_start = data
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|position| position + 4);
        }
        if let Some(start) = body_start {
            let head = String::from_utf8_lossy(&data[..start]).to_lowercase();
            let length = head
                .lines()
                .find_map(|line| line.strip_prefix("content-length:"))
                .and_then(|value| value.trim().parse::<usize>().ok())
                .unwrap_or(0);
            if data.len() >= start + length {
                break;
            }
        }
    }
    data
}

fn write_response(socket: &mut TcpStream, body: &str) {
    let head = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    let _ = socket.write_all(head.as_bytes());
    let _ = socket.write_all(body.as_bytes());
    let _ = socket.flush();
}

fn sse(body: &mut String, event: &str, data: Value) {
    body.push_str("event: ");
    body.push_str(event);
    body.push_str("\ndata: ");
    body.push_str(&data.to_string());
    body.push_str("\n\n");
}

fn text_of(parts: &[Part]) -> String {
    let mut text = String::new();
    for part in parts {
        if let Part::Text { text: value } = part {
            text.push_str(value);
        }
    }
    text
}

fn anthropic_stop(reason: &StopReason) -> &'static str {
    match reason {
        StopReason::EndTurn => "end_turn",
        StopReason::ToolUse => "tool_use",
        StopReason::MaxTokens => "max_tokens",
        StopReason::PauseTurn => "pause_turn",
        StopReason::ContextWindowExceeded => "model_context_window_exceeded",
        StopReason::Refusal => "refusal",
        _ => "end_turn",
    }
}

/// Anthropic Messages SSE for every scripted step.
pub fn anthropic_replies(fixture: &Fixture) -> Vec<String> {
    fixture
        .steps
        .iter()
        .enumerate()
        .map(|(step, script)| {
            let mut body = String::new();
            sse(
                &mut body,
                "message_start",
                json!({"type": "message_start", "message": {
                    "id": format!("msg_{step}"),
                    "usage": {
                        "input_tokens": script.usage.input.uncached,
                        "cache_read_input_tokens": script.usage.input.cache_read,
                        "cache_creation_input_tokens": script.usage.input.cache_write,
                        "output_tokens": 1,
                    },
                }}),
            );
            for (index, item) in script.items.iter().enumerate() {
                match item {
                    Item::Reasoning { summary, .. } => {
                        sse(&mut body, "content_block_start", json!({
                            "type": "content_block_start",
                            "index": index,
                            "content_block": {"type": "thinking", "thinking": ""},
                        }));
                        sse(&mut body, "content_block_delta", json!({
                            "type": "content_block_delta",
                            "index": index,
                            "delta": {"type": "thinking_delta", "thinking": text_of(summary)},
                        }));
                        sse(&mut body, "content_block_delta", json!({
                            "type": "content_block_delta",
                            "index": index,
                            "delta": {"type": "signature_delta", "signature": "sig-anthropic"},
                        }));
                        sse(
                            &mut body,
                            "content_block_stop",
                            json!({"type": "content_block_stop", "index": index}),
                        );
                    }
                    Item::ToolCall {
                        call_id,
                        name,
                        raw_args,
                        ..
                    } => {
                        sse(&mut body, "content_block_start", json!({
                            "type": "content_block_start",
                            "index": index,
                            "content_block": {
                                "type": "tool_use",
                                "id": call_id,
                                "name": name.as_str(),
                                "input": {},
                            },
                        }));
                        sse(&mut body, "content_block_delta", json!({
                            "type": "content_block_delta",
                            "index": index,
                            "delta": {"type": "input_json_delta", "partial_json": raw_args.as_str()},
                        }));
                        sse(
                            &mut body,
                            "content_block_stop",
                            json!({"type": "content_block_stop", "index": index}),
                        );
                    }
                    Item::Message { parts, .. } => {
                        sse(&mut body, "content_block_start", json!({
                            "type": "content_block_start",
                            "index": index,
                            "content_block": {"type": "text", "text": ""},
                        }));
                        sse(&mut body, "content_block_delta", json!({
                            "type": "content_block_delta",
                            "index": index,
                            "delta": {"type": "text_delta", "text": text_of(parts)},
                        }));
                        sse(
                            &mut body,
                            "content_block_stop",
                            json!({"type": "content_block_stop", "index": index}),
                        );
                    }
                    _ => {}
                }
            }
            sse(
                &mut body,
                "message_delta",
                json!({
                    "type": "message_delta",
                    "delta": {"stop_reason": anthropic_stop(&script.stop_reason)},
                    "usage": {
                        "output_tokens": script.usage.output.total,
                        "output_tokens_details": {"thinking_tokens": script.usage.output.reasoning},
                    },
                }),
            );
            sse(&mut body, "message_stop", json!({"type": "message_stop"}));
            body
        })
        .collect()
}

/// OpenAI Responses SSE for every scripted step.
pub fn openai_replies(fixture: &Fixture) -> Vec<String> {
    fixture
        .steps
        .iter()
        .enumerate()
        .map(|(step, script)| {
            let mut body = String::new();
            let mut sequence = 0u64;
            sse(&mut body, "response.created", json!({
                "type": "response.created",
                "sequence_number": sequence,
                "response": {"id": format!("resp_{step}"), "status": "in_progress"},
            }));
            sequence += 1;

            let mut output = Vec::new();
            for (index, item) in script.items.iter().enumerate() {
                let (added, delta_kind, delta, done) = match item {
                    Item::Reasoning { summary, .. } => (
                        json!({"type": "reasoning", "id": format!("rs_{step}_{index}"), "summary": []}),
                        "response.reasoning_summary_text.delta",
                        text_of(summary),
                        json!({
                            "type": "reasoning",
                            "id": format!("rs_{step}_{index}"),
                            "summary": [{"type": "summary_text", "text": text_of(summary)}],
                            "encrypted_content": format!("enc-{step}-{index}"),
                        }),
                    ),
                    Item::ToolCall {
                        call_id,
                        name,
                        raw_args,
                        ..
                    } => (
                        json!({
                            "type": "function_call",
                            "id": format!("fc_{step}_{index}"),
                            "call_id": call_id,
                            "name": name.as_str(),
                            "arguments": "",
                        }),
                        "response.function_call_arguments.delta",
                        raw_args.as_str().to_owned(),
                        json!({
                            "type": "function_call",
                            "id": format!("fc_{step}_{index}"),
                            "call_id": call_id,
                            "name": name.as_str(),
                            "arguments": raw_args.as_str(),
                        }),
                    ),
                    Item::Message { parts, .. } => (
                        json!({
                            "type": "message",
                            "id": format!("msg_{step}_{index}"),
                            "role": "assistant",
                            "content": [],
                        }),
                        "response.output_text.delta",
                        text_of(parts),
                        json!({
                            "type": "message",
                            "id": format!("msg_{step}_{index}"),
                            "role": "assistant",
                            "status": "completed",
                            "content": [{"type": "output_text", "text": text_of(parts)}],
                        }),
                    ),
                    _ => continue,
                };
                sse(&mut body, "response.output_item.added", json!({
                    "type": "response.output_item.added",
                    "sequence_number": sequence,
                    "output_index": index,
                    "item": added,
                }));
                sequence += 1;
                sse(&mut body, delta_kind, json!({
                    "type": delta_kind,
                    "sequence_number": sequence,
                    "output_index": index,
                    "content_index": 0,
                    "summary_index": 0,
                    "delta": delta,
                }));
                sequence += 1;
                sse(&mut body, "response.output_item.done", json!({
                    "type": "response.output_item.done",
                    "sequence_number": sequence,
                    "output_index": index,
                    "item": done,
                }));
                sequence += 1;
                output.push(done);
            }

            let input = script.usage.input;
            sse(&mut body, "response.completed", json!({
                "type": "response.completed",
                "sequence_number": sequence,
                "response": {
                    "id": format!("resp_{step}"),
                    "status": "completed",
                    "output": output,
                    "usage": {
                        "input_tokens": input.uncached + input.cache_read + input.cache_write,
                        "input_tokens_details": {
                            "cached_tokens": input.cache_read,
                            "cache_write_tokens": input.cache_write,
                        },
                        "output_tokens": script.usage.output.total,
                        "output_tokens_details": {"reasoning_tokens": script.usage.output.reasoning},
                    },
                },
            }));
            body
        })
        .collect()
}
