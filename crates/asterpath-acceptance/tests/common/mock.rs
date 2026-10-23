//! Each provider's wire rendering of a scenario fixture, served by the shared
//! loopback mock server.
//!
//! Both builders consume the same [`Fixture`] (the single source for scenario
//! (b)) and render its canonical items into Anthropic Messages / OpenAI
//! Responses SSE, one reply per scripted step.

use asterpath_protocol::{Item, Part, StopReason};
use asterpath_testing::Fixture;
use asterpath_testing::http_mock::Reply;
use serde_json::{Value, json};

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
pub fn anthropic_replies(fixture: &Fixture) -> Vec<Reply> {
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
            Reply::sse(body)
        })
        .collect()
}

/// OpenAI Responses SSE for every scripted step.
pub fn openai_replies(fixture: &Fixture) -> Vec<Reply> {
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
            Reply::sse(body)
        })
        .collect()
}
