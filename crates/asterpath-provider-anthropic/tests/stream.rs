//! SSE normalization, usage, error classification and stream invariants.

mod common;

use asterpath_protocol::{
    Item, Part, PartDelta, ProviderError, ProviderErrorKind, StopReason, StreamEvent, Usage,
};
use common::{MockServer, Reply, base_request, drain, empty_sse, model, provider, sse_body};
use std::time::Duration;

fn ok(events: Vec<Result<StreamEvent, ProviderError>>) -> Vec<StreamEvent> {
    events
        .into_iter()
        .map(|event| event.expect("stream event"))
        .collect()
}

fn text_raw(json: &str) -> asterpath_protocol::RawProviderData {
    asterpath_protocol::RawProviderData {
        provider: model().provider,
        model: Some(model().model),
        kind: "anthropic.text".to_owned(),
        json: asterpath_protocol::JsonString::new(json),
    }
}

fn first_error(events: Vec<Result<StreamEvent, ProviderError>>) -> ProviderError {
    events
        .into_iter()
        .find_map(|event| event.err())
        .expect("stream error")
}

fn happy_sse() -> String {
    sse_body(&[
        (
            "message_start",
            r#"{"type":"message_start","message":{"id":"msg_happy","type":"message","role":"assistant","content":[],"model":"claude-test","stop_reason":null,"usage":{"input_tokens":25,"cache_creation_input_tokens":3,"cache_read_input_tokens":7,"output_tokens":1}}}"#,
        ),
        (
            "content_block_start",
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}"#,
        ),
        (
            "content_block_delta",
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Let me"}}"#,
        ),
        (
            "content_block_delta",
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig-abc"}}"#,
        ),
        (
            "content_block_stop",
            r#"{"type":"content_block_stop","index":0}"#,
        ),
        (
            "content_block_start",
            r#"{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}"#,
        ),
        (
            "content_block_delta",
            r#"{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"Hello"}}"#,
        ),
        (
            "content_block_delta",
            r#"{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":", world"}}"#,
        ),
        (
            "content_block_stop",
            r#"{"type":"content_block_stop","index":1}"#,
        ),
        (
            "content_block_start",
            r#"{"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"toolu_1","name":"search","input":{}}}"#,
        ),
        (
            "content_block_delta",
            r#"{"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"{\"q\":"}}"#,
        ),
        (
            "content_block_delta",
            r#"{"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":"\"rust\"}"}}"#,
        ),
        (
            "content_block_stop",
            r#"{"type":"content_block_stop","index":2}"#,
        ),
        (
            "content_block_start",
            r#"{"type":"content_block_start","index":3,"content_block":{"type":"redacted_thinking","data":"opaque=="}}"#,
        ),
        (
            "content_block_stop",
            r#"{"type":"content_block_stop","index":3}"#,
        ),
        (
            "message_delta",
            r#"{"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":42,"output_tokens_details":{"thinking_tokens":5}}}"#,
        ),
        ("message_stop", r#"{"type":"message_stop"}"#),
    ])
}

#[tokio::test]
async fn normalizes_the_full_stream_into_s1_to_s10() {
    let server = MockServer::start(vec![Reply::sse(happy_sse())]);
    let events = ok(drain(&provider(&server), base_request()).await);

    assert_eq!(
        events.len(),
        1 + 4 * 2 + 5 + 1,
        "started, 4 blocks, 5 deltas, ended"
    );
    assert!(matches!(
        events.first(),
        Some(StreamEvent::ResponseStarted {
            sequence: 0,
            request_id: Some(id)
        }) if id == "msg_happy"
    ));
    match events.last() {
        Some(StreamEvent::ResponseEnded {
            stop_reason, usage, ..
        }) => {
            assert_eq!(*stop_reason, StopReason::ToolUse);
            assert_eq!(
                *usage,
                Usage {
                    input: asterpath_protocol::InputTokens {
                        uncached: 25,
                        cache_read: 7,
                        cache_write: 3,
                    },
                    output: asterpath_protocol::OutputTokens {
                        total: 42,
                        reasoning: 5,
                    },
                }
            );
        }
        other => panic!("expected ResponseEnded, got {other:?}"),
    }

    let sequences: Vec<u64> = events
        .iter()
        .map(|event| match event {
            StreamEvent::ResponseStarted { sequence, .. }
            | StreamEvent::ItemStarted { sequence, .. }
            | StreamEvent::PartDelta { sequence, .. }
            | StreamEvent::ItemCompleted { sequence, .. }
            | StreamEvent::ResponseEnded { sequence, .. } => *sequence,
            other => panic!("unknown stream event: {other:?}"),
        })
        .collect();
    assert!(
        sequences.windows(2).all(|window| window[1] > window[0]),
        "sequence strictly increases: {sequences:?}"
    );

    let mut started: Vec<u32> = Vec::new();
    let mut completed: Vec<u32> = Vec::new();
    for (position, event) in events.iter().enumerate() {
        match event {
            StreamEvent::ItemStarted { output_index, .. } => {
                assert!(!started.contains(output_index), "output_index is unique");
                assert!(!completed.contains(output_index));
                started.push(*output_index);
            }
            StreamEvent::PartDelta { output_index, .. } => {
                assert!(
                    started.contains(output_index) && !completed.contains(output_index),
                    "delta only between start and complete"
                );
            }
            StreamEvent::ItemCompleted { output_index, .. } => {
                assert!(started.contains(output_index));
                assert!(!completed.contains(output_index), "completed exactly once");
                completed.push(*output_index);
            }
            _ => {}
        }
        assert!(
            !matches!(event, StreamEvent::ResponseStarted { .. }) || position == 0,
            "ResponseStarted exactly once, first"
        );
        assert!(
            !matches!(event, StreamEvent::ResponseEnded { .. }) || position == events.len() - 1,
            "ResponseEnded exactly once, last"
        );
    }
    assert_eq!(completed, vec![0, 1, 2, 3]);

    let items: Vec<&Item> = events
        .iter()
        .filter_map(|event| match event {
            StreamEvent::ItemCompleted { item, .. } => Some(item),
            _ => None,
        })
        .collect();
    match items[0] {
        Item::Reasoning {
            summary,
            binding,
            raw,
        } => {
            assert_eq!(
                summary,
                &vec![Part::Text {
                    text: "Let me".to_owned()
                }]
            );
            assert_eq!(binding.provider, model().provider);
            assert_eq!(binding.model, model().model);
            assert_eq!(binding.prefix_epoch, 0);
            assert_eq!(binding.tools_hash.len(), 64, "sha-256 hex");
            let value: serde_json::Value = serde_json::from_str(raw.json.as_str()).unwrap();
            assert_eq!(value["type"], "thinking");
            assert_eq!(value["thinking"], "Let me");
            assert_eq!(value["signature"], "sig-abc");
        }
        other => panic!("expected reasoning, got {other:?}"),
    }
    assert_eq!(
        items[1],
        &Item::Message {
            role: asterpath_protocol::Role::Assistant,
            parts: vec![Part::Text {
                text: "Hello, world".to_owned()
            }],
            raw: Some(text_raw(r#"{"text":"Hello, world","type":"text"}"#)),
        }
    );
    match items[2] {
        Item::ToolCall {
            call_id,
            name,
            raw_args,
            ..
        } => {
            assert_eq!(call_id, "toolu_1");
            assert_eq!(name.as_str(), "search");
            assert_eq!(raw_args.as_str(), r#"{"q":"rust"}"#);
        }
        other => panic!("expected tool call, got {other:?}"),
    }
    match items[3] {
        Item::Reasoning { summary, raw, .. } => {
            assert!(summary.is_empty(), "redacted thinking has no visible text");
            let value: serde_json::Value = serde_json::from_str(raw.json.as_str()).unwrap();
            assert_eq!(value["type"], "redacted_thinking");
            assert_eq!(value["data"], "opaque==");
        }
        other => panic!("expected redacted reasoning, got {other:?}"),
    }

    let deltas: Vec<&PartDelta> = events
        .iter()
        .filter_map(|event| match event {
            StreamEvent::PartDelta { delta, .. } => Some(delta),
            _ => None,
        })
        .collect();
    assert_eq!(deltas.len(), 5, "signature_delta is internal");
    assert_eq!(
        deltas[0],
        &PartDelta::ReasoningSummary {
            text: "Let me".to_owned()
        }
    );
    assert_eq!(
        deltas[1],
        &PartDelta::Text {
            text: "Hello".to_owned()
        }
    );
    assert_eq!(
        deltas[2],
        &PartDelta::Text {
            text: ", world".to_owned()
        }
    );
    assert_eq!(
        deltas[3],
        &PartDelta::JsonArgs {
            fragment: r#"{"q":"#.to_owned()
        }
    );
    assert_eq!(
        deltas[4],
        &PartDelta::JsonArgs {
            fragment: r#""rust"}"#.to_owned()
        }
    );
}

#[tokio::test]
async fn parallel_tool_use_json_deltas_carry_their_block_output_index() {
    let tool_use = |index: u32, id: &str| {
        format!(
            r#"{{"type":"content_block_start","index":{index},"content_block":{{"type":"tool_use","id":"{id}","name":"search","input":{{}}}}}}"#
        )
    };
    let json_delta = |index: u32, partial: &str| {
        format!(
            r#"{{"type":"content_block_delta","index":{index},"delta":{{"type":"input_json_delta","partial_json":{}}}}}"#,
            serde_json::to_string(partial).unwrap()
        )
    };
    let stop = |index: u32| format!(r#"{{"type":"content_block_stop","index":{index}}}"#);
    let frames = [
        (
            "message_start",
            r#"{"type":"message_start","message":{"id":"msg_par","usage":{"input_tokens":1,"output_tokens":1}}}"#.to_owned(),
        ),
        ("content_block_start", tool_use(0, "toolu_a")),
        ("content_block_delta", json_delta(0, r#"{"q":"#)),
        ("content_block_delta", json_delta(0, r#""a"}"#)),
        ("content_block_stop", stop(0)),
        ("content_block_start", tool_use(1, "toolu_b")),
        ("content_block_delta", json_delta(1, r#"{"q":"#)),
        ("content_block_delta", json_delta(1, r#""b"}"#)),
        ("content_block_stop", stop(1)),
        (
            "message_delta",
            r#"{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":2}}"#.to_owned(),
        ),
        ("message_stop", r#"{"type":"message_stop"}"#.to_owned()),
    ];
    let frames: Vec<(&str, &str)> = frames
        .iter()
        .map(|(event, data)| (*event, data.as_str()))
        .collect();
    let server = MockServer::start(vec![Reply::sse(sse_body(&frames))]);
    let events = ok(drain(&provider(&server), base_request()).await);

    let mut fragments: std::collections::BTreeMap<u32, String> = Default::default();
    let mut calls = std::collections::BTreeMap::new();
    for event in &events {
        match event {
            StreamEvent::PartDelta {
                output_index,
                part_index: 0,
                delta: PartDelta::JsonArgs { fragment },
                ..
            } => fragments
                .entry(*output_index)
                .or_default()
                .push_str(fragment),
            StreamEvent::ItemCompleted {
                output_index,
                item: Item::ToolCall {
                    call_id, raw_args, ..
                },
                ..
            } => {
                calls.insert(*output_index, (call_id.clone(), raw_args.clone()));
            }
            _ => {}
        }
    }
    assert_eq!(fragments.len(), 2);
    for (index, call_id) in [(0, "toolu_a"), (1, "toolu_b")] {
        assert_eq!(calls[&index].0, call_id);
        assert_eq!(calls[&index].1.as_str(), fragments[&index]);
    }
    assert_eq!(fragments[&1], r#"{"q":"b"}"#);
}

#[tokio::test]
async fn maps_server_tools_and_unknown_blocks_to_raw_items() {
    let body = sse_body(&[
        (
            "message_start",
            r#"{"type":"message_start","message":{"id":"msg_s","usage":{"input_tokens":1}}}"#,
        ),
        (
            "content_block_start",
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"server_tool_use","id":"srv_1","name":"web_search","input":{}}}"#,
        ),
        (
            "content_block_delta",
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"query\":\"x\"}"}}"#,
        ),
        (
            "content_block_stop",
            r#"{"type":"content_block_stop","index":0}"#,
        ),
        (
            "content_block_start",
            r#"{"type":"content_block_start","index":1,"content_block":{"type":"web_search_tool_result","tool_use_id":"srv_1","content":[{"type":"text","text":"result"}]}}"#,
        ),
        (
            "content_block_stop",
            r#"{"type":"content_block_stop","index":1}"#,
        ),
        (
            "content_block_start",
            r#"{"type":"content_block_start","index":2,"content_block":{"type":"mystery_block","value":7}}"#,
        ),
        (
            "content_block_stop",
            r#"{"type":"content_block_stop","index":2}"#,
        ),
        (
            "message_delta",
            r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}"#,
        ),
        ("message_stop", r#"{"type":"message_stop"}"#),
    ]);
    let server = MockServer::start(vec![Reply::sse(body)]);
    let events = ok(drain(&provider(&server), base_request()).await);
    let items: Vec<&Item> = events
        .iter()
        .filter_map(|event| match event {
            StreamEvent::ItemCompleted { item, .. } => Some(item),
            _ => None,
        })
        .collect();
    match items[0] {
        Item::ServerToolCall {
            provider,
            name,
            call_id,
            raw,
        } => {
            assert_eq!(provider.as_str(), "anthropic");
            assert_eq!(name, "web_search");
            assert_eq!(call_id.as_deref(), Some("srv_1"));
            let value: serde_json::Value = serde_json::from_str(raw.json.as_str()).unwrap();
            assert_eq!(value["input"]["query"], "x");
        }
        other => panic!("expected server tool call, got {other:?}"),
    }
    match items[1] {
        Item::ServerToolResult {
            call_id,
            parts,
            raw,
            ..
        } => {
            assert_eq!(call_id.as_deref(), Some("srv_1"));
            assert_eq!(
                parts,
                &vec![Part::Text {
                    text: "result".to_owned()
                }]
            );
            assert!(raw.json.as_str().contains("web_search_tool_result"));
        }
        other => panic!("expected server tool result, got {other:?}"),
    }
    match items[2] {
        Item::ProviderOpaque { kind, raw, .. } => {
            assert_eq!(kind, "mystery_block");
            assert!(raw.json.as_str().contains("mystery_block"));
        }
        other => panic!("expected opaque item, got {other:?}"),
    }
}

#[tokio::test]
async fn missing_usage_fields_are_zero() {
    let body = sse_body(&[
        (
            "message_start",
            r#"{"type":"message_start","message":{"id":"msg_u","usage":{}}}"#,
        ),
        (
            "message_delta",
            r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{}}"#,
        ),
        ("message_stop", r#"{"type":"message_stop"}"#),
    ]);
    let server = MockServer::start(vec![Reply::sse(body)]);
    let events = ok(drain(&provider(&server), base_request()).await);
    match events.last() {
        Some(StreamEvent::ResponseEnded { usage, .. }) => {
            assert_eq!(*usage, Usage::default())
        }
        other => panic!("expected ResponseEnded, got {other:?}"),
    }
}

#[tokio::test]
async fn stop_reasons_map_from_anthropic_labels() {
    let cases = [
        ("end_turn", StopReason::EndTurn),
        ("tool_use", StopReason::ToolUse),
        ("max_tokens", StopReason::MaxTokens),
        ("stop_sequence", StopReason::StopSequence),
        ("pause_turn", StopReason::PauseTurn),
        (
            "model_context_window_exceeded",
            StopReason::ContextWindowExceeded,
        ),
        ("refusal", StopReason::Refusal),
        (
            "mystery",
            StopReason::Other {
                provider_label: "mystery".to_owned(),
            },
        ),
    ];
    let replies = cases
        .iter()
        .map(|(label, _)| {
            Reply::sse(sse_body(&[
                (
                    "message_start",
                    r#"{"type":"message_start","message":{"id":"msg_r","usage":{"input_tokens":1}}}"#,
                ),
                (
                    "message_delta",
                    &format!(
                        r#"{{"type":"message_delta","delta":{{"stop_reason":"{label}"}},"usage":{{"output_tokens":1}}}}"#
                    ),
                ),
                ("message_stop", r#"{"type":"message_stop"}"#),
            ]))
        })
        .collect();
    let server = MockServer::start(replies);
    let provider = provider(&server);
    for (_, expected) in cases {
        let events = ok(drain(&provider, base_request()).await);
        match events.last() {
            Some(StreamEvent::ResponseEnded { stop_reason, .. }) => {
                assert_eq!(*stop_reason, expected)
            }
            other => panic!("expected ResponseEnded, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn refusal_stop_reason_coexists_with_visible_content() {
    let body = sse_body(&[
        (
            "message_start",
            r#"{"type":"message_start","message":{"id":"msg_ref","usage":{"input_tokens":2}}}"#,
        ),
        (
            "content_block_start",
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
        ),
        (
            "content_block_delta",
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"I cannot"}}"#,
        ),
        (
            "content_block_stop",
            r#"{"type":"content_block_stop","index":0}"#,
        ),
        (
            "message_delta",
            r#"{"type":"message_delta","delta":{"stop_reason":"refusal"},"usage":{"output_tokens":2}}"#,
        ),
        ("message_stop", r#"{"type":"message_stop"}"#),
    ]);
    let server = MockServer::start(vec![Reply::sse(body)]);
    let events = ok(drain(&provider(&server), base_request()).await);
    assert_eq!(
        events
            .iter()
            .filter_map(|event| match event {
                StreamEvent::ItemCompleted { item, .. } => Some(item),
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![&Item::Message {
            role: asterpath_protocol::Role::Assistant,
            parts: vec![Part::Text {
                text: "I cannot".to_owned()
            }],
            raw: Some(text_raw(r#"{"text":"I cannot","type":"text"}"#)),
        }]
    );
    assert!(matches!(
        events.last(),
        Some(StreamEvent::ResponseEnded {
            stop_reason: StopReason::Refusal,
            ..
        })
    ));
}

#[tokio::test]
async fn a_stream_without_message_stop_is_a_transport_truncation() {
    let body = sse_body(&[
        (
            "message_start",
            r#"{"type":"message_start","message":{"id":"msg_t","usage":{"input_tokens":9}}}"#,
        ),
        (
            "content_block_start",
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
        ),
        (
            "content_block_delta",
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"partial"}}"#,
        ),
        (
            "content_block_stop",
            r#"{"type":"content_block_stop","index":0}"#,
        ),
    ]);
    let server = MockServer::start(vec![Reply::sse(body)]);
    let events = drain(&provider(&server), base_request()).await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Ok(StreamEvent::ResponseEnded { .. }))),
        "a failed stream must not emit ResponseEnded"
    );
    let error = first_error(events.clone());
    assert_eq!(error.kind, ProviderErrorKind::Transport);
    assert!(error.retryable);
    assert_eq!(
        error.partial_usage,
        Some(Usage {
            input: asterpath_protocol::InputTokens {
                uncached: 9,
                cache_read: 0,
                cache_write: 0,
            },
            output: Default::default(),
        })
    );
    assert_eq!(error.request_id.as_deref(), Some("msg_t"));
}

#[tokio::test]
async fn a_connection_cut_mid_stream_is_a_transport_truncation() {
    let body = happy_sse();
    let server = MockServer::start(vec![Reply::truncated_sse(body, 500)]);
    let events = drain(&provider(&server), base_request()).await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Ok(StreamEvent::ResponseEnded { .. })))
    );
    let error = first_error(events);
    assert_eq!(error.kind, ProviderErrorKind::Transport);
    assert!(error.retryable);
}

#[tokio::test]
async fn stream_protocol_violations_are_reported_without_a_terminal_event() {
    let cases = [
        // content block before message_start
        sse_body(&[
            (
                "content_block_start",
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
            ),
            (
                "content_block_stop",
                r#"{"type":"content_block_stop","index":0}"#,
            ),
        ]),
        // duplicate output_index
        sse_body(&[
            (
                "message_start",
                r#"{"type":"message_start","message":{"id":"m","usage":{}}}"#,
            ),
            (
                "content_block_start",
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
            ),
            (
                "content_block_start",
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
            ),
        ]),
        // stop for an unstarted block
        sse_body(&[
            (
                "message_start",
                r#"{"type":"message_start","message":{"id":"m","usage":{}}}"#,
            ),
            (
                "content_block_stop",
                r#"{"type":"content_block_stop","index":0}"#,
            ),
        ]),
        // message_stop with an unfinished block
        sse_body(&[
            (
                "message_start",
                r#"{"type":"message_start","message":{"id":"m","usage":{}}}"#,
            ),
            (
                "content_block_start",
                r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
            ),
            ("message_stop", r#"{"type":"message_stop"}"#),
        ]),
        // malformed JSON data
        sse_body(&[
            (
                "message_start",
                r#"{"type":"message_start","message":{"id":"m","usage":{}}}"#,
            ),
            ("content_block_start", "{not json"),
        ]),
    ];
    for body in cases {
        let server = MockServer::start(vec![Reply::sse(body)]);
        let events = drain(&provider(&server), base_request()).await;
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, Ok(StreamEvent::ResponseEnded { .. }))),
            "protocol violations never emit ResponseEnded"
        );
        let error = first_error(events);
        assert_eq!(error.kind, ProviderErrorKind::StreamProtocol);
        assert!(!error.retryable);
    }
}

#[tokio::test]
async fn an_sse_error_event_is_classified_with_partial_usage() {
    let body = sse_body(&[
        (
            "message_start",
            r#"{"type":"message_start","message":{"id":"msg_err","usage":{"input_tokens":10,"cache_read_input_tokens":2}}}"#,
        ),
        (
            "error",
            r#"{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}"#,
        ),
    ]);
    let server = MockServer::start(vec![Reply::sse(body)]);
    let events = drain(&provider(&server), base_request()).await;
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Ok(StreamEvent::ResponseEnded { .. })))
    );
    let error = first_error(events);
    assert_eq!(error.kind, ProviderErrorKind::Overloaded);
    assert!(error.retryable);
    assert_eq!(error.request_id.as_deref(), Some("msg_err"));
    let usage = error.partial_usage.expect("partial usage");
    assert_eq!(usage.input.uncached, 10);
    assert_eq!(usage.input.cache_read, 2);
    let raw = error.raw.expect("raw error body");
    assert!(raw.json.as_str().contains("overloaded_error"));
}

#[tokio::test]
async fn http_error_responses_map_to_normalized_kinds() {
    let cases = [
        (401, None, ProviderErrorKind::Auth, false, false),
        (403, None, ProviderErrorKind::Auth, false, false),
        (400, None, ProviderErrorKind::InvalidRequest, false, false),
        (404, None, ProviderErrorKind::InvalidRequest, false, false),
        (418, None, ProviderErrorKind::InvalidRequest, false, false),
        (408, None, ProviderErrorKind::Timeout, true, false),
        (429, Some(7), ProviderErrorKind::RateLimited, true, true),
        (500, None, ProviderErrorKind::Server, true, false),
        (502, None, ProviderErrorKind::Server, true, false),
        (503, Some(2), ProviderErrorKind::Overloaded, true, true),
        (529, Some(2), ProviderErrorKind::Overloaded, true, true),
        (599, None, ProviderErrorKind::Server, true, false),
    ];
    let replies = cases
        .iter()
        .map(|(status, retry, _, _, _)| {
            let mut reply = Reply::json(
                *status,
                r#"{"type":"error","error":{"type":"api_error","message":"failed"}}"#,
            )
            .header("request-id", "req-1");
            if let Some(seconds) = retry {
                reply = reply.header("retry-after", &seconds.to_string());
            }
            reply
        })
        .collect();
    let server = MockServer::start(replies);
    let provider = provider(&server);
    for (status, retry, kind, retryable, has_retry_after) in cases {
        let events = drain(&provider, base_request()).await;
        assert_eq!(events.len(), 1, "status {status}");
        let error = first_error(events);
        assert_eq!(error.kind, kind, "status {status}");
        assert_eq!(error.retryable, retryable, "status {status}");
        assert_eq!(
            error.retry_after.is_some(),
            has_retry_after,
            "status {status}"
        );
        assert_eq!(error.request_id.as_deref(), Some("req-1"));
        assert_eq!(error.message, "failed");
        assert!(
            error
                .raw
                .as_ref()
                .is_some_and(|raw| raw.json.as_str().contains("failed")),
            "raw error body preserved"
        );
        if let Some(seconds) = retry {
            assert_eq!(error.retry_after, Some(Duration::from_secs(seconds)));
        }
    }
}

#[tokio::test]
async fn context_window_messages_map_to_context_exceeded() {
    let server = MockServer::start(vec![Reply::json(
        400,
        r#"{"type":"error","error":{"type":"invalid_request_error","message":"prompt is too long: 210000 tokens > 200000 maximum"}}"#,
    )]);
    let events = drain(&provider(&server), base_request()).await;
    let error = first_error(events);
    assert_eq!(error.kind, ProviderErrorKind::ContextExceeded);
    assert!(!error.retryable);
    assert!(error.context_exceeded());
}

#[tokio::test]
async fn a_spend_limit_429_is_not_retryable() {
    let server = MockServer::start(vec![Reply::json(
        429,
        r#"{"type":"error","error":{"type":"rate_limit_error","message":"spend limit reached","details":{"error_code":"enforced_spend_limit_reached"}}}"#,
    )]);
    let error = first_error(drain(&provider(&server), base_request()).await);
    assert_eq!(error.kind, ProviderErrorKind::RateLimited);
    assert!(!error.retryable);
}

#[tokio::test]
async fn non_transient_error_types_are_not_retryable_on_both_paths() {
    let cases = [
        (402, "billing_error", false),
        (404, "not_found_error", false),
        (413, "request_too_large", false),
        (529, "overloaded_error", true),
        (500, "api_error", true),
        (504, "timeout_error", true),
    ];
    let mut replies = Vec::new();
    for (status, label, _) in cases {
        let body =
            format!(r#"{{"type":"error","error":{{"type":"{label}","message":"{label}"}}}}"#);
        replies.push(Reply::json(status, body.clone()));
        replies.push(Reply::sse(sse_body(&[
            (
                "message_start",
                r#"{"type":"message_start","message":{"id":"m","usage":{}}}"#,
            ),
            ("error", &body),
        ])));
    }
    let server = MockServer::start(replies);
    let provider = provider(&server);
    for (status, label, retryable) in cases {
        for path in ["http", "sse"] {
            let error = first_error(drain(&provider, base_request()).await);
            assert_eq!(error.retryable, retryable, "{path} {status} {label}");
        }
    }
}

#[tokio::test]
async fn message_delta_cumulative_input_usage_overrides_message_start() {
    let body = sse_body(&[
        (
            "message_start",
            r#"{"type":"message_start","message":{"id":"m","usage":{"input_tokens":10,"cache_read_input_tokens":4,"cache_creation_input_tokens":2,"output_tokens":1}}}"#,
        ),
        (
            "message_delta",
            r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":30,"cache_read_input_tokens":12,"cache_creation_input_tokens":6,"output_tokens":9}}"#,
        ),
        ("message_stop", r#"{"type":"message_stop"}"#),
    ]);
    let server = MockServer::start(vec![Reply::sse(body)]);
    let events = ok(drain(&provider(&server), base_request()).await);
    match events.last() {
        Some(StreamEvent::ResponseEnded { usage, .. }) => assert_eq!(
            *usage,
            Usage {
                input: asterpath_protocol::InputTokens {
                    uncached: 30,
                    cache_read: 12,
                    cache_write: 6,
                },
                output: asterpath_protocol::OutputTokens {
                    total: 9,
                    reasoning: 0,
                },
            }
        ),
        other => panic!("expected ResponseEnded, got {other:?}"),
    }
}

#[tokio::test]
async fn a_closed_connection_is_a_retryable_transport_error() {
    let server = MockServer::start(vec![Reply::close()]);
    let events = drain(&provider(&server), base_request()).await;
    assert_eq!(events.len(), 1);
    let error = first_error(events);
    assert_eq!(error.kind, ProviderErrorKind::Transport);
    assert!(error.retryable);
}

#[tokio::test]
async fn an_empty_200_body_is_a_truncated_stream() {
    let server = MockServer::start(vec![Reply::sse("")]);
    let events = drain(&provider(&server), base_request()).await;
    let error = first_error(events);
    assert_eq!(error.kind, ProviderErrorKind::Transport);
    assert!(error.retryable);
}

#[tokio::test]
async fn empty_response_is_a_valid_end_turn() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let events = ok(drain(&provider(&server), base_request()).await);
    assert!(matches!(
        events.first(),
        Some(StreamEvent::ResponseStarted { sequence: 0, .. })
    ));
    assert!(matches!(
        events.last(),
        Some(StreamEvent::ResponseEnded {
            stop_reason: StopReason::EndTurn,
            ..
        })
    ));
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, StreamEvent::ItemStarted { .. }))
    );
}

struct AnthropicWire;

impl asterpath_testing::ProviderWire for AnthropicWire {
    fn provider(&self, base_url: &str) -> Box<dyn asterpath_protocol::Provider> {
        Box::new(
            asterpath_provider_anthropic::AnthropicProvider::new("test-key")
                .with_base_url(base_url),
        )
    }

    fn text_response(&self, text: &str) -> Vec<serde_json::Value> {
        use serde_json::json;
        vec![
            json!({"type": "message_start", "message": {"id": "msg_c", "usage": {"input_tokens": 1, "output_tokens": 0}}}),
            json!({"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}),
            json!({"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": text}}),
            json!({"type": "content_block_stop", "index": 0}),
            json!({"type": "message_delta", "delta": {"stop_reason": "end_turn"}, "usage": {"output_tokens": 1}}),
            json!({"type": "message_stop"}),
        ]
    }
}

#[tokio::test]
async fn text_citations_are_kept_in_raw_and_replayed_only_to_the_same_provider() {
    let citation = r#"{"type":"char_location","cited_text":"sky is blue","document_index":0,"document_title":"Doc","start_char_index":0,"end_char_index":11}"#;
    let delta = format!(
        r#"{{"type":"content_block_delta","index":0,"delta":{{"type":"citations_delta","citation":{citation}}}}}"#
    );
    let body = sse_body(&[
        (
            "message_start",
            r#"{"type":"message_start","message":{"id":"msg_cite","usage":{"input_tokens":2}}}"#,
        ),
        (
            "content_block_start",
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":"","citations":[]}}"#,
        ),
        ("content_block_delta", &delta),
        (
            "content_block_delta",
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"The sky is blue."}}"#,
        ),
        (
            "content_block_stop",
            r#"{"type":"content_block_stop","index":0}"#,
        ),
        (
            "message_delta",
            r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":4}}"#,
        ),
        ("message_stop", r#"{"type":"message_stop"}"#),
    ]);
    let server = MockServer::start(vec![
        Reply::sse(body),
        Reply::sse(empty_sse()),
        Reply::sse(empty_sse()),
        Reply::sse(empty_sse()),
    ]);
    let provider = provider(&server);
    let message = ok(drain(&provider, base_request()).await)
        .into_iter()
        .find_map(|event| match event {
            StreamEvent::ItemCompleted { item, .. } => Some(item),
            _ => None,
        })
        .expect("text item");
    let citation: serde_json::Value = serde_json::from_str(citation).unwrap();
    let Item::Message {
        raw: Some(raw),
        parts,
        ..
    } = &message
    else {
        panic!("text item carries raw: {message:?}");
    };
    assert_eq!(
        parts,
        &[Part::Text {
            text: "The sky is blue.".to_owned()
        }]
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(raw.json.as_str()).unwrap(),
        serde_json::json!({"type": "text", "text": "The sky is blue.", "citations": [citation]})
    );

    let replayed_block = |request: usize| {
        let body = server.requests()[request].json_body();
        let messages = body["messages"].as_array().unwrap().clone();
        messages.last().unwrap()["content"][0].clone()
    };

    let mut replay = base_request();
    replay.items.push(message.clone());
    let _ = drain(&provider, replay).await;
    let block = replayed_block(1);
    assert_eq!(block["text"], "The sky is blue.");
    assert_eq!(block["citations"], serde_json::json!([citation]));

    let mut rewritten = message.clone();
    if let Item::Message { parts, .. } = &mut rewritten {
        *parts = vec![Part::Text {
            text: "Compacted.".to_owned(),
        }];
    }
    let mut replay = base_request();
    replay.items.push(rewritten);
    let _ = drain(&provider, replay).await;
    let block = replayed_block(2);
    assert_eq!(block["text"], "Compacted.");
    assert_eq!(block["citations"], serde_json::json!([citation]));

    let mut foreign = message;
    if let Item::Message { raw: Some(raw), .. } = &mut foreign {
        raw.provider = asterpath_protocol::ProviderId::new("other");
    }
    let mut replay = base_request();
    replay.items.push(foreign);
    let _ = drain(&provider, replay).await;
    let block = replayed_block(3);
    assert_eq!(block["text"], "The sky is blue.");
    assert!(block.get("citations").is_none(), "{block}");
}

#[tokio::test]
async fn anthropic_satisfies_the_provider_contract() {
    asterpath_testing::provider_contract(&AnthropicWire).await;
}
