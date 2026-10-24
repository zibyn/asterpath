use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use asterpath_testing::http_mock::{MockServer, Reply as MockResponse};

use asterpath_protocol::{
    ImageSource, Item, JsonString, ModelId, ModelRequest, ModelSpec, Part, PartDelta, Provider,
    ProviderCapabilities, ProviderError, ProviderErrorKind, ProviderId, RawProviderData,
    ReasoningBinding, Role, StopReason, StreamEvent, ToolChoice, ToolDescriptor, ToolName, Usage,
};
use futures_util::StreamExt;
use serde_json::{Value, json};

use asterpath_provider_openai::{OpenAiProvider, TraceMark, TraceSink};

#[derive(Default)]
struct RecordingSink(Mutex<Vec<TraceMark>>);

impl TraceSink for RecordingSink {
    fn mark(&self, mark: TraceMark) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(mark);
    }
}

impl RecordingSink {
    fn marks(&self) -> Vec<TraceMark> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

fn spec(model: &str) -> ModelSpec {
    ModelSpec {
        provider: ProviderId::new("openai"),
        model: ModelId::new(model),
        context_window: Some(128_000),
    }
}

fn provider(server: &MockServer) -> OpenAiProvider {
    OpenAiProvider::new("test-key").with_base_url(format!("{}/v1", server.base_url()))
}

fn request(items: Vec<Item>) -> ModelRequest {
    ModelRequest {
        spec: spec("gpt-5"),
        prefix_epoch: 0,
        system: vec![Part::Text {
            text: "You are terse.".to_owned(),
        }],
        items,
        tools: vec![tool("get_weather")],
        tool_choice: ToolChoice::Auto,
        disable_parallel_tool_use: false,
        sampling: None,
        max_output_tokens: 128,
        reasoning: None,
    }
}

fn tool(name: &str) -> ToolDescriptor {
    ToolDescriptor {
        name: ToolName::new(name),
        description: "Look up weather".to_owned(),
        schema: json!({"type": "object", "properties": {"city": {"type": "string"}}}),
        parallelism: asterpath_protocol::Parallelism::Parallel,
        replay: asterpath_protocol::ReplaySafety::Safe,
        side_effect: asterpath_protocol::SideEffect::ReadOnly,
        provenance: asterpath_protocol::Provenance::InProcess,
        timeout: None,
        max_output_bytes: None,
    }
}

fn user_text(text: &str) -> Item {
    Item::Message {
        role: Role::User,
        parts: vec![Part::Text {
            text: text.to_owned(),
        }],
    }
}

fn binding(model: &str, prefix_epoch: u32, tools_hash: &str) -> ReasoningBinding {
    ReasoningBinding {
        provider: ProviderId::new("openai"),
        model: ModelId::new(model),
        prefix_epoch,
        tools_hash: tools_hash.to_owned(),
    }
}

fn raw_data(provider: &str, kind: &str, json: &str) -> RawProviderData {
    RawProviderData {
        provider: ProviderId::new(provider),
        model: None,
        kind: kind.to_owned(),
        json: JsonString::new(json),
    }
}

fn reasoning_item(binding: ReasoningBinding, raw: &str) -> Item {
    Item::Reasoning {
        summary: vec![Part::Text {
            text: "thought".to_owned(),
        }],
        binding,
        raw: raw_data("openai", "openai.reasoning", raw),
    }
}

async fn collect(
    provider: &OpenAiProvider,
    request: ModelRequest,
) -> Vec<Result<StreamEvent, ProviderError>> {
    provider.stream(request).collect().await
}

fn event(data: Value) -> String {
    let kind = data["type"].as_str().expect("event type").to_owned();
    format!("event: {kind}\ndata: {data}\n\n")
}

fn sse(events: &[Value]) -> String {
    events.iter().map(|data| event(data.clone())).collect()
}

fn created(sequence: u64) -> Value {
    json!({
        "type": "response.created",
        "sequence_number": sequence,
        "response": {"id": "resp_1", "status": "in_progress", "output": []}
    })
}

fn added(sequence: u64, index: u32, item: Value) -> Value {
    json!({
        "type": "response.output_item.added",
        "sequence_number": sequence,
        "output_index": index,
        "item": item
    })
}

fn text_delta(sequence: u64, index: u32, text: &str) -> Value {
    json!({
        "type": "response.output_text.delta",
        "sequence_number": sequence,
        "item_id": "msg_1",
        "output_index": index,
        "content_index": 0,
        "delta": text
    })
}

fn done(sequence: u64, index: u32, item: Value) -> Value {
    json!({
        "type": "response.output_item.done",
        "sequence_number": sequence,
        "output_index": index,
        "item": item
    })
}

fn message_item(id: &str, text: &str) -> Value {
    json!({
        "id": id,
        "type": "message",
        "status": "completed",
        "role": "assistant",
        "content": [{"type": "output_text", "text": text, "annotations": []}]
    })
}

fn function_call_item(id: &str, call_id: &str, name: &str, arguments: &str) -> Value {
    json!({
        "id": id,
        "type": "function_call",
        "status": "completed",
        "call_id": call_id,
        "name": name,
        "arguments": arguments
    })
}

fn usage_json(input: u64, cached: u64, output: u64) -> Value {
    json!({
        "input_tokens": input,
        "input_tokens_details": {"cached_tokens": cached},
        "output_tokens": output,
        "output_tokens_details": {"reasoning_tokens": 0},
        "total_tokens": 999_999
    })
}

fn completed(sequence: u64, output: Vec<Value>, usage: Value) -> Value {
    json!({
        "type": "response.completed",
        "sequence_number": sequence,
        "response": {
            "id": "resp_1",
            "status": "completed",
            "output": output,
            "usage": usage
        }
    })
}

fn incomplete(sequence: u64, output: Vec<Value>, reason: &str, usage: Value) -> Value {
    json!({
        "type": "response.incomplete",
        "sequence_number": sequence,
        "response": {
            "id": "resp_1",
            "status": "incomplete",
            "incomplete_details": {"reason": reason},
            "output": output,
            "usage": usage
        }
    })
}

fn completed_text_body(text: &str) -> String {
    let item = message_item("msg_1", text);
    sse(&[
        created(0),
        added(1, 0, message_item("msg_1", "")),
        text_delta(2, 0, text),
        done(3, 0, item.clone()),
        completed(4, vec![item], usage_json(10, 0, 2)),
    ])
}

fn completed_item(text: &str) -> Item {
    Item::Message {
        role: Role::Assistant,
        parts: vec![Part::Text {
            text: text.to_owned(),
        }],
    }
}

fn response_ended(events: &[Result<StreamEvent, ProviderError>]) -> (StopReason, Usage) {
    let mut terminal = None;
    for event in events {
        if let Ok(StreamEvent::ResponseEnded {
            stop_reason, usage, ..
        }) = event
        {
            assert!(terminal.is_none(), "more than one ResponseEnded");
            terminal = Some((stop_reason.clone(), *usage));
        }
    }
    terminal.expect("one ResponseEnded")
}

fn completed_items(events: &[Result<StreamEvent, ProviderError>]) -> Vec<(u32, Item)> {
    events
        .iter()
        .filter_map(|event| match event {
            Ok(StreamEvent::ItemCompleted {
                output_index, item, ..
            }) => Some((*output_index, item.clone())),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn request_always_disables_server_state_and_maps_system_to_instructions() {
    let server = MockServer::start(vec![MockResponse::sse(completed_text_body("Hi"))]);
    let provider = provider(&server);
    let events = collect(&provider, request(vec![user_text("hello")])).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");

    let body: Value = serde_json::from_str(&server.requests()[0].body).expect("request json");
    assert_eq!(body["store"], json!(false));
    assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
    assert!(body.get("previous_response_id").is_none());
    assert!(body.get("conversation").is_none());
    assert_eq!(body["instructions"], json!("You are terse."));
    assert_eq!(body["model"], json!("gpt-5"));
    assert_eq!(body["max_output_tokens"], json!(128));
    assert_eq!(
        body["input"],
        json!([{
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "hello"}]
        }])
    );
    assert_eq!(body["tools"][0]["type"], json!("function"));
    assert_eq!(body["tools"][0]["name"], json!("get_weather"));
    assert!(body["tools"][0].get("parameters").is_some());
    assert_eq!(body["tool_choice"], json!("auto"));
}

#[tokio::test]
async fn provider_declares_openai_capabilities() {
    let provider = provider(&MockServer::start(Vec::new()));
    let capabilities = provider.capabilities(&spec("gpt-5"));
    assert_eq!(provider.id(), ProviderId::new("openai"));
    assert_eq!(
        capabilities,
        ProviderCapabilities {
            server_state: false,
            forced_tool_choice: true,
            sampling: true,
            reasoning_roundtrip: true,
            citations: false,
            builtin_tools: false,
            cache_control: false,
            image_input: true,
            parallel_tool_calls: true,
        }
    );
}

#[tokio::test]
async fn user_images_render_as_input_image() {
    let server = MockServer::start(vec![MockResponse::sse(completed_text_body("Hi"))]);
    let provider = provider(&server);
    let image = request(vec![Item::Message {
        role: Role::User,
        parts: vec![
            Part::Text {
                text: "look".to_owned(),
            },
            Part::Image {
                media_type: "image/png".to_owned(),
                source: ImageSource::Base64 {
                    data: "AAAA".to_owned(),
                },
            },
            Part::Image {
                media_type: "image/jpeg".to_owned(),
                source: ImageSource::Url {
                    url: "https://example.invalid/x.jpg".to_owned(),
                },
            },
        ],
    }]);
    let events = collect(&provider, image).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");

    let body: Value = serde_json::from_str(&server.requests()[0].body).expect("request json");
    assert_eq!(
        body["input"][0]["content"],
        json!([
            {"type": "input_text", "text": "look"},
            {"type": "input_image", "image_url": "data:image/png;base64,AAAA"},
            {"type": "input_image", "image_url": "https://example.invalid/x.jpg"}
        ])
    );
}

#[tokio::test]
async fn request_renders_forced_choice_sampling_parallel_and_reasoning() {
    let server = MockServer::start(vec![MockResponse::sse(completed_text_body("Hi"))]);
    let provider = provider(&server);
    let mut request = request(vec![user_text("hello")]);
    request.tool_choice = ToolChoice::Tool(ToolName::new("get_weather"));
    request.disable_parallel_tool_use = true;
    request.sampling = Some(asterpath_protocol::SamplingParams {
        temperature: Some(0.25),
        top_p: Some(0.5),
        top_k: None,
        seed: None,
    });
    request.reasoning = Some(asterpath_protocol::ReasoningParams {
        effort: Some(asterpath_protocol::ReasoningEffort::High),
        budget_tokens: None,
        summary: true,
    });
    let events = collect(&provider, request).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");

    let body: Value = serde_json::from_str(&server.requests()[0].body).expect("request json");
    assert_eq!(
        body["tool_choice"],
        json!({"type": "function", "name": "get_weather"})
    );
    assert_eq!(body["parallel_tool_calls"], json!(false));
    assert_eq!(body["temperature"], json!(0.25));
    assert_eq!(body["top_p"], json!(0.5));
    assert!(body.get("seed").is_none());
    assert_eq!(
        body["stream"],
        json!(true),
        "Responses API only streams SSE when asked"
    );
    assert_eq!(
        body["reasoning"],
        json!({"effort": "high", "summary": "auto"})
    );
}

#[tokio::test]
async fn interleaved_items_are_normalized_by_output_index() {
    let first = message_item("msg_first", "first");
    let second = message_item("msg_second", "second");
    let server = MockServer::start(vec![MockResponse::sse(sse(&[
        created(0),
        added(1, 1, message_item("msg_second", "")),
        added(2, 0, message_item("msg_first", "")),
        text_delta(3, 1, "second"),
        text_delta(4, 0, "first"),
        done(5, 1, second.clone()),
        done(6, 0, first.clone()),
        completed(7, vec![first, second], usage_json(10, 0, 2)),
    ]))]);
    let events = collect(&provider(&server), request(vec![user_text("go")])).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");

    let completed = completed_items(&events);
    assert_eq!(completed.len(), 2);
    assert_eq!(completed[0].0, 1, "provider completion order is preserved");
    assert_eq!(completed[1].0, 0);
    let by_index: std::collections::BTreeMap<u32, Item> = completed.into_iter().collect();
    assert_eq!(by_index[&0], completed_item("first"));
    assert_eq!(by_index[&1], completed_item("second"));

    let mut last_sequence = None;
    for event in events {
        let sequence = match event.expect("event") {
            StreamEvent::ResponseStarted { sequence, .. }
            | StreamEvent::ItemStarted { sequence, .. }
            | StreamEvent::PartDelta { sequence, .. }
            | StreamEvent::ItemCompleted { sequence, .. }
            | StreamEvent::ResponseEnded { sequence, .. } => sequence,
            other => panic!("unknown stream event: {other:?}"),
        };
        if let Some(previous) = last_sequence {
            assert!(sequence > previous, "sequences must increase");
        }
        last_sequence = Some(sequence);
    }
}

#[tokio::test]
async fn parallel_function_call_argument_deltas_carry_their_item_output_index() {
    let args_delta = |sequence: u64, index: u32, id: &str, delta: &str| {
        json!({
            "type": "response.function_call_arguments.delta",
            "sequence_number": sequence,
            "item_id": id,
            "output_index": index,
            "delta": delta
        })
    };
    let first = function_call_item("fc_a", "call_a", "search", r#"{"q":"a"}"#);
    let second = function_call_item("fc_b", "call_b", "search", r#"{"q":"b"}"#);
    let server = MockServer::start(vec![MockResponse::sse(sse(&[
        created(0),
        added(1, 0, function_call_item("fc_a", "call_a", "search", "")),
        added(2, 1, function_call_item("fc_b", "call_b", "search", "")),
        args_delta(3, 1, "fc_b", r#"{"q":"#),
        args_delta(4, 0, "fc_a", r#"{"q":"#),
        args_delta(5, 0, "fc_a", r#""a"}"#),
        args_delta(6, 1, "fc_b", r#""b"}"#),
        done(7, 0, first.clone()),
        done(8, 1, second.clone()),
        completed(9, vec![first, second], usage_json(10, 0, 2)),
    ]))]);
    let events = collect(&provider(&server), request(vec![user_text("go")])).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");

    let mut fragments: std::collections::BTreeMap<u32, String> = Default::default();
    let mut order = Vec::new();
    for event in &events {
        if let Ok(StreamEvent::PartDelta {
            output_index,
            part_index: 0,
            delta: PartDelta::JsonArgs { fragment },
            ..
        }) = event
        {
            order.push(*output_index);
            fragments
                .entry(*output_index)
                .or_default()
                .push_str(fragment);
        }
    }
    assert_eq!(order, [1, 0, 0, 1]);
    let calls: std::collections::BTreeMap<u32, (String, String)> = completed_items(&events)
        .into_iter()
        .map(|(index, item)| match item {
            Item::ToolCall {
                call_id, raw_args, ..
            } => (index, (call_id, raw_args.as_str().to_owned())),
            other => panic!("expected a tool call, got {other:?}"),
        })
        .collect();
    assert_eq!(calls[&0], ("call_a".to_owned(), fragments[&0].clone()));
    assert_eq!(calls[&1], ("call_b".to_owned(), fragments[&1].clone()));
    assert_eq!(fragments[&0], r#"{"q":"a"}"#);
}

#[tokio::test]
async fn function_call_results_pair_and_arguments_stay_verbatim() {
    let raw_args = r#"{ "city" :  "Paris" }"#;
    let item = function_call_item("fc_1", "call_1", "get_weather", raw_args);
    let server = MockServer::start(vec![
        MockResponse::sse(sse(&[
            created(0),
            added(1, 0, item.clone()),
            done(2, 0, item),
            completed(
                3,
                vec![function_call_item(
                    "fc_1",
                    "call_1",
                    "get_weather",
                    raw_args,
                )],
                usage_json(10, 0, 2),
            ),
        ])),
        MockResponse::sse(completed_text_body("done")),
    ]);

    let mut followup = request(vec![
        user_text("weather?"),
        Item::ToolCall {
            call_id: "call_1".to_owned(),
            provider_item_id: Some("fc_1".to_owned()),
            name: ToolName::new("get_weather"),
            raw_args: JsonString::new(raw_args),
            raw: None,
        },
        Item::ToolResult {
            call_id: "call_1".to_owned(),
            provider_item_id: Some("fc_1".to_owned()),
            parts: vec![Part::Text {
                text: "sunny".to_owned(),
            }],
            is_error: false,
            raw: None,
        },
    ]);
    followup.max_output_tokens = 32;

    let events = collect(&provider(&server), request(vec![user_text("weather?")])).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    let completed = completed_items(&events);
    assert_eq!(
        completed[0].1,
        Item::ToolCall {
            call_id: "call_1".to_owned(),
            provider_item_id: Some("fc_1".to_owned()),
            name: ToolName::new("get_weather"),
            raw_args: JsonString::new(raw_args),
            raw: None,
        }
    );
    assert_eq!(response_ended(&events).0, StopReason::ToolUse);

    let _ = collect(&provider(&server), followup).await;
    let body: Value = serde_json::from_str(&server.requests()[1].body).expect("request json");
    assert_eq!(body["input"][1]["type"], json!("function_call"));
    assert_eq!(body["input"][1]["call_id"], json!("call_1"));
    assert_eq!(body["input"][1]["arguments"], json!(raw_args));
    assert_eq!(body["input"][2]["type"], json!("function_call_output"));
    assert_eq!(body["input"][2]["call_id"], json!("call_1"));
    assert_eq!(body["input"][2]["output"], json!("sunny"));
}

#[tokio::test]
async fn encrypted_reasoning_replays_byte_for_byte_for_matching_binding() {
    let raw_item = r#"{"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"thought"}],"encrypted_content":"gAAA\\n==","extra" : { "z": 1, "a": 2 }}"#;
    let done_payload = format!(
        r#"{{"type":"response.output_item.done","sequence_number":2,"output_index":0,"item":{raw_item}}}"#
    );
    let completed_payload = format!(
        r#"{{"type":"response.completed","sequence_number":3,"response":{{"id":"resp_1","status":"completed","output":[{raw_item}],"usage":{{"input_tokens":10,"output_tokens":2}}}}}}"#
    );
    let mut body = String::new();
    body.push_str(&event(created(0)));
    body.push_str(&event(added(
        1,
        0,
        json!({"type": "reasoning", "id": "rs_1", "summary": []}),
    )));
    body.push_str(&format!("data: {done_payload}\n\n"));
    body.push_str(&format!("data: {completed_payload}\n\n"));
    let server = MockServer::start(vec![
        MockResponse::sse(body),
        MockResponse::sse(completed_text_body("ok")),
    ]);
    let provider = provider(&server);

    let first = collect(&provider, request(vec![user_text("think")])).await;
    let (summary, produced_binding, raw) = completed_items(&first)
        .into_iter()
        .find_map(|(_, item)| match item {
            Item::Reasoning {
                summary,
                binding,
                raw,
            } => Some((summary, binding, raw)),
            _ => None,
        })
        .expect("reasoning item");
    assert_eq!(raw.json.as_str(), raw_item);

    let replay = Item::Reasoning {
        summary,
        binding: produced_binding,
        raw,
    };
    let events = collect(&provider, request(vec![user_text("again"), replay])).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");

    let body = &server.requests()[1].body;
    assert!(
        body.contains(raw_item),
        "raw reasoning should be spliced byte-for-byte, got {body}"
    );
}

#[tokio::test]
async fn reasoning_is_dropped_and_traced_when_binding_does_not_match() {
    let raw_item = r#"{"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":"gAAA"}"#;
    let cases = [
        (
            ReasoningBinding {
                provider: ProviderId::new("other"),
                model: ModelId::new("gpt-5"),
                prefix_epoch: 0,
                tools_hash: "x".to_owned(),
            },
            "cross_provider",
        ),
        (binding("gpt-4", 0, "x"), "model_mismatch"),
        (binding("gpt-5", 1, "x"), "prefix_epoch_mismatch"),
        (binding("gpt-5", 0, "not-the-hash"), "tools_hash_mismatch"),
    ];
    for (binding, reason) in cases {
        let server = MockServer::start(vec![MockResponse::sse(completed_text_body("ok"))]);
        let sink = std::sync::Arc::new(RecordingSink::default());
        let provider = provider(&server).with_trace_sink(sink.clone());
        let events = collect(
            &provider,
            request(vec![user_text("go"), reasoning_item(binding, raw_item)]),
        )
        .await;
        assert!(events.iter().all(Result::is_ok), "{events:?}");
        assert!(
            !server.requests()[0].body.contains(raw_item),
            "mismatched reasoning must not be replayed"
        );
        let marks = sink.marks();
        assert_eq!(marks.len(), 1, "{marks:?}");
        assert_eq!(marks[0].name, TraceMark::DROPPED_ITEMS);
        assert_eq!(marks[0].kind, "reasoning");
        assert_eq!(marks[0].reason, reason);
    }
}

#[tokio::test]
async fn usage_is_exclusive_and_ignores_total_tokens() {
    let item = message_item("msg_1", "ok");
    let usage = json!({
        "input_tokens": 100,
        "input_tokens_details": {"cached_tokens": 30, "cache_write_tokens": 10},
        "output_tokens": 50,
        "output_tokens_details": {"reasoning_tokens": 20},
        "total_tokens": 999_999
    });
    let server = MockServer::start(vec![MockResponse::sse(sse(&[
        created(0),
        added(1, 0, message_item("msg_1", "")),
        done(2, 0, item.clone()),
        completed(3, vec![item], usage),
    ]))]);
    let events = collect(&provider(&server), request(vec![user_text("go")])).await;
    let (_, usage) = response_ended(&events);
    assert_eq!(usage.input.uncached, 60);
    assert_eq!(usage.input.cache_read, 30);
    assert_eq!(usage.input.cache_write, 10);
    assert_eq!(usage.output.total, 50);
    assert_eq!(usage.output.reasoning, 20);
    assert_eq!(usage.total(), 150);
}

#[tokio::test]
async fn usage_missing_fields_are_zero() {
    let item = message_item("msg_1", "ok");
    let server = MockServer::start(vec![MockResponse::sse(sse(&[
        created(0),
        added(1, 0, message_item("msg_1", "")),
        done(2, 0, item.clone()),
        completed(3, vec![item], json!({})),
    ]))]);
    let events = collect(&provider(&server), request(vec![user_text("go")])).await;
    let (_, usage) = response_ended(&events);
    assert_eq!(usage, Usage::default());
}

#[tokio::test]
async fn status_and_incomplete_details_map_to_stop_reason() {
    let text = message_item("msg_1", "ok");
    let call = function_call_item("fc_1", "call_1", "get_weather", "{}");
    let expected = [
        (
            sse(&[
                created(0),
                added(1, 0, message_item("msg_1", "")),
                done(2, 0, text.clone()),
                completed(3, vec![text.clone()], usage_json(1, 0, 1)),
            ]),
            StopReason::EndTurn,
        ),
        (
            sse(&[
                created(0),
                added(1, 0, call.clone()),
                done(2, 0, call.clone()),
                completed(3, vec![call], usage_json(1, 0, 1)),
            ]),
            StopReason::ToolUse,
        ),
        (
            sse(&[
                created(0),
                added(1, 0, message_item("msg_1", "")),
                done(2, 0, text.clone()),
                incomplete(
                    3,
                    vec![text.clone()],
                    "max_output_tokens",
                    usage_json(1, 0, 1),
                ),
            ]),
            StopReason::MaxTokens,
        ),
        (
            sse(&[
                created(0),
                added(1, 0, message_item("msg_1", "")),
                done(2, 0, text.clone()),
                incomplete(3, vec![text], "content_filter", usage_json(1, 0, 1)),
            ]),
            StopReason::Refusal,
        ),
    ];
    for (body, expected) in expected {
        let server = MockServer::start(vec![MockResponse::sse(body)]);
        let events = collect(&provider(&server), request(vec![user_text("go")])).await;
        assert_eq!(response_ended(&events).0, expected, "{events:?}");
    }
}

#[tokio::test]
async fn failed_response_is_an_error_without_response_ended() {
    let server = MockServer::start(vec![MockResponse::sse(sse(&[
        created(0),
        json!({
            "type": "response.failed",
            "sequence_number": 1,
            "response": {
                "id": "resp_1",
                "status": "failed",
                "error": {"code": "server_error", "message": "boom"},
                "usage": {
                    "input_tokens": 12,
                    "input_tokens_details": {"cached_tokens": 2},
                    "output_tokens": 3
                }
            }
        }),
    ]))]);
    let events = collect(&provider(&server), request(vec![user_text("go")])).await;
    let errors: Vec<_> = events
        .iter()
        .filter_map(|event| event.as_ref().err())
        .collect();
    assert_eq!(errors.len(), 1, "{events:?}");
    assert_eq!(errors[0].kind, ProviderErrorKind::Server);
    assert!(errors[0].retryable);
    let partial = errors[0].partial_usage.expect("partial usage");
    assert_eq!(partial.input.uncached, 10);
    assert_eq!(partial.input.cache_read, 2);
    assert_eq!(partial.output.total, 3);
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, Ok(StreamEvent::ResponseEnded { .. })))
    );
}

#[tokio::test]
async fn stream_without_terminal_event_is_a_transport_error() {
    let server = MockServer::start(vec![MockResponse::sse(sse(&[
        created(0),
        added(1, 0, message_item("msg_1", "")),
        done(2, 0, message_item("msg_1", "partial")),
    ]))]);
    let events = collect(&provider(&server), request(vec![user_text("go")])).await;
    let errors: Vec<_> = events
        .iter()
        .filter_map(|event| event.as_ref().err())
        .collect();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].kind, ProviderErrorKind::Transport);
    assert!(errors[0].retryable);
}

#[tokio::test]
async fn sequence_regression_is_a_stream_protocol_error() {
    let server = MockServer::start(vec![MockResponse::sse(sse(&[
        created(2),
        added(1, 0, message_item("msg_1", "")),
    ]))]);
    let events = collect(&provider(&server), request(vec![user_text("go")])).await;
    let errors: Vec<_> = events
        .iter()
        .filter_map(|event| event.as_ref().err())
        .collect();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].kind, ProviderErrorKind::StreamProtocol);
    assert!(!errors[0].retryable);
}

#[tokio::test]
async fn duplicate_item_completion_is_a_stream_protocol_error() {
    let item = message_item("msg_1", "ok");
    let server = MockServer::start(vec![MockResponse::sse(sse(&[
        created(0),
        added(1, 0, item.clone()),
        done(2, 0, item.clone()),
        done(3, 0, item),
    ]))]);
    let events = collect(&provider(&server), request(vec![user_text("go")])).await;
    let errors: Vec<_> = events
        .iter()
        .filter_map(|event| event.as_ref().err())
        .collect();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].kind, ProviderErrorKind::StreamProtocol);
}

#[tokio::test]
async fn tool_result_images_render_as_text_and_are_traced() {
    let server = MockServer::start(vec![MockResponse::sse(completed_text_body("ok"))]);
    let sink = std::sync::Arc::new(RecordingSink::default());
    let provider = provider(&server).with_trace_sink(sink.clone());
    let events = collect(
        &provider,
        request(vec![
            Item::ToolCall {
                call_id: "call_1".to_owned(),
                provider_item_id: None,
                name: ToolName::new("get_weather"),
                raw_args: JsonString::new("{}"),
                raw: None,
            },
            Item::ToolResult {
                call_id: "call_1".to_owned(),
                provider_item_id: None,
                parts: vec![
                    Part::Text {
                        text: "ok".to_owned(),
                    },
                    Part::Image {
                        media_type: "image/png".to_owned(),
                        source: ImageSource::Url {
                            url: "https://example.test/x.png".to_owned(),
                        },
                    },
                ],
                is_error: true,
                raw: None,
            },
        ]),
    )
    .await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    let body: Value = serde_json::from_str(&server.requests()[0].body).expect("request json");
    assert_eq!(
        body["input"][1]["output"],
        json!("[tool error] ok[image omitted: image/png]")
    );
    let marks = sink.marks();
    assert_eq!(marks.len(), 1);
    assert_eq!(marks[0].name, TraceMark::RENDERED_AS_TEXT);
    assert_eq!(marks[0].kind, "image/png");
}

#[tokio::test]
async fn refusal_parts_survive_with_a_refusal_stop_reason() {
    let item = json!({
        "id": "msg_1",
        "type": "message",
        "status": "incomplete",
        "role": "assistant",
        "content": [{"type": "refusal", "refusal": "I cannot do that"}]
    });
    let server = MockServer::start(vec![MockResponse::sse(sse(&[
        created(0),
        added(1, 0, item.clone()),
        done(2, 0, item.clone()),
        incomplete(3, vec![item], "content_filter", usage_json(1, 0, 1)),
    ]))]);
    let events = collect(&provider(&server), request(vec![user_text("go")])).await;
    assert_eq!(
        completed_items(&events)[0].1,
        Item::Message {
            role: Role::Assistant,
            parts: vec![Part::Refusal {
                text: "I cannot do that".to_owned()
            }],
        }
    );
    assert_eq!(response_ended(&events).0, StopReason::Refusal);
}

#[tokio::test]
async fn compaction_renders_as_a_user_text_message() {
    let server = MockServer::start(vec![MockResponse::sse(completed_text_body("ok"))]);
    let events = collect(
        &provider(&server),
        request(vec![Item::Compaction {
            parts: vec![Part::Text {
                text: "summary of earlier turns".to_owned(),
            }],
        }]),
    )
    .await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    let body: Value = serde_json::from_str(&server.requests()[0].body).expect("request json");
    assert_eq!(
        body["input"][0],
        json!({
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "summary of earlier turns"}]
        })
    );
}

#[tokio::test]
async fn provider_opaque_and_server_tools_replay_only_for_the_same_provider() {
    let raw = r#"{"type":"web_search_call","id":"ws_1","status":"completed"}"#;
    let server = MockServer::start(vec![
        MockResponse::sse(completed_text_body("ok")),
        MockResponse::sse(completed_text_body("ok")),
    ]);
    let sink = std::sync::Arc::new(RecordingSink::default());
    let provider = provider(&server).with_trace_sink(sink.clone());

    let same = Item::ProviderOpaque {
        provider: ProviderId::new("openai"),
        kind: "web_search_call".to_owned(),
        raw: raw_data("openai", "openai.opaque", raw),
    };
    let _ = collect(&provider, request(vec![same])).await;
    assert!(server.requests()[0].body.contains(raw));

    let other = Item::ServerToolCall {
        provider: ProviderId::new("other"),
        name: "web_search".to_owned(),
        call_id: Some("ws_2".to_owned()),
        raw: raw_data("other", "other.server_tool_call", raw),
    };
    let _ = collect(&provider, request(vec![other])).await;
    assert!(!server.requests()[1].body.contains(raw));
    let marks = sink.marks();
    assert_eq!(marks.len(), 1);
    assert_eq!(marks[0].name, TraceMark::DROPPED_ITEMS);
    assert_eq!(marks[0].kind, "server_tool_call");
    assert_eq!(marks[0].reason, "cross_provider");
}

#[tokio::test]
async fn unsupported_requests_fail_before_http_without_dropping_fields() {
    let server = MockServer::start(Vec::new());
    let provider = provider(&server);

    let mut top_k = request(vec![user_text("go")]);
    top_k.sampling = Some(asterpath_protocol::SamplingParams {
        top_k: Some(5),
        ..Default::default()
    });
    let mut seed = request(vec![user_text("go")]);
    seed.sampling = Some(asterpath_protocol::SamplingParams {
        seed: Some(7),
        ..Default::default()
    });
    let mut budget = request(vec![user_text("go")]);
    budget.reasoning = Some(asterpath_protocol::ReasoningParams {
        budget_tokens: Some(1024),
        ..Default::default()
    });
    // Responses assistant output content does not accept images.
    let image = request(vec![Item::Message {
        role: Role::Assistant,
        parts: vec![Part::Image {
            media_type: "image/png".to_owned(),
            source: ImageSource::Base64 {
                data: "AAAA".to_owned(),
            },
        }],
    }]);

    for request in [top_k, seed, budget, image] {
        let events = collect(&provider, request).await;
        let errors: Vec<_> = events
            .iter()
            .filter_map(|event| event.as_ref().err())
            .collect();
        assert_eq!(errors.len(), 1, "{events:?}");
        assert_eq!(errors[0].kind, ProviderErrorKind::Unsupported);
        assert!(!errors[0].retryable);
    }
    assert!(
        server.requests().is_empty(),
        "capability failures must not reach HTTP"
    );
}

#[tokio::test]
async fn identical_requests_render_identical_bytes() {
    let server = MockServer::start(vec![
        MockResponse::sse(completed_text_body("ok")),
        MockResponse::sse(completed_text_body("ok")),
    ]);
    let provider = provider(&server);
    let _ = collect(&provider, request(vec![user_text("same")])).await;
    let _ = collect(&provider, request(vec![user_text("same")])).await;
    let bodies = server.requests();
    assert_eq!(bodies[0].body, bodies[1].body);
}

#[tokio::test]
async fn http_errors_are_classified() {
    let server = MockServer::start(vec![
        MockResponse::json(
            429,
            r#"{"error":{"message":"slow down","type":"requests","code":"rate_limit_exceeded"}}"#,
        )
        .header("retry-after", "7")
        .header("x-request-id", "req_1"),
        MockResponse::json(401, r#"{"error":{"message":"bad key"}}"#),
        MockResponse::json(400, r#"{"error":{"code":"context_length_exceeded"}}"#),
    ]);
    let provider = provider(&server);

    let rate = collect(&provider, request(vec![user_text("go")])).await;
    let rate = rate[0].as_ref().expect_err("rate limited");
    assert_eq!(rate.kind, ProviderErrorKind::RateLimited);
    assert!(rate.retryable);
    assert_eq!(rate.retry_after, Some(Duration::from_secs(7)));
    assert_eq!(rate.request_id.as_deref(), Some("req_1"));

    let auth = collect(&provider, request(vec![user_text("go")])).await;
    let auth = auth[0].as_ref().expect_err("auth");
    assert_eq!(auth.kind, ProviderErrorKind::Auth);
    assert!(!auth.retryable);

    let context = collect(&provider, request(vec![user_text("go")])).await;
    let context = context[0].as_ref().expect_err("context");
    assert_eq!(context.kind, ProviderErrorKind::ContextExceeded);
    assert!(!context.retryable);
    assert!(context.context_exceeded());
}

#[tokio::test]
async fn quota_and_spend_limit_429s_are_not_retryable() {
    let bodies = [
        r#"{"error":{"message":"quota","type":"insufficient_quota","code":null}}"#,
        r#"{"error":{"message":"spend","type":"requests","code":"project_spend_limit_exceeded"}}"#,
        r#"{"error":{"message":"spend","type":"requests","code":"organization_spend_limit_exceeded"}}"#,
    ];
    let server = MockServer::start(
        bodies
            .iter()
            .map(|body| MockResponse::json(429, *body))
            .collect(),
    );
    let provider = provider(&server);
    for body in bodies {
        let events = collect(&provider, request(vec![user_text("go")])).await;
        let error = events[0].as_ref().expect_err("quota");
        assert!(!error.retryable, "{body}");
    }
}

fn failed_with_code(code: &str) -> String {
    sse(&[
        created(0),
        json!({
            "type": "response.failed",
            "sequence_number": 1,
            "response": {
                "id": "resp_1",
                "status": "failed",
                "error": {"code": code, "message": "failed"}
            }
        }),
    ])
}

fn stream_error_with_code(code: &str) -> String {
    sse(&[
        created(0),
        json!({
            "type": "error",
            "sequence_number": 1,
            "code": code,
            "message": "failed",
            "param": null
        }),
    ])
}

async fn single_error(body: String) -> ProviderError {
    let server = MockServer::start(vec![MockResponse::sse(body)]);
    let events = collect(&provider(&server), request(vec![user_text("go")])).await;
    let errors: Vec<_> = events.into_iter().filter_map(Result::err).collect();
    assert_eq!(errors.len(), 1, "{errors:?}");
    errors.into_iter().next().expect("one error")
}

#[tokio::test]
async fn failed_and_stream_error_codes_share_one_classification() {
    let timeout = single_error(failed_with_code("vector_store_timeout")).await;
    assert!(timeout.retryable);

    let invalid = single_error(failed_with_code("invalid_prompt")).await;
    assert_eq!(invalid.kind, ProviderErrorKind::InvalidRequest);
    assert!(!invalid.retryable);

    let rate = single_error(stream_error_with_code("rate_limit_exceeded")).await;
    assert_eq!(rate.kind, ProviderErrorKind::RateLimited);
    assert!(rate.retryable);

    let quota = single_error(stream_error_with_code("insufficient_quota")).await;
    assert!(!quota.retryable);

    let unknown = single_error(stream_error_with_code("something_new")).await;
    assert_eq!(unknown.kind, ProviderErrorKind::Server);
    assert!(unknown.retryable);
}

#[tokio::test]
async fn only_server_executed_output_items_are_server_tool_calls() {
    let items = [
        json!({"id": "ws_1", "type": "web_search_call", "status": "completed"}),
        json!({"id": "ct_1", "type": "custom_tool_call", "call_id": "call_1", "name": "x", "input": "y"}),
        json!({"id": "sh_1", "type": "shell_call", "call_id": "call_2", "status": "completed"}),
        json!({"id": "cu_1", "type": "computer_call", "call_id": "call_3", "status": "completed"}),
    ];
    let mut events = vec![created(0)];
    let mut sequence = 1;
    for (index, item) in (0u32..).zip(&items) {
        events.push(added(sequence, index, item.clone()));
        events.push(done(sequence + 1, index, item.clone()));
        sequence += 2;
    }
    events.push(completed(sequence, items.to_vec(), usage_json(1, 0, 1)));
    let server = MockServer::start(vec![MockResponse::sse(sse(&events))]);
    let events = collect(&provider(&server), request(vec![user_text("go")])).await;
    let completed = completed_items(&events);
    assert_eq!(completed.len(), 4, "{events:?}");
    assert!(
        matches!(&completed[0].1, Item::ServerToolCall { name, .. } if name == "web_search_call")
    );
    for (_, item) in &completed[1..] {
        assert!(matches!(item, Item::ProviderOpaque { .. }), "{item:?}");
    }
}

struct OpenAiWire;

impl asterpath_testing::ProviderWire for OpenAiWire {
    fn provider(&self, base_url: &str) -> Box<dyn Provider> {
        Box::new(OpenAiProvider::new("test-key").with_base_url(format!("{base_url}/v1")))
    }

    fn text_response(&self, text: &str) -> Vec<Value> {
        let item = message_item("msg_1", text);
        vec![
            created(0),
            added(1, 0, message_item("msg_1", "")),
            text_delta(2, 0, text),
            done(3, 0, item.clone()),
            completed(4, vec![item], usage_json(10, 0, 2)),
        ]
    }
}

#[tokio::test]
async fn openai_satisfies_the_provider_contract() {
    asterpath_testing::provider_contract(&OpenAiWire).await;
}
