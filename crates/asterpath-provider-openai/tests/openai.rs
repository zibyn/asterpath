use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use asterpath_testing::http_mock::{MockServer, Reply as MockResponse};

use asterpath_protocol::{
    FileSource, ImageSource, Item, JsonString, ModelId, ModelRequest, ModelSpec, Part, PartDelta,
    Provider, ProviderCapabilities, ProviderError, ProviderErrorKind, ProviderFileId, ProviderId,
    RawProviderData, ReasoningBinding, Role, StopReason, StreamEvent, ToolChoice, ToolDescriptor,
    ToolName, Usage,
};
use futures_util::StreamExt;
use serde_json::{Value, json};

use asterpath_provider_openai::{OpenAiModelProfile, OpenAiProvider, TraceMark, TraceSink};

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
    let mut request = ModelRequest::new(
        spec("gpt-5"),
        0,
        vec![Part::Text {
            text: "You are terse.".to_owned(),
        }],
        items,
        vec![tool("get_weather")],
        vec![tool("get_weather")],
        &asterpath_protocol::RequestConfig::default(),
    );
    request.max_output_tokens = 128;
    request
}

fn tool(name: &str) -> ToolDescriptor {
    ToolDescriptor::new(
        ToolName::new(name),
        "Look up weather",
        json!({"type": "object", "properties": {"city": {"type": "string"}}}),
    )
    .with_parallelism(asterpath_protocol::Parallelism::Parallel)
    .with_replay(asterpath_protocol::ReplaySafety::Safe)
    .with_side_effect(asterpath_protocol::SideEffect::ReadOnly)
}

fn user_text(text: &str) -> Item {
    Item::Message {
        role: Role::User,
        parts: vec![Part::Text {
            text: text.to_owned(),
        }],
        raw: None,
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

fn assistant_parts(item: &Item) -> &[Part] {
    match item {
        Item::Message {
            role: Role::Assistant,
            parts,
            ..
        } => parts,
        other => panic!("expected an assistant message, got {other:?}"),
    }
}

fn texts(text: &str) -> Vec<Part> {
    vec![Part::Text {
        text: text.to_owned(),
    }]
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
    assert_eq!(capabilities, {
        let mut capabilities = ProviderCapabilities::default();
        capabilities.forced_tool_choice = true;
        capabilities.sampling = true;
        capabilities.reasoning_roundtrip = true;
        capabilities.structured_output = true;
        capabilities.server_tools = true;
        capabilities.image_input = true;
        capabilities.file_input = true;
        capabilities.parallel_tool_calls = true;
        capabilities
    });
}

fn pdf(source: FileSource) -> Part {
    Part::File {
        media_type: "application/pdf".to_owned(),
        filename: Some("report.pdf".to_owned()),
        source,
    }
}

fn openai_file(id: &str) -> ProviderFileId {
    ProviderFileId::new(ProviderId::new("openai"), id)
}

#[tokio::test]
async fn user_files_render_as_input_file() {
    let server = MockServer::start(vec![MockResponse::sse(completed_text_body("Hi"))]);
    let provider = provider(&server);
    let files = request(vec![Item::Message {
        role: Role::User,
        parts: vec![
            pdf(FileSource::Base64 {
                data: "JVBE".to_owned(),
            }),
            pdf(FileSource::Url {
                url: "https://example.invalid/r.pdf".to_owned(),
            }),
            pdf(FileSource::ProviderFile(openai_file("file-a"))),
            Part::Image {
                media_type: "image/png".to_owned(),
                source: ImageSource::ProviderFile(openai_file("file-b")),
            },
        ],
        raw: None,
    }]);
    let events = collect(&provider, files).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");

    let body: Value = serde_json::from_str(&server.requests()[0].body).expect("request json");
    assert_eq!(
        body["input"][0]["content"],
        json!([
            {"type": "input_file", "filename": "report.pdf", "file_data": "data:application/pdf;base64,JVBE"},
            {"type": "input_file", "filename": "report.pdf", "file_url": "https://example.invalid/r.pdf"},
            {"type": "input_file", "filename": "report.pdf", "file_id": "file-a"},
            {"type": "input_image", "file_id": "file-b"}
        ])
    );
}

#[tokio::test]
async fn tool_results_with_files_render_a_content_array() {
    let server = MockServer::start(vec![MockResponse::sse(completed_text_body("ok"))]);
    let provider = provider(&server);
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
                    pdf(FileSource::ProviderFile(openai_file("file-a"))),
                    Part::Text {
                        text: "partial".to_owned(),
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
        json!([
            {"type": "input_file", "filename": "report.pdf", "file_id": "file-a"},
            {"type": "input_text", "text": "[tool error] partial"},
            {"type": "input_text", "text": "[image omitted: image/png]"}
        ])
    );
}

#[tokio::test]
async fn unrenderable_files_fail_before_http() {
    let server = MockServer::start(Vec::new());
    let other = ProviderFileId::new(ProviderId::new("anthropic"), "file_x");
    let user = |parts| Item::Message {
        role: Role::User,
        parts,
        raw: None,
    };
    let cases = [
        user(vec![Part::File {
            media_type: "application/x-unknown".to_owned(),
            filename: None,
            source: FileSource::Base64 {
                data: "YQ==".to_owned(),
            },
        }]),
        Item::Message {
            role: Role::Assistant,
            parts: vec![pdf(FileSource::Base64 {
                data: "JVBE".to_owned(),
            })],
            raw: None,
        },
        user(vec![pdf(FileSource::ProviderFile(other.clone()))]),
        user(vec![Part::Image {
            media_type: "image/png".to_owned(),
            source: ImageSource::ProviderFile(other.clone()),
        }]),
        Item::ToolResult {
            call_id: "call_1".to_owned(),
            provider_item_id: None,
            parts: vec![pdf(FileSource::ProviderFile(other))],
            is_error: false,
            raw: None,
        },
    ];
    let provider = provider(&server);
    for item in cases {
        let events = collect(&provider, request(vec![item])).await;
        assert!(
            matches!(&events[..], [Err(error)] if error.kind == ProviderErrorKind::Unsupported),
            "{events:?}"
        );
    }

    let no_files = provider
        .with_default_profile(OpenAiModelProfile::latest().with_file_media_types(Vec::new()));
    assert!(!no_files.capabilities(&spec("gpt-5")).file_input);
    let events = collect(
        &no_files,
        request(vec![user(vec![pdf(FileSource::Base64 {
            data: "JVBE".to_owned(),
        })])]),
    )
    .await;
    assert!(
        matches!(&events[..], [Err(error)] if error.kind == ProviderErrorKind::Unsupported),
        "{events:?}"
    );
    assert!(server.requests().is_empty());
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
        raw: None,
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
    let mut sampling = asterpath_protocol::SamplingParams::default();
    sampling.temperature = Some(0.25);
    sampling.top_p = Some(0.5);
    request.sampling = Some(sampling);
    let mut reasoning = asterpath_protocol::ReasoningParams::default();
    reasoning.effort = Some(asterpath_protocol::ReasoningEffort::High);
    reasoning.summary = true;
    request.reasoning = Some(reasoning);
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
async fn output_format_renders_as_strict_text_format_and_tool_strict_only_when_set() {
    let server = MockServer::start(vec![MockResponse::sse(completed_text_body("{}"))]);
    let provider = provider(&server);
    let mut request = request(vec![user_text("hello")]);
    request.output_format = Some(asterpath_protocol::OutputFormat::new(
        "answer",
        json!({"type": "object", "properties": {}, "additionalProperties": false}),
    ));
    let mut strict = tool("strict_tool");
    strict.strict = Some(true);
    let mut loose = tool("loose_tool");
    loose.strict = Some(false);
    request.tools = vec![strict, loose, tool("get_weather")];
    let events = collect(&provider, request).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");

    let body: Value = serde_json::from_str(&server.requests()[0].body).expect("request json");
    assert_eq!(
        body["text"],
        json!({"format": {
            "type": "json_schema",
            "name": "answer",
            "schema": {"type": "object", "properties": {}, "additionalProperties": false},
            "strict": true
        }})
    );
    assert_eq!(body["tools"][0]["strict"], json!(true));
    assert_eq!(body["tools"][1]["strict"], json!(false));
    assert!(body["tools"][2].get("strict").is_none());
}

#[tokio::test]
async fn text_is_omitted_without_output_format() {
    let server = MockServer::start(vec![MockResponse::sse(completed_text_body("Hi"))]);
    let events = collect(&provider(&server), request(vec![user_text("hello")])).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    let body: Value = serde_json::from_str(&server.requests()[0].body).expect("request json");
    assert!(body.get("text").is_none());
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
    assert_eq!(assistant_parts(&by_index[&0]), texts("first"));
    assert_eq!(assistant_parts(&by_index[&1]), texts("second"));

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
async fn reasoning_is_replayed_after_the_tool_set_changes() {
    let raw_item = r#"{"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":"gAAA"}"#;
    let server = MockServer::start(vec![MockResponse::sse(completed_text_body("ok"))]);
    let events = collect(
        &provider(&server),
        request(vec![
            user_text("go"),
            reasoning_item(binding("gpt-5", 0, "an-older-tool-set"), raw_item),
        ]),
    )
    .await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    assert!(
        server.requests()[0].body.contains(raw_item),
        "reasoning from a different tool set must still be replayed"
    );
}

fn reasoning_wire_item(id: &str, summary: &[&str], content: &[&str]) -> Value {
    json!({
        "id": id,
        "type": "reasoning",
        "summary": summary.iter().map(|text| json!({"type": "summary_text", "text": text})).collect::<Vec<_>>(),
        "content": content.iter().map(|text| json!({"type": "reasoning_text", "text": text})).collect::<Vec<_>>()
    })
}

fn reasoning_delta(sequence: u64, index: u32, kind: &str, part: u32, text: &str) -> Value {
    let (event_type, index_key) = match kind {
        "summary" => ("response.reasoning_summary_text.delta", "summary_index"),
        _ => ("response.reasoning_text.delta", "content_index"),
    };
    json!({
        "type": event_type,
        "sequence_number": sequence,
        "item_id": "rs",
        "output_index": index,
        index_key: part,
        "delta": text
    })
}

/// `(output_index, part_index, text)` for every reasoning delta, in order.
fn reasoning_deltas(events: &[Result<StreamEvent, ProviderError>]) -> Vec<(u32, u32, String)> {
    events
        .iter()
        .filter_map(|event| match event {
            Ok(StreamEvent::PartDelta {
                output_index,
                part_index,
                delta: PartDelta::ReasoningSummary { text },
                ..
            }) => Some((*output_index, *part_index, text.clone())),
            _ => None,
        })
        .collect()
}

fn reasoning_summary(item: &Item) -> Vec<Part> {
    match item {
        Item::Reasoning { summary, .. } => summary.clone(),
        other => panic!("expected reasoning, got {other:?}"),
    }
}

#[tokio::test]
async fn reasoning_text_deltas_surface_as_reasoning_summary() {
    let item = reasoning_wire_item("rs_1", &[], &["think", "ing"]);
    let server = MockServer::start(vec![MockResponse::sse(sse(&[
        created(0),
        added(1, 0, reasoning_wire_item("rs_1", &[], &[])),
        reasoning_delta(2, 0, "text", 0, "thi"),
        reasoning_delta(3, 0, "text", 0, "nk"),
        reasoning_delta(4, 0, "text", 1, "ing"),
        done(5, 0, item.clone()),
        completed(6, vec![item], usage_json(10, 0, 2)),
    ]))]);
    let events = collect(&provider(&server), request(vec![user_text("go")])).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");

    let deltas = reasoning_deltas(&events);
    assert_eq!(
        deltas,
        [
            (0, 0, "thi".to_owned()),
            (0, 0, "nk".to_owned()),
            (0, 1, "ing".to_owned())
        ]
    );
    let summary = reasoning_summary(&completed_items(&events)[0].1);
    let joined: String = deltas.iter().map(|(_, _, text)| text.as_str()).collect();
    let summary_text: String = summary
        .iter()
        .map(|part| match part {
            Part::Text { text } => text.as_str(),
            other => panic!("unexpected part {other:?}"),
        })
        .collect();
    assert_eq!(summary_text, joined);
    assert_eq!(summary, [texts("think"), texts("ing")].concat());
}

#[tokio::test]
async fn the_first_reasoning_delta_kind_locks_each_item_independently() {
    let summary_first = reasoning_wire_item("rs_a", &["from summary"], &["from content"]);
    let text_first = reasoning_wire_item("rs_b", &["from summary"], &["from content"]);
    let server = MockServer::start(vec![MockResponse::sse(sse(&[
        created(0),
        added(1, 0, reasoning_wire_item("rs_a", &[], &[])),
        added(2, 1, reasoning_wire_item("rs_b", &[], &[])),
        reasoning_delta(3, 0, "summary", 0, "s0"),
        reasoning_delta(4, 1, "text", 0, "t1"),
        reasoning_delta(5, 0, "text", 0, "dropped"),
        reasoning_delta(6, 1, "summary", 0, "dropped"),
        done(7, 0, summary_first.clone()),
        done(8, 1, text_first.clone()),
        completed(9, vec![summary_first, text_first], usage_json(10, 0, 2)),
    ]))]);
    let events = collect(&provider(&server), request(vec![user_text("go")])).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");

    assert_eq!(
        reasoning_deltas(&events),
        [(0, 0, "s0".to_owned()), (1, 0, "t1".to_owned())]
    );
    let by_index: std::collections::BTreeMap<u32, Item> =
        completed_items(&events).into_iter().collect();
    assert_eq!(reasoning_summary(&by_index[&0]), texts("from summary"));
    assert_eq!(reasoning_summary(&by_index[&1]), texts("from content"));
}

#[tokio::test]
async fn reasoning_without_deltas_prefers_summary_then_falls_back_to_content() {
    let mut content_only = reasoning_wire_item("rs_a", &[], &["from content"]);
    content_only["content"]
        .as_array_mut()
        .expect("content")
        .push(json!({"type": "other_text", "text": "not reasoning"}));
    let both = reasoning_wire_item("rs_b", &["from summary"], &["from content"]);
    let neither = reasoning_wire_item("rs_c", &[], &[]);
    let server = MockServer::start(vec![MockResponse::sse(sse(&[
        created(0),
        added(1, 0, content_only.clone()),
        added(2, 1, both.clone()),
        added(3, 2, neither.clone()),
        done(4, 0, content_only.clone()),
        done(5, 1, both.clone()),
        done(6, 2, neither.clone()),
        completed(7, vec![content_only, both, neither], usage_json(10, 0, 2)),
    ]))]);
    let events = collect(&provider(&server), request(vec![user_text("go")])).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");

    let by_index: std::collections::BTreeMap<u32, Item> =
        completed_items(&events).into_iter().collect();
    assert_eq!(reasoning_summary(&by_index[&0]), texts("from content"));
    assert_eq!(reasoning_summary(&by_index[&1]), texts("from summary"));
    assert_eq!(reasoning_summary(&by_index[&2]), []);
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
        assistant_parts(&completed_items(&events)[0].1),
        [Part::Refusal {
            text: "I cannot do that".to_owned()
        }]
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
async fn an_instruction_renders_as_a_developer_message_in_place() {
    let server = MockServer::start(vec![MockResponse::sse(completed_text_body("ok"))]);
    let mut request = request(vec![
        user_text("hi"),
        Item::Instruction {
            key: None,
            parts: texts("answer in French"),
        },
    ]);
    request.system = texts("be helpful");
    let events = collect(&provider(&server), request).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    let body: Value = serde_json::from_str(&server.requests()[0].body).expect("request json");
    assert_eq!(
        body["instructions"], "be helpful",
        "the baseline is unchanged"
    );
    assert_eq!(
        body["input"][1],
        json!({
            "type": "message",
            "role": "developer",
            "content": [{"type": "input_text", "text": "answer in French"}]
        })
    );
}

#[tokio::test]
async fn a_tool_change_sends_the_effective_tools_and_renders_no_item() {
    let raw_item = r#"{"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":"gAAA"}"#;
    let server = MockServer::start(vec![MockResponse::sse(completed_text_body("ok"))]);
    let mut request = request(vec![
        user_text("hi"),
        reasoning_item(binding("gpt-5", 0, "an-older-tool-set"), raw_item),
        Item::ToolsChanged {
            added: vec![tool("get_time")],
            removed: vec![ToolName::new("get_weather")],
        },
    ]);
    request.tools = vec![tool("get_time")];
    let events = collect(&provider(&server), request).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    let body: Value = serde_json::from_str(&server.requests()[0].body).expect("request json");
    let names: Vec<&Value> = body["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .map(|tool| &tool["name"])
        .collect();
    assert_eq!(
        names,
        [&json!("get_time")],
        "top-level tools are the effective set"
    );
    assert_eq!(body["input"].as_array().expect("input").len(), 2);
    assert!(
        server.requests()[0].body.contains(raw_item),
        "reasoning is replayed"
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
    let mut sampling = asterpath_protocol::SamplingParams::default();
    sampling.top_k = Some(5);
    top_k.sampling = Some(sampling);
    let mut seed = request(vec![user_text("go")]);
    let mut sampling = asterpath_protocol::SamplingParams::default();
    sampling.seed = Some(7);
    seed.sampling = Some(sampling);
    let mut budget = request(vec![user_text("go")]);
    let mut reasoning = asterpath_protocol::ReasoningParams::default();
    reasoning.budget_tokens = Some(1024);
    budget.reasoning = Some(reasoning);
    // Responses assistant output content does not accept images.
    let image = request(vec![Item::Message {
        role: Role::Assistant,
        parts: vec![Part::Image {
            media_type: "image/png".to_owned(),
            source: ImageSource::Base64 {
                data: "AAAA".to_owned(),
            },
        }],
        raw: None,
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

    for code in [
        "bio_policy",
        "misalignment_policy_violation",
        "data_residency_mismatch",
    ] {
        let policy = single_error(stream_error_with_code(code)).await;
        assert_eq!(policy.kind, ProviderErrorKind::InvalidRequest, "{code}");
        assert!(!policy.retryable, "{code}");
    }
}

#[tokio::test]
async fn only_server_executed_output_items_are_server_tool_calls() {
    let items = [
        json!({"id": "ws_1", "type": "web_search_call", "status": "completed"}),
        json!({"id": "ct_1", "type": "custom_tool_call", "call_id": "call_1", "name": "x", "input": "y"}),
        json!({"id": "sh_1", "type": "shell_call", "call_id": "call_2", "status": "completed"}),
        json!({"id": "cu_1", "type": "computer_call", "call_id": "call_3", "status": "completed"}),
        json!({"id": "ts_1", "type": "tool_search_output", "call_id": "call_4"}),
        json!({"id": "x_1", "type": "future_tool_result", "call_id": "call_5"}),
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
    assert_eq!(completed.len(), 6, "{events:?}");
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

fn assistant_with_raw(text: &str, raw: Option<RawProviderData>) -> Item {
    Item::Message {
        role: Role::Assistant,
        parts: vec![Part::Text {
            text: text.to_owned(),
        }],
        raw,
    }
}

async fn rendered_input(item: Item) -> Value {
    let server = MockServer::start(vec![MockResponse::sse(completed_text_body("ok"))]);
    let events = collect(&provider(&server), request(vec![item])).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    let body: Value = serde_json::from_str(&server.requests()[0].body).expect("request json");
    body["input"][0].clone()
}

#[tokio::test]
async fn message_phase_round_trips_to_the_same_provider_without_its_id() {
    let mut item = message_item("msg_1", "Looking into it");
    item["phase"] = json!("commentary");
    let server = MockServer::start(vec![MockResponse::sse(sse(&[
        created(0),
        added(1, 0, item.clone()),
        done(2, 0, item.clone()),
        completed(3, vec![item.clone()], usage_json(10, 0, 2)),
    ]))]);
    let events = collect(&provider(&server), request(vec![user_text("hi")])).await;
    let (_, parsed) = completed_items(&events).remove(0);
    let Item::Message { raw: Some(raw), .. } = &parsed else {
        panic!("message carries raw: {parsed:?}");
    };
    assert_eq!(raw.provider, ProviderId::new("openai"));
    assert_eq!(
        serde_json::from_str::<Value>(raw.json.as_str()).unwrap(),
        item
    );

    let rendered = rendered_input(parsed).await;
    assert_eq!(rendered["phase"], json!("commentary"));
    assert!(rendered.get("id").is_none(), "{rendered}");
    assert_eq!(rendered["content"][0]["text"], json!("Looking into it"));
}

#[tokio::test]
async fn message_raw_without_phase_renders_like_no_raw() {
    let raw = raw_data(
        "openai",
        "openai.message",
        &message_item("msg_1", "old").to_string(),
    );
    assert_eq!(
        rendered_input(assistant_with_raw("hello", Some(raw))).await,
        rendered_input(assistant_with_raw("hello", None)).await
    );
}

#[tokio::test]
async fn message_raw_with_a_null_phase_renders_like_no_raw() {
    let mut item = message_item("msg_1", "old");
    item["phase"] = Value::Null;
    let raw = raw_data("openai", "openai.message", &item.to_string());
    assert_eq!(
        rendered_input(assistant_with_raw("hello", Some(raw))).await,
        rendered_input(assistant_with_raw("hello", None)).await
    );
}

#[tokio::test]
async fn rewritten_message_text_wins_over_raw() {
    let mut item = message_item("msg_1", "old text");
    item["phase"] = json!("final_answer");
    let raw = raw_data("openai", "openai.message", &item.to_string());
    let rendered = rendered_input(assistant_with_raw("new text", Some(raw))).await;
    assert_eq!(rendered["content"][0]["text"], json!("new text"));
    assert_eq!(rendered["phase"], json!("final_answer"));
    assert!(!rendered.to_string().contains("old text"));
}

#[tokio::test]
async fn message_raw_from_another_provider_is_ignored() {
    let mut item = message_item("msg_1", "hello");
    item["phase"] = json!("commentary");
    let raw = raw_data("other", "other.message", &item.to_string());
    assert_eq!(
        rendered_input(assistant_with_raw("hello", Some(raw))).await,
        rendered_input(assistant_with_raw("hello", None)).await
    );
}

fn with_effort(model: &str, effort: asterpath_protocol::ReasoningEffort) -> ModelRequest {
    let mut request = request(vec![user_text("hello")]);
    request.spec = spec(model);
    let mut reasoning = asterpath_protocol::ReasoningParams::default();
    reasoning.effort = Some(effort);
    request.reasoning = Some(reasoning);
    request
}

#[tokio::test]
async fn every_effort_renders_its_wire_value_on_a_model_that_accepts_it() {
    use asterpath_protocol::ReasoningEffort::{High, Low, Max, Medium, Minimal, Off, XHigh};
    let cases = [
        ("gpt-6-luna", Off, "none"),
        ("gpt-5", Minimal, "minimal"),
        ("gpt-6.1-sol", Low, "low"),
        ("gpt-6.1-sol", Medium, "medium"),
        ("gpt-5", High, "high"),
        ("gpt-5.5", XHigh, "xhigh"),
        ("gpt-6-astra", Max, "max"),
    ];
    let server = MockServer::start(
        cases
            .iter()
            .map(|_| MockResponse::sse(completed_text_body("Hi")))
            .collect(),
    );
    let provider = provider(&server);
    for (model, effort, _) in cases {
        let events = collect(&provider, with_effort(model, effort)).await;
        assert!(
            events.iter().all(Result::is_ok),
            "{model} {effort:?}: {events:?}"
        );
    }
    let recorded = server.requests();
    assert_eq!(recorded.len(), cases.len());
    for (recorded, (model, _, wire)) in recorded.iter().zip(cases) {
        let body: Value = serde_json::from_str(&recorded.body).expect("request json");
        assert_eq!(body["reasoning"], json!({"effort": wire}), "{model}");
    }
}

#[tokio::test]
async fn efforts_a_model_does_not_accept_are_unsupported_before_http() {
    use asterpath_protocol::ReasoningEffort::{High, Low, Max, Minimal, Off, XHigh};
    let server = MockServer::start(Vec::new());
    let provider = provider(&server);
    for (model, effort) in [
        ("gpt-6.1-sol", Off),
        ("gpt-6.1-sol", Minimal),
        ("gpt-6-astra", Off),
        ("gpt-5", Off),
        ("gpt-5", XHigh),
        ("gpt-5.5", Max),
        ("gpt-5.1", Minimal),
        ("gpt-5-pro", Low),
        ("gpt-5.5-pro", Low),
        ("gpt-5.4-pro", Off),
        // Unknown ids get the newest rules (GPT-6.1 Sol): no `none`.
        ("gpt-next", Off),
    ] {
        let events = collect(&provider, with_effort(model, effort)).await;
        let error = events[0].as_ref().expect_err("unsupported");
        assert_eq!(
            error.kind,
            ProviderErrorKind::Unsupported,
            "{model} {effort:?}"
        );
        assert!(error.message.contains(&format!("{effort:?}")), "{error:?}");
    }
    assert!(server.requests().is_empty(), "rejected before any HTTP");

    let server = MockServer::start(vec![MockResponse::sse(completed_text_body("Hi"))]);
    let events = collect(&self::provider(&server), with_effort("gpt-next", High)).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
}

#[tokio::test]
async fn same_provider_options_merge_into_the_top_level_body() {
    let server = MockServer::start(vec![MockResponse::sse(completed_text_body("Hi"))]);
    let mut request = request(vec![user_text("hello")]);
    request.provider_options.insert(
        ProviderId::new("openai"),
        JsonString::new(r#"{"prompt_cache_key":"session-1","service_tier":"flex"}"#),
    );
    request.provider_options.insert(
        ProviderId::new("anthropic"),
        JsonString::new(r#"{"metadata":{"user_id":"u-1"}}"#),
    );
    let events = collect(&provider(&server), request).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");

    let body: Value = serde_json::from_str(&server.requests()[0].body).expect("request json");
    assert_eq!(body["prompt_cache_key"], json!("session-1"));
    assert_eq!(body["service_tier"], json!("flex"));
    assert!(body.get("metadata").is_none(), "other provider ignored");
    assert_eq!(body["store"], json!(false));
}

#[tokio::test]
async fn provider_options_can_set_reasoning_summary_alongside_the_typed_effort() {
    let server = MockServer::start(vec![MockResponse::sse(completed_text_body("Hi"))]);
    let mut request = with_effort("gpt-5", asterpath_protocol::ReasoningEffort::High);
    request.provider_options.insert(
        ProviderId::new("openai"),
        JsonString::new(r#"{"reasoning":{"summary":"detailed"}}"#),
    );
    let events = collect(&provider(&server), request).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");

    let body: Value = serde_json::from_str(&server.requests()[0].body).expect("request json");
    assert_eq!(
        body["reasoning"],
        json!({"effort": "high", "summary": "detailed"})
    );
}

#[tokio::test]
async fn provider_options_may_not_replace_the_typed_reasoning_summary() {
    let server = MockServer::start(Vec::new());
    let mut request = with_effort("gpt-5", asterpath_protocol::ReasoningEffort::High);
    if let Some(reasoning) = request.reasoning.as_mut() {
        reasoning.summary = true;
    }
    request.provider_options.insert(
        ProviderId::new("openai"),
        JsonString::new(r#"{"reasoning":{"summary":"detailed"}}"#),
    );
    let events = collect(&provider(&server), request).await;
    let error = events[0].as_ref().expect_err("rejected");
    assert_eq!(error.kind, ProviderErrorKind::Unsupported);
    assert!(error.message.contains("reasoning.summary"), "{error:?}");
    assert!(server.requests().is_empty(), "rejected before any HTTP");
}

#[tokio::test]
async fn bad_provider_options_and_stop_sequences_fail_before_http() {
    let server = MockServer::start(Vec::new());
    let provider = provider(&server);
    let with_options = |options: &str| {
        let mut request = request(vec![user_text("go")]);
        request
            .provider_options
            .insert(ProviderId::new("openai"), JsonString::new(options));
        request
    };
    let mut stop = request(vec![user_text("go")]);
    stop.stop_sequences = vec!["END".to_owned()];

    for (request, kind) in [
        (
            with_options(r#"{"store":true}"#),
            ProviderErrorKind::Unsupported,
        ),
        (
            with_options(r#"{"input":[]}"#),
            ProviderErrorKind::Unsupported,
        ),
        (
            with_options(r#"{"previous_response_id":"resp_1"}"#),
            ProviderErrorKind::Unsupported,
        ),
        (
            with_options(r#"{"conversation":"conv_1"}"#),
            ProviderErrorKind::Unsupported,
        ),
        (
            with_options(r#"{"tool_choice":{"type":"none"}}"#),
            ProviderErrorKind::Unsupported,
        ),
        (with_options(r#""flex""#), ProviderErrorKind::InvalidRequest),
        (stop, ProviderErrorKind::Unsupported),
    ] {
        let events = collect(&provider, request).await;
        assert_eq!(events.len(), 1, "{events:?}");
        let error = events[0].as_ref().expect_err("rejected");
        assert_eq!(error.kind, kind, "{error:?}");
        assert!(!error.retryable);
    }
    assert!(server.requests().is_empty(), "rejected before any HTTP");
}

fn deepseek_effort(model: &str, effort: asterpath_protocol::ReasoningEffort) -> ModelRequest {
    let mut request = with_effort(model, effort);
    request.spec.provider = ProviderId::new("deepseek");
    request
}

#[tokio::test]
async fn with_id_names_the_instance_and_gates_replay_by_it() {
    let server = MockServer::start(vec![MockResponse::sse(completed_text_body("Hi"))]);
    let deepseek = provider(&server).with_id("deepseek");
    assert_eq!(deepseek.id(), ProviderId::new("deepseek"));

    let tools_hash = asterpath_protocol::tools_hash(&[tool("get_weather")], &[]);
    let mut request = request(vec![
        reasoning_item(
            binding("deepseek-flash", 0, &tools_hash),
            r#"{"type":"reasoning","id":"rs_openai","encrypted_content":"from-openai"}"#,
        ),
        user_text("hello"),
    ]);
    request.spec = spec("deepseek-flash");
    request.spec.provider = ProviderId::new("deepseek");
    let events = collect(&deepseek, request).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    let body = &server.requests()[0].body;
    assert!(!body.contains("from-openai"), "{body}");
}

#[tokio::test]
async fn an_injected_profile_allows_off_and_sets_capabilities() {
    use asterpath_protocol::ReasoningEffort::{High, Low, Off};
    let server = MockServer::start(Vec::new());
    let uninjected = provider(&server).with_id("deepseek");
    let events = collect(&uninjected, deepseek_effort("deepseek-flash", Off)).await;
    assert_eq!(
        events[0].as_ref().expect_err("unsupported").kind,
        ProviderErrorKind::Unsupported
    );
    assert!(server.requests().is_empty());

    let server = MockServer::start(vec![
        MockResponse::sse(completed_text_body("Hi")),
        MockResponse::sse(completed_text_body("Hi")),
        MockResponse::sse(completed_text_body("Hi")),
    ]);
    let injected = provider(&server)
        .with_id("deepseek")
        .with_default_profile(
            OpenAiModelProfile::latest()
                .with_efforts(vec![Off, Low, High])
                .with_sampling(false),
        )
        .with_model_profile(
            "deepseek-v4-pro",
            OpenAiModelProfile::latest()
                .with_efforts(vec![Low])
                .with_forced_tool_choice(false),
        )
        // An application entry overrides an official id.
        .with_model_profile(
            "gpt-6.1-sol",
            OpenAiModelProfile::latest().with_efforts(vec![Off]),
        );
    let events = collect(&injected, deepseek_effort("deepseek-flash", Off)).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    let body: Value = serde_json::from_str(&server.requests()[0].body).unwrap();
    assert_eq!(body["reasoning"], json!({"effort": "none"}));

    // The model entry wins over the instance default.
    let events = collect(&injected, deepseek_effort("deepseek-v4-pro", Off)).await;
    assert_eq!(
        events[0].as_ref().expect_err("unsupported").kind,
        ProviderErrorKind::Unsupported
    );
    let events = collect(&injected, deepseek_effort("gpt-6.1-sol", Off)).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    // The instance default beats the built-in row (`gpt-5` has no `none`).
    let events = collect(&injected, deepseek_effort("gpt-5", Off)).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");

    let flash = injected.capabilities(&spec("deepseek-flash"));
    assert!(flash.forced_tool_choice);
    assert!(!flash.sampling);
    let pro = injected.capabilities(&spec("deepseek-v4-pro"));
    assert!(!pro.forced_tool_choice);
    assert!(pro.sampling);
}

const WEB_SEARCH: &str = r#"{"type":"web_search","search_context_size":"low"}"#;

fn with_web_search(mut request: ModelRequest) -> ModelRequest {
    request
        .server_tools
        .insert(ProviderId::new("openai"), vec![JsonString::new(WEB_SEARCH)]);
    request.server_tools.insert(
        ProviderId::new("anthropic"),
        vec![JsonString::new(
            r#"{"type":"web_search_20250305","name":"web_search"}"#,
        )],
    );
    request
}

#[tokio::test]
async fn server_tools_append_after_function_tools_and_enter_the_binding() {
    let raw_item = r#"{"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":"gAAA"}"#;
    let mut body = String::new();
    body.push_str(&event(created(0)));
    body.push_str(&event(added(
        1,
        0,
        json!({"type": "reasoning", "id": "rs_1", "summary": []}),
    )));
    body.push_str(&format!(
        r#"data: {{"type":"response.output_item.done","sequence_number":2,"output_index":0,"item":{raw_item}}}"#
    ));
    body.push_str("\n\n");
    body.push_str(&format!(
        r#"data: {{"type":"response.completed","sequence_number":3,"response":{{"id":"resp_1","status":"completed","output":[{raw_item}],"usage":{{"input_tokens":1,"output_tokens":1}}}}}}"#
    ));
    body.push_str("\n\n");
    let server = MockServer::start(vec![MockResponse::sse(body)]);

    let events = collect(
        &provider(&server),
        with_web_search(request(vec![user_text("search")])),
    )
    .await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");

    let sent: Value = serde_json::from_str(&server.requests()[0].body).expect("request json");
    let tools = sent["tools"].as_array().expect("tools");
    assert_eq!(tools.len(), 2, "{sent}");
    assert_eq!(tools[0]["name"], json!("get_weather"));
    assert_eq!(tools[1], serde_json::from_str::<Value>(WEB_SEARCH).unwrap());

    let binding = completed_items(&events)
        .into_iter()
        .find_map(|(_, item)| match item {
            Item::Reasoning { binding, .. } => Some(binding),
            _ => None,
        })
        .expect("reasoning item");
    assert_eq!(
        binding.tools_hash,
        asterpath_protocol::tools_hash(
            &[tool("get_weather")],
            &[serde_json::from_str(WEB_SEARCH).unwrap()]
        )
    );
}

#[tokio::test]
async fn server_tools_fail_before_http_when_unsupported_or_not_objects() {
    let server = MockServer::start(Vec::new());
    let unsupported = with_web_search(request(vec![user_text("hi")]));
    let provider = provider(&server).with_model_profile(
        "gpt-5",
        OpenAiModelProfile::latest().with_server_tools(false),
    );
    let events = collect(&provider, unsupported).await;
    assert!(
        matches!(&events[..], [Err(error)] if error.kind == ProviderErrorKind::Unsupported),
        "{events:?}"
    );

    let mut not_object = request(vec![user_text("hi")]);
    not_object
        .server_tools
        .insert(ProviderId::new("openai"), vec![JsonString::new("1")]);
    let events = collect(&self::provider(&server), not_object).await;
    assert!(
        matches!(&events[..], [Err(error)] if error.kind == ProviderErrorKind::InvalidRequest),
        "{events:?}"
    );
    assert!(server.requests().is_empty());
}

#[test]
fn server_tools_are_advertised_for_listed_models_only() {
    let provider = OpenAiProvider::new("key");
    assert!(provider.capabilities(&spec("gpt-5.5")).server_tools);
    assert!(provider.capabilities(&spec("o3")).server_tools);
    assert!(!provider.capabilities(&spec("o1")).server_tools);
    assert!(!provider.capabilities(&spec("deepseek-flash")).server_tools);
}

#[tokio::test]
async fn unrecognized_model_ids_have_no_file_input_or_structured_output() {
    // protocol.md §5: unknown means false.
    let server = MockServer::start(Vec::new());
    let provider = provider(&server);
    let capabilities = provider.capabilities(&spec("deepseek-flash"));
    assert!(!capabilities.file_input);
    assert!(!capabilities.structured_output);

    let mut files = request(vec![Item::Message {
        role: Role::User,
        parts: vec![pdf(FileSource::Base64 {
            data: "JVBE".to_owned(),
        })],
        raw: None,
    }]);
    files.spec = spec("deepseek-flash");
    let events = collect(&provider, files).await;
    assert!(
        matches!(&events[..], [Err(error)] if error.kind == ProviderErrorKind::Unsupported),
        "{events:?}"
    );
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn output_format_is_unsupported_before_http_where_the_model_lacks_it() {
    let server = MockServer::start(Vec::new());
    let provider = provider(&server);
    // The model pages for gpt-5.4-pro, gpt-5.2-pro and o1-mini do not list
    // structured outputs; unrecognized ids are not assumed to have them.
    for model in [
        "gpt-5.4-pro",
        "gpt-5.2-pro-2026-01-01",
        "o1-mini",
        "deepseek-flash",
    ] {
        assert!(
            !provider.capabilities(&spec(model)).structured_output,
            "{model}"
        );
        let mut request = request(vec![user_text("hello")]);
        request.spec = spec(model);
        request.output_format = Some(asterpath_protocol::OutputFormat::new(
            "answer",
            json!({"type": "object"}),
        ));
        let events = collect(&provider, request).await;
        assert!(
            matches!(&events[..], [Err(error)] if error.kind == ProviderErrorKind::Unsupported),
            "{model}: {events:?}"
        );
    }
    assert!(server.requests().is_empty());
    for model in ["gpt-6.1-sol", "gpt-5.5-pro", "gpt-5", "o1", "o3", "o4-mini"] {
        assert!(
            provider.capabilities(&spec(model)).structured_output,
            "{model}"
        );
    }
}
