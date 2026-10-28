//! Request rendering: `ModelRequest` -> Anthropic Messages HTTP body.

mod common;

use asterpath_protocol::{
    CacheLifetime, CacheTtl, FileSource, ImageSource, Item, JsonString, ModelId, ModelRequest,
    Part, Provider, ProviderErrorKind, ProviderFileId, ProviderId, RawProviderData,
    ReasoningBinding, ReasoningEffort, ReasoningParams, RequestConfig, Role, SamplingParams,
    ToolChoice, ToolName,
};
use asterpath_provider_anthropic::{
    AnthropicModelProfile, AnthropicProvider, TraceMark, TraceSink,
};
use common::{
    MockServer, Reply, base_request, descriptor, drain, empty_sse, model, provider, text,
};
use std::sync::{Arc, Mutex, PoisonError};

fn request_for(model: &str) -> ModelRequest {
    let mut request = base_request();
    request.spec.model = ModelId::new(model);
    request
}

fn effort(effort: ReasoningEffort) -> Option<ReasoningParams> {
    let mut params = ReasoningParams::default();
    params.effort = Some(effort);
    Some(params)
}

fn budget(tokens: u32) -> Option<ReasoningParams> {
    let mut params = ReasoningParams::default();
    params.budget_tokens = Some(tokens);
    Some(params)
}

fn user_message(parts: Vec<Part>) -> Item {
    Item::Message {
        role: Role::User,
        parts,
        raw: None,
    }
}

fn assistant_message(parts: Vec<Part>) -> Item {
    Item::Message {
        role: Role::Assistant,
        parts,
        raw: None,
    }
}

async fn one_request(
    server: &MockServer,
    request: ModelRequest,
) -> asterpath_protocol::ProviderError {
    let provider = provider(server);
    let events = drain(&provider, request).await;
    match events.into_iter().next().expect("one event") {
        Err(error) => error,
        Ok(event) => panic!("expected an error, got {event:?}"),
    }
}

#[tokio::test]
async fn renders_system_tools_sampling_tool_choice_and_thinking() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let mut request = request_for("claude-sonnet-4-5");
    request.system = vec![text("be helpful"), text("be terse")];
    request.items = vec![user_message(vec![text("hi")])];
    request.tools = vec![descriptor("search")];
    request.baseline_tools = request.tools.clone();
    request.tool_choice = ToolChoice::Required;
    request.disable_parallel_tool_use = true;
    let mut sampling = SamplingParams::default();
    sampling.temperature = Some(0.4);
    sampling.top_p = Some(0.9);
    sampling.top_k = Some(5);
    request.sampling = Some(sampling);
    let mut reasoning = ReasoningParams::default();
    reasoning.budget_tokens = Some(2048);
    request.reasoning = Some(reasoning);
    request.max_output_tokens = 512;

    drain(&provider(&server), request).await;
    let recorded = server.requests();
    assert_eq!(recorded.len(), 1);
    assert!(
        recorded[0]
            .head
            .to_ascii_lowercase()
            .contains("x-api-key: test-key"),
        "API key header sent: {}",
        recorded[0].head
    );
    assert!(
        recorded[0]
            .head
            .to_ascii_lowercase()
            .contains("anthropic-version: 2023-06-01")
    );

    let body = recorded[0].json_body();
    assert_eq!(body["model"], "claude-sonnet-4-5");
    assert_eq!(body["max_tokens"], 512);
    assert_eq!(body["stream"], true);
    assert_eq!(
        body["system"],
        serde_json::json!([
            {"type": "text", "text": "be helpful"},
            {"type": "text", "text": "be terse", "cache_control": {"type": "ephemeral"}}
        ])
    );
    assert_eq!(body["tools"][0]["name"], "search");
    assert_eq!(body["tools"][0]["description"], "search tool");
    assert_eq!(body["tools"][0]["input_schema"]["type"], "object");
    assert_eq!(
        body["tools"][0]["input_schema"]["properties"]["q"]["type"],
        "string"
    );
    assert_eq!(
        body["tool_choice"],
        serde_json::json!({"type": "any", "disable_parallel_tool_use": true})
    );
    assert_eq!(body["temperature"], 0.4);
    assert_eq!(body["top_p"], 0.9);
    assert_eq!(body["top_k"], 5);
    assert_eq!(
        body["thinking"],
        serde_json::json!({"type": "enabled", "budget_tokens": 2048})
    );
    assert!(body.get("seed").is_none(), "seed has no Anthropic mapping");
    assert!(body.get("output_config").is_none());
    assert_eq!(
        body["messages"],
        serde_json::json!([{"role": "user", "content": [
            {"type": "text", "text": "hi", "cache_control": {"type": "ephemeral"}}
        ]}])
    );
    assert!(
        body["messages"]
            .as_array()
            .expect("messages")
            .iter()
            .all(|message| message["role"] != "system"),
        "system never appears in messages"
    );
}

#[tokio::test]
async fn renders_every_tool_choice_variant() {
    let server = MockServer::start(vec![
        Reply::sse(empty_sse()),
        Reply::sse(empty_sse()),
        Reply::sse(empty_sse()),
    ]);
    let provider = provider(&server);
    for choice in [
        ToolChoice::Auto,
        ToolChoice::None,
        ToolChoice::Tool(ToolName::new("search")),
    ] {
        let mut request = request_for("claude-opus-5");
        request.tools = vec![descriptor("search")];
        request.baseline_tools = request.tools.clone();
        request.tool_choice = choice;
        drain(&provider, request).await;
    }
    let recorded = server.requests();
    assert_eq!(
        recorded[0].json_body()["tool_choice"],
        serde_json::json!({"type": "auto"})
    );
    assert_eq!(
        recorded[1].json_body()["tool_choice"],
        serde_json::json!({"type": "none"})
    );
    assert_eq!(
        recorded[2].json_body()["tool_choice"],
        serde_json::json!({"type": "tool", "name": "search"})
    );
}

#[tokio::test]
async fn renders_tool_calls_and_merges_consecutive_results_before_text() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let mut request = base_request();
    request.items = vec![
        assistant_message(vec![text("thinking aloud")]),
        Item::ToolCall {
            call_id: "toolu_a".to_owned(),
            provider_item_id: None,
            name: ToolName::new("search"),
            raw_args: JsonString::new(r#"{"q": "a"}"#),
            raw: None,
        },
        Item::ToolCall {
            call_id: "toolu_b".to_owned(),
            provider_item_id: None,
            name: ToolName::new("search"),
            raw_args: JsonString::new(r#"{"q": "b"}"#),
            raw: None,
        },
        Item::ToolResult {
            call_id: "toolu_a".to_owned(),
            provider_item_id: None,
            parts: vec![text("first")],
            is_error: false,
            raw: None,
        },
        Item::ToolResult {
            call_id: "toolu_b".to_owned(),
            provider_item_id: None,
            parts: vec![text("second")],
            is_error: true,
            raw: None,
        },
        user_message(vec![text("follow up")]),
    ];
    drain(&provider(&server), request).await;
    let body = server.requests()[0].json_body();
    let messages = body["messages"].as_array().expect("messages");
    assert_eq!(
        messages.len(),
        2,
        "one assistant and one merged user message"
    );
    assert_eq!(messages[0]["role"], "assistant");
    assert_eq!(
        messages[0]["content"],
        serde_json::json!([
            {"type": "text", "text": "thinking aloud"},
            {"type": "tool_use", "id": "toolu_a", "name": "search", "input": {"q": "a"}},
            {"type": "tool_use", "id": "toolu_b", "name": "search", "input": {"q": "b"}, "cache_control": {"type": "ephemeral"}}
        ])
    );
    assert_eq!(messages[1]["role"], "user");
    assert_eq!(
        messages[1]["content"],
        serde_json::json!([
            {"type": "tool_result", "tool_use_id": "toolu_a", "content": [{"type": "text", "text": "first"}], "is_error": false},
            {"type": "tool_result", "tool_use_id": "toolu_b", "content": [{"type": "text", "text": "second"}], "is_error": true},
            {"type": "text", "text": "follow up", "cache_control": {"type": "ephemeral"}}
        ]),
        "tool_result blocks come before text in the merged user message"
    );
}

#[tokio::test]
async fn renders_images_and_refusals() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let mut request = base_request();
    request.items = vec![
        user_message(vec![
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
        ]),
        assistant_message(vec![Part::Refusal {
            text: "I will not".to_owned(),
        }]),
    ];
    drain(&provider(&server), request).await;
    let body = server.requests()[0].json_body();
    assert_eq!(
        body["messages"][0]["content"],
        serde_json::json!([
            {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "AAAA"}},
            {"type": "image", "source": {"type": "url", "url": "https://example.invalid/x.jpg"}, "cache_control": {"type": "ephemeral"}}
        ])
    );
    assert_eq!(
        body["messages"][1]["content"],
        serde_json::json!([{"type": "text", "text": "I will not", "cache_control": {"type": "ephemeral"}}])
    );
}

#[tokio::test]
async fn tool_result_images_render_natively() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let mut request = base_request();
    request.items = vec![Item::ToolResult {
        call_id: "toolu_a".to_owned(),
        provider_item_id: None,
        parts: vec![
            Part::Image {
                media_type: "image/png".to_owned(),
                source: ImageSource::Base64 {
                    data: "AAAA".to_owned(),
                },
            },
            text("caption"),
        ],
        is_error: false,
        raw: None,
    }];
    drain(&provider(&server), request).await;
    let body = server.requests()[0].json_body();
    assert_eq!(
        body["messages"][0]["content"][0],
        serde_json::json!({
            "type": "tool_result",
            "tool_use_id": "toolu_a",
            "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "AAAA"}},
                {"type": "text", "text": "caption"}
            ],
            "is_error": false,
            "cache_control": {"type": "ephemeral"}
        })
    );
}

fn pdf(source: FileSource) -> Part {
    Part::File {
        media_type: "application/pdf".to_owned(),
        filename: Some("report.pdf".to_owned()),
        source,
    }
}

fn anthropic_file(id: &str) -> ProviderFileId {
    ProviderFileId::new(ProviderId::new("anthropic"), id)
}

#[tokio::test]
async fn files_render_as_document_blocks() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let mut request = request_for("claude-opus-5-5");
    request.items = vec![
        user_message(vec![
            pdf(FileSource::Base64 {
                data: "JVBE".to_owned(),
            }),
            pdf(FileSource::Url {
                url: "https://example.invalid/r.pdf".to_owned(),
            }),
            pdf(FileSource::ProviderFile(anthropic_file("file_a"))),
            Part::Image {
                media_type: "image/png".to_owned(),
                source: ImageSource::ProviderFile(anthropic_file("file_b")),
            },
        ]),
        Item::ToolResult {
            call_id: "toolu_a".to_owned(),
            provider_item_id: None,
            parts: vec![pdf(FileSource::ProviderFile(anthropic_file("file_c")))],
            is_error: false,
            raw: None,
        },
    ];
    drain(&provider(&server), request).await;
    let body = server.requests()[0].json_body();
    assert_eq!(
        body["messages"][0]["content"],
        serde_json::json!([
            {"type": "tool_result", "tool_use_id": "toolu_a", "content": [
                {"type": "document", "source": {"type": "file", "file_id": "file_c"}, "title": "report.pdf"}
            ], "is_error": false},
            {"type": "document", "source": {"type": "base64", "media_type": "application/pdf", "data": "JVBE"}, "title": "report.pdf"},
            {"type": "document", "source": {"type": "url", "url": "https://example.invalid/r.pdf"}, "title": "report.pdf"},
            {"type": "document", "source": {"type": "file", "file_id": "file_a"}, "title": "report.pdf"},
            {"type": "image", "source": {"type": "file", "file_id": "file_b"}, "cache_control": {"type": "ephemeral"}}
        ])
    );
    assert!(
        !server.requests()[0]
            .head
            .to_ascii_lowercase()
            .contains("anthropic-beta"),
        "the Files API reference is GA"
    );
}

#[tokio::test]
async fn unrenderable_files_fail_before_any_http() {
    let server = MockServer::start(Vec::new());
    let other = ProviderFileId::new(ProviderId::new("openai"), "file-x");
    let cases = [
        // Media type the model rules do not accept.
        user_message(vec![Part::File {
            media_type: "text/csv".to_owned(),
            filename: None,
            source: FileSource::Base64 {
                data: "YQ==".to_owned(),
            },
        }]),
        assistant_message(vec![pdf(FileSource::Base64 {
            data: "JVBE".to_owned(),
        })]),
        user_message(vec![pdf(FileSource::ProviderFile(other.clone()))]),
        user_message(vec![Part::Image {
            media_type: "image/png".to_owned(),
            source: ImageSource::ProviderFile(other.clone()),
        }]),
        Item::ToolResult {
            call_id: "toolu_a".to_owned(),
            provider_item_id: None,
            parts: vec![pdf(FileSource::ProviderFile(other))],
            is_error: false,
            raw: None,
        },
    ];
    for item in cases {
        let mut request = request_for("claude-opus-5-5");
        request.items = vec![item];
        let error = one_request(&server, request).await;
        assert_eq!(error.kind, ProviderErrorKind::Unsupported, "{error:?}");
    }

    // A profile that accepts no file types has no file input at all.
    let provider = provider(&server)
        .with_default_profile(AnthropicModelProfile::latest().with_file_media_types(Vec::new()));
    let mut request = request_for("claude-opus-5-5");
    request.items = vec![user_message(vec![pdf(FileSource::Base64 {
        data: "JVBE".to_owned(),
    })])];
    assert!(!provider.capabilities(&request.spec).file_input);
    let events = drain(&provider, request).await;
    assert!(matches!(
        &events[0],
        Err(error) if error.kind == ProviderErrorKind::Unsupported
    ));
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn unsupported_request_fields_fail_before_any_http() {
    let server = MockServer::start(Vec::new());

    let mut seed = base_request();
    let mut sampling = SamplingParams::default();
    sampling.seed = Some(7);
    seed.sampling = Some(sampling);
    let error = one_request(&server, seed).await;
    assert_eq!(error.kind, ProviderErrorKind::Unsupported);
    assert!(!error.retryable);

    // Effort is documented only for Opus 4.5 among extended-thinking-only models.
    let mut effort = request_for("claude-sonnet-4-5");
    let mut reasoning = ReasoningParams::default();
    reasoning.effort = Some(ReasoningEffort::High);
    effort.reasoning = Some(reasoning);
    let error = one_request(&server, effort).await;
    assert_eq!(error.kind, ProviderErrorKind::Unsupported);

    let mut bad_system = base_request();
    bad_system.system = vec![Part::Refusal {
        text: "nope".to_owned(),
    }];
    let error = one_request(&server, bad_system).await;
    assert_eq!(error.kind, ProviderErrorKind::InvalidRequest);

    let mut bad_args = base_request();
    bad_args.items = vec![Item::ToolCall {
        call_id: "toolu_a".to_owned(),
        provider_item_id: None,
        name: ToolName::new("search"),
        raw_args: JsonString::new("not json"),
        raw: None,
    }];
    let error = one_request(&server, bad_args).await;
    assert_eq!(error.kind, ProviderErrorKind::InvalidRequest);

    assert!(
        server.requests().is_empty(),
        "capability/render checks run before sending HTTP"
    );
}

#[tokio::test]
async fn the_same_log_renders_byte_identical_requests() {
    let server = MockServer::start(vec![Reply::sse(empty_sse()), Reply::sse(empty_sse())]);
    let provider = provider(&server);
    let mut request = base_request();
    request.items = vec![user_message(vec![text("hi")])];
    request.tools = vec![descriptor("a"), descriptor("b")];
    request.baseline_tools = request.tools.clone();
    request.max_output_tokens = 100;
    drain(&provider, request.clone()).await;
    drain(&provider, request).await;
    let recorded = server.requests();
    assert_eq!(recorded.len(), 2);
    assert_eq!(recorded[0].body, recorded[1].body);
}

#[tokio::test]
async fn reasoning_raw_json_is_replayed_byte_for_byte() {
    let raw = "{\"type\":\"thinking\",\n  \"thinking\":\"why\",  \"signature\":\"sig-123\"}";
    let reasoning = Item::Reasoning {
        summary: vec![text("why")],
        binding: ReasoningBinding {
            provider: model().provider,
            model: model().model,
            prefix_epoch: 0,
            tools_hash: String::new(), // replaced below with the produced fingerprint
        },
        raw: RawProviderData {
            provider: ProviderId::new("anthropic"),
            model: Some(ModelId::new("claude-test")),
            kind: "anthropic.thinking".to_owned(),
            json: JsonString::new(raw),
        },
    };
    // First produce a reasoning item from a real stream so we learn the tools hash
    // the adapter binds; then replay the hand-crafted raw with that same binding.
    let thinking_sse = common::sse_body(&[
        (
            "message_start",
            r#"{"type":"message_start","message":{"id":"msg_t","usage":{"input_tokens":1}}}"#,
        ),
        (
            "content_block_start",
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}"#,
        ),
        (
            "content_block_delta",
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"why"}}"#,
        ),
        (
            "content_block_delta",
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig-123"}}"#,
        ),
        (
            "content_block_stop",
            r#"{"type":"content_block_stop","index":0}"#,
        ),
        (
            "message_delta",
            r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}"#,
        ),
        ("message_stop", r#"{"type":"message_stop"}"#),
    ]);
    let server = MockServer::start(vec![
        Reply::sse(thinking_sse),
        Reply::sse(empty_sse()),
        Reply::sse(empty_sse()),
        Reply::sse(empty_sse()),
        Reply::sse(empty_sse()),
        Reply::sse(empty_sse()),
        Reply::sse(empty_sse()),
    ]);
    let provider = provider(&server);
    let events = drain(&provider, base_request()).await;
    let produced = events
        .iter()
        .find_map(|event| match event {
            Ok(asterpath_protocol::StreamEvent::ItemCompleted {
                item: Item::Reasoning { binding, .. },
                ..
            }) => Some(binding.clone()),
            _ => None,
        })
        .expect("produced reasoning");
    let produced_raw = events
        .iter()
        .find_map(|event| match event {
            Ok(asterpath_protocol::StreamEvent::ItemCompleted {
                item: Item::Reasoning { raw, .. },
                ..
            }) => Some(raw.json.clone()),
            _ => None,
        })
        .expect("produced reasoning raw");
    let reasoning = match reasoning {
        Item::Reasoning { summary, raw, .. } => Item::Reasoning {
            summary,
            binding: produced.clone(),
            raw,
        },
        other => other,
    };

    let mut replay = base_request();
    replay.items = vec![reasoning.clone()];
    drain(&provider, replay).await;
    let body = server.requests()[1].body.clone();
    assert!(
        body.contains(raw),
        "raw reasoning JSON is spliced unchanged:\n{body}"
    );

    // The adapter's own produced raw round-trips byte-for-byte too.
    let mut own_replay = base_request();
    own_replay.items = vec![Item::Reasoning {
        summary: vec![text("why")],
        binding: produced,
        raw: RawProviderData {
            provider: ProviderId::new("anthropic"),
            model: Some(ModelId::new("claude-test")),
            kind: "anthropic.thinking".to_owned(),
            json: produced_raw.clone(),
        },
    }];
    drain(&provider, own_replay).await;
    assert!(
        server.requests()[2].body.contains(produced_raw.as_str()),
        "produced raw is replayed unchanged"
    );

    let mut wrong_epoch = base_request();
    wrong_epoch.prefix_epoch = 1;
    wrong_epoch.items = vec![reasoning.clone()];
    drain(&provider, wrong_epoch).await;
    assert!(!server.requests()[3].body.contains(raw));

    let mut wrong_model = base_request();
    wrong_model.spec.model = ModelId::new("other-model");
    wrong_model.items = vec![reasoning.clone()];
    drain(&provider, wrong_model).await;
    assert!(!server.requests()[4].body.contains(raw));

    // Non-empty tools change the fingerprint, so the reasoning is dropped.
    let mut wrong_tools = base_request();
    wrong_tools.tools = vec![descriptor("search")];
    wrong_tools.baseline_tools = wrong_tools.tools.clone();
    wrong_tools.items = vec![reasoning.clone()];
    drain(&provider, wrong_tools).await;
    assert!(!server.requests()[5].body.contains(raw));

    // A binding from another provider is never replayed.
    let mut other_provider = reasoning;
    if let Item::Reasoning { binding, .. } = &mut other_provider {
        binding.provider = ProviderId::new("other");
    }
    let mut wrong_provider = base_request();
    wrong_provider.items = vec![other_provider];
    drain(&provider, wrong_provider).await;
    assert!(!server.requests()[6].body.contains(raw));
}

#[tokio::test]
async fn xhigh_and_max_render_on_models_that_accept_them() {
    let cases = [
        (ReasoningEffort::XHigh, "claude-opus-5-5", "xhigh"),
        (ReasoningEffort::XHigh, "claude-opus-4-7", "xhigh"),
        (ReasoningEffort::Max, "claude-sonnet-4-6", "max"),
        (ReasoningEffort::Max, "claude-mythos-preview", "max"),
    ];
    let server = MockServer::start(cases.iter().map(|_| Reply::sse(empty_sse())).collect());
    let provider = provider(&server);
    for (level, model, _) in cases {
        let mut request = request_for(model);
        request.reasoning = effort(level);
        drain(&provider, request).await;
    }
    let recorded = server.requests();
    assert_eq!(recorded.len(), cases.len());
    for (recorded, (_, model, wire)) in recorded.iter().zip(cases) {
        assert_eq!(
            recorded.json_body()["output_config"],
            serde_json::json!({"effort": wire}),
            "{model}"
        );
    }
}

#[tokio::test]
async fn off_sends_neither_thinking_nor_effort_where_thinking_defaults_off() {
    let models = ["claude-opus-4-7", "claude-sonnet-4-6", "claude-sonnet-4-5"];
    let server = MockServer::start(models.iter().map(|_| Reply::sse(empty_sse())).collect());
    let provider = provider(&server);
    for model in models {
        let mut request = request_for(model);
        let mut reasoning = ReasoningParams::default();
        reasoning.effort = Some(ReasoningEffort::Off);
        reasoning.summary = true;
        request.reasoning = Some(reasoning);
        drain(&provider, request).await;
    }
    let recorded = server.requests();
    assert_eq!(recorded.len(), models.len());
    for (recorded, model) in recorded.iter().zip(models) {
        let body = recorded.json_body();
        assert!(body.get("thinking").is_none(), "{model}: {body}");
        assert!(body.get("output_config").is_none(), "{model}: {body}");
    }
}

#[tokio::test]
async fn off_sends_disabled_thinking_where_thinking_defaults_on() {
    let models = ["claude-opus-5", "claude-sonnet-5", "claude-haiku-5-5"];
    let server = MockServer::start(models.iter().map(|_| Reply::sse(empty_sse())).collect());
    let provider = provider(&server);
    for model in models {
        let mut request = request_for(model);
        let mut reasoning = ReasoningParams::default();
        reasoning.effort = Some(ReasoningEffort::Off);
        reasoning.summary = true;
        request.reasoning = Some(reasoning);
        drain(&provider, request).await;
    }
    let recorded = server.requests();
    assert_eq!(recorded.len(), models.len());
    for (recorded, model) in recorded.iter().zip(models) {
        let body = recorded.json_body();
        assert_eq!(
            body["thinking"],
            serde_json::json!({"type": "disabled"}),
            "{model}"
        );
        assert!(body.get("output_config").is_none(), "{model}: {body}");
    }
}

#[tokio::test]
async fn efforts_a_model_does_not_accept_are_unsupported_before_http() {
    let server = MockServer::start(Vec::new());
    for (level, model) in [
        // Every Anthropic model: there is no `minimal`.
        (ReasoningEffort::Minimal, "claude-opus-5-5"),
        // Thinking cannot be disabled (always on, or `disabled` rejected).
        (ReasoningEffort::Off, "claude-opus-5-5"),
        (ReasoningEffort::Off, "claude-sonnet-5-5"),
        (ReasoningEffort::Off, "claude-fable-5"),
        (ReasoningEffort::Off, "claude-mythos-preview"),
        (ReasoningEffort::Off, "claude-test"),
        // `max` without `xhigh`.
        (ReasoningEffort::XHigh, "claude-opus-4-6"),
        (ReasoningEffort::XHigh, "claude-mythos-preview"),
        // Opus 4.5 documents only low, medium and high.
        (ReasoningEffort::Max, "claude-opus-4-5-20251101"),
        // No effort at all.
        (ReasoningEffort::Low, "claude-sonnet-4-5"),
    ] {
        let mut request = request_for(model);
        request.reasoning = effort(level);
        let error = one_request(&server, request).await;
        assert_eq!(
            error.kind,
            ProviderErrorKind::Unsupported,
            "{model} {level:?}"
        );
        assert!(error.message.contains(&format!("{level:?}")), "{error:?}");
    }
    assert!(server.requests().is_empty(), "rejected before any HTTP");
}

#[tokio::test]
async fn current_models_render_adaptive_thinking_with_effort() {
    let server = MockServer::start(vec![
        Reply::sse(empty_sse()),
        Reply::sse(empty_sse()),
        Reply::sse(empty_sse()),
    ]);
    let provider = provider(&server);
    for (level, model) in [
        (ReasoningEffort::Low, "claude-opus-5-5"),
        (ReasoningEffort::Medium, "claude-haiku-5-5"),
        (ReasoningEffort::High, "claude-opus-4-7"),
    ] {
        let mut request = request_for(model);
        request.reasoning = effort(level);
        drain(&provider, request).await;
    }
    let recorded = server.requests();
    assert_eq!(recorded.len(), 3);
    for (recorded, expected) in recorded.iter().zip(["low", "medium", "high"]) {
        let body = recorded.json_body();
        assert_eq!(body["thinking"], serde_json::json!({"type": "adaptive"}));
        assert_eq!(
            body["output_config"],
            serde_json::json!({"effort": expected})
        );
    }
}

#[tokio::test]
async fn summary_requests_summarized_thinking_display_where_supported() {
    let server = MockServer::start(vec![
        Reply::sse(empty_sse()),
        Reply::sse(empty_sse()),
        Reply::sse(empty_sse()),
    ]);
    let provider = provider(&server);
    for (model, budget_tokens) in [
        ("claude-opus-5-5", None),
        ("claude-opus-4-6", Some(2048)),
        ("claude-sonnet-4-5", Some(2048)),
    ] {
        let mut request = request_for(model);
        let mut reasoning = ReasoningParams::default();
        reasoning.budget_tokens = budget_tokens;
        reasoning.summary = true;
        request.reasoning = Some(reasoning);
        drain(&provider, request).await;
    }
    let recorded = server.requests();
    assert_eq!(
        recorded[0].json_body()["thinking"],
        serde_json::json!({"type": "adaptive", "display": "summarized"})
    );
    assert_eq!(
        recorded[1].json_body()["thinking"],
        serde_json::json!({"type": "enabled", "budget_tokens": 2048, "display": "summarized"})
    );
    // Pre-4.6 models predate `display` and always summarize.
    assert_eq!(
        recorded[2].json_body()["thinking"],
        serde_json::json!({"type": "enabled", "budget_tokens": 2048})
    );
}

#[tokio::test]
async fn transitional_and_opus_4_5_models_keep_their_documented_shapes() {
    let server = MockServer::start(vec![
        Reply::sse(empty_sse()),
        Reply::sse(empty_sse()),
        Reply::sse(empty_sse()),
    ]);
    let provider = provider(&server);

    // 4.6 accepts both; adaptive is used unless a budget is asked for.
    let mut adaptive = request_for("claude-sonnet-4-6");
    adaptive.reasoning = effort(ReasoningEffort::Medium);
    drain(&provider, adaptive).await;
    let mut extended = request_for("claude-opus-4-6");
    extended.reasoning = budget(2048);
    drain(&provider, extended).await;
    // Opus 4.5 is extended-thinking-only but documents effort alongside the budget.
    let mut opus_4_5 = request_for("claude-opus-4-5-20251101");
    let mut reasoning = ReasoningParams::default();
    reasoning.effort = Some(ReasoningEffort::Low);
    reasoning.budget_tokens = Some(2048);
    opus_4_5.reasoning = Some(reasoning);
    drain(&provider, opus_4_5).await;

    let recorded = server.requests();
    let body = recorded[0].json_body();
    assert_eq!(body["thinking"], serde_json::json!({"type": "adaptive"}));
    assert_eq!(
        body["output_config"],
        serde_json::json!({"effort": "medium"})
    );
    let body = recorded[1].json_body();
    assert_eq!(
        body["thinking"],
        serde_json::json!({"type": "enabled", "budget_tokens": 2048})
    );
    assert!(body.get("output_config").is_none());
    let body = recorded[2].json_body();
    assert_eq!(
        body["thinking"],
        serde_json::json!({"type": "enabled", "budget_tokens": 2048})
    );
    assert_eq!(body["output_config"], serde_json::json!({"effort": "low"}));
}

#[tokio::test]
async fn current_models_reject_budget_forced_tool_choice_and_sampling_before_http() {
    let server = MockServer::start(Vec::new());

    let mut with_budget = request_for("claude-opus-5-5");
    with_budget.reasoning = budget(2048);
    let mut forced = request_for("claude-sonnet-5-5");
    forced.tools = vec![descriptor("search")];
    forced.baseline_tools = forced.tools.clone();
    forced.tool_choice = ToolChoice::Required;
    let mut named = request_for("claude-fable-5-1");
    named.tools = vec![descriptor("search")];
    named.baseline_tools = named.tools.clone();
    named.tool_choice = ToolChoice::Tool(ToolName::new("search"));
    let mut sampled = request_for("claude-opus-4-7");
    let mut sampling = SamplingParams::default();
    sampling.temperature = Some(0.2);
    sampled.sampling = Some(sampling);

    for request in [with_budget, forced, named, sampled] {
        let error = one_request(&server, request).await;
        assert_eq!(error.kind, ProviderErrorKind::Unsupported, "{error:?}");
    }
    assert!(server.requests().is_empty(), "rejected before any HTTP");
}

#[tokio::test]
async fn unrecognized_model_ids_are_treated_as_the_latest_generation() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let provider = provider(&server);
    let capabilities = asterpath_protocol::Provider::capabilities(&provider, &model());
    assert!(!capabilities.forced_tool_choice);
    assert!(!capabilities.sampling);

    let mut with_budget = base_request();
    with_budget.reasoning = budget(2048);
    let error = one_request(&server, with_budget).await;
    assert_eq!(error.kind, ProviderErrorKind::Unsupported);

    let mut request = base_request();
    request.reasoning = effort(ReasoningEffort::High);
    drain(&provider, request).await;
    let recorded = server.requests();
    assert_eq!(recorded.len(), 1);
    let body = recorded[0].json_body();
    assert_eq!(body["model"], "claude-test");
    assert_eq!(body["thinking"], serde_json::json!({"type": "adaptive"}));
    assert_eq!(body["output_config"], serde_json::json!({"effort": "high"}));
}

#[tokio::test]
async fn unrecognized_model_ids_have_no_file_input() {
    // protocol.md §5: unknown means false.
    let server = MockServer::start(Vec::new());
    let provider = provider(&server);
    for id in ["claude-test", "deepseek-v4-pro"] {
        let mut request = request_for(id);
        assert!(!provider.capabilities(&request.spec).file_input, "{id}");
        request.items = vec![user_message(vec![pdf(FileSource::Base64 {
            data: "JVBE".to_owned(),
        })])];
        let error = one_request(&server, request).await;
        assert_eq!(error.kind, ProviderErrorKind::Unsupported, "{id}");
    }
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn older_models_keep_forced_tool_choice_and_sampling() {
    let provider = provider(&MockServer::start(Vec::new()));
    for model in ["claude-sonnet-4-5", "claude-opus-4-6"] {
        let mut spec = common::model();
        spec.model = ModelId::new(model);
        let capabilities = asterpath_protocol::Provider::capabilities(&provider, &spec);
        assert!(capabilities.forced_tool_choice, "{model}");
        assert!(capabilities.sampling, "{model}");
    }
    // Opus 5 still accepts forced tool use but has no sampling parameters.
    let mut spec = common::model();
    spec.model = ModelId::new("claude-opus-5");
    let capabilities = asterpath_protocol::Provider::capabilities(&provider, &spec);
    assert!(capabilities.forced_tool_choice);
    assert!(!capabilities.sampling);
}

fn breakpoints(body: &serde_json::Value) -> usize {
    match body {
        serde_json::Value::Object(map) => {
            usize::from(map.contains_key("cache_control"))
                + map.values().map(breakpoints).sum::<usize>()
        }
        serde_json::Value::Array(items) => items.iter().map(breakpoints).sum(),
        _ => 0,
    }
}

#[test]
fn cache_control_is_advertised() {
    use asterpath_protocol::Provider;
    let provider = asterpath_provider_anthropic::AnthropicProvider::new("key");
    assert!(provider.capabilities(&model()).cache_control);
}

#[tokio::test]
async fn sets_cache_breakpoints_on_last_tool_system_block_and_last_two_messages() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let mut request = base_request();
    request.system = vec![text("be helpful"), text("be terse")];
    request.tools = vec![descriptor("a"), descriptor("b")];
    request.baseline_tools = request.tools.clone();
    request.items = vec![
        user_message(vec![text("hi")]),
        assistant_message(vec![text("hello")]),
        user_message(vec![text("first"), text("second")]),
    ];
    drain(&provider(&server), request).await;
    let body = server.requests()[0].json_body();

    assert!(body["tools"][0].get("cache_control").is_none());
    assert_eq!(
        body["tools"][1]["cache_control"],
        serde_json::json!({"type": "ephemeral"})
    );
    assert_eq!(
        body["system"],
        serde_json::json!([
            {"type": "text", "text": "be helpful"},
            {"type": "text", "text": "be terse", "cache_control": {"type": "ephemeral"}}
        ])
    );
    assert_eq!(
        body["messages"][2]["content"],
        serde_json::json!([
            {"type": "text", "text": "first"},
            {"type": "text", "text": "second", "cache_control": {"type": "ephemeral"}}
        ])
    );
    // Fallback on the previous message, in case the step added more than the
    // API's 20-block lookback.
    assert_eq!(
        body["messages"][1]["content"],
        serde_json::json!([
            {"type": "text", "text": "hello", "cache_control": {"type": "ephemeral"}}
        ])
    );
    assert_eq!(breakpoints(&body), 4, "the API allows at most 4");
}

/// Every `cache_control` in body order (tools, system, messages).
fn cache_controls(body: &serde_json::Value) -> Vec<serde_json::Value> {
    let mut found = Vec::new();
    for section in ["tools", "system", "messages"] {
        collect_cache_controls(&body[section], &mut found);
    }
    found
}

fn collect_cache_controls(value: &serde_json::Value, found: &mut Vec<serde_json::Value>) {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(control) = map.get("cache_control") {
                found.push(control.clone());
            }
            map.values()
                .for_each(|value| collect_cache_controls(value, found));
        }
        serde_json::Value::Array(items) => items
            .iter()
            .for_each(|value| collect_cache_controls(value, found)),
        _ => {}
    }
}

fn cached_request(prefix: CacheLifetime, messages: CacheLifetime) -> ModelRequest {
    let mut request = base_request();
    request.tools = vec![descriptor("a")];
    request.baseline_tools = request.tools.clone();
    request.items = vec![
        user_message(vec![text("hi")]),
        assistant_message(vec![text("hello")]),
        user_message(vec![text("again")]),
    ];
    request.cache_ttl = CacheTtl { prefix, messages };
    request
}

#[tokio::test]
async fn cache_ttl_sets_one_hour_or_five_minute_breakpoints() {
    let five = serde_json::json!({"type": "ephemeral"});
    let hour = serde_json::json!({"type": "ephemeral", "ttl": "1h"});
    let cases = [
        (
            CacheLifetime::FiveMinutes,
            CacheLifetime::FiveMinutes,
            [&five, &five, &five, &five],
        ),
        (
            CacheLifetime::OneHour,
            CacheLifetime::OneHour,
            [&hour, &hour, &hour, &hour],
        ),
        // Mixed: 1h on the stable prefix, 5m on the moving message breakpoints.
        (
            CacheLifetime::OneHour,
            CacheLifetime::FiveMinutes,
            [&hour, &hour, &five, &five],
        ),
        // The API needs 1h breakpoints before 5m ones, so the prefix is lifted.
        (
            CacheLifetime::FiveMinutes,
            CacheLifetime::OneHour,
            [&hour, &hour, &hour, &hour],
        ),
    ];
    let server = MockServer::start(cases.iter().map(|_| Reply::sse(empty_sse())).collect());
    let provider = provider(&server);
    for (prefix, messages, _) in &cases {
        drain(&provider, cached_request(*prefix, *messages)).await;
    }
    for (recorded, (prefix, messages, expected)) in server.requests().iter().zip(&cases) {
        let found = cache_controls(&recorded.json_body());
        let expected: Vec<serde_json::Value> =
            expected.iter().map(|value| (*value).clone()).collect();
        assert_eq!(found, expected, "prefix {prefix:?}, messages {messages:?}");
    }
}

#[test]
fn default_cache_ttl_is_five_minutes_everywhere() {
    assert_eq!(
        RequestConfig::default().cache_ttl,
        CacheTtl {
            prefix: CacheLifetime::FiveMinutes,
            messages: CacheLifetime::FiveMinutes,
        }
    );
}

#[tokio::test]
async fn empty_text_blocks_never_carry_a_breakpoint() {
    // The API rejects cache_control on an empty text block.
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let mut request = base_request();
    request.system = vec![text("be terse"), text("")];
    request.items = vec![user_message(vec![text("first"), text("")])];
    drain(&provider(&server), request).await;
    let body = server.requests()[0].json_body();

    assert_eq!(
        body["system"],
        serde_json::json!([
            {"type": "text", "text": "be terse", "cache_control": {"type": "ephemeral"}},
            {"type": "text", "text": ""}
        ])
    );
    assert_eq!(
        body["messages"][0]["content"],
        serde_json::json!([
            {"type": "text", "text": "first", "cache_control": {"type": "ephemeral"}},
            {"type": "text", "text": ""}
        ])
    );
}

#[tokio::test]
async fn breakpoints_are_omitted_without_tools_or_system() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let mut request = base_request();
    request.system = Vec::new();
    request.items = vec![user_message(vec![text("hi")])];
    drain(&provider(&server), request).await;
    let body = server.requests()[0].json_body();
    assert!(body.get("tools").is_none());
    assert!(body.get("system").is_none());
    assert_eq!(
        body["messages"][0]["content"],
        serde_json::json!([{"type": "text", "text": "hi", "cache_control": {"type": "ephemeral"}}])
    );
    assert_eq!(breakpoints(&body), 1);
}

#[tokio::test]
async fn tool_result_blocks_carry_the_message_breakpoint() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let mut request = base_request();
    request.system = Vec::new();
    request.items = vec![Item::ToolResult {
        call_id: "toolu_a".to_owned(),
        provider_item_id: None,
        parts: vec![text("done")],
        is_error: false,
        raw: None,
    }];
    drain(&provider(&server), request).await;
    let body = server.requests()[0].json_body();
    assert_eq!(
        body["messages"][0]["content"][0]["cache_control"],
        serde_json::json!({"type": "ephemeral"})
    );
    assert_eq!(breakpoints(&body), 1);
}

#[tokio::test]
async fn breakpoint_skips_replayed_raw_blocks_and_leaves_them_byte_identical() {
    let raw = "{\"type\":\"redacted_thinking\",\n  \"data\":\"opaque\"}";
    let opaque = || Part::ProviderOpaque {
        provider: ProviderId::new("anthropic"),
        kind: "anthropic.redacted_thinking".to_owned(),
        raw: RawProviderData {
            provider: ProviderId::new("anthropic"),
            model: Some(ModelId::new("claude-test")),
            kind: "anthropic.redacted_thinking".to_owned(),
            json: JsonString::new(raw),
        },
    };
    let server = MockServer::start(vec![Reply::sse(empty_sse()), Reply::sse(empty_sse())]);
    let provider = provider(&server);

    // A raw last block moves the breakpoint to the previous carrier.
    let mut moved = base_request();
    moved.system = Vec::new();
    moved.items = vec![assistant_message(vec![text("before"), opaque()])];
    drain(&provider, moved).await;
    // Only raw blocks in the last message: the breakpoint is omitted.
    let mut omitted = base_request();
    omitted.system = Vec::new();
    omitted.items = vec![
        user_message(vec![text("hi")]),
        assistant_message(vec![opaque()]),
    ];
    drain(&provider, omitted).await;

    let recorded = server.requests();
    assert!(recorded[0].body.contains(raw), "{}", recorded[0].body);
    let body = recorded[0].json_body();
    assert_eq!(
        body["messages"][0]["content"][0],
        serde_json::json!({"type": "text", "text": "before", "cache_control": {"type": "ephemeral"}})
    );
    assert_eq!(breakpoints(&body), 1);

    assert!(recorded[1].body.contains(raw), "{}", recorded[1].body);
    // Only the previous message's fallback remains.
    let body = recorded[1].json_body();
    assert_eq!(
        body["messages"][0]["content"][0]["cache_control"],
        serde_json::json!({"type": "ephemeral"})
    );
    assert_eq!(breakpoints(&body), 1);
}

#[tokio::test]
async fn renders_stop_sequences_and_same_provider_options_at_top_level() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let mut request = base_request();
    request.stop_sequences = vec!["END".to_owned(), "\n\nHuman:".to_owned()];
    request.provider_options.insert(
        ProviderId::new("anthropic"),
        JsonString::new(r#"{"service_tier":"standard_only","metadata":{"user_id":"u-1"}}"#),
    );
    request.provider_options.insert(
        ProviderId::new("openai"),
        JsonString::new(r#"{"prompt_cache_key":"k"}"#),
    );

    drain(&provider(&server), request).await;
    let body = server.requests()[0].json_body();
    assert_eq!(
        body["stop_sequences"],
        serde_json::json!(["END", "\n\nHuman:"])
    );
    assert_eq!(body["service_tier"], "standard_only");
    assert_eq!(body["metadata"], serde_json::json!({"user_id": "u-1"}));
    assert!(
        body.get("prompt_cache_key").is_none(),
        "other provider ignored"
    );
}

#[tokio::test]
async fn empty_stop_sequences_are_not_sent() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    drain(&provider(&server), base_request()).await;
    assert!(
        server.requests()[0]
            .json_body()
            .get("stop_sequences")
            .is_none()
    );
}

#[tokio::test]
async fn provider_options_deep_merge_into_objects_the_adapter_writes() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let mut request = request_for("claude-opus-5-5");
    request.reasoning = effort(ReasoningEffort::High);
    request.provider_options.insert(
        ProviderId::new("anthropic"),
        JsonString::new(r#"{"output_config":{"format":{"type":"json_schema"}}}"#),
    );

    drain(&provider(&server), request).await;
    let body = server.requests()[0].json_body();
    assert_eq!(
        body["output_config"],
        serde_json::json!({"effort": "high", "format": {"type": "json_schema"}})
    );
}

#[tokio::test]
async fn provider_options_may_not_replace_a_nested_value_the_adapter_writes() {
    let server = MockServer::start(Vec::new());
    let mut request = request_for("claude-opus-5-5");
    request.reasoning = effort(ReasoningEffort::High);
    request.provider_options.insert(
        ProviderId::new("anthropic"),
        JsonString::new(r#"{"output_config":{"effort":"low"}}"#),
    );

    let error = one_request(&server, request).await;
    assert_eq!(error.kind, ProviderErrorKind::Unsupported);
    assert!(error.message.contains("output_config.effort"), "{error:?}");
    assert!(server.requests().is_empty(), "rejected before any HTTP");
}

#[tokio::test]
async fn provider_options_conflicting_or_non_object_fail_before_http() {
    let server = MockServer::start(Vec::new());
    for (options, kind) in [
        (r#"{"model":"other"}"#, ProviderErrorKind::Unsupported),
        (r#"{"messages":[]}"#, ProviderErrorKind::Unsupported),
        (r#"{"max_tokens":1}"#, ProviderErrorKind::Unsupported),
        (r#"{"stream":false}"#, ProviderErrorKind::Unsupported),
        (r#"{"system":"other"}"#, ProviderErrorKind::Unsupported),
        (r#"["service_tier"]"#, ProviderErrorKind::InvalidRequest),
        ("not json", ProviderErrorKind::InvalidRequest),
    ] {
        let mut request = base_request();
        request
            .provider_options
            .insert(ProviderId::new("anthropic"), JsonString::new(options));
        let error = one_request(&server, request).await;
        assert_eq!(error.kind, kind, "{options}: {error:?}");
        assert!(!error.retryable);
    }
    assert!(server.requests().is_empty(), "rejected before any HTTP");
}

fn deepseek_request(model: &str) -> ModelRequest {
    let mut request = request_for(model);
    request.spec.provider = ProviderId::new("deepseek");
    request
}

#[tokio::test]
async fn with_id_names_the_instance_and_gates_replay_by_it() {
    let thinking_sse = common::sse_body(&[
        (
            "message_start",
            r#"{"type":"message_start","message":{"id":"msg_t","usage":{"input_tokens":1}}}"#,
        ),
        (
            "content_block_start",
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}"#,
        ),
        (
            "content_block_delta",
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig-ds"}}"#,
        ),
        (
            "content_block_stop",
            r#"{"type":"content_block_stop","index":0}"#,
        ),
        (
            "message_delta",
            r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}"#,
        ),
        ("message_stop", r#"{"type":"message_stop"}"#),
    ]);
    let server = MockServer::start(vec![Reply::sse(thinking_sse), Reply::sse(empty_sse())]);
    let deepseek = provider(&server).with_id("deepseek");
    assert_eq!(
        asterpath_protocol::Provider::id(&deepseek),
        ProviderId::new("deepseek")
    );
    assert_eq!(
        asterpath_protocol::Provider::id(&provider(&server)),
        ProviderId::new("anthropic")
    );

    let events = drain(&deepseek, deepseek_request("deepseek-flash")).await;
    let binding = events
        .iter()
        .find_map(|event| match event {
            Ok(asterpath_protocol::StreamEvent::ItemCompleted {
                item: Item::Reasoning { binding, raw, .. },
                ..
            }) => {
                assert_eq!(raw.provider, ProviderId::new("deepseek"));
                Some(binding.clone())
            }
            _ => None,
        })
        .expect("produced reasoning");
    assert_eq!(binding.provider, ProviderId::new("deepseek"));

    // Data from the real Anthropic instance never reaches the deepseek one.
    let anthropic_raw = r#"{"type":"thinking","thinking":"a","signature":"sig-anthropic"}"#;
    let opaque_raw = r#"{"type":"server_thing","id":"anthropic-opaque"}"#;
    let mut replay = deepseek_request("deepseek-flash");
    replay.items = vec![
        Item::Reasoning {
            summary: vec![text("a")],
            binding: ReasoningBinding {
                provider: ProviderId::new("anthropic"),
                ..binding
            },
            raw: RawProviderData {
                provider: ProviderId::new("anthropic"),
                model: Some(ModelId::new("deepseek-flash")),
                kind: "anthropic.thinking".to_owned(),
                json: JsonString::new(anthropic_raw),
            },
        },
        assistant_message(vec![Part::ProviderOpaque {
            provider: ProviderId::new("anthropic"),
            kind: "anthropic.server_thing".to_owned(),
            raw: RawProviderData {
                provider: ProviderId::new("anthropic"),
                model: None,
                kind: "anthropic.server_thing".to_owned(),
                json: JsonString::new(opaque_raw),
            },
        }]),
        user_message(vec![text("next")]),
    ];
    drain(&deepseek, replay).await;
    let body = server.requests()[1].body.clone();
    assert!(!body.contains("sig-anthropic"), "{body}");
    assert!(!body.contains("anthropic-opaque"), "{body}");
}

#[tokio::test]
async fn an_injected_default_profile_lets_off_disable_thinking() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let uninjected = provider(&server).with_id("deepseek");
    let mut request = deepseek_request("deepseek-flash");
    request.reasoning = effort(ReasoningEffort::Off);
    let error = match drain(&uninjected, request.clone()).await.remove(0) {
        Err(error) => error,
        Ok(event) => panic!("expected an error, got {event:?}"),
    };
    assert_eq!(error.kind, ProviderErrorKind::Unsupported);
    assert!(server.requests().is_empty(), "rejected before any HTTP");

    let injected = provider(&server).with_id("deepseek").with_default_profile(
        AnthropicModelProfile::latest()
            .with_efforts(vec![
                ReasoningEffort::Off,
                ReasoningEffort::Low,
                ReasoningEffort::High,
                ReasoningEffort::Max,
            ])
            .with_thinking_default_on(true),
    );
    drain(&injected, request).await;
    let body = server.requests()[0].json_body();
    assert_eq!(body["thinking"], serde_json::json!({"type": "disabled"}));
    assert!(body.get("output_config").is_none(), "{body}");
}

#[tokio::test]
async fn profile_lookup_prefers_model_entries_then_default_then_builtin() {
    let off = || {
        AnthropicModelProfile::latest()
            .with_efforts(vec![ReasoningEffort::Off, ReasoningEffort::Low])
            .with_thinking_default_on(true)
    };
    let low_only = || AnthropicModelProfile::latest().with_efforts(vec![ReasoningEffort::Low]);
    let provider = AnthropicProvider::new("k")
        .with_default_profile(off())
        .with_model_profile("deepseek-v4-pro", low_only())
        .with_model_profile("deepseek-v4", off())
        // An application entry overrides an official id.
        .with_model_profile("claude-opus-5-5", off().with_forced_tool_choice(true));
    let server = MockServer::start((0..4).map(|_| Reply::sse(empty_sse())).collect());
    let provider = provider.with_base_url(server.base_url());
    let mut accepted = Vec::new();
    for model in [
        "deepseek-v4-pro-0601",
        "deepseek-v4-flash",
        "claude-sonnet-5-5",
        "claude-opus-5-5",
        "deepseek-flash",
    ] {
        let mut request = request_for(model);
        request.reasoning = effort(ReasoningEffort::Off);
        if drain(&provider, request).await[0].is_ok() {
            accepted.push(model);
        }
    }
    // The first registered model entry wins over a later shorter prefix; the
    // instance default beats the built-in table (Sonnet 5.5 rejects `Off`).
    assert_eq!(
        accepted,
        [
            "deepseek-v4-flash",
            "claude-sonnet-5-5",
            "claude-opus-5-5",
            "deepseek-flash"
        ]
    );
    // The application entry's capabilities replace the official row's.
    let mut spec = model();
    spec.model = ModelId::new("claude-opus-5-5");
    let capabilities = asterpath_protocol::Provider::capabilities(&provider, &spec);
    assert!(capabilities.forced_tool_choice);
    assert!(!capabilities.sampling);

    // Without injection: built-in table, then UNRECOGNIZED.
    let plain = AnthropicProvider::new("k");
    let mut spec = model();
    spec.model = ModelId::new("claude-sonnet-4-5");
    assert!(asterpath_protocol::Provider::capabilities(&plain, &spec).sampling);
    spec.model = ModelId::new("deepseek-flash");
    assert!(!asterpath_protocol::Provider::capabilities(&plain, &spec).sampling);
}

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
    fn marks(&self) -> Vec<(&'static str, String, String)> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .map(|mark| (mark.name, mark.kind.clone(), mark.reason.clone()))
            .collect()
    }
}

fn dropped_reasoning(reason: &str) -> Vec<(&'static str, String, String)> {
    vec![(
        TraceMark::DROPPED_ITEMS,
        "reasoning".to_owned(),
        reason.to_owned(),
    )]
}

const THINKING: &str = r#"{"type":"thinking","thinking":"plan","signature":"sig-1"}"#;
const LATER_THINKING: &str = r#"{"type":"thinking","thinking":"again","signature":"sig-2"}"#;

fn thinking(model_id: &str, raw: &str) -> Item {
    Item::Reasoning {
        summary: Vec::new(),
        binding: ReasoningBinding {
            provider: model().provider,
            model: ModelId::new(model_id),
            prefix_epoch: 0,
            tools_hash: asterpath_protocol::tools_hash(&[], &[]),
        },
        raw: RawProviderData {
            provider: model().provider,
            model: Some(ModelId::new(model_id)),
            kind: "anthropic.thinking".to_owned(),
            json: JsonString::new(raw),
        },
    }
}

/// A tool loop with an instruction appended after the tool result.
fn instructed_request(model_id: &str) -> ModelRequest {
    let mut request = request_for(model_id);
    request.items = vec![
        user_message(vec![text("hi")]),
        thinking(model_id, THINKING),
        Item::ToolCall {
            call_id: "toolu_1".to_owned(),
            provider_item_id: None,
            name: ToolName::new("echo"),
            raw_args: JsonString::new("{}"),
            raw: None,
        },
        Item::ToolResult {
            call_id: "toolu_1".to_owned(),
            provider_item_id: None,
            parts: vec![text("done")],
            is_error: false,
            raw: None,
        },
        Item::Instruction {
            key: None,
            parts: vec![text("answer in French")],
        },
    ];
    request
}

#[tokio::test]
async fn an_instruction_renders_as_a_system_message_and_keeps_old_thinking() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    drain(&provider(&server), instructed_request("claude-opus-5-5")).await;
    let request = &server.requests()[0];
    assert!(request.body.contains(THINKING), "old thinking is replayed");
    let body = request.json_body();
    assert_eq!(body["system"][0]["text"], "be helpful");
    assert_eq!(
        body["system"].as_array().unwrap().len(),
        1,
        "top-level system is unchanged"
    );
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 4, "{body}");
    assert_eq!(messages[2]["content"][0]["type"], "tool_result");
    assert_eq!(messages[3]["role"], "system");
    assert_eq!(messages[3]["content"][0]["text"], "answer in French");
}

#[tokio::test]
async fn an_instruction_followed_by_user_input_moves_after_it() {
    // The API accepts a system message only before an assistant turn or at the end.
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let mut request = request_for("claude-opus-5-5");
    request.items = vec![
        user_message(vec![text("first")]),
        Item::Instruction {
            key: None,
            parts: vec![text("rule")],
        },
        user_message(vec![text("second")]),
        assistant_message(vec![text("ok")]),
    ];
    drain(&provider(&server), request).await;
    let body = server.requests()[0].json_body();
    let roles: Vec<&str> = body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|message| message["role"].as_str().unwrap())
        .collect();
    assert_eq!(roles, ["user", "system", "assistant"], "{body}");
    assert_eq!(body["messages"][0]["content"][1]["text"], "second");
}

#[tokio::test]
async fn without_inline_changes_an_instruction_joins_the_top_level_system_and_drops_old_thinking() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let sink = Arc::new(RecordingSink::default());
    let provider = provider(&server)
        .with_model_profile(
            "claude-opus-5-5",
            AnthropicModelProfile::latest().with_inline_changes(false),
        )
        .with_trace_sink(sink.clone());
    let mut request = instructed_request("claude-opus-5-5");
    request
        .items
        .push(thinking("claude-opus-5-5", LATER_THINKING));
    request
        .items
        .push(assistant_message(vec![text("d'accord")]));
    request.items.push(user_message(vec![text("merci")]));
    drain(&provider, request).await;
    let sent = &server.requests()[0];
    assert!(
        !sent.body.contains(THINKING),
        "thinking before the change is dropped"
    );
    assert!(
        sent.body.contains(LATER_THINKING),
        "thinking after it is kept"
    );
    assert_eq!(sink.marks(), dropped_reasoning("system_changed"));
    let body = sent.json_body();
    let system: Vec<&str> = body["system"]
        .as_array()
        .unwrap()
        .iter()
        .map(|block| block["text"].as_str().unwrap())
        .collect();
    assert_eq!(system, ["be helpful", "answer in French"]);
    assert!(
        body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .all(|message| message["role"] != "system"),
        "{body}"
    );
}

/// A tool loop whose tool set changed after the tool result: `echo` was
/// the baseline, `write` replaced it.
fn tools_changed_request(model_id: &str) -> ModelRequest {
    let mut request = instructed_request(model_id);
    request.items.pop();
    if let Item::Reasoning { binding, .. } = &mut request.items[1] {
        binding.tools_hash = asterpath_protocol::tools_hash(&[descriptor("echo")], &[]);
    }
    request.items.push(Item::ToolsChanged {
        added: vec![descriptor("write")],
        removed: vec![ToolName::new("echo")],
    });
    request.baseline_tools = vec![descriptor("echo")];
    request.tools = vec![descriptor("write")];
    request
}

fn tool_names(body: &serde_json::Value) -> Vec<&str> {
    body["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect()
}

const INLINE_TOOLS_BETA: &str = "anthropic-beta: inline-tools-2026-09-15";

#[tokio::test]
async fn a_tool_change_renders_inline_and_keeps_the_top_level_tools_and_old_thinking() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    drain(&provider(&server), tools_changed_request("claude-opus-5-5")).await;
    let sent = &server.requests()[0];
    assert!(
        sent.head.to_ascii_lowercase().contains(INLINE_TOOLS_BETA),
        "{}",
        sent.head
    );
    assert!(sent.body.contains(THINKING), "old thinking is replayed");
    let body = sent.json_body();
    assert_eq!(
        tool_names(&body),
        ["echo"],
        "top-level tools stay the baseline"
    );
    let messages = body["messages"].as_array().unwrap();
    let last = messages.last().unwrap();
    assert_eq!(last["role"], "system", "{body}");
    assert_eq!(
        last["content"][0],
        serde_json::json!({
            "type": "tool_addition",
            "tool": {
                "type": "tool_definition",
                "definition": {
                    "name": "write",
                    "description": "write tool",
                    "input_schema": descriptor("write").schema,
                },
            },
        })
    );
    assert_eq!(last["content"][1]["type"], "tool_removal", "{body}");
    assert_eq!(
        last["content"][1]["tool"],
        serde_json::json!({"type": "tool_reference", "name": "echo"})
    );
}

#[tokio::test]
async fn without_inline_changes_a_tool_change_rewrites_the_top_level_tools_and_drops_old_thinking()
{
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let sink = Arc::new(RecordingSink::default());
    let provider = provider(&server)
        .with_model_profile(
            "claude-opus-5-5",
            AnthropicModelProfile::latest().with_inline_changes(false),
        )
        .with_trace_sink(sink.clone());
    drain(&provider, tools_changed_request("claude-opus-5-5")).await;
    assert_eq!(sink.marks(), dropped_reasoning("tools_hash_mismatch"));
    let sent = &server.requests()[0];
    assert!(
        !sent.head.to_ascii_lowercase().contains("anthropic-beta"),
        "{}",
        sent.head
    );
    assert!(
        !sent.body.contains(THINKING),
        "thinking bound to the old tools is dropped"
    );
    let body = sent.json_body();
    assert_eq!(
        tool_names(&body),
        ["write"],
        "top-level tools are the effective set"
    );
    assert!(
        body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .all(|message| message["role"] != "system"),
        "{body}"
    );
}

/// A second turn after the effort changed from `from` to `to`.
fn effort_changed_request(
    model_id: &str,
    from: ReasoningEffort,
    to: ReasoningEffort,
) -> ModelRequest {
    let mut request = request_for(model_id);
    request.baseline_reasoning = effort(from);
    request.reasoning = effort(to);
    request.items = vec![
        user_message(vec![text("hi")]),
        assistant_message(vec![text("ok")]),
        user_message(vec![text("more")]),
        Item::ReasoningChanged {
            reasoning: effort(to),
        },
    ];
    request
}

const OUTPUT_CONFIG_BETA: &str = "mid-conversation-output-config-2026-07-01";

#[tokio::test]
async fn an_effort_change_renders_as_an_effort_only_system_message_and_keeps_the_top_level_effort()
{
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let request = effort_changed_request(
        "claude-opus-5-5",
        ReasoningEffort::High,
        ReasoningEffort::Low,
    );
    drain(&provider(&server), request).await;
    let sent = &server.requests()[0];
    assert!(
        sent.head.to_ascii_lowercase().contains(OUTPUT_CONFIG_BETA),
        "{}",
        sent.head
    );
    let body = sent.json_body();
    assert_eq!(body["output_config"]["effort"], "high", "{body}");
    let messages = body["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 4, "{body}");
    assert_eq!(
        messages[3],
        serde_json::json!({"role": "system", "content": [], "output_config": {"effort": "low"}})
    );
    assert_eq!(
        messages[2]["content"][0]["cache_control"]["type"], "ephemeral",
        "the tail breakpoint skips the empty system message: {body}"
    );
}

#[tokio::test]
async fn an_effort_change_joins_a_pending_system_message() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let mut request = effort_changed_request(
        "claude-opus-5-5",
        ReasoningEffort::High,
        ReasoningEffort::Low,
    );
    request.items.insert(
        3,
        Item::Instruction {
            key: None,
            parts: vec![text("rule")],
        },
    );
    drain(&provider(&server), request).await;
    let body = server.requests()[0].json_body();
    let last = body["messages"].as_array().unwrap().last().unwrap().clone();
    assert_eq!(last["role"], "system", "{body}");
    assert_eq!(last["content"][0]["text"], "rule");
    assert_eq!(last["output_config"], serde_json::json!({"effort": "low"}));
}

#[tokio::test]
async fn without_inline_effort_an_effort_change_rewrites_the_top_level_effort() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let provider = provider(&server).with_model_profile(
        "claude-opus-5-5",
        AnthropicModelProfile::latest().with_inline_effort(false),
    );
    let request = effort_changed_request(
        "claude-opus-5-5",
        ReasoningEffort::High,
        ReasoningEffort::Low,
    );
    drain(&provider, request).await;
    let sent = &server.requests()[0];
    assert!(!sent.head.to_ascii_lowercase().contains(OUTPUT_CONFIG_BETA));
    let body = sent.json_body();
    assert_eq!(body["output_config"]["effort"], "low");
    assert_eq!(body["messages"].as_array().unwrap().len(), 3, "{body}");
}

#[tokio::test]
async fn turning_reasoning_off_rewrites_the_top_level_even_with_inline_effort() {
    // Off changes `thinking`, which only the top level carries.
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let request =
        effort_changed_request("claude-opus-5", ReasoningEffort::High, ReasoningEffort::Off);
    drain(&provider(&server), request).await;
    let sent = &server.requests()[0];
    assert!(!sent.head.to_ascii_lowercase().contains(OUTPUT_CONFIG_BETA));
    let body = sent.json_body();
    assert_eq!(body["thinking"]["type"], "disabled", "{body}");
    assert!(body.get("output_config").is_none(), "{body}");
    assert_eq!(body["messages"].as_array().unwrap().len(), 3, "{body}");
}

fn answer_format() -> Option<asterpath_protocol::OutputFormat> {
    Some(asterpath_protocol::OutputFormat::new(
        "answer",
        serde_json::json!({
            "type": "object",
            "properties": {"answer": {"type": "string"}},
            "required": ["answer"],
            "additionalProperties": false
        }),
    ))
}

#[tokio::test]
async fn output_format_renders_as_output_config_format_beside_effort() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let mut request = request_for("claude-opus-5-5");
    request.output_format = answer_format();
    request.reasoning = effort(ReasoningEffort::High);

    drain(&provider(&server), request).await;
    let sent = &server.requests()[0];
    assert_eq!(
        sent.json_body()["output_config"],
        serde_json::json!({
            "effort": "high",
            "format": {
                "type": "json_schema",
                "schema": {
                    "type": "object",
                    "properties": {"answer": {"type": "string"}},
                    "required": ["answer"],
                    "additionalProperties": false
                }
            }
        })
    );
    assert!(
        !sent.head.to_ascii_lowercase().contains("anthropic-beta"),
        "structured outputs are GA"
    );
}

#[tokio::test]
async fn output_format_is_unsupported_before_http_where_the_model_lacks_it() {
    let server = MockServer::start(Vec::new());
    // Claude Sonnet 4 and Claude 3 predate structured outputs; unrecognized
    // ids (third-party endpoints) are not assumed to have them.
    for model in [
        "claude-sonnet-4-20250514",
        "claude-3-haiku",
        "deepseek-v4-pro",
    ] {
        let mut request = request_for(model);
        request.output_format = answer_format();
        let error = one_request(&server, request).await;
        assert_eq!(error.kind, ProviderErrorKind::Unsupported, "{model}");
    }
    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn tool_strict_is_sent_only_when_set() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let mut request = base_request();
    let mut strict = descriptor("strict");
    strict.strict = Some(true);
    let mut loose = descriptor("loose");
    loose.strict = Some(false);
    request.tools = vec![strict, loose, descriptor("unset")];
    request.baseline_tools = request.tools.clone();

    drain(&provider(&server), request).await;
    let body = server.requests()[0].json_body();
    assert_eq!(body["tools"][0]["strict"], true);
    assert_eq!(body["tools"][1]["strict"], false);
    assert!(body["tools"][2].get("strict").is_none());
}

const WEB_SEARCH: &str = r#"{"type":"web_search_20250305","name":"web_search","max_uses":3}"#;

fn web_search() -> serde_json::Value {
    serde_json::from_str(WEB_SEARCH).unwrap()
}

fn with_web_search(mut request: ModelRequest) -> ModelRequest {
    request.server_tools.insert(
        ProviderId::new("anthropic"),
        vec![JsonString::new(WEB_SEARCH)],
    );
    request.server_tools.insert(
        ProviderId::new("openai"),
        vec![JsonString::new(r#"{"type":"web_search"}"#)],
    );
    request
}

fn anthropic_raw(kind: &str, json: &str) -> RawProviderData {
    RawProviderData {
        provider: ProviderId::new("anthropic"),
        model: Some(ModelId::new("claude-opus-5-5")),
        kind: kind.to_owned(),
        json: JsonString::new(json),
    }
}

#[test]
fn server_tools_are_advertised_for_listed_models_only() {
    use asterpath_protocol::Provider;
    let provider = AnthropicProvider::new("key");
    let spec = |id: &str| asterpath_protocol::ModelSpec {
        model: ModelId::new(id),
        ..model()
    };
    assert!(provider.capabilities(&spec("claude-opus-5-5")).server_tools);
    assert!(
        provider
            .capabilities(&spec("claude-sonnet-4-5"))
            .server_tools
    );
    assert!(!provider.capabilities(&spec("claude-test")).server_tools);
    assert!(!provider.capabilities(&spec("deepseek-v4-pro")).server_tools);
}

#[tokio::test]
async fn server_tools_append_after_the_baseline_on_the_inline_path_and_keep_old_thinking() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let mut request = with_web_search(tools_changed_request("claude-opus-5-5"));
    if let Item::Reasoning { binding, .. } = &mut request.items[1] {
        binding.tools_hash = asterpath_protocol::tools_hash(&[descriptor("echo")], &[web_search()]);
    }
    const CALL: &str =
        r#"{"type":"server_tool_use","id":"srvtoolu_1","name":"web_search","input":{"query":"q"}}"#;
    const RESULT: &str =
        r#"{"type":"web_search_tool_result","tool_use_id":"srvtoolu_1","content":[]}"#;
    request.items.insert(
        2,
        Item::ServerToolCall {
            provider: ProviderId::new("anthropic"),
            name: "web_search".to_owned(),
            call_id: Some("srvtoolu_1".to_owned()),
            raw: anthropic_raw("anthropic.server_tool_use", CALL),
        },
    );
    request.items.insert(
        3,
        Item::ServerToolResult {
            provider: ProviderId::new("anthropic"),
            call_id: Some("srvtoolu_1".to_owned()),
            parts: Vec::new(),
            raw: anthropic_raw("anthropic.web_search_tool_result", RESULT),
        },
    );

    drain(&provider(&server), request).await;
    let sent = &server.requests()[0];
    assert!(sent.body.contains(THINKING), "old thinking is replayed");
    assert!(sent.body.contains(CALL) && sent.body.contains(RESULT));
    let body = sent.json_body();
    let tools = body["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 2, "{body}");
    assert_eq!(tools[0]["name"], "echo");
    assert_eq!(tools[1], web_search(), "verbatim, no breakpoint added");
}

#[tokio::test]
async fn server_tools_append_after_the_effective_tools_on_the_fallback_path() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let mut request = with_web_search(request_for("claude-opus-4-7"));
    request.tools = vec![descriptor("echo")];
    drain(&provider(&server), request).await;
    let body = server.requests()[0].json_body();
    assert_eq!(body["tools"][0]["name"], "echo");
    assert_eq!(body["tools"][1], web_search());
    assert_eq!(body["tools"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn server_tools_alone_render_tools_and_tool_choice() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    drain(
        &provider(&server),
        with_web_search(request_for("claude-opus-5-5")),
    )
    .await;
    let body = server.requests()[0].json_body();
    assert_eq!(body["tools"], serde_json::json!([web_search()]));
    assert_eq!(body["tool_choice"], serde_json::json!({"type": "auto"}));
}

#[tokio::test]
async fn reasoning_binds_the_fingerprint_with_server_tools() {
    let thinking_sse = common::sse_body(&[
        (
            "message_start",
            r#"{"type":"message_start","message":{"id":"msg_t","usage":{"input_tokens":1}}}"#,
        ),
        (
            "content_block_start",
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}"#,
        ),
        (
            "content_block_delta",
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"s"}}"#,
        ),
        (
            "content_block_stop",
            r#"{"type":"content_block_stop","index":0}"#,
        ),
        (
            "message_delta",
            r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}"#,
        ),
        ("message_stop", r#"{"type":"message_stop"}"#),
    ]);
    let server = MockServer::start(vec![Reply::sse(thinking_sse)]);
    let mut request = with_web_search(request_for("claude-opus-5-5"));
    request.tools = vec![descriptor("echo")];
    request.baseline_tools = request.tools.clone();
    let events = drain(&provider(&server), request).await;
    let binding = events
        .iter()
        .find_map(|event| match event {
            Ok(asterpath_protocol::StreamEvent::ItemCompleted {
                item: Item::Reasoning { binding, .. },
                ..
            }) => Some(binding.clone()),
            _ => None,
        })
        .expect("reasoning item");
    assert_eq!(
        binding.tools_hash,
        asterpath_protocol::tools_hash(&[descriptor("echo")], &[web_search()])
    );
}

#[tokio::test]
async fn server_tools_fail_before_http_when_unsupported_or_not_objects() {
    let server = MockServer::start(Vec::new());
    let error = one_request(&server, with_web_search(request_for("claude-test"))).await;
    assert_eq!(error.kind, ProviderErrorKind::Unsupported);

    let mut not_object = request_for("claude-opus-5-5");
    not_object.server_tools.insert(
        ProviderId::new("anthropic"),
        vec![JsonString::new(r#"["web_search"]"#)],
    );
    let error = one_request(&server, not_object).await;
    assert_eq!(error.kind, ProviderErrorKind::InvalidRequest);

    assert!(server.requests().is_empty());
}

#[tokio::test]
async fn invalid_requests_fail_before_any_http() {
    let server = MockServer::start(Vec::new());

    let mut other_provider = base_request();
    other_provider.spec.provider = ProviderId::new("other");
    let error = one_request(&server, other_provider).await;
    assert_eq!(error.kind, ProviderErrorKind::InvalidRequest);

    let mut non_text_instruction = base_request();
    non_text_instruction.items = vec![
        user_message(vec![text("go")]),
        Item::Instruction {
            key: None,
            parts: vec![Part::Image {
                media_type: "image/png".to_owned(),
                source: ImageSource::Base64 {
                    data: "AAAA".to_owned(),
                },
            }],
        },
    ];
    let error = one_request(&server, non_text_instruction).await;
    assert_eq!(error.kind, ProviderErrorKind::InvalidRequest);

    assert!(
        server.requests().is_empty(),
        "invalid requests must not reach HTTP"
    );
}

#[tokio::test]
async fn reasoning_is_dropped_and_traced_when_its_binding_does_not_match() {
    let mismatched = |change: fn(&mut ReasoningBinding, &mut RawProviderData)| {
        let mut item = thinking("claude-opus-5-5", THINKING);
        if let Item::Reasoning { binding, raw, .. } = &mut item {
            change(binding, raw);
        }
        item
    };
    let cases = [
        (
            mismatched(|binding, _| binding.provider = ProviderId::new("other")),
            "cross_provider",
        ),
        (
            mismatched(|_, raw| raw.provider = ProviderId::new("other")),
            "cross_provider",
        ),
        (
            mismatched(|binding, _| binding.model = ModelId::new("claude-other")),
            "model_mismatch",
        ),
        (
            mismatched(|binding, _| binding.prefix_epoch = 1),
            "prefix_epoch_mismatch",
        ),
        (
            mismatched(|binding, _| binding.tools_hash = "stale".to_owned()),
            "tools_hash_mismatch",
        ),
    ];
    for (item, reason) in cases {
        let server = MockServer::start(vec![Reply::sse(empty_sse())]);
        let sink = Arc::new(RecordingSink::default());
        let provider = provider(&server).with_trace_sink(sink.clone());
        let mut request = request_for("claude-opus-5-5");
        request.items = vec![
            user_message(vec![text("hi")]),
            item,
            assistant_message(vec![text("ok")]),
            user_message(vec![text("next")]),
        ];
        drain(&provider, request).await;
        assert!(!server.requests()[0].body.contains(THINKING), "{reason}");
        assert_eq!(sink.marks(), dropped_reasoning(reason));
    }
}
