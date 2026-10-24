//! Request rendering: `ModelRequest` -> Anthropic Messages HTTP body.

mod common;

use asterpath_protocol::{
    ImageSource, Item, JsonString, ModelId, ModelRequest, Part, ProviderErrorKind, ProviderId,
    RawProviderData, ReasoningBinding, ReasoningEffort, ReasoningParams, Role, SamplingParams,
    ToolChoice, ToolName,
};
use common::{
    MockServer, Reply, base_request, descriptor, drain, empty_sse, model, provider, text,
};

fn request_for(model: &str) -> ModelRequest {
    let mut request = base_request();
    request.spec.model = ModelId::new(model);
    request
}

fn effort(effort: ReasoningEffort) -> Option<ReasoningParams> {
    Some(ReasoningParams {
        effort: Some(effort),
        ..ReasoningParams::default()
    })
}

fn budget(tokens: u32) -> Option<ReasoningParams> {
    Some(ReasoningParams {
        budget_tokens: Some(tokens),
        ..ReasoningParams::default()
    })
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
    request.tool_choice = ToolChoice::Required;
    request.disable_parallel_tool_use = true;
    request.sampling = Some(SamplingParams {
        temperature: Some(0.4),
        top_p: Some(0.9),
        top_k: Some(5),
        seed: None,
    });
    request.reasoning = Some(ReasoningParams {
        budget_tokens: Some(2048),
        ..ReasoningParams::default()
    });
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
            {"type": "tool_use", "id": "toolu_b", "name": "search", "input": {"q": "b"}}
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
            {"type": "image", "source": {"type": "url", "url": "https://example.invalid/x.jpg"}}
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

#[tokio::test]
async fn unsupported_request_fields_fail_before_any_http() {
    let server = MockServer::start(Vec::new());

    let mut seed = base_request();
    seed.sampling = Some(SamplingParams {
        seed: Some(7),
        ..SamplingParams::default()
    });
    let error = one_request(&server, seed).await;
    assert_eq!(error.kind, ProviderErrorKind::Unsupported);
    assert!(!error.retryable);

    // Effort is documented only for Opus 4.5 among extended-thinking-only models.
    let mut effort = request_for("claude-sonnet-4-5");
    effort.reasoning = Some(ReasoningParams {
        effort: Some(ReasoningEffort::High),
        ..ReasoningParams::default()
    });
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
        request.reasoning = Some(ReasoningParams {
            effort: Some(ReasoningEffort::Off),
            summary: true,
            ..ReasoningParams::default()
        });
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
        request.reasoning = Some(ReasoningParams {
            effort: Some(ReasoningEffort::Off),
            summary: true,
            ..ReasoningParams::default()
        });
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
        request.reasoning = Some(ReasoningParams {
            budget_tokens,
            summary: true,
            ..ReasoningParams::default()
        });
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

#[test]
fn builtin_tools_are_not_advertised() {
    use asterpath_protocol::Provider;
    let provider = asterpath_provider_anthropic::AnthropicProvider::new("key");
    assert!(!provider.capabilities(&model()).builtin_tools);
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
    opus_4_5.reasoning = Some(ReasoningParams {
        effort: Some(ReasoningEffort::Low),
        budget_tokens: Some(2048),
        ..ReasoningParams::default()
    });
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
    forced.tool_choice = ToolChoice::Required;
    let mut named = request_for("claude-fable-5-1");
    named.tools = vec![descriptor("search")];
    named.tool_choice = ToolChoice::Tool(ToolName::new("search"));
    let mut sampled = request_for("claude-opus-4-7");
    sampled.sampling = Some(SamplingParams {
        temperature: Some(0.2),
        ..SamplingParams::default()
    });

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
async fn sets_cache_breakpoints_on_last_tool_system_block_and_message_block() {
    let server = MockServer::start(vec![Reply::sse(empty_sse())]);
    let mut request = base_request();
    request.system = vec![text("be helpful"), text("be terse")];
    request.tools = vec![descriptor("a"), descriptor("b")];
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
    assert_eq!(
        breakpoints(&body),
        3,
        "at most 3 of the API's 4 breakpoints"
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
    assert_eq!(breakpoints(&recorded[1].json_body()), 0);
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
async fn provider_options_conflicting_or_non_object_fail_before_http() {
    let server = MockServer::start(Vec::new());
    for (options, kind) in [
        (r#"{"model":"other"}"#, ProviderErrorKind::Unsupported),
        (r#"{"messages":[]}"#, ProviderErrorKind::Unsupported),
        (
            r#"{"thinking":{"type":"adaptive"}}"#,
            ProviderErrorKind::Unsupported,
        ),
        (
            r#"{"stop_sequences":["x"]}"#,
            ProviderErrorKind::Unsupported,
        ),
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
