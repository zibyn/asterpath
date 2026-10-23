//! `ModelRequest` -> Anthropic Messages request body.
//!
//! Rendering is deterministic: the same log and request always produce the same
//! bytes. Persisted provider raw JSON is spliced into the content arrays
//! verbatim and is never parsed or re-serialized.

use asterpath_protocol::{
    ImageSource, Item, ModelRequest, Part, ProviderCapabilities, ProviderError, ProviderErrorKind,
    ProviderId, RawProviderData, ReasoningBinding, Role, ToolChoice,
};
use serde_json::{Value, json};

pub(crate) const ANTHROPIC_VERSION: &str = "2023-06-01";
/// One rendered JSON fragment: ordinary values are serialized compactly,
/// replayed provider raw data is inserted exactly as persisted.
enum Frag {
    Json(Value),
    Raw(String),
}

impl Frag {
    fn write(self, out: &mut String) {
        match self {
            Self::Json(value) => {
                out.push_str(&serde_json::to_string(&value).expect("JSON value serializes"));
            }
            Self::Raw(raw) => out.push_str(&raw),
        }
    }
}

fn object(fields: Vec<(&str, Frag)>) -> String {
    let mut out = String::from("{");
    for (position, (key, value)) in fields.into_iter().enumerate() {
        if position > 0 {
            out.push(',');
        }
        out.push_str(&serde_json::to_string(key).expect("key serializes"));
        out.push(':');
        value.write(&mut out);
    }
    out.push('}');
    out
}

fn array(items: Vec<Frag>) -> String {
    let mut out = String::from("[");
    for (position, item) in items.into_iter().enumerate() {
        if position > 0 {
            out.push(',');
        }
        item.write(&mut out);
    }
    out.push(']');
    out
}

/// Capability and structural checks that must run before any HTTP request.
pub(crate) fn check(
    capabilities: ProviderCapabilities,
    request: &ModelRequest,
) -> Result<(), ProviderError> {
    if let Some(feature) = capabilities.unsupported_feature(request) {
        return Err(unsupported(feature));
    }
    if !request
        .system
        .iter()
        .all(|part| matches!(part, Part::Text { .. }))
    {
        return Err(invalid_request("system may only contain text parts"));
    }
    if request
        .sampling
        .as_ref()
        .is_some_and(|sampling| sampling.seed.is_some())
    {
        return Err(unsupported("sampling.seed"));
    }
    if let Some(reasoning) = &request.reasoning {
        // Anthropic has no `effort`; `summary` is ignored by design.
        if reasoning.effort.is_some() {
            return Err(unsupported("reasoning.effort"));
        }
        if reasoning.budget_tokens.is_some() && !capabilities.reasoning_roundtrip {
            return Err(unsupported("thinking"));
        }
    }
    Ok(())
}

fn unsupported(detail: &str) -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::Unsupported,
        format!("{detail} is not supported by the Anthropic Messages adapter"),
    )
}

fn invalid_request(message: impl Into<String>) -> ProviderError {
    ProviderError::new(ProviderErrorKind::InvalidRequest, message)
}

/// Render the full Messages request body. Capability checks are the caller's
/// responsibility so they can run before anything is serialized.
pub(crate) fn render(
    provider: &ProviderId,
    request: &ModelRequest,
) -> Result<String, ProviderError> {
    let tools_hash = asterpath_protocol::tools_hash(&request.tools);
    let messages = render_messages(provider, request, &tools_hash)?;

    let mut fields: Vec<(&str, Frag)> = vec![
        ("model", Frag::Json(json!(request.spec.model.as_str()))),
        ("max_tokens", Frag::Json(json!(request.max_output_tokens))),
    ];
    if !request.system.is_empty() {
        let blocks: Vec<Value> = request
            .system
            .iter()
            .map(|part| match part {
                Part::Text { text } => json!({"type": "text", "text": text}),
                _ => unreachable!("check rejects non-text system parts"),
            })
            .collect();
        fields.push(("system", Frag::Json(Value::Array(blocks))));
    }
    fields.push(("messages", Frag::Raw(messages)));
    if !request.tools.is_empty() {
        let tools = request
            .tools
            .iter()
            .map(|tool| {
                json!({
                    "name": tool.name.as_str(),
                    "description": tool.description,
                    "input_schema": tool.schema,
                })
            })
            .collect();
        fields.push(("tools", Frag::Json(Value::Array(tools))));
        let mut choice = match &request.tool_choice {
            ToolChoice::Auto => json!({"type": "auto"}),
            ToolChoice::None => json!({"type": "none"}),
            ToolChoice::Required => json!({"type": "any"}),
            ToolChoice::Tool(name) => json!({"type": "tool", "name": name.as_str()}),
        };
        if request.disable_parallel_tool_use {
            choice["disable_parallel_tool_use"] = json!(true);
        }
        fields.push(("tool_choice", Frag::Json(choice)));
    }
    if let Some(sampling) = &request.sampling {
        if let Some(temperature) = sampling.temperature {
            fields.push((
                "temperature",
                Frag::Raw(serde_json::to_string(&temperature).expect("f32 serializes")),
            ));
        }
        if let Some(top_p) = sampling.top_p {
            fields.push((
                "top_p",
                Frag::Raw(serde_json::to_string(&top_p).expect("f32 serializes")),
            ));
        }
        if let Some(top_k) = sampling.top_k {
            fields.push(("top_k", Frag::Json(json!(top_k))));
        }
    }
    if let Some(budget) = request.reasoning.as_ref().and_then(|r| r.budget_tokens) {
        fields.push((
            "thinking",
            Frag::Json(json!({"type": "enabled", "budget_tokens": budget})),
        ));
    }
    fields.push(("stream", Frag::Json(json!(true))));
    Ok(object(fields))
}

struct PendingMessage {
    role: &'static str,
    tool_results: Vec<Frag>,
    other: Vec<Frag>,
}

fn render_messages(
    provider: &ProviderId,
    request: &ModelRequest,
    tools_hash: &str,
) -> Result<String, ProviderError> {
    let mut messages: Vec<PendingMessage> = Vec::new();
    for item in &request.items {
        match item {
            Item::Message { role, parts } => {
                let blocks: Vec<Frag> = parts
                    .iter()
                    .filter_map(|part| render_part(provider, part))
                    .collect();
                if !blocks.is_empty() {
                    push_blocks(&mut messages, role_name(*role), Vec::new(), blocks);
                }
            }
            Item::Reasoning { binding, raw, .. } => {
                if replayable(binding, raw, request, tools_hash) {
                    push_blocks(
                        &mut messages,
                        "assistant",
                        Vec::new(),
                        vec![Frag::Raw(raw.json.as_str().to_owned())],
                    );
                }
            }
            Item::ToolCall {
                call_id,
                name,
                raw_args,
                ..
            } => {
                let input: Value = serde_json::from_str(raw_args.as_str()).map_err(|error| {
                    invalid_request(format!("tool call {call_id} raw_args is not JSON: {error}"))
                })?;
                if !input.is_object() {
                    return Err(invalid_request(format!(
                        "tool call {call_id} raw_args must be a JSON object"
                    )));
                }
                push_blocks(
                    &mut messages,
                    "assistant",
                    Vec::new(),
                    vec![Frag::Json(json!({
                        "type": "tool_use",
                        "id": call_id,
                        "name": name.as_str(),
                        "input": input,
                    }))],
                );
            }
            Item::ToolResult {
                call_id,
                parts,
                is_error,
                ..
            } => {
                let content: Vec<Frag> = parts
                    .iter()
                    .filter_map(|part| render_part(provider, part))
                    .collect();
                let block = object(vec![
                    ("type", Frag::Json(json!("tool_result"))),
                    ("tool_use_id", Frag::Json(json!(call_id))),
                    ("content", Frag::Raw(array(content))),
                    ("is_error", Frag::Json(json!(is_error))),
                ]);
                push_blocks(&mut messages, "user", vec![Frag::Raw(block)], Vec::new());
            }
            Item::ServerToolCall {
                provider: item_provider,
                raw,
                ..
            }
            | Item::ServerToolResult {
                provider: item_provider,
                raw,
                ..
            }
            | Item::ProviderOpaque {
                provider: item_provider,
                raw,
                ..
            } => {
                if item_provider == provider {
                    push_blocks(
                        &mut messages,
                        "assistant",
                        Vec::new(),
                        vec![Frag::Raw(raw.json.as_str().to_owned())],
                    );
                }
            }
            Item::Compaction { parts } => {
                let blocks: Vec<Frag> = parts
                    .iter()
                    .filter_map(|part| render_part(provider, part))
                    .collect();
                if !blocks.is_empty() {
                    push_blocks(&mut messages, "user", Vec::new(), blocks);
                }
            }
        }
    }

    let rendered = messages
        .into_iter()
        .map(|message| {
            let mut content = message.tool_results;
            content.extend(message.other);
            Frag::Raw(object(vec![
                ("role", Frag::Json(json!(message.role))),
                ("content", Frag::Raw(array(content))),
            ]))
        })
        .collect();
    Ok(array(rendered))
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::User => "user",
        Role::Assistant => "assistant",
    }
}

fn push_blocks(
    messages: &mut Vec<PendingMessage>,
    role: &'static str,
    tool_results: Vec<Frag>,
    other: Vec<Frag>,
) {
    match messages.last_mut() {
        Some(last) if last.role == role => {
            last.tool_results.extend(tool_results);
            last.other.extend(other);
        }
        _ => messages.push(PendingMessage {
            role,
            tool_results,
            other,
        }),
    }
}

fn render_part(provider: &ProviderId, part: &Part) -> Option<Frag> {
    match part {
        Part::Text { text } | Part::Refusal { text } => {
            Some(Frag::Json(json!({"type": "text", "text": text})))
        }
        Part::Image { media_type, source } => Some(Frag::Json(match source {
            ImageSource::Base64 { data } => json!({
                "type": "image",
                "source": {"type": "base64", "media_type": media_type, "data": data},
            }),
            ImageSource::Url { url } => json!({
                "type": "image",
                "source": {"type": "url", "url": url},
            }),
        })),
        Part::ProviderOpaque {
            provider: item_provider,
            raw,
            ..
        } => (item_provider == provider).then(|| Frag::Raw(raw.json.as_str().to_owned())),
    }
}

fn replayable(
    binding: &ReasoningBinding,
    raw: &RawProviderData,
    request: &ModelRequest,
    tools_hash: &str,
) -> bool {
    binding.provider == request.spec.provider
        && raw.provider == request.spec.provider
        && binding.model == request.spec.model
        && binding.prefix_epoch == request.prefix_epoch
        && binding.tools_hash == tools_hash
}
