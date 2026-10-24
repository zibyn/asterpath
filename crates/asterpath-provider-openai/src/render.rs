//! Canonical request rendering for the OpenAI Responses API.

use std::collections::BTreeMap;

use asterpath_http::{
    merge_provider_option, provider_options_for, same_provider_raw_field, server_tools_for,
    unrenderable_part,
};
use asterpath_protocol::{
    FileSource, ImageSource, Item, ModelRequest, Part, ProviderCapabilities, ProviderError,
    ProviderErrorKind, ProviderId, RawProviderData, ReasoningEffort, Role, ToolChoice,
};
use serde_json::value::RawValue;
use serde_json::{Map, Value, json};

use asterpath_http::{TraceMark, TraceSink};

use crate::models::OpenAiModelProfile;

/// Serialize the complete request and the `tools_hash` of its tools.
/// Deterministic: struct field order, sorted tool fingerprints and
/// `serde_json`'s ordered maps make repeated renders of the same
/// `ModelRequest` byte-identical.
pub(crate) fn render(
    request: &ModelRequest,
    provider: &ProviderId,
    capabilities: ProviderCapabilities,
    profile: &OpenAiModelProfile,
    trace: Option<&dyn TraceSink>,
) -> Result<(Vec<u8>, String), ProviderError> {
    validate(request, provider, capabilities, profile)?;
    // R1: no server state, written or not.
    let options = provider_options_for(request, &["previous_response_id", "conversation"])?;

    let server_tools = server_tools_for(request)?;

    let instructions = system_text(&request.system);
    let input = render_input(request, provider, trace)?;
    let mut tools: Vec<Value> = request
        .tools
        .iter()
        .map(|tool| {
            let mut function = json!({
                "type": "function",
                "name": tool.name.as_str(),
                "description": tool.description,
                "parameters": tool.schema,
            });
            if let Some(strict) = tool.strict {
                function["strict"] = json!(strict);
            }
            function
        })
        .collect();
    let text = request.output_format.as_ref().map(|format| {
        json!({"format": {
            "type": "json_schema",
            "name": format.name,
            "schema": format.schema,
            "strict": true,
        }})
    });
    let tools_hash = asterpath_protocol::tools_hash(&request.tools, &server_tools);
    tools.extend(server_tools);

    #[derive(serde::Serialize)]
    struct Body<'a> {
        model: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        instructions: Option<String>,
        input: Vec<Box<RawValue>>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        tools: Vec<Value>,
        tool_choice: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        parallel_tool_calls: Option<bool>,
        max_output_tokens: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        temperature: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        top_p: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        reasoning: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        text: Option<Value>,
        store: bool,
        stream: bool,
        include: &'static [&'static str],
    }

    let sampling = request.sampling.as_ref();
    let body = Body {
        model: request.spec.model.as_str(),
        instructions,
        input,
        tools,
        tool_choice: tool_choice(&request.tool_choice),
        parallel_tool_calls: request.disable_parallel_tool_use.then_some(false),
        max_output_tokens: request.max_output_tokens,
        temperature: sampling.and_then(|params| params.temperature),
        top_p: sampling.and_then(|params| params.top_p),
        reasoning: request.reasoning.as_ref().map(reasoning),
        text,
        store: false,
        stream: true,
        include: &["reasoning.encrypted_content"],
    };

    let bytes = serde_json::to_vec(&body).map_err(not_serializable)?;
    if options.is_empty() {
        return Ok((bytes, tools_hash));
    }
    // Top-level fields stay raw, so replayed input and f32 sampling bytes are untouched;
    // only a field an option merges into is parsed and re-serialized.
    let mut fields: BTreeMap<String, Box<RawValue>> =
        serde_json::from_slice(&bytes).map_err(not_serializable)?;
    for (key, value) in options {
        let value = match fields.get(&key) {
            Some(existing) => {
                let mut target: Value =
                    serde_json::from_str(existing.get()).map_err(not_serializable)?;
                merge_provider_option(&mut target, value, &key)?;
                target
            }
            None => value,
        };
        fields.insert(key, raw(value)?);
    }
    Ok((
        serde_json::to_vec(&fields).map_err(not_serializable)?,
        tools_hash,
    ))
}

fn not_serializable(error: serde_json::Error) -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::InvalidRequest,
        format!("request is not serializable: {error}"),
    )
}

fn validate(
    request: &ModelRequest,
    provider: &ProviderId,
    capabilities: ProviderCapabilities,
    profile: &OpenAiModelProfile,
) -> Result<(), ProviderError> {
    if let Some(feature) = capabilities.unsupported_feature(request) {
        return Err(unsupported(feature));
    }
    if let Some(detail) = unrenderable_part(request, &profile.file_media_types) {
        return Err(unsupported(&detail));
    }
    if request.spec.provider != *provider {
        return Err(invalid_request(format!(
            "request provider {} does not match adapter {provider}",
            request.spec.provider
        )));
    }
    if !request
        .system
        .iter()
        .all(|part| matches!(part, Part::Text { .. }))
    {
        return Err(invalid_request("system may only contain text parts"));
    }
    // The Responses create body has no `stop`/`stop_sequences` parameter
    // (https://developers.openai.com/api/reference/resources/responses/methods/create, 2026-10-24).
    if !request.stop_sequences.is_empty() {
        return Err(unsupported("stop_sequences"));
    }
    if let Some(sampling) = &request.sampling
        && sampling.top_k.is_some()
    {
        return Err(unsupported("top_k sampling"));
    }
    if let Some(sampling) = &request.sampling
        && sampling.seed.is_some()
    {
        return Err(unsupported("seed"));
    }
    if let Some(reasoning) = &request.reasoning
        && reasoning.budget_tokens.is_some()
    {
        return Err(unsupported("reasoning budget_tokens"));
    }
    if let Some(effort) = request.reasoning.as_ref().and_then(|params| params.effort)
        && !profile.efforts.contains(&effort)
    {
        return Err(unsupported(&format!(
            "reasoning effort {effort:?} on {}",
            request.spec.model
        )));
    }
    Ok(())
}

fn system_text(system: &[Part]) -> Option<String> {
    let texts: Vec<&str> = system
        .iter()
        .filter_map(|part| match part {
            Part::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    (!texts.is_empty()).then(|| texts.join("\n"))
}

fn tool_choice(choice: &ToolChoice) -> Value {
    match choice {
        ToolChoice::Auto => json!("auto"),
        ToolChoice::None => json!("none"),
        ToolChoice::Required => json!("required"),
        ToolChoice::Tool(name) => json!({"type": "function", "name": name.as_str()}),
    }
}

fn reasoning(params: &asterpath_protocol::ReasoningParams) -> Value {
    let mut object = Map::new();
    if let Some(effort) = params.effort {
        object.insert(
            "effort".to_owned(),
            json!(match effort {
                ReasoningEffort::Off => "none",
                ReasoningEffort::Minimal => "minimal",
                ReasoningEffort::Low => "low",
                ReasoningEffort::Medium => "medium",
                ReasoningEffort::High => "high",
                ReasoningEffort::XHigh => "xhigh",
                ReasoningEffort::Max => "max",
                _ => unreachable!("validate rejects efforts missing from the model table"),
            }),
        );
    }
    if params.summary {
        object.insert("summary".to_owned(), json!("auto"));
    }
    Value::Object(object)
}

fn render_input(
    request: &ModelRequest,
    provider: &ProviderId,
    trace: Option<&dyn TraceSink>,
) -> Result<Vec<Box<RawValue>>, ProviderError> {
    let mut input = Vec::with_capacity(request.items.len());
    for item in &request.items {
        match item {
            Item::Message {
                role,
                parts,
                raw: data,
            } => {
                let mut message = message_item(*role, parts, provider, trace)?;
                // Only `phase` is merged from same-provider raw; the id is never replayed (ADR 0010).
                if let Some(phase) = same_provider_raw_field(data.as_ref(), provider, "phase") {
                    message["phase"] = phase;
                }
                input.push(raw(message)?);
            }
            Item::Reasoning {
                binding, raw: data, ..
            } => {
                let reason = if binding.provider != *provider {
                    Some("cross_provider")
                } else if binding.model != request.spec.model {
                    Some("model_mismatch")
                } else if binding.prefix_epoch != request.prefix_epoch {
                    Some("prefix_epoch_mismatch")
                } else {
                    // No tools_hash check: OpenAI documents no binding between
                    // reasoning items and the tool set (#108).
                    None
                };
                match reason {
                    Some(reason) => drop_mark(provider, "reasoning", reason, trace),
                    None => input.push(raw_json(data)?),
                }
            }
            Item::ToolCall {
                call_id,
                name,
                raw_args,
                ..
            } => input.push(raw(json!({
                "type": "function_call",
                "call_id": call_id,
                "name": name.as_str(),
                "arguments": raw_args.as_str(),
            }))?),
            Item::ToolResult {
                call_id,
                parts,
                is_error,
                ..
            } => input.push(raw(json!({
                "type": "function_call_output",
                "call_id": call_id,
                "output": tool_output(parts, *is_error, provider, trace),
            }))?),
            Item::ServerToolCall {
                provider: source,
                raw: data,
                ..
            }
            | Item::ServerToolResult {
                provider: source,
                raw: data,
                ..
            }
            | Item::ProviderOpaque {
                provider: source,
                raw: data,
                ..
            } => {
                if source == provider {
                    input.push(raw_json(data)?);
                } else {
                    drop_mark(source, raw_kind(item), "cross_provider", trace);
                }
            }
            Item::Compaction { parts } => {
                input.push(raw(json!({
                    "type": "message",
                    "role": "user",
                    "content": text_content(parts, provider, trace),
                }))?);
            }
            Item::Instruction { parts } => {
                input.push(raw(json!({
                    "type": "message",
                    "role": "developer",
                    "content": text_content(parts, provider, trace),
                }))?);
            }
            // Top-level `tools` is already the effective set (ADR-0030).
            Item::ToolsChanged { .. } => {}
        }
    }
    Ok(input)
}

fn message_item(
    role: Role,
    parts: &[Part],
    provider: &ProviderId,
    trace: Option<&dyn TraceSink>,
) -> Result<Value, ProviderError> {
    let text_type = match role {
        Role::User => "input_text",
        Role::Assistant => "output_text",
    };
    let mut content = Vec::with_capacity(parts.len());
    for part in parts {
        match part {
            Part::Text { text } => content.push(json!({"type": text_type, "text": text})),
            Part::Refusal { text } => content.push(json!({"type": "refusal", "refusal": text})),
            Part::Image { media_type, source } if role == Role::User => {
                content.push(match source {
                    ImageSource::Url { url } => json!({"type": "input_image", "image_url": url}),
                    ImageSource::Base64 { data } => json!({
                        "type": "input_image",
                        "image_url": format!("data:{media_type};base64,{data}"),
                    }),
                    ImageSource::ProviderFile(file) => {
                        json!({"type": "input_image", "file_id": file.id})
                    }
                });
            }
            Part::File {
                media_type,
                filename,
                source,
            } => content.push(input_file(media_type, filename.as_ref(), source)),
            // Responses assistant output content does not accept images.
            Part::Image { .. } => return Err(unsupported("image input")),
            Part::ProviderOpaque {
                provider: source,
                kind,
                raw: data,
            } => {
                if source == provider {
                    content.push(json!(raw_json(data)?));
                } else {
                    drop_mark(source, kind, "cross_provider", trace);
                }
            }
            _ => unreachable!("validate rejects unknown parts"),
        }
    }
    Ok(json!({"type": "message", "role": role_name(role), "content": content}))
}

/// Tool output is a string unless it carries a `File`, which needs the
/// content-array form (protocol.md §10.4). Images and opaque parts become
/// placeholders and a trace mark either way.
fn tool_output(
    parts: &[Part],
    is_error: bool,
    provider: &ProviderId,
    trace: Option<&dyn TraceSink>,
) -> Value {
    let prefix = if is_error { "[tool error] " } else { "" };
    if !parts.iter().any(|part| matches!(part, Part::File { .. })) {
        let text: String = parts
            .iter()
            .map(|part| tool_output_text(part, provider, trace))
            .collect();
        return json!(format!("{prefix}{text}"));
    }
    let mut content: Vec<Value> = parts
        .iter()
        .map(|part| match part {
            Part::File {
                media_type,
                filename,
                source,
            } => input_file(media_type, filename.as_ref(), source),
            _ => json!({"type": "input_text", "text": tool_output_text(part, provider, trace)}),
        })
        .collect();
    if is_error {
        match content.iter_mut().find(|item| item["type"] == "input_text") {
            Some(item) => {
                item["text"] = json!(format!(
                    "{prefix}{}",
                    item["text"].as_str().unwrap_or_default()
                ))
            }
            None => content.insert(0, json!({"type": "input_text", "text": prefix})),
        }
    }
    Value::Array(content)
}

/// The text a non-file tool output part renders as.
fn tool_output_text(part: &Part, provider: &ProviderId, trace: Option<&dyn TraceSink>) -> String {
    let (kind, placeholder) = match part {
        Part::Text { text } | Part::Refusal { text } => return text.clone(),
        Part::Image { media_type, .. } => (media_type, format!("[image omitted: {media_type}]")),
        Part::File { .. } => unreachable!("tool_output renders files as input_file"),
        Part::ProviderOpaque { kind, .. } => (kind, format!("[opaque omitted: {kind}]")),
        _ => unreachable!("validate rejects unknown parts"),
    };
    mark(
        trace,
        TraceMark::rendered_as_text(provider.clone(), kind.clone(), "tool output is not text"),
    );
    placeholder
}

/// A `Part::File` as an `input_file` content item.
fn input_file(media_type: &str, filename: Option<&String>, source: &FileSource) -> Value {
    let mut item = json!({"type": "input_file"});
    if let Some(filename) = filename {
        item["filename"] = json!(filename);
    }
    match source {
        FileSource::Base64 { data } => {
            item["file_data"] = json!(format!("data:{media_type};base64,{data}"));
        }
        FileSource::Url { url } => item["file_url"] = json!(url),
        FileSource::ProviderFile(file) => item["file_id"] = json!(file.id),
        _ => unreachable!("validate rejects unknown file sources"),
    }
    item
}

fn text_content(
    parts: &[Part],
    provider: &ProviderId,
    trace: Option<&dyn TraceSink>,
) -> Vec<Value> {
    let mut content = Vec::with_capacity(parts.len());
    for part in parts {
        match part {
            Part::Text { text } => content.push(json!({"type": "input_text", "text": text})),
            Part::Refusal { text } => content.push(json!({"type": "input_text", "text": text})),
            Part::Image { media_type, .. } => {
                content.push(omitted(provider, trace, media_type, "image"));
            }
            Part::File { media_type, .. } => {
                content.push(omitted(provider, trace, media_type, "file"));
            }
            Part::ProviderOpaque { kind, .. } => {
                content.push(omitted(provider, trace, kind, "opaque"));
            }
            _ => unreachable!("validate rejects unknown parts"),
        }
    }
    content
}

/// A text placeholder for a non-text compaction or instruction part, traced.
fn omitted(provider: &ProviderId, trace: Option<&dyn TraceSink>, kind: &str, noun: &str) -> Value {
    mark(
        trace,
        TraceMark::rendered_as_text(provider.clone(), kind, "compaction content is text-only"),
    );
    json!({"type": "input_text", "text": format!("[{noun} omitted: {kind}]")})
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::User => "user",
        Role::Assistant => "assistant",
    }
}

fn raw_kind(item: &Item) -> &'static str {
    match item {
        Item::ServerToolCall { .. } => "server_tool_call",
        Item::ServerToolResult { .. } => "server_tool_result",
        Item::ProviderOpaque { .. } => "provider_opaque",
        _ => "item",
    }
}

fn raw(value: Value) -> Result<Box<RawValue>, ProviderError> {
    serde_json::value::to_raw_value(&value).map_err(|error| invalid_request(error.to_string()))
}

fn raw_json(data: &RawProviderData) -> Result<Box<RawValue>, ProviderError> {
    RawValue::from_string(data.json.as_str().to_owned())
        .map_err(|error| invalid_request(format!("invalid raw provider data: {error}")))
}

fn drop_mark(provider: &ProviderId, kind: &str, reason: &str, trace: Option<&dyn TraceSink>) {
    mark(trace, TraceMark::dropped(provider.clone(), kind, reason));
}

fn mark(trace: Option<&dyn TraceSink>, mark: TraceMark) {
    if let Some(sink) = trace {
        sink.mark(mark);
    }
}

fn unsupported(detail: &str) -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::Unsupported,
        format!("{detail} is not supported by the openai adapter"),
    )
}

fn invalid_request(detail: impl Into<String>) -> ProviderError {
    ProviderError::new(ProviderErrorKind::InvalidRequest, detail)
}
