//! Canonical request rendering for the OpenAI Responses API.

use asterpath_protocol::{
    ImageSource, Item, ModelRequest, Part, ProviderCapabilities, ProviderError, ProviderErrorKind,
    ProviderId, RawProviderData, ReasoningEffort, Role, ToolChoice,
};
use serde_json::value::RawValue;
use serde_json::{Map, Value, json};

use crate::TraceSink;
use crate::models::efforts_for;

/// A protocol-level trace mark emitted while rendering.
///
/// `name` is `protocol.dropped_items` or `protocol.rendered_as_text`; `provider`
/// and `kind` identify the source, `reason` explains the drop or text fallback.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceMark {
    pub name: &'static str,
    pub provider: ProviderId,
    pub kind: String,
    pub reason: String,
}

impl TraceMark {
    pub const DROPPED_ITEMS: &'static str = "protocol.dropped_items";
    pub const RENDERED_AS_TEXT: &'static str = "protocol.rendered_as_text";

    pub(crate) fn dropped(provider: ProviderId, kind: impl Into<String>, reason: &str) -> Self {
        Self {
            name: Self::DROPPED_ITEMS,
            provider,
            kind: kind.into(),
            reason: reason.to_owned(),
        }
    }

    pub(crate) fn rendered_as_text(
        provider: ProviderId,
        kind: impl Into<String>,
        reason: &str,
    ) -> Self {
        Self {
            name: Self::RENDERED_AS_TEXT,
            provider,
            kind: kind.into(),
            reason: reason.to_owned(),
        }
    }
}

/// Serialize the complete request. Deterministic: struct field order, sorted
/// tool fingerprints and `serde_json`'s ordered maps make repeated renders of
/// the same `ModelRequest` byte-identical.
pub(crate) fn render(
    request: &ModelRequest,
    provider: &ProviderId,
    capabilities: ProviderCapabilities,
    trace: Option<&dyn TraceSink>,
) -> Result<Vec<u8>, ProviderError> {
    validate(request, provider, capabilities)?;
    let options = request.provider_options_for(OWNED_KEYS)?;

    let instructions = system_text(&request.system);
    let input = render_input(request, provider, trace)?;
    let tools: Vec<Value> = request
        .tools
        .iter()
        .map(|tool| {
            json!({
                "type": "function",
                "name": tool.name.as_str(),
                "description": tool.description,
                "parameters": tool.schema,
            })
        })
        .collect();

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
        store: bool,
        stream: bool,
        include: &'static [&'static str],
        #[serde(flatten)]
        options: Map<String, Value>,
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
        store: false,
        stream: true,
        include: &["reasoning.encrypted_content"],
        options,
    };

    serde_json::to_vec(&body).map_err(|error| {
        ProviderError::new(
            ProviderErrorKind::InvalidRequest,
            format!("request is not serializable: {error}"),
        )
    })
}

/// Every top-level key [`render`] can write; `provider_options` may not set them.
const OWNED_KEYS: &[&str] = &[
    "model",
    "instructions",
    "input",
    "tools",
    "tool_choice",
    "parallel_tool_calls",
    "max_output_tokens",
    "temperature",
    "top_p",
    "reasoning",
    "store",
    "stream",
    "include",
];

fn validate(
    request: &ModelRequest,
    provider: &ProviderId,
    capabilities: ProviderCapabilities,
) -> Result<(), ProviderError> {
    if let Some(feature) = capabilities.unsupported_feature(request) {
        return Err(unsupported(feature));
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
        && !efforts_for(request.spec.model.as_str()).contains(&effort)
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
    let tools_hash = asterpath_protocol::tools_hash(&request.tools);
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
                if let Some(phase) = data
                    .as_ref()
                    .filter(|data| &data.provider == provider)
                    .and_then(|data| serde_json::from_str::<Value>(data.json.as_str()).ok())
                    .and_then(|mut value| value.get_mut("phase").map(Value::take))
                {
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
                } else if binding.tools_hash != tools_hash {
                    Some("tools_hash_mismatch")
                } else {
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
                let image_url = match source {
                    ImageSource::Url { url } => url.clone(),
                    ImageSource::Base64 { data } => format!("data:{media_type};base64,{data}"),
                };
                content.push(json!({"type": "input_image", "image_url": image_url}));
            }
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
        }
    }
    Ok(json!({"type": "message", "role": role_name(role), "content": content}))
}

/// Tool output is text-only on the v1 OpenAI path; non-text parts become
/// placeholders and a trace mark.
fn tool_output(
    parts: &[Part],
    is_error: bool,
    provider: &ProviderId,
    trace: Option<&dyn TraceSink>,
) -> String {
    let mut text = String::new();
    for part in parts {
        match part {
            Part::Text { text: value } | Part::Refusal { text: value } => text.push_str(value),
            Part::Image { media_type, .. } => {
                mark(
                    trace,
                    TraceMark::rendered_as_text(
                        provider.clone(),
                        media_type.clone(),
                        "tool output is not text",
                    ),
                );
                text.push_str(&format!("[image omitted: {media_type}]"));
            }
            Part::ProviderOpaque { kind, .. } => {
                mark(
                    trace,
                    TraceMark::rendered_as_text(
                        provider.clone(),
                        kind.clone(),
                        "tool output is not text",
                    ),
                );
                text.push_str(&format!("[opaque omitted: {kind}]"));
            }
        }
    }
    if is_error {
        format!("[tool error] {text}")
    } else {
        text
    }
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
                mark(
                    trace,
                    TraceMark::rendered_as_text(
                        provider.clone(),
                        media_type.clone(),
                        "compaction content is text-only",
                    ),
                );
                content.push(json!({
                    "type": "input_text",
                    "text": format!("[image omitted: {media_type}]"),
                }));
            }
            Part::ProviderOpaque { kind, .. } => {
                mark(
                    trace,
                    TraceMark::rendered_as_text(
                        provider.clone(),
                        kind.clone(),
                        "compaction content is text-only",
                    ),
                );
                content.push(json!({
                    "type": "input_text",
                    "text": format!("[opaque omitted: {kind}]"),
                }));
            }
        }
    }
    content
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
