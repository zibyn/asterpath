//! `ModelRequest` -> Anthropic Messages request body.
//!
//! Rendering is deterministic: the same log and request always produce the same
//! bytes. Persisted provider raw JSON is spliced into the content arrays
//! verbatim and is never parsed or re-serialized.

use asterpath_http::{
    TraceMark, TraceSink, merge_provider_option, provider_option_conflict, provider_options_for,
    same_provider_raw_field, server_tools_for, unrenderable_part,
};
use asterpath_protocol::{
    CacheLifetime, FileSource, ImageSource, Item, ModelRequest, Part, ProviderCapabilities,
    ProviderError, ProviderErrorKind, ProviderId, RawProviderData, ReasoningBinding,
    ReasoningEffort, ReasoningParams, Role, ToolChoice, ToolDescriptor,
};

use crate::models::{AnthropicModelProfile, Thinking};
use serde_json::{Value, json};

pub(crate) const ANTHROPIC_VERSION: &str = "2023-06-01";
const INLINE_TOOLS_BETA: &str = "inline-tools-2026-09-15";
const OUTPUT_CONFIG_BETA: &str = "mid-conversation-output-config-2026-07-01";

/// The rendered top-level `tools`: the baseline when tool changes render
/// inline, otherwise the effective set (ADR-0030). `tools_hash` covers these.
pub(crate) fn top_level_tools<'a>(
    rules: &AnthropicModelProfile,
    request: &'a ModelRequest,
) -> &'a [ToolDescriptor] {
    if rules.inline_changes {
        &request.baseline_tools
    } else {
        &request.tools
    }
}

/// Whether reasoning changes render in place (#180): an effort-only system
/// message carries the new `output_config.effort` and the top level keeps
/// `baseline_reasoning`. Only the effort can change in place; `thinking`
/// (including `Off`, which disables it or omits effort), `budget_tokens`,
/// `summary` (`thinking.display`) and every other request field live only
/// at the top level, so any of those changing, or an effort the model does
/// not accept, falls back to the effective reasoning at the top level. In
/// place, a change after a tool result takes effect from the next user
/// input, not the next step.
pub(crate) fn inline_effort(rules: &AnthropicModelProfile, request: &ModelRequest) -> bool {
    let baseline = request.baseline_reasoning.as_ref();
    let in_place = |params: Option<&ReasoningParams>| {
        params.is_some_and(|params| {
            params.effort.is_some_and(|effort| {
                effort != ReasoningEffort::Off && rules.efforts.contains(&effort)
            }) && baseline.is_some_and(|baseline| {
                params.budget_tokens == baseline.budget_tokens && params.summary == baseline.summary
            })
        })
    };
    rules.inline_effort
        && in_place(baseline)
        && request.items.iter().all(|item| match item {
            Item::ReasoningChanged { reasoning } => in_place(reasoning.as_ref()),
            _ => true,
        })
}

/// The `anthropic-beta` header the rendered body needs.
pub(crate) fn beta(rules: &AnthropicModelProfile, request: &ModelRequest) -> Option<String> {
    let has = |predicate: fn(&Item) -> bool| request.items.iter().any(predicate);
    let mut betas = Vec::new();
    if rules.inline_changes && has(|item| matches!(item, Item::ToolsChanged { .. })) {
        betas.push(INLINE_TOOLS_BETA);
    }
    if has(|item| matches!(item, Item::ReasoningChanged { .. })) && inline_effort(rules, request) {
        betas.push(OUTPUT_CONFIG_BETA);
    }
    (!betas.is_empty()).then(|| betas.join(","))
}

fn tool_definition(tool: &ToolDescriptor) -> Value {
    let mut definition = json!({
        "name": tool.name.as_str(),
        "description": tool.description,
        "input_schema": tool.schema,
    });
    if let Some(strict) = tool.strict {
        definition["strict"] = json!(strict);
    }
    definition
}

/// One rendered JSON fragment: ordinary values are serialized compactly,
/// replayed provider raw data is inserted exactly as persisted.
enum Frag {
    Json(Value),
    Raw(String),
    /// An object whose fields may hold raw data; written by [`object`].
    Object(Vec<(&'static str, Frag)>),
}

impl Frag {
    fn write(self, out: &mut String) {
        match self {
            Self::Json(value) => {
                out.push_str(&serde_json::to_string(&value).expect("JSON value serializes"));
            }
            Self::Raw(raw) => out.push_str(&raw),
            Self::Object(fields) => out.push_str(&object(fields)),
        }
    }

    /// Add an ephemeral cache breakpoint. Replayed raw blocks are never
    /// modified (ADR 0010), so they cannot carry one, and the API rejects one
    /// on an empty text block; returns whether it was set.
    fn set_cache_control(&mut self, ttl: CacheLifetime) -> bool {
        match self {
            Self::Json(value) if is_empty_text(value) => false,
            Self::Json(Value::Object(map)) => {
                map.insert("cache_control".to_owned(), ephemeral(ttl));
                true
            }
            Self::Object(fields) => {
                fields.push(("cache_control", Frag::Json(ephemeral(ttl))));
                true
            }
            Self::Json(_) | Self::Raw(_) => false,
        }
    }
}

fn is_empty_text(block: &Value) -> bool {
    block["type"] == "text" && block["text"] == ""
}

fn ephemeral(ttl: CacheLifetime) -> Value {
    match ttl {
        // No `ttl` field: the API default, and the bytes sessions already cached.
        CacheLifetime::FiveMinutes => json!({"type": "ephemeral"}),
        CacheLifetime::OneHour => json!({"type": "ephemeral", "ttl": "1h"}),
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
    rules: &AnthropicModelProfile,
    request: &ModelRequest,
) -> Result<(), ProviderError> {
    if let Some(feature) = capabilities.unsupported_feature(request) {
        return Err(unsupported(feature));
    }
    if let Some(detail) = unrenderable_part(request, &rules.file_media_types) {
        return Err(unsupported(&detail));
    }
    if !request
        .system
        .iter()
        .chain(instruction_parts(request))
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
        if let Some(effort) = reasoning.effort
            && !rules.efforts.contains(&effort)
        {
            return Err(unsupported(&format!(
                "reasoning.effort {effort:?} on {}",
                request.spec.model
            )));
        }
        if reasoning.effort == Some(ReasoningEffort::Off) && reasoning.budget_tokens.is_some() {
            return Err(invalid_request(
                "reasoning.effort Off contradicts reasoning.budget_tokens",
            ));
        }
        if reasoning.budget_tokens.is_some() {
            if rules.thinking == Thinking::Adaptive {
                return Err(unsupported("reasoning.budget_tokens"));
            }
            if !capabilities.reasoning_roundtrip {
                return Err(unsupported("thinking"));
            }
        }
    }
    Ok(())
}

/// Every appended instruction's parts, in history order.
fn instruction_parts(request: &ModelRequest) -> impl Iterator<Item = &Part> {
    request.items.iter().flat_map(|item| match item {
        Item::Instruction { parts, .. } => parts.as_slice(),
        _ => &[],
    })
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

/// Render the full Messages request body and the `tools_hash` of its
/// top-level tools. Capability checks are the caller's responsibility so they
/// can run before anything is serialized.
pub(crate) fn render(
    provider: &ProviderId,
    rules: &AnthropicModelProfile,
    request: &ModelRequest,
    trace: Option<&dyn TraceSink>,
) -> Result<(String, String), ProviderError> {
    let options = provider_options_for(request, &[])?;
    let server_tools = server_tools_for(request)?;
    let top_tools = top_level_tools(rules, request);
    let tools_hash = asterpath_protocol::tools_hash(top_tools, &server_tools);
    let messages = render_messages(
        provider,
        request,
        &tools_hash,
        rules,
        inline_effort(rules, request),
        trace,
    )?;
    let mut system: Vec<&Part> = request.system.iter().collect();
    if !rules.inline_changes {
        // Fallback: appended instructions join the top-level system.
        system.extend(instruction_parts(request));
    }

    // Cache order is tools, system, messages, and the API needs longer TTLs first.
    let prefix_ttl = request.cache_ttl.prefix.max(request.cache_ttl.messages);
    let mut fields: Vec<(&str, Frag)> = vec![
        ("model", Frag::Json(json!(request.spec.model.as_str()))),
        ("max_tokens", Frag::Json(json!(request.max_output_tokens))),
    ];
    if !system.is_empty() {
        let mut blocks: Vec<Value> = system
            .into_iter()
            .map(|part| match part {
                Part::Text { text } => json!({"type": "text", "text": text}),
                _ => unreachable!("check rejects non-text system parts"),
            })
            .collect();
        if let Some(last) = blocks.iter_mut().rev().find(|block| !is_empty_text(block)) {
            last["cache_control"] = ephemeral(prefix_ttl);
        }
        fields.push(("system", Frag::Json(Value::Array(blocks))));
    }
    fields.push(("messages", Frag::Raw(messages)));
    if !top_tools.is_empty() || !server_tools.is_empty() {
        let mut tools: Vec<Value> = top_tools.iter().map(tool_definition).collect();
        if let Some(last) = tools.last_mut() {
            last["cache_control"] = ephemeral(prefix_ttl);
        }
        // Server tools go last, verbatim; the breakpoint stays on the function tools.
        tools.extend(server_tools);
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
    let inline_effort = inline_effort(rules, request);
    let reasoning = if inline_effort {
        &request.baseline_reasoning
    } else {
        &request.reasoning
    };
    if let Some(reasoning) = reasoning {
        // check() has rejected combinations the model does not accept;
        // `inline_effort` has checked the baseline.
        if reasoning.effort == Some(ReasoningEffort::Off) {
            if rules.thinking_default_on {
                fields.push(("thinking", Frag::Json(json!({"type": "disabled"}))));
            }
        } else {
            render_reasoning(&mut fields, reasoning, rules);
        }
    }
    if let Some(format) = &request.output_format {
        let format = json!({"type": "json_schema", "schema": format.schema});
        match fields.iter_mut().find(|(name, _)| *name == "output_config") {
            Some((_, Frag::Json(config))) => config["format"] = format,
            _ => fields.push(("output_config", Frag::Json(json!({"format": format})))),
        }
    }
    if !request.stop_sequences.is_empty() {
        fields.push(("stop_sequences", Frag::Json(json!(request.stop_sequences))));
    }
    fields.push(("stream", Frag::Json(json!(true))));
    for (key, value) in &options {
        match fields.iter_mut().find(|(name, _)| name == key) {
            Some((_, Frag::Json(existing))) => merge_provider_option(existing, value.clone(), key)?,
            // Raw fragments (messages, f32 sampling values) are never merged into.
            Some(_) => return Err(provider_option_conflict(key)),
            None => fields.push((key.as_str(), Frag::Json(value.clone()))),
        }
    }
    Ok((object(fields), tools_hash))
}

fn render_reasoning(
    fields: &mut Vec<(&str, Frag)>,
    reasoning: &ReasoningParams,
    rules: &AnthropicModelProfile,
) {
    let thinking = match (reasoning.budget_tokens, rules.thinking) {
        (Some(budget), _) => Some(json!({"type": "enabled", "budget_tokens": budget})),
        (None, Thinking::Extended) => None,
        (None, Thinking::Both | Thinking::Adaptive) => Some(json!({"type": "adaptive"})),
    };
    if let Some(mut thinking) = thinking {
        if reasoning.summary && rules.thinking_display() {
            thinking["display"] = json!("summarized");
        }
        fields.push(("thinking", Frag::Json(thinking)));
    }
    if let Some(effort) = reasoning.effort {
        fields.push((
            "output_config",
            Frag::Json(json!({"effort": effort_name(effort)})),
        ));
    }
}

fn effort_name(effort: ReasoningEffort) -> &'static str {
    match effort {
        ReasoningEffort::Low => "low",
        ReasoningEffort::Medium => "medium",
        ReasoningEffort::High => "high",
        ReasoningEffort::XHigh => "xhigh",
        ReasoningEffort::Max => "max",
        _ => unreachable!("check rejects efforts missing from the model table"),
    }
}

struct PendingMessage {
    role: &'static str,
    tool_results: Vec<Frag>,
    other: Vec<Frag>,
    /// The message-level `output_config` of a system message.
    output_config: Option<Value>,
}

/// Messages being rendered. Instruction blocks wait for the next assistant
/// turn (or the end): the API accepts a content-carrying system message only
/// after a user turn and before an assistant turn or the end of `messages`.
#[derive(Default)]
struct Messages {
    list: Vec<PendingMessage>,
    pending_system: Vec<Frag>,
    /// An effort change rides on the pending system message, or alone with
    /// empty content; the last of adjacent changes wins.
    pending_effort: Option<&'static str>,
}

impl Messages {
    fn push(&mut self, role: &'static str, tool_results: Vec<Frag>, other: Vec<Frag>) {
        if role == "assistant" {
            self.flush_system();
        }
        push_blocks(&mut self.list, role, tool_results, other);
    }

    fn flush_system(&mut self) {
        let effort = self.pending_effort.take();
        if !self.pending_system.is_empty() || effort.is_some() {
            let system = std::mem::take(&mut self.pending_system);
            push_blocks(&mut self.list, "system", Vec::new(), system);
            if let (Some(effort), Some(last)) = (effort, self.list.last_mut()) {
                last.output_config = Some(json!({"effort": effort}));
            }
        }
    }
}

fn render_messages(
    provider: &ProviderId,
    request: &ModelRequest,
    tools_hash: &str,
    rules: &AnthropicModelProfile,
    inline_effort: bool,
    trace: Option<&dyn TraceSink>,
) -> Result<String, ProviderError> {
    let inline_changes = rules.inline_changes;
    // On the fallback path the top-level system holds every instruction, so
    // thinking produced before the last one was bound to a different system.
    let replay_from = if inline_changes {
        0
    } else {
        request
            .items
            .iter()
            .rposition(|item| matches!(item, Item::Instruction { .. }))
            .map_or(0, |index| index + 1)
    };
    let mut messages = Messages::default();
    for (index, item) in request.items.iter().enumerate() {
        match item {
            Item::Message { role, parts, raw } => {
                let mut blocks: Vec<Frag> = parts
                    .iter()
                    .filter_map(|part| render_part(provider, part))
                    .collect();
                // Only the text block's `citations` are merged from same-provider raw (ADR 0010).
                if let Some(citations) =
                    same_provider_raw_field(raw.as_ref(), provider, "citations").filter(
                        |citations| citations.as_array().is_some_and(|list| !list.is_empty()),
                    )
                    && let Some(Frag::Json(block)) = blocks
                        .iter_mut()
                        .find(|block| matches!(block, Frag::Json(value) if value["type"] == "text"))
                {
                    block["citations"] = citations;
                }
                if !blocks.is_empty() {
                    messages.push(role_name(*role), Vec::new(), blocks);
                }
            }
            Item::Reasoning { binding, raw, .. } => {
                let reason = drop_reason(binding, raw, request, tools_hash)
                    .or((index < replay_from).then_some("system_changed"));
                match (reason, trace) {
                    (None, _) => messages.push(
                        "assistant",
                        Vec::new(),
                        vec![Frag::Raw(raw.json.as_str().to_owned())],
                    ),
                    (Some(reason), Some(sink)) => {
                        sink.mark(TraceMark::dropped(provider.clone(), "reasoning", reason))
                    }
                    (Some(_), None) => {}
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
                messages.push(
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
                let block = Frag::Object(vec![
                    ("type", Frag::Json(json!("tool_result"))),
                    ("tool_use_id", Frag::Json(json!(call_id))),
                    ("content", Frag::Raw(array(content))),
                    ("is_error", Frag::Json(json!(is_error))),
                ]);
                messages.push("user", vec![block], Vec::new());
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
                    messages.push(
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
                    messages.push("user", Vec::new(), blocks);
                }
            }
            Item::Instruction { parts, .. } if inline_changes => {
                messages
                    .pending_system
                    .extend(parts.iter().filter_map(|part| render_part(provider, part)));
            }
            // Rendered into the top-level system by `render`.
            Item::Instruction { .. } => {}
            Item::ToolsChanged { added, removed } if rules.inline_changes => {
                messages.pending_system.extend(added.iter().map(|tool| {
                    Frag::Json(json!({
                        "type": "tool_addition",
                        "tool": {"type": "tool_definition", "definition": tool_definition(tool)},
                    }))
                }));
                messages.pending_system.extend(removed.iter().map(|name| {
                    Frag::Json(json!({
                        "type": "tool_removal",
                        "tool": {"type": "tool_reference", "name": name.as_str()},
                    }))
                }));
            }
            // The top-level `tools` is already the effective set.
            Item::ToolsChanged { .. } => {}
            Item::ReasoningChanged { reasoning } if inline_effort => {
                messages.pending_effort = reasoning
                    .as_ref()
                    .and_then(|params| params.effort)
                    .map(effort_name);
            }
            // The top level already carries the effective reasoning.
            Item::ReasoningChanged { .. } => {}
        }
    }
    messages.flush_system();

    // The last two messages with blocks get breakpoints; the previous one is
    // a fallback for steps that add more than the API's 20-block lookback.
    // An effort-only system message has no block to carry one.
    let carriers: Vec<usize> = messages
        .list
        .iter()
        .enumerate()
        .filter(|(_, message)| !message.tool_results.is_empty() || !message.other.is_empty())
        .map(|(position, _)| position)
        .collect();
    let marked = &carriers[carriers.len().saturating_sub(2)..];
    let rendered = messages
        .list
        .into_iter()
        .enumerate()
        .map(|(position, message)| {
            let mut content = message.tool_results;
            content.extend(message.other);
            if marked.contains(&position) {
                // Breakpoint on the last carrier block; omitted if every block is raw.
                let ttl = request.cache_ttl.messages;
                let _ = content
                    .iter_mut()
                    .rev()
                    .any(|block| block.set_cache_control(ttl));
            }
            let mut fields = vec![
                ("role", Frag::Json(json!(message.role))),
                ("content", Frag::Raw(array(content))),
            ];
            if let Some(config) = message.output_config {
                fields.push(("output_config", Frag::Json(config)));
            }
            Frag::Raw(object(fields))
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
            output_config: None,
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
            ImageSource::ProviderFile(file) => json!({
                "type": "image",
                "source": {"type": "file", "file_id": file.id},
            }),
        })),
        Part::File {
            media_type,
            filename,
            source,
        } => {
            let source = match source {
                FileSource::Base64 { data } => {
                    json!({"type": "base64", "media_type": media_type, "data": data})
                }
                FileSource::Url { url } => json!({"type": "url", "url": url}),
                FileSource::ProviderFile(file) => json!({"type": "file", "file_id": file.id}),
                _ => unreachable!("check rejects unknown file sources"),
            };
            let mut block = json!({"type": "document", "source": source});
            if let Some(filename) = filename {
                block["title"] = json!(filename);
            }
            Some(Frag::Json(block))
        }
        Part::ProviderOpaque {
            provider: item_provider,
            raw,
            ..
        } => (item_provider == provider).then(|| Frag::Raw(raw.json.as_str().to_owned())),
        _ => unreachable!("check rejects unknown parts"),
    }
}

/// Why a reasoning item may not be replayed into this request, if it may not.
fn drop_reason(
    binding: &ReasoningBinding,
    raw: &RawProviderData,
    request: &ModelRequest,
    tools_hash: &str,
) -> Option<&'static str> {
    if binding.provider != request.spec.provider || raw.provider != request.spec.provider {
        Some("cross_provider")
    } else if binding.model != request.spec.model {
        Some("model_mismatch")
    } else if binding.prefix_epoch != request.prefix_epoch {
        Some("prefix_epoch_mismatch")
    } else if binding.tools_hash != tools_hash {
        Some("tools_hash_mismatch")
    } else {
        None
    }
}
