//! OpenAI Responses SSE decoding and canonical event normalization.

use std::collections::{HashMap, HashSet};

use asterpath_http::{SseEvent, SseNormalizer};
use asterpath_protocol::{
    Item, JsonString, ModelSpec, Part, PartDelta, ProviderError, ProviderErrorKind, ProviderId,
    RawProviderData, ReasoningBinding, Role, StopReason, StreamEvent, ToolName, Usage,
};
use serde::Deserialize;
use serde_json::Value;
use serde_json::value::RawValue;

#[derive(Clone)]
pub(crate) struct WireContext {
    pub provider: ProviderId,
    pub spec: ModelSpec,
    pub prefix_epoch: u32,
    pub tools_hash: String,
}

#[derive(Deserialize)]
struct WireEvent<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    #[serde(default)]
    sequence_number: Option<u64>,
    #[serde(default)]
    output_index: Option<u32>,
    #[serde(default)]
    content_index: Option<u32>,
    #[serde(default)]
    summary_index: Option<u32>,
    #[serde(default)]
    delta: Option<String>,
    #[serde(default, borrow)]
    item: Option<&'a RawValue>,
    #[serde(default, borrow)]
    response: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct WireItem<'a> {
    #[serde(rename = "type", default)]
    kind: Option<&'a str>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    role: Option<String>,
    #[serde(default, borrow)]
    content: Option<Vec<&'a RawValue>>,
    #[serde(default, borrow)]
    summary: Option<Vec<&'a RawValue>>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    call_id: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

#[derive(Deserialize)]
struct WirePart<'a> {
    #[serde(rename = "type")]
    kind: &'a str,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    refusal: Option<String>,
}

pub(crate) struct Normalizer {
    context: WireContext,
    header_request_id: Option<String>,
    request_id: Option<String>,
    sequence: u64,
    last_wire_sequence: Option<u64>,
    started: bool,
    seen_started: HashSet<u32>,
    seen_completed: HashSet<u32>,
    completed_count: usize,
    has_tool_call: bool,
    /// Which reasoning delta kind each item locked onto with its first delta.
    reasoning_source: HashMap<u32, ReasoningSource>,
    terminal: bool,
    poisoned: bool,
}

#[derive(Clone, Copy, PartialEq)]
enum ReasoningSource {
    Summary,
    Text,
}

impl Normalizer {
    pub(crate) fn new(context: WireContext) -> Self {
        Self {
            context,
            header_request_id: None,
            request_id: None,
            sequence: 0,
            last_wire_sequence: None,
            started: false,
            seen_started: HashSet::new(),
            seen_completed: HashSet::new(),
            completed_count: 0,
            has_tool_call: false,
            reasoning_source: HashMap::new(),
            terminal: false,
            poisoned: false,
        }
    }

    fn finished(&self) -> bool {
        self.terminal || self.poisoned
    }

    fn next_sequence(&mut self) -> u64 {
        let sequence = self.sequence;
        self.sequence += 1;
        sequence
    }

    fn start(
        &mut self,
        out: &mut Vec<Result<StreamEvent, ProviderError>>,
        request_id: Option<String>,
    ) {
        if self.started {
            return;
        }
        self.started = true;
        if request_id.is_some() {
            self.request_id = request_id;
        }
        let request_id = self
            .request_id
            .clone()
            .or_else(|| self.header_request_id.clone());
        let sequence = self.next_sequence();
        out.push(Ok(StreamEvent::ResponseStarted {
            sequence,
            request_id,
        }));
    }

    fn emit(
        &mut self,
        out: &mut Vec<Result<StreamEvent, ProviderError>>,
        event: impl FnOnce(u64) -> StreamEvent,
    ) {
        let sequence = self.next_sequence();
        out.push(Ok(event(sequence)));
    }

    fn poison(&mut self, error: ProviderError) -> Vec<Result<StreamEvent, ProviderError>> {
        self.poisoned = true;
        vec![Err(error)]
    }

    fn handle(&mut self, data: &str) -> Vec<Result<StreamEvent, ProviderError>> {
        if self.finished() {
            return Vec::new();
        }
        let event: WireEvent<'_> = match serde_json::from_str(data) {
            Ok(event) => event,
            Err(error) => {
                return self.poison(protocol(format!("invalid sse payload: {error}")));
            }
        };
        if let Some(wire_sequence) = event.sequence_number {
            if let Some(last) = self.last_wire_sequence
                && wire_sequence <= last
            {
                return self.poison(protocol(format!(
                    "provider sequence regressed from {last} to {wire_sequence}"
                )));
            }
            self.last_wire_sequence = Some(wire_sequence);
        }

        let mut out = Vec::new();
        match event.kind {
            "response.created" | "response.in_progress" | "response.queued" => {
                let request_id = event
                    .response
                    .and_then(|response| response_id(response).map(str::to_owned));
                self.start(&mut out, request_id);
            }
            "response.output_item.added" => {
                self.start(&mut out, None);
                let Some(index) = event.output_index else {
                    return self.poison(protocol("output_item.added without output_index"));
                };
                if !self.seen_started.insert(index) {
                    return self.poison(protocol(format!(
                        "duplicate output_index {index} in output_item.added"
                    )));
                }
                self.emit(&mut out, |sequence| StreamEvent::ItemStarted {
                    sequence,
                    output_index: index,
                });
            }
            "response.output_text.delta"
            | "response.function_call_arguments.delta"
            | "response.reasoning_summary_text.delta"
            | "response.reasoning_text.delta" => {
                self.start(&mut out, None);
                let Some(index) = event.output_index else {
                    return self.poison(protocol("delta event without output_index"));
                };
                if !self.seen_started.contains(&index) {
                    return self.poison(protocol(format!(
                        "delta for output_index {index} before ItemStarted"
                    )));
                }
                let fragment = event.delta.unwrap_or_default();
                let mut part_index = event.content_index.unwrap_or_default();
                let delta = match event.kind {
                    "response.output_text.delta" => PartDelta::Text { text: fragment },
                    "response.function_call_arguments.delta" => PartDelta::JsonArgs { fragment },
                    kind => {
                        let source = if kind == "response.reasoning_text.delta" {
                            ReasoningSource::Text
                        } else {
                            part_index = event.summary_index.unwrap_or_default();
                            ReasoningSource::Summary
                        };
                        // First kind wins per item so summary and text indices never share a part.
                        if *self.reasoning_source.entry(index).or_insert(source) != source {
                            return out;
                        }
                        PartDelta::ReasoningSummary { text: fragment }
                    }
                };
                self.emit(&mut out, |sequence| StreamEvent::PartDelta {
                    sequence,
                    output_index: index,
                    part_index,
                    delta: delta.clone(),
                });
            }
            "response.output_item.done" => {
                self.start(&mut out, None);
                let Some(index) = event.output_index else {
                    return self.poison(protocol("output_item.done without output_index"));
                };
                if !self.seen_started.contains(&index) {
                    return self.poison(protocol(format!(
                        "output_item.done for output_index {index} before ItemStarted"
                    )));
                }
                if !self.seen_completed.insert(index) {
                    return self.poison(protocol(format!(
                        "duplicate output_index {index} in output_item.done"
                    )));
                }
                let Some(raw) = event.item else {
                    return self.poison(protocol("output_item.done without item"));
                };
                let item = match self.normalize_item(index, raw) {
                    Ok(item) => item,
                    Err(error) => return self.poison(error),
                };
                if matches!(item, Item::ToolCall { .. }) {
                    self.has_tool_call = true;
                }
                self.completed_count += 1;
                self.emit(&mut out, |sequence| StreamEvent::ItemCompleted {
                    sequence,
                    output_index: index,
                    item: item.clone(),
                });
            }
            "response.completed" | "response.incomplete" => {
                let Some(raw) = event.response else {
                    return self.poison(protocol("terminal event without response"));
                };
                let value: Value = match serde_json::from_str(raw.get()) {
                    Ok(value) => value,
                    Err(error) => {
                        return self.poison(protocol(format!("invalid response object: {error}")));
                    }
                };
                self.start(&mut out, response_id(raw).map(str::to_owned));
                if let Some(output) = value.get("output").and_then(Value::as_array)
                    && output.len() != self.completed_count
                {
                    return self.poison(protocol(format!(
                        "response.output has {} items but {} were completed",
                        output.len(),
                        self.completed_count
                    )));
                }
                let stop_reason = match event.kind {
                    "response.incomplete" => {
                        let reason = value
                            .pointer("/incomplete_details/reason")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        match reason {
                            "max_output_tokens" => StopReason::MaxTokens,
                            "content_filter" => StopReason::Refusal,
                            other => StopReason::Other {
                                provider_label: other.to_owned(),
                            },
                        }
                    }
                    _ => {
                        if self.has_tool_call {
                            StopReason::ToolUse
                        } else {
                            StopReason::EndTurn
                        }
                    }
                };
                let usage = usage(value.get("usage"));
                self.terminal = true;
                self.emit(&mut out, |sequence| StreamEvent::ResponseEnded {
                    sequence,
                    stop_reason: stop_reason.clone(),
                    usage,
                });
            }
            "response.failed" => {
                let Some(raw) = event.response else {
                    return self.poison(protocol("response.failed without response"));
                };
                self.start(&mut out, response_id(raw).map(str::to_owned));
                let error = self.failed_error(raw);
                self.terminal = true;
                out.push(Err(error));
            }
            "error" => {
                let value = serde_json::from_str::<Value>(data).unwrap_or(Value::Null);
                let message = value
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("provider error")
                    .to_owned();
                let (kind, retryable) =
                    classify_code(value.get("code").and_then(Value::as_str), None)
                        .unwrap_or((ProviderErrorKind::Server, true));
                let mut error = ProviderError::new(kind, message);
                error.retryable = retryable;
                return self.poison(error);
            }
            _ => {}
        }
        out
    }

    fn failed_error(&self, raw: &RawValue) -> ProviderError {
        let value: Value = serde_json::from_str(raw.get()).unwrap_or(Value::Null);
        let code = value
            .pointer("/error/code")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let message = value
            .pointer("/error/message")
            .and_then(Value::as_str)
            .unwrap_or("openai response failed")
            .to_owned();
        let (kind, retryable) = classify_code(Some(code), None).unwrap_or_else(|| {
            if context_exceeded_message(&message) {
                (ProviderErrorKind::ContextExceeded, false)
            } else {
                (ProviderErrorKind::InvalidRequest, false)
            }
        });
        let mut error = ProviderError::new(kind, message);
        error.retryable = retryable;
        error.request_id = response_id(raw)
            .map(str::to_owned)
            .or_else(|| self.request_id.clone())
            .or_else(|| self.header_request_id.clone());
        error.partial_usage = value.get("usage").map(|value| usage(Some(value)));
        error.raw = Some(RawProviderData {
            provider: self.context.provider.clone(),
            model: Some(self.context.spec.model.clone()),
            kind: "openai.response_failed".to_owned(),
            json: JsonString::new(raw.get().to_owned()),
        });
        error
    }

    fn normalize_item(&self, index: u32, raw: &RawValue) -> Result<Item, ProviderError> {
        let item: WireItem<'_> = serde_json::from_str(raw.get())
            .map_err(|error| protocol(format!("invalid output item: {error}")))?;
        let kind = item
            .kind
            .ok_or_else(|| protocol("output item without type"))?;
        let provider = self.context.provider.clone();
        match kind {
            "message" => {
                let mut parts = Vec::new();
                for content in item.content.unwrap_or_default() {
                    let part: WirePart<'_> = serde_json::from_str(content.get())
                        .map_err(|error| protocol(format!("invalid message part: {error}")))?;
                    match part.kind {
                        "output_text" => parts.push(Part::Text {
                            text: part.text.unwrap_or_default(),
                        }),
                        "refusal" => parts.push(Part::Refusal {
                            text: part.refusal.or(part.text).unwrap_or_default(),
                        }),
                        other => parts.push(Part::ProviderOpaque {
                            provider: provider.clone(),
                            kind: other.to_owned(),
                            raw: self.raw_data("openai.part", content.get()),
                        }),
                    }
                }
                let role = match item.role.as_deref() {
                    Some("user") => Role::User,
                    _ => Role::Assistant,
                };
                Ok(Item::Message {
                    role,
                    parts,
                    raw: Some(self.raw_data("openai.message", raw.get())),
                })
            }
            "function_call" => {
                let provider_item_id = item.id;
                let call_id = item
                    .call_id
                    .or_else(|| provider_item_id.clone())
                    .ok_or_else(|| protocol("function_call without call_id"))?;
                let name = item
                    .name
                    .ok_or_else(|| protocol("function_call without name"))?;
                Ok(Item::ToolCall {
                    call_id,
                    provider_item_id,
                    name: ToolName::new(name),
                    raw_args: JsonString::new(item.arguments.unwrap_or_default()),
                    raw: None,
                })
            }
            "reasoning" => {
                let texts = |parts: Option<Vec<&RawValue>>, kind: &str| {
                    let mut texts = Vec::new();
                    for content in parts.unwrap_or_default() {
                        let part: WirePart<'_> =
                            serde_json::from_str(content.get()).map_err(|error| {
                                protocol(format!("invalid reasoning part: {error}"))
                            })?;
                        if let Some(text) = part.text.filter(|_| part.kind == kind) {
                            texts.push(Part::Text { text });
                        }
                    }
                    Ok::<_, ProviderError>(texts)
                };
                let summary = match self.reasoning_source.get(&index) {
                    Some(ReasoningSource::Text) => texts(item.content, "reasoning_text")?,
                    _ => match texts(item.summary, "summary_text")? {
                        summary if summary.is_empty() => texts(item.content, "reasoning_text")?,
                        summary => summary,
                    },
                };
                Ok(Item::Reasoning {
                    summary,
                    binding: ReasoningBinding {
                        provider: provider.clone(),
                        model: self.context.spec.model.clone(),
                        prefix_epoch: self.context.prefix_epoch,
                        tools_hash: self.context.tools_hash.clone(),
                    },
                    raw: self.raw_data("openai.reasoning", raw.get()),
                })
            }
            "compaction" => Ok(Item::ProviderOpaque {
                provider,
                kind: kind.to_owned(),
                raw: self.raw_data("openai.compaction", raw.get()),
            }),
            // Only types the server executes; client-executed `*_call` items stay opaque.
            // Each carries its own result, so no separate item is a ServerToolResult:
            // `*_output` / `*_result` types (including `tool_search_output`, whose
            // `tool_search_call` may run on either side) take the opaque path below.
            "web_search_call"
            | "file_search_call"
            | "code_interpreter_call"
            | "image_generation_call"
            | "mcp_call" => Ok(Item::ServerToolCall {
                provider,
                name: kind.to_owned(),
                call_id: item.call_id.or(item.id),
                raw: self.raw_data("openai.server_tool_call", raw.get()),
            }),
            other => Ok(Item::ProviderOpaque {
                provider,
                kind: other.to_owned(),
                raw: self.raw_data("openai.opaque", raw.get()),
            }),
        }
    }

    fn raw_data(&self, kind: &str, json: &str) -> RawProviderData {
        RawProviderData {
            provider: self.context.provider.clone(),
            model: Some(self.context.spec.model.clone()),
            kind: kind.to_owned(),
            json: JsonString::new(json),
        }
    }
}

impl SseNormalizer for Normalizer {
    fn connected(&mut self, header_request_id: Option<String>) {
        self.header_request_id = header_request_id;
    }

    fn event(&mut self, event: SseEvent) -> Vec<Result<StreamEvent, ProviderError>> {
        if event.data.is_empty() || event.data == "[DONE]" {
            return Vec::new();
        }
        self.handle(&event.data)
    }

    fn terminal(&self) -> bool {
        self.finished()
    }

    /// OpenAI reports usage only on the terminal event.
    fn partial_usage(&self) -> Option<Usage> {
        None
    }

    fn request_id(&self) -> Option<String> {
        self.request_id.clone()
    }
}

fn response_id(raw: &RawValue) -> Option<&str> {
    #[derive(Deserialize)]
    struct ResponseId<'a> {
        #[serde(default)]
        id: Option<&'a str>,
    }
    let parsed: ResponseId<'_> = serde_json::from_str(raw.get()).ok()?;
    parsed.id
}

fn usage(value: Option<&Value>) -> Usage {
    let Some(value) = value else {
        return Usage::default();
    };
    let input = value
        .get("input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let cache_read = value
        .pointer("/input_tokens_details/cached_tokens")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let cache_write = value
        .pointer("/input_tokens_details/cache_write_tokens")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    Usage {
        input: asterpath_protocol::InputTokens {
            uncached: input.saturating_sub(cache_read + cache_write),
            cache_read,
            cache_write,
            cache_write_1h: 0,
        },
        output: asterpath_protocol::OutputTokens {
            total: value
                .get("output_tokens")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            reasoning: value
                .pointer("/output_tokens_details/reasoning_tokens")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
        },
    }
}

/// Kind and retryability of a non-success HTTP response; the streamed call
/// builds the rest of the error envelope.
pub(crate) fn classify(
    status: u16,
    body: Option<&Value>,
    message: &str,
) -> (ProviderErrorKind, bool) {
    let field = |name: &str| {
        body.and_then(|body| body.pointer(name))
            .and_then(Value::as_str)
    };
    classify_code(field("/error/code"), field("/error/type")).unwrap_or_else(|| {
        let kind = if context_exceeded_message(message) {
            ProviderErrorKind::ContextExceeded
        } else {
            asterpath_http::classify_status(status)
        };
        (kind, kind.retryable_by_default())
    })
}

/// Kind and retryability for a documented OpenAI error `code` / `type`; `None` when unknown.
fn classify_code(
    code: Option<&str>,
    error_type: Option<&str>,
) -> Option<(ProviderErrorKind, bool)> {
    [code, error_type].into_iter().flatten().find_map(|code| {
        let kind = match code {
            // Billing / spend caps: a 429 that waiting will not fix.
            "insufficient_quota"
            | "credit_balance_exhausted"
            | "organization_spend_limit_exceeded"
            | "project_spend_limit_exceeded"
            | "organization_usage_limit_exceeded" => {
                return Some((ProviderErrorKind::RateLimited, false));
            }
            "rate_limit_exceeded" => ProviderErrorKind::RateLimited,
            "server_error" => ProviderErrorKind::Server,
            "vector_store_timeout" => ProviderErrorKind::Timeout,
            "context_length_exceeded" => ProviderErrorKind::ContextExceeded,
            "invalid_prompt"
            | "invalid_image"
            | "invalid_image_format"
            | "invalid_base64_image"
            | "invalid_image_url"
            | "image_too_large"
            | "image_too_small"
            | "image_parse_error"
            | "image_content_policy_violation"
            | "invalid_image_mode"
            | "image_file_too_large"
            | "unsupported_image_media_type"
            | "empty_image_file"
            | "failed_to_download_image"
            | "image_file_not_found"
            | "bio_policy"
            | "misalignment_policy_violation"
            | "data_residency_mismatch" => ProviderErrorKind::InvalidRequest,
            _ => return None,
        };
        Some((kind, kind.retryable_by_default()))
    })
}

/// Message-text fallback for gateways that omit the `context_length_exceeded` code.
fn context_exceeded_message(text: &str) -> bool {
    let lower = text.to_lowercase();
    lower.contains("context_length_exceeded")
        || lower.contains("maximum context length")
        || lower.contains("context window")
        || lower.contains("too many tokens")
}

fn protocol(message: impl Into<String>) -> ProviderError {
    ProviderError::new(ProviderErrorKind::StreamProtocol, message)
}
