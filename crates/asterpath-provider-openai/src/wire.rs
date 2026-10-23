//! OpenAI Responses SSE decoding and canonical event normalization.

use std::collections::HashSet;

use asterpath_http::{SseDecoder, SseEvent};
use asterpath_protocol::{
    Item, JsonString, ModelSpec, Part, PartDelta, ProviderError, ProviderErrorKind, ProviderId,
    RawProviderData, ReasoningBinding, Role, StopReason, StreamEvent, ToolName, Usage,
};
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};
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

/// Stream one HTTP response body, yielding normalized events.
pub(crate) fn stream(
    response: reqwest::Response,
    context: WireContext,
) -> BoxStream<'static, Result<StreamEvent, ProviderError>> {
    let header_request_id = crate::request_id(&response);
    let state = State {
        normalizer: Normalizer::new(context, header_request_id),
        response,
        decoder: SseDecoder::new(),
        done: false,
    };
    stream::unfold(Some(state), |state| async move {
        let mut state = state?;
        loop {
            if state.done {
                return None;
            }
            match state.response.chunk().await {
                Ok(Some(chunk)) => {
                    let events = match state.decoder.push(&chunk) {
                        Ok(frames) => state.process(frames),
                        Err(error) => return Some((vec![Err(error)], None)),
                    };
                    if !events.is_empty() {
                        let finished = state.normalizer.finished();
                        return Some((events, (!finished).then_some(state)));
                    }
                }
                Ok(None) => {
                    state.done = true;
                    let mut events = match state.decoder.finish() {
                        Ok(frames) => state.process(frames),
                        Err(error) => return Some((vec![Err(error)], None)),
                    };
                    if events.is_empty() && !state.normalizer.finished() {
                        events.push(Err(transport(
                            "openai stream ended without a terminal event",
                        )));
                    }
                    return (!events.is_empty()).then_some((events, None));
                }
                Err(error) => {
                    return Some((vec![Err(transport(error.to_string()))], None));
                }
            }
        }
    })
    .flat_map(stream::iter)
    .boxed()
}

struct Normalizer {
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
    terminal: bool,
    poisoned: bool,
}

impl Normalizer {
    fn new(context: WireContext, header_request_id: Option<String>) -> Self {
        Self {
            context,
            header_request_id,
            request_id: None,
            sequence: 0,
            last_wire_sequence: None,
            started: false,
            seen_started: HashSet::new(),
            seen_completed: HashSet::new(),
            completed_count: 0,
            has_tool_call: false,
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
            | "response.reasoning_summary_text.delta" => {
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
                let part_index = event
                    .content_index
                    .or(event.summary_index)
                    .unwrap_or_default();
                let delta = match event.kind {
                    "response.output_text.delta" => PartDelta::Text { text: fragment },
                    "response.function_call_arguments.delta" => PartDelta::JsonArgs { fragment },
                    _ => PartDelta::ReasoningSummary { text: fragment },
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
                let item = match self.normalize_item(raw) {
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
                let message = serde_json::from_str::<Value>(data)
                    .ok()
                    .and_then(|value| {
                        value
                            .get("message")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    })
                    .unwrap_or_else(|| "provider error".to_owned());
                let error = ProviderError::new(ProviderErrorKind::Server, message);
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
        let combined = format!("{code} {message}").to_lowercase();
        let kind = if combined.contains("context_length")
            || combined.contains("maximum context length")
            || combined.contains("context window")
            || combined.contains("too many tokens")
        {
            ProviderErrorKind::ContextExceeded
        } else if combined.contains("rate_limit") {
            ProviderErrorKind::RateLimited
        } else if combined.contains("server_error") || combined.contains("internal") {
            ProviderErrorKind::Server
        } else {
            ProviderErrorKind::InvalidRequest
        };
        let mut error = ProviderError::new(kind, message);
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

    fn normalize_item(&self, raw: &RawValue) -> Result<Item, ProviderError> {
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
                Ok(Item::Message { role, parts })
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
                let mut summary = Vec::new();
                for content in item.summary.unwrap_or_default() {
                    let part: WirePart<'_> = serde_json::from_str(content.get())
                        .map_err(|error| protocol(format!("invalid reasoning summary: {error}")))?;
                    if let Some(text) = part.text {
                        summary.push(Part::Text { text });
                    }
                }
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
            other if other.ends_with("_call") => Ok(Item::ServerToolCall {
                provider,
                name: other.to_owned(),
                call_id: item.call_id.or(item.id),
                raw: self.raw_data("openai.server_tool_call", raw.get()),
            }),
            other if other.ends_with("_result") || other.ends_with("_output") => {
                Ok(Item::ServerToolResult {
                    provider,
                    call_id: item.call_id.or(item.id),
                    parts: Vec::new(),
                    raw: self.raw_data("openai.server_tool_result", raw.get()),
                })
            }
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

impl State {
    fn process(&mut self, frames: Vec<SseEvent>) -> Vec<Result<StreamEvent, ProviderError>> {
        let mut events = Vec::new();
        for frame in frames {
            if frame.data.is_empty() || frame.data == "[DONE]" {
                continue;
            }
            events.extend(self.normalizer.handle(&frame.data));
            if self.normalizer.finished() {
                break;
            }
        }
        events
    }
}

struct State {
    response: reqwest::Response,
    normalizer: Normalizer,
    decoder: SseDecoder,
    done: bool,
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

fn protocol(message: impl Into<String>) -> ProviderError {
    ProviderError::new(ProviderErrorKind::StreamProtocol, message)
}

fn transport(message: impl Into<String>) -> ProviderError {
    ProviderError::new(ProviderErrorKind::Transport, message)
}
