//! Anthropic SSE -> normalized [`StreamEvent`].
//!
//! The decoder enforces S1–S10: one `ResponseStarted`, one `ResponseEnded`
//! (never after a failure), unique `output_index`, one `ItemCompleted` per
//! started block, and strictly increasing adapter-assigned `sequence` values.

use std::collections::{BTreeMap, HashSet};

use asterpath_http::{SseEvent, SseNormalizer};
use asterpath_protocol::{
    ImageSource, InputTokens, Item, JsonString, ModelId, OutputTokens, Part, PartDelta,
    ProviderError, ProviderId, RawProviderData, ReasoningBinding, Role, StopReason, StreamEvent,
    ToolName, Usage,
};
use serde_json::{Value, json};

use crate::error;

pub(crate) struct Decoder {
    provider: ProviderId,
    model: ModelId,
    prefix_epoch: u32,
    tools_hash: String,

    terminal: bool,

    sequence: u64,
    started: bool,
    request_id_header: Option<String>,
    request_id: Option<String>,
    blocks: BTreeMap<u32, Block>,
    completed: HashSet<u32>,
    stop_reason: Option<StopReason>,
    usage_input: InputTokens,
    output_total: u64,
    output_reasoning: u64,
    usage_known: bool,
}

struct Block {
    kind: String,
    start: Value,
    text: String,
    thinking: String,
    signature: String,
    json_args: String,
    has_json_args: bool,
}

impl Block {
    fn new(kind: String, start: &Value) -> Self {
        Self {
            kind,
            start: start.clone(),
            text: string_field(start, "text").unwrap_or_default(),
            thinking: string_field(start, "thinking").unwrap_or_default(),
            signature: string_field(start, "signature").unwrap_or_default(),
            json_args: String::new(),
            has_json_args: false,
        }
    }
}

impl Decoder {
    pub(crate) fn new(
        provider: ProviderId,
        model: ModelId,
        prefix_epoch: u32,
        tools_hash: String,
    ) -> Self {
        Self {
            provider,
            model,
            prefix_epoch,
            tools_hash,
            terminal: false,
            sequence: 0,
            started: false,
            request_id_header: None,
            request_id: None,
            blocks: BTreeMap::new(),
            completed: HashSet::new(),
            stop_reason: None,
            usage_input: InputTokens::default(),
            output_total: 0,
            output_reasoning: 0,
            usage_known: false,
        }
    }

    fn handle_event(
        &mut self,
        event_name: Option<&str>,
        data: &str,
    ) -> Result<Vec<StreamEvent>, ProviderError> {
        let value: Value = serde_json::from_str(data)
            .map_err(|failure| error::protocol(format!("malformed SSE data: {failure}")))?;
        let kind = value
            .get("type")
            .and_then(Value::as_str)
            .or(event_name)
            .unwrap_or_default();
        match kind {
            "message_start" => self.message_start(&value),
            "content_block_start" => self.content_block_start(&value),
            "content_block_delta" => self.content_block_delta(&value),
            "content_block_stop" => self.content_block_stop(&value),
            "message_delta" => self.message_delta(&value),
            "message_stop" => self.message_stop(),
            "ping" => Ok(Vec::new()),
            "error" => Err(error::sse(
                data,
                &value,
                self.request_id.clone(),
                self.partial_usage(),
                &self.provider,
                &self.model,
            )),
            // Unknown events are ignored for forward compatibility.
            _ => Ok(Vec::new()),
        }
    }

    fn message_start(&mut self, value: &Value) -> Result<Vec<StreamEvent>, ProviderError> {
        if self.started {
            return Err(error::protocol("duplicate message_start"));
        }
        self.started = true;
        self.usage_known = true;
        let message = value
            .get("message")
            .ok_or_else(|| error::protocol("message_start without message"))?;
        let usage = message.get("usage");
        self.usage_input = InputTokens {
            uncached: u64_field(usage, "input_tokens"),
            cache_read: u64_field(usage, "cache_read_input_tokens"),
            cache_write: u64_field(usage, "cache_creation_input_tokens"),
            cache_write_1h: one_hour_writes(usage).unwrap_or(0),
        };
        self.output_total = u64_field(usage, "output_tokens");
        self.output_reasoning = usage
            .and_then(|usage| usage.get("output_tokens_details"))
            .map(|details| u64_field(Some(details), "thinking_tokens"))
            .unwrap_or(0);
        self.request_id = self
            .request_id_header
            .clone()
            .or_else(|| message.get("id").and_then(Value::as_str).map(str::to_owned));
        let sequence = self.next_sequence();
        Ok(vec![StreamEvent::ResponseStarted {
            sequence,
            request_id: self.request_id.clone(),
        }])
    }

    fn content_block_start(&mut self, value: &Value) -> Result<Vec<StreamEvent>, ProviderError> {
        if !self.started {
            return Err(error::protocol("content_block_start before message_start"));
        }
        let index = block_index(value)?;
        let block = value
            .get("content_block")
            .ok_or_else(|| error::protocol("content_block_start without content_block"))?;
        let kind = string_field(block, "type")
            .ok_or_else(|| error::protocol("content_block without type"))?;
        if self.blocks.contains_key(&index) || self.completed.contains(&index) {
            return Err(error::protocol(format!("duplicate output_index {index}")));
        }
        self.blocks.insert(index, Block::new(kind, block));
        let sequence = self.next_sequence();
        Ok(vec![StreamEvent::ItemStarted {
            sequence,
            output_index: index,
        }])
    }

    fn content_block_delta(&mut self, value: &Value) -> Result<Vec<StreamEvent>, ProviderError> {
        if !self.started {
            return Err(error::protocol("content_block_delta before message_start"));
        }
        let index = block_index(value)?;
        let delta = value
            .get("delta")
            .ok_or_else(|| error::protocol("content_block_delta without delta"))?;
        let kind = delta
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        match kind {
            "text_delta" => {
                let text = string_field(delta, "text").unwrap_or_default();
                let block = self.open_block(index)?;
                block.text.push_str(&text);
                let sequence = self.next_sequence();
                Ok(vec![StreamEvent::PartDelta {
                    sequence,
                    output_index: index,
                    part_index: 0,
                    delta: PartDelta::Text { text },
                }])
            }
            "input_json_delta" => {
                let fragment = string_field(delta, "partial_json").unwrap_or_default();
                let block = self.open_block(index)?;
                block.json_args.push_str(&fragment);
                block.has_json_args = true;
                let sequence = self.next_sequence();
                Ok(vec![StreamEvent::PartDelta {
                    sequence,
                    output_index: index,
                    part_index: 0,
                    delta: PartDelta::JsonArgs { fragment },
                }])
            }
            "thinking_delta" => {
                let text = string_field(delta, "thinking").unwrap_or_default();
                let block = self.open_block(index)?;
                block.thinking.push_str(&text);
                let sequence = self.next_sequence();
                Ok(vec![StreamEvent::PartDelta {
                    sequence,
                    output_index: index,
                    part_index: 0,
                    delta: PartDelta::ReasoningSummary { text },
                }])
            }
            "signature_delta" => {
                let signature = string_field(delta, "signature").unwrap_or_default();
                let block = self.open_block(index)?;
                block.signature.push_str(&signature);
                Ok(Vec::new())
            }
            // Citations stay in provider raw data; v1 has no typed citations.
            "citations_delta" => {
                let citation = delta.get("citation").cloned().unwrap_or(Value::Null);
                let block = self.open_block(index)?;
                match block
                    .start
                    .get_mut("citations")
                    .and_then(Value::as_array_mut)
                {
                    Some(citations) => citations.push(citation),
                    None => block.start["citations"] = json!([citation]),
                }
                Ok(Vec::new())
            }
            _ => Ok(Vec::new()),
        }
    }

    fn open_block(&mut self, index: u32) -> Result<&mut Block, ProviderError> {
        self.blocks
            .get_mut(&index)
            .ok_or_else(|| error::protocol(format!("delta for unstarted output_index {index}")))
    }

    fn content_block_stop(&mut self, value: &Value) -> Result<Vec<StreamEvent>, ProviderError> {
        if !self.started {
            return Err(error::protocol("content_block_stop before message_start"));
        }
        let index = block_index(value)?;
        let block = self
            .blocks
            .remove(&index)
            .ok_or_else(|| error::protocol(format!("content_block_stop without start: {index}")))?;
        if !self.completed.insert(index) {
            return Err(error::protocol(format!(
                "duplicate ItemCompleted for output_index {index}"
            )));
        }
        let item = self.build_item(block)?;
        let sequence = self.next_sequence();
        Ok(vec![StreamEvent::ItemCompleted {
            sequence,
            output_index: index,
            item,
        }])
    }

    fn message_delta(&mut self, value: &Value) -> Result<Vec<StreamEvent>, ProviderError> {
        if !self.started {
            return Err(error::protocol("message_delta before message_start"));
        }
        if let Some(delta) = value.get("delta")
            && let Some(label) = delta.get("stop_reason").and_then(Value::as_str)
        {
            self.stop_reason = Some(map_stop_reason(label));
        }
        if let Some(usage) = value.get("usage") {
            // Cumulative: server tool loops grow the input past message_start's count.
            for (field, slot) in [
                ("input_tokens", &mut self.usage_input.uncached),
                ("cache_read_input_tokens", &mut self.usage_input.cache_read),
                (
                    "cache_creation_input_tokens",
                    &mut self.usage_input.cache_write,
                ),
            ] {
                if let Some(tokens) = usage.get(field).and_then(Value::as_u64) {
                    *slot = tokens;
                }
            }
            if let Some(tokens) = one_hour_writes(Some(usage)) {
                self.usage_input.cache_write_1h = tokens;
            }
            if let Some(total) = usage.get("output_tokens").and_then(Value::as_u64) {
                self.output_total = total;
            }
            if let Some(reasoning) = usage
                .get("output_tokens_details")
                .and_then(|details| details.get("thinking_tokens"))
                .and_then(Value::as_u64)
            {
                self.output_reasoning = reasoning;
            }
        }
        Ok(Vec::new())
    }

    fn message_stop(&mut self) -> Result<Vec<StreamEvent>, ProviderError> {
        if !self.started {
            return Err(error::protocol("message_stop before message_start"));
        }
        if !self.blocks.is_empty() {
            return Err(error::protocol(format!(
                "message_stop with unfinished content blocks: {:?}",
                self.blocks.keys().collect::<Vec<_>>()
            )));
        }
        let stop_reason = self.stop_reason.clone().unwrap_or(StopReason::Other {
            provider_label: String::new(),
        });
        self.terminal = true;
        let sequence = self.next_sequence();
        Ok(vec![StreamEvent::ResponseEnded {
            sequence,
            stop_reason,
            usage: self.usage(),
        }])
    }

    fn build_item(&self, block: Block) -> Result<Item, ProviderError> {
        match block.kind.as_str() {
            "text" => {
                let mut raw = block.start;
                raw["text"] = json!(&block.text);
                Ok(Item::Message {
                    role: Role::Assistant,
                    parts: vec![Part::Text { text: block.text }],
                    raw: Some(self.raw("anthropic.text", &raw)),
                })
            }
            "thinking" => {
                let mut raw = block.start;
                raw["thinking"] = json!(&block.thinking);
                raw["signature"] = json!(&block.signature);
                Ok(Item::Reasoning {
                    summary: vec![Part::Text {
                        text: block.thinking,
                    }],
                    binding: self.binding(),
                    raw: self.raw("anthropic.thinking", &raw),
                })
            }
            "redacted_thinking" => Ok(Item::Reasoning {
                summary: Vec::new(),
                binding: self.binding(),
                raw: self.raw("anthropic.redacted_thinking", &block.start),
            }),
            "tool_use" => {
                let call_id = string_field(&block.start, "id")
                    .ok_or_else(|| error::protocol("tool_use without id"))?;
                let name = string_field(&block.start, "name")
                    .ok_or_else(|| error::protocol("tool_use without name"))?;
                let raw_args = if block.has_json_args {
                    block.json_args
                } else {
                    block
                        .start
                        .get("input")
                        .map(|input| serde_json::to_string(input).expect("input serializes"))
                        .unwrap_or_else(|| "{}".to_owned())
                };
                Ok(Item::ToolCall {
                    call_id,
                    provider_item_id: None,
                    name: ToolName::new(name),
                    raw_args: JsonString::new(raw_args),
                    raw: None,
                })
            }
            "server_tool_use" => {
                let call_id = string_field(&block.start, "id")
                    .ok_or_else(|| error::protocol("server_tool_use without id"))?;
                let name = string_field(&block.start, "name")
                    .ok_or_else(|| error::protocol("server_tool_use without name"))?;
                let mut raw = block.start;
                if block.has_json_args
                    && let Ok(input) = serde_json::from_str::<Value>(&block.json_args)
                {
                    raw["input"] = input;
                }
                Ok(Item::ServerToolCall {
                    provider: self.provider.clone(),
                    name,
                    call_id: Some(call_id),
                    raw: self.raw("anthropic.server_tool_use", &raw),
                })
            }
            kind if kind.ends_with("_tool_result") => {
                let call_id = string_field(&block.start, "tool_use_id");
                let parts = block
                    .start
                    .get("content")
                    .and_then(Value::as_array)
                    .map(|blocks| blocks.iter().filter_map(part_from_block).collect())
                    .unwrap_or_default();
                Ok(Item::ServerToolResult {
                    provider: self.provider.clone(),
                    call_id,
                    parts,
                    raw: self.raw("anthropic.server_tool_result", &block.start),
                })
            }
            kind => Ok(Item::ProviderOpaque {
                provider: self.provider.clone(),
                kind: kind.to_owned(),
                raw: self.raw("anthropic.content_block", &block.start),
            }),
        }
    }

    fn raw(&self, kind: &str, value: &Value) -> RawProviderData {
        RawProviderData {
            provider: self.provider.clone(),
            model: Some(self.model.clone()),
            kind: kind.to_owned(),
            json: JsonString::new(serde_json::to_string(value).expect("provider block serializes")),
        }
    }

    fn binding(&self) -> ReasoningBinding {
        ReasoningBinding {
            provider: self.provider.clone(),
            model: self.model.clone(),
            prefix_epoch: self.prefix_epoch,
            tools_hash: self.tools_hash.clone(),
        }
    }

    fn next_sequence(&mut self) -> u64 {
        let sequence = self.sequence;
        self.sequence += 1;
        sequence
    }

    fn usage(&self) -> Usage {
        Usage {
            input: self.usage_input,
            output: OutputTokens {
                total: self.output_total,
                reasoning: self.output_reasoning,
            },
        }
    }

    fn partial_usage(&self) -> Option<Usage> {
        self.usage_known.then(|| self.usage())
    }
}

impl SseNormalizer for Decoder {
    fn connected(&mut self, header_request_id: Option<String>) {
        self.request_id_header = header_request_id;
    }

    fn event(&mut self, event: SseEvent) -> Vec<Result<StreamEvent, ProviderError>> {
        match self.handle_event(event.event.as_deref(), &event.data) {
            Ok(events) => events.into_iter().map(Ok).collect(),
            Err(error) => vec![Err(error)],
        }
    }

    fn terminal(&self) -> bool {
        self.terminal
    }

    fn partial_usage(&self) -> Option<Usage> {
        Decoder::partial_usage(self)
    }

    fn request_id(&self) -> Option<String> {
        self.request_id.clone()
    }
}

fn block_index(value: &Value) -> Result<u32, ProviderError> {
    value
        .get("index")
        .and_then(Value::as_u64)
        .and_then(|index| u32::try_from(index).ok())
        .ok_or_else(|| error::protocol("SSE block event without a valid index"))
}

fn string_field(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(Value::as_str).map(str::to_owned)
}

fn u64_field(value: Option<&Value>, field: &str) -> u64 {
    value
        .and_then(|value| value.get(field))
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

/// `usage.cache_creation.ephemeral_1h_input_tokens`: the 1h share of
/// `cache_creation_input_tokens`.
fn one_hour_writes(usage: Option<&Value>) -> Option<u64> {
    usage?
        .pointer("/cache_creation/ephemeral_1h_input_tokens")
        .and_then(Value::as_u64)
}

fn map_stop_reason(label: &str) -> StopReason {
    match label {
        "end_turn" => StopReason::EndTurn,
        "tool_use" => StopReason::ToolUse,
        "max_tokens" => StopReason::MaxTokens,
        "stop_sequence" => StopReason::StopSequence,
        "pause_turn" => StopReason::PauseTurn,
        "model_context_window_exceeded" => StopReason::ContextWindowExceeded,
        "refusal" => StopReason::Refusal,
        other => StopReason::Other {
            provider_label: other.to_owned(),
        },
    }
}

fn part_from_block(block: &Value) -> Option<Part> {
    match block.get("type").and_then(Value::as_str)? {
        "text" => Some(Part::Text {
            text: string_field(block, "text")?,
        }),
        "image" => {
            let source = block.get("source")?;
            let media_type = string_field(source, "media_type").unwrap_or_default();
            match source.get("type").and_then(Value::as_str)? {
                "base64" => Some(Part::Image {
                    media_type,
                    source: ImageSource::Base64 {
                        data: string_field(source, "data")?,
                    },
                }),
                "url" => Some(Part::Image {
                    media_type,
                    source: ImageSource::Url {
                        url: string_field(source, "url")?,
                    },
                }),
                _ => None,
            }
        }
        _ => None,
    }
}
