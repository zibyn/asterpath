//! Shared HTTP and request-rendering helpers for provider and MCP adapters.
//!
//! Not part of the stable surface (ADR-0026, re-audit candidate 5 revision).
#![allow(clippy::result_large_err)]

use std::time::Duration;

use asterpath_protocol::{
    FileSource, Item, ModelRequest, Part, ProviderError, ProviderErrorKind, ProviderId,
    RawProviderData,
};
use reqwest::header::{HeaderMap, RETRY_AFTER};
use serde_json::{Map, Value};

/// One dispatched server-sent event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    /// The `event:` field, if the event named one.
    pub event: Option<String>,
    /// All `data:` lines joined with `\n`.
    pub data: String,
}

/// Incremental `text/event-stream` decoder: bytes in, events out.
///
/// Buffers raw bytes and decodes UTF-8 only on complete lines, so a multibyte
/// character split across chunks survives. Lines end in LF or CRLF; a blank
/// line ends an event; comment lines (`:`) and `id` / `retry` are ignored.
#[derive(Debug, Default)]
pub struct SseDecoder {
    buffer: Vec<u8>,
    event: Option<String>,
    data: String,
    has_data: bool,
}

impl SseDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one chunk; returns the events completed by it.
    pub fn push(&mut self, chunk: &[u8]) -> Result<Vec<SseEvent>, ProviderError> {
        self.buffer.extend_from_slice(chunk);
        let mut events = Vec::new();
        let mut start = 0;
        while let Some(offset) = self.buffer[start..].iter().position(|&byte| byte == b'\n') {
            let end = start + offset;
            let line = &self.buffer[start..end];
            let line = line.strip_suffix(b"\r").unwrap_or(line);
            let line = std::str::from_utf8(line)
                .map_err(|error| protocol(format!("invalid UTF-8 in SSE stream: {error}")))?
                .to_owned();
            start = end + 1;
            events.extend(self.line(&line));
        }
        self.buffer.drain(..start);
        Ok(events)
    }

    /// End of stream: decode a trailing unterminated line and dispatch any
    /// pending event, so a body missing its final blank line is not lost.
    pub fn finish(&mut self) -> Result<Vec<SseEvent>, ProviderError> {
        let mut events = if self.buffer.is_empty() {
            Vec::new()
        } else {
            self.push(b"\n")?
        };
        events.extend(self.line(""));
        Ok(events)
    }

    fn line(&mut self, line: &str) -> Option<SseEvent> {
        if line.is_empty() {
            let event = self.event.take();
            if !std::mem::take(&mut self.has_data) {
                return None;
            }
            return Some(SseEvent {
                event,
                data: std::mem::take(&mut self.data),
            });
        }
        if line.starts_with(':') {
            return None;
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => self.event = Some(value.to_owned()),
            "data" => {
                if self.has_data {
                    self.data.push('\n');
                }
                self.data.push_str(value);
                self.has_data = true;
            }
            _ => {}
        }
        None
    }
}

/// Generic class of a non-success HTTP status. Adapters check provider-specific
/// error bodies (for example context overflow) before falling back to this.
pub fn classify_status(status: u16) -> ProviderErrorKind {
    match status {
        401 | 403 => ProviderErrorKind::Auth,
        408 => ProviderErrorKind::Timeout,
        429 => ProviderErrorKind::RateLimited,
        503 | 529 => ProviderErrorKind::Overloaded,
        500..=599 => ProviderErrorKind::Server,
        _ => ProviderErrorKind::InvalidRequest,
    }
}

/// `Retry-After` as a delay.
pub fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    // shortcut: only integer-seconds Retry-After; upgrade if a gateway sends HTTP-date.
    headers
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
}

/// The provider's request id (`request-id` or `x-request-id`).
pub fn request_id(headers: &HeaderMap) -> Option<String> {
    ["request-id", "x-request-id"]
        .into_iter()
        .find_map(|name| headers.get(name)?.to_str().ok())
        .map(str::to_owned)
}

fn protocol(message: String) -> ProviderError {
    ProviderError::new(ProviderErrorKind::StreamProtocol, message)
}

/// The `provider_options` entry for `request.spec.provider` as top-level body fields,
/// rejecting any key in `reserved` (keys the adapter never lets an application set,
/// written or not) as `Unsupported` and a non-object value as `InvalidRequest`.
/// Merge the result with [`merge_provider_option`].
pub fn provider_options_for(
    request: &ModelRequest,
    reserved: &[&str],
) -> Result<Map<String, Value>, ProviderError> {
    let provider = &request.spec.provider;
    let Some(options) = request.provider_options.get(provider) else {
        return Ok(Map::new());
    };
    let options = match serde_json::from_str(options.as_str()) {
        Ok(Value::Object(map)) => map,
        _ => {
            return Err(ProviderError::new(
                ProviderErrorKind::InvalidRequest,
                format!("provider_options for {provider} must be a JSON object"),
            ));
        }
    };
    if let Some(key) = options.keys().find(|key| reserved.contains(&key.as_str())) {
        return Err(provider_option_conflict(key));
    }
    Ok(options)
}

/// The `server_tools` entries for `request.spec.provider`, in declaration
/// order, to append verbatim after the function tools; a non-object entry is
/// `InvalidRequest`. Contents are not validated (ADR-0031).
pub fn server_tools_for(request: &ModelRequest) -> Result<Vec<Value>, ProviderError> {
    let provider = &request.spec.provider;
    let Some(tools) = request.server_tools.get(provider) else {
        return Ok(Vec::new());
    };
    tools
        .iter()
        .map(|tool| match serde_json::from_str(tool.as_str()) {
            Ok(value @ Value::Object(_)) => Ok(value),
            _ => Err(ProviderError::new(
                ProviderErrorKind::InvalidRequest,
                format!("server_tools for {provider} must be JSON objects"),
            )),
        })
        .collect()
}

/// Why the request holds a part the adapters cannot render: a `Part`
/// variant they do not know yet, or a `Part::File` whose media type is not in
/// the model's `accepted` list (protocol.md R14) or whose source they do not
/// know yet. The detail of an `Unsupported` error; with it `None`, adapters
/// may treat unknown variants as unreachable.
pub fn unrenderable_part(request: &ModelRequest, accepted: &[&str]) -> Option<String> {
    let known = |part: &Part| {
        matches!(
            part,
            Part::Text { .. }
                | Part::Image { .. }
                | Part::File { .. }
                | Part::Refusal { .. }
                | Part::ProviderOpaque { .. }
        )
    };
    if !all_parts(request).all(known) {
        return Some("this content part".to_owned());
    }
    request.content_parts().find_map(|(_, part)| match part {
        Part::File {
            media_type,
            source: FileSource::Base64 { .. } | FileSource::Url { .. } | FileSource::ProviderFile(_),
            ..
        } => (!accepted.contains(&media_type.as_str()))
            .then(|| format!("file media type {media_type} on {}", request.spec.model)),
        Part::File { .. } => Some("this file source".to_owned()),
        _ => None,
    })
}

/// Every part of the request: the system, then each item's parts.
fn all_parts(request: &ModelRequest) -> impl Iterator<Item = &Part> {
    let item_parts = request.items.iter().flat_map(|item| match item {
        Item::Message { parts, .. }
        | Item::ToolResult { parts, .. }
        | Item::ServerToolResult { parts, .. }
        | Item::Compaction { parts, .. }
        | Item::Instruction { parts, .. } => parts.as_slice(),
        Item::Reasoning { summary, .. } => summary.as_slice(),
        // Listed, not `_`, so a new item with parts fails to compile here.
        Item::ToolCall { .. }
        | Item::ServerToolCall { .. }
        | Item::ToolsChanged { .. }
        | Item::ProviderOpaque { .. } => &[],
    });
    request.system.iter().chain(item_parts)
}

/// Deep-merge one `provider_options` value into the value the adapter wrote at
/// `path`: objects merge key by key, new keys are added, and any other overlap
/// would replace the adapter's value, so it is `Unsupported`.
pub fn merge_provider_option(
    target: &mut Value,
    source: Value,
    path: &str,
) -> Result<(), ProviderError> {
    match (target, source) {
        (Value::Object(target), Value::Object(source)) => {
            for (key, value) in source {
                let nested = format!("{path}.{key}");
                match target.get_mut(&key) {
                    Some(existing) => merge_provider_option(existing, value, &nested)?,
                    None => {
                        target.insert(key, value);
                    }
                }
            }
            Ok(())
        }
        _ => Err(provider_option_conflict(path)),
    }
}

/// `provider_options` tried to set `path`, which the adapter writes or reserves.
pub fn provider_option_conflict(path: &str) -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::Unsupported,
        format!("provider_options may not set {path:?}: the adapter writes or reserves it"),
    )
}

/// One top-level field of a message's raw JSON, only when the raw came from
/// `provider` (ADR 0010); a missing or `null` field is `None`.
pub fn same_provider_raw_field(
    raw: Option<&RawProviderData>,
    provider: &ProviderId,
    field: &str,
) -> Option<Value> {
    raw.filter(|raw| &raw.provider == provider)
        .and_then(|raw| serde_json::from_str::<Value>(raw.json.as_str()).ok())
        .and_then(|mut value| value.get_mut(field).map(Value::take))
        .filter(|value| !value.is_null())
}

/// Receives an adapter's protocol-level trace marks.
///
/// The tracing module is not wired in v1 yet; applications inject a sink (or ignore
/// marks) until `protocol.dropped_items` / `protocol.rendered_as_text` have a
/// real span to land on.
pub trait TraceSink: Send + Sync {
    fn mark(&self, mark: TraceMark);
}

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

    pub fn dropped(provider: ProviderId, kind: impl Into<String>, reason: &str) -> Self {
        Self {
            name: Self::DROPPED_ITEMS,
            provider,
            kind: kind.into(),
            reason: reason.to_owned(),
        }
    }

    pub fn rendered_as_text(provider: ProviderId, kind: impl Into<String>, reason: &str) -> Self {
        Self {
            name: Self::RENDERED_AS_TEXT,
            provider,
            kind: kind.into(),
            reason: reason.to_owned(),
        }
    }
}
