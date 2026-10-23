//! Shared HTTP helpers for provider and MCP adapters.
//!
//! Not part of the stable surface (ADR-0026, re-audit candidate 5 revision).
#![allow(clippy::result_large_err)]

use std::time::Duration;

use asterpath_protocol::{ProviderError, ProviderErrorKind};
use reqwest::header::{HeaderMap, RETRY_AFTER};

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
