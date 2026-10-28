//! Streamed call (GLOSSARY): one model request's HTTP round trip. Send the
//! POST, turn a non-success status into the error envelope, pump SSE through
//! the adapter's normalizer, report truncation and read failures, and stop
//! reading at the terminal event.

use asterpath_protocol::{
    JsonString, ModelId, ProviderError, ProviderErrorKind, ProviderId, RawProviderData,
    StreamEvent, Usage,
};
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};
use reqwest::header::HeaderMap;
use serde_json::Value;

use crate::{SseDecoder, SseEvent, request_id, retry_after};

/// An adapter's per-response SSE normalization state.
pub trait SseNormalizer: Send + 'static {
    /// The call connected with a success status; `header_request_id` is the
    /// response's `request-id` / `x-request-id`.
    fn connected(&mut self, header_request_id: Option<String>);

    /// Normalize one SSE event. An `Err` ends the call after the events before it.
    fn event(&mut self, event: SseEvent) -> Vec<Result<StreamEvent, ProviderError>>;

    /// The terminal event has been seen: the call stops reading.
    fn terminal(&self) -> bool;

    /// Usage known so far, attached to truncation and read failures.
    fn partial_usage(&self) -> Option<Usage>;

    /// A request id the events carried; the call falls back to the header's.
    fn request_id(&self) -> Option<String>;
}

/// How a non-success response becomes a [`ProviderError`]: the call parses the
/// body, picks the message (`error.message`, else the body, else the status)
/// and attaches `retry_after`, `request_id` and the raw body; the adapter only
/// classifies.
pub struct ErrorEnvelope {
    pub provider: ProviderId,
    pub model: ModelId,
    /// `RawProviderData.kind` for the error body, e.g. `anthropic.error`.
    pub raw_kind: &'static str,
    /// Status, parsed JSON body (if any) and message to kind and retryability.
    pub classify: fn(u16, Option<&Value>, &str) -> (ProviderErrorKind, bool),
}

impl ErrorEnvelope {
    fn error(&self, status: u16, headers: &HeaderMap, body: String) -> ProviderError {
        let parsed: Option<Value> = serde_json::from_str(&body).ok();
        let message = parsed
            .as_ref()
            .and_then(|value| value.pointer("/error/message"))
            .and_then(Value::as_str)
            .filter(|message| !message.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| {
                if body.is_empty() {
                    format!("{} returned HTTP {status}", self.provider)
                } else {
                    body.clone()
                }
            });
        let (kind, retryable) = (self.classify)(status, parsed.as_ref(), &message);
        let mut error = ProviderError::new(kind, message);
        error.retryable = retryable;
        error.retry_after = retry_after(headers);
        error.request_id = request_id(headers);
        if !body.is_empty() {
            error.raw = Some(RawProviderData {
                provider: self.provider.clone(),
                model: Some(self.model.clone()),
                kind: self.raw_kind.to_owned(),
                json: JsonString::new(body),
            });
        }
        error
    }
}

/// Run one streamed call: send `request`, normalize its SSE with `normalizer`.
pub fn streamed_call<N: SseNormalizer>(
    request: reqwest::RequestBuilder,
    normalizer: N,
    errors: ErrorEnvelope,
) -> BoxStream<'static, Result<StreamEvent, ProviderError>> {
    let call = Call {
        request: Some(request),
        response: None,
        header_request_id: None,
        normalizer,
        errors,
        sse: SseDecoder::new(),
    };
    stream::unfold(Some(call), |call| async move {
        let mut call = call?;
        let (events, more) = call.step().await;
        Some((events, more.then_some(call)))
    })
    .flat_map(stream::iter)
    .boxed()
}

type Step = (Vec<Result<StreamEvent, ProviderError>>, bool);

struct Call<N> {
    request: Option<reqwest::RequestBuilder>,
    response: Option<reqwest::Response>,
    header_request_id: Option<String>,
    normalizer: N,
    errors: ErrorEnvelope,
    sse: SseDecoder,
}

impl<N: SseNormalizer> Call<N> {
    /// Connect, or read one chunk; the flag is `false` once the call is over.
    async fn step(&mut self) -> Step {
        let Some(response) = &mut self.response else {
            return match self.connect().await {
                Ok(response) => {
                    self.response = Some(response);
                    (Vec::new(), true)
                }
                Err(error) => (vec![Err(error)], false),
            };
        };
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let events = self.sse.push(&chunk);
                self.pump(events)
            }
            Ok(None) => {
                let events = self.sse.finish();
                let (mut events, more) = self.pump(events);
                if more {
                    let error = ProviderError::new(
                        ProviderErrorKind::Transport,
                        "SSE stream ended before the terminal event",
                    );
                    events.push(Err(self.annotate(error)));
                }
                (events, false)
            }
            Err(failure) => (vec![Err(self.annotate(transport(failure)))], false),
        }
    }

    async fn connect(&mut self) -> Result<reqwest::Response, ProviderError> {
        let request = self.request.take().expect("connect runs once");
        let response = request.send().await.map_err(transport)?;
        self.header_request_id = request_id(response.headers());
        let status = response.status();
        if status.is_success() {
            self.normalizer.connected(self.header_request_id.clone());
            return Ok(response);
        }
        let headers = response.headers().clone();
        let body = response.text().await.map_err(|failure| {
            let mut error = transport(failure);
            error.request_id = self.header_request_id.clone();
            error
        })?;
        Err(self.errors.error(status.as_u16(), &headers, body))
    }

    fn pump(&mut self, frames: Result<Vec<SseEvent>, ProviderError>) -> Step {
        let frames = match frames {
            Ok(frames) => frames,
            Err(error) => return (vec![Err(error)], false),
        };
        let mut out = Vec::new();
        for frame in frames {
            let events = self.normalizer.event(frame);
            let failed = events.iter().any(Result::is_err);
            out.extend(events);
            if failed || self.normalizer.terminal() {
                return (out, false);
            }
        }
        (out, true)
    }

    /// Attach the best known request id and the usage so far.
    fn annotate(&self, mut error: ProviderError) -> ProviderError {
        error.request_id = self
            .normalizer
            .request_id()
            .or_else(|| self.header_request_id.clone());
        error.partial_usage = self.normalizer.partial_usage();
        error
    }
}

/// Connection, DNS, TLS, reset and timeout failures.
fn transport(failure: reqwest::Error) -> ProviderError {
    let kind = if failure.is_timeout() {
        ProviderErrorKind::Timeout
    } else {
        ProviderErrorKind::Transport
    };
    ProviderError::new(kind, failure.to_string())
}
