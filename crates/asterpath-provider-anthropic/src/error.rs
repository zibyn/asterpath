//! Provider failure classification (protocol.md §8).
//!
//! The loop only reads `retryable` and `ContextExceeded`; every HTTP status and
//! SSE error shape is translated here.

use asterpath_protocol::{
    JsonString, ModelId, ProviderError, ProviderErrorKind, ProviderId, RawProviderData, Usage,
};
use serde_json::Value;

/// Kind and retryability of a non-success HTTP response; the streamed call
/// builds the rest of the error envelope.
pub(crate) fn classify(
    status: u16,
    body: Option<&Value>,
    message: &str,
) -> (ProviderErrorKind, bool) {
    let kind = if context_exceeded(message) {
        ProviderErrorKind::ContextExceeded
    } else {
        asterpath_http::classify_status(status)
    };
    let error = body.and_then(|body| body.get("error"));
    (kind, kind.retryable_by_default() && !spend_limited(error))
}

/// An in-band SSE `error` event.
pub(crate) fn sse(
    data: &str,
    value: &Value,
    request_id: Option<String>,
    partial_usage: Option<Usage>,
    provider: &ProviderId,
    model: &ModelId,
) -> ProviderError {
    let error = value.get("error");
    let label = error
        .and_then(|error| error.get("type"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let message = error
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .unwrap_or(data)
        .to_owned();
    let kind = if context_exceeded(&message) {
        ProviderErrorKind::ContextExceeded
    } else {
        match label {
            "authentication_error" | "permission_error" => ProviderErrorKind::Auth,
            "invalid_request_error" | "billing_error" | "not_found_error" | "request_too_large" => {
                ProviderErrorKind::InvalidRequest
            }
            "rate_limit_error" => ProviderErrorKind::RateLimited,
            "overloaded_error" => ProviderErrorKind::Overloaded,
            "timeout_error" => ProviderErrorKind::Timeout,
            _ => ProviderErrorKind::Server,
        }
    };
    let mut provider_error = ProviderError::new(kind, message);
    if spend_limited(error) {
        provider_error.retryable = false;
    }
    if let Some(request_id) = request_id {
        provider_error = provider_error.with_request_id(request_id);
    }
    if let Some(usage) = partial_usage {
        provider_error = provider_error.with_partial_usage(usage);
    }
    provider_error.with_raw(raw_error(provider, model, data))
}

/// Stream structure/order violations (protocol.md §8, S1–S10).
pub(crate) fn protocol(message: impl Into<String>) -> ProviderError {
    ProviderError::new(ProviderErrorKind::StreamProtocol, message)
}

/// A spend-limit 429 keeps failing until the next billing period.
fn spend_limited(error: Option<&Value>) -> bool {
    error
        .and_then(|error| error.pointer("/details/error_code"))
        .and_then(Value::as_str)
        == Some("enforced_spend_limit_reached")
}

fn raw_error(provider: &ProviderId, model: &ModelId, body: &str) -> RawProviderData {
    RawProviderData {
        provider: provider.clone(),
        model: Some(model.clone()),
        kind: "anthropic.error".to_owned(),
        json: JsonString::new(body),
    }
}

fn context_exceeded(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("prompt is too long")
        || (message.contains("context")
            && (message.contains("too long")
                || message.contains("exceed")
                || message.contains("maximum")))
        || (message.contains("input") && message.contains("too many tokens"))
}
