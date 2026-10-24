//! Provider failure classification (protocol.md §8).
//!
//! The loop only reads `retryable` and `ContextExceeded`; every HTTP status and
//! SSE error shape is translated here.

use asterpath_http::{request_id, retry_after};
use asterpath_protocol::{
    JsonString, ModelId, ProviderError, ProviderErrorKind, ProviderId, RawProviderData, Usage,
};
use reqwest::header::HeaderMap;
use serde_json::Value;

/// Connection, DNS, TLS, reset and request-timeout failures.
pub(crate) fn transport(
    error: reqwest::Error,
    request_id: Option<String>,
    partial_usage: Option<Usage>,
) -> ProviderError {
    let kind = if error.is_timeout() {
        ProviderErrorKind::Timeout
    } else {
        ProviderErrorKind::Transport
    };
    let mut provider_error = ProviderError::new(kind, error.to_string());
    if let Some(request_id) = request_id {
        provider_error = provider_error.with_request_id(request_id);
    }
    if let Some(usage) = partial_usage {
        provider_error = provider_error.with_partial_usage(usage);
    }
    provider_error
}

/// A non-success HTTP response before any SSE event.
pub(crate) fn http(
    status: u16,
    headers: &HeaderMap,
    body: String,
    provider: &ProviderId,
    model: &ModelId,
) -> ProviderError {
    let parsed: Option<Value> = serde_json::from_str(&body).ok();
    let error_object = parsed.as_ref().and_then(|value| value.get("error"));
    let message = error_object
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| body.clone());
    let kind = classify_status(status, &message);
    let mut provider_error =
        not_retryable_if_spend_limited(ProviderError::new(kind, message), error_object);
    if let Some(retry_after) = retry_after(headers) {
        provider_error = provider_error.with_retry_after(retry_after);
    }
    if let Some(request_id) = request_id(headers) {
        provider_error = provider_error.with_request_id(request_id);
    }
    if !body.is_empty() {
        provider_error = provider_error.with_raw(raw_error(provider, model, &body));
    }
    provider_error
}

fn classify_status(status: u16, message: &str) -> ProviderErrorKind {
    if context_exceeded(message) {
        return ProviderErrorKind::ContextExceeded;
    }
    asterpath_http::classify_status(status)
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
    let mut provider_error =
        not_retryable_if_spend_limited(ProviderError::new(kind, message), error);
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

/// SSE ended without a terminal event: retryable transport truncation.
pub(crate) fn truncated(request_id: Option<String>, partial_usage: Option<Usage>) -> ProviderError {
    let mut error = ProviderError::new(
        ProviderErrorKind::Transport,
        "SSE stream ended before message_stop",
    );
    if let Some(request_id) = request_id {
        error = error.with_request_id(request_id);
    }
    if let Some(usage) = partial_usage {
        error = error.with_partial_usage(usage);
    }
    error
}

/// A spend-limit 429 keeps failing until the next billing period.
fn not_retryable_if_spend_limited(mut error: ProviderError, body: Option<&Value>) -> ProviderError {
    if body
        .and_then(|body| body.pointer("/details/error_code"))
        .and_then(Value::as_str)
        == Some("enforced_spend_limit_reached")
    {
        error.retryable = false;
    }
    error
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
