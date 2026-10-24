//! OpenAI Responses protocol adapter.
//!
//! Renders canonical [`ModelRequest`]s into the Responses wire format and
//! normalizes the interleaved SSE output items back into canonical
//! [`StreamEvent`]s. There is no server-side state: every request carries the
//! full history with `store: false`, and replayable reasoning is sent as raw
//! JSON with `include: ["reasoning.encrypted_content"]`.
//!
//! The adapter owns transport, wire mapping, usage normalization and error
//! classification. It never retries, executes tools or writes logs.

// `ProviderError` is the fixed protocol shape; boxing it would change the contract.
#![allow(clippy::result_large_err)]

mod models;
mod render;
mod wire;

use std::sync::Arc;

use asterpath_protocol::{
    ModelRequest, ModelSpec, Provider, ProviderCapabilities, ProviderError, ProviderErrorKind,
    ProviderId, StreamEvent,
};
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};

pub use render::TraceMark;
use wire::WireContext;

/// Receives the adapter's protocol-level trace marks.
///
/// The tracing module is not wired in v1 yet; hosts inject a sink (or ignore
/// marks) until `protocol.dropped_items` / `protocol.rendered_as_text` have a
/// real span to land on.
pub trait TraceSink: Send + Sync {
    fn mark(&self, mark: TraceMark);
}

/// OpenAI Responses provider.
pub struct OpenAiProvider {
    client: reqwest::Client,
    api_key: String,
    base_url: String,
    id: ProviderId,
    trace: Option<Arc<dyn TraceSink>>,
}

impl OpenAiProvider {
    /// Defaults to `https://api.openai.com/v1`.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key: api_key.into(),
            base_url: "https://api.openai.com/v1".to_owned(),
            id: ProviderId::new("openai"),
            trace: None,
        }
    }

    #[must_use]
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    #[must_use]
    pub fn with_trace_sink(mut self, sink: Arc<dyn TraceSink>) -> Self {
        self.trace = Some(sink);
        self
    }

    fn endpoint(&self) -> String {
        format!("{}/responses", self.base_url.trim_end_matches('/'))
    }
}

impl std::fmt::Debug for OpenAiProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenAiProvider")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl Provider for OpenAiProvider {
    fn id(&self) -> ProviderId {
        self.id.clone()
    }

    fn capabilities(&self, _spec: &ModelSpec) -> ProviderCapabilities {
        ProviderCapabilities {
            server_state: false,
            forced_tool_choice: true,
            sampling: true,
            reasoning_roundtrip: true,
            citations: false,
            // Only `function` tools are rendered today.
            builtin_tools: false,
            cache_control: false,
            image_input: true,
            parallel_tool_calls: true,
        }
    }

    fn stream(&self, request: ModelRequest) -> BoxStream<'_, Result<StreamEvent, ProviderError>> {
        let capabilities = self.capabilities(&request.spec);
        let rendered = render::render(&request, &self.id, capabilities, self.trace.as_deref());
        let body = match rendered {
            Ok(rendered) => rendered,
            Err(error) => return stream::once(async move { Err(error) }).boxed(),
        };

        let send = self
            .client
            .post(self.endpoint())
            .header("authorization", format!("Bearer {}", self.api_key))
            .header("content-type", "application/json")
            .header("accept", "text/event-stream")
            .body(body)
            .send();

        let context = WireContext {
            provider: self.id.clone(),
            spec: request.spec,
            prefix_epoch: request.prefix_epoch,
            tools_hash: asterpath_protocol::tools_hash(&request.tools),
        };

        stream::once(async move { connect(send).await })
            .flat_map(move |result| match result {
                Ok(response) => wire::stream(response, context.clone()),
                Err(error) => stream::once(async move { Err(error) }).boxed(),
            })
            .boxed()
    }
}

async fn connect(
    send: impl std::future::Future<Output = Result<reqwest::Response, reqwest::Error>>,
) -> Result<reqwest::Response, ProviderError> {
    let response = send.await.map_err(transport_error)?;
    if response.status().is_success() {
        return Ok(response);
    }
    Err(http_error(response).await)
}

fn transport_error(error: reqwest::Error) -> ProviderError {
    let kind = if error.is_timeout() {
        ProviderErrorKind::Timeout
    } else {
        ProviderErrorKind::Transport
    };
    ProviderError::new(kind, error.to_string())
}

async fn http_error(response: reqwest::Response) -> ProviderError {
    let status = response.status();
    let request_id = asterpath_http::request_id(response.headers());
    let retry_after = asterpath_http::retry_after(response.headers());
    let body = response.text().await.unwrap_or_default();
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
    let field = |name: &str| parsed.pointer(name).and_then(serde_json::Value::as_str);
    let (kind, retryable) = wire::classify_code(field("/error/code"), field("/error/type"))
        .unwrap_or_else(|| {
            let kind = if wire::context_exceeded_message(&body) {
                ProviderErrorKind::ContextExceeded
            } else {
                asterpath_http::classify_status(status.as_u16())
            };
            (kind, kind.retryable_by_default())
        });
    let message = if body.is_empty() {
        format!("openai returned HTTP {status}")
    } else {
        body.clone()
    };
    let mut error = ProviderError::new(kind, message);
    error.retryable = retryable;
    error.retry_after = retry_after;
    error.request_id = request_id;
    if !body.is_empty() {
        error.raw = Some(asterpath_protocol::RawProviderData {
            provider: ProviderId::new("openai"),
            model: None,
            kind: "openai.error".to_owned(),
            json: asterpath_protocol::JsonString::new(body),
        });
    }
    error
}
