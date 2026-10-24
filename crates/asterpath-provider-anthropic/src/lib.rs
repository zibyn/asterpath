//! Anthropic Messages protocol adapter.
//!
//! Renders a [`ModelRequest`] into the Messages HTTP body, streams the SSE
//! response as normalized [`StreamEvent`]s, preserves provider raw JSON for
//! byte-identical replay, and classifies provider failures. It writes no logs,
//! executes no tools and never retries: those belong to the agent loop.

// The provider error shape is fixed by the protocol contract; adapters cannot box it.
#![allow(clippy::result_large_err)]

mod error;
mod models;
mod request;
mod sse;

use asterpath_protocol::{
    ModelRequest, ModelSpec, Provider, ProviderCapabilities, ProviderError, ProviderId, StreamEvent,
};
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};

/// Default Anthropic API origin.
const DEFAULT_BASE_URL: &str = "https://api.anthropic.com";

/// Anthropic Messages adapter.
pub struct AnthropicProvider {
    api_key: String,
    base_url: String,
    client: reqwest::Client,
}

impl AnthropicProvider {
    /// Provider id used in [`ProviderId`] and reasoning bindings.
    pub const ID: &'static str = "anthropic";

    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: DEFAULT_BASE_URL.to_owned(),
            client: reqwest::Client::new(),
        }
    }

    /// Override the API origin (mock servers, gateways).
    #[must_use]
    pub fn with_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    fn messages_url(&self) -> String {
        format!("{}/v1/messages", self.base_url.trim_end_matches('/'))
    }
}

impl Provider for AnthropicProvider {
    fn id(&self) -> ProviderId {
        ProviderId::new(Self::ID)
    }

    fn capabilities(&self, spec: &ModelSpec) -> ProviderCapabilities {
        let rules = models::rules_for(spec.model.as_str());
        ProviderCapabilities {
            server_state: false,
            forced_tool_choice: rules.forced_tool_choice,
            sampling: rules.sampling,
            reasoning_roundtrip: true,
            citations: false,
            builtin_tools: true,
            cache_control: false,
            image_input: true,
            parallel_tool_calls: true,
        }
    }

    fn stream(&self, request: ModelRequest) -> BoxStream<'_, Result<StreamEvent, ProviderError>> {
        let capabilities = self.capabilities(&request.spec);
        if let Err(error) = request::check(capabilities, &request) {
            return stream::once(async move { Err(error) }).boxed();
        }
        let provider = self.id();
        let body = match request::render(&provider, &request) {
            Ok(body) => body,
            Err(error) => return stream::once(async move { Err(error) }).boxed(),
        };
        let decoder = sse::Decoder::new(
            self.client.clone(),
            self.messages_url(),
            self.api_key.clone(),
            provider,
            request.spec.model.clone(),
            request.prefix_epoch,
            asterpath_protocol::tools_hash(&request.tools),
            body,
        );
        stream::unfold(Some(decoder), |state| async move {
            let mut decoder = state?;
            let event = decoder.next_event().await;
            event.map(|event| (event, Some(decoder)))
        })
        .boxed()
    }
}
