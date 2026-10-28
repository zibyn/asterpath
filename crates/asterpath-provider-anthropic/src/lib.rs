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

use std::sync::Arc;

use asterpath_protocol::{
    ModelRequest, ModelSpec, Provider, ProviderCapabilities, ProviderError, ProviderId, StreamEvent,
};
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};

use asterpath_http::{ErrorEnvelope, streamed_call};

pub use asterpath_http::{TraceMark, TraceSink};
pub use models::{AnthropicModelProfile, Thinking};

/// Default Anthropic API origin.
const DEFAULT_BASE_URL: &str = "https://api.anthropic.com";

/// Anthropic Messages adapter.
pub struct AnthropicProvider {
    api_key: String,
    base_url: String,
    client: reqwest::Client,
    id: ProviderId,
    trace: Option<Arc<dyn TraceSink>>,
    default_profile: Option<AnthropicModelProfile>,
    model_profiles: Vec<(String, AnthropicModelProfile)>,
}

impl AnthropicProvider {
    /// Default provider id used in [`ProviderId`] and reasoning bindings.
    pub const ID: &'static str = "anthropic";

    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: DEFAULT_BASE_URL.to_owned(),
            client: reqwest::Client::new(),
            id: ProviderId::new(Self::ID),
            trace: None,
            default_profile: None,
            model_profiles: Vec::new(),
        }
    }

    /// Name this instance (one per vendor endpoint); raw and reasoning
    /// replay is gated by it.
    #[must_use]
    pub fn with_id(mut self, id: impl Into<ProviderId>) -> Self {
        self.id = id.into();
        self
    }

    /// The rules for every model no [`Self::with_model_profile`] entry matches.
    #[must_use]
    pub fn with_default_profile(mut self, profile: AnthropicModelProfile) -> Self {
        self.default_profile = Some(profile);
        self
    }

    /// The rules for model ids starting with `prefix`; the first registered
    /// match wins, and an entry overrides the built-in table.
    #[must_use]
    pub fn with_model_profile(
        mut self,
        prefix: impl Into<String>,
        profile: AnthropicModelProfile,
    ) -> Self {
        self.model_profiles.push((prefix.into(), profile));
        self
    }

    fn profile(&self, spec: &ModelSpec) -> &AnthropicModelProfile {
        models::profile_for(
            &self.model_profiles,
            self.default_profile.as_ref(),
            spec.model.as_str(),
        )
    }

    /// Override the API origin (mock servers, gateways).
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

    fn messages_url(&self) -> String {
        format!("{}/v1/messages", self.base_url.trim_end_matches('/'))
    }
}

impl Provider for AnthropicProvider {
    fn id(&self) -> ProviderId {
        self.id.clone()
    }

    fn capabilities(&self, spec: &ModelSpec) -> ProviderCapabilities {
        let rules = self.profile(spec);
        let mut capabilities = ProviderCapabilities::default();
        capabilities.forced_tool_choice = rules.forced_tool_choice;
        capabilities.sampling = rules.sampling;
        capabilities.reasoning_roundtrip = true;
        capabilities.structured_output = rules.structured_output;
        capabilities.server_tools = rules.server_tools;
        // The adapter sets its own breakpoints; see request::render.
        capabilities.cache_control = true;
        capabilities.image_input = true;
        capabilities.file_input = !rules.file_media_types.is_empty();
        capabilities.parallel_tool_calls = true;
        capabilities
    }

    fn stream(&self, request: ModelRequest) -> BoxStream<'_, Result<StreamEvent, ProviderError>> {
        let capabilities = self.capabilities(&request.spec);
        let profile = self.profile(&request.spec);
        let provider = self.id();
        if let Err(error) = request::check(&provider, capabilities, profile, &request) {
            return stream::once(async move { Err(error) }).boxed();
        }
        let (body, tools_hash, beta) =
            match request::render(&provider, profile, &request, self.trace.as_deref()) {
                Ok(rendered) => rendered,
                Err(error) => return stream::once(async move { Err(error) }).boxed(),
            };
        let mut http = self
            .client
            .post(self.messages_url())
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", request::ANTHROPIC_VERSION)
            .header("content-type", "application/json")
            .header("accept", "text/event-stream");
        if let Some(beta) = beta {
            http = http.header("anthropic-beta", beta);
        }
        let errors = ErrorEnvelope {
            provider: provider.clone(),
            model: request.spec.model.clone(),
            raw_kind: "anthropic.error",
            classify: error::classify,
        };
        let decoder = sse::Decoder::new(
            provider,
            request.spec.model,
            request.prefix_epoch,
            tools_hash,
        );
        streamed_call(http.body(body), decoder, errors)
    }
}
