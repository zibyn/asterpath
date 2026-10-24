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
        ProviderCapabilities {
            server_state: false,
            forced_tool_choice: rules.forced_tool_choice,
            sampling: rules.sampling,
            reasoning_roundtrip: true,
            citations: false,
            server_tools: rules.server_tools,
            // The adapter sets its own breakpoints; see request::render.
            cache_control: true,
            image_input: true,
            parallel_tool_calls: true,
        }
    }

    fn stream(&self, request: ModelRequest) -> BoxStream<'_, Result<StreamEvent, ProviderError>> {
        let capabilities = self.capabilities(&request.spec);
        let profile = self.profile(&request.spec);
        if let Err(error) = request::check(capabilities, profile, &request) {
            return stream::once(async move { Err(error) }).boxed();
        }
        let provider = self.id();
        let (body, tools_hash) =
            match request::render(&provider, profile, &request, self.trace.as_deref()) {
                Ok(rendered) => rendered,
                Err(error) => return stream::once(async move { Err(error) }).boxed(),
            };
        let decoder = sse::Decoder::new(
            self.client.clone(),
            self.messages_url(),
            self.api_key.clone(),
            provider,
            request.spec.model.clone(),
            request.prefix_epoch,
            tools_hash,
            body,
            request::beta(profile, &request),
        );
        stream::unfold(Some(decoder), |state| async move {
            let mut decoder = state?;
            let event = decoder.next_event().await;
            event.map(|event| (event, Some(decoder)))
        })
        .boxed()
    }
}
