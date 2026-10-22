//! Provider contract: request shape, normalized stream events, capability flags,
//! error classification and the static provider catalog.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures_core::stream::BoxStream;

use crate::config::{
    ModelSpec, ReasoningParams, RequestConfig, SamplingParams, ToolChoice, ToolDescriptor,
};
use crate::ids::ProviderId;
use crate::item::{Item, Part, RawProviderData};
use crate::usage::Usage;

/// One complete model request for one attempt. The loop builds it from the
/// session's `ConfigSnapshot` and the projection watermark.
#[derive(Clone, Debug, PartialEq)]
pub struct ModelRequest {
    pub spec: ModelSpec,
    /// Number of compactions applied at the request waterline.
    pub prefix_epoch: u32,
    /// Only `Part::Text` is valid.
    pub system: Vec<Part>,
    pub items: Vec<Item>,
    pub tools: Vec<ToolDescriptor>,
    pub tool_choice: ToolChoice,
    pub disable_parallel_tool_use: bool,
    pub sampling: Option<SamplingParams>,
    pub max_output_tokens: u32,
    pub reasoning: Option<ReasoningParams>,
}

impl ModelRequest {
    /// Build the request parameters shared by every attempt of a session.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        spec: ModelSpec,
        prefix_epoch: u32,
        system: Vec<Part>,
        items: Vec<Item>,
        tools: Vec<ToolDescriptor>,
        request: &RequestConfig,
    ) -> Self {
        Self {
            spec,
            prefix_epoch,
            system,
            items,
            tools,
            tool_choice: request.tool_choice.clone(),
            disable_parallel_tool_use: request.disable_parallel_tool_use,
            sampling: request.sampling.clone(),
            max_output_tokens: request.max_output_tokens.get(),
            reasoning: request.reasoning.clone(),
        }
    }
}

/// Normalized streaming response events. Only `ItemCompleted` is persisted.
#[derive(Clone, Debug, PartialEq)]
pub enum StreamEvent {
    ResponseStarted {
        sequence: u64,
        request_id: Option<String>,
    },
    ItemStarted {
        sequence: u64,
        output_index: u32,
    },
    PartDelta {
        sequence: u64,
        output_index: u32,
        part_index: u32,
        delta: PartDelta,
    },
    ItemCompleted {
        sequence: u64,
        output_index: u32,
        item: Item,
    },
    ResponseEnded {
        sequence: u64,
        stop_reason: crate::event::StopReason,
        usage: Usage,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartDelta {
    Text { text: String },
    JsonArgs { fragment: String },
    ReasoningSummary { text: String },
}

/// Capability declarations for one `ModelSpec`. Unknown capabilities are false.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProviderCapabilities {
    /// Always false in v1.
    pub server_state: bool,
    pub forced_tool_choice: bool,
    pub sampling: bool,
    pub reasoning_roundtrip: bool,
    pub citations: bool,
    pub builtin_tools: bool,
    pub cache_control: bool,
    pub image_input: bool,
    pub parallel_tool_calls: bool,
}

/// One model provider adapter. The loop never branches on provider identity.
pub trait Provider: Send + Sync {
    fn id(&self) -> ProviderId;
    fn capabilities(&self, spec: &ModelSpec) -> ProviderCapabilities;
    fn stream(&self, request: ModelRequest) -> BoxStream<'_, Result<StreamEvent, ProviderError>>;
}

/// Static provider set built by the composition root.
#[derive(Clone, Default)]
pub struct ProviderCatalog {
    providers: BTreeMap<ProviderId, Arc<dyn Provider>>,
}

impl ProviderCatalog {
    /// Duplicate ids are a composition error.
    pub fn new(providers: Vec<Arc<dyn Provider>>) -> Result<Self, DuplicateProviderId> {
        let mut catalog = BTreeMap::new();
        for provider in providers {
            let id = provider.id();
            if catalog.insert(id.clone(), provider).is_some() {
                return Err(DuplicateProviderId { id });
            }
        }
        Ok(Self { providers: catalog })
    }

    pub fn get(&self, id: &ProviderId) -> Option<Arc<dyn Provider>> {
        self.providers.get(id).cloned()
    }
}

impl std::fmt::Debug for ProviderCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProviderCatalog")
            .field("providers", &self.providers.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("duplicate provider id: {id}")]
pub struct DuplicateProviderId {
    pub id: ProviderId,
}

/// Normalized provider failure. Adapters classify; the loop reads `retryable`
/// and `context_exceeded`.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
#[non_exhaustive]
#[error("{kind:?}: {message}")]
pub struct ProviderError {
    pub kind: ProviderErrorKind,
    pub retryable: bool,
    pub retry_after: Option<Duration>,
    pub request_id: Option<String>,
    pub message: String,
    /// Known usage from a failed attempt. Diagnostic only.
    pub partial_usage: Option<Usage>,
    pub raw: Option<RawProviderData>,
}

impl ProviderError {
    pub fn new(kind: ProviderErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            retryable: kind.retryable_by_default(),
            retry_after: None,
            request_id: None,
            message: message.into(),
            partial_usage: None,
            raw: None,
        }
    }

    #[must_use]
    pub fn with_partial_usage(mut self, usage: Usage) -> Self {
        self.partial_usage = Some(usage);
        self
    }

    pub fn context_exceeded(&self) -> bool {
        matches!(self.kind, ProviderErrorKind::ContextExceeded)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderErrorKind {
    Unsupported,
    Auth,
    InvalidRequest,
    ContextExceeded,
    RateLimited,
    Overloaded,
    Transport,
    Timeout,
    Server,
    StreamProtocol,
}

impl ProviderErrorKind {
    /// Default retryability from `docs/design/protocol.md` §8.
    pub fn retryable_by_default(self) -> bool {
        matches!(
            self,
            Self::RateLimited | Self::Overloaded | Self::Transport | Self::Timeout | Self::Server
        )
    }
}
