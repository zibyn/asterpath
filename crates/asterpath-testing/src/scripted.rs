//! Deterministic scripted model provider.
//!
//! Replays fixed responses or failures, records every received `ModelRequest`
//! for assertions, and performs the adapter-side capability check before any
//! transport would happen. No network, no clock, no sleeping.

use std::collections::VecDeque;
use std::sync::{Mutex, PoisonError};

use asterpath_protocol::{
    Item, ModelRequest, Part, PartDelta, Provider, ProviderCapabilities, ProviderError,
    ProviderErrorKind, ProviderId, StopReason, StreamEvent, ToolChoice, Usage,
};
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};

/// One scripted model attempt.
#[derive(Clone, Debug, PartialEq)]
pub enum ScriptedStep {
    Respond {
        items: Vec<Item>,
        stop_reason: StopReason,
        usage: Usage,
    },
    /// Emit the completed items, then fail without `ResponseEnded`.
    PartialThenFail {
        items: Vec<Item>,
        error: ProviderError,
    },
    Fail(ProviderError),
}

pub struct ScriptedProvider {
    id: ProviderId,
    capabilities: ProviderCapabilities,
    script: Mutex<VecDeque<ScriptedStep>>,
    requests: Mutex<Vec<ModelRequest>>,
}

impl ScriptedProvider {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: ProviderId::new(id),
            capabilities: ProviderCapabilities::default(),
            script: Mutex::new(VecDeque::new()),
            requests: Mutex::new(Vec::new()),
        }
    }

    #[must_use]
    pub fn with_capabilities(mut self, capabilities: ProviderCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    #[must_use]
    pub fn respond(self, items: Vec<Item>, stop_reason: StopReason, usage: Usage) -> Self {
        self.push(ScriptedStep::Respond {
            items,
            stop_reason,
            usage,
        })
    }

    #[must_use]
    pub fn partial_then_fail(self, items: Vec<Item>, error: ProviderError) -> Self {
        self.push(ScriptedStep::PartialThenFail { items, error })
    }

    #[must_use]
    pub fn fail(self, error: ProviderError) -> Self {
        self.push(ScriptedStep::Fail(error))
    }

    /// Every request the provider received, in call order.
    pub fn requests(&self) -> Vec<ModelRequest> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Scripted attempts not yet consumed.
    pub fn remaining(&self) -> usize {
        self.script
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    fn push(self, step: ScriptedStep) -> Self {
        self.script
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(step);
        self
    }

    /// Adapter-side capability check: runs before any transport call.
    fn capability_error(&self, request: &ModelRequest) -> Option<ProviderError> {
        let unsupported = |detail: &str| {
            Some(ProviderError::new(
                ProviderErrorKind::Unsupported,
                format!("{detail} is not supported by this model"),
            ))
        };
        if !self.capabilities.forced_tool_choice
            && matches!(
                request.tool_choice,
                ToolChoice::Required | ToolChoice::Tool(_)
            )
        {
            return unsupported("forced tool choice");
        }
        if !self.capabilities.sampling && request.sampling.is_some() {
            return unsupported("sampling");
        }
        if !self.capabilities.parallel_tool_calls && request.disable_parallel_tool_use {
            return unsupported("disabling parallel tool calls");
        }
        if !self.capabilities.image_input
            && request.items.iter().any(|item| {
                matches!(
                    item,
                    Item::Message { parts, .. }
                        if parts.iter().any(|part| matches!(part, Part::Image { .. }))
                )
            })
        {
            return unsupported("image input");
        }
        None
    }
}

impl Provider for ScriptedProvider {
    fn id(&self) -> ProviderId {
        self.id.clone()
    }

    fn capabilities(&self, _spec: &asterpath_protocol::ModelSpec) -> ProviderCapabilities {
        self.capabilities
    }

    fn stream(&self, request: ModelRequest) -> BoxStream<'_, Result<StreamEvent, ProviderError>> {
        self.requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(request.clone());

        if let Some(error) = self.capability_error(&request) {
            return stream::once(async move { Err(error) }).boxed();
        }

        let step = self
            .script
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop_front();
        let events = match step {
            Some(ScriptedStep::Respond {
                items,
                stop_reason,
                usage,
            }) => item_events(items, Some((stop_reason, usage))),
            Some(ScriptedStep::PartialThenFail { items, error }) => {
                let mut events = item_events(items, None);
                events.push(Err(error));
                events
            }
            Some(ScriptedStep::Fail(error)) => vec![Err(error)],
            None => vec![Err(ProviderError::new(
                ProviderErrorKind::StreamProtocol,
                "scripted provider script is exhausted",
            ))],
        };
        stream::iter(events).boxed()
    }
}

fn item_events(
    items: Vec<Item>,
    terminal: Option<(StopReason, Usage)>,
) -> Vec<Result<StreamEvent, ProviderError>> {
    let mut events = Vec::new();
    let mut sequence = 0;
    events.push(Ok(StreamEvent::ResponseStarted {
        sequence,
        request_id: None,
    }));
    for (output_index, item) in items.into_iter().enumerate() {
        sequence += 1;
        let output_index = output_index as u32;
        events.push(Ok(StreamEvent::ItemStarted {
            sequence,
            output_index,
        }));
        if let Item::Message { parts, .. } = &item {
            for (part_index, part) in parts.iter().enumerate() {
                if let Part::Text { text } = part {
                    sequence += 1;
                    events.push(Ok(StreamEvent::PartDelta {
                        sequence,
                        output_index,
                        part_index: part_index as u32,
                        delta: PartDelta::Text { text: text.clone() },
                    }));
                }
            }
        }
        sequence += 1;
        events.push(Ok(StreamEvent::ItemCompleted {
            sequence,
            output_index,
            item,
        }));
    }
    if let Some((stop_reason, usage)) = terminal {
        sequence += 1;
        events.push(Ok(StreamEvent::ResponseEnded {
            sequence,
            stop_reason,
            usage,
        }));
    }
    events
}
