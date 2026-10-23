//! Deterministic scripted model provider.
//!
//! Replays fixed responses or failures, records every received `ModelRequest`
//! for assertions, and performs the adapter-side capability check before any
//! transport would happen. No network, no clock, no sleeping.

use std::collections::VecDeque;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, PoisonError};

use asterpath_protocol::{
    Item, ModelRequest, Part, PartDelta, Provider, ProviderCapabilities, ProviderError,
    ProviderErrorKind, ProviderId, StopReason, StreamEvent, ToolName, Usage,
};
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};
use serde::Deserialize;

/// One scripted model response in an acceptance fixture.
#[derive(Clone, Debug, Deserialize)]
pub struct FixtureStep {
    pub items: Vec<Item>,
    pub stop_reason: StopReason,
    #[serde(default)]
    pub usage: Usage,
}

impl FixtureStep {
    /// The provider stream events for this response. Shared by the ordered
    /// scripted provider and content-routed providers in acceptance harnesses.
    pub fn events(&self) -> Vec<Result<StreamEvent, ProviderError>> {
        item_events(
            self.items.clone(),
            Some((self.stop_reason.clone(), self.usage)),
        )
    }
}

/// Acceptance fixture: the single source for a scripted session.
///
/// Only the provider and clock are scripted; tool assembly stays in
/// `asterpath-acceptance`.
#[derive(Clone, Debug, Deserialize)]
pub struct Fixture {
    #[serde(default = "default_provider")]
    pub provider: String,
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default)]
    pub system: Vec<Part>,
    #[serde(default)]
    pub tools: Vec<ToolName>,
    /// Stream half a response, print `stream_open`, then hang. Used by the
    /// model-stream crash scenario on the first process only.
    #[serde(default)]
    pub stream_open_first_attempt: bool,
    /// Cancel the turn on the first live delta, for cancellation windows.
    #[serde(default)]
    pub cancel_on_first_delta: bool,
    pub steps: Vec<FixtureStep>,
}

fn default_provider() -> String {
    "p".to_owned()
}

fn default_model() -> String {
    "model-1".to_owned()
}

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
    stream_open_first_attempt: bool,
    first_attempt: AtomicBool,
}

impl ScriptedProvider {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: ProviderId::new(id),
            capabilities: ProviderCapabilities::default(),
            script: Mutex::new(VecDeque::new()),
            requests: Mutex::new(Vec::new()),
            stream_open_first_attempt: false,
            first_attempt: AtomicBool::new(true),
        }
    }

    /// Replay a fixture's responses in order.
    pub fn from_fixture(fixture: &Fixture) -> Self {
        let mut provider = Self::new(fixture.provider.clone());
        for step in &fixture.steps {
            provider = provider.respond(step.items.clone(), step.stop_reason.clone(), step.usage);
        }
        provider
    }

    /// On the first attempt only: emit the response without `ResponseEnded`,
    /// print the `stream_open` marker and never finish. The parent test kills
    /// this process.
    #[must_use]
    pub fn stream_open_on_first_attempt(mut self) -> Self {
        self.stream_open_first_attempt = true;
        self
    }

    /// Drop already-replayed steps after a crash; recovery resumes at the
    /// first attempt not present in the committed prefix.
    pub fn skip(&self, steps: usize) {
        let mut script = self.script.lock().unwrap_or_else(PoisonError::into_inner);
        for _ in 0..steps {
            script.pop_front();
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
        let detail = self.capabilities.unsupported_feature(request)?;
        Some(ProviderError::new(
            ProviderErrorKind::Unsupported,
            format!("{detail} is not supported by this model"),
        ))
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
        let open_on_this_attempt = matches!(step, Some(ScriptedStep::Respond { .. }))
            && self.stream_open_first_attempt
            && self.first_attempt.swap(false, Ordering::SeqCst);
        let events = match step {
            Some(ScriptedStep::Respond {
                items,
                stop_reason,
                usage,
            }) => item_events(
                items,
                (!open_on_this_attempt).then_some((stop_reason, usage)),
            ),
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
        if open_on_this_attempt {
            let mut stdout = std::io::stdout().lock();
            let _ = writeln!(stdout, "{{\"event\":\"stream_open\"}}");
            let _ = stdout.flush();
            return stream::iter(events).chain(stream::pending()).boxed();
        }
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
