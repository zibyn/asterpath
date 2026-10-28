//! Default compaction capability.
//!
//! `plan` is pure: it cuts only at committed step boundaries, keeps at least
//! the latest complete step and targets a fraction of the usable window.
//! `compact` forks the session's request cut to the replaced range and appends
//! the summary prompt, so the summary call reads the session's cached prefix
//! (#176). A different summary model shares no cache and gets an independent
//! text-only request instead. It never falls back to another provider.

// The public compaction error shape is fixed by protocol.md §6.1.
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use asterpath_protocol::{
    CompactPlan, CompactSummary, CompactionConfig, CompactionError, CompactionInput, CompactionJob,
    CompactionOutcome, Compactor, Item, ModelRequest, ModelSpec, Part, ProjectedOrigin,
    ProviderCatalog, ProviderError, ProviderErrorKind, RequestConfig, Role, StreamEvent,
    ToolChoice, TurnConfig, Usage,
};
use asterpath_protocol::{LogId, LogIdRange};
use futures_core::future::BoxFuture;
use futures_util::StreamExt;

fn origin_start(origin: &ProjectedOrigin) -> LogId {
    match origin {
        ProjectedOrigin::Log(id) => *id,
        ProjectedOrigin::Summary(range) => range.first,
    }
}

fn origin_end(origin: &ProjectedOrigin) -> LogId {
    match origin {
        ProjectedOrigin::Log(id) => *id,
        ProjectedOrigin::Summary(range) => range.last,
    }
}

/// Fixed text substituted for every non-text part.
const NON_TEXT_PLACEHOLDER: &str = "[non-text content omitted]";

/// Fixed summary prompt; not exposed to applications in v1.
const SUMMARY_PROMPT: &str = "Summarize the conversation segment for future turns. \
Keep it compact and factual. Include: the goal, work completed, key facts and files, \
open questions, and the next steps. Reply with text only; do not call tools.";

const DEFAULT_RESERVE: u32 = 8192;

/// Provider-backed compactor. The catalog is injected at construction; the
/// module never discovers providers or reads the session log.
pub struct DefaultCompactor {
    providers: Arc<ProviderCatalog>,
}

impl DefaultCompactor {
    pub fn new(providers: Arc<ProviderCatalog>) -> Self {
        Self { providers }
    }
}

impl Compactor for DefaultCompactor {
    fn plan(&self, input: &CompactionInput) -> CompactPlan {
        let Some(usable) = usable_window(&input.model, &input.config) else {
            return CompactPlan::NotConfigured;
        };
        if input.trigger == asterpath_protocol::CompactionTrigger::Threshold {
            let Some(anchor) = &input.last_usage else {
                return CompactPlan::NotNeeded;
            };
            let estimated =
                anchor.usage.input_total() + estimate_tokens(&input.history, Some(anchor.origin));
            if estimated <= u64::from(usable) {
                return CompactPlan::NotNeeded;
            }
        }
        let Some(first) = input.history.first() else {
            return CompactPlan::NothingToDo;
        };
        // A cut must keep at least the latest complete step and must remove at
        // least one complete step (C2).
        if input.step_starts.len() < 2 {
            return CompactPlan::NothingToDo;
        }

        let target = (f64::from(input.config.target_ratio) * f64::from(usable)).max(0.0) as u64;
        let retained_at =
            |index: usize| estimate_tokens(&input.history, Some(input.step_starts[index]));
        // Accumulate from the newest step backwards; the first step at which
        // retention reaches the target is the oldest step to keep.
        let mut first_kept = input.step_starts[1];
        for index in (1..input.step_starts.len()).rev() {
            if retained_at(index) >= target {
                first_kept = input.step_starts[index];
                break;
            }
        }

        let Some(last_replaced) = input
            .history
            .iter()
            .rfind(|entry| origin_start(&entry.origin) < first_kept)
        else {
            return CompactPlan::NothingToDo;
        };
        let replaces = LogIdRange {
            first: origin_start(&first.origin),
            last: origin_end(&last_replaced.origin),
        };

        let replaced = || {
            input
                .history
                .iter()
                .filter(|entry| origin_start(&entry.origin) < first_kept)
        };
        let summary_input = replaced()
            .flat_map(|entry| text_view(&entry.items))
            .collect();
        // The session request's items are `history` flattened, so the replaced
        // range is its first `cut` items.
        let cut = replaced().map(|entry| entry.items.len()).sum();
        let prefix = input.request.clone().map(|mut request| {
            request.items.truncate(cut);
            request
        });

        CompactPlan::Summarize(CompactionJob {
            first_kept,
            replaces,
            model: input
                .config
                .summary_model
                .clone()
                .unwrap_or_else(|| input.model.clone()),
            summary_input,
            max_summary_tokens: input.config.max_summary_tokens,
            usable_window: usable,
            retained_tokens: retained_at(
                input
                    .step_starts
                    .iter()
                    .position(|id| *id == first_kept)
                    .unwrap_or(1),
            ),
            turn: input.turn.clone(),
            session: None,
            prefix,
            cache_ttl: input.config.cache_ttl,
        })
    }

    fn compact<'a>(
        &'a self,
        job: CompactionJob,
    ) -> BoxFuture<'a, Result<CompactSummary, CompactionError>> {
        Box::pin(async move { self.summarize(job).await })
    }
}

impl DefaultCompactor {
    async fn summarize(&self, job: CompactionJob) -> Result<CompactSummary, CompactionError> {
        let Some(provider) = self.providers.get(&job.model.provider) else {
            return Err(CompactionError::Provider(ProviderError::new(
                ProviderErrorKind::InvalidRequest,
                format!("summary provider {} is not registered", job.model.provider),
            )));
        };
        let prompt = Item::Message {
            role: Role::User,
            parts: vec![Part::Text {
                text: SUMMARY_PROMPT.to_owned(),
            }],
            raw: None,
        };
        let mut request = match job.prefix.clone().filter(|prefix| prefix.spec == job.model) {
            Some(mut request) => {
                // Cache-safe fork (#176): the session's system, tools,
                // parameters and history, with the prompt appended last.
                request.items.push(prompt);
                // `tool_choice` and the output format are part of the cached
                // prefix; change them only when they would force a non-text reply.
                if !matches!(request.tool_choice, ToolChoice::Auto | ToolChoice::None) {
                    request.tool_choice = ToolChoice::None;
                }
                request.output_format = None;
                // A thinking budget counts against the output limit.
                let budget = request.reasoning.as_ref().and_then(|r| r.budget_tokens);
                request.max_output_tokens =
                    job.max_summary_tokens.saturating_add(budget.unwrap_or(0));
                request
            }
            None => {
                let mut items = vec![prompt];
                items.extend(job.summary_input.iter().cloned());
                let mut request = ModelRequest::new(
                    job.model.clone(),
                    0,
                    Vec::new(),
                    items,
                    Vec::new(),
                    Vec::new(),
                    &RequestConfig::default(),
                );
                request.tool_choice = ToolChoice::None;
                request.max_output_tokens = job.max_summary_tokens;
                request
            }
        };
        request.session.clone_from(&job.session);
        request.cache_ttl = job.cache_ttl;

        let (summary, usage) = match self.call_summary(provider.as_ref(), request, &job).await {
            Ok(result) => result,
            Err(error) => {
                return Err(CompactionError::Provider(error));
            }
        };
        let summary_tokens = summary.iter().map(part_bytes).sum::<u64>().div_ceil(3);
        if summary_tokens + job.retained_tokens > u64::from(job.usable_window) {
            return Ok(CompactSummary::Insufficient);
        }
        Ok(CompactSummary::Applied(CompactionOutcome {
            first_kept: job.first_kept,
            summary,
            replaces: job.replaces,
            model: job.model,
            usage,
            raw: None,
        }))
    }

    async fn call_summary(
        &self,
        provider: &dyn asterpath_protocol::Provider,
        request: ModelRequest,
        job: &CompactionJob,
    ) -> Result<(Vec<Part>, Usage), ProviderError> {
        let max_attempts = job.turn.max_attempts.get();
        let mut attempt = 1u32;
        loop {
            let result = self.call_once(provider, request.clone(), job).await;
            match result {
                Ok(result) => return Ok(result),
                Err(error) if error.retryable && attempt < max_attempts => {
                    let wait = backoff(&job.turn, attempt);
                    if !wait.is_zero() {
                        tokio::time::sleep(wait).await;
                    }
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn call_once(
        &self,
        provider: &dyn asterpath_protocol::Provider,
        request: ModelRequest,
        job: &CompactionJob,
    ) -> Result<(Vec<Part>, Usage), ProviderError> {
        let consume = async {
            let mut stream = provider.stream(request);
            let mut completed: BTreeMap<u32, Item> = BTreeMap::new();
            let mut terminal: Option<(asterpath_protocol::StopReason, Usage)> = None;
            while let Some(event) = stream.next().await {
                match event? {
                    StreamEvent::ItemCompleted {
                        output_index, item, ..
                    } => {
                        completed.insert(output_index, item);
                    }
                    StreamEvent::ResponseEnded {
                        stop_reason, usage, ..
                    } => {
                        terminal = Some((stop_reason, usage));
                        break;
                    }
                    _ => {}
                }
            }
            let Some((_, usage)) = terminal else {
                return Err(ProviderError::new(
                    ProviderErrorKind::StreamProtocol,
                    "summary stream ended without ResponseEnded",
                ));
            };
            let mut summary = Vec::new();
            for item in completed.into_values() {
                if let Item::Message { parts, .. } = item {
                    summary.extend(
                        parts
                            .into_iter()
                            .filter(|part| matches!(part, Part::Text { .. })),
                    );
                }
            }
            // A forked request keeps `tool_choice: Auto`, so the reply may be
            // tool calls only; an empty summary would erase the history.
            if !summary
                .iter()
                .any(|part| matches!(part, Part::Text { text } if !text.trim().is_empty()))
            {
                return Err(ProviderError::new(
                    ProviderErrorKind::StreamProtocol,
                    "summary reply has no text",
                ));
            }
            Ok((summary, usage))
        };
        match job.turn.request_timeout {
            Some(timeout) => tokio::time::timeout(timeout, consume)
                .await
                .unwrap_or_else(|_| {
                    Err(ProviderError::new(
                        ProviderErrorKind::Timeout,
                        "summary request timed out",
                    ))
                }),
            None => consume.await,
        }
    }
}

// `CompactionJob` carries the turn's retry policy, so the summary call uses the
// same attempt/backoff/timeout shape as normal model calls.
/// Usable window from the one configured source; `None` disables compaction.
pub(crate) fn usable_window(model: &ModelSpec, config: &CompactionConfig) -> Option<u32> {
    let window = model.context_window?;
    let reserve = config
        .reserve_output_tokens
        .unwrap_or_else(|| (window / 4).min(DEFAULT_RESERVE));
    Some(window.saturating_sub(reserve))
}

fn estimate_tokens(history: &[asterpath_protocol::ProjectedEntry], from: Option<LogId>) -> u64 {
    let mut bytes = 0u64;
    for entry in history {
        if let Some(from) = from
            && origin_start(&entry.origin) <= from
        {
            continue;
        }
        bytes += serde_json::to_string(&entry.items)
            .map(|json| json.len() as u64)
            .unwrap_or(0);
    }
    bytes.div_ceil(3)
}

fn part_bytes(part: &Part) -> u64 {
    match part {
        Part::Text { text } => text.len() as u64,
        Part::Refusal { text } => text.len() as u64,
        _ => NON_TEXT_PLACEHOLDER.len() as u64,
    }
}

/// Strip reasoning, provider raw data and non-text parts from the summary
/// input. Tool calls and results keep their text form.
fn text_view(items: &[Item]) -> Vec<Item> {
    let mut view = Vec::new();
    for item in items {
        match item {
            Item::Message { role, parts, .. } => {
                let parts: Vec<Part> = parts.iter().filter_map(text_part).collect();
                if !parts.is_empty() {
                    view.push(Item::Message {
                        role: *role,
                        parts,
                        raw: None,
                    });
                }
            }
            // Never forward reasoning: the independent summary request's system differs from
            // the session's, and Anthropic rejects thinking replayed under it (#104).
            // Instructions and tool changes are rebased into the baseline, never
            // summarized (ADR-0030).
            Item::Reasoning { .. }
            | Item::ProviderOpaque { .. }
            | Item::ServerToolCall { .. }
            | Item::Instruction { .. }
            | Item::ToolsChanged { .. }
            | Item::ReasoningChanged { .. } => {}
            Item::ToolCall { name, raw_args, .. } => view.push(text_item(format!(
                "tool call {name}({})",
                raw_args.as_str()
            ))),
            Item::ToolResult {
                call_id,
                parts,
                is_error,
                ..
            } => {
                let text = join_text(parts);
                let label = if *is_error { "error" } else { "output" };
                view.push(text_item(format!(
                    "tool result {call_id} ({label}): {text}"
                )));
            }
            Item::ServerToolResult { call_id, parts, .. } => {
                let text = join_text(parts);
                view.push(text_item(format!(
                    "server tool result {}: {text}",
                    call_id.as_deref().unwrap_or("-")
                )));
            }
            Item::Compaction { parts } => {
                let parts: Vec<Part> = parts.iter().filter_map(text_part).collect();
                if !parts.is_empty() {
                    view.push(Item::Message {
                        role: Role::User,
                        parts,
                        raw: None,
                    });
                }
            }
        }
    }
    view
}

fn text_part(part: &Part) -> Option<Part> {
    match part {
        Part::Text { .. } => Some(part.clone()),
        Part::Refusal { text } => Some(Part::Text { text: text.clone() }),
        // Provider opaque data is dropped, not rendered.
        Part::ProviderOpaque { .. } => None,
        _ => Some(Part::Text {
            text: NON_TEXT_PLACEHOLDER.to_owned(),
        }),
    }
}

fn join_text(parts: &[Part]) -> String {
    parts
        .iter()
        .filter_map(text_part)
        .map(|part| match part {
            Part::Text { text } => text,
            _ => String::new(),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn text_item(text: String) -> Item {
    Item::Message {
        role: Role::User,
        parts: vec![Part::Text { text }],
        raw: None,
    }
}

fn backoff(config: &TurnConfig, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(10);
    config
        .initial_backoff
        .saturating_mul(1u32 << exponent)
        .min(config.max_backoff)
}

#[cfg(test)]
mod tests;
