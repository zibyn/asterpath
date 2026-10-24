//! Per-model request rules: the single place the adapter branches on model id.
//!
//! Source: platform.claude.com docs as of 2026-10-23 — the per-model thinking
//! table (build-with-claude/thinking-troubleshooting), effort supported models
//! (build-with-claude/effort), "Forced tool use not supported" (api/errors) and
//! the Opus 5.5 migration guide (sampling parameters removed from 4.7 on).
//! Effort levels per model: the `xhigh` / `max` rows of the effort levels table
//! (build-with-claude/effort, as of 2026-10-24). `Off` sends no effort; where
//! the thinking table (as of 2026-10-24) defaults to Off it sends no `thinking`
//! either, where it defaults to On it sends `thinking: {type: "disabled"}`, and
//! models that are Always on or reject `"disabled"` (Sonnet 5.5) do not accept it.
//!
//! To update: add a row for each new model id. Rows match by prefix, first match
//! wins, so list a specific id (`claude-opus-5-5`) before its prefix
//! (`claude-opus-5`). Ids no row matches get [`LATEST`], so a new model works
//! without a row as long as it follows the newest rules.

use asterpath_protocol::ReasoningEffort::{self, High, Low, Max, Medium, Off, XHigh};

/// Which `thinking.type` values a model accepts for reasoning requests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Thinking {
    /// Only `enabled` + `budget_tokens` (4.5 and earlier).
    Extended,
    /// Both; `enabled` is deprecated (4.6).
    Both,
    /// Only `adaptive`; `enabled` returns 400 (4.7 and later).
    Adaptive,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ModelRules {
    pub thinking: Thinking,
    /// Accepted `ReasoningEffort` levels; every one but `Off` is sent as
    /// `output_config.effort`.
    pub efforts: &'static [ReasoningEffort],
    /// Thinking runs when `thinking` is omitted, so `Off` must send `disabled`.
    pub thinking_default_on: bool,
    /// Accepts `tool_choice` `any` / `tool`.
    pub forced_tool_choice: bool,
    /// Accepts `temperature` / `top_p` / `top_k`.
    pub sampling: bool,
}

impl ModelRules {
    /// Accepts `thinking.display`; earlier models always summarize.
    pub fn thinking_display(self) -> bool {
        self.thinking != Thinking::Extended
    }
}

const fn rules(
    thinking: Thinking,
    efforts: &'static [ReasoningEffort],
    forced: bool,
    sampling: bool,
) -> ModelRules {
    ModelRules {
        thinking,
        efforts,
        thinking_default_on: false,
        forced_tool_choice: forced,
        sampling,
    }
}

const LOW_TO_MAX: &[ReasoningEffort] = &[Low, Medium, High, XHigh, Max];
const OFF_TO_MAX: &[ReasoningEffort] = &[Off, Low, Medium, High, XHigh, Max];
const NO_XHIGH: &[ReasoningEffort] = &[Low, Medium, High, Max];

/// Opus 5.5, Sonnet 5.5, Fable 5.1, Mythos 5.1 and unrecognized ids.
pub(crate) const LATEST: ModelRules = rules(Thinking::Adaptive, LOW_TO_MAX, false, false);
/// Opus 5, Sonnet 5 and Haiku 5.5: thinking is on by default and accepts `disabled`.
const ADAPTIVE: ModelRules = ModelRules {
    thinking_default_on: true,
    ..rules(Thinking::Adaptive, OFF_TO_MAX, true, false)
};
/// Fable 5 and Mythos 5: thinking is always on.
const ALWAYS_ON: ModelRules = rules(Thinking::Adaptive, LOW_TO_MAX, true, false);
/// Opus 4.7 / 4.8: thinking is off by default.
const ADAPTIVE_OFF: ModelRules = rules(Thinking::Adaptive, OFF_TO_MAX, true, false);
const MYTHOS_PREVIEW: ModelRules = rules(Thinking::Both, NO_XHIGH, true, true);
const BOTH: ModelRules = rules(Thinking::Both, &[Off, Low, Medium, High, Max], true, true);
const EXTENDED: ModelRules = rules(Thinking::Extended, &[Off], true, true);
const EXTENDED_EFFORT: ModelRules =
    rules(Thinking::Extended, &[Off, Low, Medium, High], true, true);

const TABLE: &[(&str, ModelRules)] = &[
    ("claude-opus-5-5", LATEST),
    ("claude-sonnet-5-5", LATEST),
    ("claude-fable-5-1", LATEST),
    ("claude-mythos-5-1", LATEST),
    ("claude-haiku-5-5", ADAPTIVE),
    ("claude-opus-5", ADAPTIVE),
    ("claude-sonnet-5", ADAPTIVE),
    ("claude-fable-5", ALWAYS_ON),
    ("claude-mythos-5", ALWAYS_ON),
    ("claude-mythos-preview", MYTHOS_PREVIEW),
    ("claude-opus-4-8", ADAPTIVE_OFF),
    ("claude-opus-4-7", ADAPTIVE_OFF),
    ("claude-opus-4-6", BOTH),
    ("claude-sonnet-4-6", BOTH),
    ("claude-opus-4-5", EXTENDED_EFFORT),
    ("claude-sonnet-4-5", EXTENDED),
    ("claude-haiku-4-5", EXTENDED),
    ("claude-opus-4", EXTENDED),
    ("claude-sonnet-4", EXTENDED),
    ("claude-3", EXTENDED),
];

pub(crate) fn rules_for(model: &str) -> ModelRules {
    TABLE
        .iter()
        .find(|(prefix, _)| model.starts_with(prefix))
        .map_or(LATEST, |(_, rules)| *rules)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn specific_ids_win_over_their_prefixes() {
        assert_eq!(rules_for("claude-opus-5-5"), LATEST);
        assert_eq!(rules_for("claude-opus-5"), ADAPTIVE);
        assert_eq!(rules_for("claude-opus-4-6"), BOTH);
        assert_eq!(rules_for("claude-opus-4-5-20251101"), EXTENDED_EFFORT);
        assert_eq!(rules_for("claude-opus-4-1-20250805"), EXTENDED);
        assert_eq!(rules_for("claude-sonnet-4-20250514"), EXTENDED);
        assert_eq!(rules_for("claude-next"), LATEST);
    }

    #[test]
    fn mythos_preview_accepts_both_thinking_types() {
        assert_eq!(rules_for("claude-mythos-preview").thinking, Thinking::Both);
    }

    #[test]
    fn display_is_sent_only_from_4_6_on() {
        assert!(rules_for("claude-opus-5-5").thinking_display());
        assert!(rules_for("claude-sonnet-4-6").thinking_display());
        assert!(!rules_for("claude-sonnet-4-5").thinking_display());
    }
}
