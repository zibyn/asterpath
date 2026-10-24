//! Per-model request rules: the single place the adapter branches on model id.
//!
//! Source: platform.claude.com docs as of 2026-10-23 — the per-model thinking
//! table (build-with-claude/thinking-troubleshooting), effort supported models
//! (build-with-claude/effort), "Forced tool use not supported" (api/errors) and
//! the Opus 5.5 migration guide (sampling parameters removed from 4.7 on).
//!
//! To update: add a row for each new model id. Rows match by prefix, first match
//! wins, so list a specific id (`claude-opus-5-5`) before its prefix
//! (`claude-opus-5`). Ids no row matches get [`LATEST`], so a new model works
//! without a row as long as it follows the newest rules.

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
    /// Accepts `output_config.effort`.
    pub effort: bool,
    /// Accepts `tool_choice` `any` / `tool`.
    pub forced_tool_choice: bool,
    /// Accepts `temperature` / `top_p` / `top_k`.
    pub sampling: bool,
}

const fn rules(thinking: Thinking, effort: bool, forced: bool, sampling: bool) -> ModelRules {
    ModelRules {
        thinking,
        effort,
        forced_tool_choice: forced,
        sampling,
    }
}

/// Opus 5.5, Sonnet 5.5, Fable 5.1, Mythos 5.1 and unrecognized ids.
pub(crate) const LATEST: ModelRules = rules(Thinking::Adaptive, true, false, false);
const ADAPTIVE: ModelRules = rules(Thinking::Adaptive, true, true, false);
const BOTH: ModelRules = rules(Thinking::Both, true, true, true);
const EXTENDED: ModelRules = rules(Thinking::Extended, false, true, true);
const EXTENDED_EFFORT: ModelRules = rules(Thinking::Extended, true, true, true);

const TABLE: &[(&str, ModelRules)] = &[
    ("claude-opus-5-5", LATEST),
    ("claude-sonnet-5-5", LATEST),
    ("claude-fable-5-1", LATEST),
    ("claude-mythos-5-1", LATEST),
    ("claude-haiku-5-5", ADAPTIVE),
    ("claude-opus-5", ADAPTIVE),
    ("claude-sonnet-5", ADAPTIVE),
    ("claude-fable-5", ADAPTIVE),
    ("claude-mythos-5", ADAPTIVE),
    ("claude-opus-4-8", ADAPTIVE),
    ("claude-opus-4-7", ADAPTIVE),
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
}
