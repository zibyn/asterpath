//! Per-model request rules: the single place the adapter branches on model id.
//!
//! Source: platform.claude.com docs as of 2026-10-23 — the per-model thinking
//! table (build-with-claude/thinking-troubleshooting), effort supported models
//! (build-with-claude/effort), "Forced tool use not supported" (api/errors) and
//! the Opus 5.5 migration guide (sampling parameters removed from 4.7 on).
//! Effort levels per model: the `xhigh` / `max` rows of the effort levels table
//! (build-with-claude/effort, as of 2026-10-24). `Off` sends no effort; where
//! the thinking table (as of 2026-10-24) defaults to Off it sends no `thinking`
//! either, where it defaults to On it sends `thinking: {type: "disabled"}`.
//! `Off` is `Unsupported` only where that table (build-with-claude/
//! thinking-troubleshooting, checked 2026-10-24) rejects `"disabled"` with a 400:
//! Opus 5.5, Fable 5.1, Mythos 5.1, Fable 5, Mythos 5 and Mythos Preview (thinking
//! "Always on"; the effort page repeats this for Opus 5.5), and Sonnet 5.5 (On by
//! default, but it takes `between_tools` instead of `"disabled"`). Unrecognized
//! ids get [`UNRECOGNIZED`] and so reject `Off` too.
//!
//! `inline_changes`: the supported-models list of build-with-claude/
//! mid-conversation-system-messages (checked 2026-10-24; system messages need
//! no beta header). The same page says mid-conversation tool changes are in
//! beta "on the same models"; definitions by value need
//! `anthropic-beta: inline-tools-2026-09-15`, Claude API only. Sonnet 5 is
//! listed as unsupported; models the page does not list fall back, and so do
//! unrecognized ids (third-party endpoints such as DeepSeek reach
//! [`UNRECOGNIZED`]): only listed models render inline.
//!
//! To update: add a row for each new model id. Rows match by prefix, first match
//! wins, so list a specific id (`claude-opus-5-5`) before its prefix
//! (`claude-opus-5`). Ids no row matches get [`UNRECOGNIZED`], so a new model works
//! without a row as long as it follows the newest rules.

use std::borrow::Cow;

use asterpath_protocol::ReasoningEffort::{self, High, Low, Max, Medium, Off, XHigh};

/// Which `thinking.type` values a model accepts for reasoning requests.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Thinking {
    /// Only `enabled` + `budget_tokens` (4.5 and earlier).
    Extended,
    /// Both; `enabled` is deprecated (4.6).
    Both,
    /// Only `adaptive`; `enabled` returns 400 (4.7 and later).
    Adaptive,
}

/// The request rules for one model behind one provider instance.
///
/// The adapter's built-in table covers official model ids; hosts inject
/// profiles for other endpoints with
/// [`AnthropicProvider::with_default_profile`](crate::AnthropicProvider::with_default_profile)
/// and [`with_model_profile`](crate::AnthropicProvider::with_model_profile).
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnthropicModelProfile {
    pub thinking: Thinking,
    /// Accepted `ReasoningEffort` levels; every one but `Off` is sent as
    /// `output_config.effort`.
    pub efforts: Cow<'static, [ReasoningEffort]>,
    /// Thinking runs when `thinking` is omitted, so `Off` must send `disabled`.
    pub thinking_default_on: bool,
    /// Accepts `tool_choice` `any` / `tool`.
    pub forced_tool_choice: bool,
    /// Accepts `temperature` / `top_p` / `top_k`.
    pub sampling: bool,
    /// Accepts `role: "system"` messages inside `messages`, including
    /// `tool_addition` / `tool_removal` blocks. Without it an appended
    /// instruction joins the top-level `system` and a tool change rewrites the
    /// top-level `tools`; thinking from before the change is dropped.
    pub inline_changes: bool,
}

impl AnthropicModelProfile {
    /// The rules unrecognized model ids get: the newest generation's, without
    /// inline system messages or tools, which only listed models accept.
    pub fn latest() -> Self {
        UNRECOGNIZED
    }

    #[must_use]
    pub fn with_thinking(mut self, thinking: Thinking) -> Self {
        self.thinking = thinking;
        self
    }

    #[must_use]
    pub fn with_efforts(mut self, efforts: impl Into<Cow<'static, [ReasoningEffort]>>) -> Self {
        self.efforts = efforts.into();
        self
    }

    #[must_use]
    pub fn with_thinking_default_on(mut self, on: bool) -> Self {
        self.thinking_default_on = on;
        self
    }

    #[must_use]
    pub fn with_forced_tool_choice(mut self, forced: bool) -> Self {
        self.forced_tool_choice = forced;
        self
    }

    #[must_use]
    pub fn with_sampling(mut self, sampling: bool) -> Self {
        self.sampling = sampling;
        self
    }

    #[must_use]
    pub fn with_inline_changes(mut self, inline: bool) -> Self {
        self.inline_changes = inline;
        self
    }

    /// Accepts `thinking.display`; earlier models always summarize.
    pub(crate) fn thinking_display(&self) -> bool {
        self.thinking != Thinking::Extended
    }
}

const fn rules(
    thinking: Thinking,
    efforts: &'static [ReasoningEffort],
    forced: bool,
    sampling: bool,
) -> AnthropicModelProfile {
    AnthropicModelProfile {
        thinking,
        efforts: Cow::Borrowed(efforts),
        thinking_default_on: false,
        forced_tool_choice: forced,
        sampling,
        inline_changes: false,
    }
}

const fn inline(mut profile: AnthropicModelProfile) -> AnthropicModelProfile {
    profile.inline_changes = true;
    profile
}

const LOW_TO_MAX: &[ReasoningEffort] = &[Low, Medium, High, XHigh, Max];
const OFF_TO_MAX: &[ReasoningEffort] = &[Off, Low, Medium, High, XHigh, Max];
const NO_XHIGH: &[ReasoningEffort] = &[Low, Medium, High, Max];

/// Opus 5.5, Sonnet 5.5, Fable 5.1 and Mythos 5.1.
const LATEST: AnthropicModelProfile = inline(UNRECOGNIZED);
/// Unrecognized ids: [`LATEST`] without inline changes.
const UNRECOGNIZED: AnthropicModelProfile = rules(Thinking::Adaptive, LOW_TO_MAX, false, false);
/// Opus 5 and Haiku 5.5: thinking is on by default and accepts `disabled`.
const ADAPTIVE: AnthropicModelProfile = AnthropicModelProfile {
    thinking: Thinking::Adaptive,
    efforts: Cow::Borrowed(OFF_TO_MAX),
    thinking_default_on: true,
    forced_tool_choice: true,
    sampling: false,
    inline_changes: true,
};
/// Sonnet 5: as [`ADAPTIVE`], but no mid-conversation system messages.
const SONNET_5: AnthropicModelProfile = AnthropicModelProfile {
    inline_changes: false,
    ..ADAPTIVE
};
/// Fable 5 and Mythos 5: thinking is always on.
const ALWAYS_ON: AnthropicModelProfile = inline(rules(Thinking::Adaptive, LOW_TO_MAX, true, false));
/// Opus 4.7: thinking is off by default.
const ADAPTIVE_OFF: AnthropicModelProfile = rules(Thinking::Adaptive, OFF_TO_MAX, true, false);
/// Opus 4.8: as [`ADAPTIVE_OFF`], with mid-conversation system messages.
const OPUS_4_8: AnthropicModelProfile = inline(ADAPTIVE_OFF);
const MYTHOS_PREVIEW: AnthropicModelProfile = rules(Thinking::Both, NO_XHIGH, true, true);
const BOTH: AnthropicModelProfile =
    rules(Thinking::Both, &[Off, Low, Medium, High, Max], true, true);
const EXTENDED: AnthropicModelProfile = rules(Thinking::Extended, &[Off], true, true);
const EXTENDED_EFFORT: AnthropicModelProfile =
    rules(Thinking::Extended, &[Off, Low, Medium, High], true, true);

static TABLE: &[(&str, AnthropicModelProfile)] = &[
    ("claude-opus-5-5", LATEST),
    ("claude-sonnet-5-5", LATEST),
    ("claude-fable-5-1", LATEST),
    ("claude-mythos-5-1", LATEST),
    ("claude-haiku-5-5", ADAPTIVE),
    ("claude-opus-5", ADAPTIVE),
    ("claude-sonnet-5", SONNET_5),
    ("claude-fable-5", ALWAYS_ON),
    ("claude-mythos-5", ALWAYS_ON),
    ("claude-mythos-preview", MYTHOS_PREVIEW),
    ("claude-opus-4-8", OPUS_4_8),
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

static UNRECOGNIZED_PROFILE: AnthropicModelProfile = UNRECOGNIZED;

/// Host model entries (first prefix match wins), then the host default, then
/// the built-in table, then [`UNRECOGNIZED`].
pub(crate) fn profile_for<'a>(
    models: &'a [(String, AnthropicModelProfile)],
    default: Option<&'a AnthropicModelProfile>,
    model: &str,
) -> &'a AnthropicModelProfile {
    models
        .iter()
        .find(|(prefix, _)| model.starts_with(prefix.as_str()))
        .map(|(_, profile)| profile)
        .or(default)
        .unwrap_or_else(|| builtin(model))
}

fn builtin(model: &str) -> &'static AnthropicModelProfile {
    TABLE
        .iter()
        .find(|(prefix, _)| model.starts_with(prefix))
        .map_or(&UNRECOGNIZED_PROFILE, |(_, profile)| profile)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn specific_ids_win_over_their_prefixes() {
        assert_eq!(*builtin("claude-opus-5-5"), LATEST);
        assert_eq!(*builtin("claude-opus-5"), ADAPTIVE);
        assert_eq!(*builtin("claude-opus-4-6"), BOTH);
        assert_eq!(*builtin("claude-opus-4-5-20251101"), EXTENDED_EFFORT);
        assert_eq!(*builtin("claude-opus-4-1-20250805"), EXTENDED);
        assert_eq!(*builtin("claude-sonnet-4-20250514"), EXTENDED);
        assert_eq!(*builtin("claude-next"), UNRECOGNIZED);
    }

    #[test]
    fn inline_changes_follow_the_supported_model_list() {
        for (model, inline) in [
            ("claude-opus-5-5", true),
            ("claude-sonnet-5-5", true),
            ("claude-haiku-5-5", true),
            ("claude-fable-5-1", true),
            ("claude-mythos-5-1", true),
            ("claude-fable-5", true),
            ("claude-mythos-5", true),
            ("claude-opus-5", true),
            ("claude-opus-4-8", true),
            ("claude-next", false),
            ("deepseek-v4-pro", false),
            ("claude-sonnet-5", false),
            ("claude-opus-4-7", false),
            ("claude-mythos-preview", false),
            ("claude-sonnet-4-6", false),
            ("claude-3-haiku", false),
        ] {
            assert_eq!(builtin(model).inline_changes, inline, "{model}");
        }
    }

    #[test]
    fn mythos_preview_accepts_both_thinking_types() {
        assert_eq!(builtin("claude-mythos-preview").thinking, Thinking::Both);
    }

    #[test]
    fn display_is_sent_only_from_4_6_on() {
        assert!(builtin("claude-opus-5-5").thinking_display());
        assert!(builtin("claude-sonnet-4-6").thinking_display());
        assert!(!builtin("claude-sonnet-4-5").thinking_display());
    }
}
