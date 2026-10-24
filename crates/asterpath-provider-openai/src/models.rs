//! Per-model request rules: the single place the adapter branches on model id.
//!
//! Source: developers.openai.com docs as of 2026-10-24 — the "Reasoning.effort
//! supports" line on each model page (api/docs/models/<id>) and the effort
//! values in the reasoning guide (api/docs/guides/reasoning). The o-series
//! pages list no effort set; their row keeps the original `low|medium|high`.
//!
//! `server_tools`: built-in tools (web search, file search, code interpreter…)
//! are documented for the GPT-5 family and o3 / o4 in the Responses tool
//! guides (api/docs/guides/tools-*, checked 2026-10-24); o1 and unrecognized
//! ids (third-party endpoints) are not marked.
//!
//! To update: add a row for each new model id. Rows match by prefix, first match
//! wins, so list a specific id (`gpt-5.5-pro`) before its prefix (`gpt-5.5`),
//! and `gpt-5` last of the GPT-5 family. Ids no row matches get [`LATEST`], so
//! a new model works without a row as long as it follows the newest rules.

use std::borrow::Cow;

use asterpath_protocol::ReasoningEffort::{self, High, Low, Max, Medium, Minimal, Off, XHigh};

/// The request rules for one model behind one provider instance.
///
/// The adapter's built-in table covers official model ids; hosts inject
/// profiles for other endpoints with
/// [`OpenAiProvider::with_default_profile`](crate::OpenAiProvider::with_default_profile)
/// and [`with_model_profile`](crate::OpenAiProvider::with_model_profile).
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenAiModelProfile {
    /// Accepted `reasoning.effort` levels.
    pub efforts: Cow<'static, [ReasoningEffort]>,
    /// Accepts `tool_choice` `required` / a named function.
    pub forced_tool_choice: bool,
    /// Accepts `temperature` / `top_p`.
    pub sampling: bool,
    /// Accepts built-in (server-executed) tools in `tools`.
    pub server_tools: bool,
    /// `Part::File` media types sent as `input_file`; empty means no file
    /// input.
    pub file_media_types: Cow<'static, [&'static str]>,
}

impl OpenAiModelProfile {
    /// The rules unrecognized model ids get: the newest generation's, without
    /// built-in tools, which only listed models accept.
    pub fn latest() -> Self {
        LATEST_PROFILE.clone()
    }

    #[must_use]
    pub fn with_efforts(mut self, efforts: impl Into<Cow<'static, [ReasoningEffort]>>) -> Self {
        self.efforts = efforts.into();
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
    pub fn with_server_tools(mut self, server_tools: bool) -> Self {
        self.server_tools = server_tools;
        self
    }

    #[must_use]
    pub fn with_file_media_types(mut self, types: impl Into<Cow<'static, [&'static str]>>) -> Self {
        self.file_media_types = types.into();
        self
    }
}

const fn profile(efforts: &'static [ReasoningEffort]) -> OpenAiModelProfile {
    OpenAiModelProfile {
        efforts: Cow::Borrowed(efforts),
        forced_tool_choice: true,
        sampling: true,
        server_tools: true,
        file_media_types: Cow::Borrowed(PDF),
    }
}

const fn unlisted(mut profile: OpenAiModelProfile) -> OpenAiModelProfile {
    profile.server_tools = false;
    profile
}

/// PDF input (api/docs/guides/pdf-files) on every vision-capable model; hosts
/// widen the list for other document types.
const PDF: &[&str] = &["application/pdf"];

/// GPT-6.1 Sol and unrecognized ids.
const LATEST: &[ReasoningEffort] = &[Low, Medium, High, XHigh, Max];
const NONE_TO_MAX: &[ReasoningEffort] = &[Off, Low, Medium, High, XHigh, Max];
const NONE_TO_XHIGH: &[ReasoningEffort] = &[Off, Low, Medium, High, XHigh];
const LOW_TO_XHIGH: &[ReasoningEffort] = &[Low, Medium, High, XHigh];
const PRO: &[ReasoningEffort] = &[Medium, High, XHigh];
const LOW_TO_HIGH: &[ReasoningEffort] = &[Low, Medium, High];

static TABLE: &[(&str, OpenAiModelProfile)] = &[
    ("gpt-6.1-sol", profile(LATEST)),
    ("gpt-6-astra", profile(LATEST)),
    ("gpt-6-luna", profile(NONE_TO_MAX)),
    ("gpt-6-sol", profile(NONE_TO_MAX)),
    ("gpt-5.6", profile(NONE_TO_MAX)),
    ("gpt-5.5-pro", profile(PRO)),
    ("gpt-5.5", profile(NONE_TO_XHIGH)),
    ("gpt-5.4-pro", profile(PRO)),
    ("gpt-5.4", profile(NONE_TO_XHIGH)),
    ("gpt-5.3-codex", profile(LOW_TO_XHIGH)),
    ("gpt-5.2-codex", profile(LOW_TO_XHIGH)),
    ("gpt-5.2-pro", profile(PRO)),
    ("gpt-5.2", profile(NONE_TO_XHIGH)),
    ("gpt-5.1", profile(&[Off, Low, Medium, High])),
    ("gpt-5-pro", profile(&[High])),
    ("gpt-5", profile(&[Minimal, Low, Medium, High])),
    ("o1", unlisted(profile(LOW_TO_HIGH))),
    ("o3", profile(LOW_TO_HIGH)),
    ("o4", profile(LOW_TO_HIGH)),
];

static LATEST_PROFILE: OpenAiModelProfile = unlisted(profile(LATEST));

/// Host model entries (first prefix match wins), then the host default, then
/// the built-in table, then [`LATEST`].
pub(crate) fn profile_for<'a>(
    models: &'a [(String, OpenAiModelProfile)],
    default: Option<&'a OpenAiModelProfile>,
    model: &str,
) -> &'a OpenAiModelProfile {
    models
        .iter()
        .find(|(prefix, _)| model.starts_with(prefix.as_str()))
        .map(|(_, profile)| profile)
        .or(default)
        .unwrap_or_else(|| builtin(model))
}

fn builtin(model: &str) -> &'static OpenAiModelProfile {
    TABLE
        .iter()
        .find(|(prefix, _)| model.starts_with(prefix))
        .map_or(&LATEST_PROFILE, |(_, profile)| profile)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn specific_ids_win_over_their_prefixes() {
        let efforts = |model| &*builtin(model).efforts;
        assert_eq!(efforts("gpt-5.5-pro-2026-04-01"), PRO);
        assert_eq!(efforts("gpt-5.5"), NONE_TO_XHIGH);
        assert_eq!(efforts("gpt-5-pro"), &[High]);
        assert_eq!(efforts("gpt-5.1"), &[Off, Low, Medium, High]);
        assert_eq!(efforts("gpt-5-mini"), &[Minimal, Low, Medium, High]);
        assert_eq!(efforts("gpt-next"), LATEST);
    }
}
