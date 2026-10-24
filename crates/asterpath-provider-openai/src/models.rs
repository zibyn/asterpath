//! Per-model request rules: the single place the adapter branches on model id.
//!
//! Source: developers.openai.com docs as of 2026-10-24 — the "Reasoning.effort
//! supports" line on each model page (api/docs/models/<id>) and the effort
//! values in the reasoning guide (api/docs/guides/reasoning). The o-series
//! pages list no effort set; their row keeps the original `low|medium|high`.
//!
//! To update: add a row for each new model id. Rows match by prefix, first match
//! wins, so list a specific id (`gpt-5.5-pro`) before its prefix (`gpt-5.5`),
//! and `gpt-5` last of the GPT-5 family. Ids no row matches get [`LATEST`], so
//! a new model works without a row as long as it follows the newest rules.

use asterpath_protocol::ReasoningEffort::{self, High, Low, Max, Medium, Minimal, Off, XHigh};

/// GPT-6.1 Sol and unrecognized ids.
pub(crate) const LATEST: &[ReasoningEffort] = &[Low, Medium, High, XHigh, Max];
const NONE_TO_MAX: &[ReasoningEffort] = &[Off, Low, Medium, High, XHigh, Max];
const NONE_TO_XHIGH: &[ReasoningEffort] = &[Off, Low, Medium, High, XHigh];
const LOW_TO_XHIGH: &[ReasoningEffort] = &[Low, Medium, High, XHigh];
const PRO: &[ReasoningEffort] = &[Medium, High, XHigh];
const LOW_TO_HIGH: &[ReasoningEffort] = &[Low, Medium, High];

const TABLE: &[(&str, &[ReasoningEffort])] = &[
    ("gpt-6.1-sol", LATEST),
    ("gpt-6-astra", LATEST),
    ("gpt-6-luna", NONE_TO_MAX),
    ("gpt-6-sol", NONE_TO_MAX),
    ("gpt-5.6", NONE_TO_MAX),
    ("gpt-5.5-pro", PRO),
    ("gpt-5.5", NONE_TO_XHIGH),
    ("gpt-5.4-pro", PRO),
    ("gpt-5.4", NONE_TO_XHIGH),
    ("gpt-5.3-codex", LOW_TO_XHIGH),
    ("gpt-5.2-codex", LOW_TO_XHIGH),
    ("gpt-5.2-pro", PRO),
    ("gpt-5.2", NONE_TO_XHIGH),
    ("gpt-5.1", &[Off, Low, Medium, High]),
    ("gpt-5-pro", &[High]),
    ("gpt-5", &[Minimal, Low, Medium, High]),
    ("o1", LOW_TO_HIGH),
    ("o3", LOW_TO_HIGH),
    ("o4", LOW_TO_HIGH),
];

/// The `reasoning.effort` levels the model accepts.
pub(crate) fn efforts_for(model: &str) -> &'static [ReasoningEffort] {
    TABLE
        .iter()
        .find(|(prefix, _)| model.starts_with(prefix))
        .map_or(LATEST, |(_, efforts)| *efforts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn specific_ids_win_over_their_prefixes() {
        assert_eq!(efforts_for("gpt-5.5-pro-2026-04-01"), PRO);
        assert_eq!(efforts_for("gpt-5.5"), NONE_TO_XHIGH);
        assert_eq!(efforts_for("gpt-5-pro"), &[High]);
        assert_eq!(efforts_for("gpt-5.1"), &[Off, Low, Medium, High]);
        assert_eq!(efforts_for("gpt-5-mini"), &[Minimal, Low, Medium, High]);
        assert_eq!(efforts_for("gpt-next"), LATEST);
    }
}
