//! Canonical conversation items: the model-visible history is a flat ordered
//! `Vec<Item>`, and a `Part` is a content fragment inside an item.

use serde::{Deserialize, Serialize};

use crate::ids::{JsonString, ModelId, ProviderId, ToolName};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Role {
    User,
    Assistant,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Item {
    Message {
        role: Role,
        parts: Vec<Part>,
        /// Provider message data. Replay still renders `parts`; only the
        /// same provider merges known fields from it (ADR 0010).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        raw: Option<RawProviderData>,
    },
    Reasoning {
        summary: Vec<Part>,
        binding: ReasoningBinding,
        raw: RawProviderData,
    },
    ToolCall {
        call_id: String,
        provider_item_id: Option<String>,
        name: ToolName,
        raw_args: JsonString,
        raw: Option<RawProviderData>,
    },
    ToolResult {
        call_id: String,
        provider_item_id: Option<String>,
        parts: Vec<Part>,
        is_error: bool,
        raw: Option<RawProviderData>,
    },
    ServerToolCall {
        provider: ProviderId,
        name: String,
        call_id: Option<String>,
        raw: RawProviderData,
    },
    ServerToolResult {
        provider: ProviderId,
        call_id: Option<String>,
        parts: Vec<Part>,
        raw: RawProviderData,
    },
    Compaction {
        parts: Vec<Part>,
    },
    ProviderOpaque {
        provider: ProviderId,
        kind: String,
        raw: RawProviderData,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Part {
    Text {
        text: String,
    },
    Image {
        media_type: String,
        source: ImageSource,
    },
    Refusal {
        text: String,
    },
    ProviderOpaque {
        provider: ProviderId,
        kind: String,
        raw: RawProviderData,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ImageSource {
    Base64 { data: String },
    Url { url: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReasoningBinding {
    pub provider: ProviderId,
    pub model: ModelId,
    /// Number of compactions applied when this reasoning was produced.
    pub prefix_epoch: u32,
    /// Fingerprint of the tool set the producing request rendered.
    pub tools_hash: String,
}

/// Deterministic fingerprint of the model-visible tool set (protocol.md §9):
/// SHA-256 hex of the name-sorted, key-sorted compact JSON array of
/// `{name, description, schema}`. Persisted in [`ReasoningBinding::tools_hash`].
pub fn tools_hash(tools: &[crate::ToolDescriptor]) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;

    let mut tools: Vec<&crate::ToolDescriptor> = tools.iter().collect();
    tools.sort_by(|left, right| left.name.as_str().cmp(right.name.as_str()));
    let canonical: Vec<serde_json::Value> = tools
        .iter()
        .map(|tool| {
            serde_json::json!({
                "name": tool.name.as_str(),
                "description": tool.description,
                "schema": tool.schema,
            })
        })
        .collect();
    let text = serde_json::to_string(&sort_keys(serde_json::Value::Array(canonical)))
        .expect("tool fingerprint serializes");
    let mut hex = String::with_capacity(64);
    for byte in Sha256::digest(text.as_bytes()) {
        write!(hex, "{byte:02x}").expect("writing to a String cannot fail");
    }
    hex
}

fn sort_keys(value: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(String, Value)> = map.into_iter().collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, sort_keys(value)))
                    .collect(),
            )
        }
        Value::Array(items) => Value::Array(items.into_iter().map(sort_keys).collect()),
        other => other,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawProviderData {
    pub provider: ProviderId,
    pub model: Option<ModelId>,
    pub kind: String,
    pub json: JsonString,
}
