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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawProviderData {
    pub provider: ProviderId,
    pub model: Option<ModelId>,
    pub kind: String,
    pub json: JsonString,
}
