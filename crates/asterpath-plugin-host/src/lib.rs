//! External plugin host: manifests, loading, and MCP adapters.
//!
//! The host supplies a set of [`PluginManifest`]s; [`PluginHost::load`] validates
//! them, spawns stdio servers through the sandbox seam, performs discovery and
//! registers the discovered tools under `<plugin_id>__<sanitized name>`.

pub mod adapter;
mod host;
mod manifest;
mod stdio;

pub use adapter::{AdapterError, AdapterHandshake, AdapterTool, BoxFuture, PluginAdapter};
pub use host::{PluginHost, PluginHostError, PluginLoadStage, PluginStatus};
pub use manifest::{
    DEFAULT_CALL, DEFAULT_STARTUP, PluginCapability, PluginId, PluginManifest, PluginProtocol,
    PluginTimeouts, PluginTransport,
};
