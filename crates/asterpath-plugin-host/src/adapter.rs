//! The protocol adapter contract (`mcp.md` §4).
//!
//! An adapter exposes raw tool names only. Composition into
//! `<plugin_id>__<sanitized>` and atomic registration belong to the plugin host.

use std::future::Future;
use std::pin::Pin;

use asterpath_protocol::{
    JsonSchema, JsonString, Parallelism, ReplaySafety, SideEffect, ToolContext, ToolError,
    ToolOutput,
};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdapterHandshake {
    pub protocol_version: String,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq)]
pub struct AdapterTool {
    pub original_name: String,
    pub description: String,
    pub schema: JsonSchema,
    pub replay: ReplaySafety,
    pub side_effect: SideEffect,
    pub parallelism: Parallelism,
}

/// Adapter failure, classified by the load stage it belongs to.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AdapterError {
    #[error("spawn: {message}")]
    Spawn { message: String },
    #[error("handshake: {message}")]
    Handshake { message: String },
    #[error("discovery: {message}")]
    Discovery { message: String },
    #[error("transport: {message}")]
    Transport { message: String },
}

impl AdapterError {
    /// The stage-specific message without the stage prefix.
    pub fn message(&self) -> &str {
        match self {
            Self::Spawn { message }
            | Self::Handshake { message }
            | Self::Discovery { message }
            | Self::Transport { message } => message,
        }
    }
}

pub trait PluginAdapter: Send + Sync + 'static {
    fn handshake(&mut self) -> BoxFuture<'_, Result<AdapterHandshake, AdapterError>>;
    fn discover(&mut self) -> BoxFuture<'_, Result<Vec<AdapterTool>, AdapterError>>;
    fn call<'a>(
        &'a self,
        original_name: &'a str,
        args: JsonString,
        ctx: &'a ToolContext,
    ) -> BoxFuture<'a, Result<ToolOutput, ToolError>>;
    fn shutdown(&mut self) -> BoxFuture<'_, ()>;
}
