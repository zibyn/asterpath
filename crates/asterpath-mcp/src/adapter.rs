//! The crate-private transport adapter seam (`mcp.md` §4): stdio and HTTP.
//!
//! An adapter exposes raw tool names only. Composition into
//! `<server_id>__<sanitized>` belongs to [`crate::McpServers`].

use std::future::Future;
use std::pin::Pin;

use asterpath_protocol::{
    JsonSchema, JsonString, Parallelism, ReplaySafety, SideEffect, ToolContext, ToolError,
    ToolOutput,
};

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AdapterHandshake {
    pub(crate) protocol_version: String,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AdapterTool {
    pub(crate) original_name: String,
    pub(crate) description: String,
    pub(crate) schema: JsonSchema,
    pub(crate) replay: ReplaySafety,
    pub(crate) side_effect: SideEffect,
    pub(crate) parallelism: Parallelism,
}

/// Adapter failure, classified by the load stage it belongs to.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum AdapterError {
    #[error("spawn: {message}")]
    Spawn { message: String },
    #[error("handshake: {message}")]
    Handshake { message: String },
    #[error("discovery: {message}")]
    Discovery { message: String },
}

impl AdapterError {
    /// The stage-specific message without the stage prefix.
    pub(crate) fn message(&self) -> &str {
        match self {
            Self::Spawn { message } | Self::Handshake { message } | Self::Discovery { message } => {
                message
            }
        }
    }
}

pub(crate) trait McpAdapter: Send + Sync + 'static {
    fn handshake(&mut self) -> BoxFuture<'_, Result<AdapterHandshake, AdapterError>>;
    fn discover(&mut self) -> BoxFuture<'_, Result<Vec<AdapterTool>, AdapterError>>;
    fn call<'a>(
        &'a self,
        original_name: &'a str,
        args: JsonString,
        ctx: &'a ToolContext,
    ) -> BoxFuture<'a, Result<ToolOutput, ToolError>>;
    /// Graceful close. Takes `&self`: loaded tools share the adapter.
    fn shutdown(&self) -> BoxFuture<'_, ()>;
}
