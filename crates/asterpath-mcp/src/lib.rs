//! MCP tool source (ADR-0027).
//!
//! The embedder supplies [`McpServerConfig`]s; [`McpServers::load`] validates
//! them, starts stdio servers through the protocol sandbox seam, performs
//! discovery and returns the tools named `<server_id>__<sanitized name>` for
//! the engine builder.

mod adapter;
mod config;
mod http;
mod mcp;
mod servers;
mod stdio;

pub use config::{
    DEFAULT_CALL, DEFAULT_STARTUP, McpServerConfig, McpTimeouts, McpTransport, ServerId,
};
pub use mcp::ADAPTER_PROTOCOL_VERSIONS;
pub use servers::{LoadStage, McpError, McpServers, ServerStatus};
