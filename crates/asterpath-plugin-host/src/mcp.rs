//! Shared MCP `2026-07-28` mechanics for the stdio and HTTP adapters.
//!
//! Transport-specific framing lives in `stdio.rs` and `http.rs`; request
//! `_meta`, handshake, discovery and result/error mapping are identical.

use std::collections::HashSet;
use std::time::Duration;

use asterpath_protocol::{ImageSource, JsonString, Part, ToolError, ToolOutput};
use serde_json::Value;

use crate::adapter::{AdapterError, AdapterHandshake, AdapterTool, BoxFuture};

pub(crate) const ADAPTER_PROTOCOL_VERSIONS: &[&str] = &["2026-07-28"];
const MAX_LIST_PAGES: usize = 100;

/// A failed request: either transport-level or a JSON-RPC error response.
#[derive(Clone, Debug)]
pub(crate) enum RpcFailure {
    Transport(String),
    Rpc {
        code: i64,
        message: String,
        data: Option<Value>,
    },
}

impl RpcFailure {
    pub(crate) fn message(&self) -> String {
        match self {
            Self::Transport(message) => message.clone(),
            Self::Rpc { code, message, .. } => format!("JSON-RPC error {code}: {message}"),
        }
    }
}

/// The request side of a transport. Each adapter supplies its own framing.
pub(crate) trait RpcClient {
    fn request<'a>(
        &'a self,
        method: &'a str,
        params: &'a str,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Value, RpcFailure>>;
    fn protocol_version(&self) -> String;
    fn set_protocol_version(&self, version: &str);
}

pub(crate) fn meta_json(version: &str) -> String {
    format!(
        "{{\"io.modelcontextprotocol/protocolVersion\":{},\
          \"io.modelcontextprotocol/clientCapabilities\":{{}},\
          \"io.modelcontextprotocol/clientInfo\":{{\"name\":\"asterpath\",\"version\":{}}}}}",
        serde_json::to_string(version).expect("version is serializable"),
        serde_json::to_string(env!("CARGO_PKG_VERSION")).expect("version is serializable"),
    )
}

fn discover_params(client: &(impl RpcClient + ?Sized)) -> String {
    format!("{{\"_meta\":{}}}", meta_json(&client.protocol_version()))
}

pub(crate) async fn handshake(
    client: &(impl RpcClient + ?Sized),
    startup: Duration,
) -> Result<AdapterHandshake, AdapterError> {
    let first = client
        .request("server/discover", &discover_params(client), startup)
        .await;
    let response = match first {
        Ok(response) => response,
        Err(RpcFailure::Rpc {
            code: -32022, data, ..
        }) => {
            let supported = data
                .as_ref()
                .and_then(|data| data.get("supported"))
                .and_then(Value::as_array);
            let common = supported.and_then(|supported| {
                supported
                    .iter()
                    .filter_map(Value::as_str)
                    .find(|version| ADAPTER_PROTOCOL_VERSIONS.contains(version))
            });
            let Some(common) = common else {
                return Err(handshake_error("no common protocol version"));
            };
            client.set_protocol_version(common);
            client
                .request("server/discover", &discover_params(client), startup)
                .await
                .map_err(|error| AdapterError::Handshake {
                    message: error.message(),
                })?
        }
        Err(error) => {
            return Err(AdapterError::Handshake {
                message: error.message(),
            });
        }
    };
    let result = split_response(response).map_err(|error| AdapterError::Handshake {
        message: error.message(),
    })?;
    let version = client.protocol_version();
    let Some(supported) = result.get("supportedVersions").and_then(Value::as_array) else {
        return Err(handshake_error(
            "server/discover did not return supportedVersions",
        ));
    };
    if !supported
        .iter()
        .any(|candidate| candidate.as_str() == Some(version.as_str()))
    {
        return Err(handshake_error(format!(
            "server does not support protocol version {version}"
        )));
    }
    if result
        .pointer("/capabilities/tools")
        .is_none_or(Value::is_null)
    {
        return Err(handshake_error(
            "server does not declare the tools capability",
        ));
    }
    Ok(AdapterHandshake {
        protocol_version: version,
    })
}

fn handshake_error(message: impl Into<String>) -> AdapterError {
    AdapterError::Handshake {
        message: message.into(),
    }
}

fn discovery_error(message: impl Into<String>) -> AdapterError {
    AdapterError::Discovery {
        message: message.into(),
    }
}

pub(crate) async fn discover(
    client: &(impl RpcClient + ?Sized),
    startup: Duration,
) -> Result<Vec<AdapterTool>, AdapterError> {
    let mut cursor: Option<String> = None;
    let mut seen = HashSet::new();
    let mut tools = Vec::new();
    for _ in 0..MAX_LIST_PAGES {
        let meta = meta_json(&client.protocol_version());
        let params = match &cursor {
            Some(cursor) => format!(
                "{{\"cursor\":{},\"_meta\":{meta}}}",
                serde_json::to_string(cursor).expect("cursor is serializable"),
            ),
            None => format!("{{\"_meta\":{meta}}}"),
        };
        let response = client
            .request("tools/list", &params, startup)
            .await
            .map_err(|error| discovery_error(error.message()))?;
        let result = split_response(response).map_err(|error| discovery_error(error.message()))?;
        if let Some(result_type) = result.get("resultType")
            && result_type.as_str() != Some("complete")
        {
            return Err(discovery_error("tools/list resultType is not complete"));
        }
        let Some(page) = result.get("tools").and_then(Value::as_array) else {
            return Err(discovery_error("tools/list result has no tools array"));
        };
        for tool in page {
            tools.push(parse_tool(tool)?);
        }
        match result.get("nextCursor").and_then(Value::as_str) {
            None => return Ok(tools),
            Some(next) => {
                if !seen.insert(next.to_owned()) {
                    return Err(discovery_error("tools/list repeated a cursor"));
                }
                cursor = Some(next.to_owned());
            }
        }
    }
    Err(discovery_error("tools/list exceeded 100 pages"))
}

pub(crate) fn parse_tool(tool: &Value) -> Result<AdapterTool, AdapterError> {
    let Some(object) = tool.as_object() else {
        return Err(discovery_error("tools/list entry is not an object"));
    };
    let Some(original_name) = object.get("name").and_then(Value::as_str) else {
        return Err(discovery_error("tools/list entry has no name"));
    };
    let description = object
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let Some(schema) = object.get("inputSchema") else {
        return Err(discovery_error("tool has no inputSchema"));
    };
    if !schema.is_object() {
        return Err(discovery_error("inputSchema root must be an object"));
    }
    let read_only = object
        .get("annotations")
        .and_then(|annotations| annotations.get("readOnlyHint"))
        .and_then(Value::as_bool)
        == Some(true);
    Ok(AdapterTool {
        original_name: original_name.to_owned(),
        description,
        schema: schema.clone(),
        replay: if read_only {
            asterpath_protocol::ReplaySafety::Safe
        } else {
            asterpath_protocol::ReplaySafety::Unsafe
        },
        side_effect: if read_only {
            asterpath_protocol::SideEffect::ReadOnly
        } else {
            asterpath_protocol::SideEffect::Mutating
        },
        parallelism: asterpath_protocol::Parallelism::Parallel,
    })
}

pub(crate) fn split_response(response: Value) -> Result<Value, RpcFailure> {
    let Value::Object(mut object) = response else {
        return Err(RpcFailure::Transport(
            "MCP response is not a JSON object".to_owned(),
        ));
    };
    if let Some(error) = object.remove("error") {
        let code = error.get("code").and_then(Value::as_i64).unwrap_or(-32603);
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("JSON-RPC error")
            .to_owned();
        return Err(RpcFailure::Rpc {
            code,
            message,
            data: error.get("data").cloned(),
        });
    }
    object.remove("result").ok_or_else(|| {
        RpcFailure::Transport("MCP response has neither result nor error".to_owned())
    })
}

/// Validates call arguments without re-serializing them.
pub(crate) fn parse_call_args(args: &JsonString) -> Result<(), ToolError> {
    let parsed: Value =
        serde_json::from_str(args.as_str()).map_err(|_| ToolError::InvalidArgs {
            message: "tool arguments must be valid JSON".to_owned(),
        })?;
    if !parsed.is_object() {
        return Err(ToolError::InvalidArgs {
            message: "tool arguments must be a JSON object".to_owned(),
        });
    }
    Ok(())
}

/// Maps a call response. `on_protocol_error` lets each transport decide which
/// JSON-RPC codes poison the adapter.
pub(crate) fn map_call_response(
    received: Result<Value, RpcFailure>,
    on_protocol_error: impl Fn(i64),
) -> Result<ToolOutput, ToolError> {
    let mapped = match received {
        Ok(response) => split_response(response),
        Err(failure) => Err(failure),
    };
    match mapped {
        Ok(result) => map_result(result),
        Err(RpcFailure::Transport(message)) => Err(ToolError::Transport { message }),
        Err(RpcFailure::Rpc { code, message, .. }) => {
            if code == -32602 {
                if message.to_ascii_lowercase().contains("unknown tool") {
                    return Err(ToolError::NotFound { message });
                }
                return Err(ToolError::InvalidArgs { message });
            }
            on_protocol_error(code);
            Err(ToolError::Transport {
                message: format!("JSON-RPC error {code}: {message}"),
            })
        }
    }
}

fn map_result(result: Value) -> Result<ToolOutput, ToolError> {
    if let Some(result_type) = result.get("resultType")
        && result_type.as_str() != Some("complete")
    {
        return Err(ToolError::Internal {
            message: "MCP resultType is not supported".to_owned(),
        });
    }
    if result.get("isError").and_then(Value::as_bool) == Some(true) {
        let texts: Vec<String> = result
            .get("content")
            .and_then(Value::as_array)
            .map(|blocks| {
                blocks
                    .iter()
                    .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
                    .filter_map(|block| block.get("text").and_then(Value::as_str))
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        let message = if texts.is_empty() {
            "MCP tool reported an error".to_owned()
        } else {
            // Fail-safe bound: an error must not bypass the pipeline's default
            // output limit just because it travels in `error.message`.
            truncate_utf8(
                texts.join("\n"),
                asterpath_protocol::ToolConfig::default().max_output_bytes,
            )
        };
        return Err(ToolError::Internal { message });
    }
    let mut content = Vec::new();
    if let Some(blocks) = result.get("content").and_then(Value::as_array) {
        for block in blocks {
            content.push(map_content(block));
        }
    }
    if let Some(structured) = result.get("structuredContent") {
        content.push(Part::Text {
            text: serde_json::to_string(structured).unwrap_or_default(),
        });
    }
    Ok(ToolOutput::new(content))
}

/// Cuts at the largest UTF-8 boundary not past `limit`.
fn truncate_utf8(mut text: String, limit: usize) -> String {
    if text.len() <= limit {
        return text;
    }
    let mut end = limit;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text
}

fn map_content(block: &Value) -> Part {
    let kind = block
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    match kind {
        "text" => Part::Text {
            text: block
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
        },
        "image" => {
            let media_type = block.get("mimeType").and_then(Value::as_str).unwrap_or("");
            match (
                block.get("data").and_then(Value::as_str),
                media_type.is_empty(),
            ) {
                (Some(data), false) => Part::Image {
                    media_type: media_type.to_owned(),
                    source: ImageSource::Base64 {
                        data: data.to_owned(),
                    },
                },
                _ => Part::Text {
                    text: format!("[image content: {media_type}]"),
                },
            }
        }
        "audio" => Part::Text {
            text: format!(
                "[audio content: {}]",
                block.get("mimeType").and_then(Value::as_str).unwrap_or("")
            ),
        },
        "resource_link" => Part::Text {
            text: format!(
                "[resource_link: name={} uri={} mime={}]",
                block.get("name").and_then(Value::as_str).unwrap_or(""),
                block.get("uri").and_then(Value::as_str).unwrap_or(""),
                block.get("mimeType").and_then(Value::as_str).unwrap_or(""),
            ),
        },
        "resource" => map_resource(block.get("resource")),
        other => Part::Text {
            text: format!("[unsupported MCP content: {other}]"),
        },
    }
}

fn map_resource(resource: Option<&Value>) -> Part {
    let Some(resource) = resource else {
        return Part::Text {
            text: "[unsupported MCP resource]".to_owned(),
        };
    };
    if let Some(text) = resource.get("text").and_then(Value::as_str) {
        return Part::Text {
            text: text.to_owned(),
        };
    }
    if let Some(blob) = resource.get("blob").and_then(Value::as_str) {
        let media_type = resource
            .get("mimeType")
            .and_then(Value::as_str)
            .unwrap_or("");
        if media_type.starts_with("image/") {
            return Part::Image {
                media_type: media_type.to_owned(),
                source: ImageSource::Base64 {
                    data: blob.to_owned(),
                },
            };
        }
        return Part::Text {
            text: format!("[resource blob: {media_type}]"),
        };
    }
    Part::Text {
        text: "[unsupported MCP resource]".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use asterpath_protocol::{ToolConfig, ToolError};

    use super::map_result;

    #[test]
    fn is_error_messages_are_bounded_and_split_on_utf8_boundaries() {
        let limit = ToolConfig::default().max_output_bytes;
        let huge = "x".repeat(limit * 2);
        let error = map_result(json!({
            "isError": true,
            "content": [{"type": "text", "text": huge}],
        }))
        .unwrap_err();
        let ToolError::Internal { message } = error else {
            panic!("expected Internal, got {error:?}");
        };
        assert_eq!(message.len(), limit);

        // A multi-byte character straddling the limit is dropped whole.
        let mut text = "a".repeat(limit - 1);
        text.push('é');
        let error = map_result(json!({
            "isError": true,
            "content": [{"type": "text", "text": text}],
        }))
        .unwrap_err();
        let ToolError::Internal { message } = error else {
            panic!("expected Internal, got {error:?}");
        };
        assert_eq!(message.len(), limit - 1);
        assert!(message.is_char_boundary(message.len()));
    }
}
