//! MCP Streamable HTTP adapter (`mcp.md` §3.3, §7.3, §8).
//!
//! POST-only and request-scoped: every JSON-RPC message is one POST, answered
//! either by a JSON body or by an SSE stream whose final event carries the
//! response id. Calls are independent; only protocol-version drift poisons the
//! adapter.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use asterpath_protocol::{JsonString, ToolContext, ToolError, ToolOutput};
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;

use crate::adapter::{AdapterError, AdapterHandshake, AdapterTool, BoxFuture, PluginAdapter};
use crate::mcp::{self, RpcClient, RpcFailure};

const PROTOCOL_VERSION_HEADER: &str = "mcp-protocol-version";
const METHOD_HEADER: &str = "mcp-method";
const NAME_HEADER: &str = "mcp-name";

pub(crate) struct HttpAdapter {
    client: reqwest::Client,
    url: reqwest::Url,
    /// Manifest headers; applied after the protocol defaults.
    static_headers: HeaderMap,
    protocol_version: Mutex<String>,
    next_id: AtomicU64,
    dead: AtomicBool,
    startup: Duration,
}

impl HttpAdapter {
    pub(crate) fn new(
        url: &str,
        headers: &BTreeMap<String, String>,
        candidate_version: &str,
        startup: Duration,
    ) -> Result<Self, AdapterError> {
        // The URL is configuration, not an error payload: never include it.
        let url = reqwest::Url::parse(url).map_err(|_| AdapterError::Spawn {
            message: "invalid MCP HTTP endpoint URL".to_owned(),
        })?;
        let mut static_headers = HeaderMap::new();
        for (name, value) in headers {
            let header =
                HeaderName::from_bytes(name.as_bytes()).map_err(|_| AdapterError::Spawn {
                    message: "invalid MCP HTTP header name in manifest".to_owned(),
                })?;
            let value = HeaderValue::from_str(value).map_err(|_| AdapterError::Spawn {
                message: format!("invalid value for MCP HTTP header {name}"),
            })?;
            static_headers.insert(header, value);
        }
        let client = reqwest::Client::builder()
            // A redirect could forward manifest credentials to another host.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| AdapterError::Spawn {
                message: "failed to build the MCP HTTP client".to_owned(),
            })?;
        Ok(Self {
            client,
            url,
            static_headers,
            protocol_version: Mutex::new(candidate_version.to_owned()),
            next_id: AtomicU64::new(0),
            dead: AtomicBool::new(false),
            startup,
        })
    }

    /// Version drift means the negotiated protocol no longer matches; later
    /// calls fail immediately instead of guessing. `-32020` (transport error)
    /// fails only the current call (`mcp.md` §7.3).
    fn note_protocol_error(&self, code: i64) {
        if matches!(code, -32022..=-32021) {
            self.dead.store(true, Ordering::SeqCst);
        }
    }

    async fn post(
        &self,
        method: &str,
        params: &str,
        tool_name: Option<&str>,
        timeout: Option<Duration>,
    ) -> Result<Value, RpcFailure> {
        if self.dead.load(Ordering::SeqCst) {
            return Err(RpcFailure::Transport(
                "MCP HTTP adapter is closed".to_owned(),
            ));
        }
        let id = self.next_id.fetch_add(1, Ordering::SeqCst) + 1;
        let body = format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":{id},\"method\":{},\"params\":{params}}}",
            serde_json::to_string(method).expect("method is serializable"),
        );
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/json, text/event-stream"),
        );
        headers.insert(
            HeaderName::from_static(PROTOCOL_VERSION_HEADER),
            HeaderValue::from_str(&self.protocol_version()).map_err(|_| {
                RpcFailure::Transport("MCP protocol version is not a valid header".to_owned())
            })?,
        );
        headers.insert(
            HeaderName::from_static(METHOD_HEADER),
            HeaderValue::from_str(method).map_err(|_| {
                RpcFailure::Transport("MCP method is not a valid header".to_owned())
            })?,
        );
        if let Some(name) = tool_name {
            headers.insert(
                HeaderName::from_static(NAME_HEADER),
                HeaderValue::from_str(name).map_err(|_| {
                    RpcFailure::Transport("MCP tool name is not a valid header".to_owned())
                })?,
            );
        }
        // Manifest headers win over the defaults (for example `Accept`).
        for (name, value) in &self.static_headers {
            headers.insert(name.clone(), value.clone());
        }

        let request = async {
            let response = self
                .client
                .post(self.url.clone())
                .headers(headers)
                .body(body)
                .send()
                .await
                .map_err(|error| RpcFailure::Transport(reqwest_message(&error)))?;
            if !response.status().is_success() {
                return Err(RpcFailure::Transport(format!(
                    "MCP HTTP request failed with status {}",
                    response.status().as_u16()
                )));
            }
            let content_type = response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("");
            if content_type.contains("text/event-stream") {
                read_sse(response, id).await
            } else if content_type.contains("application/json") {
                read_json(response).await
            } else {
                Err(RpcFailure::Transport(
                    "MCP HTTP response has an unsupported content type".to_owned(),
                ))
            }
        };
        match timeout {
            Some(timeout) => tokio::time::timeout(timeout, request)
                .await
                .unwrap_or_else(|_| {
                    Err(RpcFailure::Transport(
                        "MCP HTTP request timed out".to_owned(),
                    ))
                }),
            None => request.await,
        }
    }
}

impl RpcClient for HttpAdapter {
    fn request<'a>(
        &'a self,
        method: &'a str,
        params: &'a str,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Value, RpcFailure>> {
        Box::pin(self.post(method, params, None, Some(timeout)))
    }

    fn protocol_version(&self) -> String {
        self.protocol_version
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn set_protocol_version(&self, version: &str) {
        *self
            .protocol_version
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = version.to_owned();
    }
}

/// Maps a reqwest error to a static message; `Display` includes the URL.
fn reqwest_message(error: &reqwest::Error) -> String {
    if error.is_timeout() {
        "MCP HTTP request timed out".to_owned()
    } else if error.is_connect() {
        "MCP HTTP connection failed".to_owned()
    } else if error.is_decode() {
        "MCP HTTP response could not be decoded".to_owned()
    } else {
        "MCP HTTP request failed".to_owned()
    }
}

async fn read_json(response: reqwest::Response) -> Result<Value, RpcFailure> {
    let bytes = response
        .bytes()
        .await
        .map_err(|error| RpcFailure::Transport(reqwest_message(&error)))?;
    serde_json::from_slice(&bytes)
        .map_err(|_| RpcFailure::Transport("MCP HTTP response was not valid JSON".to_owned()))
}

/// Reads events until the response with `expected_id` arrives. Notifications
/// and unrelated messages are ignored; EOF before the response is a transport
/// failure for this call only.
async fn read_sse(mut response: reqwest::Response, expected_id: u64) -> Result<Value, RpcFailure> {
    let mut buffer: Vec<u8> = Vec::new();
    let mut data = String::new();
    loop {
        let chunk = response
            .chunk()
            .await
            .map_err(|error| RpcFailure::Transport(reqwest_message(&error)))?;
        let Some(chunk) = chunk else {
            return Err(RpcFailure::Transport(
                "MCP SSE stream ended before the response".to_owned(),
            ));
        };
        buffer.extend_from_slice(&chunk);
        while let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
            let line = buffer.drain(..=newline).collect::<Vec<u8>>();
            let line = String::from_utf8_lossy(&line);
            let line = line.trim_end_matches(['\r', '\n']);
            if line.is_empty() {
                if !data.is_empty() {
                    let message = parse_sse_message(&data)?;
                    data.clear();
                    if message.get("id").and_then(Value::as_u64) == Some(expected_id) {
                        return Ok(message);
                    }
                }
                continue;
            }
            if line.starts_with(':') {
                continue;
            }
            if let Some(rest) = line.strip_prefix("data:") {
                let rest = rest.strip_prefix(' ').unwrap_or(rest);
                if !data.is_empty() {
                    data.push('\n');
                }
                data.push_str(rest);
            }
        }
    }
}

fn parse_sse_message(data: &str) -> Result<Value, RpcFailure> {
    serde_json::from_str(data)
        .map_err(|_| RpcFailure::Transport("malformed JSON in MCP SSE stream".to_owned()))
}

async fn call_impl(
    adapter: &HttpAdapter,
    original_name: &str,
    args: JsonString,
    ctx: &ToolContext,
) -> Result<ToolOutput, ToolError> {
    mcp::parse_call_args(&args)?;
    // Embed the caller's raw argument text so replay sees the same bytes.
    let params = format!(
        "{{\"name\":{},\"arguments\":{},\"_meta\":{}}}",
        serde_json::to_string(original_name).expect("name is serializable"),
        args.as_str(),
        mcp::meta_json(&adapter.protocol_version()),
    );
    tokio::select! {
        received = adapter.post("tools/call", &params, Some(original_name), None) => {
            mcp::map_call_response(received, |code| adapter.note_protocol_error(code))
        }
        // Cancellation drops the request/SSE future; no notifications/cancelled.
        _ = ctx.cancel.cancelled() => Err(ToolError::Cancelled {
            message: "tool call cancelled".to_owned(),
        }),
    }
}

impl PluginAdapter for HttpAdapter {
    fn handshake(&mut self) -> BoxFuture<'_, Result<AdapterHandshake, AdapterError>> {
        Box::pin(mcp::handshake(self, self.startup))
    }

    fn discover(&mut self) -> BoxFuture<'_, Result<Vec<AdapterTool>, AdapterError>> {
        Box::pin(mcp::discover(self, self.startup))
    }

    fn call<'a>(
        &'a self,
        original_name: &'a str,
        args: JsonString,
        ctx: &'a ToolContext,
    ) -> BoxFuture<'a, Result<ToolOutput, ToolError>> {
        Box::pin(call_impl(self, original_name, args, ctx))
    }

    fn shutdown(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.dead.store(true, Ordering::SeqCst);
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use super::HttpAdapter;

    fn adapter() -> HttpAdapter {
        HttpAdapter::new(
            "http://127.0.0.1:1/mcp",
            &BTreeMap::new(),
            "2026-07-28",
            Duration::from_secs(1),
        )
        .expect("adapter")
    }

    #[tokio::test]
    async fn version_drift_marks_only_the_adapter_dead() {
        let adapter = adapter();
        adapter.note_protocol_error(-32603);
        assert!(!adapter.dead.load(std::sync::atomic::Ordering::SeqCst));
        // -32020 fails only the call that received it.
        adapter.note_protocol_error(-32020);
        assert!(!adapter.dead.load(std::sync::atomic::Ordering::SeqCst));

        adapter.note_protocol_error(-32021);
        let error = adapter
            .post("tools/call", "{}", None, None)
            .await
            .unwrap_err();
        assert!(
            matches!(&error, crate::mcp::RpcFailure::Transport(message) if message.contains("closed")),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn no_common_version_also_marks_the_adapter_dead() {
        let adapter = adapter();
        adapter.note_protocol_error(-32022);
        assert!(adapter.dead.load(std::sync::atomic::Ordering::SeqCst));
    }
}
