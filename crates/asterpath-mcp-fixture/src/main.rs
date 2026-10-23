//! Scriptable MCP test server for `2026-07-28` (stdio and Streamable HTTP).
//!
//! The CLI contract is documented in `README.md`; keep the two in sync.

use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream, tcp::OwnedWriteHalf};

const PROTOCOL_VERSION: &str = "2026-07-28";
const LISTEN_PREFIX: &str = "listening on ";
const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

const USAGE: &str = "\
asterpath-mcp-fixture — scriptable MCP server for protocol 2026-07-28

Usage:
  asterpath-mcp-fixture --stdio [OPTIONS]
  asterpath-mcp-fixture --http <ADDR> [OPTIONS]

Transport:
  --stdio                    one JSON-RPC message per stdin line; stdout carries only messages
  --http <ADDR>              POST-only HTTP; use 127.0.0.1:0 for an ephemeral port
                             (the bound address is printed as: listening on <ADDR>)

Options:
  --page-size <N>            tools pushed per tools/list page (default 2)
  --tool-set <SET>           tools exposed: default | collision | oversized
                             (collision: a.b + a_b; oversized: one over-long name)
  --supported-versions <V>   comma-separated versions advertised by server/discover
                             (default 2026-07-28)
  --force-version-mismatch   server/discover always returns -32022 with data.supported
  --fault-repeat-cursor      tools/list always repeats the current cursor
  --fault-endless-pages      tools/list always returns a fresh nextCursor
  --fault-error-code <CODE>  every tools/call returns this JSON-RPC error code
  --fault-crash-after <N>    answer N tools/call requests; the next one exits the process
  --fault-delay-ms <MS>      sleep before answering every tools/call
  --fault-is-error           every tools/call returns result.isError = true
  --fault-input-required     every tools/call returns result.resultType = input_required
  --fault-oversized <BYTES>  every tools/call returns one text block of this size
  --fault-sse-interrupt      SSE responses send the notification, then close early
  -h, --help                 print this help

MCP methods: server/discover, tools/list, tools/call, notifications/cancelled.
Tools: echo (readOnlyHint), mutate (mutating), content (all MCP block types).

Fault precedence per tools/call: crash-after, delay, error-code, is-error,
input-required, oversized, normal dispatch. HTTP header mismatches take effect
before dispatch.
";

enum Mode {
    Stdio,
    Http(String),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ToolSet {
    Default,
    Collision,
    Oversized,
}

struct Config {
    page_size: usize,
    tool_set: ToolSet,
    supported_versions: Vec<String>,
    force_version_mismatch: bool,
    fault_repeat_cursor: bool,
    fault_endless_pages: bool,
    fault_error_code: Option<i64>,
    fault_crash_after: Option<u64>,
    fault_delay_ms: Option<u64>,
    fault_is_error: bool,
    fault_input_required: bool,
    fault_oversized: Option<usize>,
    fault_sse_interrupt: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            page_size: 2,
            tool_set: ToolSet::Default,
            supported_versions: vec![PROTOCOL_VERSION.to_string()],
            force_version_mismatch: false,
            fault_repeat_cursor: false,
            fault_endless_pages: false,
            fault_error_code: None,
            fault_crash_after: None,
            fault_delay_ms: None,
            fault_is_error: false,
            fault_input_required: false,
            fault_oversized: None,
            fault_sse_interrupt: false,
        }
    }
}

struct State {
    config: Config,
    calls: AtomicU64,
    mutations: AtomicU64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    let (mode, config) = match parse_args(args) {
        Ok(parsed) => parsed,
        Err(message) => {
            eprintln!("{message}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let state = Arc::new(State {
        config,
        calls: AtomicU64::new(0),
        mutations: AtomicU64::new(0),
    });
    let outcome = match mode {
        Mode::Stdio => run_stdio(state).await,
        Mode::Http(addr) => run_http(&addr, state).await,
    };
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("asterpath-mcp-fixture I/O error: {err}");
            ExitCode::FAILURE
        }
    }
}

fn parse_args(args: Vec<String>) -> Result<(Mode, Config), String> {
    let mut args = args.into_iter();
    let mut mode = None;
    let mut config = Config::default();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--stdio" => set_mode(&mut mode, Mode::Stdio)?,
            "--http" => {
                let addr = next_value(&mut args, "--http")?;
                set_mode(&mut mode, Mode::Http(addr))?;
            }
            "--page-size" => config.page_size = next_value(&mut args, "--page-size")?,
            "--tool-set" => {
                config.tool_set = match next_value::<String>(&mut args, "--tool-set")?.as_str() {
                    "default" => ToolSet::Default,
                    "collision" => ToolSet::Collision,
                    "oversized" => ToolSet::Oversized,
                    other => return Err(format!("unknown --tool-set value: {other}")),
                };
            }
            "--supported-versions" => {
                let value = next_value::<String>(&mut args, "--supported-versions")?;
                let versions: Vec<String> = value
                    .split(',')
                    .map(str::trim)
                    .filter(|version| !version.is_empty())
                    .map(str::to_string)
                    .collect();
                if versions.is_empty() {
                    return Err("--supported-versions needs at least one version".into());
                }
                config.supported_versions = versions;
            }
            "--force-version-mismatch" => config.force_version_mismatch = true,
            "--fault-repeat-cursor" => config.fault_repeat_cursor = true,
            "--fault-endless-pages" => config.fault_endless_pages = true,
            "--fault-error-code" => {
                config.fault_error_code = Some(next_value(&mut args, "--fault-error-code")?);
            }
            "--fault-crash-after" => {
                config.fault_crash_after = Some(next_value(&mut args, "--fault-crash-after")?);
            }
            "--fault-delay-ms" => {
                config.fault_delay_ms = Some(next_value(&mut args, "--fault-delay-ms")?);
            }
            "--fault-is-error" => config.fault_is_error = true,
            "--fault-input-required" => config.fault_input_required = true,
            "--fault-oversized" => {
                config.fault_oversized = Some(next_value(&mut args, "--fault-oversized")?);
            }
            "--fault-sse-interrupt" => config.fault_sse_interrupt = true,
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    let mode = mode.ok_or("choose a transport: --stdio or --http <ADDR>")?;
    Ok((mode, config))
}

fn set_mode(slot: &mut Option<Mode>, mode: Mode) -> Result<(), String> {
    if slot.is_some() {
        return Err("choose exactly one transport: --stdio or --http <ADDR>".into());
    }
    *slot = Some(mode);
    Ok(())
}

fn next_value<T: std::str::FromStr>(
    args: &mut impl Iterator<Item = String>,
    flag: &str,
) -> Result<T, String> {
    let raw = args
        .next()
        .ok_or_else(|| format!("{flag} requires a value"))?;
    raw.parse()
        .map_err(|_| format!("{flag}: invalid value `{raw}`"))
}

// ------------------------------------------------------------------ MCP

async fn handle(state: &State, message: &Value) -> Option<Value> {
    let Some(method) = message.get("method").and_then(Value::as_str) else {
        let id = message.get("id").cloned()?;
        return Some(error(id, -32600, "not a JSON-RPC request", None));
    };
    // Notifications (including notifications/cancelled) are accepted and ignored.
    let id = message.get("id")?.clone();
    match method {
        "server/discover" => Some(discover(state, id, message)),
        "tools/list" => Some(list(state, id, message)),
        "tools/call" => Some(call(state, id, message).await),
        other => Some(error(
            id,
            -32601,
            &format!("method not found: {other}"),
            None,
        )),
    }
}

fn discover(state: &State, id: Value, message: &Value) -> Value {
    let requested = message
        .pointer("/params/_meta")
        .and_then(|meta| meta.get("io.modelcontextprotocol/protocolVersion"))
        .and_then(Value::as_str);
    let mismatch = state.config.force_version_mismatch
        || requested.is_some_and(|version| {
            !state
                .config
                .supported_versions
                .iter()
                .any(|supported| supported == version)
        });
    if mismatch {
        return error(
            id,
            -32022,
            "no common protocol version",
            Some(json!({"supported": state.config.supported_versions})),
        );
    }
    result(
        id,
        json!({
            "supportedVersions": state.config.supported_versions,
            "capabilities": {"tools": {"listChanged": false}},
            "serverInfo": {
                "name": "asterpath-mcp-fixture",
                "version": env!("CARGO_PKG_VERSION"),
            },
        }),
    )
}

fn list(state: &State, id: Value, message: &Value) -> Value {
    let tools = tools(&state.config);
    let start = match message.pointer("/params/cursor") {
        None | Some(Value::Null) => 0,
        Some(Value::String(cursor)) => match cursor.parse::<usize>() {
            Ok(index) => index,
            Err(_) => return error(id, -32602, "invalid cursor", None),
        },
        Some(_) => return error(id, -32602, "invalid cursor", None),
    };
    if state.config.fault_repeat_cursor {
        // Echo the current cursor (or "0" for the first page): discovery must
        // reject the repeat instead of looping.
        let next = match message.pointer("/params/cursor") {
            Some(Value::String(cursor)) => cursor.clone(),
            _ => "0".to_owned(),
        };
        return result(id, json!({"tools": &tools[..1], "nextCursor": next}));
    }
    if state.config.fault_endless_pages {
        // One tool per page with a fresh cursor: discovery hits the 100-page cap.
        return result(
            id,
            json!({"tools": &tools[..1], "nextCursor": (start + 1).to_string()}),
        );
    }
    if start > tools.len() {
        return error(id, -32602, "invalid cursor", None);
    }
    let end = (start + state.config.page_size.max(1)).min(tools.len());
    let mut page = json!({"tools": &tools[start..end]});
    if end < tools.len() {
        page["nextCursor"] = json!(end.to_string());
    }
    result(id, page)
}

fn tools(config: &Config) -> Vec<Value> {
    match config.tool_set {
        ToolSet::Default => vec![
            json!({
                "name": "echo",
                "description": "Echo text back (read-only)",
                "inputSchema": {
                    "type": "object",
                    "properties": {"text": {"type": "string"}},
                    "additionalProperties": false,
                },
                "annotations": {"readOnlyHint": true},
            }),
            json!({
                "name": "mutate",
                "description": "Increment a counter (mutating)",
                "inputSchema": {"type": "object", "properties": {}},
            }),
            json!({
                "name": "content",
                "description": "Emit every MCP content block type (read-only)",
                "inputSchema": {"type": "object", "properties": {}},
                "annotations": {"readOnlyHint": true},
            }),
        ],
        // Both sanitize to `a_b`: composition must fail atomically.
        ToolSet::Collision => vec![simple_tool("a.b"), simple_tool("a_b")],
        ToolSet::Oversized => vec![simple_tool(&"x".repeat(70))],
    }
}

fn simple_tool(name: &str) -> Value {
    json!({
        "name": name,
        "description": "",
        "inputSchema": {"type": "object", "properties": {}},
    })
}

async fn call(state: &State, id: Value, message: &Value) -> Value {
    let call_number = state.calls.fetch_add(1, Ordering::SeqCst) + 1;
    if let Some(limit) = state.config.fault_crash_after
        && call_number > limit
    {
        std::process::exit(1);
    }
    if let Some(delay_ms) = state.config.fault_delay_ms {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
    if let Some(code) = state.config.fault_error_code {
        return error(id, code, "scripted JSON-RPC error", None);
    }
    if state.config.fault_is_error {
        return result(
            id,
            json!({
                "isError": true,
                "content": [
                    {"type": "text", "text": "scripted"},
                    {"type": "text", "text": "failure"},
                ],
            }),
        );
    }
    if state.config.fault_input_required {
        return result(
            id,
            json!({
                "resultType": "input_required",
                "content": [{"type": "text", "text": "input required"}],
            }),
        );
    }
    if let Some(size) = state.config.fault_oversized {
        return result(
            id,
            json!({"content": [{"type": "text", "text": "x".repeat(size)}]}),
        );
    }

    let params = message.get("params").cloned().unwrap_or_else(|| json!({}));
    let Some(name) = params.get("name").and_then(Value::as_str) else {
        return error(id, -32602, "tools/call requires params.name", None);
    };
    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if !arguments.is_object() {
        return error(id, -32602, "tool arguments must be a JSON object", None);
    }
    match name {
        "echo" => {
            let text = arguments
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or("echo");
            result(id, json!({"content": [{"type": "text", "text": text}]}))
        }
        "mutate" => {
            let count = state.mutations.fetch_add(1, Ordering::SeqCst) + 1;
            result(
                id,
                json!({
                    "content": [{"type": "text", "text": format!("mutation {count}")}],
                    "structuredContent": {"count": count},
                }),
            )
        }
        "content" => result(id, content_result()),
        other => error(id, -32602, &format!("unknown tool: {other}"), None),
    }
}

fn content_result() -> Value {
    json!({
        "content": [
            {"type": "text", "text": "hello"},
            {"type": "image", "data": "aGVsbG8=", "mimeType": "image/png"},
            {"type": "audio", "data": "aGVsbG8=", "mimeType": "audio/wav"},
            {
                "type": "resource_link",
                "uri": "file:///tmp/fixture.txt",
                "name": "fixture.txt",
                "mimeType": "text/plain",
            },
            {
                "type": "resource",
                "resource": {
                    "uri": "file:///tmp/fixture.txt",
                    "mimeType": "text/plain",
                    "text": "resource text",
                },
            },
        ],
        "structuredContent": {"kind": "content"},
    })
}

fn result(id: Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn error(id: Value, code: i64, message: &str, data: Option<Value>) -> Value {
    let mut body = json!({"code": code, "message": message});
    if let Some(data) = data {
        body["data"] = data;
    }
    json!({"jsonrpc": "2.0", "id": id, "error": body})
}

// ---------------------------------------------------------------- stdio

async fn run_stdio(state: Arc<State>) -> std::io::Result<()> {
    // Blocking stdio is fine: the stdio transport serves a single client and the
    // only await points are the scripted delays.
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let response = match serde_json::from_str::<Value>(&line) {
            Ok(message) => handle(&state, &message).await,
            Err(err) => Some(error(
                Value::Null,
                -32700,
                &format!("parse error: {err}"),
                None,
            )),
        };
        if let Some(response) = response {
            serde_json::to_writer(&mut stdout, &response).expect("response is serializable");
            stdout.write_all(b"\n")?;
            stdout.flush()?;
        }
    }
    Ok(())
}

// ----------------------------------------------------------------- HTTP

async fn run_http(addr: &str, state: Arc<State>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    {
        let mut stdout = std::io::stdout().lock();
        writeln!(stdout, "{LISTEN_PREFIX}{local}")?;
        stdout.flush()?;
    }
    loop {
        let (stream, _) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let _ = serve_connection(stream, state).await;
        });
    }
}

async fn serve_connection(stream: TcpStream, state: Arc<State>) -> std::io::Result<()> {
    let (read_half, mut writer) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let mut request_line = String::new();
    if reader.read_line(&mut request_line).await? == 0 {
        return Ok(());
    }
    let mut headers = HashMap::new();
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            return Ok(());
        }
        if line == "\r\n" || line == "\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }

    let verb = request_line.split_whitespace().next().unwrap_or("");
    if verb != "POST" {
        return write_response(
            &mut writer,
            405,
            "text/plain",
            b"method not allowed",
            &[("Allow", "POST")],
        )
        .await;
    }
    if !headers
        .get("content-type")
        .is_some_and(|value| value.to_ascii_lowercase().contains("application/json"))
    {
        return write_response(
            &mut writer,
            415,
            "text/plain",
            b"content type must be application/json",
            &[],
        )
        .await;
    }
    let Some(length) = headers
        .get("content-length")
        .and_then(|value| value.parse::<usize>().ok())
    else {
        return write_response(
            &mut writer,
            400,
            "text/plain",
            b"content-length required",
            &[],
        )
        .await;
    };
    if length > MAX_BODY_BYTES {
        return write_response(&mut writer, 413, "text/plain", b"payload too large", &[]).await;
    }
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body).await?;

    let message: Value = match serde_json::from_slice(&body) {
        Ok(message) => message,
        Err(err) => {
            return write_json(
                &mut writer,
                &error(Value::Null, -32700, &format!("parse error: {err}"), None),
            )
            .await;
        }
    };

    let body_method = message.get("method").and_then(Value::as_str).unwrap_or("");
    if let Some(header_method) = headers.get("mcp-method")
        && header_method != body_method
    {
        return write_response(
            &mut writer,
            400,
            "text/plain",
            b"Mcp-Method does not match the body method",
            &[],
        )
        .await;
    }
    if body_method == "tools/call"
        && let Some(header_name) = headers.get("mcp-name")
    {
        let body_name = message
            .pointer("/params/name")
            .and_then(Value::as_str)
            .unwrap_or("");
        if header_name != body_name {
            return write_response(
                &mut writer,
                400,
                "text/plain",
                b"Mcp-Name does not match params.name",
                &[],
            )
            .await;
        }
    }
    if let Some(version) = headers.get("mcp-protocol-version")
        && !state
            .config
            .supported_versions
            .iter()
            .any(|supported| supported == version)
    {
        let id = message.get("id").cloned().unwrap_or(Value::Null);
        return write_json(
            &mut writer,
            &error(
                id,
                -32022,
                "no common protocol version",
                Some(json!({"supported": state.config.supported_versions})),
            ),
        )
        .await;
    }

    let Some(response) = handle(&state, &message).await else {
        return write_response(&mut writer, 202, "text/plain", b"", &[]).await;
    };
    let wants_sse = headers.get("accept").is_some_and(|accept| {
        let accept = accept.to_ascii_lowercase();
        accept.contains("text/event-stream") && !accept.contains("application/json")
    });
    if wants_sse {
        write_sse(&mut writer, &state.config, &response).await
    } else {
        write_json(&mut writer, &response).await
    }
}

async fn write_json(writer: &mut OwnedWriteHalf, response: &Value) -> std::io::Result<()> {
    let body = serde_json::to_vec(response).expect("response is serializable");
    write_response(writer, 200, "application/json", &body, &[]).await
}

async fn write_response(
    writer: &mut OwnedWriteHalf,
    status: u16,
    content_type: &str,
    body: &[u8],
    extra_headers: &[(&str, &str)],
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        202 => "Accepted",
        400 => "Bad Request",
        405 => "Method Not Allowed",
        413 => "Payload Too Large",
        415 => "Unsupported Media Type",
        _ => "Error",
    };
    let mut head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in extra_headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    writer.write_all(head.as_bytes()).await?;
    writer.write_all(body).await?;
    writer.shutdown().await
}

async fn write_sse(
    writer: &mut OwnedWriteHalf,
    config: &Config,
    response: &Value,
) -> std::io::Result<()> {
    // SSE is EOF-delimited: no Content-Length, Connection: close.
    writer
        .write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n",
        )
        .await?;
    write_sse_event(
        writer,
        &json!({"jsonrpc": "2.0", "method": "notifications/progress", "params": {"progress": 0}}),
    )
    .await?;
    if !config.fault_sse_interrupt {
        write_sse_event(writer, response).await?;
    }
    writer.shutdown().await
}

async fn write_sse_event(writer: &mut OwnedWriteHalf, message: &Value) -> std::io::Result<()> {
    let data = serde_json::to_string(message).expect("message is serializable");
    writer
        .write_all(format!("event: message\ndata: {data}\n\n").as_bytes())
        .await?;
    writer.flush().await
}
