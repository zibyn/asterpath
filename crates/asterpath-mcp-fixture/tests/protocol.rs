//! Protocol-level tests: every test spawns the real fixture binary.
//!
//! Both transports are exercised (stdio and HTTP), plus every fault switch.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

const BIN: &str = env!("CARGO_BIN_EXE_asterpath-mcp-fixture");

fn protocol_meta(version: &str) -> Value {
    json!({
        "io.modelcontextprotocol/protocolVersion": version,
        "io.modelcontextprotocol/clientCapabilities": {},
        "io.modelcontextprotocol/clientInfo": {"name": "fixture-test", "version": "0.1.0"}
    })
}

fn with_meta(mut params: Value) -> Value {
    params
        .as_object_mut()
        .expect("params object")
        .insert("_meta".into(), protocol_meta("2026-07-28"));
    params
}

fn request(id: i64, method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

fn notification(method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "method": method, "params": params})
}

fn call_request(id: i64, name: &str, arguments: Value) -> Value {
    request(
        id,
        "tools/call",
        with_meta(json!({"name": name, "arguments": arguments})),
    )
}

// ---------------------------------------------------------------- stdio

struct StdioChild {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}

impl StdioChild {
    fn spawn(extra: &[&str]) -> Self {
        let mut child = Command::new(BIN)
            .arg("--stdio")
            .args(extra)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn fixture");
        let stdin = child.stdin.take().unwrap();
        let stdout = BufReader::new(child.stdout.take().unwrap());
        Self {
            child,
            stdin,
            stdout,
        }
    }

    fn send(&mut self, msg: &Value) {
        writeln!(self.stdin, "{msg}").unwrap();
        self.stdin.flush().unwrap();
    }

    fn recv(&mut self) -> Option<Value> {
        let mut line = String::new();
        let n = self.stdout.read_line(&mut line).unwrap();
        if n == 0 {
            return None;
        }
        Some(serde_json::from_str(line.trim()).expect("stdout carries only JSON-RPC messages"))
    }

    fn call(&mut self, msg: &Value) -> Value {
        self.send(msg);
        self.recv().expect("a response")
    }

    fn wait(&mut self) -> ExitStatus {
        self.child.wait().unwrap()
    }
}

impl Drop for StdioChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------- HTTP

struct HttpChild {
    child: Child,
    addr: String,
}

impl HttpChild {
    fn spawn(extra: &[&str]) -> Self {
        let mut child = Command::new(BIN)
            .args(["--http", "127.0.0.1:0"])
            .args(extra)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn fixture");
        let mut stdout = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        stdout.read_line(&mut line).expect("listen announcement");
        let addr = line
            .trim()
            .rsplit_once(' ')
            .expect("listening on <addr>")
            .1
            .to_string();
        Self { child, addr }
    }

    fn post(&self, method: &str, headers: &[(&str, &str)], body: &Value) -> HttpResponse {
        self.post_raw("POST", headers, &body.to_string(), method)
    }

    fn post_raw(
        &self,
        verb: &str,
        headers: &[(&str, &str)],
        body: &str,
        body_method: &str,
    ) -> HttpResponse {
        let mut stream = TcpStream::connect(&self.addr).expect("connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut extra = String::new();
        for (name, value) in headers {
            extra.push_str(&format!("{name}: {value}\r\n"));
        }
        let has_accept = headers
            .iter()
            .any(|(n, _)| n.eq_ignore_ascii_case("accept"));
        let accept = if has_accept {
            String::new()
        } else {
            "Accept: application/json\r\n".to_string()
        };
        let has_content_type = headers
            .iter()
            .any(|(n, _)| n.eq_ignore_ascii_case("content-type"));
        let content_type = if has_content_type {
            String::new()
        } else {
            "Content-Type: application/json\r\n".to_string()
        };
        let request = format!(
            "{verb} /mcp HTTP/1.1\r\nHost: {}\r\n{content_type}{accept}MCP-Protocol-Version: 2026-07-28\r\nMcp-Method: {body_method}\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            self.addr,
            body.len()
        );
        stream.write_all(request.as_bytes()).unwrap();
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).expect("read response");
        HttpResponse::parse(&raw)
    }
}

impl Drop for HttpChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct HttpResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: String,
}

impl HttpResponse {
    fn parse(raw: &[u8]) -> Self {
        let split = raw
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .expect("complete response head");
        let head = std::str::from_utf8(&raw[..split]).expect("utf-8 head");
        let body = String::from_utf8_lossy(&raw[split + 4..]).to_string();
        let mut lines = head.split("\r\n");
        let status = lines
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .parse()
            .unwrap();
        let headers = lines
            .filter_map(|line| line.split_once(':'))
            .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_string()))
            .collect();
        Self {
            status,
            headers,
            body,
        }
    }

    fn content_type(&self) -> &str {
        self.headers
            .get("content-type")
            .map(String::as_str)
            .unwrap_or("")
    }

    fn json(&self) -> Value {
        serde_json::from_str(&self.body).expect("JSON body")
    }
}

fn sse_messages(body: &str) -> Vec<Value> {
    body.split("\n\n")
        .flat_map(|block| {
            block
                .lines()
                .filter_map(|line| line.strip_prefix("data:").map(str::trim))
        })
        .filter(|data| !data.is_empty())
        .map(|data| serde_json::from_str(data).expect("SSE data is JSON"))
        .collect()
}

fn discover(id: i64) -> Value {
    request(
        id,
        "server/discover",
        with_meta(json!({"clientInfo": {"name": "not-asterpath"}})),
    )
}

fn list(id: i64, cursor: Option<&str>) -> Value {
    let params = match cursor {
        Some(cursor) => json!({"cursor": cursor}),
        None => json!({}),
    };
    request(id, "tools/list", with_meta(params))
}

// ---------------------------------------------------------------- tests

#[test]
fn stdio_discover_list_and_call() {
    let mut fixture = StdioChild::spawn(&["--page-size", "2"]);

    let discovered = fixture.call(&discover(1));
    let result = &discovered["result"];
    assert_eq!(result["supportedVersions"], json!(["2026-07-28"]));
    assert!(result["capabilities"]["tools"].is_object());

    let first_page = fixture.call(&list(2, None));
    let tools = first_page["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 2, "page size honoured");
    let cursor = first_page["result"]["nextCursor"]
        .as_str()
        .unwrap()
        .to_string();

    let second_page = fixture.call(&list(3, Some(&cursor)));
    let tools = second_page["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 1);
    assert!(second_page["result"].get("nextCursor").is_none());

    let mut names = Vec::new();
    for page in [first_page, second_page] {
        for tool in page["result"]["tools"].as_array().unwrap() {
            let read_only = tool["annotations"]["readOnlyHint"].as_bool();
            names.push((tool["name"].as_str().unwrap().to_string(), read_only));
            assert!(tool["inputSchema"].is_object());
        }
    }
    assert_eq!(
        names,
        vec![
            ("echo".to_string(), Some(true)),
            ("mutate".to_string(), None),
            ("content".to_string(), Some(true)),
        ]
    );

    let echoed = fixture.call(&call_request(4, "echo", json!({"text": "hello"})));
    assert_eq!(echoed["result"]["content"][0]["type"], "text");
    assert_eq!(echoed["result"]["content"][0]["text"], "hello");

    let mutated = fixture.call(&call_request(5, "mutate", json!({})));
    assert_eq!(mutated["result"]["structuredContent"]["count"], 1);

    let unknown = fixture.call(&call_request(6, "no-such-tool", json!({})));
    assert_eq!(unknown["error"]["code"], -32602);
}

#[test]
fn http_json_discover_list_and_call() {
    let fixture = HttpChild::spawn(&["--page-size", "10"]);

    let discovered = fixture.post("server/discover", &[], &discover(1));
    assert_eq!(discovered.status, 200);
    assert!(discovered.content_type().starts_with("application/json"));
    assert_eq!(
        discovered.json()["result"]["supportedVersions"][0],
        "2026-07-28"
    );
    assert!(!discovered.headers.contains_key("mcp-session-id"));

    let listed = fixture.post("tools/list", &[], &list(2, None));
    assert_eq!(
        listed.json()["result"]["tools"].as_array().unwrap().len(),
        3
    );

    let called = fixture.post(
        "tools/call",
        &[("Mcp-Name", "echo")],
        &call_request(3, "echo", json!({"text": "over http"})),
    );
    assert_eq!(called.json()["result"]["content"][0]["text"], "over http");
}

#[test]
fn http_request_scoped_sse_ignores_notifications() {
    let fixture = HttpChild::spawn(&[]);
    let response = fixture.post(
        "tools/call",
        &[("Accept", "text/event-stream")],
        &call_request(7, "echo", json!({"text": "sse"})),
    );
    assert_eq!(response.status, 200);
    assert!(response.content_type().starts_with("text/event-stream"));

    let messages = sse_messages(&response.body);
    assert_eq!(messages.len(), 2, "one notification + one final response");
    assert_eq!(messages[0]["method"], "notifications/progress");
    assert_eq!(messages[0].get("id"), None);
    assert_eq!(messages[1]["id"], 7);
    assert_eq!(messages[1]["result"]["content"][0]["text"], "sse");
}

#[test]
fn http_rejects_get_wrong_content_type_and_bad_headers() {
    let fixture = HttpChild::spawn(&[]);

    let get = fixture.post_raw("GET", &[], "", "tools/list");
    assert_eq!(get.status, 405);
    assert_eq!(get.headers.get("allow").map(String::as_str), Some("POST"));

    let wrong_type = fixture.post_raw(
        "POST",
        &[("Content-Type", "text/plain")],
        "{}",
        "tools/list",
    );
    assert_eq!(wrong_type.status, 415);

    let mismatch = fixture.post(
        "tools/list",
        &[("Mcp-Method", "tools/call")],
        &list(1, None),
    );
    assert_eq!(mismatch.status, 400);

    let bad_name = fixture.post(
        "tools/call",
        &[("Mcp-Name", "mutate")],
        &call_request(2, "echo", json!({})),
    );
    assert_eq!(bad_name.status, 400);

    let accepted = fixture.post(
        "notifications/cancelled",
        &[],
        &notification("notifications/cancelled", json!({"requestId": 1})),
    );
    assert_eq!(accepted.status, 202);
    assert!(accepted.body.is_empty());
}

#[test]
fn forced_version_mismatch_returns_supported_list() {
    let mut fixture = StdioChild::spawn(&["--force-version-mismatch"]);
    let response = fixture.call(&discover(1));
    assert_eq!(response["error"]["code"], -32022);
    assert_eq!(
        response["error"]["data"]["supported"],
        json!(["2026-07-28"])
    );

    let fixture = HttpChild::spawn(&["--force-version-mismatch"]);
    let response = fixture.post("server/discover", &[], &discover(1));
    assert_eq!(response.json()["error"]["code"], -32022);
}

#[test]
fn unsupported_request_version_makes_discover_fail() {
    let mut fixture = StdioChild::spawn(&[]);
    let request = request(
        1,
        "server/discover",
        json!({"_meta": protocol_meta("2025-11-25")}),
    );
    let response = fixture.call(&request);
    assert_eq!(response["error"]["code"], -32022);
    assert_eq!(response["error"]["data"]["supported"][0], "2026-07-28");
}

#[test]
fn pagination_is_configurable_and_rejects_bad_cursor() {
    let mut fixture = StdioChild::spawn(&["--page-size", "1"]);
    let mut cursor: Option<String> = None;
    let mut names = Vec::new();
    let mut id = 1;
    loop {
        let page = fixture.call(&list(id, cursor.as_deref()));
        id += 1;
        for tool in page["result"]["tools"].as_array().unwrap() {
            names.push(tool["name"].as_str().unwrap().to_string());
        }
        match page["result"]["nextCursor"].as_str() {
            Some(next) => cursor = Some(next.to_string()),
            None => break,
        }
    }
    assert_eq!(names, vec!["echo", "mutate", "content"]);

    let bad = fixture.call(&list(9, Some("not-a-cursor")));
    assert_eq!(bad["error"]["code"], -32602);
}

#[test]
fn fault_error_code_is_returned_verbatim() {
    let mut fixture = StdioChild::spawn(&["--fault-error-code", "-32020"]);
    let response = fixture.call(&call_request(1, "echo", json!({})));
    assert_eq!(response["error"]["code"], -32020);
    assert!(response["result"].is_null());
}

#[test]
fn fault_crash_after_n_calls_kills_the_process() {
    let mut fixture = StdioChild::spawn(&["--fault-crash-after", "1"]);
    let first = fixture.call(&call_request(1, "echo", json!({})));
    assert!(first["result"].is_object());

    fixture.send(&call_request(2, "echo", json!({})));
    assert_eq!(fixture.recv(), None, "no response before the crash");
    assert!(!fixture.wait().success());
}

#[test]
fn fault_delay_ms_slows_calls() {
    let mut fixture = StdioChild::spawn(&["--fault-delay-ms", "200"]);
    let started = Instant::now();
    let response = fixture.call(&call_request(1, "echo", json!({})));
    assert!(response["result"].is_object());
    assert!(
        started.elapsed() >= Duration::from_millis(180),
        "expected the scripted delay, got {:?}",
        started.elapsed()
    );
}

#[test]
fn fault_is_error_returns_joined_text() {
    let mut fixture = StdioChild::spawn(&["--fault-is-error"]);
    let response = fixture.call(&call_request(1, "echo", json!({})));
    assert_eq!(response["result"]["isError"], true);
    let texts: Vec<&str> = response["result"]["content"]
        .as_array()
        .unwrap()
        .iter()
        .map(|block| block["text"].as_str().unwrap())
        .collect();
    assert_eq!(texts, vec!["scripted", "failure"]);
}

#[test]
fn fault_input_required_sets_result_type() {
    let mut fixture = StdioChild::spawn(&["--fault-input-required"]);
    let response = fixture.call(&call_request(1, "echo", json!({})));
    assert_eq!(response["result"]["resultType"], "input_required");
}

#[test]
fn fault_oversized_emits_requested_bytes() {
    let mut fixture = StdioChild::spawn(&["--fault-oversized", "50000"]);
    let response = fixture.call(&call_request(1, "echo", json!({})));
    assert_eq!(
        response["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .len(),
        50000
    );
}

#[test]
fn fault_sse_interrupt_closes_without_a_response() {
    let fixture = HttpChild::spawn(&["--fault-sse-interrupt"]);
    let interrupted = fixture.post(
        "tools/call",
        &[("Accept", "text/event-stream")],
        &call_request(1, "echo", json!({})),
    );
    let messages = sse_messages(&interrupted.body);
    assert_eq!(messages.len(), 1, "stream ends after the notification");
    assert_eq!(messages[0]["method"], "notifications/progress");
    assert!(
        messages
            .iter()
            .all(|message| message.get("id").is_none() || message["id"] != 1)
    );

    // The server survives; the next request is a normal JSON response.
    let healthy = fixture.post("tools/list", &[], &list(2, None));
    assert_eq!(healthy.status, 200);
    assert!(healthy.json()["result"]["tools"].is_array());
}

#[test]
fn content_tool_emits_every_mapped_block_type() {
    let mut fixture = StdioChild::spawn(&[]);
    let response = fixture.call(&call_request(1, "content", json!({})));
    let result = &response["result"];
    let blocks = result["content"].as_array().unwrap();
    let types: Vec<&str> = blocks
        .iter()
        .map(|block| block["type"].as_str().unwrap())
        .collect();
    assert_eq!(
        types,
        vec!["text", "image", "audio", "resource_link", "resource"]
    );
    assert_eq!(blocks[1]["mimeType"], "image/png");
    assert_eq!(blocks[1]["data"], "aGVsbG8=");
    assert_eq!(blocks[2]["mimeType"], "audio/wav");
    assert_eq!(blocks[3]["name"], "fixture.txt");
    assert!(blocks[3]["uri"].as_str().unwrap().starts_with("file://"));
    assert_eq!(blocks[4]["resource"]["text"], "resource text");
    assert_eq!(result["structuredContent"]["kind"], "content");
}

#[test]
fn cancellation_notifications_are_accepted_and_ignored() {
    let mut fixture = StdioChild::spawn(&[]);
    fixture.send(&notification(
        "notifications/cancelled",
        json!({"requestId": 1, "reason": "test"}),
    ));
    let response = fixture.call(&list(2, None));
    assert_eq!(
        response["id"], 2,
        "no response was written for the notification"
    );
    assert!(response["result"]["tools"].is_array());
}

#[test]
fn unknown_method_and_malformed_json_are_protocol_errors() {
    let mut fixture = StdioChild::spawn(&[]);
    let unknown = fixture.call(&request(1, "tools/unknown", json!({})));
    assert_eq!(unknown["error"]["code"], -32601);

    writeln!(fixture.stdin, "not json").unwrap();
    fixture.stdin.flush().unwrap();
    let malformed = fixture.recv().expect("parse error response");
    assert_eq!(malformed["error"]["code"], -32700);
    assert_eq!(malformed["id"], Value::Null);
}
