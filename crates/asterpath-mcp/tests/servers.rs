//! MCP tool source scenarios P1-P5, P7-P9 and MCP M1-M17, driven by the
//! real `asterpath-mcp-fixture` binary over stdio and Streamable HTTP.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use asterpath_mcp::{
    LoadStage, McpError, McpServerConfig, McpServers, McpTimeouts, McpTransport, ServerId,
    ServerStatus,
};
use asterpath_protocol::{
    CancelToken, JsonString, Parallelism, Part, Provenance, ReplaySafety, SideEffect, Tool,
    ToolContext, ToolError, ToolName, ToolOutput, TransportKind,
};
use asterpath_protocol::{SandboxPolicy, SandboxProvider};
use asterpath_protocol::{SessionId, StepId, ToolCallId, TurnId};
use asterpath_testing::FakeSandboxProvider;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

fn fixture_bin() -> PathBuf {
    if let Ok(path) = std::env::var("ASTERPATH_MCP_FIXTURE") {
        return PathBuf::from(path);
    }
    let exe = std::env::current_exe().expect("test binary path");
    let bin = exe
        .parent()
        .and_then(|deps| deps.parent())
        .expect("target/profile directory")
        .join("asterpath-mcp-fixture");
    if !bin.exists() {
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let status = std::process::Command::new(cargo)
            .args(["build", "-p", "asterpath-mcp-fixture"])
            .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
            .status()
            .expect("run cargo build -p asterpath-mcp-fixture");
        assert!(
            status.success(),
            "cargo build -p asterpath-mcp-fixture failed"
        );
    }
    assert!(
        bin.exists(),
        "fixture binary missing at {}; run `cargo build -p asterpath-mcp-fixture`",
        bin.display()
    );
    bin
}

fn stdio_config(id: &str, args: &[&str]) -> McpServerConfig {
    McpServerConfig {
        id: ServerId::new(id).unwrap(),
        required: false,
        protocol_versions: vec!["2026-07-28".to_owned()],
        transport: McpTransport::Stdio {
            command: fixture_bin().to_string_lossy().into_owned(),
            args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            env: BTreeMap::new(),
            cwd: None,
        },
        timeouts: McpTimeouts::default(),
    }
}

fn http_config(id: &str, url: &str, headers: BTreeMap<String, String>) -> McpServerConfig {
    McpServerConfig {
        id: ServerId::new(id).unwrap(),
        required: false,
        protocol_versions: vec!["2026-07-28".to_owned()],
        transport: McpTransport::Http {
            url: url.to_owned(),
            headers,
        },
        timeouts: McpTimeouts::default(),
    }
}

fn sse_headers() -> BTreeMap<String, String> {
    BTreeMap::from([("Accept".to_owned(), "text/event-stream".to_owned())])
}

/// A fixed local URL relaying each connection to the current backend, so a
/// test can swap fixtures without freeing a port another test could take.
struct Relay {
    addr: String,
    backend: Arc<std::sync::Mutex<String>>,
}

impl Relay {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind relay");
        let addr = listener.local_addr().expect("relay addr").to_string();
        let backend = Arc::new(std::sync::Mutex::new(String::new()));
        let current = Arc::clone(&backend);
        tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                let target = current.lock().expect("relay backend").clone();
                tokio::spawn(async move {
                    if let Ok(mut outbound) = tokio::net::TcpStream::connect(target).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
            }
        });
        Self { addr, backend }
    }

    fn url(&self) -> String {
        format!("http://{}/", self.addr)
    }

    fn point_at(&self, fixture: &HttpFixture) {
        *self.backend.lock().expect("relay backend") = fixture.addr.clone();
    }
}

/// A real HTTP fixture child. `kill_on_drop` keeps a panicking test leak-free.
struct HttpFixture {
    child: Child,
    addr: String,
}

impl HttpFixture {
    async fn spawn(args: &[&str]) -> Self {
        let mut child = Command::new(fixture_bin())
            .args(["--http", "127.0.0.1:0"])
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn fixture");
        let stdout = child.stdout.take().expect("fixture stdout");
        let line = BufReader::new(stdout)
            .lines()
            .next_line()
            .await
            .expect("read listen line")
            .expect("fixture announced its address");
        let addr = line
            .strip_prefix("listening on ")
            .expect("fixture listen line")
            .to_owned();
        Self { child, addr }
    }

    fn url(&self) -> String {
        format!("http://{}/", self.addr)
    }

    async fn stop(mut self) {
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }
}

/// The embedder's sandbox wiring, reused across loads in one test.
struct Host(Option<Arc<dyn SandboxProvider>>, SandboxPolicy);

fn host(sandbox: Option<Arc<dyn SandboxProvider>>, policy: SandboxPolicy) -> Host {
    Host(sandbox, policy)
}

impl Host {
    async fn load(&self, configs: Vec<McpServerConfig>) -> Result<McpServers, McpError> {
        McpServers::load(configs, self.0.clone(), self.1.clone()).await
    }
}

/// Name lookup over the loaded tools.
trait Loaded {
    fn get(&self, name: &ToolName) -> Option<Arc<dyn Tool>>;
}

impl Loaded for McpServers {
    fn get(&self, name: &ToolName) -> Option<Arc<dyn Tool>> {
        self.tools()
            .into_iter()
            .find(|tool| &tool.descriptor().name == name)
    }
}

fn context() -> ToolContext {
    ToolContext::new(
        SessionId::from_bytes([1; 16]),
        TurnId::from_bytes([2; 16]),
        StepId::from_bytes([3; 16]),
        ToolCallId::from_bytes([4; 16]),
        CancelToken::new(),
    )
}

async fn invoke(servers: &McpServers, name: &str, args: Value) -> Result<ToolOutput, ToolError> {
    let tool = servers
        .get(&ToolName::new(name))
        .unwrap_or_else(|| panic!("tool {name} is registered"));
    tool.invoke(JsonString::new(args.to_string()), context())
        .await
}

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}

fn loaded_tools(status: &ServerStatus) -> &[ToolName] {
    match status {
        ServerStatus::Loaded { tools } => tools,
        other => panic!("expected Loaded, got {other:?}"),
    }
}

fn failed_stage(status: &ServerStatus) -> LoadStage {
    match status {
        ServerStatus::Failed { stage, .. } => *stage,
        other => panic!("expected Failed, got {other:?}"),
    }
}

// P1 -------------------------------------------------------------------

#[tokio::test]
async fn p1_duplicate_ids_fail_every_manifest_and_register_nothing() {
    let host = host(None, SandboxPolicy::FullAccess);
    let first = stdio_config("dup", &["--stdio"]);
    let second = stdio_config("dup", &["--stdio"]);

    let servers = host.load(vec![first, second]).await.expect("optional");
    let statuses = servers.statuses();
    assert_eq!(statuses.len(), 2);
    for status in statuses {
        assert_eq!(failed_stage(status), LoadStage::Config);
    }
    assert!(servers.tools().is_empty());
}

#[tokio::test]
async fn p1_a_required_duplicate_aborts_startup() {
    let host = host(None, SandboxPolicy::FullAccess);
    let first = stdio_config("dup", &["--stdio"]);
    let mut second = stdio_config("dup", &["--stdio"]);
    second.required = true;

    let error = host.load(vec![first, second]).await.unwrap_err();
    assert_eq!(error.server.as_str(), "dup");
    assert_eq!(error.stage, LoadStage::Config);
}

// P2 (registration atomicity is unit-tested; this checks server isolation) --

#[tokio::test]
async fn p2_one_broken_server_does_not_affect_another() {
    let host = host(None, SandboxPolicy::FullAccess);
    let mut broken = stdio_config("broken", &[]);
    broken.transport = McpTransport::Stdio {
        command: "/nonexistent/asterpath-test-binary".to_owned(),
        args: Vec::new(),
        env: BTreeMap::new(),
        cwd: None,
    };
    let good = stdio_config("good", &["--stdio"]);

    let servers = host.load(vec![broken, good]).await.expect("optional");
    let statuses = servers.statuses();
    assert_eq!(failed_stage(&statuses[0]), LoadStage::Spawn);
    assert_eq!(loaded_tools(&statuses[1]).len(), 3);
    assert!(servers.get(&ToolName::new("good__echo")).is_some());
    assert!(servers.get(&ToolName::new("broken__echo")).is_none());
}

// P3 / M4 --------------------------------------------------------------

#[tokio::test]
async fn p3_version_mismatch_fails_handshake_and_required_aborts() {
    let host = host(None, SandboxPolicy::FullAccess);
    let config = stdio_config("weather", &["--stdio", "--force-version-mismatch"]);
    let servers = host.load(vec![config]).await.expect("optional");
    let statuses = servers.statuses();
    assert_eq!(failed_stage(&statuses[0]), LoadStage::Handshake);
    assert!(servers.tools().is_empty());

    let mut required = stdio_config("weather", &["--stdio", "--force-version-mismatch"]);
    required.required = true;
    let error = host.load(vec![required]).await.unwrap_err();
    assert_eq!(error.stage, LoadStage::Handshake);
}

#[tokio::test]
async fn m4_a_server_without_a_common_version_fails_handshake_without_spawning() {
    let provider = Arc::new(FakeSandboxProvider::full());
    let host = host(
        Some(provider.clone() as Arc<dyn SandboxProvider>),
        SandboxPolicy::ReadOnly,
    );
    let config = stdio_config(
        "weather",
        &["--stdio", "--supported-versions", "2027-01-01"],
    );
    let servers = host.load(vec![config]).await.expect("optional");
    let statuses = servers.statuses();
    assert_eq!(failed_stage(&statuses[0]), LoadStage::Handshake);
    assert!(servers.tools().is_empty());
    assert_eq!(provider.recorded().len(), 1);
}

#[tokio::test]
async fn version_intersection_is_checked_before_spawn() {
    let provider = Arc::new(FakeSandboxProvider::full());
    let host = host(
        Some(provider.clone() as Arc<dyn SandboxProvider>),
        SandboxPolicy::ReadOnly,
    );
    let mut config = stdio_config("weather", &["--stdio"]);
    config.protocol_versions = vec!["2025-11-25".to_owned()];
    let servers = host.load(vec![config]).await.expect("optional");
    let statuses = servers.statuses();
    assert_eq!(failed_stage(&statuses[0]), LoadStage::Handshake);
    assert!(provider.recorded().is_empty());
}

// P4 / P9 --------------------------------------------------------------

#[tokio::test]
async fn p4_required_spawn_failure_aborts_and_never_leaks_secrets() {
    let host = host(None, SandboxPolicy::FullAccess);
    let mut config = stdio_config("bad", &[]);
    config.required = true;
    config.transport = McpTransport::Stdio {
        command: "/nonexistent/asterpath-test-binary".to_owned(),
        args: Vec::new(),
        env: BTreeMap::from([("SECRET".to_owned(), "s3cr4t".to_owned())]),
        cwd: None,
    };
    let debug = format!("{config:?}");
    assert!(!debug.contains("s3cr4t"), "{debug}");

    let error = host.load(vec![config]).await.unwrap_err();
    assert_eq!(error.stage, LoadStage::Spawn);
    assert!(!error.to_string().contains("s3cr4t"));
}

#[test]
fn p9_manifest_debug_redacts_env_and_header_values() {
    let stdio = McpServerConfig {
        id: ServerId::new("weather").unwrap(),
        required: false,
        protocol_versions: vec!["2026-07-28".to_owned()],
        transport: McpTransport::Stdio {
            command: "weather-mcp".to_owned(),
            args: vec!["--stdio".to_owned()],
            env: BTreeMap::from([
                ("WEATHER_API_KEY".to_owned(), "s3cr4t".to_owned()),
                ("OTHER".to_owned(), "also-s3cr4t".to_owned()),
            ]),
            cwd: None,
        },
        timeouts: McpTimeouts::default(),
    };
    let debug = format!("{stdio:?}");
    assert!(!debug.contains("s3cr4t"), "{debug}");
    assert!(debug.contains("WEATHER_API_KEY"), "{debug}");

    let http = McpServerConfig {
        transport: McpTransport::Http {
            url: "https://example.test/mcp".to_owned(),
            headers: BTreeMap::from([(
                "Authorization".to_owned(),
                "Bearer header-s3cr4t".to_owned(),
            )]),
        },
        ..stdio
    };
    let debug = format!("{http:?}");
    assert!(!debug.contains("header-s3cr4t"), "{debug}");
    assert!(debug.contains("Authorization"), "{debug}");
}

// P5 / M1 / M5 / M12 / M13 / M16 --------------------------------------

#[tokio::test]
async fn p5_m1_loads_paginates_registers_and_calls_tools() {
    let provider = Arc::new(FakeSandboxProvider::full());
    let host = host(
        Some(provider.clone() as Arc<dyn SandboxProvider>),
        SandboxPolicy::ReadOnly,
    );
    let config = stdio_config("weather", &["--stdio", "--page-size", "1"]);

    let servers = host.load(vec![config]).await.expect("load");
    let statuses = servers.statuses();
    assert_eq!(
        loaded_tools(&statuses[0]),
        &[
            ToolName::new("weather__echo"),
            ToolName::new("weather__mutate"),
            ToolName::new("weather__content"),
        ]
    );

    let recorded = provider.recorded();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].program, fixture_bin().into_os_string());
    assert_eq!(recorded[0].policy, SandboxPolicy::ReadOnly);

    let echo = servers
        .get(&ToolName::new("weather__echo"))
        .unwrap()
        .descriptor()
        .clone();
    assert_eq!(
        echo.provenance,
        Provenance::External {
            transport: TransportKind::Stdio
        }
    );
    assert_eq!(echo.timeout, Some(Duration::from_secs(120)));
    assert_eq!(echo.max_output_bytes, None);
    assert_eq!(echo.parallelism, Parallelism::Parallel);
    assert_eq!(echo.side_effect, SideEffect::ReadOnly);
    assert_eq!(echo.replay, ReplaySafety::Safe);

    let mutate = servers
        .get(&ToolName::new("weather__mutate"))
        .unwrap()
        .descriptor()
        .clone();
    assert_eq!(mutate.side_effect, SideEffect::Mutating);
    assert_eq!(mutate.replay, ReplaySafety::Unsafe);

    let output = invoke(&servers, "weather__echo", json!({"text": "hi"}))
        .await
        .expect("call");
    assert_eq!(output.content, vec![text("hi")]);

    let mutation = invoke(&servers, "weather__mutate", json!({}))
        .await
        .expect("call");
    assert_eq!(mutation.content[0], text("mutation 1"));
}

#[tokio::test]
async fn m14_maps_every_content_block_and_structured_content() {
    let host = host(None, SandboxPolicy::FullAccess);
    let servers = host
        .load(vec![stdio_config("weather", &["--stdio"])])
        .await
        .expect("load");

    let output = invoke(&servers, "weather__content", json!({}))
        .await
        .expect("call");
    assert_eq!(
        output.content,
        vec![
            text("hello"),
            Part::Image {
                media_type: "image/png".to_owned(),
                source: asterpath_protocol::ImageSource::Base64 {
                    data: "aGVsbG8=".to_owned(),
                },
            },
            text("[audio content: audio/wav]"),
            text("[resource_link: name=fixture.txt uri=file:///tmp/fixture.txt mime=text/plain]"),
            text("resource text"),
            text("{\"kind\":\"content\"}"),
        ]
    );
}

// M5 negative pagination ----------------------------------------------

#[tokio::test]
async fn m5_a_repeated_cursor_fails_discovery() {
    let host = host(None, SandboxPolicy::FullAccess);
    let servers = host
        .load(vec![stdio_config(
            "weather",
            &["--stdio", "--fault-repeat-cursor"],
        )])
        .await
        .expect("optional");
    let statuses = servers.statuses();
    assert_eq!(failed_stage(&statuses[0]), LoadStage::Discovery);
    assert!(servers.tools().is_empty());
}

#[tokio::test]
async fn m5_more_than_100_pages_fails_discovery() {
    let host = host(None, SandboxPolicy::FullAccess);
    let servers = host
        .load(vec![stdio_config(
            "weather",
            &["--stdio", "--fault-endless-pages"],
        )])
        .await
        .expect("optional");
    let statuses = servers.statuses();
    assert_eq!(failed_stage(&statuses[0]), LoadStage::Discovery);
    assert!(servers.tools().is_empty());
}

// S8: registration failure is isolated per server ---------------------

#[tokio::test]
async fn s8_a_broken_server_registers_nothing_and_a_healthy_one_still_loads() {
    for tool_set in ["collision", "oversized"] {
        let host = host(None, SandboxPolicy::FullAccess);
        let broken = stdio_config("broken", &["--stdio", "--tool-set", tool_set]);
        let healthy = stdio_config("healthy", &["--stdio"]);

        let servers = host.load(vec![broken, healthy]).await.expect("load");
        let statuses = servers.statuses();
        assert_eq!(
            failed_stage(&statuses[0]),
            LoadStage::Registration,
            "{tool_set}"
        );
        assert!(loaded_tools(&statuses[1]).len() == 3, "{tool_set}");
        let names: Vec<String> = servers
            .tools()
            .iter()
            .map(|tool| tool.descriptor().name.as_str().to_owned())
            .collect();
        assert_eq!(names.len(), 3, "{tool_set}: {names:?}");
        assert!(
            names.iter().all(|name| name.starts_with("healthy__")),
            "{tool_set}: {names:?}"
        );
    }
}

// M7 / M15 -------------------------------------------------------------

#[tokio::test]
async fn m7_is_error_maps_to_internal_with_joined_text() {
    let host = host(None, SandboxPolicy::FullAccess);
    let servers = host
        .load(vec![stdio_config(
            "weather",
            &["--stdio", "--fault-is-error"],
        )])
        .await
        .expect("load");

    let error = invoke(&servers, "weather__echo", json!({}))
        .await
        .unwrap_err();
    match error {
        ToolError::Internal { message } => assert_eq!(message, "scripted\nfailure"),
        other => panic!("expected Internal, got {other:?}"),
    }
}

#[tokio::test]
async fn m15_input_required_is_internal_and_not_retried() {
    let host = host(None, SandboxPolicy::FullAccess);
    let servers = host
        .load(vec![stdio_config(
            "weather",
            &["--stdio", "--fault-input-required"],
        )])
        .await
        .expect("load");

    let error = invoke(&servers, "weather__echo", json!({}))
        .await
        .unwrap_err();
    assert!(matches!(error, ToolError::Internal { .. }), "{error:?}");
}

// M9 / M10 -------------------------------------------------------------

#[tokio::test]
async fn m9_call_timeout_comes_from_the_manifest() {
    let host = host(None, SandboxPolicy::FullAccess);
    let mut config = stdio_config("weather", &["--stdio"]);
    config.timeouts.call = Some(Duration::from_millis(250));
    let servers = host.load(vec![config]).await.expect("load");

    let descriptor = servers
        .get(&ToolName::new("weather__echo"))
        .unwrap()
        .descriptor()
        .clone();
    assert_eq!(descriptor.timeout, Some(Duration::from_millis(250)));
}

#[tokio::test]
async fn m10_crash_fails_in_flight_and_later_calls() {
    let host = host(None, SandboxPolicy::FullAccess);
    let servers = host
        .load(vec![stdio_config(
            "weather",
            &["--stdio", "--fault-crash-after", "0"],
        )])
        .await
        .expect("load");

    let first = invoke(&servers, "weather__echo", json!({}))
        .await
        .unwrap_err();
    assert!(matches!(first, ToolError::Transport { .. }), "{first:?}");
    let second = invoke(&servers, "weather__echo", json!({}))
        .await
        .unwrap_err();
    assert!(matches!(second, ToolError::Transport { .. }), "{second:?}");
}

// P7 -------------------------------------------------------------------

#[tokio::test]
async fn p7_a_config_with_millisecond_timeouts_loads() {
    let host = host(None, SandboxPolicy::FullAccess);
    let value = json!({
        "id": "weather",
        "protocol_versions": ["2026-07-28"],
        "transport": {
            "kind": "stdio",
            "command": fixture_bin().to_string_lossy(),
            "args": ["--stdio"],
            "env": {},
        },
        "timeouts": {"startup_ms": 5000, "call_ms": 250},
    });
    let config: McpServerConfig = serde_json::from_value(value).expect("valid config");
    let servers = host.load(vec![config]).await.expect("load");
    assert_eq!(loaded_tools(&servers.statuses()[0]).len(), 3);
    assert_eq!(
        servers
            .get(&ToolName::new("weather__echo"))
            .unwrap()
            .descriptor()
            .timeout,
        Some(Duration::from_millis(250))
    );
}

#[test]
fn unknown_config_fields_are_rejected() {
    let valid = json!({
        "id": "weather",
        "protocol_versions": ["2026-07-28"],
        "transport": {"kind": "stdio", "command": "x"},
    });
    assert!(serde_json::from_value::<McpServerConfig>(valid.clone()).is_ok());

    for (field, value) in [
        ("manifest_version", json!(1)),
        ("protocol", json!("mcp")),
        ("capabilities", json!(["tools"])),
        ("future_field", json!(true)),
    ] {
        let mut config = valid.clone();
        config[field] = value;
        let error = serde_json::from_value::<McpServerConfig>(config).unwrap_err();
        assert!(error.to_string().contains(field), "{field}: {error}");
    }

    let mut transport = valid.clone();
    transport["transport"]["shell"] = json!(true);
    assert!(serde_json::from_value::<McpServerConfig>(transport).is_err());

    let mut timeouts = valid;
    timeouts["timeouts"] = json!({"idle_ms": 5});
    assert!(serde_json::from_value::<McpServerConfig>(timeouts).is_err());
}

#[test]
fn unknown_transport_kinds_are_parse_errors() {
    let value = json!({
        "id": "weather",
        "protocol_versions": ["2026-07-28"],
        "transport": {"kind": "websocket", "url": "x"},
    });
    assert!(serde_json::from_value::<McpServerConfig>(value).is_err());
}

// HTTP transport: M2, M3, M11, M16 ------------------------------------

#[tokio::test]
async fn m2_http_json_loads_paginates_registers_and_calls() {
    let fixture = HttpFixture::spawn(&["--page-size", "1"]).await;
    // No sandbox provider: HTTP has no local process (S8).
    let host = host(None, SandboxPolicy::ReadOnly);
    let config = http_config("weather", &fixture.url(), BTreeMap::new());

    let servers = host.load(vec![config]).await.expect("load");
    let statuses = servers.statuses();
    assert_eq!(
        loaded_tools(&statuses[0]),
        &[
            ToolName::new("weather__echo"),
            ToolName::new("weather__mutate"),
            ToolName::new("weather__content"),
        ]
    );

    let echo = servers
        .get(&ToolName::new("weather__echo"))
        .unwrap()
        .descriptor()
        .clone();
    assert_eq!(
        echo.provenance,
        Provenance::External {
            transport: TransportKind::Http
        }
    );
    assert_eq!(echo.timeout, Some(Duration::from_secs(120)));
    assert_eq!(echo.max_output_bytes, None);
    assert_eq!(echo.parallelism, Parallelism::Parallel);
    assert_eq!(echo.side_effect, SideEffect::ReadOnly);
    assert_eq!(echo.replay, ReplaySafety::Safe);

    let output = invoke(&servers, "weather__echo", json!({"text": "over http"}))
        .await
        .expect("call");
    assert_eq!(output.content, vec![text("over http")]);

    let mutation = invoke(&servers, "weather__mutate", json!({}))
        .await
        .expect("call");
    assert_eq!(mutation.content[0], text("mutation 1"));

    fixture.stop().await;
}

#[tokio::test]
async fn m3_http_request_scoped_sse_ignores_notifications() {
    let fixture = HttpFixture::spawn(&[]).await;
    let host = host(None, SandboxPolicy::FullAccess);
    // Manifest Accept forces the fixture's request-scoped SSE path.
    let config = http_config("weather", &fixture.url(), sse_headers());

    // Handshake and discovery are SSE too; progress notifications are ignored.
    let servers = host.load(vec![config]).await.expect("load");
    let statuses = servers.statuses();
    assert_eq!(loaded_tools(&statuses[0]).len(), 3);

    let output = invoke(&servers, "weather__echo", json!({"text": "over sse"}))
        .await
        .expect("call");
    assert_eq!(output.content, vec![text("over sse")]);

    fixture.stop().await;
}

#[tokio::test]
async fn m11_sse_interruption_fails_only_that_call() {
    let relay = Relay::start().await;
    let host = host(None, SandboxPolicy::FullAccess);

    let healthy = HttpFixture::spawn(&[]).await;
    relay.point_at(&healthy);
    let servers = host
        .load(vec![http_config("weather", &relay.url(), sse_headers())])
        .await
        .expect("load");
    healthy.stop().await;

    // Same endpoint, now closing each SSE stream before the response.
    let interrupted = HttpFixture::spawn(&["--fault-sse-interrupt"]).await;
    relay.point_at(&interrupted);
    let error = invoke(&servers, "weather__echo", json!({}))
        .await
        .unwrap_err();
    assert!(matches!(error, ToolError::Transport { .. }), "{error:?}");
    interrupted.stop().await;

    // The adapter is not dead: the next call reaches a healthy server.
    let recovered = HttpFixture::spawn(&[]).await;
    relay.point_at(&recovered);
    let output = invoke(&servers, "weather__echo", json!({"text": "again"}))
        .await
        .expect("next call succeeds");
    assert_eq!(output.content, vec![text("again")]);
    recovered.stop().await;
}

#[tokio::test]
async fn m16_http_errors_do_not_leak_url_or_header_values() {
    let headers = BTreeMap::from([(
        "Authorization".to_owned(),
        "Bearer header-s3cr4t".to_owned(),
    )]);
    let config = http_config(
        "weather",
        "http://127.0.0.1:1/mcp?token=url-s3cr4t",
        headers.clone(),
    );
    assert!(!format!("{config:?}").contains("header-s3cr4t"));
    let host = host(None, SandboxPolicy::FullAccess);
    let servers = host.load(vec![config]).await.expect("optional");
    let statuses = servers.statuses();
    assert_eq!(failed_stage(&statuses[0]), LoadStage::Handshake);
    let rendered = format!("{:?}", statuses[0]);
    assert!(!rendered.contains("s3cr4t"), "{rendered}");
    assert!(!rendered.contains("127.0.0.1"), "{rendered}");
    assert!(!rendered.contains("Bearer"), "{rendered}");
    assert!(servers.tools().is_empty());

    let mut required = http_config(
        "weather",
        "http://127.0.0.1:1/mcp?token=url-s3cr4t",
        headers,
    );
    required.required = true;
    let error = host.load(vec![required]).await.unwrap_err();
    let rendered = error.to_string();
    assert!(!rendered.contains("s3cr4t"), "{rendered}");
    assert!(!rendered.contains("127.0.0.1"), "{rendered}");
    assert!(!rendered.contains("Bearer"), "{rendered}");
}

#[tokio::test]
async fn http_no_common_version_fails_handshake() {
    let fixture = HttpFixture::spawn(&["--supported-versions", "2027-01-01"]).await;
    let host = host(None, SandboxPolicy::FullAccess);
    let servers = host
        .load(vec![http_config(
            "weather",
            &fixture.url(),
            BTreeMap::new(),
        )])
        .await
        .expect("optional");
    let statuses = servers.statuses();
    assert_eq!(failed_stage(&statuses[0]), LoadStage::Handshake);
    assert!(servers.tools().is_empty());
    fixture.stop().await;
}

// Sandbox seam ---------------------------------------------------------

#[tokio::test]
async fn restricted_policy_without_a_provider_fails_without_spawning() {
    let host = host(None, SandboxPolicy::ReadOnly);
    let servers = host
        .load(vec![stdio_config("weather", &["--stdio"])])
        .await
        .expect("optional");
    let statuses = servers.statuses();
    assert_eq!(failed_stage(&statuses[0]), LoadStage::Sandbox);
    assert!(servers.tools().is_empty());
}

#[tokio::test]
async fn spawn_requests_carry_the_confined_policy_and_manifest_env() {
    let provider = Arc::new(FakeSandboxProvider::full());
    let host = host(
        Some(provider.clone() as Arc<dyn SandboxProvider>),
        SandboxPolicy::ReadOnly,
    );
    let mut config = stdio_config("weather", &["--stdio"]);
    config.transport = McpTransport::Stdio {
        command: fixture_bin().to_string_lossy().into_owned(),
        args: vec!["--stdio".to_owned()],
        env: BTreeMap::from([("WEATHER_API_KEY".to_owned(), "test-token".to_owned())]),
        cwd: None,
    };
    host.load(vec![config]).await.expect("load");

    let recorded = provider.recorded();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].policy, SandboxPolicy::ReadOnly);
    assert_eq!(
        recorded[0].env.get(OsStr::new("WEATHER_API_KEY")),
        Some(&OsString::from("test-token"))
    );
    assert!(
        std::env::var_os("CARGO_MANIFEST_DIR").is_some(),
        "cargo sets it for the test process"
    );
    assert!(
        !recorded[0]
            .env
            .contains_key(OsStr::new("CARGO_MANIFEST_DIR")),
        "the process environment is not inherited"
    );
    assert_eq!(
        recorded[0].env.get(OsStr::new("PATH")),
        std::env::var_os("PATH").as_ref()
    );
}

#[tokio::test]
async fn a_sandbox_refusal_fails_the_server_at_the_sandbox_stage() {
    let provider = Arc::new(FakeSandboxProvider::refused("policy denies spawn"));
    let host = host(
        Some(provider.clone() as Arc<dyn SandboxProvider>),
        SandboxPolicy::ReadOnly,
    );
    let servers = host
        .load(vec![stdio_config("weather", &["--stdio"])])
        .await
        .expect("optional");
    let statuses = servers.statuses();
    assert_eq!(failed_stage(&statuses[0]), LoadStage::Sandbox);
    assert_eq!(provider.recorded().len(), 1);
    assert!(servers.tools().is_empty());
}

#[tokio::test]
async fn full_access_runs_without_a_provider() {
    let host = host(None, SandboxPolicy::FullAccess);
    let servers = host
        .load(vec![stdio_config("weather", &["--stdio"])])
        .await
        .expect("load");
    let statuses = servers.statuses();
    assert_eq!(loaded_tools(&statuses[0]).len(), 3);
}

/// A unique scratch path under the system temp dir; removed before use.
fn scratch(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("asterpath-mcp-{}-{name}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    path
}

/// A stdio config whose process, if it ever starts, runs `script` under `sh`.
fn shell_config(id: &str, script: String) -> McpServerConfig {
    let mut config = stdio_config(id, &[]);
    config.transport = McpTransport::Stdio {
        command: "/bin/sh".to_owned(),
        args: vec!["-c".to_owned(), script],
        env: BTreeMap::new(),
        cwd: None,
    };
    config
}

#[tokio::test]
async fn partial_enforcement_fails_closed_before_the_process_starts() {
    let marker = scratch("partial-started");
    let provider = Arc::new(FakeSandboxProvider::partial(vec![
        asterpath_protocol::SandboxAspect::FilesystemWrite,
    ]));
    let host = host(
        Some(provider.clone() as Arc<dyn SandboxProvider>),
        SandboxPolicy::ReadOnly,
    );
    let config = shell_config(
        "weather",
        format!(
            "touch '{}'; exec '{}' --stdio",
            marker.display(),
            fixture_bin().display()
        ),
    );

    let servers = host.load(vec![config.clone()]).await.expect("optional");
    assert_eq!(failed_stage(&servers.statuses()[0]), LoadStage::Sandbox);
    assert!(servers.tools().is_empty());
    assert_eq!(provider.recorded().len(), 1);

    let mut required = config;
    required.required = true;
    let error = host.load(vec![required]).await.unwrap_err();
    assert_eq!(error.stage, LoadStage::Sandbox);

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!marker.exists(), "the server process was started");
}

#[tokio::test]
async fn shutdown_stops_server_processes_even_while_tools_are_held() {
    let pid_file = scratch("shutdown-pid");
    let config = shell_config(
        "weather",
        format!(
            "echo $$ > '{}'; exec '{}' --stdio",
            pid_file.display(),
            fixture_bin().display()
        ),
    );
    let servers = McpServers::load(vec![config], None, SandboxPolicy::FullAccess)
        .await
        .expect("load");
    let pid = std::fs::read_to_string(&pid_file).expect("pid written");
    let proc_dir = PathBuf::from(format!("/proc/{}", pid.trim()));
    assert!(proc_dir.exists(), "server is running");

    // The engine may still hold the tools; shutdown must not wait for them.
    let held = servers.tools();
    servers.shutdown().await;

    assert!(
        !proc_dir.exists(),
        "server process still running after shutdown"
    );
    let error = held[0]
        .invoke(JsonString::new("{}".to_owned()), context())
        .await
        .unwrap_err();
    assert!(matches!(error, ToolError::Transport { .. }), "{error:?}");
    let _ = std::fs::remove_file(pid_file);
}
