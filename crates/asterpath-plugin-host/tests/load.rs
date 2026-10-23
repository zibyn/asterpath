//! Plugin-host acceptance scenarios P1-P5, P7-P9 and MCP M1-M17, driven by the
//! real `asterpath-mcp-fixture` binary over stdio and Streamable HTTP.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use asterpath_core::{SessionId, StepId, ToolCallId, TurnId};
use asterpath_plugin_host::{
    PluginCapability, PluginHost, PluginId, PluginLoadStage, PluginManifest, PluginProtocol,
    PluginStatus, PluginTimeouts, PluginTransport,
};
use asterpath_protocol::{
    CancelToken, JsonString, Parallelism, Part, Provenance, ReplaySafety, SideEffect, ToolContext,
    ToolError, ToolName, ToolOutput, ToolRegistry, TransportKind,
};
use asterpath_sandbox::{SandboxPolicy, SandboxProvider};
use asterpath_testing::FakeSandboxProvider;
use asterpath_tools::InMemoryToolRegistry;
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
    assert!(
        bin.exists(),
        "fixture binary missing at {}; run `cargo test --workspace`",
        bin.display()
    );
    bin
}

fn stdio_manifest(id: &str, args: &[&str]) -> PluginManifest {
    PluginManifest {
        manifest_version: 1,
        id: PluginId::new(id).unwrap(),
        name: None,
        version: None,
        required: false,
        protocol: PluginProtocol::Mcp,
        protocol_versions: vec!["2026-07-28".to_owned()],
        transport: PluginTransport::Stdio {
            command: fixture_bin().to_string_lossy().into_owned(),
            args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            env: BTreeMap::new(),
            cwd: None,
        },
        capabilities: vec![PluginCapability::Tools],
        timeouts: PluginTimeouts::default(),
    }
}

fn http_manifest(id: &str, url: &str, headers: BTreeMap<String, String>) -> PluginManifest {
    PluginManifest {
        manifest_version: 1,
        id: PluginId::new(id).unwrap(),
        name: None,
        version: None,
        required: false,
        protocol: PluginProtocol::Mcp,
        protocol_versions: vec!["2026-07-28".to_owned()],
        transport: PluginTransport::Http {
            url: url.to_owned(),
            headers,
        },
        capabilities: vec![PluginCapability::Tools],
        timeouts: PluginTimeouts::default(),
    }
}

fn sse_headers() -> BTreeMap<String, String> {
    BTreeMap::from([("Accept".to_owned(), "text/event-stream".to_owned())])
}

fn free_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("pick a free port");
    listener.local_addr().expect("local addr").to_string()
}

/// A real HTTP fixture child. `kill_on_drop` keeps a panicking test leak-free.
struct HttpFixture {
    child: Child,
    addr: String,
}

impl HttpFixture {
    async fn spawn(args: &[&str]) -> Self {
        Self::spawn_at("127.0.0.1:0", args).await
    }

    async fn spawn_at(addr: &str, args: &[&str]) -> Self {
        let mut child = Command::new(fixture_bin())
            .args(["--http", addr])
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

fn registry() -> Arc<InMemoryToolRegistry> {
    Arc::new(InMemoryToolRegistry::new())
}

fn host(
    registry: &Arc<InMemoryToolRegistry>,
    sandbox: Option<Arc<dyn SandboxProvider>>,
    policy: SandboxPolicy,
) -> PluginHost {
    PluginHost::new(registry.clone(), sandbox, policy)
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

async fn invoke(
    registry: &Arc<InMemoryToolRegistry>,
    name: &str,
    args: Value,
) -> Result<ToolOutput, ToolError> {
    let tool = registry
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

fn loaded_tools(status: &PluginStatus) -> &[ToolName] {
    match status {
        PluginStatus::Loaded { tools } => tools,
        other => panic!("expected Loaded, got {other:?}"),
    }
}

fn failed_stage(status: &PluginStatus) -> PluginLoadStage {
    match status {
        PluginStatus::Failed { stage, .. } => *stage,
        other => panic!("expected Failed, got {other:?}"),
    }
}

// P1 -------------------------------------------------------------------

#[tokio::test]
async fn p1_duplicate_ids_fail_every_manifest_and_register_nothing() {
    let registry = registry();
    let host = host(&registry, None, SandboxPolicy::FullAccess);
    let first = stdio_manifest("dup", &["--stdio"]);
    let second = stdio_manifest("dup", &["--stdio"]);

    let statuses = host.load(vec![first, second]).await.expect("optional");
    assert_eq!(statuses.len(), 2);
    for status in &statuses {
        assert_eq!(failed_stage(status), PluginLoadStage::Manifest);
    }
    assert!(registry.list().is_empty());
}

#[tokio::test]
async fn p1_a_required_duplicate_aborts_startup() {
    let registry = registry();
    let host = host(&registry, None, SandboxPolicy::FullAccess);
    let first = stdio_manifest("dup", &["--stdio"]);
    let mut second = stdio_manifest("dup", &["--stdio"]);
    second.required = true;

    let error = host.load(vec![first, second]).await.unwrap_err();
    assert_eq!(error.plugin.as_str(), "dup");
    assert_eq!(error.stage, PluginLoadStage::Manifest);
    assert!(registry.list().is_empty());
}

// P2 (registration atomicity is unit-tested; this checks plugin isolation) --

#[tokio::test]
async fn p2_one_broken_plugin_does_not_affect_another() {
    let registry = registry();
    let host = host(&registry, None, SandboxPolicy::FullAccess);
    let mut broken = stdio_manifest("broken", &[]);
    broken.transport = PluginTransport::Stdio {
        command: "/nonexistent/asterpath-test-binary".to_owned(),
        args: Vec::new(),
        env: BTreeMap::new(),
        cwd: None,
    };
    let good = stdio_manifest("good", &["--stdio"]);

    let statuses = host.load(vec![broken, good]).await.expect("optional");
    assert_eq!(failed_stage(&statuses[0]), PluginLoadStage::Spawn);
    assert_eq!(loaded_tools(&statuses[1]).len(), 3);
    assert!(registry.get(&ToolName::new("good__echo")).is_some());
    assert!(registry.get(&ToolName::new("broken__echo")).is_none());
}

// P3 / M4 --------------------------------------------------------------

#[tokio::test]
async fn p3_version_mismatch_fails_handshake_and_required_aborts() {
    let registry = registry();
    let host = host(&registry, None, SandboxPolicy::FullAccess);
    let manifest = stdio_manifest("weather", &["--stdio", "--force-version-mismatch"]);
    let statuses = host.load(vec![manifest]).await.expect("optional");
    assert_eq!(failed_stage(&statuses[0]), PluginLoadStage::Handshake);
    assert!(registry.list().is_empty());

    let mut required = stdio_manifest("weather", &["--stdio", "--force-version-mismatch"]);
    required.required = true;
    let error = host.load(vec![required]).await.unwrap_err();
    assert_eq!(error.stage, PluginLoadStage::Handshake);
}

#[tokio::test]
async fn m4_a_server_without_a_common_version_fails_handshake_without_spawning() {
    let provider = Arc::new(FakeSandboxProvider::full());
    let registry = registry();
    let host = host(
        &registry,
        Some(provider.clone() as Arc<dyn SandboxProvider>),
        SandboxPolicy::ReadOnly,
    );
    let manifest = stdio_manifest(
        "weather",
        &["--stdio", "--supported-versions", "2027-01-01"],
    );
    let statuses = host.load(vec![manifest]).await.expect("optional");
    assert_eq!(failed_stage(&statuses[0]), PluginLoadStage::Handshake);
    assert!(registry.list().is_empty());
    assert_eq!(provider.recorded().len(), 1);
}

#[tokio::test]
async fn version_intersection_is_checked_before_spawn() {
    let provider = Arc::new(FakeSandboxProvider::full());
    let registry = registry();
    let host = host(
        &registry,
        Some(provider.clone() as Arc<dyn SandboxProvider>),
        SandboxPolicy::ReadOnly,
    );
    let mut manifest = stdio_manifest("weather", &["--stdio"]);
    manifest.protocol_versions = vec!["2025-11-25".to_owned()];
    let statuses = host.load(vec![manifest]).await.expect("optional");
    assert_eq!(failed_stage(&statuses[0]), PluginLoadStage::Handshake);
    assert!(provider.recorded().is_empty());
}

// P4 / P9 --------------------------------------------------------------

#[tokio::test]
async fn p4_required_spawn_failure_aborts_and_never_leaks_secrets() {
    let registry = registry();
    let host = host(&registry, None, SandboxPolicy::FullAccess);
    let mut manifest = stdio_manifest("bad", &[]);
    manifest.required = true;
    manifest.transport = PluginTransport::Stdio {
        command: "/nonexistent/asterpath-test-binary".to_owned(),
        args: Vec::new(),
        env: BTreeMap::from([("SECRET".to_owned(), "s3cr4t".to_owned())]),
        cwd: None,
    };
    let debug = format!("{manifest:?}");
    assert!(!debug.contains("s3cr4t"), "{debug}");

    let error = host.load(vec![manifest]).await.unwrap_err();
    assert_eq!(error.stage, PluginLoadStage::Spawn);
    assert!(!error.to_string().contains("s3cr4t"));
    assert!(registry.list().is_empty());
}

#[test]
fn p9_manifest_debug_redacts_env_and_header_values() {
    let stdio = PluginManifest {
        manifest_version: 1,
        id: PluginId::new("weather").unwrap(),
        name: None,
        version: None,
        required: false,
        protocol: PluginProtocol::Mcp,
        protocol_versions: vec!["2026-07-28".to_owned()],
        transport: PluginTransport::Stdio {
            command: "weather-mcp".to_owned(),
            args: vec!["--stdio".to_owned()],
            env: BTreeMap::from([
                ("WEATHER_API_KEY".to_owned(), "s3cr4t".to_owned()),
                ("OTHER".to_owned(), "also-s3cr4t".to_owned()),
            ]),
            cwd: None,
        },
        capabilities: vec![PluginCapability::Tools],
        timeouts: PluginTimeouts::default(),
    };
    let debug = format!("{stdio:?}");
    assert!(!debug.contains("s3cr4t"), "{debug}");
    assert!(debug.contains("WEATHER_API_KEY"), "{debug}");

    let http = PluginManifest {
        transport: PluginTransport::Http {
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
    let registry = registry();
    let host = host(
        &registry,
        Some(provider.clone() as Arc<dyn SandboxProvider>),
        SandboxPolicy::ReadOnly,
    );
    let manifest = stdio_manifest("weather", &["--stdio", "--page-size", "1"]);

    let statuses = host.load(vec![manifest]).await.expect("load");
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

    let echo = registry
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

    let mutate = registry
        .get(&ToolName::new("weather__mutate"))
        .unwrap()
        .descriptor()
        .clone();
    assert_eq!(mutate.side_effect, SideEffect::Mutating);
    assert_eq!(mutate.replay, ReplaySafety::Unsafe);

    let output = invoke(&registry, "weather__echo", json!({"text": "hi"}))
        .await
        .expect("call");
    assert_eq!(output.content, vec![text("hi")]);

    let mutation = invoke(&registry, "weather__mutate", json!({}))
        .await
        .expect("call");
    assert_eq!(mutation.content[0], text("mutation 1"));
}

#[tokio::test]
async fn m14_maps_every_content_block_and_structured_content() {
    let registry = registry();
    let host = host(&registry, None, SandboxPolicy::FullAccess);
    host.load(vec![stdio_manifest("weather", &["--stdio"])])
        .await
        .expect("load");

    let output = invoke(&registry, "weather__content", json!({}))
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

// M7 / M15 -------------------------------------------------------------

#[tokio::test]
async fn m7_is_error_maps_to_internal_with_joined_text() {
    let registry = registry();
    let host = host(&registry, None, SandboxPolicy::FullAccess);
    host.load(vec![stdio_manifest(
        "weather",
        &["--stdio", "--fault-is-error"],
    )])
    .await
    .expect("load");

    let error = invoke(&registry, "weather__echo", json!({}))
        .await
        .unwrap_err();
    match error {
        ToolError::Internal { message } => assert_eq!(message, "scripted\nfailure"),
        other => panic!("expected Internal, got {other:?}"),
    }
}

#[tokio::test]
async fn m15_input_required_is_internal_and_not_retried() {
    let registry = registry();
    let host = host(&registry, None, SandboxPolicy::FullAccess);
    host.load(vec![stdio_manifest(
        "weather",
        &["--stdio", "--fault-input-required"],
    )])
    .await
    .expect("load");

    let error = invoke(&registry, "weather__echo", json!({}))
        .await
        .unwrap_err();
    assert!(matches!(error, ToolError::Internal { .. }), "{error:?}");
}

// M9 / M10 -------------------------------------------------------------

#[tokio::test]
async fn m9_call_timeout_comes_from_the_manifest() {
    let registry = registry();
    let host = host(&registry, None, SandboxPolicy::FullAccess);
    let mut manifest = stdio_manifest("weather", &["--stdio"]);
    manifest.timeouts.call = Some(Duration::from_millis(250));
    host.load(vec![manifest]).await.expect("load");

    let descriptor = registry
        .get(&ToolName::new("weather__echo"))
        .unwrap()
        .descriptor()
        .clone();
    assert_eq!(descriptor.timeout, Some(Duration::from_millis(250)));
}

#[tokio::test]
async fn m10_crash_fails_in_flight_and_later_calls() {
    let registry = registry();
    let host = host(&registry, None, SandboxPolicy::FullAccess);
    host.load(vec![stdio_manifest(
        "weather",
        &["--stdio", "--fault-crash-after", "0"],
    )])
    .await
    .expect("load");

    let first = invoke(&registry, "weather__echo", json!({}))
        .await
        .unwrap_err();
    assert!(matches!(first, ToolError::Transport { .. }), "{first:?}");
    let second = invoke(&registry, "weather__echo", json!({}))
        .await
        .unwrap_err();
    assert!(matches!(second, ToolError::Transport { .. }), "{second:?}");
}

// P7 -------------------------------------------------------------------

#[tokio::test]
async fn p7_unknown_fields_load_and_unknown_manifest_version_fails() {
    let registry = registry();
    let host = host(&registry, None, SandboxPolicy::FullAccess);
    let value = json!({
        "manifest_version": 1,
        "id": "weather",
        "protocol": "mcp",
        "protocol_versions": ["2026-07-28"],
        "transport": {
            "kind": "stdio",
            "command": fixture_bin().to_string_lossy(),
            "args": ["--stdio"],
            "env": {},
        },
        "timeouts": {"startup_ms": 5000, "call_ms": 250},
        "future_field": {"ignored": true},
    });
    let manifest: PluginManifest = serde_json::from_value(value).expect("unknown fields ignored");
    let statuses = host.load(vec![manifest]).await.expect("load");
    assert_eq!(loaded_tools(&statuses[0]).len(), 3);
    assert_eq!(
        registry
            .get(&ToolName::new("weather__echo"))
            .unwrap()
            .descriptor()
            .timeout,
        Some(Duration::from_millis(250))
    );

    let value = json!({
        "manifest_version": 2,
        "id": "weather",
        "protocol": "mcp",
        "protocol_versions": ["2026-07-28"],
        "transport": {
            "kind": "stdio",
            "command": fixture_bin().to_string_lossy(),
            "args": ["--stdio"],
        },
    });
    let manifest: PluginManifest = serde_json::from_value(value).expect("parses with version 2");
    let statuses = host.load(vec![manifest]).await.expect("optional");
    assert_eq!(failed_stage(&statuses[0]), PluginLoadStage::Manifest);
}

#[test]
fn p7_unknown_enum_values_are_manifest_parse_errors() {
    let value = json!({
        "manifest_version": 1,
        "id": "weather",
        "protocol": "custom",
        "protocol_versions": ["2026-07-28"],
        "transport": {"kind": "stdio", "command": "x"},
    });
    assert!(serde_json::from_value::<PluginManifest>(value).is_err());
}

// HTTP transport: M2, M3, M11, M16 ------------------------------------

#[tokio::test]
async fn m2_http_json_loads_paginates_registers_and_calls() {
    let fixture = HttpFixture::spawn(&["--page-size", "1"]).await;
    let registry = registry();
    // No sandbox provider: HTTP has no local process (S8).
    let host = host(&registry, None, SandboxPolicy::ReadOnly);
    let manifest = http_manifest("weather", &fixture.url(), BTreeMap::new());

    let statuses = host.load(vec![manifest]).await.expect("load");
    assert_eq!(
        loaded_tools(&statuses[0]),
        &[
            ToolName::new("weather__echo"),
            ToolName::new("weather__mutate"),
            ToolName::new("weather__content"),
        ]
    );

    let echo = registry
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

    let output = invoke(&registry, "weather__echo", json!({"text": "over http"}))
        .await
        .expect("call");
    assert_eq!(output.content, vec![text("over http")]);

    let mutation = invoke(&registry, "weather__mutate", json!({}))
        .await
        .expect("call");
    assert_eq!(mutation.content[0], text("mutation 1"));

    fixture.stop().await;
}

#[tokio::test]
async fn m3_http_request_scoped_sse_ignores_notifications() {
    let fixture = HttpFixture::spawn(&[]).await;
    let registry = registry();
    let host = host(&registry, None, SandboxPolicy::FullAccess);
    // Manifest Accept forces the fixture's request-scoped SSE path.
    let manifest = http_manifest("weather", &fixture.url(), sse_headers());

    // Handshake and discovery are SSE too; progress notifications are ignored.
    let statuses = host.load(vec![manifest]).await.expect("load");
    assert_eq!(loaded_tools(&statuses[0]).len(), 3);

    let output = invoke(&registry, "weather__echo", json!({"text": "over sse"}))
        .await
        .expect("call");
    assert_eq!(output.content, vec![text("over sse")]);

    fixture.stop().await;
}

#[tokio::test]
async fn m11_sse_interruption_fails_only_that_call() {
    let addr = free_addr();
    let registry = registry();
    let host = host(&registry, None, SandboxPolicy::FullAccess);

    let healthy = HttpFixture::spawn_at(&addr, &[]).await;
    host.load(vec![http_manifest(
        "weather",
        &healthy.url(),
        sse_headers(),
    )])
    .await
    .expect("load");
    healthy.stop().await;

    // Same endpoint, now closing each SSE stream before the response.
    let interrupted = HttpFixture::spawn_at(&addr, &["--fault-sse-interrupt"]).await;
    let error = invoke(&registry, "weather__echo", json!({}))
        .await
        .unwrap_err();
    assert!(matches!(error, ToolError::Transport { .. }), "{error:?}");
    interrupted.stop().await;

    // The adapter is not dead: the next call reaches a healthy server.
    let recovered = HttpFixture::spawn_at(&addr, &[]).await;
    let output = invoke(&registry, "weather__echo", json!({"text": "again"}))
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
    let manifest = http_manifest(
        "weather",
        "http://127.0.0.1:1/mcp?token=url-s3cr4t",
        headers.clone(),
    );
    assert!(!format!("{manifest:?}").contains("header-s3cr4t"));

    let registry = registry();
    let host = host(&registry, None, SandboxPolicy::FullAccess);
    let statuses = host.load(vec![manifest]).await.expect("optional");
    assert_eq!(failed_stage(&statuses[0]), PluginLoadStage::Handshake);
    let rendered = format!("{:?}", statuses[0]);
    assert!(!rendered.contains("s3cr4t"), "{rendered}");
    assert!(!rendered.contains("127.0.0.1"), "{rendered}");
    assert!(!rendered.contains("Bearer"), "{rendered}");
    assert!(registry.list().is_empty());

    let mut required = http_manifest(
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
    let registry = registry();
    let host = host(&registry, None, SandboxPolicy::FullAccess);
    let statuses = host
        .load(vec![http_manifest(
            "weather",
            &fixture.url(),
            BTreeMap::new(),
        )])
        .await
        .expect("optional");
    assert_eq!(failed_stage(&statuses[0]), PluginLoadStage::Handshake);
    assert!(registry.list().is_empty());
    fixture.stop().await;
}

// Manifest / transport gating -----------------------------------------

#[tokio::test]
async fn acp_manifests_fail_at_the_manifest_stage() {
    let registry = registry();
    let host = host(&registry, None, SandboxPolicy::FullAccess);
    let mut manifest = stdio_manifest("weather", &["--stdio"]);
    manifest.protocol = PluginProtocol::Acp;
    let statuses = host.load(vec![manifest]).await.expect("optional");
    assert_eq!(failed_stage(&statuses[0]), PluginLoadStage::Manifest);
    assert!(registry.list().is_empty());
}

// Sandbox seam ---------------------------------------------------------

#[tokio::test]
async fn restricted_policy_without_a_provider_fails_without_spawning() {
    let registry = registry();
    let host = host(&registry, None, SandboxPolicy::ReadOnly);
    let statuses = host
        .load(vec![stdio_manifest("weather", &["--stdio"])])
        .await
        .expect("optional");
    assert_eq!(failed_stage(&statuses[0]), PluginLoadStage::Spawn);
    assert!(registry.list().is_empty());
}

#[tokio::test]
async fn spawn_requests_carry_the_confined_policy_and_manifest_env() {
    let provider = Arc::new(FakeSandboxProvider::full());
    let registry = registry();
    let host = host(
        &registry,
        Some(provider.clone() as Arc<dyn SandboxProvider>),
        SandboxPolicy::ReadOnly,
    );
    let mut manifest = stdio_manifest("weather", &["--stdio"]);
    manifest.transport = PluginTransport::Stdio {
        command: fixture_bin().to_string_lossy().into_owned(),
        args: vec!["--stdio".to_owned()],
        env: BTreeMap::from([("WEATHER_API_KEY".to_owned(), "test-token".to_owned())]),
        cwd: None,
    };
    host.load(vec![manifest]).await.expect("load");

    let recorded = provider.recorded();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].policy, SandboxPolicy::ReadOnly);
    assert_eq!(
        recorded[0].env.get(OsStr::new("WEATHER_API_KEY")),
        Some(&OsString::from("test-token"))
    );
}

#[tokio::test]
async fn a_sandbox_refusal_fails_the_plugin_at_spawn() {
    let provider = Arc::new(FakeSandboxProvider::refused("policy denies spawn"));
    let registry = registry();
    let host = host(
        &registry,
        Some(provider.clone() as Arc<dyn SandboxProvider>),
        SandboxPolicy::ReadOnly,
    );
    let statuses = host
        .load(vec![stdio_manifest("weather", &["--stdio"])])
        .await
        .expect("optional");
    assert_eq!(failed_stage(&statuses[0]), PluginLoadStage::Spawn);
    assert_eq!(provider.recorded().len(), 1);
    assert!(registry.list().is_empty());
}

#[tokio::test]
async fn full_access_runs_without_a_provider() {
    let registry = registry();
    let host = host(&registry, None, SandboxPolicy::FullAccess);
    let statuses = host
        .load(vec![stdio_manifest("weather", &["--stdio"])])
        .await
        .expect("load");
    assert_eq!(loaded_tools(&statuses[0]).len(), 3);
}
