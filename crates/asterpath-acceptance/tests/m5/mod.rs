//! M5 end-to-end gate: the real fixture MCP server behind asterpath-mcp, inside
//! bwrap, plus crash recovery from the same SQLite file.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use asterpath_mcp::{
    LoadStage, McpServerConfig, McpServers, McpTimeouts, McpTransport, ServerId, ServerStatus,
};
use asterpath_protocol::SandboxPolicy;
use asterpath_protocol::{
    CancelToken, JsonString, Part, Provenance, SessionEvent, ToolContext, ToolErrorKind, ToolName,
    ToolResultStatus, TransportKind, TurnEndReason,
};
use asterpath_protocol::{SessionId, StepId, ToolCallId, TurnId};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

use crate::common::{self, McpScenario, Testdrive};

fn write_config(dir: &Path, name: &str, delay_ms: Option<u64>) -> PathBuf {
    let mut args = vec!["--stdio".to_owned()];
    if let Some(delay) = delay_ms {
        args.push("--fault-delay-ms".to_owned());
        args.push(delay.to_string());
    }
    let config = json!({
        "id": "fixture",
        "protocol_versions": ["2026-07-28"],
        "transport": {
            "kind": "stdio",
            "command": common::mcp_fixture().to_string_lossy(),
            "args": args,
        },
        "timeouts": {"startup_ms": 30000, "call_ms": 120000},
    });
    let path = dir.join(name);
    std::fs::write(&path, config.to_string()).expect("write MCP config");
    path
}

fn write_timeout_config(dir: &Path, name: &str, call_ms: u64, delay_ms: u64) -> PathBuf {
    let config = json!({
        "id": "fixture",
        "protocol_versions": ["2026-07-28"],
        "transport": {
            "kind": "stdio",
            "command": common::mcp_fixture().to_string_lossy(),
            "args": ["--stdio", "--fault-delay-ms", delay_ms.to_string()],
        },
        "timeouts": {"startup_ms": 30000, "call_ms": call_ms},
    });
    let path = dir.join(name);
    std::fs::write(&path, config.to_string()).expect("write MCP config");
    path
}

fn config(id: &str, transport: McpTransport) -> McpServerConfig {
    McpServerConfig {
        id: ServerId::new(id).unwrap(),
        required: false,
        protocol_versions: vec!["2026-07-28".to_owned()],
        transport,
        timeouts: McpTimeouts::default(),
    }
}

fn requested(events: &[SessionEvent], tool: &str) -> Vec<Provenance> {
    events
        .iter()
        .filter_map(|event| match event {
            SessionEvent::ToolCallRequested {
                name, provenance, ..
            } if name.as_str() == tool => Some(*provenance),
            _ => None,
        })
        .collect()
}

fn results(events: &[SessionEvent]) -> Vec<(ToolResultStatus, bool, bool)> {
    events
        .iter()
        .filter_map(|event| match event {
            SessionEvent::ToolResult {
                status,
                output,
                error,
                ..
            } => Some((*status, output.is_some(), error.is_some())),
            _ => None,
        })
        .collect()
}

fn result_errors(events: &[SessionEvent]) -> Vec<(ToolResultStatus, Option<ToolErrorKind>)> {
    events
        .iter()
        .filter_map(|event| match event {
            SessionEvent::ToolResult { status, error, .. } => {
                Some((*status, error.as_ref().map(|error| error.kind)))
            }
            _ => None,
        })
        .collect()
}

fn turns(events: &[SessionEvent]) -> Vec<TurnEndReason> {
    events
        .iter()
        .filter_map(|event| match event {
            SessionEvent::TurnCommitted { reason, .. } => Some(reason.clone()),
            _ => None,
        })
        .collect()
}

async fn events(db: &Path, session: SessionId) -> Vec<SessionEvent> {
    common::events(db, session).await
}

fn loaded_tools(status: &ServerStatus) -> &[ToolName] {
    match status {
        ServerStatus::Loaded { tools } => tools,
        other => panic!("expected Loaded, got {other:?}"),
    }
}

/// Scenario 1: discover / list / call through a real bwrap-confined stdio
/// fixture, and the call is attributed to the external stdio transport.
#[tokio::test]
async fn e2e_stdio_server_loads_and_calls_inside_a_read_only_sandbox() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = dir.path().join("m5.sqlite");
    let effect = dir.path().join("unused");
    let config = write_config(dir.path(), "mcp.json", None);

    let lines = Testdrive::start_mcp(McpScenario {
        run: true,
        db: &db,
        fixture: "m5_echo.json",
        effect: &effect,
        configs: std::slice::from_ref(&config),
        sandbox: "read-only",
        kill_grace_ms: None,
    })
    .wait_success();
    let session = common::session_id(&lines);

    // Discovery found all three fixture tools and the call completed.
    let statuses = common::markers(&lines, "mcp_status");
    assert_eq!(statuses.len(), 1, "{statuses:?}");
    assert_eq!(statuses[0]["status"], "Loaded", "{statuses:?}");
    let mut tools: Vec<&str> = statuses[0]["tools"]
        .as_array()
        .expect("loaded tools")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    tools.sort_unstable();
    assert_eq!(
        tools,
        ["fixture__content", "fixture__echo", "fixture__mutate"]
    );

    let events = events(&db, session).await;
    assert_eq!(
        requested(&events, "fixture__echo"),
        vec![Provenance::External {
            transport: TransportKind::Stdio
        }],
        "one external stdio intent"
    );
    assert_eq!(
        results(&events),
        vec![(ToolResultStatus::Completed, true, false)]
    );
    assert_eq!(turns(&events), vec![TurnEndReason::Completed]);
}

/// Scenario 2: a `Safe` (read-only) tool is in flight when the host is
/// `SIGKILL`ed; resume reruns it exactly once and never duplicates the intent.
#[tokio::test]
async fn safe_external_tool_reruns_once_after_a_host_sigkill() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = dir.path().join("m5.sqlite");
    let effect = dir.path().join("unused");
    let run_config = write_config(dir.path(), "run.json", Some(60_001));
    let resume_config = write_config(dir.path(), "resume.json", None);

    let mut run = Testdrive::start_mcp(McpScenario {
        run: true,
        db: &db,
        fixture: "m5_echo.json",
        effect: &effect,
        configs: std::slice::from_ref(&run_config),
        sandbox: "read-only",
        kill_grace_ms: None,
    });
    let started: Value = serde_json::from_str(&run.wait_marker("\"event\":\"tool_started\""))
        .expect("tool_started json");
    assert_eq!(started["tool"], "fixture__echo");
    let run_lines = run.kill();
    let session = common::session_id(&run_lines);
    common::assert_no_process(&common::mcp_fixture(), "60001");

    let resume_lines = Testdrive::start_mcp(McpScenario {
        run: false,
        db: &db,
        fixture: "m5_echo.json",
        effect: &effect,
        configs: std::slice::from_ref(&resume_config),
        sandbox: "read-only",
        kill_grace_ms: None,
    })
    .wait_success();
    assert_eq!(common::session_id(&resume_lines), session);

    let events = events(&db, session).await;
    assert_eq!(
        requested(&events, "fixture__echo"),
        vec![Provenance::External {
            transport: TransportKind::Stdio
        }],
        "the open intent is not requested twice"
    );
    assert_eq!(
        results(&events),
        vec![(ToolResultStatus::Completed, true, false)],
        "the safe tool reruns once and completes"
    );
    assert_eq!(turns(&events), vec![TurnEndReason::Completed]);
}

/// Scenario 3: an `Unsafe` (mutating) tool in flight at the crash is written
/// as `Interrupted` on resume and is never rerun.
#[tokio::test]
async fn unsafe_external_tool_is_interrupted_and_never_reruns_after_a_sigkill() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = dir.path().join("m5.sqlite");
    let effect = dir.path().join("unused");
    let run_config = write_config(dir.path(), "run.json", Some(60_002));
    let resume_config = write_config(dir.path(), "resume.json", None);

    let mut run = Testdrive::start_mcp(McpScenario {
        run: true,
        db: &db,
        fixture: "m5_mutate.json",
        effect: &effect,
        configs: std::slice::from_ref(&run_config),
        sandbox: "read-only",
        kill_grace_ms: None,
    });
    let started: Value = serde_json::from_str(&run.wait_marker("\"event\":\"tool_started\""))
        .expect("tool_started json");
    assert_eq!(started["tool"], "fixture__mutate");
    let run_lines = run.kill();
    let session = common::session_id(&run_lines);
    common::assert_no_process(&common::mcp_fixture(), "60002");

    let resume_lines = Testdrive::start_mcp(McpScenario {
        run: false,
        db: &db,
        fixture: "m5_mutate.json",
        effect: &effect,
        configs: std::slice::from_ref(&resume_config),
        sandbox: "read-only",
        kill_grace_ms: None,
    })
    .wait_success();
    assert_eq!(common::session_id(&resume_lines), session);

    let events = events(&db, session).await;
    assert_eq!(
        requested(&events, "fixture__mutate"),
        vec![Provenance::External {
            transport: TransportKind::Stdio
        }],
        "the unsafe intent is not requested twice"
    );
    assert_eq!(
        results(&events),
        vec![(ToolResultStatus::Interrupted, false, false)],
        "the unsafe tool is interrupted, never rerun"
    );
    assert_eq!(turns(&events), vec![TurnEndReason::Completed]);
}

/// Scenario 4: a restricted policy without a provider fails the server load at
/// `Sandbox`, before any process could exist, and loads nothing.
#[tokio::test]
async fn restricted_policy_without_a_provider_fails_closed_before_any_process() {
    let config = config(
        "fixture",
        McpTransport::Stdio {
            command: "definitely-not-a-real-binary".to_owned(),
            args: Vec::new(),
            env: BTreeMap::new(),
            cwd: None,
        },
    );

    let servers = McpServers::load(vec![config], None, SandboxPolicy::ReadOnly)
        .await
        .expect("optional");
    match &servers.statuses()[0] {
        ServerStatus::Failed { stage, message } => {
            assert_eq!(*stage, LoadStage::Sandbox);
            assert!(
                message.contains("requires a SandboxProvider"),
                "failed at the provider check, not by trying to exec: {message}"
            );
        }
        other => panic!("expected Failed at Sandbox, got {other:?}"),
    }
    assert!(servers.tools().is_empty());
}

/// Scenario 5: an HTTP server has no local process, so a restricted policy
/// without a provider neither blocks it nor sandboxes it.
#[tokio::test]
async fn http_server_loads_and_calls_without_a_sandbox() {
    let fixture = HttpFixture::spawn().await;
    let config = config(
        "fixture",
        McpTransport::Http {
            url: fixture.url(),
            headers: BTreeMap::new(),
        },
    );

    let servers = McpServers::load(vec![config], None, SandboxPolicy::ReadOnly)
        .await
        .expect("load");
    assert!(loaded_tools(&servers.statuses()[0]).contains(&ToolName::new("fixture__echo")));
    let tool = servers
        .tools()
        .into_iter()
        .find(|tool| tool.descriptor().name == ToolName::new("fixture__echo"))
        .expect("fixture__echo registered");
    assert_eq!(
        tool.descriptor().provenance,
        Provenance::External {
            transport: TransportKind::Http
        }
    );

    let output = tool
        .invoke(
            JsonString::new(r#"{"text":"over http"}"#.to_owned()),
            context(),
        )
        .await
        .expect("call over http");
    assert_eq!(
        output.content,
        vec![Part::Text {
            text: "over http".to_owned()
        }]
    );
    fixture.stop().await;
}

/// Scenario 6: a per-call deadline kills the stdio server; the next call is
/// rejected by the dead adapter, so the pipeline records Timeout then Transport.
#[tokio::test]
async fn pipeline_timeout_kills_the_server_and_later_calls_fail_transport() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = dir.path().join("m5.sqlite");
    let effect = dir.path().join("unused");
    let config = write_timeout_config(dir.path(), "mcp.json", 200, 5_000);

    let started = Instant::now();
    let lines = Testdrive::start_mcp(McpScenario {
        run: true,
        db: &db,
        fixture: "m5_timeout.json",
        effect: &effect,
        configs: std::slice::from_ref(&config),
        sandbox: "read-only",
        kill_grace_ms: Some(100),
    })
    .wait_success();
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "timeout scenario took {:?}",
        started.elapsed()
    );
    let session = common::session_id(&lines);

    let events = events(&db, session).await;
    assert_eq!(requested(&events, "fixture__echo").len(), 2);
    assert_eq!(
        result_errors(&events),
        vec![
            (ToolResultStatus::Failed, Some(ToolErrorKind::Timeout)),
            (ToolResultStatus::Failed, Some(ToolErrorKind::Transport)),
        ]
    );
    assert_eq!(turns(&events), vec![TurnEndReason::Completed]);
}

/// Scenario 7: crash mid-call on a `Safe` external tool, then resume with no
/// MCP config. The tool is absent from the snapshot, so recovery records
/// exactly one intent and one `NotFound` result and never reruns it.
#[tokio::test]
async fn safe_external_tool_missing_after_resume_is_not_found_and_not_rerun() {
    let dir = tempfile::tempdir().expect("temp dir");
    let db = dir.path().join("m5.sqlite");
    let effect = dir.path().join("unused");
    let run_config = write_config(dir.path(), "run.json", Some(60_003));

    let mut run = Testdrive::start_mcp(McpScenario {
        run: true,
        db: &db,
        fixture: "m5_echo.json",
        effect: &effect,
        configs: std::slice::from_ref(&run_config),
        sandbox: "read-only",
        kill_grace_ms: None,
    });
    let started: Value = serde_json::from_str(&run.wait_marker("\"event\":\"tool_started\""))
        .expect("tool_started json");
    assert_eq!(started["tool"], "fixture__echo");
    let run_lines = run.kill();
    let session = common::session_id(&run_lines);
    common::assert_no_process(&common::mcp_fixture(), "60003");

    let resume_lines = Testdrive::start_mcp(McpScenario {
        run: false,
        db: &db,
        fixture: "m5_echo.json",
        effect: &effect,
        configs: &[],
        sandbox: "read-only",
        kill_grace_ms: None,
    })
    .wait_success();
    assert_eq!(common::session_id(&resume_lines), session);

    let events = events(&db, session).await;
    assert_eq!(
        requested(&events, "fixture__echo").len(),
        1,
        "the open intent is not requested twice"
    );
    assert_eq!(
        result_errors(&events),
        vec![(ToolResultStatus::Failed, Some(ToolErrorKind::NotFound))],
        "the missing tool is not rerun"
    );
    assert_eq!(turns(&events), vec![TurnEndReason::Completed]);
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

struct HttpFixture {
    child: Child,
    addr: String,
}

impl HttpFixture {
    async fn spawn() -> Self {
        let mut child = Command::new(common::mcp_fixture())
            .args(["--http", "127.0.0.1:0"])
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
