//! Reference CLI wiring of MCP servers, the sandbox mode and compaction (#71).

mod common;

use std::path::{Path, PathBuf};
use std::process::Command;

use common::{MockServer, bin};

/// The `asterpath-mcp-fixture` binary, built on demand: `cargo test -p
/// asterpath-cli` does not build another package's bins.
fn mcp_fixture() -> PathBuf {
    if let Ok(path) = std::env::var("ASTERPATH_MCP_FIXTURE") {
        return PathBuf::from(path);
    }
    let bin = std::env::current_exe()
        .expect("test binary path")
        .parent()
        .and_then(Path::parent)
        .expect("target/profile directory")
        .join("asterpath-mcp-fixture");
    if !bin.exists() {
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let status = Command::new(cargo)
            .args(["build", "-p", "asterpath-mcp-fixture"])
            .current_dir(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
            .status()
            .expect("run cargo build -p asterpath-mcp-fixture");
        assert!(
            status.success(),
            "cargo build -p asterpath-mcp-fixture failed"
        );
    }
    bin
}

fn base_config(server: &MockServer, db: &Path) -> String {
    format!(
        "[storage]\npath = \"{}\"\n\n[providers.anthropic]\nbase_url = \"{}\"\n",
        db.display(),
        server.base_url()
    )
}

fn stdio_server(id: &str, extra: &str) -> String {
    format!(
        "\n[mcp_servers.{id}]\ncommand = \"{}\"\nargs = [\"--stdio\"]\n{extra}",
        mcp_fixture().display()
    )
}

fn write(dir: &Path, body: &str) -> PathBuf {
    let path = dir.join("asterpath.toml");
    std::fs::write(&path, body).expect("write config");
    path
}

fn run(dir: &Path, config: &Path, prompt: &str) -> std::process::Output {
    bin()
        .current_dir(dir)
        .args([
            "run",
            "--config",
            config.to_str().unwrap(),
            "--provider",
            "anthropic",
            "--model",
            "claude-test",
            "--approval",
            "allow",
            "--no-skills",
            prompt,
        ])
        .output()
        .expect("run asterpath")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[test]
fn run_calls_a_configured_stdio_mcp_tool() {
    let server = MockServer::start_sequence(vec![
        common::tool_use_sse("fixture__echo", r#"{"text":"from-mcp"}"#),
        common::text_sse("done"),
    ]);
    let dir = tempfile::tempdir().expect("temp dir");
    let db = dir.path().join("sessions.db");
    let config = write(
        dir.path(),
        &(base_config(&server, &db) + &stdio_server("fixture", "")),
    );

    let output = run(dir.path(), &config, "use the tool");

    assert!(output.status.success(), "stderr: {}", text(&output.stderr));
    assert_eq!(text(&output.stdout).trim(), "done");
    let requests = server.requests();
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert!(
        requests[0].contains("fixture__echo"),
        "the MCP tool is offered: {}",
        requests[0]
    );
    assert!(
        requests[1].contains("from-mcp"),
        "the tool result reaches the model: {}",
        requests[1]
    );
}

#[test]
fn config_check_prints_each_server_status() {
    let dir = tempfile::tempdir().expect("temp dir");
    let body = stdio_server("fixture", "")
        + "\n[mcp_servers.broken]\ncommand = \"/nonexistent/asterpath-mcp\"\n";
    let config = write(dir.path(), &body);

    let output = bin()
        .current_dir(dir.path())
        .args(["config", "check", "--config", config.to_str().unwrap()])
        .output()
        .expect("run config check");

    assert!(output.status.success(), "stderr: {}", text(&output.stderr));
    let stdout = text(&output.stdout);
    assert!(
        stdout.contains("mcp: fixture loaded: fixture__content, fixture__echo, fixture__mutate"),
        "{stdout}"
    );
    assert!(stdout.contains("mcp: broken failed at "), "{stdout}");
    assert!(stdout.contains("config ok"), "{stdout}");
}

#[test]
fn a_required_server_failure_is_a_config_error() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write(
        dir.path(),
        "[mcp_servers.broken]\ncommand = \"/nonexistent/asterpath-mcp\"\nrequired = true\n",
    );

    let output = bin()
        .current_dir(dir.path())
        .args(["config", "check", "--config", config.to_str().unwrap()])
        .output()
        .expect("run config check");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stderr: {}",
        text(&output.stderr)
    );
    assert!(
        text(&output.stderr).contains("MCP server broken failed during "),
        "{}",
        text(&output.stderr)
    );
}

#[test]
fn unknown_config_fields_are_rejected() {
    let dir = tempfile::tempdir().expect("temp dir");
    for body in [
        "[mcp_servers.x]\ncommand = \"true\"\nbogus = 1\n",
        "[sandbox]\nmode = \"workspace-write\"\nbogus = 1\n",
        "[model]\ncontext_window = 1000\nbogus = 1\n",
        "[storage]\nbogus = 1\n",
        "bogus = 1\n",
    ] {
        let config = write(dir.path(), body);
        let output = bin()
            .current_dir(dir.path())
            .args(["config", "check", "--config", config.to_str().unwrap()])
            .output()
            .expect("run config check");
        assert_eq!(output.status.code(), Some(2), "{body}");
        assert!(
            text(&output.stderr).contains("bogus"),
            "{body}: {}",
            text(&output.stderr)
        );
    }
}

#[test]
fn an_mcp_server_needs_exactly_one_transport() {
    let dir = tempfile::tempdir().expect("temp dir");
    for body in [
        "[mcp_servers.x]\nrequired = true\n",
        "[mcp_servers.x]\ncommand = \"true\"\nurl = \"http://127.0.0.1:1/mcp\"\n",
    ] {
        let config = write(dir.path(), body);
        let output = bin()
            .current_dir(dir.path())
            .args(["config", "check", "--config", config.to_str().unwrap()])
            .output()
            .expect("run config check");
        assert_eq!(
            output.status.code(),
            Some(2),
            "{body}: {}",
            text(&output.stderr)
        );
    }
}

/// Without bwrap (or off Linux), a restricted mode has no backend and stdio
/// servers fail closed; `full-access` runs them unconfined.
#[test]
fn full_access_runs_stdio_servers_without_a_sandbox_backend() {
    let dir = tempfile::tempdir().expect("temp dir");
    let config = write(
        dir.path(),
        &(stdio_server("fixture", "") + "\n[sandbox]\nmode = \"full-access\"\n"),
    );

    let output = bin()
        .current_dir(dir.path())
        .env("PATH", "/nonexistent")
        .args(["config", "check", "--config", config.to_str().unwrap()])
        .output()
        .expect("run config check");
    assert!(output.status.success(), "stderr: {}", text(&output.stderr));
    assert!(text(&output.stdout).contains("mcp: fixture loaded"));

    let config = write(dir.path(), &stdio_server("fixture", ""));
    let output = bin()
        .current_dir(dir.path())
        .env("PATH", "/nonexistent")
        .args(["config", "check", "--config", config.to_str().unwrap()])
        .output()
        .expect("run config check");
    assert!(output.status.success(), "stderr: {}", text(&output.stderr));
    assert!(
        text(&output.stdout).contains("mcp: fixture failed at Sandbox"),
        "default workspace-write fails closed: {}",
        text(&output.stdout)
    );
}

/// Two tool steps, the second reporting 500 input tokens; the threshold check
/// before step three compacts when a 600-token window is configured. The
/// summary call is the third model request.
fn compaction_run(window: Option<u32>) -> (std::process::Output, String, Vec<String>) {
    let server = MockServer::start_sequence(vec![
        common::tool_use_sse("fixture__echo", r#"{"text":"one"}"#),
        common::tool_use_sse("fixture__echo", r#"{"text":"two"}"#)
            .replace("\"input_tokens\":5", "\"input_tokens\":500"),
        common::text_sse("summary"),
        common::text_sse("done"),
    ]);
    let dir = tempfile::tempdir().expect("temp dir");
    let db = dir.path().join("sessions.db");
    let mut body = base_config(&server, &db) + &stdio_server("fixture", "");
    if let Some(window) = window {
        body += &format!("\n[model]\ncontext_window = {window}\n");
    }
    let config = write(dir.path(), &body);
    let output = run(dir.path(), &config, "go");
    let session = text(&output.stderr)
        .lines()
        .find_map(|line| line.strip_prefix("session: "))
        .expect("session id")
        .trim()
        .to_owned();
    let trace = bin()
        .args(["trace", "--config", config.to_str().unwrap(), &session])
        .output()
        .expect("run trace");
    (output, text(&trace.stdout), server.requests())
}

#[test]
fn context_window_enables_threshold_compaction() {
    let (output, trace, requests) = compaction_run(Some(600));
    assert!(output.status.success(), "stderr: {}", text(&output.stderr));
    assert!(trace.contains("compaction"), "{trace}");
    assert_eq!(requests.len(), 4, "two steps, a summary, the final step");
}

#[test]
fn without_a_context_window_nothing_is_compacted() {
    let (output, trace, requests) = compaction_run(None);
    assert!(output.status.success(), "stderr: {}", text(&output.stderr));
    assert!(!trace.contains("compaction"), "{trace}");
    assert_eq!(requests.len(), 3);
}
