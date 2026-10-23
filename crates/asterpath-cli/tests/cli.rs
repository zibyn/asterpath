//! Reference CLI behaviour: run, resume, inspect, trace, usage and config check.

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use asterpath::{Asterpath, anthropic::AnthropicProvider};
use asterpath_protocol::{
    CompactionConfig, ModelId, ModelSpec, Part, ProviderId, RequestConfig, SessionEvent,
    SessionSpec, SubagentConfig, ToolSelection, TurnConfig, UserInput, encode_entry,
};
use asterpath_protocol::{InputId, TurnId};
use common::{MockServer, Reply, bin};

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "asterpath-cli-{name}-{}-{}",
            std::process::id(),
            unique()
        ));
        fs::create_dir_all(&path).expect("temp dir");
        Self { path }
    }

    fn config(&self, body: &str) -> PathBuf {
        let path = self.path.join("asterpath.toml");
        fs::write(&path, body).expect("write config");
        path
    }

    fn db(&self) -> PathBuf {
        self.path.join("sessions.db")
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn unique() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

fn config_body(server: &MockServer, db: &Path) -> String {
    format!(
        "[storage]\npath = \"{}\"\n\n[providers.anthropic]\nbase_url = \"{}\"\n",
        db.display(),
        server.base_url()
    )
}

fn session_from_stderr(stderr: &str) -> String {
    stderr
        .lines()
        .find_map(|line| line.strip_prefix("session: "))
        .expect("session id on stderr")
        .trim()
        .to_owned()
}

#[test]
fn run_prints_final_assistant_text_and_session_id() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("Hello from the mock"))]);
    let dir = TestDir::new("run-ok");
    let config = dir.config(&config_body(&server, &dir.db()));

    let output = bin()
        .env("ASTERPATH_PROVIDER", "not-a-provider")
        .env("ASTERPATH_MODEL", "env-model")
        .args([
            "run",
            "--config",
            config.to_str().unwrap(),
            "--provider",
            "anthropic",
            "--model",
            "claude-test",
            "say hello",
        ])
        .output()
        .expect("run asterpath");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "Hello from the mock"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    let session = session_from_stderr(&stderr);
    assert_eq!(session.len(), 32, "hex session id: {session}");
    assert!(dir.db().exists(), "the sqlite database was created");
}

#[test]
fn run_uses_the_model_flag_over_the_environment() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("ok"))]);
    let dir = TestDir::new("model-precedence");
    let config = dir.config(&config_body(&server, &dir.db()));

    bin()
        .env("ASTERPATH_MODEL", "env-model")
        .args([
            "run",
            "--config",
            config.to_str().unwrap(),
            "--provider",
            "anthropic",
            "--model",
            "flag-model",
            "hi",
        ])
        .assert()
        .success();

    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0].body.contains("\"model\":\"flag-model\""),
        "flag model wins: {}",
        requests[0].body
    );
    assert!(!requests[0].body.contains("env-model"));
}

#[test]
fn run_fails_with_one_when_the_turn_does_not_complete() {
    let server = MockServer::start(vec![Reply::sse(common::context_exceeded_sse("truncated"))]);
    let dir = TestDir::new("run-failed");
    let config = dir.config(&config_body(&server, &dir.db()));

    bin()
        .args([
            "run",
            "--config",
            config.to_str().unwrap(),
            "--provider",
            "anthropic",
            "--model",
            "claude-test",
            "hi",
        ])
        .assert()
        .code(1);
}

#[test]
fn run_rejects_a_session_option_and_other_usage_errors_with_two() {
    let dir = TestDir::new("run-usage");
    let config = dir.config("");

    bin()
        .args([
            "run",
            "--config",
            config.to_str().unwrap(),
            "--session",
            "00000000000000000000000000000000",
            "hi",
        ])
        .assert()
        .code(2);

    // Provider and model are required to assemble a SessionSpec.
    bin()
        .args(["run", "--config", config.to_str().unwrap(), "hi"])
        .assert()
        .code(2);
}

#[test]
fn config_check_validates_without_initialising_modules() {
    let dir = TestDir::new("config-ok");
    let db = dir.db();
    let config = dir.config(&format!(
        "[storage]\npath = \"{}\"\n\n[model_aliases.fast]\nprovider = \"anthropic\"\nmodel = \"claude-test\"\n",
        db.display()
    ));

    bin()
        .args(["config", "check", "--config", config.to_str().unwrap()])
        .assert()
        .success();

    assert!(!db.exists(), "config check must not open the database");
}

#[test]
fn config_check_rejects_zero_max_steps_with_two() {
    let dir = TestDir::new("config-zero");
    let config = dir.config("[turn]\nmax_steps_per_turn = 0\n");

    bin()
        .args(["config", "check", "--config", config.to_str().unwrap()])
        .assert()
        .code(2);
}

#[test]
fn config_check_rejects_secret_fields_with_two() {
    for field in ["api_key", "secret", "token"] {
        let dir = TestDir::new(&format!("config-secret-{field}"));
        let config = dir.config(&format!("[providers.anthropic]\n{field} = \"leaked\"\n"));
        let output = bin()
            .args(["config", "check", "--config", config.to_str().unwrap()])
            .output()
            .expect("run config check");
        assert_eq!(
            output.status.code(),
            Some(2),
            "{field} must be rejected: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn config_check_rejects_an_alias_to_an_unknown_provider() {
    let dir = TestDir::new("config-alias");
    let config = dir.config("[model_aliases.fast]\nprovider = \"nope\"\nmodel = \"m\"\n");

    bin()
        .args(["config", "check", "--config", config.to_str().unwrap()])
        .assert()
        .code(2);
}

#[test]
fn config_flag_wins_over_the_environment() {
    let dir = TestDir::new("config-precedence");
    let invalid = dir.config("[turn]\nmax_steps_per_turn = 0\n");
    let valid = dir.path.join("valid.toml");
    fs::write(&valid, "[turn]\nmax_steps_per_turn = 3\n").expect("write valid config");

    bin()
        .env("ASTERPATH_CONFIG", &valid)
        .args(["config", "check", "--config", invalid.to_str().unwrap()])
        .assert()
        .code(2);

    bin()
        .env("ASTERPATH_CONFIG", &invalid)
        .args(["config", "check", "--config", valid.to_str().unwrap()])
        .assert()
        .success();
}

#[test]
fn inspect_reports_the_projection_summary_and_open_intents() {
    let (dir, session) = completed_session("inspect-summary");
    let config = dir.path.join("asterpath.toml");

    let output = bin()
        .args(["inspect", "--config", config.to_str().unwrap(), &session])
        .output()
        .expect("run inspect");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains(&format!("session: {session}")), "{stdout}");
    assert!(stdout.contains("state: idle"), "{stdout}");
    assert!(stdout.contains("open intents: none"), "{stdout}");
}

#[test]
fn inspect_json_prints_the_full_projection() {
    let (dir, session) = completed_session("inspect-json");
    let config = dir.path.join("asterpath.toml");

    let output = bin()
        .args([
            "inspect",
            "--json",
            "--config",
            config.to_str().unwrap(),
            &session,
        ])
        .output()
        .expect("run inspect");
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).expect("json projection");
    let items = value.as_array().expect("projection is an array");
    assert!(
        items
            .iter()
            .any(|item| item.to_string().contains("Hello from the mock")),
        "{items:?}"
    );
}

#[test]
fn inspect_unknown_session_exits_one() {
    let dir = TestDir::new("inspect-missing");
    let config = dir.config(&format!("[storage]\npath = \"{}\"\n", dir.db().display()));

    bin()
        .args([
            "inspect",
            "--config",
            config.to_str().unwrap(),
            "00000000000000000000000000000000",
        ])
        .assert()
        .code(1);
}

#[test]
fn db_flag_wins_over_the_environment() {
    let (dir, session) = completed_session("db-precedence");
    let config = dir.path.join("asterpath.toml");
    let real_db = dir.db();
    let other_db = dir.path.join("other.db");

    bin()
        .env("ASTERPATH_DB", &other_db)
        .args([
            "inspect",
            "--config",
            config.to_str().unwrap(),
            "--db",
            real_db.to_str().unwrap(),
            &session,
        ])
        .assert()
        .success();

    bin()
        .env("ASTERPATH_DB", &real_db)
        .args([
            "inspect",
            "--config",
            config.to_str().unwrap(),
            "--db",
            other_db.to_str().unwrap(),
            &session,
        ])
        .assert()
        .code(1);
}

#[test]
fn resume_prints_idle_for_a_finished_session() {
    let (dir, session) = completed_session("resume-idle");
    let config = dir.path.join("asterpath.toml");

    let output = bin()
        .args(["resume", "--config", config.to_str().unwrap(), &session])
        .output()
        .expect("run resume");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "idle");
}

#[test]
fn resume_continues_an_open_turn() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("Resumed turn"))]);
    let (dir, session) = prepared_session("resume-open", &server, true);
    let config = dir.path.join("asterpath.toml");

    let output = bin()
        .args(["resume", "--config", config.to_str().unwrap(), &session])
        .output()
        .expect("run resume");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "Resumed turn"
    );
    assert_eq!(server.requests().len(), 1);
}

#[test]
fn resume_continues_pending_input() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("Pending input ran"))]);
    let (dir, session) = prepared_session("resume-pending", &server, false);
    let config = dir.path.join("asterpath.toml");

    let output = bin()
        .args(["resume", "--config", config.to_str().unwrap(), &session])
        .output()
        .expect("run resume");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "Pending input ran"
    );
}

#[test]
fn resume_unknown_session_exits_one() {
    let dir = TestDir::new("resume-missing");
    let config = dir.config(&format!("[storage]\npath = \"{}\"\n", dir.db().display()));

    bin()
        .args([
            "resume",
            "--config",
            config.to_str().unwrap(),
            "00000000000000000000000000000000",
        ])
        .assert()
        .code(1);
}

#[test]
fn trace_prints_the_span_tree() {
    let (dir, session) = completed_session("trace-tree");
    let config = dir.path.join("asterpath.toml");

    let output = bin()
        .args(["trace", "--config", config.to_str().unwrap(), &session])
        .output()
        .expect("run trace");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    for prefix in [
        "session session",
        "  turn turn",
        "    step step",
        "      model_call chat",
    ] {
        assert!(
            stdout.lines().any(|line| line.starts_with(prefix)),
            "missing {prefix:?} in:\n{stdout}"
        );
    }
}

#[test]
fn trace_json_prints_the_full_spans() {
    let (dir, session) = completed_session("trace-json");
    let config = dir.path.join("asterpath.toml");

    let output = bin()
        .args([
            "trace",
            "--json",
            "--config",
            config.to_str().unwrap(),
            &session,
        ])
        .output()
        .expect("run trace");
    assert!(output.status.success());
    let spans: serde_json::Value = serde_json::from_slice(&output.stdout).expect("span json");
    let spans = spans.as_array().expect("span list");
    for kind in ["Session", "Turn", "Step", "ModelCall"] {
        assert!(
            spans.iter().any(|span| span["kind"] == kind),
            "missing {kind}: {spans:?}"
        );
    }
    assert!(spans.iter().all(|span| span["session"].is_string()));
}

#[test]
fn trace_unknown_session_exits_one() {
    let dir = TestDir::new("trace-missing");
    let config = dir.config(&format!("[storage]\npath = \"{}\"\n", dir.db().display()));

    bin()
        .args([
            "trace",
            "--config",
            config.to_str().unwrap(),
            "00000000000000000000000000000000",
        ])
        .assert()
        .code(1);
}

#[test]
fn usage_lists_own_and_descendants_totals() {
    let (dir, session) = completed_session("usage-text");
    let config = dir.path.join("asterpath.toml");

    let output = bin()
        .args(["usage", "--config", config.to_str().unwrap(), &session])
        .output()
        .expect("run usage");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("own: input=5 output=2 total=7"), "{stdout}");
    assert!(
        stdout.contains("descendants: input=0 output=0 total=0"),
        "{stdout}"
    );
}

#[test]
fn usage_json_prints_the_report() {
    let (dir, session) = completed_session("usage-json");
    let config = dir.path.join("asterpath.toml");

    let output = bin()
        .args([
            "usage",
            "--json",
            "--config",
            config.to_str().unwrap(),
            &session,
        ])
        .output()
        .expect("run usage");
    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).expect("usage json");
    assert_eq!(report["own"]["model_calls"]["input"]["uncached"], 5);
    assert_eq!(report["own"]["model_calls"]["output"]["total"], 2);
    assert_eq!(report["descendants"]["model_calls"]["input"]["uncached"], 0);
}

#[test]
fn usage_unknown_session_exits_one() {
    let dir = TestDir::new("usage-missing");
    let config = dir.config(&format!("[storage]\npath = \"{}\"\n", dir.db().display()));

    bin()
        .args([
            "usage",
            "--config",
            config.to_str().unwrap(),
            "00000000000000000000000000000000",
        ])
        .assert()
        .code(1);
}

#[test]
fn explicit_ask_without_a_tty_is_a_usage_error() {
    let dir = TestDir::new("ask-non-tty");
    let config = dir.config("");

    bin()
        .args([
            "run",
            "--config",
            config.to_str().unwrap(),
            "--approval",
            "ask",
            "hi",
        ])
        .assert()
        .code(2);
}

/// Create a session through the library: pending input, optionally with an
/// already-open turn. No drive happens, so the CLI must resume it.
fn prepared_session(name: &str, server: &MockServer, turn_started: bool) -> (TestDir, String) {
    let dir = TestDir::new(name);
    dir.config(&config_body(server, &dir.db()));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let session = runtime.block_on(async {
        let provider = AnthropicProvider::new("test-key").with_base_url(server.base_url());
        let asterpath = Asterpath::builder()
            .database(dir.db())
            .provider(Arc::new(provider))
            .build()
            .await
            .expect("build");
        let handle = asterpath
            .create_session(SessionSpec {
                model: ModelSpec {
                    provider: ProviderId::new("anthropic"),
                    model: ModelId::new("claude-test"),
                    context_window: None,
                },
                system: Vec::new(),
                tools: ToolSelection::All,
                request: RequestConfig::default(),
                turn: TurnConfig::default(),
                compaction: CompactionConfig::default(),
                subagents: SubagentConfig::default(),
            })
            .await
            .expect("create session");
        handle
            .submit_input(UserInput {
                id: InputId::from_bytes([7; 16]),
                parts: vec![Part::Text {
                    text: "hi".to_owned(),
                }],
            })
            .await
            .expect("submit input");
        if turn_started {
            let entry = encode_entry(
                handle.id(),
                None,
                &SessionEvent::TurnStarted {
                    turn_id: TurnId::from_bytes([9; 16]),
                },
            )
            .expect("encode turn");
            asterpath.store().append(entry).await.expect("append turn");
        }
        let session = handle.id().to_hex();
        asterpath.shutdown().await.expect("shutdown");
        session
    });
    (dir, session)
}

/// Run one successful turn and return the directory and session id.
fn completed_session(name: &str) -> (TestDir, String) {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("Hello from the mock"))]);
    let dir = TestDir::new(name);
    let config = dir.config(&config_body(&server, &dir.db()));
    let output = bin()
        .args([
            "run",
            "--config",
            config.to_str().unwrap(),
            "--provider",
            "anthropic",
            "--model",
            "claude-test",
            "hi",
        ])
        .output()
        .expect("run asterpath");
    assert!(output.status.success());
    let session = session_from_stderr(&String::from_utf8_lossy(&output.stderr));
    (dir, session)
}
