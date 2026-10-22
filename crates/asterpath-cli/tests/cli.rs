//! Reference CLI behaviour: run, inspect and config check.

mod common;

use std::fs;
use std::path::{Path, PathBuf};

use assert_cmd::Command;
use common::MockServer;

fn bin() -> Command {
    let mut command = Command::cargo_bin("asterpath").expect("asterpath binary");
    // The host environment must not decide what the tests exercise.
    for name in [
        "ASTERPATH_CONFIG",
        "ASTERPATH_DB",
        "ASTERPATH_PROVIDER",
        "ASTERPATH_MODEL",
        "ANTHROPIC_API_KEY",
        "OPENAI_API_KEY",
    ] {
        command.env_remove(name);
    }
    command.env("ANTHROPIC_API_KEY", "test-key");
    command
}

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
    let server = MockServer::start(common::text_sse("Hello from the mock"));
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
    let server = MockServer::start(common::text_sse("ok"));
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
        requests[0].contains("\"model\":\"flag-model\""),
        "flag model wins: {}",
        requests[0]
    );
    assert!(!requests[0].contains("env-model"));
}

#[test]
fn run_fails_with_one_when_the_turn_does_not_complete() {
    let server = MockServer::start(common::context_exceeded_sse("truncated"));
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

/// Run one successful turn and return the directory and session id.
fn completed_session(name: &str) -> (TestDir, String) {
    let server = MockServer::start(common::text_sse("Hello from the mock"));
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
