//! Interactive mode (packaging.md §4.5), driven over piped stdin.

mod common;

use std::fs;
use std::path::PathBuf;

use common::{MockServer, Reply, bin};

struct Run {
    stdout: String,
    stderr: String,
    success: bool,
}

/// `asterpath --config <file> [args]` in a fresh directory, fed `stdin`.
fn run(server: &MockServer, args: &[&str], stdin: &str) -> (Run, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("temp dir");
    let root: PathBuf = dir.path().canonicalize().expect("canonical");
    let config = root.join("asterpath.toml");
    fs::write(
        &config,
        format!(
            concat!(
                "[model]\nprovider = \"anthropic\"\nid = \"claude-test\"\n\n",
                "[storage]\npath = \"{}\"\n\n[providers.anthropic]\nbase_url = \"{}\"\n\n",
                "[skills]\nenabled = false\n",
            ),
            root.join("sessions.db").display(),
            server.base_url()
        ),
    )
    .expect("config");
    let output = bin()
        .current_dir(&root)
        .args(["--config", config.to_str().unwrap()])
        .args(args)
        .write_stdin(stdin)
        .output()
        .expect("run asterpath");
    (
        Run {
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            success: output.status.success(),
        },
        dir,
    )
}

fn sessions(stderr: &str) -> Vec<&str> {
    stderr
        .lines()
        .filter_map(|line| line.strip_prefix("session: "))
        .collect()
}

#[test]
fn prompt_then_lines_are_turns_of_one_session_until_eof() {
    let server = MockServer::start(vec![
        Reply::sse(common::text_sse("first answer")),
        Reply::sse(common::text_sse("second answer")),
    ]);

    let (out, _dir) = run(&server, &["hello"], "and more\n");

    assert!(out.success, "stderr: {}", out.stderr);
    assert!(out.stdout.contains("first answer"), "{}", out.stdout);
    assert!(out.stdout.contains("second answer"), "{}", out.stdout);
    assert_eq!(sessions(&out.stderr).len(), 1);
    let body = server.requests()[1].body.clone();
    assert!(
        body.contains("hello") && body.contains("first answer") && body.contains("and more"),
        "{body}"
    );
}

#[test]
fn approval_is_asked_on_the_prompt_and_tool_calls_are_shown() {
    for (answer, ran) in [("y\n", true), ("n\n", false)] {
        let server = MockServer::start(vec![
            Reply::sse(common::tool_use_sse(
                "bash",
                r#"{"command":"touch ran && echo made"}"#,
            )),
            Reply::sse(common::text_sse("done")),
        ]);

        let (out, dir) = run(&server, &["-s", "full-access", "go"], answer);

        assert!(out.success, "stderr: {}", out.stderr);
        assert!(out.stdout.contains("allow? [y/N]"), "{}", out.stdout);
        assert_eq!(dir.path().join("ran").exists(), ran, "{answer:?}");
        let started = out
            .stdout
            .contains(r#"[tool] bash {"command":"touch ran && echo made"}"#);
        assert_eq!(started, ran, "{}", out.stdout);
        if ran {
            assert!(out.stdout.contains("[Completed] made"), "{}", out.stdout);
        }
    }
}

#[test]
fn slash_commands_switch_model_report_status_and_start_a_new_session() {
    let server = MockServer::start(vec![
        Reply::sse(common::text_sse("on the other model")),
        Reply::sse(common::text_sse("fresh answer")),
    ]);
    let stdin = concat!(
        "/help\n/status\n/model claude-other\nhi\n/compact\n/bogus\n",
        "/new\nfresh start\n/exit\nnever sent\n",
    );

    let (out, _dir) = run(&server, &[], stdin);

    assert!(out.success, "stderr: {}", out.stderr);
    let stdout = &out.stdout;
    assert!(stdout.contains("/compact"), "help: {stdout}");
    assert!(stdout.contains("model: anthropic/claude-test"), "{stdout}");
    assert!(stdout.contains("state: Idle"), "{stdout}");
    assert!(stdout.contains("model: claude-other"), "{stdout}");
    assert!(stdout.contains("compact: NotConfigured"), "{stdout}");
    assert!(stdout.contains("unknown command /bogus"), "{stdout}");
    let requests = server.requests();
    assert_eq!(requests.len(), 2, "/exit stops reading");
    assert_eq!(requests[0].json_body()["model"], "claude-other");
    // `/new` opens another session with an empty history.
    let ids = sessions(&out.stderr);
    assert_eq!(ids.len(), 2, "{}", out.stderr);
    assert_ne!(ids[0], ids[1]);
    let body = requests[1].body.clone();
    assert!(
        body.contains("fresh start") && !body.contains("\"hi\""),
        "{body}"
    );
    assert_eq!(
        requests[1].json_body()["model"],
        "claude-other",
        "/new keeps the model chosen with /model"
    );
}
