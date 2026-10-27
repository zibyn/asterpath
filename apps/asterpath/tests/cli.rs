//! Reference CLI behaviour (packaging.md §4): `exec`, session continuation and
//! forking, configuration and `-c`, `doctor` and `debug`.

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use assert_cmd::Command;
use asterpath::{Asterpath, anthropic::AnthropicProvider};
use asterpath_protocol::{
    CompactionConfig, ModelId, ModelSpec, Part, ProviderId, RequestConfig, SessionEvent,
    SessionSpec, SubagentConfig, ToolSelection, TurnConfig, UserInput, encode_entry,
};
use asterpath_protocol::{InputId, SessionStore, TurnId};
use common::{MockServer, Reply, bin};

struct TestDir {
    path: PathBuf,
}

impl TestDir {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "asterpath-app-{name}-{}-{}",
            std::process::id(),
            unique()
        ));
        fs::create_dir_all(&path).expect("temp dir");
        Self {
            path: path.canonicalize().expect("canonical temp dir"),
        }
    }

    fn config(&self, body: &str) -> PathBuf {
        let path = self.path.join("asterpath.toml");
        fs::write(&path, body).expect("write config");
        path
    }

    fn config_path(&self) -> PathBuf {
        self.path.join("asterpath.toml")
    }

    fn db(&self) -> PathBuf {
        self.path.join("sessions.db")
    }

    /// A subdirectory to run in.
    fn sub(&self, name: &str) -> PathBuf {
        let path = self.path.join(name);
        fs::create_dir_all(&path).expect("subdir");
        path
    }

    /// `asterpath exec --config <this config>` run from `cwd`.
    fn exec(&self, cwd: &Path) -> Command {
        let mut command = bin();
        command
            .current_dir(cwd)
            .args(["exec", "--config", self.config_path().to_str().unwrap()]);
        command
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
        concat!(
            "[model]\nprovider = \"anthropic\"\nid = \"claude-test\"\n\n",
            "[storage]\npath = \"{}\"\n\n[providers.anthropic]\nbase_url = \"{}\"\n\n",
            "[skills]\nenabled = false\n",
        ),
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

/// Run to success and return (stdout, session id).
fn ok(command: &mut Command) -> (String, String) {
    let output = command.output().expect("run asterpath");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "stderr: {stderr}");
    (
        String::from_utf8_lossy(&output.stdout).trim().to_owned(),
        session_from_stderr(&stderr),
    )
}

fn code(command: &mut Command) -> Option<i32> {
    command.output().expect("run asterpath").status.code()
}

/// The session metadata `session` was created with.
fn created_metadata(db: &Path, session: &str) -> std::collections::BTreeMap<String, String> {
    let id: asterpath_protocol::SessionId =
        serde_json::from_value(serde_json::json!(session)).expect("session id");
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let entries = runtime.block_on(async {
        let store = asterpath_storage_sqlite::open(db).expect("open the database");
        asterpath_protocol::scan_all(&*store, id)
            .await
            .expect("scan")
    });
    match asterpath_protocol::decode_entry(&entries[0]).expect("decode") {
        SessionEvent::SessionCreated { metadata, .. } => metadata,
        other => panic!("{other:?}"),
    }
}

/// Every text the model saw in request `index`, joined.
fn seen(server: &MockServer, index: usize) -> String {
    server.requests()[index].body.clone()
}

#[test]
fn exec_prints_final_assistant_text_and_session_id() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("Hello from the mock"))]);
    let dir = TestDir::new("exec-ok");
    dir.config(&config_body(&server, &dir.db()));

    let (stdout, session) = ok(dir.exec(&dir.path).arg("say hello"));

    assert_eq!(stdout, "Hello from the mock");
    assert_eq!(session.len(), 32, "hex session id: {session}");
    assert!(dir.db().exists(), "the sqlite database was created");
}

#[test]
fn model_flag_beats_override_beats_file() {
    let server = MockServer::start(vec![
        Reply::sse(common::text_sse("ok")),
        Reply::sse(common::text_sse("ok")),
        Reply::sse(common::text_sse("ok")),
    ]);
    let dir = TestDir::new("model-precedence");
    dir.config(&config_body(&server, &dir.db()));

    ok(dir.exec(&dir.path).arg("hi"));
    ok(dir.exec(&dir.path).args(["-c", "model.id=c-model", "hi"]));
    ok(dir
        .exec(&dir.path)
        .args(["-c", "model.id=c-model", "-m", "flag-model", "hi"]));

    let models: Vec<_> = server
        .requests()
        .iter()
        .map(|request| request.json_body()["model"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(models, ["claude-test", "c-model", "flag-model"]);
}

#[test]
fn exec_fails_with_one_when_the_turn_does_not_complete() {
    let server = MockServer::start(vec![Reply::sse(common::context_exceeded_sse("truncated"))]);
    let dir = TestDir::new("exec-failed");
    dir.config(&config_body(&server, &dir.db()));

    assert_eq!(code(dir.exec(&dir.path).arg("hi")), Some(1));
}

#[test]
fn removed_commands_and_flags_and_missing_model_are_usage_errors() {
    let dir = TestDir::new("usage");
    let config = dir.config("");
    let config = config.to_str().unwrap();

    for args in [
        &["run", "--config", config, "hi"][..],
        &["exec", "--config", config, "--provider", "anthropic", "hi"],
        &["exec", "--config", config, "--db", "x.db", "hi"],
        &["exec", "--config", config, "--approval", "ask", "hi"],
        &["exec", "--config", config, "--no-tools", "hi"],
        &["config", "check", "--config", config],
        // No `[model] provider` / `id`.
        &["exec", "--config", config, "hi"],
        &[
            "exec",
            "--config",
            config,
            "-c",
            "model.provider=anthropic",
            "hi",
        ],
        // `-r` without a value needs a terminal.
        &["exec", "--config", config, "-r"],
        &["exec", "--config", config, "--fork-session", "hi"],
        &["exec", "--config", config, "-r", "x", "-n", "name", "hi"],
        &["exec", "--config", config, "-C", "/does/not/exist", "hi"],
        &["exec", "--config", config, "-s", "no-sandbox", "hi"],
        &["exec", "--config", config, "-a", "on-request", "hi"],
    ] {
        let output = bin().current_dir(&dir.path).args(args).output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(2),
            "{args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn exec_reads_stdin_without_a_prompt_and_appends_piped_stdin_to_one() {
    let server = MockServer::start(vec![
        Reply::sse(common::text_sse("ok")),
        Reply::sse(common::text_sse("ok")),
    ]);
    let dir = TestDir::new("stdin");
    dir.config(&config_body(&server, &dir.db()));

    ok(dir.exec(&dir.path).write_stdin("from stdin only"));
    ok(dir
        .exec(&dir.path)
        .arg("explain this")
        .write_stdin("piped body"));

    assert!(seen(&server, 0).contains("from stdin only"));
    assert!(
        seen(&server, 1).contains(r"explain this\n\n<stdin>\npiped body\n</stdin>"),
        "{}",
        seen(&server, 1)
    );
    // Neither a prompt nor stdin: nothing to run.
    assert_eq!(code(dir.exec(&dir.path).write_stdin("")), Some(2));
}

#[test]
fn json_output_is_one_result_object() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("Hello"))]);
    let dir = TestDir::new("json");
    dir.config(&config_body(&server, &dir.db()));

    let (stdout, session) = ok(dir.exec(&dir.path).args(["--output-format", "json", "hi"]));

    let result: serde_json::Value = serde_json::from_str(&stdout).expect("one json object");
    assert_eq!(result["session"], session);
    assert_eq!(result["result"], "Hello");
    assert_eq!(result["reason"], "Completed");
    assert_eq!(result["usage"]["input"]["uncached"], 5, "{result}");
}

#[test]
fn stream_json_output_prints_each_event_then_the_result_object() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("Hello"))]);
    let dir = TestDir::new("stream-json");
    dir.config(&config_body(&server, &dir.db()));

    let (stdout, session) = ok(dir
        .exec(&dir.path)
        .args(["--output-format", "stream-json", "hi"]));

    let lines: Vec<serde_json::Value> = stdout
        .lines()
        .map(|line| serde_json::from_str(line).expect("one json value per line"))
        .collect();
    let types: Vec<&str> = lines[..lines.len() - 1]
        .iter()
        .map(|event| event["type"].as_str().expect("tagged event"))
        .collect();
    assert_eq!(types.first(), Some(&"turn_started"), "{types:?}");
    assert!(types.contains(&"model_delta"), "{types:?}");
    assert_eq!(types.last(), Some(&"turn_committed"), "{types:?}");
    let result = lines.last().unwrap();
    assert_eq!(result["session"], session);
    assert_eq!(result["result"], "Hello");
    assert_eq!(result["reason"], "Completed");
    assert_eq!(result["usage"]["input"]["uncached"], 5, "{result}");
}

/// `exec` in stream-json mode with an application on stdin. Every read times out, so
/// a hang fails the test; the child is killed on drop.
struct App {
    child: std::process::Child,
    stdin: Option<std::process::ChildStdin>,
    lines: std::sync::mpsc::Receiver<String>,
}

impl App {
    fn start(dir: &TestDir) -> Self {
        use std::io::BufRead;
        use std::process::Stdio;

        let mut child = common::process_bin()
            .current_dir(&dir.path)
            .args(["exec", "--config", dir.config_path().to_str().unwrap()])
            .args(["-s", "full-access", "--output-format", "stream-json"])
            .args(["--input-format", "stream-json"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn asterpath");
        let stdin = child.stdin.take();
        let stdout = std::io::BufReader::new(child.stdout.take().unwrap());
        let (tx, lines) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in stdout.lines() {
                let Ok(line) = line else { break };
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            stdin,
            lines,
        }
    }

    fn send(&mut self, line: &str) {
        use std::io::Write;
        writeln!(self.stdin.as_mut().expect("stdin open"), "{line}").unwrap();
    }

    fn close(&mut self) {
        self.stdin = None;
    }

    fn next(&self) -> serde_json::Value {
        let line = self
            .lines
            .recv_timeout(std::time::Duration::from_secs(20))
            .expect("a stdout line in time");
        serde_json::from_str(&line).expect("json line")
    }

    /// Skip to the next event of `kind`.
    fn until(&self, kind: &str) -> serde_json::Value {
        loop {
            let event = self.next();
            if event["type"] == kind {
                return event;
            }
        }
    }

    fn exit_code(&mut self) -> Option<i32> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status.code();
            }
            assert!(std::time::Instant::now() < deadline, "asterpath exits");
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }
}

impl Drop for App {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

const GO: &str = r#"{"type":"user_input","parts":[{"Text":{"text":"go"}}]}"#;

/// An application on stdin answers the `bash` approval; closing stdin while idle ends
/// the run with the result object.
#[test]
fn stream_json_input_round_trips_an_approval_through_the_application() {
    let server = MockServer::start(vec![
        Reply::sse(common::tool_use_sse("bash", r#"{"command":"touch ran"}"#)),
        Reply::sse(common::text_sse("done")),
    ]);
    let dir = TestDir::new("stream-input");
    dir.config(&config_body(&server, &dir.db()));
    let mut app = App::start(&dir);

    app.send(GO);
    let request = app.until("approval_requested");
    assert_eq!(request["tool"], "bash");
    assert!(!dir.path.join("ran").exists(), "asked before running");
    app.send(&format!(
        r#"{{"type":"approval","call":{},"decision":"Allow"}}"#,
        request["tool_call_id"]
    ));
    app.until("turn_committed");
    app.close();

    let result = app.next();
    assert_eq!(result["result"], "done");
    assert_eq!(result["reason"], "Completed");
    assert_eq!(app.exit_code(), Some(0));
    assert!(dir.path.join("ran").exists(), "the allowed bash ran");
}

/// Closing stdin while an approval waits cancels the turn instead of hanging.
#[test]
fn stream_json_stdin_closed_during_an_approval_cancels_the_turn() {
    let server = MockServer::start(vec![Reply::sse(common::tool_use_sse(
        "bash",
        r#"{"command":"touch ran"}"#,
    ))]);
    let dir = TestDir::new("stream-eof");
    dir.config(&config_body(&server, &dir.db()));
    let mut app = App::start(&dir);

    app.send(GO);
    app.until("approval_requested");
    app.close();

    assert_eq!(app.until("turn_committed")["reason"], "Cancelled");
    assert_eq!(app.next()["reason"], "Cancelled", "the result line");
    assert_eq!(app.exit_code(), Some(1));
    assert!(!dir.path.join("ran").exists());
}

/// A bad command cancels the open turn, still prints the result line, then
/// exits with a usage error.
#[test]
fn stream_json_bad_command_cancels_and_exits_two_after_the_result() {
    let server = MockServer::start(vec![Reply::sse(common::tool_use_sse(
        "bash",
        r#"{"command":"touch ran"}"#,
    ))]);
    let dir = TestDir::new("stream-bad");
    dir.config(&config_body(&server, &dir.db()));
    let mut app = App::start(&dir);

    app.send(GO);
    app.until("approval_requested");
    app.send(r#"{"type":"quit"}"#);

    assert_eq!(app.until("turn_committed")["reason"], "Cancelled");
    let result = app.next();
    assert_eq!(result["reason"], "Cancelled", "{result}");
    assert!(result.get("session").is_some(), "{result}");
    assert_eq!(app.exit_code(), Some(2));
    assert!(!dir.path.join("ran").exists());
}

/// The one `tool_finished` of a call that failed before it started, and
/// asserts no `tool_started` came before it.
fn pre_failed(events: &[serde_json::Value]) -> &serde_json::Value {
    let tool_events: Vec<_> = events
        .iter()
        .filter(|event| {
            matches!(
                event["type"].as_str(),
                Some("tool_started" | "tool_finished")
            )
        })
        .collect();
    // The call never ran, yet it is announced before it finishes.
    assert_eq!(tool_events.len(), 2, "{events:?}");
    let (started, finished) = (tool_events[0], tool_events[1]);
    assert_eq!(started["type"], "tool_started");
    assert_eq!(finished["type"], "tool_finished");
    assert_eq!(started["call"], finished["call"]);
    assert_eq!(finished["status"], "Failed");
    assert_eq!(finished["output"], serde_json::Value::Null);
    finished
}

/// The logged `ToolResult.error` of `call` (a JSON id) in `session`.
fn logged_error(dir: &TestDir, session: &str, call: &serde_json::Value) -> serde_json::Value {
    let session: asterpath_protocol::SessionId =
        serde_json::from_value(serde_json::json!(session)).expect("session id");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let asterpath = Asterpath::builder()
            .database(dir.db())
            .provider(Arc::new(AnthropicProvider::new("test-key")))
            .build()
            .await
            .expect("build");
        let entries = asterpath_protocol::scan_all(&*asterpath.store(), session)
            .await
            .expect("scan");
        asterpath.shutdown().await.expect("shutdown");
        entries
            .iter()
            .find_map(
                |entry| match asterpath_protocol::decode_entry(entry).ok()? {
                    SessionEvent::ToolResult {
                        tool_call_id,
                        error,
                        ..
                    } if serde_json::to_value(tool_call_id).unwrap() == *call => {
                        Some(serde_json::to_value(error).unwrap())
                    }
                    _ => None,
                },
            )
            .expect("a logged result for the call")
    })
}

fn stream_json_events(stdout: &str) -> Vec<serde_json::Value> {
    stdout
        .lines()
        .map(|line| serde_json::from_str(line).expect("one json value per line"))
        .collect()
}

#[test]
fn stream_json_reports_a_call_denied_without_an_endpoint() {
    let server = MockServer::start(vec![
        Reply::sse(common::tool_use_sse("bash", r#"{"command":"touch ran"}"#)),
        Reply::sse(common::text_sse("done")),
    ]);
    let dir = TestDir::new("stream-no-endpoint");
    dir.config(&config_body(&server, &dir.db()));

    let (stdout, session) = ok(dir.exec(&dir.path).args([
        "-a",
        "untrusted",
        "-s",
        "full-access",
        "--output-format",
        "stream-json",
        "go",
    ]));

    let events = stream_json_events(&stdout);
    let finished = pre_failed(&events);
    assert_eq!(finished["error"]["kind"], "Denied");
    assert_eq!(
        finished["error"]["message"],
        asterpath_protocol::NO_APPROVAL_ENDPOINT
    );
    assert_eq!(
        finished["error"],
        logged_error(&dir, &session, &finished["call"])
    );
}

#[test]
fn stream_json_reports_a_call_the_application_denied() {
    let server = MockServer::start(vec![
        Reply::sse(common::tool_use_sse("bash", r#"{"command":"touch ran"}"#)),
        Reply::sse(common::text_sse("done")),
    ]);
    let dir = TestDir::new("stream-deny");
    dir.config(&config_body(&server, &dir.db()));
    let mut app = App::start(&dir);

    app.send(GO);
    let request = app.until("approval_requested");
    app.send(&format!(
        r#"{{"type":"approval","call":{},"decision":{{"Deny":{{"reason":"e2e says no"}}}}}}"#,
        request["tool_call_id"]
    ));
    let mut events = Vec::new();
    loop {
        let event = app.next();
        if event["type"] == "turn_committed" {
            break;
        }
        events.push(event);
    }
    app.close();
    let session = app.next()["session"].as_str().unwrap().to_owned();
    assert_eq!(app.exit_code(), Some(0));

    let finished = pre_failed(&events);
    assert_eq!(finished["call"], request["tool_call_id"]);
    assert_eq!(finished["error"]["kind"], "Denied");
    assert!(
        finished["error"]["message"]
            .as_str()
            .unwrap()
            .contains("e2e says no"),
        "{finished}"
    );
    assert_eq!(
        finished["error"],
        logged_error(&dir, &session, &finished["call"])
    );
    assert!(!dir.path.join("ran").exists());
}

#[test]
fn stream_json_reports_a_call_to_an_unregistered_tool() {
    let server = MockServer::start(vec![
        Reply::sse(common::tool_use_sse("no_such_tool", "{}")),
        Reply::sse(common::text_sse("done")),
    ]);
    let dir = TestDir::new("stream-not-found");
    dir.config(&config_body(&server, &dir.db()));

    let (stdout, session) = ok(dir
        .exec(&dir.path)
        .args(["--output-format", "stream-json", "go"]));

    let events = stream_json_events(&stdout);
    let finished = pre_failed(&events);
    assert_eq!(finished["error"]["kind"], "NotFound");
    assert_eq!(
        finished["error"],
        logged_error(&dir, &session, &finished["call"])
    );
}

#[test]
fn stream_json_input_needs_stream_json_output() {
    let dir = TestDir::new("stream-input-usage");
    let server = MockServer::start(Vec::new());
    dir.config(&config_body(&server, &dir.db()));
    assert_eq!(
        code(dir.exec(&dir.path).args(["--input-format", "stream-json"])),
        Some(2)
    );
}

#[test]
fn output_schema_constrains_the_request() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("{}"))]);
    let dir = TestDir::new("schema");
    dir.config(&config_body(&server, &dir.db()));
    let schema = dir.path.join("schema.json");
    fs::write(
        &schema,
        r#"{"type":"object","properties":{"verdict_field":{"type":"string"}}}"#,
    )
    .unwrap();

    ok(dir.exec(&dir.path).args(["-m", "claude-fable-5-1"]).args([
        "--output-schema",
        schema.to_str().unwrap(),
        "hi",
    ]));

    assert!(
        seen(&server, 0).contains("verdict_field"),
        "{}",
        seen(&server, 0)
    );
}

#[test]
fn output_mode_needs_an_output_schema() {
    let server = MockServer::start(Vec::new());
    let dir = TestDir::new("mode-usage");
    dir.config(&config_body(&server, &dir.db()));
    assert_eq!(
        code(dir.exec(&dir.path).args(["--output-mode", "tool", "hi"])),
        Some(2)
    );
    assert!(server.requests().is_empty());
}

/// Tool mode on either protocol: the output tool and a forced call replace
/// the native constraint, and the tool's arguments are the result.
#[test]
fn output_mode_tool_returns_the_output_tool_arguments() {
    let args = r#"{"city":"Paris"}"#;
    for (protocol, reply, tool_name, strict, choice, native) in [
        (
            "anthropic",
            common::tool_use_sse("output", args),
            "/name",
            "/strict",
            ("/tool_choice/type", "any"),
            "/output_config",
        ),
        (
            "openai",
            common::openai_call_sse("output", args),
            "/name",
            "/strict",
            ("/tool_choice", "required"),
            "/text/format",
        ),
    ] {
        let server = MockServer::start(vec![Reply::sse(reply)]);
        let dir = TestDir::new("mode-tool");
        dir.config(&vendor_protocol_config(
            protocol,
            &server,
            &dir.db(),
            "[providers.ds.profile]\nforced_tool_choice = true\n",
        ));
        let schema = dir.path.join("schema.json");
        fs::write(&schema, r#"{"type":"object"}"#).unwrap();

        let (stdout, _) = ok(dir
            .exec(&dir.path)
            .env("DEEPSEEK_API_KEY", "ds-key")
            .args(["--output-mode", "tool"])
            .args(["--output-schema", schema.to_str().unwrap()])
            .args(["--output-format", "json", "hi"]));

        let result: serde_json::Value = serde_json::from_str(&stdout).expect("json result");
        assert_eq!(result["result"], args, "{protocol}");
        assert_eq!(result["reason"], "Completed", "{protocol}");
        let requests = server.requests();
        assert_eq!(requests.len(), 1, "{protocol}");
        let body = requests[0].json_body();
        let tools = body["tools"].as_array().expect("tools");
        let output = serde_json::json!("output");
        let tool = tools
            .iter()
            .find(|tool| tool.pointer(tool_name) == Some(&output))
            .unwrap_or_else(|| panic!("no output tool: {body}"));
        assert_eq!(
            tool.pointer(strict),
            Some(&serde_json::json!(true)),
            "{body}"
        );
        assert_eq!(
            body.pointer(choice.0),
            Some(&serde_json::json!(choice.1)),
            "{body}"
        );
        assert_eq!(body.pointer(native), None, "{body}");
    }
}

#[test]
fn output_mode_tool_fails_when_the_model_only_replies_with_text() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("just text"))]);
    let dir = TestDir::new("mode-text");
    dir.config(&config_body(&server, &dir.db()));
    let schema = dir.path.join("schema.json");
    fs::write(&schema, r#"{"type":"object"}"#).unwrap();

    let output = dir
        .exec(&dir.path)
        .args(["-c", "providers.anthropic.profile.forced_tool_choice=true"])
        .args(["--output-mode", "tool"])
        .args(["--output-schema", schema.to_str().unwrap(), "hi"])
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("did not call the output tool"), "{stderr}");
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "just text");
}

/// A schema would stay in a continued session's config after this exec, so
/// it is refused there; a fork is a new session and takes it.
#[test]
fn output_schema_on_a_continued_session_is_a_usage_error() {
    let server = MockServer::start(vec![
        Reply::sse(common::text_sse("first")),
        Reply::sse(common::text_sse("{}")),
    ]);
    let dir = TestDir::new("schema-resume");
    dir.config(&config_body(&server, &dir.db()));
    let schema = dir.path.join("schema.json");
    fs::write(&schema, r#"{"type":"object"}"#).unwrap();
    let (_, session) = ok(dir.exec(&dir.path).arg("hi"));

    let schema = schema.to_str().unwrap();
    assert_eq!(
        code(
            dir.exec(&dir.path)
                .args(["-r", &session, "--output-schema", schema, "again"])
        ),
        Some(2)
    );
    assert_eq!(server.requests().len(), 1, "nothing was sent");
    ok(dir.exec(&dir.path).args([
        "-r",
        &session,
        "--fork-session",
        "-m",
        "claude-fable-5-1",
        "--output-schema",
        schema,
        "again",
    ]));
}

#[test]
fn image_flag_attaches_the_file_to_the_first_input() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("a picture"))]);
    let dir = TestDir::new("image");
    dir.config(&config_body(&server, &dir.db()));
    fs::write(dir.path.join("pic.png"), b"abc").unwrap();

    ok(dir.exec(&dir.path).args(["-i", "pic.png", "what is this"]));

    let body = seen(&server, 0);
    assert!(
        body.contains("image/png") && body.contains("YWJj"),
        "{body}"
    );
    assert_eq!(
        code(dir.exec(&dir.path).args(["-i", "notes.txt", "hi"])),
        Some(2)
    );
}

#[test]
fn a_new_session_reads_agents_md_from_the_work_root() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("ok"))]);
    let dir = TestDir::new("agents-md");
    dir.config(&config_body(&server, &dir.db()));
    let project = dir.sub("project");
    fs::write(project.join("AGENTS.md"), "Always answer in haiku.").unwrap();

    // `-C` picks the work root regardless of the process directory.
    ok(dir
        .exec(&dir.path)
        .args(["-C", project.to_str().unwrap(), "hi"]));

    let body = server.requests()[0].json_body();
    assert!(
        body["system"]
            .to_string()
            .contains("Always answer in haiku."),
        "{body}"
    );
}

#[test]
fn agent_selects_a_profile_and_agents_lists_where_each_comes_from() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("ok"))]);
    let dir = TestDir::new("agent");
    dir.config(&config_body(&server, &dir.db()));
    let user = dir.sub("xdg/asterpath/agents");
    let project = dir.sub("project/.asterpath/agents");
    fs::write(
        user.join("code.md"),
        "---\ndescription: writes code\n---\nuser code",
    )
    .unwrap();
    fs::write(user.join("review.md"), "user review").unwrap();
    fs::write(project.join("review.md"), "Review in haiku.").unwrap();
    let root = dir.path.join("project");
    let xdg = dir.path.join("xdg");

    ok(dir
        .exec(&root)
        .env("XDG_CONFIG_HOME", &xdg)
        .args(["--agent", "review", "hi"]));
    let body = server.requests()[0].json_body();
    assert!(
        body["system"].to_string().contains("Review in haiku."),
        "{body}"
    );

    assert_eq!(
        code(
            dir.exec(&root)
                .env("XDG_CONFIG_HOME", &xdg)
                .args(["--agent", "missing", "hi"])
        ),
        Some(2)
    );

    let output = bin()
        .current_dir(&root)
        .env("XDG_CONFIG_HOME", &xdg)
        .args(["agents", "--config", dir.config_path().to_str().unwrap()])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = stdout.trim().lines().collect();
    assert_eq!(lines.len(), 3, "{stdout}");
    assert_eq!(
        lines[0],
        format!("code\t{}\twrites code", user.join("code.md").display())
    );
    // `default` comes from the repository's source directory in a dev build;
    // its description is the builtin text, so only the prefix is fixed here.
    assert!(
        lines[1].starts_with(&format!(
            "default\t{}/agents/default.md\t",
            env!("CARGO_MANIFEST_DIR")
        )),
        "{stdout}"
    );
    assert_eq!(
        lines[2],
        format!("review\t{}", project.join("review.md").display())
    );
}

#[test]
fn a_broken_profile_is_reported_when_a_session_is_created_not_when_it_continues() {
    let server = MockServer::start(vec![
        Reply::sse(common::text_sse("one")),
        Reply::sse(common::text_sse("two")),
    ]);
    let dir = TestDir::new("broken-agent");
    dir.config(&config_body(&server, &dir.db()));
    let project = dir.sub("project/.asterpath/agents");
    fs::write(project.join("broken.md"), "---\nmood: grumpy\n---\nbody").unwrap();
    let root = dir.path.join("project");
    let stderr = |output: std::process::Output| {
        assert!(output.status.success(), "{output:?}");
        String::from_utf8_lossy(&output.stderr).into_owned()
    };

    let created = stderr(dir.exec(&root).arg("hi").output().unwrap());
    assert_eq!(created.matches("broken.md").count(), 1, "{created}");
    let session = session_from_stderr(&created);
    let continued = stderr(
        dir.exec(&root)
            .args(["-r", &session, "again"])
            .output()
            .unwrap(),
    );
    assert!(!continued.contains("broken.md"), "{continued}");
}

#[test]
fn styles_lists_the_builtin_styles() {
    let dir = TestDir::new("builtin-styles");
    dir.config("");
    let root = dir.sub("project");
    let output = bin()
        .current_dir(&root)
        .env("XDG_CONFIG_HOME", dir.sub("xdg"))
        .args(["styles", "--config", dir.config_path().to_str().unwrap()])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    // A dev build reads the repository's source directory.
    let builtin = format!("{}/styles", env!("CARGO_MANIFEST_DIR"));
    assert_eq!(
        stdout.trim().lines().collect::<Vec<_>>(),
        [
            format!(
                "explanatory\t{builtin}/explanatory.md\tExplains the reasoning behind its work as it goes"
            ),
            format!(
                "learning\t{builtin}/learning.md\tLeaves small, meaningful pieces of code for you to write, and explains as it goes"
            ),
        ]
    );
}

#[test]
fn exec_takes_only_an_explicit_style_and_a_continued_session_keeps_its_own() {
    let server = MockServer::start(vec![
        Reply::sse(common::text_sse("one")),
        Reply::sse(common::text_sse("two")),
        Reply::sse(common::text_sse("three")),
    ]);
    let dir = TestDir::new("style");
    dir.config(&format!(
        "output_style = \"warm\"\n{}",
        config_body(&server, &dir.db())
    ));
    let user = dir.sub("xdg/asterpath/styles");
    let project = dir.sub("project/.asterpath/styles");
    fs::write(
        user.join("warm.md"),
        "---\ndescription: friendly\n---\nBe warm.",
    )
    .unwrap();
    fs::write(project.join("terse.md"), "Be terse.").unwrap();
    let root = dir.path.join("project");
    let xdg = dir.path.join("xdg");
    let framing = "The output style below takes precedence";

    // exec ignores `output_style`.
    let (_, plain) = ok(dir.exec(&root).env("XDG_CONFIG_HOME", &xdg).arg("hi"));
    assert!(!seen(&server, 0).contains(framing), "{}", seen(&server, 0));
    assert_eq!(created_metadata(&dir.db(), &plain).get("style"), None);

    let (_, id) = ok(dir
        .exec(&root)
        .env("XDG_CONFIG_HOME", &xdg)
        .args(["--style", "terse", "hi"]));
    assert!(seen(&server, 1).contains(framing), "{}", seen(&server, 1));
    assert!(
        seen(&server, 1).contains("Be terse."),
        "{}",
        seen(&server, 1)
    );
    assert_eq!(
        created_metadata(&dir.db(), &id)
            .get("style")
            .map(String::as_str),
        Some("terse")
    );

    // Changing the file does not change the continued session's system.
    fs::write(project.join("terse.md"), "Be verbose.").unwrap();
    ok(dir
        .exec(&root)
        .env("XDG_CONFIG_HOME", &xdg)
        .args(["-r", &id, "again"]));
    assert!(
        seen(&server, 2).contains("Be terse."),
        "{}",
        seen(&server, 2)
    );
    assert!(!seen(&server, 2).contains("Be verbose."));

    assert_eq!(
        code(
            dir.exec(&root)
                .env("XDG_CONFIG_HOME", &xdg)
                .args(["--style", "missing", "hi"])
        ),
        Some(2)
    );
    assert_eq!(
        code(
            dir.exec(&root)
                .env("XDG_CONFIG_HOME", &xdg)
                .args(["--style", "terse", "-r", &id, "hi"])
        ),
        Some(2)
    );

    let output = bin()
        .current_dir(&root)
        .env("XDG_CONFIG_HOME", &xdg)
        .args(["styles", "--config", dir.config_path().to_str().unwrap()])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Builtin styles have their own test.
    let builtin = format!("\t{}/styles/", env!("CARGO_MANIFEST_DIR"));
    let lines: Vec<&str> = stdout
        .trim()
        .lines()
        .filter(|line| !line.contains(&builtin))
        .collect();
    assert_eq!(
        lines,
        [
            format!("terse\t{}", project.join("terse.md").display()),
            format!("warm\t{}\tfriendly", user.join("warm.md").display()),
        ],
        "{stdout}"
    );
}

#[test]
fn continue_picks_the_last_active_session_of_the_work_root() {
    let server = MockServer::start(vec![
        Reply::sse(common::text_sse("one")),
        Reply::sse(common::text_sse("two")),
        Reply::sse(common::text_sse("elsewhere")),
        Reply::sse(common::text_sse("continued")),
    ]);
    let dir = TestDir::new("continue");
    dir.config(&config_body(&server, &dir.db()));
    let a = dir.sub("a");
    let b = dir.sub("b");

    ok(dir.exec(&a).arg("first in a"));
    let (_, latest) = ok(dir.exec(&a).arg("second in a"));
    ok(dir.exec(&b).arg("only in b"));
    let (stdout, session) = ok(dir.exec(&a).args(["--continue", "more"]));

    assert_eq!(stdout, "continued");
    assert_eq!(session, latest);
    let body = seen(&server, 3);
    assert!(
        body.contains("second in a") && body.contains("more"),
        "{body}"
    );
    assert!(!body.contains("first in a") && !body.contains("only in b"));
    // A directory without sessions has nothing to continue.
    let empty = dir.sub("empty");
    assert_eq!(code(dir.exec(&empty).args(["--continue", "x"])), Some(1));
}

#[test]
fn resume_finds_a_session_by_id_or_by_name() {
    let server = MockServer::start(vec![
        Reply::sse(common::text_sse("named")),
        Reply::sse(common::text_sse("by name")),
        Reply::sse(common::text_sse("by id")),
    ]);
    let dir = TestDir::new("resume-name");
    dir.config(&config_body(&server, &dir.db()));
    let (_, named) = ok(dir.exec(&dir.path).args(["-n", "bugfix", "remember 42"]));

    let (stdout, session) = ok(dir.exec(&dir.path).args(["-r", "bugfix", "and?"]));
    assert_eq!(
        (stdout.as_str(), session.as_str()),
        ("by name", named.as_str())
    );
    // From another directory: a name matches only the work root, an id anywhere.
    let other = dir.sub("other");
    assert_eq!(code(dir.exec(&other).args(["-r", "bugfix", "x"])), Some(1));
    let (_, session) = ok(dir.exec(&other).args(["-r", &named, "again"]));
    assert_eq!(session, named);
    assert!(seen(&server, 2).contains("remember 42"));

    assert_eq!(code(dir.exec(&other).args(["-r", "nope", "x"])), Some(1));
    assert_eq!(
        code(
            dir.exec(&other)
                .args(["-r", "00000000000000000000000000000000", "x"])
        ),
        Some(1)
    );
}

#[test]
fn a_continued_session_runs_in_its_recorded_work_root() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("ok"))]);
    let dir = TestDir::new("resume-root");
    dir.config(&config_body(&server, &dir.db()));
    let project = dir.sub("project");
    let (_, session) = ok(dir.exec(&project).arg("hi"));

    fs::remove_dir_all(&project).unwrap();
    // The recorded `cwd` is gone: a configuration error, not a silent fallback.
    assert_eq!(code(dir.exec(&dir.path).args(["-r", &session])), Some(2));
    // An explicit `-C` overrides the recorded directory.
    let (stdout, _) =
        ok(dir
            .exec(&dir.path)
            .args(["-C", dir.path.to_str().unwrap(), "-r", &session]));
    assert_eq!(stdout, "idle");
}

#[test]
fn fork_session_copies_the_history_into_a_new_session() {
    let server = MockServer::start(vec![
        Reply::sse(common::text_sse("source")),
        Reply::sse(common::text_sse("forked")),
        Reply::sse(common::text_sse("source again")),
    ]);
    let dir = TestDir::new("fork");
    dir.config(&config_body(&server, &dir.db()));
    let (_, source) = ok(dir.exec(&dir.path).arg("the secret is 7"));

    let (stdout, fork) = ok(dir.exec(&dir.path).args([
        "-r",
        &source,
        "--fork-session",
        "-n",
        "branch",
        "fork turn",
    ]));
    assert_eq!(stdout, "forked");
    assert_ne!(fork, source);
    assert!(seen(&server, 1).contains("the secret is 7"));

    // The source never saw the fork's turn; the fork is now the last active.
    ok(dir.exec(&dir.path).args(["-r", &source, "back"]));
    assert!(!seen(&server, 2).contains("fork turn"));
    let output = dir.exec(&dir.path).args(["-r", "branch"]).output().unwrap();
    assert_eq!(
        session_from_stderr(&String::from_utf8_lossy(&output.stderr)),
        fork
    );
}

#[test]
fn model_flag_on_a_continued_session_switches_its_model() {
    let server = MockServer::start(vec![
        Reply::sse(common::text_sse("ok")),
        Reply::sse(common::text_sse("ok")),
    ]);
    let dir = TestDir::new("resume-model");
    dir.config(&config_body(&server, &dir.db()));
    let (_, session) = ok(dir.exec(&dir.path).arg("hi"));

    ok(dir
        .exec(&dir.path)
        .args(["-r", &session, "-m", "claude-next", "again"]));

    assert_eq!(server.requests()[1].json_body()["model"], "claude-next");
}

#[test]
fn resume_without_a_prompt_prints_idle_for_a_finished_session() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("Hello"))]);
    let dir = TestDir::new("resume-idle");
    dir.config(&config_body(&server, &dir.db()));
    let (_, session) = ok(dir.exec(&dir.path).arg("hi"));

    let (stdout, _) = ok(dir.exec(&dir.path).args(["-r", &session]));
    assert_eq!(stdout, "idle");
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_of_a_session_driven_elsewhere_falls_back_to_a_read_only_view() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("Hello"))]);
    let dir = TestDir::new("resume-held");
    dir.config(&config_body(&server, &dir.db()));
    let (_, session) = ok(dir.exec(&dir.path).arg("hi"));
    // Another process (this test) owns the session's tree (ADR-0036).
    let store = asterpath_storage_sqlite::open(dir.db()).expect("open the database");
    let id: asterpath_protocol::SessionId =
        serde_json::from_value(serde_json::json!(session)).expect("session id");
    let _claim = store.claim(id).await.expect("claim");

    let output = dir.exec(&dir.path).args(["-r", &session]).output().unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "stderr: {stderr}");
    assert!(stderr.contains("read-only"), "stderr: {stderr}");
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "Hello");

    assert_eq!(
        code(dir.exec(&dir.path).args(["-r", &session, "more"])),
        Some(1)
    );
    assert_eq!(
        server.requests().len(),
        1,
        "nothing ran in the second process"
    );
}

#[test]
fn resume_recovers_an_open_turn_before_new_input() {
    let server = MockServer::start(vec![
        Reply::sse(common::text_sse("Resumed turn")),
        Reply::sse(common::text_sse("Recovered first")),
        Reply::sse(common::text_sse("New input ran")),
    ]);
    let (dir, session) = prepared_session("resume-open", &server, true, "anthropic");

    let (stdout, _) = ok(dir.exec(&dir.path).args(["-r", &session]));
    assert_eq!(stdout, "Resumed turn");

    let (dir, session) = prepared_session("resume-open-input", &server, true, "anthropic");
    let (stdout, _) = ok(dir.exec(&dir.path).args(["-r", &session, "next"]));
    assert_eq!(stdout, "New input ran");
    let requests = server.requests();
    assert_eq!(requests.len(), 3, "recovery, then the new input");
    assert!(!requests[1].body.contains("next") && requests[2].body.contains("next"));
}

#[test]
fn resume_continues_pending_input() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("Pending input ran"))]);
    let (dir, session) = prepared_session("resume-pending", &server, false, "anthropic");

    let (stdout, _) = ok(dir.exec(&dir.path).args(["-r", &session]));
    assert_eq!(stdout, "Pending input ran");
}

#[test]
fn untrusted_denies_mutating_tools_in_exec_and_never_runs_them() {
    for (flags, ran) in [
        (&["-s", "full-access"][..], false),
        (&["-a", "never", "-s", "full-access"], true),
        (
            &["-c", "approval.default=\"never\"", "-s", "full-access"],
            true,
        ),
        (
            &[
                "-c",
                "approval.default=\"never\"",
                "-a",
                "untrusted",
                "-s",
                "full-access",
            ],
            false,
        ),
    ] {
        let server = MockServer::start(vec![
            Reply::sse(common::tool_use_sse("bash", r#"{"command":"touch ran"}"#)),
            Reply::sse(common::text_sse("done")),
        ]);
        let dir = TestDir::new("approval");
        dir.config(&config_body(&server, &dir.db()));
        ok(dir.exec(&dir.path).args(flags).arg("go"));
        assert_eq!(dir.path.join("ran").exists(), ran, "{flags:?}");
    }
}

/// `-C` roots bash in the work root without moving the process, and bash
/// sees only the minimal inherited environment plus `[sandbox] env`
/// (ADR-0037).
#[test]
fn bash_runs_in_the_work_root_without_the_process_environment() {
    let server = MockServer::start(vec![
        Reply::sse(common::tool_use_sse(
            "bash",
            r#"{"command":"pwd > where.txt; echo \"${ASTERPATH_LEAK-unset} ${ASTERPATH_PASS-unset}\" >> where.txt"}"#,
        )),
        Reply::sse(common::text_sse("done")),
    ]);
    let dir = TestDir::new("bash-env");
    dir.config(&format!(
        "{}\n[sandbox]\nenv = [\"ASTERPATH_PASS\"]\n",
        config_body(&server, &dir.db())
    ));
    let project = dir.sub("project");

    ok(dir
        .exec(&dir.path)
        .env("ASTERPATH_LEAK", "secret")
        .env("ASTERPATH_PASS", "passed")
        .args(["-a", "never", "-s", "full-access", "-C"])
        .arg(&project)
        .arg("go"));

    assert_eq!(
        fs::read_to_string(project.join("where.txt")).unwrap(),
        format!(
            "{}\nunset passed\n",
            project.canonicalize().unwrap().display()
        )
    );
}

#[test]
fn doctor_validates_without_initialising_modules() {
    let dir = TestDir::new("doctor-ok");
    let db = dir.db();
    let config = dir.config(&format!(
        "[storage]\npath = \"{}\"\n\n[model_aliases.fast]\nprovider = \"anthropic\"\nmodel = \"claude-test\"\n",
        db.display()
    ));

    bin()
        .args(["doctor", "--config", config.to_str().unwrap()])
        .assert()
        .success();

    assert!(!db.exists(), "doctor must not open the database");
}

#[test]
fn doctor_reports_a_missing_sandbox_backend_with_its_reason() {
    let dir = TestDir::new("doctor-sandbox");
    let config = dir.config(&format!("[storage]\npath = \"{}\"\n", dir.db().display()));

    let output = bin()
        .env("PATH", "")
        .args(["doctor", "--config", config.to_str().unwrap()])
        .output()
        .expect("doctor");

    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    let line = stdout
        .lines()
        .find(|line| line.starts_with("sandbox: "))
        .unwrap_or_else(|| panic!("no sandbox line in {stdout}"));
    if cfg!(target_os = "linux") {
        assert_eq!(line, "sandbox: none, bwrap not found: bwrap");
    } else {
        assert!(line.starts_with("sandbox: none, "), "{line}");
    }
}

#[test]
fn doctor_rejects_bad_configs_with_two() {
    for (body, overrides) in [
        ("[turn]\nmax_steps_per_turn = 0\n", &[][..]),
        ("[model]\napi_key = \"leaked\"\n", &[]),
        ("[providers.anthropic]\nsecret = \"leaked\"\n", &[]),
        ("[providers.anthropic]\ntoken = \"leaked\"\n", &[]),
        ("", &["-c", "providers.anthropic.api_key=leaked"]),
        ("", &["-c", "turn.max_steps_per_turn=0"]),
        ("", &["-c", "turn.mystery=1"]),
        ("", &["-c", "model.provider=nope"]),
        (
            "[model_aliases.fast]\nprovider = \"nope\"\nmodel = \"m\"\n",
            &[],
        ),
    ] {
        let dir = TestDir::new("doctor-bad");
        let config = dir.config(body);
        let output = bin()
            .args(["doctor", "--config", config.to_str().unwrap()])
            .args(overrides)
            .output()
            .expect("run doctor");
        assert_eq!(
            output.status.code(),
            Some(2),
            "{body} {overrides:?} must be rejected: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn an_override_beats_the_file() {
    let dir = TestDir::new("override");
    let config = dir.config("[turn]\nmax_steps_per_turn = 0\n");

    bin()
        .args(["doctor", "--config", config.to_str().unwrap()])
        .args(["-c", "turn.max_steps_per_turn=3"])
        .assert()
        .success();
}

#[test]
fn config_flag_beats_the_environment_which_beats_the_default_path() {
    let dir = TestDir::new("config-precedence");
    let invalid = dir.config("[turn]\nmax_steps_per_turn = 0\n");
    let valid = dir.path.join("valid.toml");
    fs::write(&valid, "[turn]\nmax_steps_per_turn = 3\n").expect("write valid config");
    let xdg = dir.sub("xdg");
    fs::create_dir_all(xdg.join("asterpath")).unwrap();
    fs::copy(&invalid, xdg.join("asterpath/config.toml")).unwrap();

    let doctor = || {
        let mut command = bin();
        command.env("XDG_CONFIG_HOME", &xdg).arg("doctor");
        command
    };
    // The default path is read when nothing else names a file.
    assert_eq!(code(&mut doctor()), Some(2));
    doctor().env("ASTERPATH_CONFIG", &valid).assert().success();
    doctor()
        .env("ASTERPATH_CONFIG", &valid)
        .args(["--config", invalid.to_str().unwrap()])
        .assert()
        .code(2);
    doctor()
        .env("ASTERPATH_CONFIG", &invalid)
        .args(["--config", valid.to_str().unwrap()])
        .assert()
        .success();
    // A named file must exist; only the default path may be missing.
    let missing = dir.path.join("missing.toml");
    doctor()
        .args(["--config", missing.to_str().unwrap()])
        .assert()
        .code(2);
    doctor().env("ASTERPATH_CONFIG", &missing).assert().code(2);
    assert!(!missing.exists(), "a named file is never created");
}

#[test]
fn a_missing_database_directory_is_created() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("hi"))]);
    let dir = TestDir::new("db-dir");
    let db = dir.path.join("data/asterpath/sessions.db");
    dir.config(&config_body(&server, &db));
    ok(dir.exec(&dir.path).arg("hello"));
    assert!(db.exists());
}

#[test]
fn first_run_writes_an_owner_only_template() {
    use std::os::unix::fs::PermissionsExt;
    let dir = TestDir::new("first-run");
    let xdg = dir.path.join("xdg");
    bin()
        .env("XDG_CONFIG_HOME", &xdg)
        .arg("doctor")
        .assert()
        .success();
    let file = xdg.join("asterpath/config.toml");
    let mode = |path: &Path| fs::metadata(path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode(&file), 0o600);
    assert_eq!(mode(&xdg.join("asterpath")), 0o700);
    assert!(fs::read_to_string(&file).unwrap().contains("# api_key"));
}

#[test]
fn a_template_that_cannot_be_written_is_only_a_hint() {
    // `common::bin` points the default path under an unwritable directory.
    let output = bin().arg("doctor").output().unwrap();
    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("cannot create"), "{stderr}");
}

/// `config_body` plus a key in the file, owner-only.
fn keyed_config(dir: &TestDir, server: &MockServer) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let body = config_body(server, &dir.db()).replace(
        "[providers.anthropic]\n",
        "[providers.anthropic]\napi_key = \"file-key-123\"\n",
    );
    let path = dir.config(&body);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    path
}

#[test]
fn the_environment_key_beats_the_file_key() {
    let server = MockServer::start(vec![
        Reply::sse(common::text_sse("one")),
        Reply::sse(common::text_sse("two")),
    ]);
    let dir = TestDir::new("file-key");
    keyed_config(&dir, &server);

    ok(dir
        .exec(&dir.path)
        .env_remove("ANTHROPIC_API_KEY")
        .arg("hi"));
    ok(dir.exec(&dir.path).arg("hi"));
    let requests = server.requests();
    assert!(
        requests[0].head.contains("file-key-123"),
        "{}",
        requests[0].head
    );
    assert!(
        requests[1].head.contains("test-key"),
        "{}",
        requests[1].head
    );
}

#[test]
fn a_key_file_open_to_others_is_refused() {
    use std::os::unix::fs::PermissionsExt;
    let server = MockServer::start(Vec::new());
    let dir = TestDir::new("key-mode");
    let path = keyed_config(&dir, &server);
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    let output = bin()
        .args(["doctor", "--config", path.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("chmod 600"), "{stderr}");
    assert!(!stderr.contains("file-key-123"), "{stderr}");
    // Without a key the mode is not checked.
    let plain = dir.config("[turn]\nmax_steps_per_turn = 3\n");
    fs::set_permissions(&plain, fs::Permissions::from_mode(0o644)).unwrap();
    bin()
        .args(["doctor", "--config", plain.to_str().unwrap()])
        .assert()
        .success();
}

#[test]
fn doctor_shows_key_sources_not_keys() {
    let server = MockServer::start(Vec::new());
    let dir = TestDir::new("key-doctor");
    let path = keyed_config(&dir, &server);
    let doctor = || {
        let mut command = bin();
        command.args(["doctor", "--config", path.to_str().unwrap()]);
        command
    };
    let output = doctor().env_remove("ANTHROPIC_API_KEY").output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("provider anthropic: key file"), "{stdout}");
    assert!(!stdout.contains("file-key-123"), "{stdout}");
    let output = doctor().output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("provider anthropic: key env:ANTHROPIC_API_KEY"),
        "{stdout}"
    );
    assert!(!stdout.contains("test-key"), "{stdout}");
}

#[test]
fn debug_prompt_input_prints_the_projection_and_open_intents() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("Hello from the mock"))]);
    let dir = TestDir::new("prompt-input");
    let config = dir.config(&config_body(&server, &dir.db()));
    let (_, session) = ok(dir.exec(&dir.path).arg("hi"));

    let output = bin()
        .args([
            "debug",
            "prompt-input",
            "--config",
            config.to_str().unwrap(),
            &session,
        ])
        .output()
        .expect("run debug");
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).expect("json");
    let items = value["items"].as_array().expect("items");
    assert_eq!(items.len(), 2, "{value}");
    assert_eq!(items[0]["Message"]["parts"][0]["Text"]["text"], "hi");
    assert_eq!(
        items[1]["Message"]["parts"][0]["Text"]["text"],
        "Hello from the mock"
    );
    assert_eq!(value["open_intents"], serde_json::json!([]));
}

#[test]
fn debug_trace_prints_the_spans() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("Hello"))]);
    let dir = TestDir::new("trace");
    let config = dir.config(&config_body(&server, &dir.db()));
    let (_, session) = ok(dir.exec(&dir.path).arg("hi"));

    let output = bin()
        .args([
            "debug",
            "trace",
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
}

#[test]
fn debug_of_an_unknown_session_exits_one() {
    let dir = TestDir::new("debug-missing");
    let config = dir.config(&format!("[storage]\npath = \"{}\"\n", dir.db().display()));

    for command in ["prompt-input", "trace"] {
        bin()
            .args([
                "debug",
                command,
                "--config",
                config.to_str().unwrap(),
                "00000000000000000000000000000000",
            ])
            .assert()
            .code(1);
    }
}

#[test]
fn a_vendor_provider_uses_its_own_endpoint_key_and_profile() {
    let anthropic = MockServer::start(Vec::new());
    let deepseek = MockServer::start(vec![Reply::sse(common::text_sse("from deepseek"))]);
    let dir = TestDir::new("vendor");
    dir.config(&vendor_config(&anthropic, &deepseek, &dir.db()));

    let (stdout, _) = ok(dir.exec(&dir.path).env("DEEPSEEK_API_KEY", "ds-key").args([
        "-c",
        "model.provider=deepseek",
        "-m",
        "deepseek-flash",
        "hi",
    ]));
    assert_eq!(stdout, "from deepseek");
    assert!(anthropic.requests().is_empty());
    let requests = deepseek.requests();
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0].head.contains("x-api-key: ds-key"),
        "{}",
        requests[0].head
    );
    // The injected profile accepts `off` and disables default-on thinking.
    let body = requests[0].json_body();
    assert_eq!(body["thinking"], serde_json::json!({"type": "disabled"}));
}

#[test]
fn resume_rebuilds_a_vendor_provider_from_the_snapshot() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("resumed"))]);
    let (dir, session) = prepared_session("resume-vendor", &server, false, "deepseek");

    ok(dir
        .exec(&dir.path)
        .env_remove("ANTHROPIC_API_KEY")
        .env("DEEPSEEK_API_KEY", "ds-key")
        .args(["-r", &session]));
    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    assert!(
        requests[0].head.contains("x-api-key: ds-key"),
        "{}",
        requests[0].head
    );
}

#[test]
fn a_vendor_model_entry_replaces_the_vendor_default() {
    let deepseek = MockServer::start(vec![Reply::sse(common::text_sse("pro"))]);
    let dir = TestDir::new("vendor-model");
    let config = dir.config(&format!(
        concat!(
            "[model]\nprovider = \"deepseek\"\n\n",
            "[storage]\npath = \"{db}\"\n\n",
            "[providers.deepseek]\nprotocol = \"anthropic\"\nbase_url = \"{url}\"\n",
            "api_key_env = \"DEEPSEEK_API_KEY\"\n\n",
            "[providers.deepseek.profile]\nefforts = [\"low\"]\n\n",
            "[providers.deepseek.models.\"deepseek-v4\"]\nefforts = [\"low\"]\n\n",
            "[providers.deepseek.models.\"deepseek-v4-pro\"]\nefforts = [\"off\"]\n",
            "thinking_default_on = true\n\n",
            "[request.reasoning]\neffort = \"off\"\n",
        ),
        db = dir.db().display(),
        url = deepseek.base_url(),
    ));
    let run = |model: &str| {
        dir.exec(&dir.path)
            .env("DEEPSEEK_API_KEY", "ds-key")
            .args(["-m", model, "hi"])
            .output()
            .expect("run asterpath")
    };

    // The default lists no `off`: rejected before any HTTP.
    let rejected = run("deepseek-flash");
    assert!(!rejected.status.success());
    assert!(deepseek.requests().is_empty());
    // The provider's error reaches stderr and the trace, not just `Failed`.
    let stderr = String::from_utf8_lossy(&rejected.stderr);
    assert!(
        stderr.contains("turn ended with Failed: Unsupported: "),
        "stderr: {stderr}"
    );
    let trace = bin()
        .args([
            "debug",
            "trace",
            "--config",
            config.to_str().unwrap(),
            &session_from_stderr(&stderr),
        ])
        .output()
        .expect("run trace");
    let trace = String::from_utf8_lossy(&trace.stdout);
    assert!(trace.contains("Unsupported"), "trace: {trace}");
    // The longer prefix wins over `deepseek-v4` though it sorts after it.
    let output = run("deepseek-v4-pro-0601");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(deepseek.requests().len(), 1);
}

/// A vendor instance of `protocol` on `server`, with `extra` appended.
fn vendor_protocol_config(protocol: &str, server: &MockServer, db: &Path, extra: &str) -> String {
    format!(
        concat!(
            "[model]\nprovider = \"ds\"\nid = \"ds-flash\"\n\n",
            "[storage]\npath = \"{db}\"\n\n[skills]\nenabled = false\n\n",
            "[providers.ds]\nprotocol = \"{protocol}\"\nbase_url = \"{url}\"\n",
            "api_key_env = \"DEEPSEEK_API_KEY\"\n\n{extra}",
        ),
        db = db.display(),
        protocol = protocol,
        url = server.base_url(),
        extra = extra,
    )
}

/// `structured_output` in the profile lets a vendor model take a schema;
/// without it the adapter refuses before any HTTP.
#[test]
fn a_profile_can_turn_on_structured_output_for_a_vendor_model() {
    for (protocol, reply, format) in [
        ("openai", common::openai_text_sse("{}"), "/text/format/type"),
        (
            "anthropic",
            common::text_sse("{}"),
            "/output_config/format/type",
        ),
    ] {
        let server = MockServer::start(vec![Reply::sse(reply)]);
        let dir = TestDir::new("vendor-schema");
        dir.config(&vendor_protocol_config(protocol, &server, &dir.db(), ""));
        let schema = dir.path.join("schema.json");
        fs::write(&schema, r#"{"type":"object"}"#).unwrap();
        let exec = |flags: &[&str]| {
            let mut command = dir.exec(&dir.path);
            command.env("DEEPSEEK_API_KEY", "ds-key").args(flags).args([
                "--output-schema",
                schema.to_str().unwrap(),
                "hi",
            ]);
            command
        };

        let refused = exec(&[]).output().unwrap();
        assert!(!refused.status.success(), "{protocol}");
        assert!(
            String::from_utf8_lossy(&refused.stderr).contains("Unsupported"),
            "{protocol}"
        );
        assert!(server.requests().is_empty(), "{protocol}");

        ok(&mut exec(&[
            "-c",
            "providers.ds.profile.structured_output=true",
        ]));
        let body = server.requests()[0].json_body();
        assert_eq!(
            body.pointer(format),
            Some(&serde_json::json!("json_schema")),
            "{body}"
        );
    }
}

/// A model entry's fields apply to its prefix only; `file_media_types` lets
/// a PDF through.
#[test]
fn a_model_entry_turns_on_capabilities_for_its_prefix_only() {
    let server = MockServer::start(vec![
        Reply::sse(common::openai_text_sse("{}")),
        Reply::sse(common::openai_text_sse("read")),
    ]);
    let dir = TestDir::new("vendor-prefix");
    dir.config(&vendor_protocol_config(
        "openai",
        &server,
        &dir.db(),
        concat!(
            "[providers.ds.models.\"ds-pro\"]\nstructured_output = true\n",
            "file_media_types = [\"application/pdf\"]\n",
        ),
    ));
    let schema = dir.path.join("schema.json");
    fs::write(&schema, r#"{"type":"object"}"#).unwrap();
    fs::write(dir.path.join("doc.pdf"), b"abc").unwrap();
    let run = |args: &[&str]| {
        dir.exec(&dir.path)
            .env("DEEPSEEK_API_KEY", "ds-key")
            .args(args)
            .output()
            .unwrap()
            .status
            .success()
    };
    let schema = schema.to_str().unwrap();

    assert!(!run(&["--output-schema", schema, "hi"]));
    assert!(!run(&["-i", "doc.pdf", "hi"]));
    assert!(server.requests().is_empty());
    assert!(run(&["-m", "ds-pro-1", "--output-schema", schema, "hi"]));
    assert!(run(&["-m", "ds-pro-1", "-i", "doc.pdf", "hi"]));
    let body = seen(&server, 1);
    assert!(
        body.contains("application/pdf") && body.contains("YWJj"),
        "{body}"
    );
}

#[test]
fn doctor_accepts_vendor_providers_and_aliases_to_them() {
    let dir = TestDir::new("vendor-check");
    let config = dir.config(concat!(
        "[providers.deepseek]\nprotocol = \"openai\"\n\n",
        "[providers.deepseek.profile]\nefforts = [\"off\"]\nsampling = false\n\n",
        "[model_aliases.fast]\nprovider = \"deepseek\"\nmodel = \"deepseek-flash\"\n",
    ));
    bin()
        .args(["doctor", "--config", config.to_str().unwrap()])
        .assert()
        .success();
}

#[test]
fn doctor_rejects_bad_vendor_provider_sections_with_two() {
    for body in [
        "[providers.deepseek]\nprotocol = \"anthropic\"\nmystery = 1\n",
        "[providers.deepseek]\nprotocol = \"gemini\"\n",
        "[providers.deepseek]\nbase_url = \"http://x\"\n",
        "[providers.deepseek]\nprotocol = \"anthropic\"\n[providers.deepseek.profile]\nefforts = [\"none\"]\n",
        "[providers.deepseek]\nprotocol = \"anthropic\"\n[providers.deepseek.models.m]\nmystery = 1\n",
        "[providers.deepseek]\nprotocol = \"openai\"\n[providers.deepseek.profile]\nthinking = \"adaptive\"\n",
        "[providers.openai.models.m]\nthinking_default_on = true\n",
        "[providers.deepseek]\nprotocol = \"openai\"\n[providers.deepseek.profile]\ninline_changes = true\n",
        "[providers.deepseek]\nprotocol = \"anthropic\"\n[providers.deepseek.profile]\nthinking = \"sometimes\"\n",
        "[providers.deepseek.profile]\napi_key = \"leaked\"\n",
    ] {
        let dir = TestDir::new("vendor-bad");
        let config = dir.config(body);
        let output = bin()
            .args(["doctor", "--config", config.to_str().unwrap()])
            .output()
            .expect("run doctor");
        assert_eq!(
            output.status.code(),
            Some(2),
            "{body} must be rejected: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn inline_changes_on_an_openai_provider_names_the_field() {
    let dir = TestDir::new("inline-openai");
    let config = dir.config(concat!(
        "[providers.deepseek]\nprotocol = \"openai\"\n",
        "[providers.deepseek.models.\"ds-pro\"]\ninline_changes = true\n",
    ));
    let output = bin()
        .args(["doctor", "--config", config.to_str().unwrap()])
        .output()
        .expect("run doctor");

    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(
            "providers.deepseek.models.ds-pro: `inline_changes` needs the anthropic protocol"
        ),
        "{stderr}"
    );
}

/// Create a session through the library: pending input, optionally with an
/// already-open turn, on provider instance `id` (an anthropic-protocol
/// section reading `DEEPSEEK_API_KEY` unless `id` is `anthropic`). No drive
/// happens, so the CLI must resume it.
fn prepared_session(
    name: &str,
    server: &MockServer,
    turn_started: bool,
    id: &str,
) -> (TestDir, String) {
    let dir = TestDir::new(name);
    let mut config = config_body(server, &dir.db());
    if id != "anthropic" {
        config.push_str(&format!(
            "\n[providers.{id}]\nprotocol = \"anthropic\"\nbase_url = \"{}\"\napi_key_env = \"DEEPSEEK_API_KEY\"\n",
            server.base_url()
        ));
    }
    dir.config(&config);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let session = runtime.block_on(async {
        let provider = AnthropicProvider::new("test-key")
            .with_base_url(server.base_url())
            .with_id(id);
        let asterpath = Asterpath::builder()
            .database(dir.db())
            .provider(Arc::new(provider))
            .build()
            .await
            .expect("build");
        let handle = asterpath
            .create_session(SessionSpec {
                model: ModelSpec {
                    provider: ProviderId::new(id),
                    model: ModelId::new("claude-test"),
                    context_window: None,
                },
                system: Vec::new(),
                tools: ToolSelection::All,
                request: RequestConfig::default(),
                turn: TurnConfig::default(),
                compaction: CompactionConfig::default(),
                subagents: SubagentConfig::default(),
                metadata: Default::default(),
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

fn vendor_config(anthropic: &MockServer, deepseek: &MockServer, db: &Path) -> String {
    format!(
        concat!(
            "[storage]\npath = \"{db}\"\n\n",
            "[providers.anthropic]\nbase_url = \"{anthropic}\"\n\n",
            "[providers.deepseek]\nprotocol = \"anthropic\"\nbase_url = \"{deepseek}\"\n",
            "api_key_env = \"DEEPSEEK_API_KEY\"\n\n",
            "[providers.deepseek.profile]\nefforts = [\"off\", \"low\", \"high\", \"max\"]\n",
            "thinking_default_on = true\n\n",
            "[providers.deepseek.models.\"deepseek-v4-pro\"]\nefforts = [\"low\", \"high\", \"max\"]\n\n",
            "[request.reasoning]\neffort = \"off\"\n",
        ),
        db = db.display(),
        anthropic = anthropic.base_url(),
        deepseek = deepseek.base_url(),
    )
}

#[test]
fn global_options_before_the_subcommand_reach_it() {
    let server = MockServer::start(vec![Reply::sse(common::text_sse("ok"))]);
    let dir = TestDir::new("global-first");
    let config = dir.config(&config_body(&server, &dir.db()));
    let config = config.to_str().unwrap();

    let output = bin()
        .current_dir(&dir.path)
        .args(["--config", config, "doctor"])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    assert!(String::from_utf8_lossy(&output.stdout).contains("config ok"));

    let (stdout, _) = ok(bin().current_dir(&dir.path).args([
        "--config",
        config,
        "-c",
        "model.id=c-model",
        "exec",
        "--output-format",
        "json",
        "hi",
    ]));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&stdout).unwrap()["result"],
        "ok"
    );
    assert_eq!(server.requests()[0].json_body()["model"], "c-model");

    // Session options and a prompt before a subcommand are never dropped silently.
    for args in [
        &["--config", config, "-C", ".", "doctor"][..],
        &["--config", config, "-m", "x", "exec", "hi"],
    ] {
        let output = bin().current_dir(&dir.path).args(args).output().unwrap();
        assert_eq!(output.status.code(), Some(2), "{args:?}: {output:?}");
    }
    assert_eq!(server.requests().len(), 1, "no model call");
}
