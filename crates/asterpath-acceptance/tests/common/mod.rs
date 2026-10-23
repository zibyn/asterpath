//! Shared acceptance-test glue: spawn `testdrive`, wait on JSON markers, and
//! read the committed log back from the same SQLite file.

#![allow(dead_code)]

pub mod mock;

use std::io::{BufRead, BufReader};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use asterpath_protocol::SessionEvent;
use asterpath_protocol::{EventLog, LogEntry, LogQuery, SessionId};
use asterpath_storage_sqlite::SqliteStorage;
use futures_util::StreamExt;
use serde_json::Value;

pub const MARKER_TIMEOUT: Duration = Duration::from_secs(15);

pub fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(name)
}

/// The `asterpath-mcp-fixture` binary. `cargo test -p asterpath-acceptance` does
/// not build another package's bins, so build it on demand when missing.
pub fn mcp_fixture() -> PathBuf {
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
    assert!(
        bin.exists(),
        "fixture binary missing at {}; build it with `cargo build -p asterpath-mcp-fixture`",
        bin.display()
    );
    bin
}

/// Proves a crashed host leaves no sandbox or fixture process behind. Because
/// the adapter now spawns its server in its own process group, a SIGKILL of the
/// driver does not reach it: leftovers matching the fixture path and `delay`
/// (a unique `--fault-delay-ms` value) are killed here before asserting.
pub fn assert_no_process(fixture: &Path, delay: &str) {
    let fixture = fixture.to_string_lossy().into_owned();
    for _ in 0..100 {
        let found = matching_pids(&fixture, delay);
        if found.is_empty() {
            return;
        }
        for pid in found {
            // SAFETY: best-effort cleanup of processes started by the test.
            unsafe {
                libc::kill(pid, libc::SIGKILL);
            }
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!(
        "processes still running for fixture {fixture} delay {delay}: {:?}",
        matching_pids(&fixture, delay)
    );
}

fn matching_pids(fixture: &str, delay: &str) -> Vec<i32> {
    let mut pids = Vec::new();
    for entry in std::fs::read_dir("/proc").expect("proc") {
        let Ok(entry) = entry else { continue };
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<i32>() else {
            continue;
        };
        let Ok(cmdline) = std::fs::read(entry.path().join("cmdline")) else {
            continue;
        };
        let args: Vec<&[u8]> = cmdline.split(|byte| *byte == 0).collect();
        if args.contains(&fixture.as_bytes()) && args.contains(&delay.as_bytes()) {
            pids.push(pid);
        }
    }
    pids
}

pub fn open(db: &Path) -> std::sync::Arc<SqliteStorage> {
    asterpath_storage_sqlite::open(db).expect("open the acceptance database")
}

pub async fn scan(db: &Path, session: SessionId) -> Vec<LogEntry> {
    let log = open(db);
    let mut stream = log.scan(LogQuery {
        session,
        after: None,
        limit: None,
    });
    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await {
        entries.push(entry.expect("scan"));
    }
    entries
}

pub async fn events(db: &Path, session: SessionId) -> Vec<SessionEvent> {
    scan(db, session)
        .await
        .iter()
        .map(|entry| asterpath_protocol::decode_entry(entry).expect("decode"))
        .collect()
}

pub fn kind_count(entries: &[LogEntry], kind: &str) -> usize {
    entries
        .iter()
        .filter(|entry| {
            asterpath_protocol::decode_entry(entry)
                .map(|event| event.kind() == kind)
                .unwrap_or(false)
        })
        .count()
}

/// A `testdrive` process whose stdout is streamed line by line.
pub struct Testdrive {
    child: Child,
    rx: Receiver<String>,
    lines: Vec<String>,
    stderr: Option<JoinHandle<String>>,
    name: String,
}

pub struct Scenario<'a> {
    pub run: bool,
    pub db: &'a Path,
    pub fixture: &'a str,
    pub effect: &'a Path,
    pub hold_ms: u64,
    pub failpoints: Vec<String>,
}

/// M5 scenarios: the same driver plus MCP server configs and a sandbox policy.
pub struct McpScenario<'a> {
    pub run: bool,
    pub db: &'a Path,
    pub fixture: &'a str,
    pub effect: &'a Path,
    pub configs: &'a [PathBuf],
    pub sandbox: &'a str,
    pub kill_grace_ms: Option<u64>,
}

impl Testdrive {
    pub fn start(scenario: Scenario<'_>) -> Self {
        let name = scenario.fixture.to_owned();
        let command = Self::command(&scenario);
        Self::spawn(command, name)
    }

    pub fn start_mcp(scenario: McpScenario<'_>) -> Self {
        let name = scenario.fixture.to_owned();
        let mut command = Self::base_command(
            scenario.run,
            scenario.db,
            scenario.fixture,
            scenario.effect,
            0,
        );
        for config in scenario.configs {
            command.arg("--mcp-config").arg(config);
        }
        command.arg("--sandbox").arg(scenario.sandbox);
        if let Some(grace) = scenario.kill_grace_ms {
            command.arg("--kill-grace-ms").arg(grace.to_string());
        }
        Self::spawn(command, name)
    }

    /// Scenario (b): the same binary and fixture, with a real adapter pointed at
    /// a loopback mock. Both provider keys are set so the helper stays neutral.
    pub fn start_provider(scenario: Scenario<'_>, provider: &str, base_url: &str) -> Self {
        let name = scenario.fixture.to_owned();
        let mut command = Self::command(&scenario);
        command
            .arg("--provider")
            .arg(provider)
            .arg("--base-url")
            .arg(base_url)
            .env("ANTHROPIC_API_KEY", "test-key")
            .env("OPENAI_API_KEY", "test-key");
        Self::spawn(command, name)
    }

    /// Resume one known session. Scenario (c) resumes the parent: its spawn
    /// replay drives the children, so the process must not start elsewhere.
    pub fn resume_session(scenario: Scenario<'_>, session: SessionId) -> Self {
        let name = scenario.fixture.to_owned();
        let mut command = Self::command(&scenario);
        command.arg("--session").arg(session.to_hex());
        Self::spawn(command, name)
    }

    fn command(scenario: &Scenario<'_>) -> Command {
        let mut command = Self::base_command(
            scenario.run,
            scenario.db,
            scenario.fixture,
            scenario.effect,
            scenario.hold_ms,
        );
        for failpoint in &scenario.failpoints {
            command.arg("--failpoint").arg(failpoint);
        }
        command
    }

    fn base_command(
        run: bool,
        db: &Path,
        fixture_name: &str,
        effect: &Path,
        hold_ms: u64,
    ) -> Command {
        let mut command = Command::new(assert_cmd::cargo::cargo_bin("testdrive"));
        command
            .arg(if run { "run" } else { "resume" })
            .arg("--db")
            .arg(db)
            .arg("--fixture")
            .arg(fixture(fixture_name))
            .env("ASTERPATH_EFFECT_FILE", effect)
            .env("ASTERPATH_TOOL_HOLD_MS", hold_ms.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        // The crash scenarios start a bwrap + fixture tree; killing the whole
        // group mirrors a host crash and leaves nothing behind.
        command.process_group(0);
        command
    }

    fn spawn(mut command: Command, name: String) -> Self {
        let mut child = command.spawn().expect("spawn testdrive");
        let stdout = child.stdout.take().expect("stdout");
        let stderr = child.stderr.take().expect("stderr");
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                match line {
                    Ok(line) => {
                        if tx.send(line).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        let stderr = thread::spawn(move || {
            let mut buffer = String::new();
            use std::io::Read;
            let _ = BufReader::new(stderr).read_to_string(&mut buffer);
            buffer
        });
        Self {
            child,
            rx,
            lines: Vec::new(),
            stderr: Some(stderr),
            name,
        }
    }

    /// Block until a stdout line contains `needle` (15s cap), then return it.
    pub fn wait_marker(&mut self, needle: &str) -> String {
        loop {
            match self.rx.recv_timeout(MARKER_TIMEOUT) {
                Ok(line) => {
                    self.lines.push(line.clone());
                    if line.contains(needle) {
                        return line;
                    }
                }
                Err(error) => {
                    // Unblock the stderr reader before collecting it.
                    kill_group(&mut self.child);
                    let stderr = self.take_stderr();
                    panic!(
                        "testdrive {} did not emit {needle:?} within {MARKER_TIMEOUT:?}: {error}\nstdout: {:#?}\nstderr: {stderr}",
                        self.name, self.lines
                    );
                }
            }
        }
    }

    /// Signal-level crash: `SIGKILL`, never `SIGTERM` or a panic.
    pub fn kill(mut self) -> Vec<String> {
        kill_group(&mut self.child);
        self.drain();
        std::mem::take(&mut self.lines)
    }

    /// Wait for a process that is expected to finish on its own.
    pub fn wait_success(mut self) -> Vec<String> {
        let status = self.child.wait().expect("wait for testdrive");
        self.drain();
        let stderr = self.take_stderr();
        assert!(
            status.success(),
            "testdrive {} failed with {status}\nstdout: {:#?}\nstderr: {stderr}",
            self.name,
            self.lines
        );
        std::mem::take(&mut self.lines)
    }

    fn drain(&mut self) {
        while let Ok(line) = self.rx.recv_timeout(Duration::from_secs(5)) {
            self.lines.push(line);
        }
    }

    fn take_stderr(&mut self) -> String {
        self.stderr
            .take()
            .map(|handle| handle.join().unwrap_or_default())
            .unwrap_or_default()
    }
}

impl Drop for Testdrive {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            kill_group(&mut self.child);
        }
    }
}

/// `process_group(0)` made the child a group leader, so a negative pid kills
/// the driver and everything it started.
fn kill_group(child: &mut Child) {
    // SAFETY: the group belongs to the child we spawned and it has not been
    // reaped yet.
    unsafe {
        libc::kill(-(child.id() as i32), libc::SIGKILL);
    }
    let _ = child.wait();
}

pub fn session_id(lines: &[String]) -> SessionId {
    for line in lines {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if value.get("event").and_then(Value::as_str) == Some("session")
            && let Some(hex) = value.get("id").and_then(Value::as_str)
        {
            return parse_session(hex);
        }
    }
    panic!("no session marker in {lines:#?}");
}

pub fn parse_session(hex: &str) -> SessionId {
    let bytes: Vec<u8> = hex
        .as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).expect("hex"))
        .collect();
    SessionId::from_bytes(bytes.try_into().expect("16 bytes"))
}

pub fn markers(lines: &[String], event: &str) -> Vec<Value> {
    lines
        .iter()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|value| value.get("event").and_then(Value::as_str) == Some(event))
        .collect()
}

pub fn effect_lines(effect: &Path) -> Vec<String> {
    std::fs::read_to_string(effect)
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect()
}
