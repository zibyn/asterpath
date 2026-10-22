//! Shared acceptance-test glue: spawn `testdrive`, wait on JSON markers, and
//! read the committed log back from the same SQLite file.

#![allow(dead_code)]

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use asterpath_core::{EventLog, LogEntry, LogQuery, SessionId};
use asterpath_protocol::SessionEvent;
use asterpath_storage_sqlite::SqliteStorage;
use futures_util::StreamExt;
use serde_json::Value;

pub const MARKER_TIMEOUT: Duration = Duration::from_secs(15);

pub fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(name)
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

impl Testdrive {
    pub fn start(scenario: Scenario<'_>) -> Self {
        let mut command = Command::new(assert_cmd::cargo::cargo_bin("testdrive"));
        command
            .arg(if scenario.run { "run" } else { "resume" })
            .arg("--db")
            .arg(scenario.db)
            .arg("--fixture")
            .arg(fixture(scenario.fixture))
            .env("ASTERPATH_EFFECT_FILE", scenario.effect)
            .env("ASTERPATH_TOOL_HOLD_MS", scenario.hold_ms.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for failpoint in &scenario.failpoints {
            command.arg("--failpoint").arg(failpoint);
        }
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
        let name = scenario.fixture.to_owned();
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
                    let _ = self.child.kill();
                    let _ = self.child.wait();
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
        // SAFETY: the pid belongs to the child we spawned and it has not been
        // reaped yet.
        unsafe {
            libc::kill(self.child.id() as i32, libc::SIGKILL);
        }
        let _ = self.child.wait();
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
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
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
