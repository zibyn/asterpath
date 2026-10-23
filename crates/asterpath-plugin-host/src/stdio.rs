//! MCP stdio adapter for the stateless `2026-07-28` revision.
//!
//! One JSON-RPC object per line. stdout carries protocol messages only; stderr
//! is drained and discarded. Requests are multiplexed by id through a reader
//! task. On EOF, read error or process exit the adapter is marked dead and every
//! pending call fails with `Transport`.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use asterpath_protocol::{JsonString, ToolContext, ToolError, ToolOutput};
use asterpath_sandbox::ConfinedSpawn;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, Command};
use tokio::sync::{Notify, oneshot};

use crate::adapter::{AdapterError, AdapterHandshake, AdapterTool, BoxFuture, PluginAdapter};
use crate::mcp::{self, RpcClient, RpcFailure};

const KILL_WAIT: Duration = Duration::from_millis(500);

type PendingSender = oneshot::Sender<Result<Value, RpcFailure>>;

struct Shared {
    next_id: AtomicU64,
    pending: Mutex<HashMap<u64, PendingSender>>,
    dead: AtomicBool,
    process_exited: AtomicBool,
    exit_notify: Notify,
    writer: tokio::sync::Mutex<Option<ChildStdin>>,
    pid: u32,
    protocol_version: Mutex<String>,
    startup: Duration,
    grace: Duration,
}

pub(crate) struct StdioAdapter {
    shared: Arc<Shared>,
}

impl StdioAdapter {
    pub(crate) fn spawn(
        confined: &ConfinedSpawn,
        candidate_version: &str,
        startup: Duration,
        grace: Duration,
    ) -> io::Result<Self> {
        let mut command = Command::new(&confined.program);
        command
            .args(&confined.args)
            .env_clear()
            .envs(&confined.env)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        if let Some(cwd) = &confined.cwd {
            command.current_dir(cwd);
        }
        // Own process group so the whole tree (bwrap and its child) can be
        // signalled at once (`mcp.md` §3.3).
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command.spawn()?;
        let pid = child.id().unwrap_or_default();
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| io::Error::other("plugin process has no stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("plugin process has no stdout"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| io::Error::other("plugin process has no stderr"))?;

        let shared = Arc::new(Shared {
            next_id: AtomicU64::new(0),
            pending: Mutex::new(HashMap::new()),
            dead: AtomicBool::new(false),
            process_exited: AtomicBool::new(false),
            exit_notify: Notify::new(),
            writer: tokio::sync::Mutex::new(Some(stdin)),
            pid,
            protocol_version: Mutex::new(candidate_version.to_owned()),
            startup,
            grace,
        });

        // stderr is protocol-external and may contain secrets: drain and drop.
        tokio::spawn(async move {
            let mut stderr = stderr;
            let _ = tokio::io::copy(&mut stderr, &mut tokio::io::sink()).await;
        });

        let reaper = Arc::clone(&shared);
        tokio::spawn(async move {
            let _ = child.wait().await;
            reaper.process_exited.store(true, Ordering::SeqCst);
            mark_dead(&reaper, "MCP stdio server exited");
            reaper.exit_notify.notify_waiters();
        });

        let reader = Arc::clone(&shared);
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            loop {
                match lines.next_line().await {
                    Ok(Some(line)) => route_incoming(&reader, &line),
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            mark_dead(&reader, "MCP stdio connection closed");
        });

        Ok(Self { shared })
    }

    fn shared(&self) -> &Shared {
        &self.shared
    }
}

fn route_incoming(shared: &Shared, line: &str) {
    let line = line.trim();
    if line.is_empty() {
        return;
    }
    let Ok(message) = serde_json::from_str::<Value>(line) else {
        mark_dead(shared, "malformed JSON from MCP server");
        return;
    };
    // Notifications (no id) and server-originated messages are ignored: the
    // stateless revision has no server requests and v1 opens no subscriptions.
    let Some(id) = message.get("id").and_then(Value::as_u64) else {
        return;
    };
    let sender = shared
        .pending
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(&id);
    if let Some(sender) = sender {
        let _ = sender.send(Ok(message));
    }
}

fn mark_dead(shared: &Shared, message: &str) {
    if shared.dead.swap(true, Ordering::SeqCst) {
        return;
    }
    let pending = std::mem::take(
        &mut *shared
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner),
    );
    for (_, sender) in pending {
        let _ = sender.send(Err(RpcFailure::Transport(message.to_owned())));
    }
}

async fn write_line(shared: &Shared, line: &str) -> io::Result<()> {
    let mut writer = shared.writer.lock().await;
    let Some(stdin) = writer.as_mut() else {
        return Err(io::Error::new(io::ErrorKind::BrokenPipe, "stdin is closed"));
    };
    stdin.write_all(line.as_bytes()).await?;
    stdin.write_all(b"\n").await?;
    stdin.flush().await
}

async fn request_impl(
    shared: &Shared,
    method: &str,
    params: &str,
    timeout: Duration,
) -> Result<Value, RpcFailure> {
    if shared.dead.load(Ordering::SeqCst) {
        return Err(RpcFailure::Transport(
            "MCP stdio connection is closed".to_owned(),
        ));
    }
    let id = shared.next_id.fetch_add(1, Ordering::SeqCst) + 1;
    let line = format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":{id},\"method\":{},\"params\":{params}}}",
        serde_json::to_string(method).expect("method is serializable"),
    );
    let (sender, receiver) = oneshot::channel();
    shared
        .pending
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(id, sender);
    if shared.dead.load(Ordering::SeqCst) {
        shared
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&id);
        return Err(RpcFailure::Transport(
            "MCP stdio connection is closed".to_owned(),
        ));
    }
    if let Err(error) = write_line(shared, &line).await {
        shared
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&id);
        mark_dead(shared, "failed to write to MCP server");
        return Err(RpcFailure::Transport(format!(
            "failed to send MCP request: {error}"
        )));
    }
    match tokio::time::timeout(timeout, receiver).await {
        Ok(Ok(response)) => response,
        Ok(Err(_)) => Err(RpcFailure::Transport(
            "MCP response channel closed".to_owned(),
        )),
        Err(_) => {
            shared
                .pending
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&id);
            Err(RpcFailure::Transport("MCP request timed out".to_owned()))
        }
    }
}

impl RpcClient for Shared {
    fn request<'a>(
        &'a self,
        method: &'a str,
        params: &'a str,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<Value, RpcFailure>> {
        Box::pin(request_impl(self, method, params, timeout))
    }

    fn protocol_version(&self) -> String {
        self.protocol_version
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn set_protocol_version(&self, version: &str) {
        *self
            .protocol_version
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = version.to_owned();
    }
}

/// Kills the process group and marks the adapter dead if the call future is
/// dropped while the call is still in flight (the pipeline drops it at grace
/// expiry). The pipeline, not the adapter, writes the terminal then.
struct InFlight<'a> {
    shared: &'a Shared,
    armed: bool,
}

impl<'a> InFlight<'a> {
    fn new(shared: &'a Shared) -> Self {
        Self {
            shared,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        if self.armed {
            mark_dead(self.shared, "MCP stdio call abandoned");
            kill_process(self.shared.pid);
        }
    }
}

/// Removes JSON whitespace outside string literals so a legal multi-line
/// argument value still fits the one-message-per-line framing. Bytes inside
/// strings (including escapes) and number lexemes are preserved.
fn compact_json(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut in_string = false;
    let mut escaped = false;
    for ch in raw.chars() {
        if in_string {
            out.push(ch);
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
        } else if ch == '"' {
            in_string = true;
            out.push(ch);
        } else if !ch.is_ascii_whitespace() {
            out.push(ch);
        }
    }
    out
}

async fn call_impl(
    adapter: &StdioAdapter,
    original_name: &str,
    args: JsonString,
    ctx: &ToolContext,
) -> Result<ToolOutput, ToolError> {
    mcp::parse_call_args(&args)?;
    let shared = adapter.shared();
    if shared.dead.load(Ordering::SeqCst) {
        return Err(ToolError::Transport {
            message: "MCP stdio connection is closed".to_owned(),
        });
    }
    let id = shared.next_id.fetch_add(1, Ordering::SeqCst) + 1;
    // Compact (not re-serialize): interior bytes and number lexemes survive,
    // while the request stays on one line.
    let args = compact_json(args.as_str());
    let params = format!(
        "{{\"name\":{},\"arguments\":{},\"_meta\":{}}}",
        serde_json::to_string(original_name).expect("name is serializable"),
        args,
        mcp::meta_json(&shared.protocol_version()),
    );
    let line = format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":{id},\"method\":\"tools/call\",\"params\":{params}}}"
    );
    let (sender, mut receiver) = oneshot::channel();
    shared
        .pending
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(id, sender);
    if shared.dead.load(Ordering::SeqCst) {
        shared
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&id);
        return Err(ToolError::Transport {
            message: "MCP stdio connection is closed".to_owned(),
        });
    }
    if let Err(error) = write_line(shared, &line).await {
        shared
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&id);
        mark_dead(shared, "failed to write to MCP server");
        return Err(ToolError::Transport {
            message: format!("failed to send MCP request: {error}"),
        });
    }

    let mut guard = InFlight::new(shared);
    let on_protocol_error = |code| {
        if matches!(
            code,
            -32020 | -32021 | -32022 | -32600 | -32601 | -32603 | -32700
        ) {
            mark_dead(shared, "MCP protocol error");
        }
    };
    tokio::select! {
        response = &mut receiver => {
            guard.disarm();
            let received = match response {
                Ok(received) => received,
                Err(_) => Err(RpcFailure::Transport("MCP response channel closed".to_owned())),
            };
            mcp::map_call_response(received, on_protocol_error)
        }
        _ = ctx.cancel.cancelled() => {
            send_cancelled(shared, id).await;
            // A response inside the grace window wins; otherwise kill the
            // process group, mark dead and stay pending. The pipeline owns the
            // terminal (`mcp.md` §8, `tools.md` §6): its per-call deadline
            // writes `Timeout`, a turn cancel writes `Interrupted`.
            match tokio::time::timeout(shared.grace, &mut receiver).await {
                Ok(received) => {
                    guard.disarm();
                    let received = match received {
                        Ok(response) => response,
                        Err(_) => Err(RpcFailure::Transport(
                            "MCP response channel closed".to_owned(),
                        )),
                    };
                    mcp::map_call_response(received, on_protocol_error)
                }
                Err(_) => {
                    kill_now(shared).await;
                    std::future::pending::<Result<ToolOutput, ToolError>>().await
                }
            }
        }
    }
}

async fn send_cancelled(shared: &Shared, id: u64) {
    let line = format!(
        "{{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{{\"requestId\":{id},\"reason\":\"cancelled\"}}}}"
    );
    let _ = write_line(shared, &line).await;
}

async fn wait_exited(shared: &Shared, duration: Duration) -> bool {
    if shared.process_exited.load(Ordering::SeqCst) {
        return true;
    }
    let notified = shared.exit_notify.notified();
    if shared.process_exited.load(Ordering::SeqCst) {
        return true;
    }
    tokio::time::timeout(duration, notified).await.is_ok()
        || shared.process_exited.load(Ordering::SeqCst)
}

async fn close_stdin(shared: &Shared) {
    let mut writer = shared.writer.lock().await;
    writer.take();
}

async fn kill_now(shared: &Shared) {
    mark_dead(shared, "MCP stdio server stopped");
    close_stdin(shared).await;
    terminate(shared.pid);
    if !wait_exited(shared, KILL_WAIT).await {
        kill_process(shared.pid);
    }
}

async fn shutdown_impl(shared: &Shared) {
    mark_dead(shared, "adapter shut down");
    close_stdin(shared).await;
    if wait_exited(shared, shared.grace).await {
        return;
    }
    terminate(shared.pid);
    if wait_exited(shared, KILL_WAIT).await {
        return;
    }
    kill_process(shared.pid);
}

#[cfg(unix)]
fn terminate(pid: u32) {
    signal_process_tree(pid, libc::SIGTERM);
}

#[cfg(unix)]
fn kill_process(pid: u32) {
    signal_process_tree(pid, libc::SIGKILL);
}

/// Signals the child's process group (it is spawned as the group leader); if
/// the group is already gone, falls back to the direct pid.
#[cfg(unix)]
fn signal_process_tree(pid: u32, signal: libc::c_int) {
    // `kill(0)` would signal our own group; never do that.
    if pid == 0 {
        return;
    }
    // SAFETY: kill(2) with our own child's pid and a valid signal.
    let sent = unsafe { libc::kill(-(pid as libc::pid_t), signal) };
    if sent != 0 {
        // SAFETY: same child, direct-pid fallback.
        unsafe { libc::kill(pid as libc::pid_t, signal) };
    }
}

#[cfg(not(unix))]
fn terminate(_pid: u32) {}

#[cfg(not(unix))]
fn kill_process(_pid: u32) {}

impl PluginAdapter for StdioAdapter {
    fn handshake(&mut self) -> BoxFuture<'_, Result<AdapterHandshake, AdapterError>> {
        Box::pin(mcp::handshake(self.shared.as_ref(), self.shared.startup))
    }

    fn discover(&mut self) -> BoxFuture<'_, Result<Vec<AdapterTool>, AdapterError>> {
        Box::pin(mcp::discover(self.shared.as_ref(), self.shared.startup))
    }

    fn call<'a>(
        &'a self,
        original_name: &'a str,
        args: JsonString,
        ctx: &'a ToolContext,
    ) -> BoxFuture<'a, Result<ToolOutput, ToolError>> {
        Box::pin(call_impl(self, original_name, args, ctx))
    }

    fn shutdown(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(shutdown_impl(&self.shared))
    }
}

impl Drop for StdioAdapter {
    fn drop(&mut self) {
        mark_dead(&self.shared, "adapter dropped");
        if !self.shared.process_exited.load(Ordering::SeqCst) {
            kill_process(self.shared.pid);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::path::PathBuf;
    use std::time::Duration;

    use asterpath_core::{SessionId, StepId, ToolCallId, TurnId};
    use asterpath_protocol::{CancelToken, ToolContext};
    use asterpath_sandbox::{ConfinedSpawn, Enforcement};
    use serde_json::json;

    use super::StdioAdapter;
    use crate::adapter::{AdapterError, PluginAdapter};

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
                .current_dir(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
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

    fn adapter(args: &[&str], grace: Duration) -> StdioAdapter {
        let mut full_args = vec![OsString::from("--stdio")];
        full_args.extend(args.iter().map(OsString::from));
        let confined = ConfinedSpawn::new(
            fixture_bin().into_os_string(),
            full_args,
            None,
            std::collections::BTreeMap::new(),
            Enforcement::Full,
        );
        StdioAdapter::spawn(&confined, "2026-07-28", Duration::from_secs(5), grace)
            .expect("spawn fixture")
    }

    fn context(cancel: CancelToken) -> ToolContext {
        ToolContext::new(
            SessionId::from_bytes([1; 16]),
            TurnId::from_bytes([2; 16]),
            StepId::from_bytes([3; 16]),
            ToolCallId::from_bytes([4; 16]),
            cancel,
        )
    }

    #[tokio::test]
    async fn discovery_rejects_a_non_object_input_schema() {
        let schema = json!("not an object");
        let error = crate::mcp::parse_tool(&json!({
            "name": "bad",
            "inputSchema": schema,
        }))
        .unwrap_err();
        assert!(matches!(error, AdapterError::Discovery { .. }), "{error:?}");
    }

    #[tokio::test]
    async fn notifications_do_not_mark_the_connection_dead() {
        let adapter = adapter(&[], Duration::from_secs(1));
        super::route_incoming(
            adapter.shared(),
            "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/tools/list_changed\"}",
        );
        assert!(
            !adapter
                .shared()
                .dead
                .load(std::sync::atomic::Ordering::SeqCst)
        );
        super::shutdown_impl(adapter.shared()).await;
    }

    #[tokio::test]
    async fn unknown_tool_maps_to_not_found() {
        let mut adapter = adapter(&[], Duration::from_secs(1));
        adapter.handshake().await.expect("handshake");
        adapter.discover().await.expect("discover");
        let error = adapter
            .call(
                "missing",
                asterpath_protocol::JsonString::new("{}"),
                &context(CancelToken::new()),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, asterpath_protocol::ToolError::NotFound { .. }),
            "{error:?}"
        );
        adapter.shutdown().await;
    }

    #[tokio::test]
    async fn multiline_argument_whitespace_stays_on_one_line() {
        let mut adapter = adapter(&[], Duration::from_secs(1));
        adapter.handshake().await.expect("handshake");
        adapter.discover().await.expect("discover");
        let output = adapter
            .call(
                "echo",
                asterpath_protocol::JsonString::new(
                    "{\n  \"text\"\t: \"line\\nbreak\",\n  \"n\": 1.5e2\n}",
                ),
                &context(CancelToken::new()),
            )
            .await
            .expect("multiline arguments are compacted");
        assert_eq!(
            output.content,
            vec![asterpath_protocol::Part::Text {
                text: "line\nbreak".to_owned()
            }]
        );
        adapter.shutdown().await;
    }

    #[tokio::test]
    async fn cancellation_within_grace_returns_the_real_response() {
        let mut adapter = adapter(&["--fault-delay-ms", "50"], Duration::from_secs(1));
        adapter.handshake().await.expect("handshake");
        adapter.discover().await.expect("discover");
        let cancel = CancelToken::new();
        let task = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            task.cancel();
        });
        let output = adapter
            .call(
                "echo",
                asterpath_protocol::JsonString::new("{\"text\":\"late\"}"),
                &context(cancel),
            )
            .await
            .expect("the response wins inside the grace window");
        assert_eq!(
            output.content,
            vec![asterpath_protocol::Part::Text {
                text: "late".to_owned()
            }]
        );
        adapter.shutdown().await;
    }

    #[tokio::test]
    async fn cancellation_after_grace_kills_and_never_resolves_the_call() {
        let mut adapter = adapter(&["--fault-delay-ms", "70234"], Duration::from_millis(100));
        adapter.handshake().await.expect("handshake");
        adapter.discover().await.expect("discover");
        let cancel = CancelToken::new();
        let task = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            task.cancel();
        });
        // The adapter must not settle: the pipeline owns Timeout/Interrupted.
        let result = tokio::time::timeout(
            Duration::from_millis(600),
            adapter.call(
                "echo",
                asterpath_protocol::JsonString::new("{}"),
                &context(cancel),
            ),
        )
        .await;
        assert!(result.is_err(), "call resolved on its own: {result:?}");
        let follow_up = adapter
            .call(
                "echo",
                asterpath_protocol::JsonString::new("{}"),
                &context(CancelToken::new()),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(follow_up, asterpath_protocol::ToolError::Transport { .. }),
            "{follow_up:?}"
        );
        adapter.shutdown().await;
        #[cfg(target_os = "linux")]
        assert_no_pids(&fixture_bin(), "70234");
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn cancel_kills_a_wrapper_and_its_child_process_group() {
        let fixture = fixture_bin();
        let delay = "70123";
        // `sh -c` wraps the fixture in a second process. The trailing command
        // stops the shell from exec-optimizing its last command, so the wrapper
        // stays alive while the fixture runs (no temp script: an inherited
        // write fd could make exec fail with ETXTBSY).
        let args = vec![
            OsString::from("-c"),
            OsString::from("\"$@\"\ncode=$?\nexit $code"),
            OsString::from("sh"),
            fixture.clone().into_os_string(),
            OsString::from("--stdio"),
            OsString::from("--fault-delay-ms"),
            OsString::from(delay),
        ];
        let confined = ConfinedSpawn::new(
            OsString::from("/bin/sh"),
            args,
            None,
            std::collections::BTreeMap::new(),
            Enforcement::Full,
        );
        let mut adapter = StdioAdapter::spawn(
            &confined,
            "2026-07-28",
            Duration::from_secs(5),
            Duration::from_millis(100),
        )
        .expect("spawn wrapper");
        adapter.handshake().await.expect("handshake");
        adapter.discover().await.expect("discover");
        // The shell wrapper and the fixture it runs are both alive.
        let pids = matching_pids(&fixture, delay);
        assert!(
            pids.len() >= 2,
            "expected a wrapper plus its child: {pids:?}"
        );

        let cancel = CancelToken::new();
        let task = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            task.cancel();
        });
        let result = tokio::time::timeout(
            Duration::from_millis(800),
            adapter.call(
                "echo",
                asterpath_protocol::JsonString::new("{}"),
                &context(cancel),
            ),
        )
        .await;
        assert!(result.is_err(), "call resolved on its own: {result:?}");
        let follow_up = adapter
            .call(
                "echo",
                asterpath_protocol::JsonString::new("{}"),
                &context(CancelToken::new()),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(follow_up, asterpath_protocol::ToolError::Transport { .. }),
            "{follow_up:?}"
        );
        adapter.shutdown().await;
        assert_no_pids(&fixture, delay);
    }

    /// Pids whose command line names this fixture with this unique delay arg.
    #[cfg(target_os = "linux")]
    fn matching_pids(fixture: &std::path::Path, delay: &str) -> Vec<i32> {
        let fixture = fixture.to_string_lossy();
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

    #[cfg(target_os = "linux")]
    fn assert_no_pids(fixture: &std::path::Path, delay: &str) {
        for _ in 0..100 {
            if matching_pids(fixture, delay).is_empty() {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!(
            "processes still running for fixture {} delay {delay}: {:?}",
            fixture.display(),
            matching_pids(fixture, delay)
        );
    }
}
