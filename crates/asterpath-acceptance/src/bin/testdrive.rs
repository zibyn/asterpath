//! Acceptance driver: real SQLite and real modules, scripted provider/clock.
//!
//! This binary is test glue. It composes the same facade the reference CLI
//! uses; only the provider and the clock are scripted. Progress is reported as
//! JSON lines on stdout so the parent test can wait for a window and `SIGKILL`.

use std::io::Write;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use asterpath::{Asterpath, AsterpathBuilder};
use asterpath_core::{Clock, EventLog, InputId, LogQuery, SessionCatalog, SessionId, Storage};
use asterpath_protocol::{
    AgentEvent, Item, ModelId, ModelSpec, Part, Provider, ProviderId, RequestConfig, SessionSpec,
    Tool, ToolArgs, ToolContext, ToolDescriptor, ToolError, ToolName, ToolOutput, ToolSelection,
    TurnConfig, TurnEndReason, UserInput, decode_entry,
};
use asterpath_testing::{
    Failpoint, FailpointEventLog, FailpointTool, Fixture, ManualClock, Point, ScriptedProvider,
};
use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use serde_json::json;

const INPUT: InputId = InputId::from_bytes([42; 16]);

fn main() {
    let code = match run() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("testdrive: {error}");
            1
        }
    };
    std::process::exit(code);
}

struct Args {
    command: String,
    db: PathBuf,
    fixture: Option<PathBuf>,
    session: Option<String>,
    failpoints: Vec<Failpoint>,
    /// When set, use the real adapter for this id instead of the scripted one.
    provider: Option<String>,
    base_url: Option<String>,
}

fn usage_error(message: &str) -> i32 {
    eprintln!("testdrive: {message}");
    2
}

fn run() -> Result<i32, String> {
    let Some(command) = std::env::args().nth(1) else {
        return Ok(usage_error(
            "usage: testdrive <run|resume|inspect> --db <path> [--fixture <path>] [--provider <id> --base-url <url>]",
        ));
    };
    let mut db = None;
    let mut fixture = None;
    let mut session = None;
    let mut specs = Vec::new();
    let mut provider = None;
    let mut base_url = None;
    let mut args = std::env::args().skip(2);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => db = args.next().map(PathBuf::from),
            "--fixture" => fixture = args.next().map(PathBuf::from),
            "--session" => session = args.next(),
            "--provider" => provider = args.next(),
            "--base-url" => base_url = args.next(),
            "--failpoint" => match args.next() {
                Some(spec) => specs.push(spec),
                None => return Ok(usage_error("--failpoint needs a value")),
            },
            other => {
                return Ok(usage_error(&format!("unknown argument {other}")));
            }
        }
    }
    let Some(db) = db else {
        return Ok(usage_error("--db is required"));
    };
    if provider.is_some() != base_url.is_some() {
        return Ok(usage_error(
            "--provider and --base-url are required together",
        ));
    }
    let failpoints = match parse_failpoints(&specs) {
        Ok(failpoints) => failpoints,
        Err(error) => return Ok(usage_error(&error)),
    };
    let args = Args {
        command,
        db,
        fixture,
        session,
        failpoints,
        provider,
        base_url,
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(execute(args))
}

fn parse_failpoints(specs: &[String]) -> Result<Vec<Failpoint>, String> {
    let mut failpoints = Vec::new();
    for spec in specs {
        let (name, occurrence) = match spec.split_once('=') {
            Some((name, occurrence)) => (name, Some(occurrence)),
            None => (spec.as_str(), None),
        };
        let Some(point) = Point::parse(name) else {
            return Err(format!("unknown failpoint {name}"));
        };
        let mut failpoint = Failpoint::halt(point);
        if let Some(occurrence) = occurrence {
            let occurrence = occurrence
                .parse::<usize>()
                .ok()
                .and_then(NonZeroUsize::new)
                .ok_or_else(|| format!("invalid occurrence in {spec}"))?;
            failpoint = failpoint.with_occurrence(occurrence);
        }
        failpoints.push(failpoint);
    }
    Ok(failpoints)
}

async fn execute(args: Args) -> Result<i32, String> {
    match args.command.as_str() {
        "run" => run_session(&args, true).await,
        "resume" => run_session(&args, false).await,
        "inspect" => inspect(&args).await,
        other => Ok(usage_error(&format!("unknown command {other}"))),
    }
}

fn fixture_from(path: &Path) -> Result<Fixture, String> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| format!("cannot read fixture {}: {error}", path.display()))?;
    serde_json::from_str(&contents)
        .map_err(|error| format!("invalid fixture {}: {error}", path.display()))
}

fn hold_millis(run_mode: bool) -> u64 {
    match std::env::var("ASTERPATH_TOOL_HOLD_MS") {
        Ok(value) => value.parse().unwrap_or(0),
        Err(_) if run_mode => 30_000,
        Err(_) => 0,
    }
}

/// Real adapters differ only in this table row: an id, its key variable and its
/// constructor. The scenario code below never branches on the provider.
struct Adapter {
    id: &'static str,
    key_env: &'static str,
    build: fn(&str, &str) -> Arc<dyn Provider>,
}

fn build_anthropic(api_key: &str, base_url: &str) -> Arc<dyn Provider> {
    Arc::new(asterpath::anthropic::AnthropicProvider::new(api_key).with_base_url(base_url))
}

fn build_openai(api_key: &str, base_url: &str) -> Arc<dyn Provider> {
    Arc::new(asterpath::openai::OpenAiProvider::new(api_key).with_base_url(base_url))
}

const ADAPTERS: &[Adapter] = &[
    Adapter {
        id: "anthropic",
        key_env: "ANTHROPIC_API_KEY",
        build: build_anthropic,
    },
    Adapter {
        id: "openai",
        key_env: "OPENAI_API_KEY",
        build: build_openai,
    },
];

fn real_provider(id: &str, base_url: &str) -> Result<Arc<dyn Provider>, String> {
    let adapter = ADAPTERS
        .iter()
        .find(|adapter| adapter.id == id)
        .ok_or_else(|| format!("unknown provider {id}"))?;
    let api_key = std::env::var(adapter.key_env)
        .map_err(|_| format!("{} is required for provider {id}", adapter.key_env))?;
    Ok((adapter.build)(&api_key, base_url))
}

fn effect_file() -> Result<PathBuf, String> {
    std::env::var("ASTERPATH_EFFECT_FILE")
        .map(PathBuf::from)
        .map_err(|_| "ASTERPATH_EFFECT_FILE is required for tool scenarios".to_owned())
}

fn emit(value: serde_json::Value) {
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    let _ = writeln!(stdout, "{value}");
    let _ = stdout.flush();
}

fn marker(event: &str) {
    emit(json!({"event": event}));
}

fn now(clock: &ManualClock) -> i64 {
    clock.now()
}

async fn scan(
    log: Arc<dyn EventLog>,
    session: SessionId,
) -> Result<Vec<asterpath_core::LogEntry>, String> {
    let mut stream = log.scan(LogQuery {
        session,
        after: None,
        limit: None,
    });
    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await {
        entries.push(entry.map_err(|error| error.to_string())?);
    }
    Ok(entries)
}

async fn assemble(
    args: &Args,
    fixture: &Fixture,
    provider: Arc<dyn Provider>,
) -> Result<Asterpath, String> {
    let clock = Arc::new(ManualClock::new());
    let mut tools: Vec<Arc<dyn Tool>> = Vec::new();
    let hold = Duration::from_millis(hold_millis(args.command == "run"));
    for name in &fixture.tools {
        let mut tool: Arc<dyn Tool> = match name.as_str() {
            "set_marker" => Arc::new(SetMarker::new(effect_file()?, hold)),
            "append_line" => Arc::new(AppendLine::new(effect_file()?, hold)),
            other => return Err(format!("unknown fixture tool {other}")),
        };
        for failpoint in args.failpoints.iter().filter(|fp| fp.point.is_tool()) {
            tool = Arc::new(FailpointTool::new(tool, failpoint.clone()));
        }
        tools.push(tool);
    }

    let provider: Arc<dyn Provider> = provider;
    let mut builder = AsterpathBuilder::new()
        .providers(vec![provider])
        .tools(tools)
        .clock(clock);
    if !args.failpoints.is_empty() {
        let backend =
            asterpath_storage_sqlite::open(&args.db).map_err(|error| error.to_string())?;
        let mut log: Arc<dyn EventLog> = backend.clone();
        for failpoint in args.failpoints.iter().filter(|fp| fp.point.is_append()) {
            log = Arc::new(FailpointEventLog::new(log, failpoint.clone()));
        }
        let storage: Arc<dyn Storage> = backend.clone();
        let catalog: Arc<dyn SessionCatalog> = backend;
        builder = builder
            .event_log(log)
            .storage(storage)
            .session_catalog(catalog);
    } else {
        builder = builder.database(&args.db);
    }
    builder.build().await.map_err(|error| error.to_string())
}

async fn run_session(args: &Args, run_mode: bool) -> Result<i32, String> {
    let path = args
        .fixture
        .as_ref()
        .ok_or_else(|| "--fixture is required".to_owned())?;
    let fixture = fixture_from(path)?;

    let scripted = if args.provider.is_none() {
        let mut provider = ScriptedProvider::from_fixture(&fixture);
        if run_mode && fixture.stream_open_first_attempt {
            provider = provider.stream_open_on_first_attempt();
        }
        Some(Arc::new(provider))
    } else {
        None
    };
    let provider: Arc<dyn Provider> = match &scripted {
        Some(scripted) => Arc::clone(scripted) as Arc<dyn Provider>,
        None => real_provider(
            args.provider.as_deref().expect("provider parsed"),
            args.base_url.as_deref().expect("base url parsed"),
        )?,
    };
    let asterpath = assemble(args, &fixture, provider).await?;

    let session = if run_mode {
        let spec = session_spec(&fixture, args.provider.as_deref());
        let handle = asterpath
            .create_session(spec)
            .await
            .map_err(|error| error.to_string())?;
        emit(json!({"event": "session", "id": handle.id().to_hex()}));
        handle
            .submit_input(UserInput {
                id: INPUT,
                parts: vec![Part::Text {
                    text: "go".to_owned(),
                }],
            })
            .await
            .map_err(|error| error.to_string())?;
        handle
    } else {
        let clock = ManualClock::new();
        let wakeable = asterpath
            .storage()
            .wakeable(now(&clock))
            .await
            .map_err(|error| error.to_string())?;
        emit(
            json!({"event": "wakeable", "sessions": wakeable.iter().map(|session| session.to_hex()).collect::<Vec<_>>()}),
        );
        let Some(session) = wakeable.first().copied() else {
            marker("nothing_to_do");
            asterpath
                .shutdown()
                .await
                .map_err(|error| error.to_string())?;
            return Ok(0);
        };
        let entries = scan(asterpath.event_log(), session).await?;
        let committed_attempts = entries
            .iter()
            .filter_map(|entry| decode_entry(entry).ok())
            .filter(|event| match event {
                asterpath_protocol::SessionEvent::AssistantMessage { .. } => true,
                // A crash abort is appended by recovery itself: the next retry
                // replays that same scripted step.
                asterpath_protocol::SessionEvent::ModelCallAborted { kind, .. } => {
                    *kind != asterpath_protocol::AbortKind::Crash
                }
                _ => false,
            })
            .count();
        if let Some(scripted) = &scripted {
            scripted.skip(committed_attempts);
        }
        emit(json!({"event": "session", "id": session.to_hex()}));
        asterpath
            .open_session(session)
            .await
            .map_err(|error| error.to_string())?
    };

    let mut stream = session.drive();
    let mut cancelled = false;
    while let Some(event) = stream.next().await {
        let event = event.map_err(|error| error.to_string())?;
        match event {
            AgentEvent::ModelDelta { .. } if fixture.cancel_on_first_delta && !cancelled => {
                cancelled = true;
                session.cancel().await.map_err(|error| error.to_string())?;
            }
            AgentEvent::TurnCommitted { reason, .. } => {
                emit(json!({"event": "turn_committed", "reason": reason_name(&reason)}));
            }
            AgentEvent::Idle { .. } => marker("idle"),
            _ => {}
        }
    }
    drop(stream);

    if let Some(scripted) = &scripted {
        for (index, request) in scripted.requests().iter().enumerate() {
            let tool_results = request
                .items
                .iter()
                .filter(|item| matches!(item, Item::ToolResult { .. }))
                .count();
            emit(json!({"event": "model_request", "index": index, "tool_results": tool_results}));
        }
    }
    marker("drive_complete");
    asterpath
        .shutdown()
        .await
        .map_err(|error| error.to_string())?;
    Ok(0)
}

fn session_spec(fixture: &Fixture, provider: Option<&str>) -> SessionSpec {
    SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new(provider.unwrap_or(&fixture.provider)),
            model: ModelId::new(fixture.model.clone()),
            context_window: None,
        },
        system: fixture.system.clone(),
        tools: ToolSelection::Names(fixture.tools.clone()),
        request: RequestConfig::default(),
        turn: TurnConfig {
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            ..TurnConfig::default()
        },
        compaction: asterpath_protocol::CompactionConfig::default(),
        subagents: asterpath_protocol::SubagentConfig::default(),
    }
}

fn reason_name(reason: &TurnEndReason) -> String {
    match reason {
        TurnEndReason::Completed => "Completed".to_owned(),
        TurnEndReason::Cancelled => "Cancelled".to_owned(),
        TurnEndReason::Failed => "Failed".to_owned(),
        TurnEndReason::Stopped { reason } => format!("Stopped:{reason}"),
        TurnEndReason::MaxStepsExceeded => "MaxStepsExceeded".to_owned(),
        TurnEndReason::ContextExhausted => "ContextExhausted".to_owned(),
        TurnEndReason::CompactionFailed => "CompactionFailed".to_owned(),
        other => format!("{other:?}"),
    }
}

async fn inspect(args: &Args) -> Result<i32, String> {
    let backend = asterpath_storage_sqlite::open(&args.db).map_err(|error| error.to_string())?;
    let storage: Arc<dyn Storage> = backend.clone();
    let log: Arc<dyn EventLog> = backend;
    let clock = ManualClock::new();
    let wakeable = storage
        .wakeable(now(&clock))
        .await
        .map_err(|error| error.to_string())?;
    emit(
        json!({"event": "wakeable", "sessions": wakeable.iter().map(|session| session.to_hex()).collect::<Vec<_>>()}),
    );
    if let Some(hex) = &args.session {
        let bytes = parse_session_id(hex)?;
        let entries = scan(log, bytes).await?;
        for entry in &entries {
            let kind = decode_entry(entry)
                .map(|event| event.kind())
                .unwrap_or("undecodable");
            emit(json!({"event": "log", "session": hex, "seq": entry.seq, "kind": kind}));
        }
        emit(json!({"event": "inspect_complete", "session": hex, "entries": entries.len()}));
    }
    Ok(0)
}

fn parse_session_id(hex: &str) -> Result<SessionId, String> {
    let bytes: [u8; 16] = hex
        .as_bytes()
        .chunks(2)
        .map(|pair| {
            std::str::from_utf8(pair)
                .ok()
                .and_then(|value| u8::from_str_radix(value, 16).ok())
                .ok_or_else(|| format!("invalid session id {hex}"))
        })
        .collect::<Result<Vec<u8>, String>>()?
        .try_into()
        .map_err(|_| format!("invalid session id {hex}"))?;
    Ok(SessionId::from_bytes(bytes))
}

fn descriptor(name: &str, replay: asterpath_protocol::ReplaySafety) -> ToolDescriptor {
    ToolDescriptor {
        name: ToolName::new(name),
        description: String::new(),
        schema: serde_json::json!({"type": "object"}),
        parallelism: asterpath_protocol::Parallelism::Parallel,
        replay,
        side_effect: asterpath_protocol::SideEffect::Mutating,
        provenance: asterpath_protocol::Provenance::InProcess,
        timeout: None,
        max_output_bytes: None,
    }
}

/// Idempotent overwrite: a crash after the write must not duplicate anything.
struct SetMarker {
    descriptor: ToolDescriptor,
    effect: PathBuf,
    hold: Duration,
}

impl SetMarker {
    fn new(effect: PathBuf, hold: Duration) -> Self {
        Self {
            descriptor: descriptor("set_marker", asterpath_protocol::ReplaySafety::Safe),
            effect,
            hold,
        }
    }
}

impl Tool for SetMarker {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        args: ToolArgs,
        _ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async move {
            let parsed: serde_json::Value =
                serde_json::from_str(args.as_str()).map_err(|error| ToolError::InvalidArgs {
                    message: error.to_string(),
                })?;
            let key = parsed
                .get("key")
                .and_then(|value| value.as_str())
                .unwrap_or("k");
            let value = parsed
                .get("value")
                .and_then(|value| value.as_str())
                .unwrap_or("v");
            let mut map = serde_json::Map::new();
            map.insert(key.to_owned(), serde_json::Value::String(value.to_owned()));
            let body = serde_json::to_string(&map).map_err(|error| ToolError::Internal {
                message: error.to_string(),
            })?;
            std::fs::write(&self.effect, body).map_err(|error| ToolError::Internal {
                message: error.to_string(),
            })?;
            marker("effect_applied");
            tokio::time::sleep(self.hold).await;
            Ok(ToolOutput::new(vec![Part::Text {
                text: format!("{key}={value}"),
            }]))
        })
    }
}

/// Append-only side effect: replaying it would be visible, so it is `Unsafe`.
struct AppendLine {
    descriptor: ToolDescriptor,
    effect: PathBuf,
    hold: Duration,
}

impl AppendLine {
    fn new(effect: PathBuf, hold: Duration) -> Self {
        Self {
            descriptor: descriptor("append_line", asterpath_protocol::ReplaySafety::Unsafe),
            effect,
            hold,
        }
    }
}

impl Tool for AppendLine {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        _args: ToolArgs,
        _ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async move {
            use std::fs::OpenOptions;
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.effect)
                .map_err(|error| ToolError::Internal {
                    message: error.to_string(),
                })?;
            writeln!(file, "line").map_err(|error| ToolError::Internal {
                message: error.to_string(),
            })?;
            marker("effect_applied");
            tokio::time::sleep(self.hold).await;
            Ok(ToolOutput::new(vec![Part::Text {
                text: "appended".to_owned(),
            }]))
        })
    }
}
