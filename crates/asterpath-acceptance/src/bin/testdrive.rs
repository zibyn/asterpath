//! Acceptance driver: real SQLite and real modules, scripted provider/clock.
//!
//! This binary is test glue. It composes the same facade the reference CLI
//! uses; only the provider and the clock are scripted. Progress is reported as
//! JSON lines on stdout so the parent test can wait for a window and `SIGKILL`.

use std::collections::HashSet;
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use asterpath::{Asterpath, AsterpathBuilder};
use asterpath_core::{Clock, EventLog, InputId, LogQuery, SessionCatalog, SessionId, Storage};
use asterpath_plugin_host::{PluginHost, PluginManifest, PluginStatus};
use asterpath_protocol::{
    AgentEvent, ApprovalPolicy, ApprovalRequest, ApprovalVerdict, Item, ModelId, ModelRequest,
    ModelSpec, Part, Provider, ProviderCapabilities, ProviderError, ProviderErrorKind, ProviderId,
    RequestConfig, SessionSpec, StreamEvent, Tool, ToolArgs, ToolConfig, ToolContext,
    ToolDescriptor, ToolError, ToolName, ToolOutput, ToolRegistry, ToolSelection, TurnConfig,
    TurnEndReason, UserInput, decode_entry,
};
use asterpath_sandbox::{SandboxPolicy, SandboxProvider};
use asterpath_sandbox_linux::LinuxSandbox;
use asterpath_testing::{
    Failpoint, FailpointEventLog, FailpointTool, Fixture, FixtureStep, ManualClock, Point,
    ScriptedProvider,
};
use asterpath_tools::InMemoryToolRegistry;
use futures_util::future::BoxFuture;
use futures_util::{StreamExt, stream};
use serde::Deserialize;
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
    plugin_manifests: Vec<PathBuf>,
    sandbox: SandboxPolicy,
    sandbox_provider: bool,
    kill_grace: Duration,
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
    let mut plugin_manifests = Vec::new();
    let mut policy = "full".to_owned();
    let mut writable_roots = Vec::new();
    let mut sandbox_provider = true;
    let mut kill_grace = ToolConfig::default().kill_grace;
    let mut args = std::env::args().skip(2);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => db = args.next().map(PathBuf::from),
            "--fixture" => fixture = args.next().map(PathBuf::from),
            "--session" => session = args.next(),
            "--provider" => provider = args.next(),
            "--base-url" => base_url = args.next(),
            "--plugin-manifest" => match args.next() {
                Some(path) => plugin_manifests.push(PathBuf::from(path)),
                None => return Ok(usage_error("--plugin-manifest needs a value")),
            },
            "--sandbox" => match args.next() {
                Some(value) => policy = value,
                None => return Ok(usage_error("--sandbox needs a value")),
            },
            "--writable-root" => match args.next() {
                Some(path) => writable_roots.push(PathBuf::from(path)),
                None => return Ok(usage_error("--writable-root needs a value")),
            },
            "--no-sandbox-provider" => sandbox_provider = false,
            "--kill-grace-ms" => match args.next() {
                Some(value) => {
                    let millis = value
                        .parse::<u64>()
                        .map_err(|_| format!("invalid --kill-grace-ms value {value}"))?;
                    kill_grace = Duration::from_millis(millis);
                }
                None => return Ok(usage_error("--kill-grace-ms needs a value")),
            },
            "--failpoint" => match args.next() {
                Some(spec) => specs.push(spec),
                None => return Ok(usage_error("--failpoint needs a value")),
            },
            other => {
                return Ok(usage_error(&format!("unknown argument {other}")));
            }
        }
    }
    let sandbox = match policy.as_str() {
        "full" => SandboxPolicy::FullAccess,
        "read-only" => SandboxPolicy::ReadOnly,
        "workspace-write" => SandboxPolicy::WorkspaceWrite { writable_roots },
        other => return Ok(usage_error(&format!("unknown sandbox policy {other}"))),
    };
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
        plugin_manifests,
        sandbox,
        sandbox_provider,
        kill_grace,
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

struct LoadedFixture {
    fixture: Fixture,
    /// Present when the fixture scripts responses by request content instead
    /// of in call order (parallel fan-out).
    routing: Option<RoutingFixture>,
}

#[derive(Deserialize)]
struct RoutingFixture {
    provider: String,
    routes: Vec<Route>,
}

#[derive(Clone, Deserialize)]
struct Route {
    #[serde(default)]
    name: String,
    /// Matched against the concatenated text of message and tool-result items.
    #[serde(default)]
    contains: Option<String>,
    /// Minimum number of `ToolResult` items the request must carry.
    #[serde(default)]
    tool_results: usize,
    /// One of the parallel fan-out children.
    #[serde(default)]
    child: bool,
    response: FixtureStep,
}

impl Route {
    fn matches(&self, texts: &[String], tool_results: usize) -> bool {
        tool_results >= self.tool_results
            && self
                .contains
                .as_ref()
                .is_none_or(|needle| texts.iter().any(|text| text.contains(needle)))
    }
}

fn fixture_from(path: &Path) -> Result<LoadedFixture, String> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| format!("cannot read fixture {}: {error}", path.display()))?;
    let value: serde_json::Value = serde_json::from_str(&contents)
        .map_err(|error| format!("invalid fixture {}: {error}", path.display()))?;
    let routing = if value.get("routes").is_some() {
        Some(
            serde_json::from_value(value.clone())
                .map_err(|error| format!("invalid fixture {}: {error}", path.display()))?,
        )
    } else {
        None
    };
    let fixture = serde_json::from_value(value)
        .map_err(|error| format!("invalid fixture {}: {error}", path.display()))?;
    Ok(LoadedFixture { fixture, routing })
}

/// Routes each request by content: call order is not deterministic once two
/// children are driven in parallel, and a crash replay makes it worse.
struct RoutingProvider {
    id: ProviderId,
    routes: Vec<Route>,
    served_children: Mutex<HashSet<String>>,
    waiting_emitted: AtomicBool,
    /// Run mode only: hold the last child's response so the parent cannot
    /// finish before the parent test's `SIGKILL`.
    hold: Option<Duration>,
}

impl RoutingProvider {
    fn new(id: ProviderId, routes: Vec<Route>, hold: Option<Duration>) -> Self {
        Self {
            id,
            routes,
            served_children: Mutex::new(HashSet::new()),
            waiting_emitted: AtomicBool::new(false),
            hold,
        }
    }

    fn child_count(&self) -> usize {
        self.routes.iter().filter(|route| route.child).count()
    }

    /// Marks a child route served; true when this call completed the set.
    fn served_last_child(&self, name: &str) -> bool {
        let mut served = self
            .served_children
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        served.insert(name.to_owned());
        let all = self
            .routes
            .iter()
            .filter(|route| route.child)
            .all(|route| served.contains(&route.name));
        all && !self.waiting_emitted.swap(true, Ordering::SeqCst)
    }
}

fn request_texts(request: &ModelRequest) -> Vec<String> {
    let mut texts = Vec::new();
    for item in &request.items {
        let parts = match item {
            Item::Message { parts, .. } | Item::ToolResult { parts, .. } => parts,
            _ => continue,
        };
        for part in parts {
            if let Part::Text { text } = part {
                texts.push(text.clone());
            }
        }
    }
    texts
}

impl Provider for RoutingProvider {
    fn id(&self) -> ProviderId {
        self.id.clone()
    }

    fn capabilities(&self, _spec: &ModelSpec) -> ProviderCapabilities {
        ProviderCapabilities::default()
    }

    fn stream(
        &self,
        request: ModelRequest,
    ) -> stream::BoxStream<'_, Result<StreamEvent, ProviderError>> {
        let texts = request_texts(&request);
        let tool_results = request
            .items
            .iter()
            .filter(|item| matches!(item, Item::ToolResult { .. }))
            .count();
        let Some(route) = self
            .routes
            .iter()
            .find(|route| route.matches(&texts, tool_results))
        else {
            return stream::once(async {
                Err(ProviderError::new(
                    ProviderErrorKind::StreamProtocol,
                    "no scripted route matches the request",
                ))
            })
            .boxed();
        };
        emit(json!({
            "event": "model_request",
            "route": route.name,
            "tool_results": tool_results,
            "texts": texts,
        }));
        if let Some(hold) = self.hold
            && route.child
            && self.served_last_child(&route.name)
        {
            emit(json!({"event": "parent_waiting", "children": self.child_count()}));
            std::thread::sleep(hold);
        }
        stream::iter(route.response.events()).boxed()
    }
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
    let (plugin_tools, plugin_registry) = load_plugins(args).await?;
    let mut tools: Vec<Arc<dyn Tool>> = Vec::new();
    let hold = Duration::from_millis(hold_millis(args.command == "run"));
    for name in &fixture.tools {
        // External names are registered by plugin-host (or, after a resume
        // without a manifest, intentionally absent).
        if name.as_str().contains("__") {
            continue;
        }
        // Plugin tools are already registered by the plugin host.
        if plugin_registry
            .as_ref()
            .is_some_and(|registry| registry.get(name).is_some())
        {
            continue;
        }
        let mut tool: Arc<dyn Tool> = match name.as_str() {
            "set_marker" => Arc::new(SetMarker::new(effect_file()?, hold)),
            "append_line" => Arc::new(AppendLine::new(effect_file()?, hold)),
            // Registered by the scheduler module in the real composition root.
            "spawn_agent" => continue,
            other => return Err(format!("unknown fixture tool {other}")),
        };
        for failpoint in args.failpoints.iter().filter(|fp| fp.point.is_tool()) {
            tool = Arc::new(FailpointTool::new(tool, failpoint.clone()));
        }
        tools.push(tool);
    }
    tools.extend(plugin_tools);

    let provider: Arc<dyn Provider> = provider;
    let mut builder = AsterpathBuilder::new()
        .providers(vec![provider])
        .tools(tools)
        .clock(clock)
        .tool(ToolConfig {
            kill_grace: args.kill_grace,
            ..ToolConfig::default()
        });
    if !args.plugin_manifests.is_empty() {
        // External tools are `Ask` by default and there is no approval endpoint
        // in this driver, so the M5 scenarios allow them explicitly.
        builder = builder.approval(Arc::new(AllowExternal));
    }
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

/// Loads the M5 plugin manifests and returns the registered external tools.
async fn load_plugins(
    args: &Args,
) -> Result<(Vec<Arc<dyn Tool>>, Option<Arc<InMemoryToolRegistry>>), String> {
    if args.plugin_manifests.is_empty() {
        return Ok((Vec::new(), None));
    }
    let registry = Arc::new(InMemoryToolRegistry::new());
    let sandbox: Option<Arc<dyn SandboxProvider>> =
        if args.sandbox_provider && !matches!(args.sandbox, SandboxPolicy::FullAccess) {
            Some(Arc::new(LinuxSandbox::new()))
        } else {
            None
        };
    let host = PluginHost::new(registry.clone(), sandbox, args.sandbox.clone());
    let manifests = read_manifests(&args.plugin_manifests)?;
    match host.load(manifests).await {
        Ok(statuses) => {
            for status in &statuses {
                emit_plugin_status(status);
                if let PluginStatus::Failed { stage, message } = status {
                    return Err(format!("plugin failed at {stage:?}: {message}"));
                }
            }
        }
        Err(error) => {
            emit(json!({
                "event": "plugin_status",
                "status": "Failed",
                "id": error.plugin.as_str(),
                "stage": format!("{:?}", error.stage),
                "message": error.message,
            }));
            return Err(error.to_string());
        }
    }
    let tools = registry.list();
    Ok((tools, Some(registry)))
}

/// Each file holds one manifest object or an array of them.
fn read_manifests(paths: &[PathBuf]) -> Result<Vec<PluginManifest>, String> {
    let mut manifests = Vec::new();
    for path in paths {
        let text = std::fs::read_to_string(path)
            .map_err(|error| format!("cannot read manifest {}: {error}", path.display()))?;
        let value: serde_json::Value = serde_json::from_str(&text)
            .map_err(|error| format!("invalid manifest {}: {error}", path.display()))?;
        let items = match value {
            serde_json::Value::Array(items) => items,
            value => vec![value],
        };
        for item in items {
            manifests.push(
                serde_json::from_value(item)
                    .map_err(|error| format!("invalid manifest {}: {error}", path.display()))?,
            );
        }
    }
    Ok(manifests)
}

fn emit_plugin_status(status: &PluginStatus) {
    let value = match status {
        PluginStatus::Loaded { tools } => json!({
            "event": "plugin_status",
            "status": "Loaded",
            "tools": tools.iter().map(|tool| tool.as_str()).collect::<Vec<_>>(),
        }),
        PluginStatus::Failed { stage, message } => json!({
            "event": "plugin_status",
            "status": "Failed",
            "stage": format!("{stage:?}"),
            "message": message,
        }),
        PluginStatus::Skipped { reason } => json!({
            "event": "plugin_status",
            "status": "Skipped",
            "reason": reason,
        }),
        _ => return,
    };
    emit(value);
}

/// Test-only: the driver has no approval endpoint, so external tools pass.
struct AllowExternal;

impl ApprovalPolicy for AllowExternal {
    fn classify(&self, _request: &ApprovalRequest) -> ApprovalVerdict {
        ApprovalVerdict::Allow
    }
}

async fn run_session(args: &Args, run_mode: bool) -> Result<i32, String> {
    let path = args
        .fixture
        .as_ref()
        .ok_or_else(|| "--fixture is required".to_owned())?;
    let loaded = fixture_from(path)?;
    let fixture = &loaded.fixture;

    let scripted = if args.provider.is_none() && loaded.routing.is_none() {
        let mut provider = ScriptedProvider::from_fixture(fixture);
        if run_mode && fixture.stream_open_first_attempt {
            provider = provider.stream_open_on_first_attempt();
        }
        Some(Arc::new(provider))
    } else {
        None
    };
    // Run mode holds the last child's response so the parent cannot finish
    // before the test's `SIGKILL` at `parent_waiting`.
    let routing = match &loaded.routing {
        Some(routing) if args.provider.is_none() => Some(Arc::new(RoutingProvider::new(
            ProviderId::new(&routing.provider),
            routing.routes.clone(),
            run_mode.then(|| Duration::from_secs(30)),
        ))),
        _ => None,
    };
    let provider: Arc<dyn Provider> = if let Some(scripted) = &scripted {
        Arc::clone(scripted) as Arc<dyn Provider>
    } else if let Some(routing) = &routing {
        Arc::clone(routing) as Arc<dyn Provider>
    } else {
        real_provider(
            args.provider.as_deref().expect("provider parsed"),
            args.base_url.as_deref().expect("base url parsed"),
        )?
    };
    let asterpath = assemble(args, fixture, provider).await?;

    let session = if run_mode {
        let spec = session_spec(fixture, args.provider.as_deref());
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
        // `--session` names the follow-up session to drive (scenario (c)
        // resumes the parent, whose spawn replay recovers the children).
        let session = match args.session.as_deref() {
            Some(hex) => parse_session_id(hex)?,
            None => {
                let Some(session) = wakeable.first().copied() else {
                    marker("nothing_to_do");
                    asterpath
                        .shutdown()
                        .await
                        .map_err(|error| error.to_string())?;
                    return Ok(0);
                };
                session
            }
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
            AgentEvent::ToolStarted { tool, .. } => {
                emit(json!({"event": "tool_started", "tool": tool.as_str()}));
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
