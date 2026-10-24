//! Reference CLI: `run`, `resume`, `inspect`, `trace`, `usage` and `config check`.
//!
//! The binary owns host policy (exit codes, stdout/stderr, approval prompts) and
//! error reporting; it composes the same facade an embedder would.

mod config;

use std::io::{self, BufRead, IsTerminal, Write};
use std::path::PathBuf;
use std::sync::Arc;

use asterpath::{
    AsterError, Asterpath, AsterpathBuilder, AsterpathConfig, DefaultCompactor, SessionHandle,
};
use asterpath_mcp::{McpServers, ServerStatus};
use asterpath_protocol::scan_all;
use asterpath_protocol::{
    AgentEvent, ApprovalDecision, ApprovalRequest, ConfigSnapshot, Item, Part, ProviderCatalog,
    Role, SandboxPolicy, SandboxProvider, SessionState, Span, SpanId, SpanKind, SpanStatus, Tool,
    TurnEndReason, UsageScope, UsageTotals, UserInput, decode_entry, history,
};
use asterpath_protocol::{
    Clock, InputId, LogEntry, SessionId, SessionStore, SystemClock, unique_bytes,
};
use asterpath_skills::{
    DEFAULT_INDEX_BUDGET, LoadSkillTool, SkillDiagnostic, SkillDiagnosticKind, SkillRoot,
    SkillRootLabel, discover, render_index,
};
use clap::{Args, Parser, Subcommand};
use futures_util::StreamExt;

#[derive(Parser)]
#[command(name = "asterpath", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create a session, run one turn, and print the final assistant text.
    Run(RunArgs),
    /// Open a session and continue its open turn or pending input.
    Resume(SessionArgs),
    /// Print a projection summary and the open intents of a session.
    Inspect(SessionArgs),
    /// Print the span tree of a session.
    Trace(SessionArgs),
    /// Print own and descendant token usage of a session.
    Usage(SessionArgs),
    /// Configuration commands.
    Config(ConfigArgs),
}

#[derive(Args)]
struct RunArgs {
    #[command(flatten)]
    common: Common,
    /// Skip skill discovery for this new session.
    #[arg(long)]
    no_skills: bool,
    /// Register only these built-in tools (comma-separated).
    #[arg(long, value_delimiter = ',', conflicts_with = "no_tools")]
    tools: Option<Vec<String>>,
    /// Register no built-in tools.
    #[arg(long)]
    no_tools: bool,
    /// User input for the new session.
    prompt: String,
}

#[derive(Args)]
struct SessionArgs {
    #[command(flatten)]
    common: Common,
    /// Session id in hex.
    session: String,
}

#[derive(Args)]
struct ConfigArgs {
    #[command(subcommand)]
    command: ConfigCommand,
}

#[derive(Subcommand)]
enum ConfigCommand {
    /// Validate the configuration and report each MCP server's load status,
    /// without opening the database or the engine.
    Check(CheckArgs),
}

#[derive(Args)]
struct CheckArgs {
    #[command(flatten)]
    common: Common,
}

/// Options shared by every command. Unknown options (notably `--session` on
/// `run`) are usage errors, which clap exits with 2.
#[derive(Args)]
struct Common {
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long)]
    db: Option<PathBuf>,
    #[arg(long)]
    provider: Option<String>,
    #[arg(long)]
    model: Option<String>,
    #[arg(long, value_enum)]
    approval: Option<config::ApprovalMode>,
    /// Emit machine-readable output where the command supports it.
    #[arg(long)]
    json: bool,
}

impl Common {
    fn overrides(&self) -> config::Overrides {
        config::Overrides {
            config: self.config.clone(),
            db: self.db.clone(),
            provider: self.provider.clone(),
            model: self.model.clone(),
            approval: self.approval,
        }
    }
}

#[derive(Debug)]
struct CliError {
    code: i32,
    message: String,
}

impl CliError {
    fn config(message: impl Into<String>) -> Self {
        Self {
            code: 2,
            message: message.into(),
        }
    }

    fn run(message: impl Into<String>) -> Self {
        Self {
            code: 1,
            message: message.into(),
        }
    }
}

type CliResult<T> = Result<T, CliError>;

#[tokio::main]
async fn main() {
    let code = match dispatch(Cli::parse()).await {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("asterpath: {}", error.message);
            error.code
        }
    };
    std::process::exit(code);
}

async fn dispatch(cli: Cli) -> CliResult<()> {
    match cli.command {
        Command::Run(args) => run(args).await,
        Command::Resume(args) => resume(args).await,
        Command::Inspect(args) => inspect(args).await,
        Command::Trace(args) => trace(args).await,
        Command::Usage(args) => usage(args).await,
        Command::Config(args) => match args.command {
            ConfigCommand::Check(args) => config_check(args).await,
        },
    }
}

fn load(common: &Common) -> CliResult<config::Settings> {
    let settings = config::load(common.overrides()).map_err(CliError::config)?;
    config::validate(&settings).map_err(CliError::config)?;
    Ok(settings)
}

/// Resolve the approval mode the drive will use.
///
/// `ask` needs a live prompt, so without a TTY a configured or default `ask`
/// degrades to `deny`; an explicit `--approval ask` is a usage error because it
/// cannot be honoured.
fn effective_approval(
    resolved: config::ApprovalMode,
    explicit: Option<config::ApprovalMode>,
    tty: bool,
) -> CliResult<config::ApprovalMode> {
    if matches!(resolved, config::ApprovalMode::Ask) && !tty {
        if matches!(explicit, Some(config::ApprovalMode::Ask)) {
            return Err(CliError::config(
                "--approval ask requires an interactive terminal",
            ));
        }
        return Ok(config::ApprovalMode::Deny);
    }
    Ok(resolved)
}

fn tty() -> bool {
    io::stdin().is_terminal() && io::stdout().is_terminal()
}

async fn config_check(args: CheckArgs) -> CliResult<()> {
    let settings = load(&args.common)?;
    // The `enabled` switch only governs new sessions, so diagnostics are
    // always available here.
    let discovery = discover(&skill_roots(&settings));
    let (_, budget) = render_index(&discovery.skills, DEFAULT_INDEX_BUDGET);
    for diagnostic in discovery.diagnostics.iter().chain(&budget) {
        println!("skill: {diagnostic}");
    }
    builtin_tools(&settings, settings.tool_names())?;
    // Loading is the only way to report a server's status; stop them again.
    let mcp = load_mcp(&settings).await?;
    for line in mcp_status_lines(&settings, &mcp)? {
        println!("{line}");
    }
    mcp.shutdown().await;
    println!("config ok");
    Ok(())
}

async fn run(args: RunArgs) -> CliResult<()> {
    let settings = load(&args.common)?;
    let approval = effective_approval(settings.approval, args.common.approval, tty())?;
    let selection = if args.no_tools {
        Some(&[][..])
    } else {
        args.tools.as_deref().or(settings.tool_names())
    };
    let builtin = builtin_tools(&settings, selection)?;
    let mut skills = skill_setup(&settings, args.no_skills);
    for diagnostic in skills
        .diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.kind != SkillDiagnosticKind::AuthorityFieldIgnored)
    {
        eprintln!("skill: {diagnostic}");
    }
    let mcp = load_mcp(&settings).await?;
    report_mcp_failures(&settings, &mcp)?;
    let mut tools = builtin;
    tools.extend(skills.tools());
    tools.extend(mcp.tools());
    let mut spec = config::session_spec(&settings).map_err(CliError::config)?;
    if let Some(index) = skills.index.take() {
        spec.system.push(index);
    }
    let providers = config::providers(&settings).map_err(CliError::config)?;
    let asterpath = assemble(&settings, providers, approval, tools).await?;

    let handle = asterpath.create_session(spec).await.map_err(aster_error)?;
    eprintln!("session: {}", handle.id().to_hex());
    handle
        .submit_input(UserInput {
            id: fresh_input_id(),
            parts: vec![Part::Text { text: args.prompt }],
        })
        .await
        .map_err(aster_error)?;

    let outcome = drive_approving(&asterpath, &handle, approval).await?;
    finish(asterpath, mcp, &handle, outcome, false).await
}

/// Shared tail: print the result, close the composition root and map the turn
/// reason to the process exit code.
async fn finish(
    asterpath: Asterpath,
    mcp: McpServers,
    handle: &SessionHandle,
    outcome: DriveOutcome,
    print_idle: bool,
) -> CliResult<()> {
    if print_idle && outcome.idle {
        println!("idle");
    } else if let Some(text) = final_text(&asterpath, handle).await? {
        println!("{text}");
    }
    // Engine first, then the MCP servers its tools call (ADR-0027).
    let shutdown = asterpath.shutdown().await;
    mcp.shutdown().await;
    shutdown.map_err(aster_error)?;

    match outcome.reason {
        None | Some(TurnEndReason::Completed) => Ok(()),
        Some(other) => Err(CliError::run(format!("turn ended with {other:?}"))),
    }
}

async fn resume(args: SessionArgs) -> CliResult<()> {
    let settings = load(&args.common)?;
    let session = parse_session(&args.session)?;
    let snapshot = session_snapshot(&settings, session).await?;
    let approval = effective_approval(settings.approval, args.common.approval, tty())?;

    // The session's model provider is fixed at creation; the flag cannot move it.
    let mut settings = settings;
    settings.provider = Some(snapshot.model.provider.as_str().to_owned());
    let providers = config::providers(&settings).map_err(CliError::config)?;
    // `resume` keeps the frozen index and always registers the loader; the
    // `[skills] enabled` switch only governs new sessions.
    let mcp = load_mcp(&settings).await?;
    report_mcp_failures(&settings, &mcp)?;
    // The frozen snapshot decides what the model sees; register every
    // built-in tool so any subset chosen at `run` still resolves.
    let mut tools = builtin_tools(&settings, None)?;
    tools.push(Arc::new(LoadSkillTool::new(skill_roots(&settings))));
    tools.extend(mcp.tools());
    let asterpath = assemble(&settings, providers, approval, tools).await?;
    let handle = asterpath.open_session(session).await.map_err(aster_error)?;

    let outcome = drive_approving(&asterpath, &handle, approval).await?;
    finish(asterpath, mcp, &handle, outcome, true).await
}

async fn inspect(args: SessionArgs) -> CliResult<()> {
    let settings = load(&args.common)?;
    let session = parse_session(&args.session)?;
    if !settings.db.exists() {
        return Err(not_found(session));
    }
    let asterpath = bare(&settings).await?;

    let entries = scan(asterpath.store(), session)
        .await
        .map_err(CliError::run)?;
    if entries.is_empty() {
        return Err(not_found(session));
    }

    let handle = asterpath.open_session(session).await.map_err(aster_error)?;
    let facts = handle.facts().await.map_err(aster_error)?;
    let items: Vec<Item> = handle
        .projection()
        .await
        .map_err(aster_error)?
        .into_iter()
        .flat_map(|projected| projected.items)
        .collect();
    if args.common.json {
        let json = serde_json::to_string_pretty(&items)
            .map_err(|error| CliError::run(error.to_string()))?;
        println!("{json}");
    } else {
        println!("session: {}", session.to_hex());
        println!("state: {}", state_label(facts.state()));
        println!("entries: {}", entries.len());
        println!("projected items: {}", items.len());
        let open = facts.open_intents();
        if open.is_empty() {
            println!("open intents: none");
        } else {
            println!("open intents:");
            for intent in open {
                println!("  {intent:?}");
            }
        }
    }

    asterpath.shutdown().await.map_err(aster_error)?;
    Ok(())
}

async fn trace(args: SessionArgs) -> CliResult<()> {
    let settings = load(&args.common)?;
    let session = parse_session(&args.session)?;
    if !settings.db.exists() {
        return Err(not_found(session));
    }
    let asterpath = bare(&settings).await?;

    let spans = match asterpath.trace(session).await {
        Ok(spans) => spans,
        Err(error) => {
            let _ = asterpath.shutdown().await;
            return Err(CliError::run(error.to_string()));
        }
    };
    asterpath.shutdown().await.map_err(aster_error)?;

    if args.common.json {
        let json = serde_json::to_string_pretty(&spans)
            .map_err(|error| CliError::run(error.to_string()))?;
        println!("{json}");
    } else {
        print!("{}", render_tree(&spans));
    }
    Ok(())
}

async fn usage(args: SessionArgs) -> CliResult<()> {
    let settings = load(&args.common)?;
    let session = parse_session(&args.session)?;
    if !settings.db.exists() {
        return Err(not_found(session));
    }
    let asterpath = bare(&settings).await?;

    let report = asterpath.usage(UsageScope::SessionSubtree(session)).await;
    let report = match report {
        Ok(report) => report,
        Err(error) => {
            let _ = asterpath.shutdown().await;
            return Err(CliError::run(error.to_string()));
        }
    };
    asterpath.shutdown().await.map_err(aster_error)?;

    if args.common.json {
        let json = serde_json::to_string_pretty(&report)
            .map_err(|error| CliError::run(error.to_string()))?;
        println!("{json}");
    } else {
        print_totals("own", &report.own);
        print_totals("descendants", &report.descendants);
    }
    Ok(())
}

fn print_totals(label: &str, totals: &UsageTotals) {
    let input = totals.model_calls.input_total() + totals.compaction.input_total();
    let output = totals.model_calls.output.total + totals.compaction.output.total;
    println!(
        "{label}: input={input} output={output} total={}",
        totals.total()
    );
}

fn render_tree(spans: &[Span]) -> String {
    fn walk(spans: &[Span], parent: Option<SpanId>, depth: usize, out: &mut String) {
        for span in spans.iter().filter(|span| span.parent_span == parent) {
            out.push_str(&"  ".repeat(depth));
            out.push_str(kind_label(span.kind));
            out.push(' ');
            out.push_str(&span.name);
            if !matches!(span.status, SpanStatus::Ok | SpanStatus::Unset) {
                out.push_str(" [");
                out.push_str(&format!("{:?}", span.status).to_lowercase());
                out.push(']');
            }
            out.push('\n');
            walk(spans, Some(span.span), depth + 1, out);
        }
    }
    let mut out = String::new();
    walk(spans, None, 0, &mut out);
    out
}

fn kind_label(kind: SpanKind) -> &'static str {
    match kind {
        SpanKind::Session => "session",
        SpanKind::Turn => "turn",
        SpanKind::Step => "step",
        SpanKind::ModelCall => "model_call",
        SpanKind::ToolCall => "tool_call",
        SpanKind::Subagent => "subagent",
        SpanKind::Wait => "wait",
        SpanKind::Compaction => "compaction",
        _ => "span",
    }
}

/// One drive with the approval endpoint registered only when a prompt will
/// actually happen; otherwise the framework's `Ask` (gate closed) fails closed.
async fn drive_approving(
    asterpath: &Asterpath,
    handle: &SessionHandle,
    approval: config::ApprovalMode,
) -> CliResult<DriveOutcome> {
    let _endpoint =
        matches!(approval, config::ApprovalMode::Ask).then(|| asterpath.approval_gate().register());
    let mut prompt = if matches!(approval, config::ApprovalMode::Ask) {
        Some(PromptIo {
            reader: io::stdin().lock(),
            writer: io::stderr(),
        })
    } else {
        None
    };
    drive(handle, prompt.as_mut()).await
}

#[derive(Default)]
struct DriveOutcome {
    idle: bool,
    reason: Option<TurnEndReason>,
}

/// Drive to `TurnCommitted` or `Idle`. Injected prompt IO keeps the endpoint
/// unit-testable; there is no exit code for "stopped at a wait".
async fn drive<R: BufRead, W: Write>(
    handle: &SessionHandle,
    mut prompt: Option<&mut PromptIo<R, W>>,
) -> CliResult<DriveOutcome> {
    let mut outcome = DriveOutcome::default();
    {
        let mut stream = handle.drive();
        while let Some(event) = stream.next().await {
            match event {
                Ok(AgentEvent::ApprovalRequested(request)) => {
                    let decision = match prompt.as_deref_mut() {
                        Some(io) => prompt_decision(io, &request),
                        None => ApprovalDecision::Deny { reason: None },
                    };
                    handle
                        .approve(request.tool_call_id, decision)
                        .await
                        .map_err(aster_error)?;
                }
                Ok(AgentEvent::TurnCommitted { reason, .. }) => outcome.reason = Some(reason),
                Ok(AgentEvent::Idle { .. }) => outcome.idle = true,
                Ok(_) => {}
                Err(error) => return Err(CliError::run(format!("drive failed: {error}"))),
            }
        }
    }
    Ok(outcome)
}

struct PromptIo<R, W> {
    reader: R,
    writer: W,
}

/// Ask the operator; anything other than yes (including EOF) denies.
fn prompt_decision<R: BufRead, W: Write>(
    io: &mut PromptIo<R, W>,
    request: &ApprovalRequest,
) -> ApprovalDecision {
    let _ = writeln!(io.writer, "tool: {}", request.tool.as_str());
    let _ = writeln!(io.writer, "args: {}", request.args.as_str());
    let _ = writeln!(io.writer, "replay: {:?}", request.replay);
    let _ = writeln!(io.writer, "side_effect: {:?}", request.side_effect);
    let _ = writeln!(io.writer, "provenance: {:?}", request.provenance);
    let _ = write!(io.writer, "allow? [y/N] ");
    let _ = io.writer.flush();

    let mut answer = String::new();
    let allowed = io.reader.read_line(&mut answer).is_ok()
        && matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes");
    if allowed {
        ApprovalDecision::Allow
    } else {
        ApprovalDecision::Deny { reason: None }
    }
}

async fn final_text(asterpath: &Asterpath, handle: &SessionHandle) -> CliResult<Option<String>> {
    let entries = scan(asterpath.store(), handle.id())
        .await
        .map_err(CliError::run)?;
    Ok(final_assistant_text(&entries))
}

async fn session_snapshot(
    settings: &config::Settings,
    session: SessionId,
) -> CliResult<ConfigSnapshot> {
    if !settings.db.exists() {
        return Err(not_found(session));
    }
    let asterpath = bare(settings).await?;
    if let Err(error) = asterpath.open_session(session).await {
        let _ = asterpath.shutdown().await;
        return Err(aster_error(error));
    }
    let entries = scan(asterpath.store(), session)
        .await
        .map_err(CliError::run)?;
    let snapshot = entries
        .iter()
        .find_map(|entry| match decode_entry(entry).ok()? {
            asterpath_protocol::SessionEvent::SessionCreated { config, .. } => Some(config),
            _ => None,
        });
    asterpath.shutdown().await.map_err(aster_error)?;
    snapshot.ok_or_else(|| CliError::run(format!("session {session:?} has no SessionCreated")))
}

struct SkillSetup {
    roots: Vec<SkillRoot>,
    index: Option<Part>,
    diagnostics: Vec<SkillDiagnostic>,
}

impl SkillSetup {
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        if self.roots.is_empty() {
            Vec::new()
        } else {
            vec![Arc::new(LoadSkillTool::new(self.roots.clone()))]
        }
    }
}

/// New-session skill policy: `--no-skills` beats `[skills] enabled`.
fn skill_setup(settings: &config::Settings, no_skills: bool) -> SkillSetup {
    if no_skills || !settings.skills_enabled() {
        return SkillSetup {
            roots: Vec::new(),
            index: None,
            diagnostics: Vec::new(),
        };
    }
    let roots = skill_roots(settings);
    let discovery = discover(&roots);
    let (index, budget) = render_index(&discovery.skills, DEFAULT_INDEX_BUDGET);
    SkillSetup {
        roots,
        index,
        diagnostics: discovery.diagnostics.into_iter().chain(budget).collect(),
    }
}

/// Explicit paths, then the project and user roots in priority order.
fn skill_roots(settings: &config::Settings) -> Vec<SkillRoot> {
    let mut roots: Vec<SkillRoot> = settings
        .skill_paths()
        .iter()
        .map(|path| SkillRoot {
            path: path.clone(),
            label: SkillRootLabel::Explicit,
        })
        .collect();
    let cwd = std::env::current_dir().unwrap_or_default();
    roots.push(SkillRoot {
        path: cwd.join(".asterpath/skills"),
        label: SkillRootLabel::ProjectNative,
    });
    roots.push(SkillRoot {
        path: cwd.join(".agents/skills"),
        label: SkillRootLabel::ProjectAgents,
    });
    let config_home = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")));
    if let Some(home) = config_home {
        roots.push(SkillRoot {
            path: home.join("asterpath/skills"),
            label: SkillRootLabel::UserNative,
        });
    }
    if let Some(home) = std::env::var_os("HOME") {
        roots.push(SkillRoot {
            path: PathBuf::from(home).join(".agents/skills"),
            label: SkillRootLabel::UserAgents,
        });
    }
    roots
}

/// `asterpath-tools` rooted at the working directory under the `[sandbox]`
/// policy, filtered to `selection` (`None` = all) in the order given.
fn builtin_tools(
    settings: &config::Settings,
    selection: Option<&[String]>,
) -> CliResult<Vec<Arc<dyn Tool>>> {
    let root = std::env::current_dir()
        .map_err(|error| CliError::config(format!("cannot read working directory: {error}")))?;
    let policy = settings.sandbox_policy();
    let all = asterpath_tools::coding(root, sandbox_provider(&policy), policy)
        .map_err(|error| CliError::config(error.to_string()))?;
    let Some(selection) = selection else {
        return Ok(all);
    };
    selection
        .iter()
        .map(|name| {
            all.iter()
                .find(|tool| tool.descriptor().name.as_str() == name)
                .cloned()
                .ok_or_else(|| CliError::config(format!("unknown tool {name}")))
        })
        .collect()
}

async fn assemble(
    settings: &config::Settings,
    providers: Vec<Arc<dyn asterpath_protocol::Provider>>,
    approval: config::ApprovalMode,
    tools: Vec<Arc<dyn Tool>>,
) -> CliResult<Asterpath> {
    // Sessions without a context window still report `NotConfigured`.
    let catalog = ProviderCatalog::new(providers.clone())
        .map_err(|error| CliError::config(error.to_string()))?;
    let config = AsterpathConfig::default();
    let approval = config::approval_policy(approval, Arc::clone(&config.gate));
    AsterpathBuilder::from_config(config)
        .compactor(Arc::new(DefaultCompactor::new(Arc::new(catalog))))
        .database(&settings.db)
        .providers(providers)
        .tools(tools)
        .tool(config::tool_config(settings))
        .max_live_children(config::max_live_children(settings))
        .model_aliases(config::model_aliases(settings))
        .approval(approval)
        .build()
        .await
        .map_err(aster_error)
}

/// Start every configured MCP server under the `[sandbox]` policy. A
/// `required` failure is a configuration error.
async fn load_mcp(settings: &config::Settings) -> CliResult<McpServers> {
    let configs = settings.mcp_servers().map_err(CliError::config)?;
    let policy = settings.sandbox_policy();
    McpServers::load(configs, sandbox_provider(&policy), policy)
        .await
        .map_err(|error| CliError::config(error.to_string()))
}

/// bwrap on Linux. Elsewhere a restricted policy has no backend, so stdio
/// servers fail closed at the sandbox stage unless the mode is `full-access`.
fn sandbox_provider(policy: &SandboxPolicy) -> Option<Arc<dyn SandboxProvider>> {
    #[cfg(target_os = "linux")]
    if !matches!(policy, SandboxPolicy::FullAccess) {
        return Some(Arc::new(asterpath_sandbox_linux::LinuxSandbox::new()));
    }
    let _ = policy;
    None
}

/// One `mcp: <id> ...` line per configured server, in config order.
fn mcp_status_lines(settings: &config::Settings, mcp: &McpServers) -> CliResult<Vec<String>> {
    let configs = settings.mcp_servers().map_err(CliError::config)?;
    Ok(configs
        .iter()
        .zip(mcp.statuses())
        .map(|(config, status)| match status {
            ServerStatus::Loaded { tools } => {
                let mut names: Vec<&str> = tools.iter().map(|tool| tool.as_str()).collect();
                names.sort_unstable();
                format!("mcp: {} loaded: {}", config.id, names.join(", "))
            }
            ServerStatus::Failed { stage, message } => {
                format!("mcp: {} failed at {stage:?}: {message}", config.id)
            }
            other => format!("mcp: {} {other:?}", config.id),
        })
        .collect())
}

/// Optional servers that failed are reported and skipped.
fn report_mcp_failures(settings: &config::Settings, mcp: &McpServers) -> CliResult<()> {
    let lines = mcp_status_lines(settings, mcp)?;
    for (line, status) in lines.iter().zip(mcp.statuses()) {
        if !matches!(status, ServerStatus::Loaded { .. }) {
            eprintln!("{line}");
        }
    }
    Ok(())
}

/// No providers and no drive: enough for log-derived projections.
async fn bare(settings: &config::Settings) -> CliResult<Asterpath> {
    Asterpath::builder()
        .database(&settings.db)
        .build()
        .await
        .map_err(aster_error)
}

async fn scan(
    event_log: Arc<dyn SessionStore>,
    session: SessionId,
) -> Result<Vec<LogEntry>, String> {
    scan_all(&*event_log, session)
        .await
        .map_err(|error| error.to_string())
}

fn state_label(state: SessionState) -> String {
    match state {
        SessionState::Idle => "idle".to_owned(),
        SessionState::TurnOpen { turn, .. } => format!("turn-open {}", turn.to_hex()),
    }
}

/// The last assistant message in the model-visible projection.
fn final_assistant_text(entries: &[LogEntry]) -> Option<String> {
    let items = history(entries).ok()?;
    for item in items.iter().rev() {
        if let Item::Message {
            role: Role::Assistant,
            parts,
        } = item
        {
            let text: String = parts
                .iter()
                .filter_map(|part| match part {
                    Part::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect();
            if !text.is_empty() {
                return Some(text);
            }
        }
    }
    None
}

fn not_found(session: SessionId) -> CliError {
    CliError::run(format!("session {session:?} not found"))
}

fn parse_session(value: &str) -> CliResult<SessionId> {
    serde_json::from_value(serde_json::Value::String(value.to_owned()))
        .map_err(|_| CliError::config(format!("invalid session id {value}")))
}

fn aster_error(error: AsterError) -> CliError {
    match error {
        AsterError::Config { message } => CliError::config(message),
        other => CliError::run(other.to_string()),
    }
}

fn fresh_input_id() -> InputId {
    InputId::from_bytes(unique_bytes(SystemClock.now()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use asterpath_protocol::ToolCallId;
    use asterpath_protocol::{
        ApprovalPolicy, ApprovalRequest, ApprovalVerdict, CompactionConfig, JsonString, ModelId,
        ModelSpec, Parallelism, Provenance, Provider, ProviderId, ReplaySafety, RequestConfig,
        SessionEvent, SessionSpec, SideEffect, StopReason, SubagentConfig, Tool, ToolArgs,
        ToolConfig, ToolContext, ToolDescriptor, ToolError, ToolName, ToolOutput, ToolSelection,
        TurnConfig, Usage,
    };
    use asterpath_testing::ScriptedProvider;
    use futures_util::future::BoxFuture;

    fn request() -> ApprovalRequest {
        ApprovalRequest::new(
            SessionId::from_bytes([1; 16]),
            asterpath_protocol::TurnId::from_bytes([2; 16]),
            asterpath_protocol::StepId::from_bytes([3; 16]),
            ToolCallId::from_bytes([4; 16]),
            ToolName::new("echo"),
            JsonString::new(r#"{"q":"hi"}"#),
            ReplaySafety::Safe,
            SideEffect::ReadOnly,
            Provenance::InProcess,
        )
    }

    #[test]
    fn prompt_prints_every_approval_field_and_maps_the_answer() {
        for (answer, expected) in [
            ("y\n", ApprovalDecision::Allow),
            ("yes\n", ApprovalDecision::Allow),
            ("n\n", ApprovalDecision::Deny { reason: None }),
            ("\n", ApprovalDecision::Deny { reason: None }),
        ] {
            let mut io = PromptIo {
                reader: Cursor::new(answer.as_bytes()),
                writer: Vec::new(),
            };
            assert_eq!(prompt_decision(&mut io, &request()), expected);
            let rendered = String::from_utf8(io.writer).unwrap();
            for field in [
                "tool: echo",
                r#"args: {"q":"hi"}"#,
                "replay: Safe",
                "side_effect: ReadOnly",
                "provenance: InProcess",
            ] {
                assert!(rendered.contains(field), "missing {field}: {rendered}");
            }
        }
    }

    #[test]
    fn non_tty_ask_degrades_but_explicit_ask_is_a_usage_error() {
        use config::ApprovalMode;
        assert!(matches!(
            effective_approval(ApprovalMode::Ask, None, false).expect("configured ask degrades"),
            ApprovalMode::Deny
        ));
        let error = effective_approval(ApprovalMode::Ask, Some(ApprovalMode::Ask), false)
            .expect_err("explicit ask without a TTY");
        assert_eq!(error.code, 2);
        assert!(matches!(
            effective_approval(ApprovalMode::Ask, None, true).unwrap(),
            ApprovalMode::Ask
        ));
        assert!(matches!(
            effective_approval(ApprovalMode::Deny, None, false).unwrap(),
            ApprovalMode::Deny
        ));
    }

    struct EchoTool {
        descriptor: ToolDescriptor,
        calls: Arc<AtomicUsize>,
    }

    impl Tool for EchoTool {
        fn descriptor(&self) -> &ToolDescriptor {
            &self.descriptor
        }

        fn invoke(
            &self,
            _args: ToolArgs,
            _ctx: ToolContext,
        ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(ToolOutput::new(vec![Part::Text { text: "ok".into() }])) })
        }
    }

    fn echo_tool(calls: Arc<AtomicUsize>) -> Arc<dyn Tool> {
        Arc::new(EchoTool {
            descriptor: ToolDescriptor {
                name: ToolName::new("echo"),
                description: String::new(),
                schema: serde_json::json!({"type": "object"}),
                parallelism: Parallelism::Parallel,
                replay: ReplaySafety::Safe,
                side_effect: SideEffect::ReadOnly,
                provenance: Provenance::InProcess,
                timeout: None,
                max_output_bytes: None,
            },
            calls,
        })
    }

    struct AlwaysAsk;

    impl ApprovalPolicy for AlwaysAsk {
        fn classify(&self, _request: &ApprovalRequest) -> ApprovalVerdict {
            ApprovalVerdict::Ask
        }
    }

    fn tool_call() -> Item {
        Item::ToolCall {
            call_id: "call-1".to_owned(),
            provider_item_id: None,
            name: ToolName::new("echo"),
            raw_args: JsonString::new(r#"{"q":"hi"}"#),
            raw: None,
        }
    }

    fn final_message() -> Item {
        Item::Message {
            role: Role::Assistant,
            parts: vec![Part::Text {
                text: "done".into(),
            }],
        }
    }

    fn spec() -> SessionSpec {
        SessionSpec {
            model: ModelSpec {
                provider: ProviderId::new("p"),
                model: ModelId::new("model-1"),
                context_window: None,
            },
            system: Vec::new(),
            tools: ToolSelection::All,
            request: RequestConfig::default(),
            turn: TurnConfig {
                initial_backoff: Duration::ZERO,
                max_backoff: Duration::ZERO,
                ..TurnConfig::default()
            },
            compaction: CompactionConfig::default(),
            subagents: SubagentConfig::default(),
        }
    }

    fn temp_db(tag: &str) -> PathBuf {
        use std::sync::atomic::AtomicU64;
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "asterpath-cli-approval-{tag}-{}-{}.db",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn cleanup(db: &Path) {
        let _ = std::fs::remove_file(db);
        let _ = std::fs::remove_file(format!("{}-wal", db.display()));
        let _ = std::fs::remove_file(format!("{}-shm", db.display()));
    }

    fn script() -> Arc<ScriptedProvider> {
        Arc::new(
            ScriptedProvider::new("p")
                .respond(vec![tool_call()], StopReason::ToolUse, Usage::default())
                .respond(vec![final_message()], StopReason::EndTurn, Usage::default()),
        )
    }

    async fn build(db: &Path, provider: Arc<ScriptedProvider>, tool: Arc<dyn Tool>) -> Asterpath {
        Asterpath::builder()
            .database(db)
            .providers(vec![provider as Arc<dyn Provider>])
            .tools(vec![tool])
            .tool(ToolConfig::default())
            .approval(Arc::new(AlwaysAsk))
            .build()
            .await
            .expect("build")
    }

    #[tokio::test]
    async fn allow_prompt_is_delivered_and_executes_the_tool() {
        let db = temp_db("allow");
        let calls = Arc::new(AtomicUsize::new(0));
        let asterpath = build(&db, script(), echo_tool(Arc::clone(&calls))).await;
        let _endpoint = asterpath.approval_gate().register();
        let handle = asterpath.create_session(spec()).await.unwrap();
        handle
            .submit_input(UserInput {
                id: InputId::from_bytes([1; 16]),
                parts: vec![Part::Text { text: "go".into() }],
            })
            .await
            .unwrap();

        let mut io = PromptIo {
            reader: Cursor::new(b"y\n"),
            writer: Vec::new(),
        };
        let outcome = drive(&handle, Some(&mut io)).await.expect("drive");

        assert!(matches!(outcome.reason, Some(TurnEndReason::Completed)));
        assert!(!outcome.idle);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(String::from_utf8(io.writer).unwrap().contains("tool: echo"));
        assert_eq!(signal_count(&asterpath, handle.id()).await, 1);

        asterpath.shutdown().await.unwrap();
        cleanup(&db);
    }

    #[tokio::test]
    async fn deny_prompt_records_denied_and_skips_the_tool() {
        let db = temp_db("deny");
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(ScriptedProvider::new("p").respond(
            vec![tool_call()],
            StopReason::ToolUse,
            Usage::default(),
        ));
        let asterpath = build(&db, provider, echo_tool(Arc::clone(&calls))).await;
        let _endpoint = asterpath.approval_gate().register();
        let handle = asterpath.create_session(spec()).await.unwrap();
        handle
            .submit_input(UserInput {
                id: InputId::from_bytes([1; 16]),
                parts: vec![Part::Text { text: "go".into() }],
            })
            .await
            .unwrap();

        let mut io = PromptIo {
            reader: Cursor::new(b"n\n"),
            writer: Vec::new(),
        };
        let outcome = drive(&handle, Some(&mut io)).await.expect("drive");

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let values = tool_result_values(&asterpath, handle.id()).await;
        assert_eq!(values.len(), 1);
        assert!(values[0].to_lowercase().contains("denied"), "{values:?}");
        assert!(outcome.reason.is_some() || outcome.idle);

        asterpath.shutdown().await.unwrap();
        cleanup(&db);
    }

    #[tokio::test]
    async fn recorded_decision_is_reused_on_restart_without_prompting() {
        let db = temp_db("reuse");
        let calls = Arc::new(AtomicUsize::new(0));

        // Phase 1: reach the approval, record Allow, then drop the drive.
        let asterpath = build(&db, script(), echo_tool(Arc::clone(&calls))).await;
        let _endpoint = asterpath.approval_gate().register();
        let handle = asterpath.create_session(spec()).await.unwrap();
        handle
            .submit_input(UserInput {
                id: InputId::from_bytes([1; 16]),
                parts: vec![Part::Text { text: "go".into() }],
            })
            .await
            .unwrap();
        let mut stream = handle.drive();
        let request = loop {
            let event = tokio::time::timeout(Duration::from_secs(2), stream.next())
                .await
                .expect("approval requested")
                .expect("event")
                .expect("no error");
            if let AgentEvent::ApprovalRequested(request) = event {
                break request;
            }
        };
        handle
            .approve(request.tool_call_id, ApprovalDecision::Allow)
            .await
            .unwrap();
        drop(stream);
        assert_eq!(calls.load(Ordering::SeqCst), 0, "not executed before crash");
        let session = handle.id();
        drop(handle);
        asterpath.shutdown().await.unwrap();

        // Phase 2: restart with a provider that can only answer the final step.
        let provider = Arc::new(ScriptedProvider::new("p").respond(
            vec![final_message()],
            StopReason::EndTurn,
            Usage::default(),
        ));
        let asterpath = build(&db, provider, echo_tool(Arc::clone(&calls))).await;
        let _endpoint = asterpath.approval_gate().register();
        let handle = asterpath.open_session(session).await.unwrap();

        let mut io = PromptIo {
            reader: Cursor::new(b"n\n"),
            writer: Vec::new(),
        };
        let outcome = drive(&handle, Some(&mut io)).await.expect("resume");

        assert!(matches!(outcome.reason, Some(TurnEndReason::Completed)));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the recorded Allow ran the tool"
        );
        assert!(
            io.writer.is_empty(),
            "a recorded decision must not re-prompt"
        );
        assert_eq!(signal_count(&asterpath, handle.id()).await, 1);

        asterpath.shutdown().await.unwrap();
        cleanup(&db);
    }

    /// The CLI `ask` policy over a real `bash` tool that would create `ran`.
    async fn ask_bash(db: &Path, root: &Path) -> Asterpath {
        let provider = Arc::new(ScriptedProvider::new("p").respond(
            vec![Item::ToolCall {
                call_id: "call-1".to_owned(),
                provider_item_id: None,
                name: ToolName::new("bash"),
                raw_args: JsonString::new(r#"{"command":"touch ran"}"#),
                raw: None,
            }],
            StopReason::ToolUse,
            Usage::default(),
        ));
        let bash = asterpath_tools::bash(root.to_path_buf(), None, SandboxPolicy::FullAccess)
            .expect("absolute root");
        let config = AsterpathConfig::default();
        let policy = config::approval_policy(config::ApprovalMode::Ask, Arc::clone(&config.gate));
        AsterpathBuilder::from_config(config)
            .database(db)
            .providers(vec![provider as Arc<dyn Provider>])
            .tools(vec![bash])
            .approval(policy)
            .build()
            .await
            .expect("build")
    }

    #[tokio::test]
    async fn ask_mode_asks_before_bash_and_closed_gate_denies_without_spawning() {
        for open in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let db = dir.path().join("ask.db");
            let asterpath = ask_bash(&db, dir.path()).await;
            let _endpoint = open.then(|| asterpath.approval_gate().register());
            let handle = asterpath.create_session(spec()).await.unwrap();
            handle
                .submit_input(UserInput {
                    id: InputId::from_bytes([1; 16]),
                    parts: vec![Part::Text { text: "go".into() }],
                })
                .await
                .unwrap();

            let mut io = PromptIo {
                reader: Cursor::new(b"n\n"),
                writer: Vec::new(),
            };
            drive(&handle, Some(&mut io)).await.expect("drive");

            let asked = String::from_utf8(io.writer).unwrap().contains("tool: bash");
            assert_eq!(asked, open, "ApprovalRequested only while the gate is open");
            assert!(!dir.path().join("ran").exists(), "bash never started");
            let values = tool_result_values(&asterpath, handle.id()).await;
            assert_eq!(values.len(), 1, "{values:?}");
            let reason = if open {
                "tool call denied"
            } else {
                asterpath_protocol::NO_APPROVAL_ENDPOINT
            };
            assert!(values[0].contains(reason), "{values:?}");
            asterpath.shutdown().await.unwrap();
        }
    }

    async fn signal_count(asterpath: &Asterpath, session: SessionId) -> usize {
        scan(asterpath.store(), session)
            .await
            .unwrap()
            .iter()
            .filter(|entry| decode_entry(entry).is_ok_and(|event| event.kind() == "SignalReceived"))
            .count()
    }

    async fn tool_result_values(asterpath: &Asterpath, session: SessionId) -> Vec<String> {
        scan(asterpath.store(), session)
            .await
            .unwrap()
            .iter()
            .filter_map(|entry| match decode_entry(entry).ok()? {
                SessionEvent::ToolResult { status, error, .. } => {
                    Some(format!("{status:?} {:?}", error.map(|error| error.message)))
                }
                _ => None,
            })
            .collect()
    }
}
