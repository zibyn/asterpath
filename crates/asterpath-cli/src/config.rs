//! `asterpath.toml`, environment and flag resolution.
//!
//! Precedence is flag > environment > file. API keys are deliberately not part
//! of this model: they are read only from the environment.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use asterpath_mcp::{
    ADAPTER_PROTOCOL_VERSIONS, McpServerConfig, McpTimeouts, McpTransport, ServerId,
};
use asterpath_protocol::{
    ApprovalGate, ApprovalPolicy, ApprovalRequest, ApprovalVerdict, CompactionConfig,
    ContextPolicy, DefaultApprovalPolicy, ModelAlias, ModelId, ModelSpec, NO_APPROVAL_ENDPOINT,
    Provider, ProviderId, ReasoningEffort, ReasoningParams, RequestConfig, SamplingParams,
    SandboxPolicy, SideEffect, SubagentConfig, ToolChoice, ToolConfig, ToolName, TurnConfig,
};
use clap::ValueEnum;
use serde::Deserialize;

#[derive(Clone, Copy, Debug, Default, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum ApprovalMode {
    #[default]
    Ask,
    Allow,
    Deny,
}

/// Values resolved from flags, environment and the file, in that order.
pub struct Settings {
    pub db: PathBuf,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub approval: ApprovalMode,
    pub file: FileConfig,
}

pub struct Overrides {
    pub config: Option<PathBuf>,
    pub db: Option<PathBuf>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub approval: Option<ApprovalMode>,
}

/// Load the configuration file (when named) and apply flag and environment
/// overrides. A missing file means defaults.
pub fn load(overrides: Overrides) -> Result<Settings, String> {
    let path = overrides
        .config
        .or_else(|| env_os("ASTERPATH_CONFIG").map(PathBuf::from));
    let file = match path {
        Some(path) if path.exists() => {
            let contents = std::fs::read_to_string(&path)
                .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
            parse(&contents)?
        }
        _ => FileConfig::default(),
    };

    let db = overrides
        .db
        .or_else(|| env_os("ASTERPATH_DB").map(PathBuf::from))
        .or_else(|| file.storage.path.clone())
        .unwrap_or_else(default_db);

    Ok(Settings {
        db: expand_tilde(db),
        provider: overrides
            .provider
            .or_else(|| env_string("ASTERPATH_PROVIDER")),
        model: overrides.model.or_else(|| env_string("ASTERPATH_MODEL")),
        approval: overrides
            .approval
            .or(file.approval.default)
            .unwrap_or_default(),
        file,
    })
}

/// Parse and validate without opening any module.
pub fn validate(settings: &Settings) -> Result<(), String> {
    if settings.file.turn.max_steps_per_turn == Some(0) {
        return Err("max_steps_per_turn must be greater than zero".to_owned());
    }
    for id in settings.file.providers.keys() {
        if !provider_available(id) {
            return Err(format!("unknown provider {id}"));
        }
    }
    for (alias, model) in &settings.file.model_aliases {
        if !provider_available(&model.provider) {
            return Err(format!(
                "model alias {alias} references unknown provider {}",
                model.provider
            ));
        }
    }
    if let Some(provider) = &settings.provider
        && !provider_available(provider)
    {
        return Err(format!("unknown provider {provider}"));
    }
    if let Some(model) = &settings.model
        && model.trim().is_empty()
    {
        return Err("--model must not be empty".to_owned());
    }
    // Resolve the value-shaped sections now so type errors exit 2, not at run.
    settings.file.request_config()?;
    settings.mcp_servers()?;
    Ok(())
}

/// The selected provider plus every provider a model alias targets.
pub fn providers(settings: &Settings) -> Result<Vec<Arc<dyn Provider>>, String> {
    let selected = settings
        .provider
        .as_deref()
        .ok_or_else(|| "a provider is required (--provider or ASTERPATH_PROVIDER)".to_owned())?;
    let mut ids = vec![selected.to_owned()];
    for model in settings.file.model_aliases.values() {
        if !ids.contains(&model.provider) {
            ids.push(model.provider.clone());
        }
    }
    ids.iter()
        .map(|id| build_provider(id, &settings.file))
        .collect()
}

pub fn session_spec(settings: &Settings) -> Result<asterpath_protocol::SessionSpec, String> {
    let provider = settings
        .provider
        .clone()
        .ok_or_else(|| "a provider is required (--provider or ASTERPATH_PROVIDER)".to_owned())?;
    let model = settings
        .model
        .clone()
        .ok_or_else(|| "a model is required (--model or ASTERPATH_MODEL)".to_owned())?;
    Ok(asterpath_protocol::SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new(provider),
            model: ModelId::new(model),
            context_window: settings.file.model.context_window,
        },
        system: Vec::new(),
        tools: asterpath_protocol::ToolSelection::All,
        request: settings.file.request_config()?,
        turn: turn_config(settings),
        compaction: compaction_config(settings),
        subagents: subagent_config(settings),
    })
}

pub fn turn_config(settings: &Settings) -> TurnConfig {
    TurnConfig {
        max_steps_per_turn: settings.file.turn.max_steps_per_turn,
        ..TurnConfig::default()
    }
}

pub fn compaction_config(settings: &Settings) -> CompactionConfig {
    settings.file.compaction_config()
}

pub fn subagent_config(settings: &Settings) -> SubagentConfig {
    settings.file.subagent_config()
}

pub fn max_live_children(settings: &Settings) -> NonZeroUsize {
    settings.file.max_live_children()
}

pub fn model_aliases(settings: &Settings) -> BTreeMap<ModelAlias, ModelSpec> {
    settings
        .file
        .model_aliases
        .iter()
        .map(|(alias, model)| (ModelAlias::new(alias), model.spec()))
        .collect()
}

pub fn tool_config(settings: &Settings) -> ToolConfig {
    let mut config = ToolConfig::default();
    if let Some(calls) = settings.file.tools.max_parallel_calls {
        config.max_parallel_calls = calls;
    }
    config
}

/// `Ask` asks for every `Mutating` tool (the framework default allows all
/// in-process tools) and otherwise keeps the default; both deny when no
/// endpoint is open.
pub fn approval_policy(mode: ApprovalMode, gate: Arc<ApprovalGate>) -> Arc<dyn ApprovalPolicy> {
    match mode {
        ApprovalMode::Ask => Arc::new(AskMutating {
            fallback: DefaultApprovalPolicy::new(Arc::clone(&gate)),
            gate,
        }),
        ApprovalMode::Allow => Arc::new(FixedApproval(ApprovalVerdict::Allow)),
        ApprovalMode::Deny => Arc::new(FixedApproval(ApprovalVerdict::Deny { reason: None })),
    }
}

struct AskMutating {
    gate: Arc<ApprovalGate>,
    fallback: DefaultApprovalPolicy,
}

impl ApprovalPolicy for AskMutating {
    fn classify(&self, request: &ApprovalRequest) -> ApprovalVerdict {
        match request.side_effect {
            SideEffect::Mutating if self.gate.is_open() => ApprovalVerdict::Ask,
            SideEffect::Mutating => ApprovalVerdict::Deny {
                reason: Some(NO_APPROVAL_ENDPOINT.to_owned()),
            },
            _ => self.fallback.classify(request),
        }
    }
}

struct FixedApproval(ApprovalVerdict);

impl ApprovalPolicy for FixedApproval {
    fn classify(&self, _request: &ApprovalRequest) -> ApprovalVerdict {
        self.0.clone()
    }
}

fn parse(contents: &str) -> Result<FileConfig, String> {
    let value: toml::Value =
        toml::from_str(contents).map_err(|error| format!("invalid config: {error}"))?;
    if let Some(field) = secret_field(&value) {
        return Err(format!(
            "config must not contain `{field}`; API keys are read from the environment"
        ));
    }
    toml::from_str(contents).map_err(|error| format!("invalid config: {error}"))
}

fn secret_field(value: &toml::Value) -> Option<String> {
    match value {
        toml::Value::Table(table) => table.iter().find_map(|(key, value)| {
            if matches!(key.as_str(), "api_key" | "secret" | "token") {
                Some(key.clone())
            } else {
                secret_field(value)
            }
        }),
        toml::Value::Array(items) => items.iter().find_map(secret_field),
        _ => None,
    }
}

// Feature flags, not a literal set: the suggestion to use `matches!` is wrong.
#[allow(clippy::match_like_matches_macro)]
fn provider_available(id: &str) -> bool {
    match id {
        "anthropic" => cfg!(feature = "anthropic"),
        "openai" => cfg!(feature = "openai"),
        _ => false,
    }
}

fn build_provider(id: &str, file: &FileConfig) -> Result<Arc<dyn Provider>, String> {
    let base = file
        .providers
        .get(id)
        .and_then(|section| section.base_url.as_deref());
    match id {
        "anthropic" => build_anthropic(base),
        "openai" => build_openai(base),
        other => Err(format!("unknown provider {other}")),
    }
}

#[cfg(feature = "anthropic")]
fn build_anthropic(base_url: Option<&str>) -> Result<Arc<dyn Provider>, String> {
    let key = env_string("ANTHROPIC_API_KEY")
        .ok_or_else(|| "ANTHROPIC_API_KEY is required for the anthropic provider".to_owned())?;
    let mut provider = asterpath::anthropic::AnthropicProvider::new(key);
    if let Some(base_url) = base_url {
        provider = provider.with_base_url(base_url);
    }
    Ok(Arc::new(provider))
}

#[cfg(not(feature = "anthropic"))]
fn build_anthropic(_base_url: Option<&str>) -> Result<Arc<dyn Provider>, String> {
    Err("the anthropic feature is not enabled".to_owned())
}

#[cfg(feature = "openai")]
fn build_openai(base_url: Option<&str>) -> Result<Arc<dyn Provider>, String> {
    let key = env_string("OPENAI_API_KEY")
        .ok_or_else(|| "OPENAI_API_KEY is required for the openai provider".to_owned())?;
    let mut provider = asterpath::openai::OpenAiProvider::new(key);
    if let Some(base_url) = base_url {
        provider = provider.with_base_url(base_url);
    }
    Ok(Arc::new(provider))
}

#[cfg(not(feature = "openai"))]
fn build_openai(_base_url: Option<&str>) -> Result<Arc<dyn Provider>, String> {
    Err("the openai feature is not enabled".to_owned())
}

fn env_string(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

fn env_os(name: &str) -> Option<std::ffi::OsString> {
    std::env::var_os(name).filter(|value| !value.is_empty())
}

fn expand_tilde(path: PathBuf) -> PathBuf {
    let Some(rest) = path.to_str().and_then(|value| value.strip_prefix("~/")) else {
        return path;
    };
    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home).join(rest),
        None => path,
    }
}

fn default_db() -> PathBuf {
    let base = std::env::var_os("XDG_DATA_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/share")))
        .unwrap_or_else(|| PathBuf::from("."));
    base.join("asterpath").join("sessions.db")
}

fn parse_tool_choice(value: &str) -> Result<ToolChoice, String> {
    match value {
        "auto" => Ok(ToolChoice::Auto),
        "none" => Ok(ToolChoice::None),
        "required" => Ok(ToolChoice::Required),
        "" => Err("tool_choice must not be empty".to_owned()),
        name => Ok(ToolChoice::Tool(ToolName::new(name))),
    }
}

fn parse_effort(value: &str) -> Result<ReasoningEffort, String> {
    match value {
        "low" => Ok(ReasoningEffort::Low),
        "medium" => Ok(ReasoningEffort::Medium),
        "high" => Ok(ReasoningEffort::High),
        other => Err(format!("unknown reasoning effort {other}")),
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FileConfig {
    model: ModelSection,
    sandbox: SandboxSection,
    mcp_servers: BTreeMap<String, McpServerSection>,
    storage: StorageSection,
    providers: BTreeMap<String, ProviderSection>,
    turn: TurnSection,
    request: RequestSection,
    tools: ToolsSection,
    model_aliases: BTreeMap<String, ModelItem>,
    compaction: CompactionSection,
    subagents: SubagentSection,
    approval: ApprovalSection,
    skills: SkillsSection,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct StorageSection {
    path: Option<PathBuf>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ProviderSection {
    base_url: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct TurnSection {
    max_steps_per_turn: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RequestSection {
    max_output_tokens: Option<std::num::NonZeroU32>,
    tool_choice: Option<String>,
    disable_parallel_tool_use: Option<bool>,
    sampling: SamplingSection,
    reasoning: ReasoningSection,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SamplingSection {
    temperature: Option<f32>,
    top_p: Option<f32>,
    top_k: Option<u32>,
    seed: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ReasoningSection {
    effort: Option<String>,
    budget_tokens: Option<u32>,
    summary: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ToolsSection {
    max_parallel_calls: Option<NonZeroUsize>,
    /// Built-in tools for new sessions; absent means all of them.
    enabled: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelItem {
    provider: String,
    model: String,
    #[serde(default)]
    context_window: Option<u32>,
}

impl ModelItem {
    fn spec(&self) -> ModelSpec {
        ModelSpec {
            provider: ProviderId::new(self.provider.clone()),
            model: ModelId::new(self.model.clone()),
            context_window: self.context_window,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct CompactionSection {
    reserve_output_tokens: Option<u32>,
    target_ratio: Option<f32>,
    max_auto_compactions_per_turn: Option<u32>,
    max_summary_tokens: Option<u32>,
    summary_model: Option<ModelItem>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SubagentSection {
    default_context: Option<ContextPolicy>,
    max_depth: Option<u8>,
    max_concurrent_children: Option<NonZeroUsize>,
    max_live_children: Option<NonZeroUsize>,
    spawn_timeout: Option<Duration>,
    cancel_grace: Option<Duration>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ApprovalSection {
    default: Option<ApprovalMode>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SkillsSection {
    enabled: Option<bool>,
    paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ModelSection {
    /// Maps to `ModelSpec.context_window`; absent leaves compaction `NotConfigured`.
    context_window: Option<u32>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum SandboxMode {
    ReadOnly,
    #[default]
    WorkspaceWrite,
    FullAccess,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SandboxSection {
    mode: SandboxMode,
}

/// Codex-style `[mcp_servers.<id>]`: stdio uses `command`/`args`/`env`/`cwd`,
/// HTTP uses `url`/`headers`.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct McpServerSection {
    command: Option<String>,
    args: Vec<String>,
    env: BTreeMap<String, String>,
    cwd: Option<PathBuf>,
    url: Option<String>,
    headers: BTreeMap<String, String>,
    required: bool,
    protocol_versions: Option<Vec<String>>,
}

impl Settings {
    /// `[skills] enabled`; absent means enabled.
    pub fn skills_enabled(&self) -> bool {
        self.file.skills.enabled.unwrap_or(true)
    }

    /// `[tools] enabled`; `None` means every built-in tool.
    pub fn tool_names(&self) -> Option<&[String]> {
        self.file.tools.enabled.as_deref()
    }

    /// Explicit skill roots, highest priority first.
    pub fn skill_paths(&self) -> &[PathBuf] {
        &self.file.skills.paths
    }

    /// `[sandbox] mode`; `workspace-write` makes the working directory the
    /// only writable root.
    pub fn sandbox_policy(&self) -> SandboxPolicy {
        match self.file.sandbox.mode {
            SandboxMode::ReadOnly => SandboxPolicy::ReadOnly,
            SandboxMode::WorkspaceWrite => SandboxPolicy::WorkspaceWrite {
                writable_roots: std::env::current_dir().into_iter().collect(),
            },
            SandboxMode::FullAccess => SandboxPolicy::FullAccess,
        }
    }

    /// `[mcp_servers.<id>]` mapped to adapter configs, in id order.
    pub fn mcp_servers(&self) -> Result<Vec<McpServerConfig>, String> {
        self.file
            .mcp_servers
            .iter()
            .map(|(id, server)| {
                let transport = match (&server.command, &server.url) {
                    (Some(command), None) => McpTransport::Stdio {
                        command: command.clone(),
                        args: server.args.clone(),
                        env: server.env.clone(),
                        cwd: server.cwd.clone(),
                    },
                    (None, Some(url)) => McpTransport::Http {
                        url: url.clone(),
                        headers: server.headers.clone(),
                    },
                    _ => {
                        return Err(format!(
                            "mcp_servers.{id}: set exactly one of `command` or `url`"
                        ));
                    }
                };
                Ok(McpServerConfig {
                    id: ServerId::new(id.clone())
                        .map_err(|error| format!("mcp_servers.{id}: {error}"))?,
                    transport,
                    required: server.required,
                    protocol_versions: server.protocol_versions.clone().unwrap_or_else(|| {
                        ADAPTER_PROTOCOL_VERSIONS
                            .iter()
                            .map(|version| (*version).to_owned())
                            .collect()
                    }),
                    timeouts: McpTimeouts::default(),
                })
            })
            .collect()
    }
}

impl FileConfig {
    fn request_config(&self) -> Result<RequestConfig, String> {
        let default = RequestConfig::default();
        let sampling = &self.request.sampling;
        let reasoning = &self.request.reasoning;
        Ok(RequestConfig {
            max_output_tokens: self
                .request
                .max_output_tokens
                .unwrap_or(default.max_output_tokens),
            tool_choice: match &self.request.tool_choice {
                Some(value) => parse_tool_choice(value)?,
                None => default.tool_choice,
            },
            disable_parallel_tool_use: self
                .request
                .disable_parallel_tool_use
                .unwrap_or(default.disable_parallel_tool_use),
            sampling: if sampling.temperature.is_none()
                && sampling.top_p.is_none()
                && sampling.top_k.is_none()
                && sampling.seed.is_none()
            {
                None
            } else {
                Some(SamplingParams {
                    temperature: sampling.temperature,
                    top_p: sampling.top_p,
                    top_k: sampling.top_k,
                    seed: sampling.seed,
                })
            },
            reasoning: if reasoning.effort.is_none()
                && reasoning.budget_tokens.is_none()
                && reasoning.summary.is_none()
            {
                None
            } else {
                Some(ReasoningParams {
                    effort: reasoning.effort.as_deref().map(parse_effort).transpose()?,
                    budget_tokens: reasoning.budget_tokens,
                    summary: reasoning.summary.unwrap_or(false),
                })
            },
        })
    }

    fn compaction_config(&self) -> CompactionConfig {
        let default = CompactionConfig::default();
        CompactionConfig {
            reserve_output_tokens: self
                .compaction
                .reserve_output_tokens
                .or(default.reserve_output_tokens),
            target_ratio: self.compaction.target_ratio.unwrap_or(default.target_ratio),
            max_auto_compactions_per_turn: self
                .compaction
                .max_auto_compactions_per_turn
                .unwrap_or(default.max_auto_compactions_per_turn),
            max_summary_tokens: self
                .compaction
                .max_summary_tokens
                .unwrap_or(default.max_summary_tokens),
            summary_model: self
                .compaction
                .summary_model
                .as_ref()
                .map(ModelItem::spec)
                .or(default.summary_model),
        }
    }

    /// Process-wide; maps to the facade, not the per-session snapshot.
    fn max_live_children(&self) -> NonZeroUsize {
        self.subagents
            .max_live_children
            .unwrap_or(NonZeroUsize::new(32).expect("non-zero"))
    }

    fn subagent_config(&self) -> SubagentConfig {
        let default = SubagentConfig::default();
        SubagentConfig {
            default_context: self
                .subagents
                .default_context
                .clone()
                .unwrap_or(default.default_context),
            max_depth: self.subagents.max_depth.unwrap_or(default.max_depth),
            max_concurrent_children: self
                .subagents
                .max_concurrent_children
                .unwrap_or(default.max_concurrent_children),
            spawn_timeout: self.subagents.spawn_timeout.or(default.spawn_timeout),
            cancel_grace: self.subagents.cancel_grace.unwrap_or(default.cancel_grace),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_fields_are_found_at_any_depth() {
        let value: toml::Value = toml::from_str("[a]\ntoken = \"x\"\n").unwrap();
        assert_eq!(secret_field(&value).as_deref(), Some("token"));
    }

    #[test]
    fn tool_choice_accepts_keywords_and_names() {
        assert_eq!(parse_tool_choice("auto").unwrap(), ToolChoice::Auto);
        assert_eq!(
            parse_tool_choice("search").unwrap(),
            ToolChoice::Tool(ToolName::new("search"))
        );
        assert!(parse_tool_choice("").is_err());
    }

    #[test]
    fn subagents_max_live_children_is_the_global_cap() {
        let file = parse("[subagents]\nmax_live_children = 4\n").unwrap();
        assert_eq!(file.max_live_children().get(), 4);
        assert_eq!(parse("").unwrap().max_live_children().get(), 32);
    }
}
