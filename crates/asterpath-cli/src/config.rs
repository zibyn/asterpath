//! `config.toml`, `-c key=value` overrides and flag resolution.
//!
//! Precedence is flag > `-c` > file. The only environment variables are
//! `ASTERPATH_CONFIG` and the provider API keys; keys are deliberately not part
//! of this model.

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
    ContextPolicy, DefaultApprovalPolicy, ModelAlias, ModelId, ModelSpec, Provider, ProviderId,
    ReasoningEffort, ReasoningParams, RequestConfig, SamplingParams, SandboxPolicy, SideEffect,
    SubagentConfig, ToolChoice, ToolConfig, ToolName, TurnConfig,
};
use clap::ValueEnum;
use serde::Deserialize;

/// `-a` and `[approval] default`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum ApprovalMode {
    /// Read-only in-process tools run; mutating and external tools ask.
    #[default]
    Untrusted,
    /// Never ask; the sandbox is the only guard.
    Never,
}

/// `-s` and `[sandbox] mode`.
#[derive(Clone, Copy, Debug, Default, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxMode {
    ReadOnly,
    #[default]
    WorkspaceWrite,
    FullAccess,
}

/// The file with `-c` overrides merged in, then the dedicated flags.
pub struct Settings {
    pub db: PathBuf,
    pub approval: ApprovalMode,
    pub file: FileConfig,
}

/// Load the configuration file (`--config`, else `ASTERPATH_CONFIG`, else
/// `$XDG_CONFIG_HOME/asterpath/config.toml`; only that default may be missing) and merge
/// each `-c key=value` over it.
pub fn load(config: Option<PathBuf>, overrides: &[String]) -> Result<Settings, String> {
    let explicit = config.or_else(|| env_os("ASTERPATH_CONFIG").map(PathBuf::from));
    // Only the default file may be missing; a named one that is not there is a typo.
    let named = explicit.is_some();
    let path = explicit.or_else(|| config_home().map(|home| home.join("asterpath/config.toml")));
    let mut table = match path {
        Some(path) if named || path.exists() => {
            let contents = std::fs::read_to_string(&path)
                .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
            toml::from_str(&contents).map_err(|error| format!("invalid config: {error}"))?
        }
        _ => toml::Table::new(),
    };
    for raw in overrides {
        merge(&mut table, parse_override(raw)?);
    }
    let value = toml::Value::Table(table);
    if let Some(field) = secret_field(&value) {
        return Err(format!(
            "config must not contain `{field}`; API keys are read from the environment"
        ));
    }
    let file: FileConfig = value
        .try_into()
        .map_err(|error| format!("invalid config: {error}"))?;

    let db = expand_tilde(file.storage.path.clone().unwrap_or_else(default_db));
    // Absolute now: the work root may change the process directory later.
    let db = std::path::absolute(&db).unwrap_or(db);
    Ok(Settings {
        db,
        approval: file.approval.default.unwrap_or_default(),
        file,
    })
}

/// `key=value` as a one-line TOML document: the key may be dotted or quoted,
/// and a value that is not valid TOML is taken as a string.
fn parse_override(raw: &str) -> Result<toml::Table, String> {
    let (key, value) = raw
        .split_once('=')
        .ok_or_else(|| format!("-c {raw}: expected key=value"))?;
    let key = key.trim();
    toml::from_str(&format!("{key} = {value}"))
        .or_else(|_| {
            toml::from_str(&format!(
                "{key} = {}",
                toml::Value::String(value.to_owned())
            ))
        })
        .map_err(|_| format!("-c {raw}: invalid key {key}"))
}

/// Tables merge key by key; anything else replaces.
fn merge(target: &mut toml::Table, source: toml::Table) {
    for (key, value) in source {
        match (target.get_mut(&key), value) {
            (Some(toml::Value::Table(target)), toml::Value::Table(source)) => merge(target, source),
            (_, value) => {
                target.insert(key, value);
            }
        }
    }
}

/// `$XDG_CONFIG_HOME`, else `~/.config`.
pub fn config_home() -> Option<PathBuf> {
    env_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
}

/// Parse and validate without opening any module.
pub fn validate(settings: &Settings) -> Result<(), String> {
    if settings.file.turn.max_steps_per_turn == Some(0) {
        return Err("max_steps_per_turn must be greater than zero".to_owned());
    }
    for (id, section) in &settings.file.providers {
        let protocol = protocol(id, &settings.file)?;
        if !protocol.enabled() {
            return Err(format!("unknown provider {id}"));
        }
        section.check(id, protocol)?;
    }
    for (alias, model) in &settings.file.model_aliases {
        if !provider_available(&model.provider, &settings.file) {
            return Err(format!(
                "model alias {alias} references unknown provider {}",
                model.provider
            ));
        }
    }
    if let Some(provider) = &settings.file.model.provider
        && !provider_available(provider, &settings.file)
    {
        return Err(format!("unknown provider {provider}"));
    }
    if let Some(model) = &settings.file.model.id
        && model.trim().is_empty()
    {
        return Err("the model id must not be empty".to_owned());
    }
    // Resolve the value-shaped sections now so type errors exit 2, not at run.
    settings.file.request_config()?;
    settings.mcp_servers()?;
    Ok(())
}

/// The selected provider plus every provider a model alias targets.
pub fn providers(settings: &Settings) -> Result<Vec<Arc<dyn Provider>>, String> {
    let selected = settings.provider()?;
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
    let provider = settings.provider()?.to_owned();
    let model = settings
        .file
        .model
        .id
        .clone()
        .ok_or_else(|| "a model is required ([model] id or -m)".to_owned())?;
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
        metadata: Default::default(),
    })
}

pub fn turn_config(settings: &Settings) -> TurnConfig {
    let mut config = TurnConfig::default();
    config.max_steps_per_turn = settings.file.turn.max_steps_per_turn;
    config
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

/// `untrusted` asks for every `Mutating` tool and otherwise keeps the
/// framework default (in-process allowed, external asked); the pipeline
/// denies `Ask` when no endpoint is open. `never` allows everything.
pub fn approval_policy(mode: ApprovalMode, gate: Arc<ApprovalGate>) -> Arc<dyn ApprovalPolicy> {
    match mode {
        ApprovalMode::Untrusted => Arc::new(AskMutating(DefaultApprovalPolicy::new(gate))),
        ApprovalMode::Never => Arc::new(FixedApproval(ApprovalVerdict::Allow)),
    }
}

struct AskMutating(DefaultApprovalPolicy);

impl ApprovalPolicy for AskMutating {
    fn classify(&self, request: &ApprovalRequest) -> ApprovalVerdict {
        match request.side_effect {
            SideEffect::Mutating => ApprovalVerdict::Ask,
            _ => self.0.classify(request),
        }
    }
}

struct FixedApproval(ApprovalVerdict);

impl ApprovalPolicy for FixedApproval {
    fn classify(&self, _request: &ApprovalRequest) -> ApprovalVerdict {
        self.0.clone()
    }
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Protocol {
    Anthropic,
    OpenAi,
}

impl Protocol {
    fn parse(name: &str) -> Option<Self> {
        match name {
            "anthropic" => Some(Self::Anthropic),
            "openai" => Some(Self::OpenAi),
            _ => None,
        }
    }

    fn enabled(self) -> bool {
        match self {
            Self::Anthropic => cfg!(feature = "anthropic"),
            Self::OpenAi => cfg!(feature = "openai"),
        }
    }

    fn key_env(self) -> &'static str {
        match self {
            Self::Anthropic => "ANTHROPIC_API_KEY",
            Self::OpenAi => "OPENAI_API_KEY",
        }
    }
}

/// `protocol` when set, else the built-in protocol of the same name.
fn protocol(id: &str, file: &FileConfig) -> Result<Protocol, String> {
    let section = file.providers.get(id);
    match section.and_then(|section| section.protocol.as_deref()) {
        Some(name) => {
            Protocol::parse(name).ok_or_else(|| format!("providers.{id}: unknown protocol {name}"))
        }
        None => Protocol::parse(id).ok_or_else(|| match section {
            Some(_) => format!("providers.{id}: `protocol` must be \"anthropic\" or \"openai\""),
            None => format!("unknown provider {id}"),
        }),
    }
}

fn provider_available(id: &str, file: &FileConfig) -> bool {
    protocol(id, file).is_ok_and(Protocol::enabled)
}

fn build_provider(id: &str, file: &FileConfig) -> Result<Arc<dyn Provider>, String> {
    let protocol = protocol(id, file)?;
    let section = file.providers.get(id).cloned().unwrap_or_default();
    let key_env = section
        .api_key_env
        .clone()
        .unwrap_or_else(|| protocol.key_env().to_owned());
    let key = env_string(&key_env)
        .ok_or_else(|| format!("{key_env} is required for the {id} provider"))?;
    match protocol {
        Protocol::Anthropic => build_anthropic(id, key, &section),
        Protocol::OpenAi => build_openai(id, key, &section),
    }
}

/// The instance default and the model entries, longest prefix first: the
/// table is read in key order, which would let `a` shadow `a-pro`.
#[cfg(any(feature = "anthropic", feature = "openai"))]
type Profiles<P> = (Option<P>, Vec<(String, P)>);

#[cfg(any(feature = "anthropic", feature = "openai"))]
fn profiles<P>(
    id: &str,
    section: &ProviderSection,
    profile: impl Fn(&ProfileSection) -> Result<P, String>,
) -> Result<Profiles<P>, String> {
    let default = section
        .profile
        .as_ref()
        .map(&profile)
        .transpose()
        .map_err(|error| format!("providers.{id}.profile: {error}"))?;
    let mut models = section
        .models
        .iter()
        .map(|(prefix, entry)| {
            profile(entry)
                .map(|profile| (prefix.clone(), profile))
                .map_err(|error| format!("providers.{id}.models.{prefix}: {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    models.sort_by_key(|(prefix, _)| std::cmp::Reverse(prefix.len()));
    Ok((default, models))
}

#[cfg(feature = "anthropic")]
fn build_anthropic(
    id: &str,
    key: String,
    section: &ProviderSection,
) -> Result<Arc<dyn Provider>, String> {
    use asterpath::anthropic::{AnthropicModelProfile, Thinking};
    let (default, models) = profiles(id, section, |entry| {
        let mut profile = AnthropicModelProfile::latest();
        if let Some(efforts) = entry.efforts()? {
            profile = profile.with_efforts(efforts);
        }
        if let Some(thinking) = entry.thinking {
            profile = profile.with_thinking(match thinking {
                ThinkingSection::Extended => Thinking::Extended,
                ThinkingSection::Both => Thinking::Both,
                ThinkingSection::Adaptive => Thinking::Adaptive,
            });
        }
        if let Some(on) = entry.thinking_default_on {
            profile = profile.with_thinking_default_on(on);
        }
        if let Some(forced) = entry.forced_tool_choice {
            profile = profile.with_forced_tool_choice(forced);
        }
        if let Some(sampling) = entry.sampling {
            profile = profile.with_sampling(sampling);
        }
        if let Some(structured) = entry.structured_output {
            profile = profile.with_structured_output(structured);
        }
        if let Some(types) = entry.file_media_types() {
            profile = profile.with_file_media_types(types);
        }
        if let Some(inline) = entry.inline_changes {
            profile = profile.with_inline_changes(inline);
        }
        Ok(profile)
    })?;
    let mut provider = asterpath::anthropic::AnthropicProvider::new(key).with_id(id);
    if let Some(base_url) = &section.base_url {
        provider = provider.with_base_url(base_url);
    }
    if let Some(default) = default {
        provider = provider.with_default_profile(default);
    }
    for (prefix, profile) in models {
        provider = provider.with_model_profile(prefix, profile);
    }
    Ok(Arc::new(provider))
}

#[cfg(not(feature = "anthropic"))]
fn build_anthropic(
    _id: &str,
    _key: String,
    _section: &ProviderSection,
) -> Result<Arc<dyn Provider>, String> {
    Err("the anthropic feature is not enabled".to_owned())
}

#[cfg(feature = "openai")]
fn build_openai(
    id: &str,
    key: String,
    section: &ProviderSection,
) -> Result<Arc<dyn Provider>, String> {
    use asterpath::openai::OpenAiModelProfile;
    let (default, models) = profiles(id, section, |entry| {
        let mut profile = OpenAiModelProfile::latest();
        if let Some(efforts) = entry.efforts()? {
            profile = profile.with_efforts(efforts);
        }
        if let Some(forced) = entry.forced_tool_choice {
            profile = profile.with_forced_tool_choice(forced);
        }
        if let Some(sampling) = entry.sampling {
            profile = profile.with_sampling(sampling);
        }
        if let Some(structured) = entry.structured_output {
            profile = profile.with_structured_output(structured);
        }
        if let Some(types) = entry.file_media_types() {
            profile = profile.with_file_media_types(types);
        }
        Ok(profile)
    })?;
    let mut provider = asterpath::openai::OpenAiProvider::new(key).with_id(id);
    if let Some(base_url) = &section.base_url {
        provider = provider.with_base_url(base_url);
    }
    if let Some(default) = default {
        provider = provider.with_default_profile(default);
    }
    for (prefix, profile) in models {
        provider = provider.with_model_profile(prefix, profile);
    }
    Ok(Arc::new(provider))
}

#[cfg(not(feature = "openai"))]
fn build_openai(
    _id: &str,
    _key: String,
    _section: &ProviderSection,
) -> Result<Arc<dyn Provider>, String> {
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
        "off" => Ok(ReasoningEffort::Off),
        "minimal" => Ok(ReasoningEffort::Minimal),
        "low" => Ok(ReasoningEffort::Low),
        "medium" => Ok(ReasoningEffort::Medium),
        "high" => Ok(ReasoningEffort::High),
        "xhigh" => Ok(ReasoningEffort::XHigh),
        "max" => Ok(ReasoningEffort::Max),
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
    /// Which adapter: `anthropic` or `openai`; defaults to the table name.
    protocol: Option<String>,
    base_url: Option<String>,
    /// Defaults to the protocol's own variable.
    api_key_env: Option<String>,
    /// The instance default; absent fields are the adapter's newest rules.
    profile: Option<ProfileSection>,
    /// Model id prefix -> a complete profile. Absent fields are the adapter's
    /// newest rules, not `profile`'s.
    models: BTreeMap<String, ProfileSection>,
}

impl ProviderSection {
    fn check(&self, id: &str, protocol: Protocol) -> Result<(), String> {
        let entries = self
            .profile
            .iter()
            .map(|entry| (format!("providers.{id}.profile"), entry))
            .chain(
                self.models
                    .iter()
                    .map(|(prefix, entry)| (format!("providers.{id}.models.{prefix}"), entry)),
            );
        for (path, entry) in entries {
            entry
                .efforts()
                .map_err(|error| format!("{path}: {error}"))?;
            if protocol == Protocol::OpenAi
                && (entry.thinking.is_some() || entry.thinking_default_on.is_some())
            {
                return Err(format!(
                    "{path}: `thinking` and `thinking_default_on` need the anthropic protocol"
                ));
            }
            if protocol == Protocol::OpenAi && entry.inline_changes.is_some() {
                return Err(format!(
                    "{path}: `inline_changes` needs the anthropic protocol"
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ProfileSection {
    efforts: Option<Vec<String>>,
    thinking: Option<ThinkingSection>,
    thinking_default_on: Option<bool>,
    forced_tool_choice: Option<bool>,
    sampling: Option<bool>,
    structured_output: Option<bool>,
    file_media_types: Option<Vec<String>>,
    /// Anthropic only.
    inline_changes: Option<bool>,
}

impl ProfileSection {
    fn efforts(&self) -> Result<Option<Vec<ReasoningEffort>>, String> {
        self.efforts
            .as_ref()
            .map(|efforts| efforts.iter().map(|effort| parse_effort(effort)).collect())
            .transpose()
    }

    /// The adapters take `'static` media types.
    // shortcut: leaks the strings once per provider build at CLI startup; take
    // owned strings in the adapter profiles if providers are ever rebuilt per run.
    #[cfg(any(feature = "anthropic", feature = "openai"))]
    fn file_media_types(&self) -> Option<Vec<&'static str>> {
        self.file_media_types.as_ref().map(|types| {
            types
                .iter()
                .map(|media_type| &*Box::leak(media_type.clone().into_boxed_str()))
                .collect()
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ThinkingSection {
    Extended,
    Both,
    Adaptive,
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
    /// Workspace tools for new sessions; absent means all of them.
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
    /// The provider id of new sessions.
    provider: Option<String>,
    /// The model id of new sessions.
    id: Option<String>,
    /// Maps to `ModelSpec.context_window`; absent leaves compaction `NotConfigured`.
    context_window: Option<u32>,
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
    /// `-m`: the model id of a new session.
    pub fn set_model(&mut self, model: String) {
        self.file.model.id = Some(model);
    }

    /// The provider of a continued session comes from its log, not the file.
    pub fn set_provider(&mut self, provider: String) {
        self.file.model.provider = Some(provider);
    }

    /// `-s`.
    pub fn set_sandbox(&mut self, mode: SandboxMode) {
        self.file.sandbox.mode = mode;
    }

    /// `[model] provider`.
    pub fn provider(&self) -> Result<&str, String> {
        self.file.model.provider.as_deref().ok_or_else(|| {
            "a provider is required ([model] provider or -c model.provider=…)".to_owned()
        })
    }

    /// `[skills] enabled`; absent means enabled.
    pub fn skills_enabled(&self) -> bool {
        self.file.skills.enabled.unwrap_or(true)
    }

    /// `[tools] enabled`; `None` means every workspace tool.
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
        let sampling = &self.request.sampling;
        let reasoning = &self.request.reasoning;
        let mut config = RequestConfig::default();
        if let Some(max_output_tokens) = self.request.max_output_tokens {
            config.max_output_tokens = max_output_tokens;
        }
        if let Some(value) = &self.request.tool_choice {
            config.tool_choice = parse_tool_choice(value)?;
        }
        if let Some(disable) = self.request.disable_parallel_tool_use {
            config.disable_parallel_tool_use = disable;
        }
        if sampling.temperature.is_some()
            || sampling.top_p.is_some()
            || sampling.top_k.is_some()
            || sampling.seed.is_some()
        {
            let mut params = SamplingParams::default();
            params.temperature = sampling.temperature;
            params.top_p = sampling.top_p;
            params.top_k = sampling.top_k;
            params.seed = sampling.seed;
            config.sampling = Some(params);
        }
        if reasoning.effort.is_some()
            || reasoning.budget_tokens.is_some()
            || reasoning.summary.is_some()
        {
            let mut params = ReasoningParams::default();
            params.effort = reasoning.effort.as_deref().map(parse_effort).transpose()?;
            params.budget_tokens = reasoning.budget_tokens;
            params.summary = reasoning.summary.unwrap_or(false);
            config.reasoning = Some(params);
        }
        Ok(config)
    }

    fn compaction_config(&self) -> CompactionConfig {
        let mut config = CompactionConfig::default();
        let file = &self.compaction;
        config.reserve_output_tokens = file.reserve_output_tokens.or(config.reserve_output_tokens);
        config.target_ratio = file.target_ratio.unwrap_or(config.target_ratio);
        config.max_auto_compactions_per_turn = file
            .max_auto_compactions_per_turn
            .unwrap_or(config.max_auto_compactions_per_turn);
        config.max_summary_tokens = file.max_summary_tokens.unwrap_or(config.max_summary_tokens);
        config.summary_model = file
            .summary_model
            .as_ref()
            .map(ModelItem::spec)
            .or(config.summary_model);
        config
    }

    /// Process-wide; maps to the facade, not the per-session snapshot.
    fn max_live_children(&self) -> NonZeroUsize {
        self.subagents
            .max_live_children
            .unwrap_or(NonZeroUsize::new(32).expect("non-zero"))
    }

    fn subagent_config(&self) -> SubagentConfig {
        let mut config = SubagentConfig::default();
        let file = &self.subagents;
        config.default_context = file
            .default_context
            .clone()
            .unwrap_or(config.default_context);
        config.max_depth = file.max_depth.unwrap_or(config.max_depth);
        config.max_concurrent_children = file
            .max_concurrent_children
            .unwrap_or(config.max_concurrent_children);
        config.spawn_timeout = file.spawn_timeout.or(config.spawn_timeout);
        config.cancel_grace = file.cancel_grace.unwrap_or(config.cancel_grace);
        config
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
    fn effort_accepts_every_level() {
        for (text, effort) in [
            ("off", ReasoningEffort::Off),
            ("minimal", ReasoningEffort::Minimal),
            ("low", ReasoningEffort::Low),
            ("medium", ReasoningEffort::Medium),
            ("high", ReasoningEffort::High),
            ("xhigh", ReasoningEffort::XHigh),
            ("max", ReasoningEffort::Max),
        ] {
            assert_eq!(parse_effort(text).unwrap(), effort);
        }
        assert!(parse_effort("none").is_err());
    }

    fn overridden(file: &str, overrides: &[&str]) -> toml::Table {
        let mut table: toml::Table = toml::from_str(file).unwrap();
        for raw in overrides {
            merge(&mut table, parse_override(raw).unwrap());
        }
        table
    }

    #[test]
    fn overrides_parse_toml_values_and_fall_back_to_strings() {
        let table = overridden(
            "[model]\nprovider = \"openai\"\ncontext_window = 1\n",
            &[
                "model.id=claude-test",
                "model.context_window=200000",
                "tools.enabled=[\"read_file\"]",
                "skills.enabled=false",
                "providers.deepseek.models.\"v4-pro\".efforts=[\"low\"]",
            ],
        );
        let expected: toml::Table = toml::from_str(concat!(
            "[model]\nprovider = \"openai\"\nid = \"claude-test\"\ncontext_window = 200000\n",
            "[tools]\nenabled = [\"read_file\"]\n[skills]\nenabled = false\n",
            "[providers.deepseek.models.\"v4-pro\"]\nefforts = [\"low\"]\n",
        ))
        .unwrap();
        assert_eq!(table, expected);
    }

    #[test]
    fn overrides_reject_a_missing_value_or_a_bad_key() {
        assert!(parse_override("model.id").is_err());
        assert!(parse_override("a b=1").is_err());
        assert!(parse_override("=1").is_err());
    }

    fn file(contents: &str) -> FileConfig {
        toml::from_str(contents).unwrap()
    }

    #[test]
    fn subagents_max_live_children_is_the_global_cap() {
        let config = file("[subagents]\nmax_live_children = 4\n");
        assert_eq!(config.max_live_children().get(), 4);
        assert_eq!(file("").max_live_children().get(), 32);
    }
}
