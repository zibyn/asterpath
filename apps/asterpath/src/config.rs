//! `config.toml`, `-c key=value` overrides and flag resolution.
//!
//! Precedence is flag > `-c` > file. The only environment variables are
//! `ASTERPATH_CONFIG` and the provider API keys. A key may also sit in
//! `[providers.<id>] api_key`; the environment wins (ADR-0050).
// `AsterError` is the facade's error, returned as is.
#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use asterpath::{AsterError, Asterpath, AsterpathBuilder, AsterpathConfig, DefaultCompactor};
use asterpath_mcp::{
    ADAPTER_PROTOCOL_VERSIONS, McpServerConfig, McpTimeouts, McpTransport, ServerId,
};
use asterpath_protocol::{
    CompactionConfig, ContextPolicy, ModelAlias, ModelId, ModelSpec, Provider, ProviderCatalog,
    ProviderId, ReasoningEffort, ReasoningParams, RequestConfig, SamplingParams, SubagentConfig,
    Tool, ToolChoice, ToolConfig, ToolName, TurnConfig,
};
use asterpath_skills::{SkillRoot, SkillRootLabel};
use serde::Deserialize;

use crate::approval::ApprovalModes;
use crate::workspace::{SandboxBackend, SandboxMode, WorkRoot, Workspace, WorkspaceError};

/// `-a` and `[approval] default`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ApprovalMode {
    /// Calls outside the sandbox and external tools ask (ADR-0055).
    #[default]
    Untrusted,
    /// Never ask; the sandbox, if any, is the only guard.
    Never,
}

/// The file with `-c` overrides merged in, then the dedicated flags.
#[derive(Clone)]
pub struct Settings {
    pub db: PathBuf,
    pub approval: ApprovalMode,
    pub file: FileConfig,
    /// The file read, or the default path; `None` without a home directory.
    pub path: Option<PathBuf>,
}

/// Load the configuration file (`--config`, else `ASTERPATH_CONFIG`, else
/// `$XDG_CONFIG_HOME/asterpath/config.toml`; only that default may be missing,
/// and it is created from [`TEMPLATE`] then) and merge each `-c key=value` over it.
pub fn load(config: Option<PathBuf>, overrides: &[String]) -> Result<Settings, WorkspaceError> {
    // Only the default file may be missing; a named one that is not there is a typo.
    let named = config.is_some() || env_os("ASTERPATH_CONFIG").is_some();
    let path = config_path(config);
    if let Some(path) = &path
        && !named
        && !path.exists()
        && let Err(error) = create_template(path)
    {
        eprintln!(
            "asterpath: cannot create {}: {error}; using defaults",
            path.display()
        );
    }
    let mut table = match &path {
        Some(path) if named || path.exists() => {
            let contents = std::fs::read_to_string(path).map_err(|source| WorkspaceError::Io {
                context: format!("cannot read {}", path.display()),
                source,
            })?;
            let table: toml::Table = toml::from_str(&contents)
                .map_err(|error| parse_error(&contents, error.message(), error.span()))?;
            check_key_permissions(path, &table).map_err(WorkspaceError::Config)?;
            table
        }
        _ => toml::Table::new(),
    };
    for raw in overrides {
        let parsed = parse_override(raw).map_err(WorkspaceError::Config)?;
        if has_key(&parsed, "api_key") {
            return Err(WorkspaceError::Config(format!(
                "-c {}: API keys go in the environment or the config file, not -c",
                raw.split_once('=').map_or(raw.as_str(), |(key, _)| key)
            )));
        }
        merge(&mut table, parsed);
    }
    from_table(table, path)
}

/// `--config`, else `ASTERPATH_CONFIG`, else the default path.
pub fn config_path(config: Option<PathBuf>) -> Option<PathBuf> {
    config
        .or_else(|| env_os("ASTERPATH_CONFIG").map(PathBuf::from))
        .or_else(|| config_home().map(|home| home.join("asterpath/config.toml")))
}

fn from_table(table: toml::Table, path: Option<PathBuf>) -> Result<Settings, WorkspaceError> {
    let value = toml::Value::Table(table);
    if let Some(field) = secret_field(&value, &[]) {
        return Err(WorkspaceError::Config(format!(
            "config must not contain `{field}`; API keys go in [providers.<id>] api_key or the environment"
        )));
    }
    let file: FileConfig = value
        .try_into()
        .map_err(|error| WorkspaceError::Config(format!("invalid config: {error}")))?;

    let db = expand_tilde(file.storage.path.clone().unwrap_or_else(default_db));
    // Absolute now: the work root may change the process directory later.
    let db = std::path::absolute(&db).unwrap_or(db);
    Ok(Settings {
        db,
        approval: file.approval.default.unwrap_or_default(),
        file,
        path,
    })
}

/// The file written on first run: commented examples only, no chosen values.
pub const TEMPLATE: &str = r#"# asterpath configuration. Uncomment and edit; `asterpath doctor` checks it.

# default_agent = "default"       # an agent profile; `asterpath agents` lists them
# output_style = "explanatory"    # an output style; `asterpath styles` lists them; exec ignores it

# [model]
# provider = "anthropic"          # or "openai", or a [providers.<id>] instance
# id = "claude-fable-5-1"

# [providers.anthropic]
# The environment variable (ANTHROPIC_API_KEY, OPENAI_API_KEY or api_key_env)
# wins over api_key. A file holding a key must be readable by its owner only.
# api_key = "sk-..."

# [providers.openai]
# api_key = "sk-..."

# [sandbox]
# mode = "workspace-write"        # read-only | workspace-write | full-access
"#;

/// Write [`TEMPLATE`] at `path`: the directory `0700`, the file `0600` (Unix).
fn create_template(path: &Path) -> std::io::Result<()> {
    create_parent(path)?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    match options.open(path) {
        Ok(mut file) => std::io::Write::write_all(&mut file, TEMPLATE.as_bytes()),
        // Another process created it first.
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error),
    }
}

/// `path`'s missing directories, `0700` (Unix).
fn create_parent(path: &Path) -> std::io::Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(parent)
}

/// Set `[model] provider`, `[model] id` and, unless `api_key` is `None`,
/// `[providers.<provider>] api_key` in the file at `path`, keeping its
/// The desktop settings page's values. No `Debug`: it may hold a key.
#[derive(Clone, Deserialize)]
pub struct SetupChoice {
    pub provider: String,
    pub model: String,
    /// `protocol` of a provider instance; `None` keeps the file's.
    pub protocol: Option<String>,
    /// `None` keeps the file's; empty removes it (the adapter's default endpoint).
    pub base_url: Option<String>,
    /// `None` or empty keeps the file's.
    pub api_key: Option<String>,
}

/// Write `choice` to the file at `path`: `[model] provider` / `id` and the
/// `[providers.<provider>]` fields it sets, keeping the file's comments and
/// order. The result is validated before it is written, `0600`.
pub fn save_setup(path: &Path, choice: &SetupChoice) -> Result<(), WorkspaceError> {
    let io = |context: &str, source| WorkspaceError::Io {
        context: format!("{context} {}", path.display()),
        source,
    };
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => return Err(io("cannot read", error)),
    };
    let mut doc: toml_edit::DocumentMut =
        contents.parse().map_err(|error: toml_edit::TomlError| {
            parse_error(&contents, error.message(), error.span())
        })?;
    let section = table(doc.as_table_mut(), "model", false)?;
    section["provider"] = toml_edit::value(&choice.provider);
    section["id"] = toml_edit::value(&choice.model);
    let api_key = choice.api_key.as_deref().filter(|key| !key.is_empty());
    if choice.protocol.is_some() || choice.base_url.is_some() || api_key.is_some() {
        let providers = table(doc.as_table_mut(), "providers", true)?;
        let section = table(providers, &choice.provider, false)?;
        if let Some(protocol) = &choice.protocol {
            section["protocol"] = toml_edit::value(protocol);
            // Without its own variable an instance would read the protocol's
            // (`ANTHROPIC_API_KEY`), which then beats the key saved here.
            if Protocol::parse(&choice.provider).is_none() && !section.contains_key("api_key_env") {
                let env = format!(
                    "{}_API_KEY",
                    choice.provider.to_uppercase().replace('-', "_")
                );
                section["api_key_env"] = toml_edit::value(env);
            }
        }
        match choice.base_url.as_deref() {
            Some("") => {
                section.remove("base_url");
            }
            Some(url) => section["base_url"] = toml_edit::value(url),
            None => {}
        }
        if let Some(key) = api_key {
            section["api_key"] = toml_edit::value(key);
        }
    }
    let text = doc.to_string();
    let parsed =
        toml::from_str(&text).map_err(|error| parse_error(&text, error.message(), error.span()))?;
    let settings = from_table(parsed, Some(path.to_owned()))?;
    validate(&settings)?;

    // A sibling file renamed over the original: a failed write never truncates it.
    create_parent(path).map_err(|error| io("cannot create", error))?;
    let temp = path.with_extension("toml.tmp");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options
        .open(&temp)
        .map_err(|error| io("cannot write", error))?;
    // Owner-only before the key is in it; `mode` above only applies to a new file.
    #[cfg(unix)]
    file.set_permissions(std::os::unix::fs::PermissionsExt::from_mode(0o600))
        .map_err(|error| io("cannot write", error))?;
    std::io::Write::write_all(&mut file, text.as_bytes())
        .and_then(|()| file.sync_all())
        .and_then(|()| std::fs::rename(&temp, path))
        .map_err(|error| {
            let _ = std::fs::remove_file(&temp);
            io("cannot write", error)
        })
}

/// A parse error by line, without the source line: it may hold a key.
fn parse_error(text: &str, message: &str, span: Option<std::ops::Range<usize>>) -> WorkspaceError {
    let line = span.map_or(String::new(), |span| {
        let line = text[..span.start.min(text.len())].lines().count().max(1);
        format!(" at line {line}")
    });
    WorkspaceError::Config(format!("invalid config{line}: {}", message.trim()))
}

/// The table `key` of `parent`, created when missing (`implicit`: no header of its own).
fn table<'a>(
    parent: &'a mut toml_edit::Table,
    key: &str,
    implicit: bool,
) -> Result<&'a mut toml_edit::Table, WorkspaceError> {
    let item = parent.entry(key).or_insert_with(|| {
        let mut table = toml_edit::Table::new();
        table.set_implicit(implicit);
        toml_edit::Item::Table(table)
    });
    item.as_table_mut()
        .ok_or_else(|| WorkspaceError::Config(format!("`{key}` must be a table")))
}

/// A file holding a key must not be open to the group or others (Unix).
fn check_key_permissions(path: &Path, table: &toml::Table) -> Result<(), String> {
    #[cfg(unix)]
    if has_key(table, "api_key") {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)
            .map_err(|error| format!("cannot read {}: {error}", path.display()))?
            .permissions()
            .mode();
        if mode & 0o077 != 0 {
            return Err(format!(
                "{} holds an API key but is readable by others (mode {:o}); run `chmod 600 {}`",
                path.display(),
                mode & 0o777,
                path.display()
            ));
        }
    }
    #[cfg(not(unix))]
    let _ = (path, table);
    Ok(())
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
        // Not `raw`: its value may be a key.
        .map_err(|_| format!("-c: invalid key {key}"))
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
pub fn validate(settings: &Settings) -> Result<(), WorkspaceError> {
    check(settings).map_err(WorkspaceError::Config)
}

fn check(settings: &Settings) -> Result<(), String> {
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
    settings.mcp_servers().map_err(|error| error.to_string())?;
    Ok(())
}

/// The selected provider plus every provider a model alias targets.
pub fn providers(settings: &Settings) -> Result<Vec<Arc<dyn Provider>>, WorkspaceError> {
    let selected = settings.provider()?;
    let mut ids = vec![selected.to_owned()];
    for model in settings.file.model_aliases.values() {
        if !ids.contains(&model.provider) {
            ids.push(model.provider.clone());
        }
    }
    ids.iter()
        .map(|id| build_provider(id, &settings.file).map_err(WorkspaceError::Config))
        .collect()
}

pub fn session_spec(
    settings: &Settings,
) -> Result<asterpath_protocol::SessionSpec, WorkspaceError> {
    let provider = settings.provider()?.to_owned();
    let model = settings.file.model.id.clone().ok_or_else(|| {
        WorkspaceError::Config("a model is required ([model] id or -m)".to_owned())
    })?;
    Ok(asterpath_protocol::SessionSpec {
        model: ModelSpec {
            provider: ProviderId::new(provider),
            model: ModelId::new(model),
            context_window: settings.file.model.context_window,
        },
        system: Vec::new(),
        tools: asterpath_protocol::ToolSelection::All,
        request: settings
            .file
            .request_config()
            .map_err(WorkspaceError::Config)?,
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

/// The composition root: providers, storage and limits from `settings`,
/// `tools` for every session, `environment` per session with its `bash`
/// capped by `approval`, and `approval` as the policy. No approval
/// endpoint is registered here: an application that can ask registers one around
/// its drive.
pub async fn assemble(
    settings: &Settings,
    tools: Vec<Arc<dyn Tool>>,
    environment: WorkRoot,
    approval: Arc<ApprovalModes>,
) -> Result<Asterpath, AsterError> {
    let config_error = |message: String| AsterError::Config { message };
    let providers = providers(settings).map_err(|error| config_error(error.to_string()))?;
    // Sessions without a context window still report `NotConfigured`.
    let catalog =
        ProviderCatalog::new(providers.clone()).map_err(|error| config_error(error.to_string()))?;
    create_db_dir(settings)?;
    let config = AsterpathConfig::default();
    AsterpathBuilder::from_config(config)
        .compactor(Arc::new(DefaultCompactor::new(Arc::new(catalog))))
        .database(&settings.db)
        .providers(providers)
        .tools(tools)
        .environment(approval.environment(Arc::new(environment)))
        .tool(tool_config(settings))
        .max_live_children(max_live_children(settings))
        .model_aliases(model_aliases(settings))
        .approval(approval)
        .build()
        .await
}

/// The database's missing directories, `0700` (Unix); the storage adapter
/// only opens the file.
pub fn create_db_dir(settings: &Settings) -> Result<(), AsterError> {
    create_parent(&settings.db).map_err(|error| {
        AsterError::Storage(asterpath_protocol::StorageError::Backend(format!(
            "cannot create the directory of {}: {error}",
            settings.db.display()
        )))
    })
}

/// A secret-looking field anywhere but `providers.<id>.api_key`.
fn secret_field(value: &toml::Value, path: &[&str]) -> Option<String> {
    match value {
        toml::Value::Table(table) => table.iter().find_map(|(key, value)| {
            let allowed = key == "api_key" && matches!(path, ["providers", _]);
            if !allowed && matches!(key.as_str(), "api_key" | "secret" | "token") {
                Some(key.clone())
            } else {
                secret_field(value, &[path, &[key.as_str()]].concat())
            }
        }),
        toml::Value::Array(items) => items.iter().find_map(|item| secret_field(item, path)),
        _ => None,
    }
}

fn has_key(table: &toml::Table, name: &str) -> bool {
    fn within(value: &toml::Value, name: &str) -> bool {
        match value {
            toml::Value::Table(table) => has_key(table, name),
            toml::Value::Array(items) => items.iter().any(|item| within(item, name)),
            _ => false,
        }
    }
    table
        .iter()
        .any(|(key, value)| key == name || within(value, name))
}

/// Where a provider's API key comes from; never the key itself.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", content = "name", rename_all = "snake_case")]
pub enum KeySource {
    /// The named environment variable.
    Env(String),
    /// `[providers.<id>] api_key`.
    File,
    /// Neither is set.
    Missing,
}

impl std::fmt::Display for KeySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Env(name) => write!(f, "env:{name}"),
            Self::File => f.write_str("file"),
            Self::Missing => f.write_str("not set"),
        }
    }
}

/// The selected provider, the aliases' and every `[providers.<id>]`, each
/// with its key source; unavailable ids are left out.
pub fn key_sources(settings: &Settings) -> Vec<(String, KeySource)> {
    let file = &settings.file;
    let mut ids: Vec<&String> = file.model.provider.iter().collect();
    ids.extend(file.model_aliases.values().map(|model| &model.provider));
    ids.extend(file.providers.keys());
    let mut seen = Vec::new();
    for id in ids {
        if !seen.contains(id) && provider_available(id, file) {
            seen.push(id.clone());
        }
    }
    seen.into_iter()
        .map(|id| {
            let source = file_key_source(&id, file);
            (id, source)
        })
        .collect()
}

/// Provider `id`'s key source; `None` when `id` is not available.
pub fn key_source(settings: &Settings, id: &str) -> Option<KeySource> {
    provider_available(id, &settings.file).then(|| file_key_source(id, &settings.file))
}

/// Provider `id`'s protocol name and `base_url`; `None` when `id` is not available.
pub fn endpoint<'a>(settings: &'a Settings, id: &str) -> Option<(&'static str, Option<&'a str>)> {
    let protocol = protocol(id, &settings.file).ok().filter(|p| p.enabled())?;
    let base_url = settings
        .file
        .providers
        .get(id)
        .and_then(|section| section.base_url.as_deref());
    Some((protocol.name(), base_url))
}

/// The environment variable first, then the file.
fn file_key_source(id: &str, file: &FileConfig) -> KeySource {
    let section = file.providers.get(id);
    let env = section
        .and_then(|section| section.api_key_env.clone())
        .or_else(|| protocol(id, file).ok().map(|p| p.key_env().to_owned()));
    match env {
        Some(env) if env_string(&env).is_some() => KeySource::Env(env),
        _ if section.is_some_and(|section| {
            section
                .api_key
                .as_deref()
                .is_some_and(|key| !key.is_empty())
        }) =>
        {
            KeySource::File
        }
        _ => KeySource::Missing,
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

    fn name(self) -> &'static str {
        match self {
            Self::Anthropic => "anthropic",
            Self::OpenAi => "openai",
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
        .or_else(|| section.api_key.clone().filter(|key| !key.is_empty()))
        .ok_or_else(|| {
            format!("{key_env} or [providers.{id}] api_key is required for the {id} provider")
        })?;
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
    /// The agent profile of new sessions (ADR-0051).
    default_agent: Option<String>,
    /// The output style of new sessions (ADR-0052).
    output_style: Option<String>,
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
    /// Used when the variable is unset (ADR-0050).
    api_key: Option<String>,
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
    /// Names of this process's variables passed on to bash (ADR-0037).
    env: Vec<String>,
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

    /// `--agent`: the agent profile of a new session.
    pub fn set_agent(&mut self, agent: String) {
        self.file.default_agent = Some(agent);
    }

    /// `--style`; `None` drops `output_style`, as `exec` does.
    pub fn set_style(&mut self, style: Option<String>) {
        self.file.output_style = style;
    }

    /// `-s`.
    pub fn set_sandbox(&mut self, mode: SandboxMode) {
        self.file.sandbox.mode = mode;
    }

    /// `[model] id`.
    pub fn model_id(&self) -> Option<&str> {
        self.file.model.id.as_deref()
    }

    /// `[model] provider`.
    pub fn provider(&self) -> Result<&str, WorkspaceError> {
        self.file.model.provider.as_deref().ok_or_else(|| {
            let file = self
                .path
                .as_ref()
                .map(|path| format!(" in {}", path.display()))
                .unwrap_or_default();
            WorkspaceError::Config(format!(
                "a provider is required ([model] provider{file} or -c model.provider=…)"
            ))
        })
    }

    /// The work-root values: `[sandbox]`, `[tools] enabled`, `[skills]`
    /// (absent `enabled` means enabled) with the user roots under
    /// `$XDG_CONFIG_HOME` and `$HOME`, the `[sandbox] env` variables this
    /// process has, `[mcp_servers]`, `default_agent`, `output_style` and the user agent and
    /// style directories.
    pub fn workspace(&self) -> Result<Workspace, WorkspaceError> {
        let mut user_skill_roots = Vec::new();
        if let Some(home) = config_home() {
            user_skill_roots.push(SkillRoot {
                path: home.join("asterpath/skills"),
                label: SkillRootLabel::UserNative,
            });
        }
        if let Some(home) = std::env::var_os("HOME") {
            user_skill_roots.push(SkillRoot {
                path: PathBuf::from(home).join(".agents/skills"),
                label: SkillRootLabel::UserAgents,
            });
        }
        Ok(Workspace {
            sandbox: self.file.sandbox.mode,
            sandbox_backend: SandboxBackend::probe(),
            tools: self.file.tools.enabled.clone(),
            skills_enabled: self.file.skills.enabled.unwrap_or(true),
            skill_paths: self.file.skills.paths.clone(),
            user_skill_roots,
            spawn_vars: self
                .file
                .sandbox
                .env
                .iter()
                .filter_map(|name| Some((OsString::from(name), env_os(name)?)))
                .collect(),
            mcp_servers: self.mcp_servers()?,
            agent: self.file.default_agent.clone(),
            user_agents: config_home().map(|home| home.join("asterpath/agents")),
            style: self.file.output_style.clone(),
            user_styles: config_home().map(|home| home.join("asterpath/styles")),
        })
    }

    /// `[mcp_servers.<id>]` mapped to adapter configs, in id order.
    pub fn mcp_servers(&self) -> Result<Vec<McpServerConfig>, WorkspaceError> {
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
                        return Err(WorkspaceError::Config(format!(
                            "mcp_servers.{id}: set exactly one of `command` or `url`"
                        )));
                    }
                };
                Ok(McpServerConfig {
                    id: ServerId::new(id.clone()).map_err(|error| {
                        WorkspaceError::Config(format!("mcp_servers.{id}: {error}"))
                    })?,
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
        assert_eq!(secret_field(&value, &[]).as_deref(), Some("token"));
    }

    #[test]
    fn api_key_is_allowed_only_in_a_provider_table() {
        let secret = |text: &str| secret_field(&toml::from_str(text).unwrap(), &[]);
        assert_eq!(secret("[providers.anthropic]\napi_key = \"k\"\n"), None);
        assert_eq!(
            secret("[model]\napi_key = \"k\"\n").as_deref(),
            Some("api_key")
        );
        assert_eq!(
            secret("[providers.a.profile]\napi_key = \"k\"\n").as_deref(),
            Some("api_key")
        );
        assert_eq!(
            secret("[providers.anthropic]\ntoken = \"k\"\n").as_deref(),
            Some("token")
        );
    }

    #[test]
    fn parse_errors_do_not_echo_the_line() {
        let text = "[providers.anthropic]\napi_key = sk-secret\n";
        let error = toml::from_str::<toml::Table>(text).unwrap_err();
        let message = parse_error(text, error.message(), error.span()).to_string();
        assert!(message.starts_with("invalid config at line 2"), "{message}");
        assert!(!message.contains("sk-secret"), "{message}");
        let error = parse_override("providers.a b.api_key=sk-secret").unwrap_err();
        assert!(!error.contains("sk-secret"), "{error}");
    }

    #[test]
    fn the_template_parses_to_defaults() {
        let table: toml::Table = toml::from_str(TEMPLATE).unwrap();
        assert!(table.is_empty());
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
