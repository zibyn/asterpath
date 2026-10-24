//! Loading pipeline: config validation, sandbox, spawn, handshake, discovery
//! and naming (`mcp.md` §2, ADR-0027).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::sync::Arc;

use asterpath_protocol::{
    ConfinedSpawn, Enforcement, JsonString, Parallelism, Provenance, SandboxPolicy,
    SandboxProvider, SpawnRequest, Tool, ToolContext, ToolDescriptor, ToolError, ToolName,
    ToolOutput, TransportKind, external_tool_name, validate_server_id,
};

use crate::adapter::{AdapterError, AdapterTool, BoxFuture, McpAdapter};
use crate::config::{McpServerConfig, McpTransport, ServerId};
use crate::http::HttpAdapter;
use crate::mcp::ADAPTER_PROTOCOL_VERSIONS;
use crate::stdio::StdioAdapter;

const CANCEL_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// One server's load result.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServerStatus {
    Loaded { tools: Vec<ToolName> },
    Failed { stage: LoadStage, message: String },
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoadStage {
    Config,
    /// No provider for a restricted policy, the provider refused, or it could
    /// not fully enforce the policy. No process was started.
    Sandbox,
    Spawn,
    Handshake,
    Discovery,
    Registration,
}

/// A required server failed; the embedder should abort startup.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("MCP server {server} failed during {stage:?}: {message}")]
pub struct McpError {
    pub server: ServerId,
    pub stage: LoadStage,
    pub message: String,
}

/// The loaded MCP servers: a tool source the embedder hands to the engine
/// builder. Shut it down after the engine (ADR-0027); dropping it without
/// [`Self::shutdown`] still kills stdio processes once every tool is dropped.
pub struct McpServers {
    /// Loaded tools in load order; names are unique.
    tools: Vec<Arc<dyn Tool>>,
    statuses: Vec<ServerStatus>,
    adapters: Vec<Arc<dyn McpAdapter>>,
}

impl fmt::Debug for McpServers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpServers")
            .field("statuses", &self.statuses)
            .finish_non_exhaustive()
    }
}

impl McpServers {
    /// Loads every config, recording one status per config in input order.
    ///
    /// All configs are validated before any process is started. Processing
    /// then stops at the first `required` failure and returns an error; servers
    /// already loaded are dropped (and their processes killed).
    pub async fn load(
        configs: Vec<McpServerConfig>,
        sandbox: Option<Arc<dyn SandboxProvider>>,
        policy: SandboxPolicy,
    ) -> Result<Self, McpError> {
        let duplicates = duplicate_ids(&configs);
        let mut statuses: Vec<Option<ServerStatus>> = vec![None; configs.len()];

        for (index, config) in configs.iter().enumerate() {
            let failure = if duplicates.contains(config.id.as_str()) {
                Some((
                    LoadStage::Config,
                    "duplicate server id in the load set".to_owned(),
                ))
            } else if let Err(failure) = validate_config(config) {
                Some(failure)
            } else if candidate_version(config).is_none() {
                Some((
                    LoadStage::Handshake,
                    "no common protocol version".to_owned(),
                ))
            } else {
                None
            };
            if let Some((stage, message)) = failure {
                if config.required {
                    return Err(McpError {
                        server: config.id.clone(),
                        stage,
                        message,
                    });
                }
                statuses[index] = Some(ServerStatus::Failed { stage, message });
            }
        }

        let mut servers = Self {
            tools: Vec::new(),
            statuses: Vec::new(),
            adapters: Vec::new(),
        };
        let loader = Loader {
            sandbox: sandbox.as_deref(),
            policy: &policy,
        };
        for (index, config) in configs.into_iter().enumerate() {
            if statuses[index].is_some() {
                continue;
            }
            let status = match loader.load_one(&config).await {
                Ok(discovered) => servers.register(&config, discovered).await,
                Err((stage, message)) => ServerStatus::Failed { stage, message },
            };
            if let ServerStatus::Failed { stage, message } = &status
                && config.required
            {
                return Err(McpError {
                    server: config.id.clone(),
                    stage: *stage,
                    message: message.clone(),
                });
            }
            statuses[index] = Some(status);
        }

        servers.statuses = statuses
            .into_iter()
            .map(|status| status.expect("every config has a status"))
            .collect();
        Ok(servers)
    }

    /// Every loaded tool, in load order.
    pub fn tools(&self) -> Vec<Arc<dyn Tool>> {
        self.tools.clone()
    }

    /// One status per config, in input order.
    pub fn statuses(&self) -> &[ServerStatus] {
        &self.statuses
    }

    /// Closes every server gracefully. Tools still held by the engine fail
    /// with `Transport` afterwards.
    pub async fn shutdown(self) {
        for adapter in &self.adapters {
            adapter.shutdown().await;
        }
    }

    /// Naming and atomic registration of one handshaken, discovered server.
    async fn register(
        &mut self,
        config: &McpServerConfig,
        (adapter, discovered, transport): Discovered,
    ) -> ServerStatus {
        let composed = match compose_names(config.id.as_str(), discovered) {
            Ok(composed) => composed,
            Err(message) => {
                adapter.shutdown().await;
                return failed(LoadStage::Registration, message);
            }
        };
        for (name, _) in &composed {
            if self
                .tools
                .iter()
                .any(|tool| &tool.descriptor().name == name)
            {
                adapter.shutdown().await;
                return failed(
                    LoadStage::Registration,
                    format!("tool {} is already registered", name.as_str()),
                );
            }
        }

        let adapter: Arc<dyn McpAdapter> = Arc::from(adapter);
        let mut names = Vec::with_capacity(composed.len());
        for (name, tool) in composed {
            let descriptor = ToolDescriptor {
                name: name.clone(),
                description: tool.description.clone(),
                schema: tool.schema.clone(),
                parallelism: Parallelism::Parallel,
                replay: tool.replay,
                side_effect: tool.side_effect,
                provenance: Provenance::External { transport },
                timeout: Some(config.timeouts.call_timeout()),
                max_output_bytes: None,
                strict: None,
            };
            self.tools.push(Arc::new(ExternalTool {
                descriptor,
                adapter: Arc::clone(&adapter),
                original_name: tool.original_name.clone(),
            }));
            names.push(name);
        }
        self.adapters.push(adapter);
        ServerStatus::Loaded { tools: names }
    }
}

type Failure = (LoadStage, String);
type Discovered = (Box<dyn McpAdapter>, Vec<AdapterTool>, TransportKind);

struct Loader<'a> {
    sandbox: Option<&'a dyn SandboxProvider>,
    policy: &'a SandboxPolicy,
}

impl Loader<'_> {
    async fn load_one(&self, config: &McpServerConfig) -> Result<Discovered, Failure> {
        let candidate = candidate_version(config).expect("checked during config validation");
        let (adapter, transport): (Box<dyn McpAdapter>, _) = match &config.transport {
            McpTransport::Http { url, headers } => {
                let adapter =
                    HttpAdapter::new(url, headers, candidate, config.timeouts.startup_timeout())
                        .map_err(|error| (LoadStage::Spawn, adapter_message(error)))?;
                (Box::new(adapter), TransportKind::Http)
            }
            McpTransport::Stdio {
                command,
                args,
                env,
                cwd,
            } => {
                let confined = self.confine(command, args, env, cwd)?;
                let adapter = StdioAdapter::spawn(
                    &confined,
                    candidate,
                    config.timeouts.startup_timeout(),
                    CANCEL_GRACE,
                )
                .map_err(|error| {
                    (
                        LoadStage::Spawn,
                        format!("failed to start MCP server process: {error}"),
                    )
                })?;
                (Box::new(adapter), TransportKind::Stdio)
            }
        };
        let (adapter, tools) = handshake_and_discover(config, adapter).await?;
        Ok((adapter, tools, transport))
    }

    /// The command to start, or a sandbox-stage failure. Fails closed on any
    /// enforcement short of `Full` (`sandbox.md` §8).
    fn confine(
        &self,
        command: &str,
        args: &[String],
        env: &BTreeMap<String, String>,
        cwd: &Option<std::path::PathBuf>,
    ) -> Result<ConfinedSpawn, Failure> {
        let sandbox_failure = |message: String| (LoadStage::Sandbox, message);
        let mut spawn_env: BTreeMap<OsString, OsString> = std::env::vars_os().collect();
        for (key, value) in env {
            spawn_env.insert(OsString::from(key), OsString::from(value));
        }
        let args: Vec<OsString> = args.iter().map(OsString::from).collect();
        let request = SpawnRequest::new(
            OsStr::new(command),
            &args,
            cwd.as_deref(),
            &spawn_env,
            self.policy,
        );
        let confined = match self.sandbox {
            Some(provider) => provider
                .confine(&request)
                .map_err(|error| sandbox_failure(error.to_string()))?,
            None if matches!(self.policy, SandboxPolicy::FullAccess) => {
                ConfinedSpawn::unconfined(&request)
            }
            None => {
                return Err(sandbox_failure(
                    "a restricted sandbox policy requires a SandboxProvider".to_owned(),
                ));
            }
        };
        // shortcut: Partial always fails; when `allow_partial` lands, let it
        // through and report the enforcement level to the embedder.
        if confined.enforcement != Enforcement::Full {
            return Err(sandbox_failure(format!(
                "sandbox did not fully enforce the policy: {:?}",
                confined.enforcement
            )));
        }
        Ok(confined)
    }
}

/// Handshake and discovery; shared by both transports. The adapter is shut
/// down on failure.
async fn handshake_and_discover(
    config: &McpServerConfig,
    mut adapter: Box<dyn McpAdapter>,
) -> Result<(Box<dyn McpAdapter>, Vec<AdapterTool>), Failure> {
    let result = async {
        let handshake = adapter
            .handshake()
            .await
            .map_err(|error| (LoadStage::Handshake, adapter_message(error)))?;
        if !config
            .protocol_versions
            .iter()
            .any(|version| version == &handshake.protocol_version)
        {
            return Err((
                LoadStage::Handshake,
                format!(
                    "negotiated protocol version {} is not in protocol_versions",
                    handshake.protocol_version
                ),
            ));
        }
        adapter
            .discover()
            .await
            .map_err(|error| (LoadStage::Discovery, adapter_message(error)))
    }
    .await;
    match result {
        Ok(tools) => Ok((adapter, tools)),
        Err(failure) => {
            adapter.shutdown().await;
            Err(failure)
        }
    }
}

fn failed(stage: LoadStage, message: impl Into<String>) -> ServerStatus {
    ServerStatus::Failed {
        stage,
        message: message.into(),
    }
}

fn adapter_message(error: AdapterError) -> String {
    error.message().to_owned()
}

fn candidate_version(config: &McpServerConfig) -> Option<&str> {
    highest_common(&config.protocol_versions, ADAPTER_PROTOCOL_VERSIONS)
}

/// The highest candidate version wins (`mcp.md` §3.2). Versions are date
/// strings, so lexicographic order is chronological.
fn highest_common<'a>(versions: &'a [String], supported: &[&str]) -> Option<&'a str> {
    versions
        .iter()
        .filter(|version| supported.contains(&version.as_str()))
        .max()
        .map(String::as_str)
}

fn validate_config(config: &McpServerConfig) -> Result<(), Failure> {
    let config_error = |message: &str| (LoadStage::Config, message.to_owned());
    validate_server_id(config.id.as_str()).map_err(|error| config_error(&error.to_string()))?;
    if config.protocol_versions.is_empty() {
        return Err(config_error("protocol_versions must not be empty"));
    }
    if config
        .timeouts
        .startup
        .is_some_and(|timeout| timeout.is_zero())
        || config
            .timeouts
            .call
            .is_some_and(|timeout| timeout.is_zero())
    {
        return Err(config_error("timeouts must be positive"));
    }
    Ok(())
}

fn duplicate_ids(configs: &[McpServerConfig]) -> BTreeSet<&str> {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for config in configs {
        *counts.entry(config.id.as_str()).or_default() += 1;
    }
    counts
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .map(|(id, _)| id)
        .collect()
}

fn compose_names(
    server_id: &str,
    tools: Vec<AdapterTool>,
) -> Result<Vec<(ToolName, AdapterTool)>, String> {
    let mut composed = Vec::with_capacity(tools.len());
    let mut seen: BTreeSet<ToolName> = BTreeSet::new();
    for tool in tools {
        let name = external_tool_name(server_id, &tool.original_name)
            .map_err(|error| error.to_string())?;
        if !seen.insert(name.clone()) {
            return Err(format!(
                "tools of server {server_id} collide on {}",
                name.as_str()
            ));
        }
        composed.push((name, tool));
    }
    Ok(composed)
}

struct ExternalTool {
    descriptor: ToolDescriptor,
    adapter: Arc<dyn McpAdapter>,
    original_name: String,
}

impl Tool for ExternalTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        args: JsonString,
        ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async move { self.adapter.call(&self.original_name, args, &ctx).await })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use asterpath_protocol::{Parallelism, ReplaySafety, SideEffect};
    use serde_json::json;

    use super::*;
    use crate::config::{McpServerConfig, McpTimeouts, McpTransport, ServerId};

    fn stdio_transport() -> McpTransport {
        McpTransport::Stdio {
            command: "server".to_owned(),
            args: Vec::new(),
            env: BTreeMap::new(),
            cwd: None,
        }
    }

    fn config(transport: McpTransport) -> McpServerConfig {
        McpServerConfig {
            id: ServerId::new("server").unwrap(),
            required: false,
            protocol_versions: vec!["2026-07-28".to_owned()],
            transport,
            timeouts: McpTimeouts::default(),
        }
    }

    fn tool(name: &str) -> AdapterTool {
        AdapterTool {
            original_name: name.to_owned(),
            description: String::new(),
            schema: json!({"type": "object"}),
            replay: ReplaySafety::Safe,
            side_effect: SideEffect::ReadOnly,
            parallelism: Parallelism::Parallel,
        }
    }

    #[test]
    fn p2_rejects_collisions_oversize_and_empty_sanitized_names() {
        let collision = compose_names("server", vec![tool("a.b"), tool("a_b")]);
        assert!(collision.is_err(), "{collision:?}");

        let oversize = compose_names("server", vec![tool(&"x".repeat(70))]);
        assert!(oversize.is_err(), "{oversize:?}");

        let empty = compose_names("server", vec![tool("")]);
        assert!(empty.is_err(), "{empty:?}");
    }

    #[test]
    fn composes_names_for_every_discovered_tool() {
        let names = compose_names("server", vec![tool("echo"), tool("a.b")]).unwrap();
        assert_eq!(names[0].0, ToolName::new("server__echo"));
        assert_eq!(names[1].0, ToolName::new("server__a_b"));
    }

    #[test]
    fn candidate_version_picks_the_highest_supported_entry() {
        let supported = ["2025-11-25", "2026-07-28"];
        assert_eq!(
            highest_common(
                &["2026-07-28".to_owned(), "2025-11-25".to_owned()],
                &supported
            ),
            Some("2026-07-28")
        );
        assert_eq!(
            highest_common(&["2025-11-25".to_owned()], &supported),
            Some("2025-11-25")
        );
        assert_eq!(highest_common(&["2024-01-01".to_owned()], &supported), None);
    }

    #[test]
    fn config_validation_covers_every_rule() {
        let mut versions = config(stdio_transport());
        versions.protocol_versions.clear();
        assert_eq!(validate_config(&versions).unwrap_err().0, LoadStage::Config);

        let mut timeouts = config(stdio_transport());
        timeouts.timeouts.call = Some(Duration::ZERO);
        assert!(validate_config(&timeouts).is_err());

        assert!(validate_config(&config(stdio_transport())).is_ok());
    }

    #[test]
    fn duplicate_ids_reports_every_shared_id() {
        let mut second = config(stdio_transport());
        second.id = ServerId::new("other").unwrap();
        let third = config(stdio_transport());
        let configs = [config(stdio_transport()), second, third];
        let duplicates = duplicate_ids(&configs);
        assert_eq!(duplicates.len(), 1);
        assert!(duplicates.contains("server"));
    }

    #[test]
    fn http_transport_is_a_valid_config() {
        let transport = McpTransport::Http {
            url: "https://example.test/mcp".to_owned(),
            headers: BTreeMap::new(),
        };
        assert!(validate_config(&config(transport)).is_ok());
    }
}
