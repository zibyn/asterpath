//! Loading pipeline: manifest validation, spawn, handshake, discovery, naming
//! and atomic registration (`plugins.md` §5, §9).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ffi::{OsStr, OsString};
use std::sync::Arc;

use asterpath_protocol::{
    JsonString, Parallelism, Provenance, Tool, ToolContext, ToolDescriptor, ToolError, ToolName,
    ToolOutput, ToolRegistry, TransportKind, external_tool_name, validate_plugin_id,
};
use asterpath_sandbox::{ConfinedSpawn, Enforcement, SandboxPolicy, SandboxProvider, SpawnRequest};

use crate::adapter::{AdapterError, AdapterTool, BoxFuture, PluginAdapter};
use crate::http::HttpAdapter;
use crate::manifest::{
    PluginCapability, PluginId, PluginManifest, PluginProtocol, PluginTransport,
};
use crate::mcp::ADAPTER_PROTOCOL_VERSIONS;
use crate::stdio::StdioAdapter;

const CANCEL_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

/// One plugin's load result. The framework never produces `Skipped`.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PluginStatus {
    Loaded {
        tools: Vec<ToolName>,
    },
    Failed {
        stage: PluginLoadStage,
        message: String,
    },
    Skipped {
        reason: String,
    },
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PluginLoadStage {
    Manifest,
    Spawn,
    Handshake,
    Discovery,
    Registration,
}

/// A required plugin failed; the host should abort startup.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("plugin {plugin} failed during {stage:?}: {message}")]
pub struct PluginHostError {
    pub plugin: PluginId,
    pub stage: PluginLoadStage,
    pub message: String,
}

/// Loads external plugins into a `ToolRegistry`.
pub struct PluginHost {
    registry: Arc<dyn ToolRegistry>,
    sandbox: Option<Arc<dyn SandboxProvider>>,
    policy: SandboxPolicy,
}

impl PluginHost {
    pub fn new(
        registry: Arc<dyn ToolRegistry>,
        sandbox: Option<Arc<dyn SandboxProvider>>,
        policy: SandboxPolicy,
    ) -> Self {
        Self {
            registry,
            sandbox,
            policy,
        }
    }

    /// Loads every manifest, returning one status per manifest in input order.
    ///
    /// All manifests are validated before any process is spawned. Processing
    /// then stops at the first `required` failure and returns an error.
    pub async fn load(
        &self,
        manifests: Vec<PluginManifest>,
    ) -> Result<Vec<PluginStatus>, PluginHostError> {
        let duplicates = duplicate_ids(&manifests);
        let mut statuses: Vec<Option<PluginStatus>> = vec![None; manifests.len()];

        for (index, manifest) in manifests.iter().enumerate() {
            let failure = if duplicates.contains(manifest.id.as_str()) {
                Some((
                    PluginLoadStage::Manifest,
                    "duplicate plugin id in the load set".to_owned(),
                ))
            } else if let Err(failure) = validate_manifest(manifest) {
                Some(failure)
            } else if candidate_version(manifest).is_none() {
                Some((
                    PluginLoadStage::Handshake,
                    "no common protocol version".to_owned(),
                ))
            } else {
                None
            };
            if let Some((stage, message)) = failure {
                if manifest.required {
                    return Err(PluginHostError {
                        plugin: manifest.id.clone(),
                        stage,
                        message,
                    });
                }
                statuses[index] = Some(PluginStatus::Failed { stage, message });
            }
        }

        for (index, manifest) in manifests.into_iter().enumerate() {
            if statuses[index].is_some() {
                continue;
            }
            let status = self.load_one(&manifest).await;
            if let PluginStatus::Failed { stage, message } = &status
                && manifest.required
            {
                return Err(PluginHostError {
                    plugin: manifest.id.clone(),
                    stage: *stage,
                    message: message.clone(),
                });
            }
            statuses[index] = Some(status);
        }

        Ok(statuses
            .into_iter()
            .map(|status| status.expect("every manifest has a status"))
            .collect())
    }

    async fn load_one(&self, manifest: &PluginManifest) -> PluginStatus {
        let candidate = candidate_version(manifest).expect("checked during manifest validation");
        match &manifest.transport {
            PluginTransport::Http { url, headers } => {
                let adapter = match HttpAdapter::new(
                    url,
                    headers,
                    candidate,
                    manifest.timeouts.startup_timeout(),
                ) {
                    Ok(adapter) => adapter,
                    Err(error) => return failed(PluginLoadStage::Spawn, adapter_message(error)),
                };
                self.finish_load(manifest, Box::new(adapter), TransportKind::Http)
                    .await
            }
            PluginTransport::Stdio {
                command,
                args,
                env,
                cwd,
            } => {
                self.load_stdio(manifest, candidate, command, args, env, cwd)
                    .await
            }
        }
    }

    async fn load_stdio(
        &self,
        manifest: &PluginManifest,
        candidate: &str,
        command: &str,
        args: &[String],
        env: &BTreeMap<String, String>,
        cwd: &Option<std::path::PathBuf>,
    ) -> PluginStatus {
        let restricted = !matches!(self.policy, SandboxPolicy::FullAccess);
        if restricted && self.sandbox.is_none() {
            return failed(
                PluginLoadStage::Spawn,
                "a restricted sandbox policy requires a SandboxProvider",
            );
        }

        let mut spawn_env: BTreeMap<OsString, OsString> = std::env::vars_os().collect();
        for (key, value) in env {
            spawn_env.insert(OsString::from(key), OsString::from(value));
        }
        let program = OsStr::new(command);
        let args: Vec<OsString> = args.iter().map(OsString::from).collect();
        let cwd = cwd.as_deref();
        let request = SpawnRequest::new(program, &args, cwd, &spawn_env, &self.policy);
        let confined = match &self.sandbox {
            Some(provider) => match provider.confine(&request) {
                Ok(confined) => confined,
                Err(error) => return failed(PluginLoadStage::Spawn, error.to_string()),
            },
            None => ConfinedSpawn::new(
                program.to_os_string(),
                args.clone(),
                cwd.map(std::path::Path::to_path_buf),
                spawn_env.clone(),
                Enforcement::Full,
            ),
        };

        let adapter = match StdioAdapter::spawn(
            &confined,
            candidate,
            manifest.timeouts.startup_timeout(),
            CANCEL_GRACE,
        ) {
            Ok(adapter) => adapter,
            Err(error) => {
                return failed(
                    PluginLoadStage::Spawn,
                    format!("failed to start plugin process: {error}"),
                );
            }
        };
        self.finish_load(manifest, Box::new(adapter), TransportKind::Stdio)
            .await
    }

    /// Handshake, discovery, naming and atomic registration; shared by both
    /// transports.
    async fn finish_load(
        &self,
        manifest: &PluginManifest,
        mut adapter: Box<dyn PluginAdapter>,
        transport: TransportKind,
    ) -> PluginStatus {
        let handshake = match adapter.handshake().await {
            Ok(handshake) => handshake,
            Err(error) => {
                adapter.shutdown().await;
                return failed(PluginLoadStage::Handshake, adapter_message(error));
            }
        };
        if !manifest
            .protocol_versions
            .iter()
            .any(|version| version == &handshake.protocol_version)
        {
            adapter.shutdown().await;
            return failed(
                PluginLoadStage::Handshake,
                format!(
                    "negotiated protocol version {} is not in protocol_versions",
                    handshake.protocol_version
                ),
            );
        }

        let discovered = match adapter.discover().await {
            Ok(tools) => tools,
            Err(error) => {
                adapter.shutdown().await;
                return failed(PluginLoadStage::Discovery, adapter_message(error));
            }
        };

        let composed = match compose_names(manifest.id.as_str(), discovered) {
            Ok(composed) => composed,
            Err(message) => {
                adapter.shutdown().await;
                return failed(PluginLoadStage::Registration, message);
            }
        };
        for (name, _) in &composed {
            if self.registry.get(name).is_some() {
                adapter.shutdown().await;
                return failed(
                    PluginLoadStage::Registration,
                    format!("tool {} is already registered", name.as_str()),
                );
            }
        }

        let adapter: Arc<dyn PluginAdapter> = Arc::from(adapter);
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
                timeout: Some(manifest.timeouts.call_timeout()),
                max_output_bytes: None,
            };
            self.registry.register(Arc::new(ExternalTool {
                descriptor,
                adapter: Arc::clone(&adapter),
                original_name: tool.original_name.clone(),
            }));
            names.push(name);
        }
        PluginStatus::Loaded { tools: names }
    }
}

fn failed(stage: PluginLoadStage, message: impl Into<String>) -> PluginStatus {
    PluginStatus::Failed {
        stage,
        message: message.into(),
    }
}

fn adapter_message(error: AdapterError) -> String {
    match error {
        AdapterError::Spawn { message }
        | AdapterError::Handshake { message }
        | AdapterError::Discovery { message }
        | AdapterError::Transport { message } => message,
    }
}

fn candidate_version(manifest: &PluginManifest) -> Option<&str> {
    manifest
        .protocol_versions
        .iter()
        .find(|version| ADAPTER_PROTOCOL_VERSIONS.contains(&version.as_str()))
        .map(String::as_str)
}

fn validate_manifest(manifest: &PluginManifest) -> Result<(), (PluginLoadStage, String)> {
    let manifest_error = |message: &str| (PluginLoadStage::Manifest, message.to_owned());
    if manifest.manifest_version != 1 {
        return Err(manifest_error("manifest_version must be 1"));
    }
    validate_plugin_id(manifest.id.as_str()).map_err(|error| manifest_error(&error.to_string()))?;
    if manifest.protocol_versions.is_empty() {
        return Err(manifest_error("protocol_versions must not be empty"));
    }
    if manifest.protocol == PluginProtocol::Acp {
        return Err(manifest_error("ACP is not supported"));
    }
    if manifest.capabilities.is_empty()
        || !manifest
            .capabilities
            .iter()
            .all(|capability| matches!(capability, PluginCapability::Tools))
    {
        return Err(manifest_error("only the tools capability is supported"));
    }
    if manifest
        .timeouts
        .startup
        .is_some_and(|timeout| timeout.is_zero())
        || manifest
            .timeouts
            .call
            .is_some_and(|timeout| timeout.is_zero())
    {
        return Err(manifest_error("timeouts must be positive"));
    }
    Ok(())
}

fn duplicate_ids(manifests: &[PluginManifest]) -> BTreeSet<&str> {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for manifest in manifests {
        *counts.entry(manifest.id.as_str()).or_default() += 1;
    }
    counts
        .into_iter()
        .filter(|(_, count)| *count > 1)
        .map(|(id, _)| id)
        .collect()
}

fn compose_names(
    plugin_id: &str,
    tools: Vec<AdapterTool>,
) -> Result<Vec<(ToolName, AdapterTool)>, String> {
    let mut composed = Vec::with_capacity(tools.len());
    let mut seen: BTreeSet<ToolName> = BTreeSet::new();
    for tool in tools {
        let name = external_tool_name(plugin_id, &tool.original_name)
            .map_err(|error| error.to_string())?;
        if !seen.insert(name.clone()) {
            return Err(format!(
                "tools of plugin {plugin_id} collide on {}",
                name.as_str()
            ));
        }
        composed.push((name, tool));
    }
    Ok(composed)
}

struct ExternalTool {
    descriptor: ToolDescriptor,
    adapter: Arc<dyn PluginAdapter>,
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
    use crate::manifest::{
        PluginCapability, PluginId, PluginManifest, PluginProtocol, PluginTimeouts, PluginTransport,
    };

    fn stdio_transport() -> PluginTransport {
        PluginTransport::Stdio {
            command: "server".to_owned(),
            args: Vec::new(),
            env: BTreeMap::new(),
            cwd: None,
        }
    }

    fn manifest(protocol: PluginProtocol, transport: PluginTransport) -> PluginManifest {
        PluginManifest {
            manifest_version: 1,
            id: PluginId::new("plugin").unwrap(),
            name: None,
            version: None,
            required: false,
            protocol,
            protocol_versions: vec!["2026-07-28".to_owned()],
            transport,
            capabilities: vec![PluginCapability::Tools],
            timeouts: PluginTimeouts::default(),
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
        let collision = compose_names("plugin", vec![tool("a.b"), tool("a_b")]);
        assert!(collision.is_err(), "{collision:?}");

        let oversize = compose_names("plugin", vec![tool(&"x".repeat(70))]);
        assert!(oversize.is_err(), "{oversize:?}");

        let empty = compose_names("plugin", vec![tool("")]);
        assert!(empty.is_err(), "{empty:?}");
    }

    #[test]
    fn composes_names_for_every_discovered_tool() {
        let names = compose_names("plugin", vec![tool("echo"), tool("a.b")]).unwrap();
        assert_eq!(names[0].0, ToolName::new("plugin__echo"));
        assert_eq!(names[1].0, ToolName::new("plugin__a_b"));
    }

    #[test]
    fn manifest_validation_covers_every_rule() {
        let mut version = manifest(PluginProtocol::Mcp, stdio_transport());
        version.manifest_version = 2;
        assert_eq!(
            validate_manifest(&version).unwrap_err().0,
            PluginLoadStage::Manifest
        );

        let acp = manifest(PluginProtocol::Acp, stdio_transport());
        assert_eq!(
            validate_manifest(&acp).unwrap_err().0,
            PluginLoadStage::Manifest
        );

        let mut versions = manifest(PluginProtocol::Mcp, stdio_transport());
        versions.protocol_versions.clear();
        assert!(validate_manifest(&versions).is_err());

        let mut capabilities = manifest(PluginProtocol::Mcp, stdio_transport());
        capabilities.capabilities.clear();
        assert!(validate_manifest(&capabilities).is_err());

        let mut timeouts = manifest(PluginProtocol::Mcp, stdio_transport());
        timeouts.timeouts.call = Some(Duration::ZERO);
        assert!(validate_manifest(&timeouts).is_err());

        assert!(validate_manifest(&manifest(PluginProtocol::Mcp, stdio_transport())).is_ok());
    }

    #[test]
    fn duplicate_ids_reports_every_shared_id() {
        let mut second = manifest(PluginProtocol::Mcp, stdio_transport());
        second.id = PluginId::new("other").unwrap();
        let third = manifest(PluginProtocol::Mcp, stdio_transport());
        let manifests = [
            manifest(PluginProtocol::Mcp, stdio_transport()),
            second,
            third,
        ];
        let duplicates = duplicate_ids(&manifests);
        assert_eq!(duplicates.len(), 1);
        assert!(duplicates.contains("plugin"));
    }

    #[test]
    fn http_transport_is_a_valid_manifest() {
        let transport = PluginTransport::Http {
            url: "https://example.test/mcp".to_owned(),
            headers: BTreeMap::new(),
        };
        let manifest = manifest(PluginProtocol::Mcp, transport);
        assert!(validate_manifest(&manifest).is_ok());
    }
}
