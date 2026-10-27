//! The approval mode (ADR-0055), shared by the CLI and the desktop window:
//! spawned commands are bounded by the sandbox, so approval only decides
//! what may run outside it; the mode also caps the sandbox, read-only
//! tightening `bash` to `ReadOnly`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use asterpath_protocol::{
    ApprovalPolicy, ApprovalRequest, ApprovalVerdict, EnvironmentAdapter, EnvironmentError,
    Provenance, SandboxPolicy, SessionEnvironment, SessionId, SideEffect, Tool, ToolArgs,
    ToolContext, ToolDescriptor, ToolError, ToolName, ToolOutput,
};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::config;
use crate::workspace::{SandboxMode, Workspace};

pub mod rules;

use rules::{CommandRules, RuleKind};

/// The one in-process tool that spawns a process.
const BASH: &str = "bash";

/// A session tree's approval mode (GLOSSARY「审批模式」): what a call
/// outside the sandbox gets, and a cap on the sandbox.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalMode {
    /// Read-only: `bash` runs in a `ReadOnly` sandbox; calls outside the
    /// sandbox are denied; MCP tools with `readOnlyHint` ask.
    Deny,
    /// Calls outside the sandbox and MCP tools ask (`config.toml`'s `untrusted`).
    #[default]
    Ask,
    /// Nothing asks (`never`).
    Allow,
}

impl From<config::ApprovalMode> for ApprovalMode {
    fn from(mode: config::ApprovalMode) -> Self {
        match mode {
            config::ApprovalMode::Untrusted => Self::Ask,
            config::ApprovalMode::Never => Self::Allow,
            config::ApprovalMode::ReadOnly => Self::Deny,
        }
    }
}

#[derive(Default)]
struct TreeApprovals {
    mode: Option<ApprovalMode>,
    allowed: BTreeSet<ToolName>,
}

/// The application's approval policy: each session tree's mode and the
/// tools allowed for it, keyed by the root so subagents follow their tree;
/// in memory only (ADR-0035), so a restart asks again.
pub struct ApprovalModes {
    default: ApprovalMode,
    /// A usable sandbox backend.
    backend: bool,
    /// The configured sandbox is `full-access`.
    full_access: bool,
    rules: CommandRules,
    trees: Mutex<HashMap<SessionId, TreeApprovals>>,
}

impl ApprovalModes {
    /// Every call counts as outside the sandbox until [`Self::with_sandbox`].
    pub fn new(default: ApprovalMode) -> Self {
        Self {
            default,
            backend: false,
            full_access: true,
            rules: CommandRules::default(),
            trees: Mutex::default(),
        }
    }

    /// The sandbox `workspace` gives `bash`.
    #[must_use]
    pub fn with_sandbox(mut self, workspace: &Workspace) -> Self {
        self.backend = workspace.sandbox_backend.unavailable().is_none();
        self.full_access = workspace.sandbox == SandboxMode::FullAccess;
        self
    }

    /// The command rules of `[approval]`; only the builtin ones until then.
    #[must_use]
    pub fn with_rules(mut self, rules: CommandRules) -> Self {
        self.rules = rules;
        self
    }

    /// The mode of the tree rooted at `root`.
    pub fn mode(&self, root: SessionId) -> ApprovalMode {
        lock(&self.trees)
            .get(&root)
            .and_then(|tree| tree.mode)
            .unwrap_or(self.default)
    }

    /// Takes effect at once, also for a call already approved.
    pub fn set_mode(&self, root: SessionId, mode: ApprovalMode) {
        lock(&self.trees).entry(root).or_default().mode = Some(mode);
    }

    /// Stop asking for `tool` anywhere in the tree rooted at `root`.
    pub fn allow(&self, root: SessionId, tool: ToolName) {
        lock(&self.trees)
            .entry(root)
            .or_default()
            .allowed
            .insert(tool);
    }

    /// `inner` with its `bash` started under the tree's mode: `ReadOnly` in
    /// a read-only tree, else the session's policy.
    pub fn environment(
        self: &Arc<Self>,
        inner: Arc<dyn EnvironmentAdapter>,
    ) -> Arc<dyn EnvironmentAdapter> {
        Arc::new(Capped {
            inner,
            modes: self.clone(),
        })
    }

    /// Whether `bash` runs in a restricted sandbox under `mode`.
    fn sandboxed(&self, mode: ApprovalMode) -> bool {
        self.backend && (mode == ApprovalMode::Deny || !self.full_access)
    }
}

impl ApprovalPolicy for ApprovalModes {
    fn classify(&self, request: &ApprovalRequest) -> ApprovalVerdict {
        let mode = self.mode(request.root);
        let rule = self
            .rules
            .matches(request.tool.as_str(), request.args.as_str());
        match verdict(mode, self.sandboxed(mode), rule, request) {
            // An ask rule outranks what the session allowed.
            ApprovalVerdict::Ask
                if rule != Some(RuleKind::Ask)
                    && lock(&self.trees)
                        .get(&request.root)
                        .is_some_and(|tree| tree.allowed.contains(&request.tool)) =>
            {
                ApprovalVerdict::Allow
            }
            verdict => verdict,
        }
    }
}

/// ADR-0055's table, then the matched command rule: deny wins, ask turns an
/// allow into ask, allow turns only ask into allow; no rule loosens a deny.
fn verdict(
    mode: ApprovalMode,
    sandboxed: bool,
    rule: Option<RuleKind>,
    request: &ApprovalRequest,
) -> ApprovalVerdict {
    match (rule, table(mode, sandboxed, request)) {
        (Some(RuleKind::Deny), _) => ApprovalVerdict::Deny {
            reason: Some("denied by a command rule".to_owned()),
        },
        (Some(RuleKind::Ask), ApprovalVerdict::Allow) => ApprovalVerdict::Ask,
        (Some(RuleKind::Allow), ApprovalVerdict::Ask) => ApprovalVerdict::Allow,
        (_, verdict) => verdict,
    }
}

/// ADR-0055's table; `sandboxed` says whether `bash` runs in a restricted sandbox.
fn table(mode: ApprovalMode, sandboxed: bool, request: &ApprovalRequest) -> ApprovalVerdict {
    let read_only = |reason: &str| ApprovalVerdict::Deny {
        reason: Some(reason.to_owned()),
    };
    let outside = match mode {
        ApprovalMode::Deny => read_only("the session is read-only"),
        ApprovalMode::Ask => ApprovalVerdict::Ask,
        ApprovalMode::Allow => ApprovalVerdict::Allow,
    };
    match (request.provenance, request.side_effect) {
        (Provenance::InProcess, _) if request.tool.as_str() == BASH => match (sandboxed, mode) {
            (true, _) => ApprovalVerdict::Allow,
            (false, ApprovalMode::Deny) => {
                read_only("the session is read-only and there is no sandbox to run commands in")
            }
            (false, _) => outside,
        },
        (Provenance::InProcess, SideEffect::ReadOnly) => ApprovalVerdict::Allow,
        (Provenance::InProcess, SideEffect::Mutating) => outside,
        // MCP servers run under the configured sandbox; the mode cannot tighten them.
        (Provenance::External { .. }, SideEffect::ReadOnly) if mode == ApprovalMode::Deny => {
            ApprovalVerdict::Ask
        }
        (Provenance::External { .. }, _) => outside,
        // The pipeline writes `NotFound` before approval for these.
        (Provenance::Unknown | Provenance::Output, _) => ApprovalVerdict::Deny { reason: None },
    }
}

struct Capped {
    inner: Arc<dyn EnvironmentAdapter>,
    modes: Arc<ApprovalModes>,
}

impl EnvironmentAdapter for Capped {
    fn resolve<'a>(
        &'a self,
        metadata: &'a BTreeMap<String, String>,
    ) -> BoxFuture<'a, Result<SessionEnvironment, EnvironmentError>> {
        Box::pin(async move {
            let mut environment = self.inner.resolve(metadata).await?;
            for tool in &mut environment.tools {
                let descriptor = tool.descriptor();
                if descriptor.name.as_str() == BASH
                    && descriptor.provenance == Provenance::InProcess
                {
                    *tool = Arc::new(CappedBash {
                        inner: tool.clone(),
                        modes: self.modes.clone(),
                    });
                }
            }
            Ok(environment)
        })
    }
}

struct CappedBash {
    inner: Arc<dyn Tool>,
    modes: Arc<ApprovalModes>,
}

impl Tool for CappedBash {
    fn descriptor(&self) -> &ToolDescriptor {
        self.inner.descriptor()
    }

    fn invoke(
        &self,
        args: ToolArgs,
        mut ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        // Read at spawn time, so a switch to read-only reaches approved calls;
        // without a backend `ReadOnly` fails closed.
        if self.modes.mode(ctx.root) == ApprovalMode::Deny {
            ctx.spawn.sandbox = Some(SandboxPolicy::ReadOnly);
        }
        self.inner.invoke(args, ctx)
    }
}

fn lock<T>(map: &Mutex<T>) -> MutexGuard<'_, T> {
    map.lock().unwrap_or_else(PoisonError::into_inner)
}
