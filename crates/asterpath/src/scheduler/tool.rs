//! The model-visible `spawn_agent` tool: a thin wrapper over spawn + wait.

use std::time::Duration;

use asterpath_protocol::ExecutionError;
use asterpath_protocol::{
    ChildOutcome, ModelAlias, Parallelism, ParentRef, Part, Provenance, ReplaySafety, SideEffect,
    Tool, ToolArgs, ToolContext, ToolDescriptor, ToolError, ToolName, ToolOutput,
};
use futures_core::future::BoxFuture;

use super::{Scheduler, SpawnError, SpawnRequest, WaitError};
use crate::agent::Agent;

pub(crate) const SPAWN_TOOL_NAME: &str = "spawn_agent";

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SpawnArgs {
    task: String,
    #[serde(default)]
    model_alias: Option<String>,
    #[serde(default)]
    tool_allowlist: Option<Vec<String>>,
    /// Per-call spawn deadline in milliseconds; overrides the config default.
    #[serde(default)]
    timeout_ms: Option<u64>,
}

/// `spawn_agent` in the tool table. Its calls never reach [`Tool::invoke`]:
/// the agent routes them to [`Scheduler::run_spawn_tool`] with itself as the
/// child driver.
pub(crate) struct SpawnTool {
    descriptor: ToolDescriptor,
}

impl SpawnTool {
    pub(crate) fn new() -> Self {
        Self {
            descriptor: descriptor(),
        }
    }
}

impl Tool for SpawnTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        _args: ToolArgs,
        _ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async {
            Err(ToolError::Internal {
                message: "spawn_agent runs only inside the agent".to_owned(),
            })
        })
    }
}

impl Scheduler {
    /// One `spawn_agent` call: spawn + wait, with `agent` driving the child.
    pub(crate) fn run_spawn_tool<'a>(
        &'a self,
        agent: &'a Agent,
        args: ToolArgs,
        ctx: ToolContext,
    ) -> BoxFuture<'a, Result<ToolOutput, ToolError>> {
        Box::pin(async move {
            let args: SpawnArgs =
                serde_json::from_str(args.as_str()).map_err(|error| ToolError::InvalidArgs {
                    message: format!("invalid spawn arguments: {error}"),
                })?;
            if args.task.trim().is_empty() {
                return Err(ToolError::InvalidArgs {
                    message: "task must not be empty".to_owned(),
                });
            }
            if args.timeout_ms == Some(0) {
                return Err(ToolError::InvalidArgs {
                    message: "timeout_ms must be greater than zero".to_owned(),
                });
            }

            let caused_by = self
                .caused_by(ctx.session, ctx.tool_call_id)
                .await
                .map_err(infra)?
                .ok_or_else(|| ToolError::Internal {
                    message: "spawn tool call has no ToolCallRequested record".to_owned(),
                })?;
            let mut request = SpawnRequest::new(
                ParentRef {
                    session: ctx.session,
                    turn: ctx.turn,
                    tool_call: ctx.tool_call_id,
                },
                caused_by,
                vec![Part::Text { text: args.task }],
            );
            request.model_alias = args.model_alias.map(ModelAlias::new);
            request.tool_allowlist = args
                .tool_allowlist
                .map(|names| names.into_iter().map(ToolName::new).collect());
            request.timeout = args.timeout_ms.map(Duration::from_millis);

            // Queued or admitted, the spawn observes the turn's token. A
            // queued entry is dropped (no child); an admitted child is
            // cancelled and closed, then the future parks until the pipeline's
            // grace writes `Interrupted`.
            let mut spawning = self.spawn(request.clone());
            let child = tokio::select! {
                biased;
                _ = ctx.cancel.cancelled() => {
                    drop(spawning);
                    if let Some(child) = self
                        .spawned_child(request.parent.clone())
                        .await
                        .map_err(infra)?
                    {
                        self.cancel_wait(child, agent).await.map_err(wait_error)?;
                    }
                    return std::future::pending().await;
                }
                result = &mut spawning => match result {
                    Ok(child) => child,
                    // Depth and argument errors mean no child exists and the
                    // model must fix its call.
                    Err(SpawnError::DepthExceeded) | Err(SpawnError::InvalidArgs { .. }) => {
                        return Err(ToolError::InvalidArgs {
                            message: "sub-agent spawn rejected: depth or arguments".to_owned(),
                        });
                    }
                    Err(SpawnError::Execution(error)) => {
                        return Err(infra(error));
                    }
                },
            };

            let mut waiting = self.await_completion(&child, agent);
            let result = tokio::select! {
                biased;
                _ = ctx.cancel.cancelled() => {
                    drop(waiting);
                    self.cancel_wait(child, agent).await.map_err(wait_error)?;
                    return std::future::pending().await;
                }
                result = &mut waiting => result,
            };

            match result {
                Ok(ChildOutcome::Completed { summary }) => Ok(ToolOutput::new(summary.items)),
                Ok(ChildOutcome::Failed { reason, summary }) => {
                    let mut parts = vec![Part::Text {
                        text: format!("sub-agent task failed: {reason:?}"),
                    }];
                    parts.extend(summary.items);
                    Ok(ToolOutput::new(parts))
                }
                Ok(ChildOutcome::Cancelled { .. }) => Ok(ToolOutput::new(vec![Part::Text {
                    text: "sub-agent task was cancelled".to_owned(),
                }])),
                Ok(ChildOutcome::Lost { reason }) => Err(ToolError::Internal {
                    message: format!("sub-agent state is unreadable: {reason}"),
                }),
                Ok(_) => Err(ToolError::Internal {
                    message: "unknown sub-agent outcome".to_owned(),
                }),
                Err(WaitError::Cancelled) => Err(ToolError::Cancelled {
                    message: "sub-agent wait cancelled".to_owned(),
                }),
                Err(WaitError::Timeout) => Err(ToolError::Timeout {
                    message: "sub-agent wait timed out".to_owned(),
                }),
                Err(WaitError::Execution(error)) => Err(infra(error)),
            }
        })
    }
}

fn infra(error: ExecutionError) -> ToolError {
    ToolError::Internal {
        message: error.to_string(),
    }
}

fn wait_error(error: WaitError) -> ToolError {
    match error {
        WaitError::Cancelled => ToolError::Cancelled {
            message: "sub-agent wait cancelled".to_owned(),
        },
        WaitError::Timeout => ToolError::Timeout {
            message: "sub-agent wait timed out".to_owned(),
        },
        WaitError::Execution(error) => infra(error),
    }
}

fn descriptor() -> ToolDescriptor {
    ToolDescriptor {
        name: ToolName::new(SPAWN_TOOL_NAME),
        description: "Run a sub-agent: starts an independent child session with a fresh \
                      context and returns its status and task summary once it finishes. \
                      The child inherits the session configuration; the model may only \
                      narrow the model alias and the tool set."
            .to_owned(),
        schema: serde_json::json!({
            "type": "object",
            "properties": {
                "task": {
                    "type": "string",
                    "description": "The task for the sub-agent. End with what to report back."
                },
                "model_alias": {
                    "type": "string",
                    "description": "Optional host-defined alias for a model on the same provider."
                },
                "tool_allowlist": {
                    "type": "array",
                    "items": { "type": "string" },
                    "description": "Optional subset of the parent's tools."
                },
                "timeout_ms": {
                    "type": "integer",
                    "minimum": 1,
                    "description": "Optional deadline for the child in milliseconds."
                }
            },
            "required": ["task"],
            "additionalProperties": false
        }),
        parallelism: Parallelism::Parallel,
        replay: ReplaySafety::Safe,
        side_effect: SideEffect::Mutating,
        provenance: Provenance::InProcess,
        timeout: None,
        max_output_bytes: None,
    }
}
