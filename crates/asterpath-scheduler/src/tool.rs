//! The model-visible `spawn_agent` tool: a thin wrapper over spawn + wait.

use std::sync::Arc;

use asterpath_core::ExecutionError;
use asterpath_protocol::{
    ChildOutcome, ModelAlias, Parallelism, ParentRef, Part, Provenance, ReplaySafety, SideEffect,
    Tool, ToolArgs, ToolContext, ToolDescriptor, ToolError, ToolName, ToolOutput,
};
use futures_core::future::BoxFuture;

use crate::{Scheduler, SpawnError, SpawnRequest, SubagentSpawner, WaitError};

pub const SPAWN_TOOL_NAME: &str = "spawn_agent";

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SpawnArgs {
    task: String,
    #[serde(default)]
    model_alias: Option<String>,
    #[serde(default)]
    tool_allowlist: Option<Vec<String>>,
}

pub struct SpawnTool {
    scheduler: Arc<Scheduler>,
    descriptor: ToolDescriptor,
}

impl SpawnTool {
    pub fn new(scheduler: Arc<Scheduler>) -> Self {
        Self {
            scheduler,
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
        args: ToolArgs,
        ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
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

            let caused_by = self
                .scheduler
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

            let child = match self.scheduler.spawn(request).await {
                Ok(child) => child,
                // Depth and argument errors mean no child exists and the model
                // must fix its call.
                Err(SpawnError::DepthExceeded) | Err(SpawnError::InvalidArgs { .. }) => {
                    return Err(ToolError::InvalidArgs {
                        message: "sub-agent spawn rejected: depth or arguments".to_owned(),
                    });
                }
                Err(SpawnError::Execution(error)) => {
                    return Err(infra(error));
                }
            };

            match self.scheduler.await_completion(&child).await {
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
                // #48 owns the persistent spawn timeout and its Failed{Timeout}
                // mapping; v1 has no timeout timer yet.
                Err(WaitError::Timeout) => Err(ToolError::Internal {
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
