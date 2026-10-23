//! `CompactionStarted.turn` (#74): in-turn compaction records its turn, idle
//! compaction records none. Live usage observation is `Hooks::on_record`
//! (ADR-0025), covered by the facade tests.

use std::sync::Arc;

use super::support::{self, SESSION, append, drive, model, text, tool_table};
use crate::agent::Agent;
use asterpath_protocol::{
    CompactPlan, CompactSummary, CompactionError, CompactionJob, CompactionOutcome, ConfigSnapshot,
    InputTokens, Item, ModelSpec, OutputTokens, Role, SessionEvent, StopReason, Tool, ToolConfig,
    ToolDescriptor, Usage, decode_entry, dedup,
};
use asterpath_protocol::{LogId, SessionStore, StepId, TurnId};
use asterpath_testing::{InMemoryEventLog, ScriptedProvider};
use futures_core::future::BoxFuture;
use futures_util::StreamExt;

const TURN: TurnId = TurnId::from_bytes([1; 16]);
const STEP: StepId = StepId::from_bytes([2; 16]);

fn usage(input: u64, output: u64) -> Usage {
    Usage {
        input: InputTokens {
            uncached: input,
            cache_read: 0,
            cache_write: 0,
        },
        output: OutputTokens {
            total: output,
            reasoning: 0,
        },
    }
}

/// The shared config with a context window, so threshold compaction can run.
fn config(tools: Vec<ToolDescriptor>) -> ConfigSnapshot {
    ConfigSnapshot {
        model: ModelSpec {
            context_window: Some(200_000),
            ..model()
        },
        ..support::config(tools)
    }
}

fn agent(
    log: Arc<InMemoryEventLog>,
    provider: Arc<ScriptedProvider>,
    tools: Vec<Arc<dyn Tool>>,
    compactor: Option<Arc<dyn asterpath_protocol::Compactor>>,
) -> Agent {
    support::agent(
        log,
        provider,
        tool_table(tools),
        ToolConfig::default(),
        Vec::new(),
    )
    .with_compactor(compactor)
}

async fn seed(log: &Arc<InMemoryEventLog>, config: ConfigSnapshot) {
    append(
        log,
        SessionEvent::SessionCreated {
            config,
            parent: None,
        },
        Some(dedup::session_created(SESSION)),
    )
    .await;
}

fn assistant(text_value: &str) -> Item {
    Item::Message {
        role: Role::Assistant,
        parts: vec![text(text_value)],
    }
}

// --- tools and executor ---------------------------------------------------

struct AlwaysCompact;

impl asterpath_protocol::Compactor for AlwaysCompact {
    fn plan(&self, input: &asterpath_protocol::CompactionInput) -> CompactPlan {
        CompactPlan::Summarize(CompactionJob {
            first_kept: input
                .step_starts
                .first()
                .copied()
                .unwrap_or(LogId::from_i64(0)),
            replaces: asterpath_protocol::LogIdRange {
                first: LogId::from_i64(0),
                last: LogId::from_i64(0),
            },
            model: input.model.clone(),
            summary_input: Vec::new(),
            max_summary_tokens: 128,
            usable_window: 1000,
            retained_tokens: 0,
            turn: input.turn.clone(),
        })
    }

    fn compact<'a>(
        &'a self,
        job: CompactionJob,
    ) -> BoxFuture<'a, Result<CompactSummary, CompactionError>> {
        Box::pin(async move {
            Ok(CompactSummary::Applied(CompactionOutcome {
                first_kept: job.first_kept,
                summary: vec![text("summary")],
                replaces: job.replaces,
                model: job.model,
                usage: usage(50, 5),
                raw: None,
            }))
        })
    }
}

// --- harness --------------------------------------------------------------

async fn submit_input(log: &Arc<InMemoryEventLog>) {
    let input = asterpath_protocol::InputId::from_bytes([4; 16]);
    append(
        log,
        SessionEvent::InputRecorded {
            input: vec![text("go")],
        },
        Some(dedup::input_recorded(input)),
    )
    .await;
}

#[tokio::test]
async fn idle_compaction_intent_records_no_turn() {
    let log = Arc::new(InMemoryEventLog::new());
    seed(&log, config(Vec::new())).await;
    append(&log, SessionEvent::TurnStarted { turn_id: TURN }, None).await;
    append(
        &log,
        SessionEvent::StepStarted {
            turn_id: TURN,
            step_id: STEP,
        },
        None,
    )
    .await;
    append(
        &log,
        SessionEvent::AssistantMessage {
            turn_id: TURN,
            step_id: STEP,
            items: vec![assistant("done")],
            raw: None,
            model: model(),
            usage: usage(10, 2),
            stop_reason: StopReason::EndTurn,
        },
        Some(dedup::assistant_message(TURN, STEP)),
    )
    .await;
    append(
        &log,
        SessionEvent::StepCommitted {
            turn_id: TURN,
            step_id: STEP,
        },
        Some(dedup::step_committed(STEP)),
    )
    .await;
    append(
        &log,
        SessionEvent::TurnCommitted {
            turn_id: TURN,
            reason: asterpath_protocol::TurnEndReason::Completed,
        },
        Some(dedup::turn_committed(TURN)),
    )
    .await;

    let agent = agent(
        Arc::clone(&log),
        Arc::new(ScriptedProvider::new("p")),
        Vec::new(),
        Some(Arc::new(AlwaysCompact)),
    );

    agent.compact_now(SESSION).await.expect("compaction");

    assert_eq!(
        started_turns(&log).await,
        vec![None],
        "intent records no turn"
    );
}

/// `turn` of every `CompactionStarted`, in log order.
async fn started_turns(log: &Arc<InMemoryEventLog>) -> Vec<Option<TurnId>> {
    let mut entries = log.scan(asterpath_protocol::LogQuery {
        session: SESSION,
        after: None,
        limit: None,
    });
    let mut turns = Vec::new();
    while let Some(entry) = entries.next().await {
        if let SessionEvent::CompactionStarted { turn, .. } =
            decode_entry(&entry.expect("scan")).expect("decode")
        {
            turns.push(turn);
        }
    }
    turns
}

#[tokio::test]
async fn threshold_compaction_intent_records_its_turn() {
    let log = Arc::new(InMemoryEventLog::new());
    seed(&log, config(Vec::new())).await;
    submit_input(&log).await;
    let provider = Arc::new(ScriptedProvider::new("p").respond(
        vec![assistant("done")],
        StopReason::EndTurn,
        usage(10, 2),
    ));
    let agent = agent(
        Arc::clone(&log),
        provider,
        Vec::new(),
        Some(Arc::new(AlwaysCompact)),
    );

    drive(&agent).await;

    let mut entries = log.scan(asterpath_protocol::LogQuery {
        session: SESSION,
        after: None,
        limit: None,
    });
    let mut opened = None;
    while let Some(entry) = entries.next().await {
        if let SessionEvent::TurnStarted { turn_id } =
            decode_entry(&entry.expect("scan")).expect("decode")
        {
            opened = Some(turn_id);
        }
    }
    assert!(opened.is_some());
    assert_eq!(started_turns(&log).await, vec![opened]);
}
