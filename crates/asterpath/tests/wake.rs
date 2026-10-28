//! Wake discovery (ADR-0023): `Asterpath::wakeable` folds session facts over
//! every listed session, on both backends.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use asterpath::Asterpath;
use asterpath::protocol::{
    CompactionConfig, CompactionTrigger, LogId, ModelId, ModelSpec, ParentRef, Part, ProviderId,
    RequestConfig, SessionEvent, SessionId, SessionSpec, StepId, SubagentConfig, ToolCallId,
    ToolSelection, TurnConfig, TurnEndReason, TurnId, UserInput, dedup, encode_entry,
};
use asterpath_testing::{InMemoryEventLog, ScriptedProvider};

fn spec() -> SessionSpec {
    SessionSpec {
        system_sections: Vec::new(),
        model: ModelSpec {
            provider: ProviderId::new("p"),
            model: ModelId::new("model-1"),
            context_window: Some(2_000),
        },
        system: vec![Part::Text {
            text: "be helpful".to_owned(),
        }],
        tools: ToolSelection::All,
        request: RequestConfig::default(),
        turn: TurnConfig::default(),
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
        metadata: Default::default(),
    }
}

async fn append(asterpath: &Asterpath, session: SessionId, event: SessionEvent) {
    append_keyed(asterpath, session, event, None).await;
}

async fn append_keyed(
    asterpath: &Asterpath,
    session: SessionId,
    event: SessionEvent,
    key: Option<asterpath::protocol::DedupKey>,
) {
    let entry = encode_entry(session, key, &event).expect("encode");
    asterpath.store().append(entry).await.expect("append");
}

async fn new_session(asterpath: &Asterpath) -> SessionId {
    asterpath.create_session(spec()).await.expect("create").id()
}

fn turn(byte: u8) -> TurnId {
    TurnId::from_bytes([byte; 16])
}

fn step(byte: u8) -> StepId {
    StepId::from_bytes([byte; 16])
}

/// Sessions of every wake-relevant shape; returns them as
/// `(wakeable, not wakeable)`.
async fn seed(asterpath: &Asterpath) -> (Vec<SessionId>, Vec<SessionId>) {
    // Idle: a committed turn and nothing pending.
    let idle = new_session(asterpath).await;
    append(
        asterpath,
        idle,
        SessionEvent::TurnStarted { turn_id: turn(1) },
    )
    .await;
    append(
        asterpath,
        idle,
        SessionEvent::TurnCommitted {
            turn_id: turn(1),
            reason: TurnEndReason::Completed,
        },
    )
    .await;

    // S13: a live session with an open step is not idle.
    let active = new_session(asterpath).await;
    append(
        asterpath,
        active,
        SessionEvent::TurnStarted { turn_id: turn(2) },
    )
    .await;
    append(
        asterpath,
        active,
        SessionEvent::StepStarted {
            turn_id: turn(2),
            step_id: step(2),
        },
    )
    .await;

    // S18: steering input recorded after the last step started, then a crash:
    // nothing is open, but the input still wakes the session.
    let steered = new_session(asterpath).await;
    append(
        asterpath,
        steered,
        SessionEvent::TurnStarted { turn_id: turn(3) },
    )
    .await;
    append(
        asterpath,
        steered,
        SessionEvent::StepStarted {
            turn_id: turn(3),
            step_id: step(3),
        },
    )
    .await;
    asterpath
        .open_session(steered)
        .await
        .expect("open")
        .submit_input(UserInput {
            id: asterpath::protocol::InputId::from_bytes([9; 16]),
            parts: vec![Part::Text {
                text: "also this".to_owned(),
            }],
        })
        .await
        .expect("steer");
    append(
        asterpath,
        steered,
        SessionEvent::StepCommitted {
            turn_id: turn(3),
            step_id: step(3),
        },
    )
    .await;
    append(
        asterpath,
        steered,
        SessionEvent::TurnCommitted {
            turn_id: turn(3),
            reason: TurnEndReason::Completed,
        },
    )
    .await;

    // Only a dangling compaction intent: discarding it is safe, so not wakeable.
    let compacting = new_session(asterpath).await;
    append_keyed(
        asterpath,
        compacting,
        SessionEvent::CompactionStarted {
            trigger: CompactionTrigger::Manual,
            replaces_from: LogId::from_i64(1),
            caused_by: LogId::from_i64(1),
            turn: None,
        },
        Some(dedup::compaction(LogId::from_i64(1))),
    )
    .await;

    // A child session with an open turn is listed and woken too.
    let child = SessionId::from_bytes([0xc1; 16]);
    let created = SessionEvent::SessionCreated {
        config: spec().snapshot(vec![]),
        parent: Some(ParentRef {
            session: idle,
            turn: turn(1),
            tool_call: ToolCallId::from_bytes([1; 16]),
        }),
        metadata: Default::default(),
    };
    append_keyed(
        asterpath,
        child,
        created,
        Some(dedup::session_created(child)),
    )
    .await;
    append(
        asterpath,
        child,
        SessionEvent::TurnStarted { turn_id: turn(4) },
    )
    .await;

    let mut wakeable = vec![active, steered, child];
    wakeable.sort();
    (wakeable, vec![idle, compacting])
}

async fn assert_wakeable(asterpath: &Asterpath, wakeable: &[SessionId], asleep: &[SessionId]) {
    let found = asterpath.wakeable().await.expect("wakeable");
    assert_eq!(found, wakeable, "exactly the unfinished sessions, by id");
    for session in asleep {
        assert!(!found.contains(session));
    }
}

#[tokio::test]
async fn memory_store_wakes_exactly_the_unfinished_sessions() {
    let asterpath = Asterpath::builder()
        .providers(vec![Arc::new(ScriptedProvider::new("p"))])
        .store(Arc::new(InMemoryEventLog::new()))
        .build()
        .await
        .expect("facade");
    let (wakeable, asleep) = seed(&asterpath).await;
    assert_wakeable(&asterpath, &wakeable, &asleep).await;
}

fn temp_db() -> PathBuf {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    std::env::temp_dir().join(format!("asterpath-wake-{}-{unique}.db", std::process::id()))
}

fn cleanup(path: &Path) {
    for suffix in ["", "-wal", "-shm", ".lock"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
}

async fn sqlite(db: &Path) -> Asterpath {
    Asterpath::builder()
        .database(db)
        .providers(vec![Arc::new(ScriptedProvider::new("p"))])
        .build()
        .await
        .expect("facade")
}

#[tokio::test]
async fn sqlite_store_wakes_exactly_the_unfinished_sessions_across_reopen() {
    let db = temp_db();
    let (wakeable, asleep) = {
        let asterpath = sqlite(&db).await;
        let seeded = seed(&asterpath).await;
        assert_wakeable(&asterpath, &seeded.0, &seeded.1).await;
        seeded
    };

    // After a crash the reopened store reports the same set.
    let asterpath = sqlite(&db).await;
    assert_wakeable(&asterpath, &wakeable, &asleep).await;

    drop(asterpath);
    cleanup(&db);
}
