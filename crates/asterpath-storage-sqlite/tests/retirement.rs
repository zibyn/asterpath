//! Session retirement (G8, R1-R9): atomic tombstone + deletes, frontier CAS,
//! idempotence across reopen, and invisibility through the read surface.

mod common;

use std::sync::Arc;

use asterpath_core::{
    DedupKey, EventLog, LogEntry, LogId, LogQuery, NewEntry, RetirementError, SessionId,
    SessionRetirement, Storage, StorageError, TurnId,
};
use asterpath_protocol::{
    CompactionTrigger, ConfigSnapshot, LOG_SCHEMA_VERSION, ModelId, ModelSpec, ParentRef,
    ProviderId, SessionEvent, dedup,
};
use asterpath_storage_sqlite::open;
use common::TempDir;
use futures_util::StreamExt;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    futures_executor::block_on(future)
}

fn session(byte: u8) -> SessionId {
    SessionId::from_bytes([byte; 16])
}

fn config() -> ConfigSnapshot {
    ConfigSnapshot {
        model: ModelSpec {
            provider: ProviderId::new("test"),
            model: ModelId::new("model"),
            context_window: Some(1_000),
        },
        system: vec![],
        tools: vec![],
        request: Default::default(),
        turn: Default::default(),
        compaction: Default::default(),
        subagents: Default::default(),
    }
}

fn session_created(parent: Option<SessionId>) -> SessionEvent {
    SessionEvent::SessionCreated {
        config: config(),
        parent: parent.map(|session| ParentRef {
            session,
            turn: TurnId::from_bytes([1; 16]),
            tool_call: asterpath_core::ToolCallId::from_bytes([1; 16]),
        }),
    }
}

fn turn_started() -> SessionEvent {
    SessionEvent::TurnStarted {
        turn_id: TurnId::from_bytes([2; 16]),
    }
}

fn input_recorded() -> SessionEvent {
    SessionEvent::InputRecorded { input: vec![] }
}

fn compaction_started(frontier: LogId) -> SessionEvent {
    SessionEvent::CompactionStarted {
        trigger: CompactionTrigger::Manual,
        replaces_from: LogId::from_i64(1),
        caused_by: frontier,
        turn: None,
    }
}

fn entry(session: SessionId, event: &SessionEvent, dedup_key: Option<DedupKey>) -> NewEntry {
    NewEntry {
        session,
        schema_version: LOG_SCHEMA_VERSION,
        dedup_key,
        payload: serde_json::to_value(event).expect("serialize event"),
    }
}

fn append(
    storage: &dyn EventLog,
    session: SessionId,
    event: &SessionEvent,
    key: Option<DedupKey>,
) -> LogId {
    block_on(storage.append(entry(session, event, key))).expect("append")
}

fn create(storage: &dyn EventLog, session: SessionId, parent: Option<SessionId>) -> LogId {
    append(
        storage,
        session,
        &session_created(parent),
        Some(dedup::session_created(session)),
    )
}

fn frontier(storage: &dyn EventLog, session: SessionId) -> Option<LogId> {
    block_on(storage.frontier(session)).expect("frontier")
}

fn scan(storage: &dyn EventLog, session: SessionId) -> Vec<Result<LogEntry, StorageError>> {
    block_on(
        storage
            .scan(LogQuery {
                session,
                after: None,
                limit: None,
            })
            .collect(),
    )
}

fn retire(storage: &dyn SessionRetirement, session: SessionId, frontier: LogId) {
    block_on(storage.retire(session, frontier)).expect("retire succeeds");
}

fn retire_error(
    storage: &dyn SessionRetirement,
    session: SessionId,
    frontier: LogId,
) -> RetirementError {
    block_on(storage.retire(session, frontier)).expect_err("retire must fail")
}

fn count(conn: &rusqlite::Connection, table: &str) -> i64 {
    conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
        row.get(0)
    })
    .expect("count rows")
}

// ---------------------------------------------------------------------------
// R2 / R3 / R7
// ---------------------------------------------------------------------------

#[test]
fn retire_hides_the_session_and_retries_idempotently_across_reopen() {
    let dir = TempDir::new("retire-basic");
    let db = dir.db();
    let target = session(0xb0);
    let captured;

    {
        let storage = open(&db).expect("open");
        create(storage.as_ref(), target, None);
        captured = frontier(storage.as_ref(), target).expect("frontier");

        retire(storage.as_ref(), target, captured);

        assert!(frontier(storage.as_ref(), target).is_none());
        assert!(scan(storage.as_ref(), target).is_empty());
        // A same-frontier retry is a success, not a missing session.
        retire(storage.as_ref(), target, captured);
    }

    // Reopen: the tombstone is the truth and the retry stays idempotent.
    {
        let storage = open(&db).expect("reopen");
        assert!(frontier(storage.as_ref(), target).is_none());
        assert!(scan(storage.as_ref(), target).is_empty());
        retire(storage.as_ref(), target, captured);
    }

    let conn = rusqlite::Connection::open(&db).expect("raw open");
    let tombstones: i64 = conn
        .query_row("SELECT COUNT(*) FROM retired_sessions", [], |row| {
            row.get(0)
        })
        .expect("count tombstones");
    assert_eq!(
        tombstones, 1,
        "the tombstone is never cleaned up or duplicated"
    );
    let (last, parent): (i64, Option<Vec<u8>>) = conn
        .query_row(
            "SELECT last_log_id, parent_id FROM retired_sessions",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("tombstone row");
    assert_eq!(last, captured.as_i64());
    assert!(parent.is_none());
    let live: i64 = conn
        .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
        .expect("count sessions");
    assert_eq!(live, 0);
}

// ---------------------------------------------------------------------------
// R1: the frontier is a CAS. A concurrent append makes retirement fail and is
// never lost.
// ---------------------------------------------------------------------------

#[test]
fn frontier_cas_sees_a_concurrent_append_and_loses_nothing() {
    let dir = TempDir::new("retire-cas");
    let db = dir.db();
    let target = session(0xb1);
    let storage = open(&db).expect("open");
    create(storage.as_ref(), target, None);
    let captured = frontier(storage.as_ref(), target).expect("frontier");

    let appender = Arc::clone(&storage);
    let appended = std::thread::spawn(move || {
        append(
            appender.as_ref(),
            target,
            &input_recorded(),
            Some(DedupKey::new("late")),
        )
    })
    .join()
    .expect("append thread");
    assert!(appended > captured);

    let error = retire_error(storage.as_ref(), target, captured);
    assert!(
        matches!(
            error,
            RetirementError::FrontierMoved {
                expected,
                actual: Some(actual),
            } if expected == captured && actual == appended
        ),
        "got {error:?}"
    );
    assert_eq!(
        scan(storage.as_ref(), target).len(),
        2,
        "the record appended after the export is not lost"
    );
    assert_eq!(frontier(storage.as_ref(), target), Some(appended));

    drop(storage);
    let conn = rusqlite::Connection::open(&db).expect("raw open");
    assert_eq!(count(&conn, "retired_sessions"), 0);
}

// ---------------------------------------------------------------------------
// R4: non-idle sessions are rejected before the frontier check, and nothing is
// touched.
// ---------------------------------------------------------------------------

#[test]
fn non_idle_session_is_rejected_before_the_frontier_check() {
    let dir = TempDir::new("retire-not-idle");
    let db = dir.db();
    let target = session(0xb2);
    let storage = open(&db).expect("open");
    create(storage.as_ref(), target, None);
    append(storage.as_ref(), target, &turn_started(), None);
    let current = frontier(storage.as_ref(), target).expect("frontier");

    // A deliberately wrong expected frontier proves NotIdle takes precedence.
    let error = retire_error(
        storage.as_ref(),
        target,
        LogId::from_i64(current.as_i64() + 100),
    );
    assert!(matches!(error, RetirementError::NotIdle), "got {error:?}");

    assert_eq!(scan(storage.as_ref(), target).len(), 2);
    assert_eq!(frontier(storage.as_ref(), target), Some(current));
    drop(storage);

    let conn = rusqlite::Connection::open(&db).expect("raw open");
    assert_eq!(count(&conn, "retired_sessions"), 0);
    assert_eq!(count(&conn, "sessions"), 1);
    assert_eq!(count(&conn, "open_intents"), 1);
}

// ---------------------------------------------------------------------------
// R5: live children block the parent; bottom-up retirement works.
// ---------------------------------------------------------------------------

#[test]
fn live_children_block_parent_retirement_until_retired_bottom_up() {
    let dir = TempDir::new("retire-children");
    let db = dir.db();
    let parent = session(0xb3);
    let child = session(0xb4);
    let storage = open(&db).expect("open");
    create(storage.as_ref(), parent, None);
    create(storage.as_ref(), child, Some(parent));
    let parent_frontier = frontier(storage.as_ref(), parent).expect("parent frontier");
    let child_frontier = frontier(storage.as_ref(), child).expect("child frontier");

    let error = retire_error(storage.as_ref(), parent, parent_frontier);
    assert!(
        matches!(error, RetirementError::HasChildren),
        "got {error:?}"
    );
    assert_eq!(scan(storage.as_ref(), parent).len(), 1);
    assert_eq!(scan(storage.as_ref(), child).len(), 1);

    retire(storage.as_ref(), child, child_frontier);
    retire(storage.as_ref(), parent, parent_frontier);
    assert!(frontier(storage.as_ref(), parent).is_none());
    assert!(frontier(storage.as_ref(), child).is_none());
}

// ---------------------------------------------------------------------------
// R6: a non-idle parent blocks the child.
// ---------------------------------------------------------------------------

#[test]
fn non_idle_parent_blocks_child_retirement() {
    let dir = TempDir::new("retire-parent-busy");
    let db = dir.db();
    let parent = session(0xb5);
    let child = session(0xb6);
    let storage = open(&db).expect("open");
    create(storage.as_ref(), parent, None);
    append(storage.as_ref(), parent, &turn_started(), None);
    create(storage.as_ref(), child, Some(parent));
    let child_frontier = frontier(storage.as_ref(), child).expect("child frontier");

    let error = retire_error(storage.as_ref(), child, child_frontier);
    assert!(
        matches!(error, RetirementError::ParentNotIdle),
        "got {error:?}"
    );
    assert_eq!(scan(storage.as_ref(), child).len(), 1, "child untouched");
}

// ---------------------------------------------------------------------------
// Exact error precedence from retention.md §5.
// ---------------------------------------------------------------------------

#[test]
fn retirement_error_precedence_matches_the_contract() {
    let dir = TempDir::new("retire-precedence");
    let db = dir.db();
    let storage = open(&db).expect("open");

    // Neither tombstone nor live session: NotFound.
    let error = retire_error(storage.as_ref(), session(0xbf), LogId::from_i64(9));
    assert!(matches!(error, RetirementError::NotFound), "got {error:?}");

    // Live session + moved frontier + live child: FrontierMoved beats HasChildren.
    let parent = session(0xc0);
    let child = session(0xc1);
    create(storage.as_ref(), parent, None);
    let exported = frontier(storage.as_ref(), parent).expect("frontier");
    append(storage.as_ref(), parent, &input_recorded(), None);
    create(storage.as_ref(), child, Some(parent));
    let error = retire_error(storage.as_ref(), parent, exported);
    assert!(
        matches!(
            error,
            RetirementError::FrontierMoved {
                actual: Some(_),
                ..
            }
        ),
        "got {error:?}"
    );

    // Idle session with a live child and a non-idle parent: HasChildren beats
    // ParentNotIdle.
    let grandparent = session(0xc2);
    let middle = session(0xc3);
    let leaf = session(0xc4);
    create(storage.as_ref(), grandparent, None);
    append(storage.as_ref(), grandparent, &turn_started(), None);
    create(storage.as_ref(), middle, Some(grandparent));
    create(storage.as_ref(), leaf, Some(middle));
    let middle_frontier = frontier(storage.as_ref(), middle).expect("frontier");
    let error = retire_error(storage.as_ref(), middle, middle_frontier);
    assert!(
        matches!(error, RetirementError::HasChildren),
        "got {error:?}"
    );

    // A tombstone outranks the live-session checks: a different expected
    // frontier reports what was actually retired.
    let done = session(0xc5);
    create(storage.as_ref(), done, None);
    let done_frontier = frontier(storage.as_ref(), done).expect("frontier");
    retire(storage.as_ref(), done, done_frontier);
    let error = retire_error(
        storage.as_ref(),
        done,
        LogId::from_i64(done_frontier.as_i64() + 1),
    );
    assert!(
        matches!(
            error,
            RetirementError::FrontierMoved {
                actual: Some(actual),
                ..
            } if actual == done_frontier
        ),
        "got {error:?}"
    );
}

// ---------------------------------------------------------------------------
// G8: every write in the retirement transaction rolls back together.
// ---------------------------------------------------------------------------

#[test]
fn a_failing_delete_rolls_back_the_whole_retirement() {
    let dir = TempDir::new("retire-atomic");
    let db = dir.db();
    let target = session(0xb7);
    let storage = open(&db).expect("open");
    create(storage.as_ref(), target, None);
    append(storage.as_ref(), target, &input_recorded(), None);
    let captured = frontier(storage.as_ref(), target).expect("frontier");

    {
        let injected = rusqlite::Connection::open(&db).expect("raw open");
        injected
            .execute_batch(
                "CREATE TRIGGER fail_pending_delete BEFORE DELETE ON pending_inputs \
                 BEGIN SELECT RAISE(ABORT, 'injected'); END;",
            )
            .expect("inject failing delete");
    }

    let error = retire_error(storage.as_ref(), target, captured);
    assert!(
        matches!(error, RetirementError::Storage(_)),
        "got {error:?}"
    );
    drop(storage);

    let conn = rusqlite::Connection::open(&db).expect("raw open");
    assert_eq!(count(&conn, "retired_sessions"), 0, "no tombstone");
    assert_eq!(count(&conn, "log"), 2, "log rows survive");
    assert_eq!(count(&conn, "sessions"), 1, "session row survives");
    assert_eq!(count(&conn, "pending_inputs"), 1, "derived rows survive");
    drop(conn);

    let storage = open(&db).expect("reopen");
    assert_eq!(frontier(storage.as_ref(), target), Some(captured));
}

// ---------------------------------------------------------------------------
// R9: no pending-input rows survive; no wake is produced.
// ---------------------------------------------------------------------------

#[test]
fn retirement_leaves_no_wakeable_pending_rows() {
    let dir = TempDir::new("retire-rows");
    let db = dir.db();
    let target = session(0xb8);
    let storage = open(&db).expect("open");
    create(storage.as_ref(), target, None);
    append(storage.as_ref(), target, &input_recorded(), None);
    let captured = frontier(storage.as_ref(), target).expect("frontier");

    retire(storage.as_ref(), target, captured);

    assert!(
        block_on(storage.wakeable(1_000))
            .expect("wakeable")
            .is_empty(),
        "a retired session is never wakeable"
    );
    drop(storage);

    let conn = rusqlite::Connection::open(&db).expect("raw open");
    for table in ["log", "open_intents", "pending_inputs", "sessions"] {
        assert_eq!(count(&conn, table), 0, "{table} must be empty");
    }
}

// ---------------------------------------------------------------------------
// An unclosed compaction intent does not make the session non-idle.
// ---------------------------------------------------------------------------

#[test]
fn an_open_compaction_intent_still_counts_as_idle() {
    let dir = TempDir::new("retire-compaction");
    let db = dir.db();
    let target = session(0xb9);
    let storage = open(&db).expect("open");
    create(storage.as_ref(), target, None);
    let intent_frontier = LogId::from_i64(1);
    append(
        storage.as_ref(),
        target,
        &compaction_started(intent_frontier),
        Some(dedup::compaction(intent_frontier)),
    );
    let captured = frontier(storage.as_ref(), target).expect("frontier");

    retire(storage.as_ref(), target, captured);
    assert!(frontier(storage.as_ref(), target).is_none());
}
