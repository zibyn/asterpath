//! SQLite-specific retirement (G8, R2, R3, R9): the atomic tombstone and
//! deletes, idempotence across reopen and the raw rows left behind. The
//! backend-neutral cases (CAS, idle, children, precedence) are in the shared
//! `session_store_contract`.

mod common;

use asterpath_protocol::{
    ConfigSnapshot, LOG_SCHEMA_VERSION, ModelId, ModelSpec, ParentRef, ProviderId, SessionEvent,
    dedup,
};
use asterpath_protocol::{
    DedupKey, LogEntry, LogId, LogQuery, NewEntry, RetirementError, SessionId, SessionStore,
    StorageError, TurnId,
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
            tool_call: asterpath_protocol::ToolCallId::from_bytes([1; 16]),
        }),
    }
}

fn input_recorded() -> SessionEvent {
    SessionEvent::InputRecorded { input: vec![] }
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
    storage: &dyn SessionStore,
    session: SessionId,
    event: &SessionEvent,
    key: Option<DedupKey>,
) -> LogId {
    block_on(storage.append(entry(session, event, key))).expect("append")
}

fn create(storage: &dyn SessionStore, session: SessionId, parent: Option<SessionId>) -> LogId {
    append(
        storage,
        session,
        &session_created(parent),
        Some(dedup::session_created(session)),
    )
}

fn frontier(storage: &dyn SessionStore, session: SessionId) -> Option<LogId> {
    block_on(storage.frontier(session)).expect("frontier")
}

fn scan(storage: &dyn SessionStore, session: SessionId) -> Vec<Result<LogEntry, StorageError>> {
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

fn retire(storage: &dyn SessionStore, session: SessionId, frontier: LogId) {
    block_on(storage.retire(session, frontier)).expect("retire succeeds");
}

fn retire_error(
    storage: &dyn SessionStore,
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
                "CREATE TRIGGER fail_sessions_delete BEFORE DELETE ON sessions \
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
    drop(conn);

    let storage = open(&db).expect("reopen");
    assert_eq!(frontier(storage.as_ref(), target), Some(captured));
}

// ---------------------------------------------------------------------------
// R9: nothing of the session survives; it is no longer listed.
// ---------------------------------------------------------------------------

#[test]
fn retirement_leaves_no_rows_behind() {
    let dir = TempDir::new("retire-rows");
    let db = dir.db();
    let target = session(0xb8);
    let storage = open(&db).expect("open");
    create(storage.as_ref(), target, None);
    append(storage.as_ref(), target, &input_recorded(), None);
    let captured = frontier(storage.as_ref(), target).expect("frontier");

    retire(storage.as_ref(), target, captured);

    assert!(
        block_on(storage.sessions(Default::default()))
            .expect("sessions")
            .is_empty(),
        "a retired session is never listed, so never woken"
    );
    drop(storage);

    let conn = rusqlite::Connection::open(&db).expect("raw open");
    for table in ["log", "sessions"] {
        assert_eq!(count(&conn, table), 0, "{table} must be empty");
    }
}
