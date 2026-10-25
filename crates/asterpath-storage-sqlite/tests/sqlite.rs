//! SQLite-specific guarantees: real-SIGKILL durability (G1), cross-process
//! session-tree ownership, schema version gate, WAL/permissions, hard errors and the session catalog.

mod common;

use std::io::{BufRead, BufReader};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::ExitStatusExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use asterpath_protocol::{
    ConfigSnapshot, DEFAULT_LEASE_TTL, LOG_SCHEMA_VERSION, ModelId, ModelSpec, ParentRef,
    ProviderId, SessionEvent, dedup,
};
use asterpath_protocol::{
    DedupKey, LogEntry, LogId, LogQuery, NewEntry, ParentFilter, SessionId, SessionQuery,
    SessionStore, StorageError, ToolCallId, TurnId,
};
use asterpath_storage_sqlite::{OpenOptions, open, open_with_clock};
use asterpath_testing::ManualClock;
use common::TempDir;
use futures_util::StreamExt;

const HELPER_MODE: &str = "ASTERPATH_STORAGE_HELPER_MODE";
const HELPER_DB: &str = "ASTERPATH_STORAGE_HELPER_DB";

/// Catalog sessions under `parent`, in id order.
fn listed(storage: &dyn SessionStore, parent: ParentFilter) -> Vec<SessionId> {
    let query = SessionQuery {
        parent,
        ..SessionQuery::default()
    };
    let entries = block_on(storage.sessions(query)).expect("sessions");
    sorted(entries.iter().map(|entry| entry.session).collect())
}

fn sorted(mut sessions: Vec<SessionId>) -> Vec<SessionId> {
    sessions.sort();
    sessions
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    futures_executor::block_on(future)
}

fn session(byte: u8) -> SessionId {
    SessionId::from_bytes([byte; 16])
}

fn helper_session() -> SessionId {
    session(0x11)
}

fn free_session() -> SessionId {
    session(0x12)
}

fn input_recorded() -> SessionEvent {
    SessionEvent::InputRecorded { input: vec![] }
}

fn entry(session: SessionId, event: &SessionEvent, dedup_key: Option<DedupKey>) -> NewEntry {
    NewEntry {
        session,
        schema_version: LOG_SCHEMA_VERSION,
        dedup_key,
        owner: None,
        payload: serde_json::to_value(event).expect("serialize event"),
    }
}

fn append(storage: &dyn SessionStore, entry: NewEntry) -> Result<LogId, StorageError> {
    block_on(storage.append(entry))
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

// ---------------------------------------------------------------------------
// Helper process: the test binary re-enters itself with an env-var mode.
// ---------------------------------------------------------------------------

#[test]
fn helper_entry() {
    let Ok(mode) = std::env::var(HELPER_MODE) else {
        return;
    };
    let db = std::env::var(HELPER_DB).expect("helper database path");
    match mode.as_str() {
        "append" => {
            let storage = open(&db).expect("helper open");
            let log_id = block_on(storage.append(entry(
                helper_session(),
                &input_recorded(),
                Some(DedupKey::new("g1")),
            )))
            .expect("helper append");
            println!("{{\"event\":\"appended\",\"log_id\":{}}}", log_id.as_i64());
            use std::io::Write;
            std::io::stdout().flush().expect("flush marker");
            // Hold the process (and the writer lock) until the parent kills it.
            std::thread::sleep(Duration::from_secs(60));
            std::process::exit(0);
        }
        "claim" => {
            let storage = open(&db).expect("helper open");
            let claim = block_on(storage.claim(helper_session())).expect("helper claim");
            println!(
                "{{\"event\":\"claimed\",\"epoch\":{}}}",
                claim.token().epoch
            );
            use std::io::Write;
            std::io::stdout().flush().expect("flush marker");
            // Hold the tree until the parent kills the process.
            std::thread::sleep(Duration::from_secs(60));
            std::process::exit(0);
        }
        // Exit 0 when the held tree is refused and a free one is driven.
        "probe_claims" => {
            let storage = open(&db).expect("probe open");
            match block_on(storage.claim(helper_session())) {
                Err(StorageError::OwnedElsewhere { root }) if root == helper_session() => {}
                other => {
                    eprintln!("held tree: expected OwnedElsewhere, got {other:?}");
                    std::process::exit(1);
                }
            }
            let free = block_on(storage.claim(free_session())).expect("claim a free tree");
            let mut owned = entry(free_session(), &input_recorded(), None);
            owned.owner = Some(free.token());
            append(storage.as_ref(), owned).expect("owner write");
            std::process::exit(0);
        }
        _ => std::process::exit(2),
    }
}

fn spawn_helper(mode: &str, db: &Path) -> Child {
    Command::new(std::env::current_exe().expect("test binary path"))
        .args(["--exact", "helper_entry", "--nocapture"])
        .env(HELPER_MODE, mode)
        .env(HELPER_DB, db)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn helper process")
}

// ---------------------------------------------------------------------------
// G1: a real SIGKILL after append returns.
// ---------------------------------------------------------------------------

#[test]
fn append_survives_a_real_sigkill() {
    let dir = TempDir::new("g1");
    let db = dir.db();
    let mut child = spawn_helper("append", &db);
    wait_for_marker(&mut child, "\"event\":\"appended\"");

    // The append has returned in the child; kill it before it can clean up.
    sigkill(child);

    let storage = open(&db).expect("reopen after SIGKILL");
    let entries = scan(storage.as_ref(), helper_session());
    assert_eq!(entries.len(), 1, "G1: the committed entry is visible");
    let entry = entries.into_iter().next().unwrap().expect("valid entry");
    assert_eq!(entry.dedup_key, Some(DedupKey::new("g1")));
    assert_eq!(entry.seq, 1);
}

/// The first stdout line of `child` containing `marker`.
fn wait_for_marker(child: &mut Child, marker: &str) -> String {
    let stdout = child.stdout.take().expect("piped stdout");
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if sender.send(line).is_err() {
                break;
            }
        }
    });

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match receiver.recv_timeout(remaining) {
            Ok(Ok(line)) if line.contains(marker) => return line,
            Ok(Ok(_)) => {}
            Ok(Err(error)) => panic!("reading helper stdout: {error}"),
            Err(error) => panic!("helper printed no {marker} within 15s: {error}"),
        }
    }
}

fn sigkill(mut child: Child) {
    unsafe { libc::kill(child.id() as i32, libc::SIGKILL) };
    let status = child.wait().expect("wait for helper");
    assert_eq!(
        status.signal(),
        Some(libc::SIGKILL),
        "the helper must die from SIGKILL, not exit"
    );
}

// ---------------------------------------------------------------------------
// Session-tree ownership across processes (ADR-0036).
// ---------------------------------------------------------------------------

#[test]
fn another_process_drives_other_trees_but_not_a_held_one() {
    let dir = TempDir::new("owned-elsewhere");
    let db = dir.db();
    let storage = open(&db).expect("first process opens");
    let _claim = block_on(storage.claim(helper_session())).expect("claim");

    let status = spawn_helper("probe_claims", &db)
        .wait()
        .expect("wait for probe");
    assert!(status.success(), "probe failed: {status:?}");
    assert_eq!(scan(storage.as_ref(), free_session()).len(), 1);
}

#[test]
fn a_killed_owners_tree_is_taken_over_once_its_lease_expires() {
    let dir = TempDir::new("killed-owner");
    let db = dir.db();
    // This process reads time from a clock the test moves.
    let clock = Arc::new(ManualClock::new());
    let storage = open_with_clock(&db, OpenOptions::default(), clock.clone()).expect("open");
    let mut child = spawn_helper("claim", &db);
    let line = wait_for_marker(&mut child, "\"event\":\"claimed\"");
    // The helper stamped its lease from its wall clock, before this.
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("wall clock")
        .as_millis() as i64;
    clock.advance(wall - ManualClock::BASE);
    let epoch: u64 = line
        .rsplit(':')
        .next()
        .and_then(|tail| tail.trim_end_matches('}').parse().ok())
        .expect("epoch in marker");
    let dead = asterpath_protocol::OwnerToken {
        root: helper_session(),
        epoch,
    };

    assert!(matches!(
        block_on(storage.claim(helper_session())),
        Err(StorageError::OwnedElsewhere { .. })
    ));
    assert_eq!(
        block_on(storage.owner(helper_session())).expect("owner"),
        Some(dead)
    );

    sigkill(child);
    assert!(
        matches!(
            block_on(storage.claim(helper_session())),
            Err(StorageError::OwnedElsewhere { .. })
        ),
        "a dead process keeps its trees until its lease expires"
    );
    clock.advance(DEFAULT_LEASE_TTL.as_millis() as i64 + 1);
    assert_eq!(
        block_on(storage.owner(helper_session())).expect("owner"),
        None,
        "the dead process's lease has expired"
    );
    let claim = block_on(storage.claim(helper_session())).expect("take over");
    assert_eq!(claim.token().epoch, epoch + 1);

    let mut stale = entry(helper_session(), &input_recorded(), None);
    stale.owner = Some(dead);
    assert!(matches!(
        append(storage.as_ref(), stale),
        Err(StorageError::OwnedElsewhere { .. })
    ));
    let mut owned = entry(helper_session(), &input_recorded(), None);
    owned.owner = Some(claim.token());
    append(storage.as_ref(), owned).expect("the new owner writes");
}

// ---------------------------------------------------------------------------
// Physical schema and configuration.
// ---------------------------------------------------------------------------

#[test]
fn higher_schema_version_is_rejected() {
    let dir = TempDir::new("schema-too-new");
    let db = dir.db();
    {
        let conn = rusqlite::Connection::open(&db).expect("raw open");
        conn.pragma_update(None, "user_version", 3)
            .expect("set user_version");
    }
    match open(&db) {
        Err(StorageError::SchemaTooNew { found, supported }) => {
            assert_eq!((found, supported), (3, 1));
        }
        Ok(_) => panic!("expected SchemaTooNew, got a successful open"),
        Err(other) => panic!("expected SchemaTooNew, got {other}"),
    }
}

#[test]
fn database_is_wal_and_private() {
    let dir = TempDir::new("wal-and-perms");
    let db = dir.db();
    let storage = open(&db).expect("open");
    // Write once so the WAL header exists.
    append(
        storage.as_ref(),
        entry(session(1), &input_recorded(), Some(DedupKey::new("perm"))),
    )
    .expect("append");
    drop(storage);

    let mode = std::fs::metadata(&db)
        .expect("metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "the database file is created private");

    let conn = rusqlite::Connection::open(&db).expect("raw open");
    let journal: String = conn
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .expect("journal_mode");
    assert_eq!(journal.to_lowercase(), "wal");
}

// ---------------------------------------------------------------------------
// Hard errors and no torn-tail repair.
// ---------------------------------------------------------------------------

#[test]
fn bad_payload_is_a_hard_error() {
    let dir = TempDir::new("bad-payload");
    let db = dir.db();
    let storage = open(&db).expect("open");
    let error = append(
        storage.as_ref(),
        NewEntry {
            session: session(2),
            schema_version: LOG_SCHEMA_VERSION,
            dedup_key: None,
            owner: None,
            payload: serde_json::json!({"NoSuchEvent": {}}),
        },
    )
    .expect_err("unknown event must be rejected");
    assert!(matches!(error, StorageError::InvalidEntry(_)));
    assert!(scan(storage.as_ref(), session(2)).is_empty());

    // The writer still works after the rejected append.
    append(
        storage.as_ref(),
        entry(
            session(2),
            &input_recorded(),
            Some(DedupKey::new("after-error")),
        ),
    )
    .expect("valid append after rejection");
    assert_eq!(scan(storage.as_ref(), session(2)).len(), 1);
}

#[test]
fn corrupt_payload_is_reported_and_not_repaired() {
    let dir = TempDir::new("corrupt");
    let db = dir.db();
    let target = session(3);
    {
        let storage = open(&db).expect("open");
        append(
            storage.as_ref(),
            entry(target, &input_recorded(), Some(DedupKey::new("one"))),
        )
        .expect("valid append");
    }
    {
        let conn = rusqlite::Connection::open(&db).expect("raw open");
        conn.execute(
            "INSERT INTO log \
             (session_id, seq, recorded_at, schema_version, event_kind, dedup_key, payload) \
             VALUES (?1, 2, 0, 1, 'InputRecorded', NULL, 'not json')",
            [target.as_bytes()],
        )
        .expect("inject corrupt row");
    }

    let storage = open(&db).expect("reopen");
    let entries = scan(storage.as_ref(), target);
    assert_eq!(entries.len(), 2, "the committed prefix is still readable");
    assert!(entries[0].is_ok());
    assert!(matches!(
        entries[1],
        Err(StorageError::Corrupt {
            session: Some(found),
            ..
        }) if found == target
    ));
    drop(storage);

    // No torn-tail repair: the bad row is still there.
    let conn = rusqlite::Connection::open(&db).expect("raw open");
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM log WHERE session_id = ?1",
            [target.as_bytes()],
            |row| row.get(0),
        )
        .expect("count rows");
    assert_eq!(count, 2, "corrupt data is a hard error, never truncated");
}

// ---------------------------------------------------------------------------
// Session catalog.
// ---------------------------------------------------------------------------

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
            tool_call: ToolCallId::from_bytes([1; 16]),
        }),
        metadata: Default::default(),
    }
}

#[test]
fn catalog_reports_parents_and_hides_retired_sessions() {
    let dir = TempDir::new("catalog");
    let db = dir.db();
    let parent = session(0xa0);
    let child = session(0xa1);
    let retired = session(0xa2);
    {
        let storage = open(&db).expect("open");
        for (session, parent) in [
            (parent, None),
            (child, Some(parent)),
            (retired, Some(parent)),
        ] {
            append(
                storage.as_ref(),
                entry(
                    session,
                    &session_created(parent),
                    Some(dedup::session_created(session)),
                ),
            )
            .expect("append SessionCreated");
        }

        assert_eq!(
            block_on(storage.parent(child)).expect("parent"),
            Some(parent)
        );
        assert_eq!(block_on(storage.parent(parent)).expect("parent"), None);
        assert_eq!(
            listed(storage.as_ref(), ParentFilter::ChildrenOf(parent)),
            sorted(vec![child, retired])
        );
    }

    // Tombstone the retired child while leaving its sessions row in place:
    // catalog lookups must still hide it.
    {
        let conn = rusqlite::Connection::open(&db).expect("raw open");
        conn.execute(
            "INSERT INTO retired_sessions (session_id, parent_id, last_log_id, retired_at) \
             VALUES (?1, ?2, 2, 0)",
            rusqlite::params![retired.as_bytes(), parent.as_bytes()],
        )
        .expect("write tombstone");
    }

    let storage = open(&db).expect("reopen");
    assert_eq!(
        block_on(storage.parent(retired)).expect("parent"),
        None,
        "retired sessions are invisible"
    );
    assert_eq!(
        listed(storage.as_ref(), ParentFilter::Any),
        sorted(vec![parent, child]),
        "sessions() hides tombstoned rows"
    );
    assert_eq!(
        listed(storage.as_ref(), ParentFilter::ChildrenOf(parent)),
        vec![child]
    );
}

// ---------------------------------------------------------------------------
// Physical layout.
// ---------------------------------------------------------------------------

fn tables(conn: &rusqlite::Connection) -> Vec<String> {
    let mut statement = conn
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
        .expect("list tables");
    statement
        .query_map([], |row| row.get(0))
        .expect("query tables")
        .map(|name| name.expect("table name"))
        .collect()
}

#[test]
fn a_new_database_has_only_the_current_tables() {
    let dir = TempDir::new("fresh-v2");
    let db = dir.db();
    drop(open(&db).expect("open"));
    let conn = rusqlite::Connection::open(&db).expect("raw open");
    assert_eq!(
        tables(&conn),
        vec![
            "log",
            "process_leases",
            "retired_sessions",
            "sessions",
            "sqlite_sequence",
            "tree_owners"
        ]
    );
}

// ---------------------------------------------------------------------------
// Writer thread: group commit with a savepoint per record (#97).
// ---------------------------------------------------------------------------

#[test]
fn reopening_right_after_drop_keeps_committed_records() {
    let dir = TempDir::new("drop-reopen");
    let storage = open(dir.db()).expect("open");
    for _ in 0..3 {
        append(storage.as_ref(), entry(session(5), &input_recorded(), None)).expect("append");
    }
    drop(storage);
    let storage = open(dir.db()).expect("reopen in the same process");
    assert_eq!(scan(storage.as_ref(), session(5)).len(), 3);
}
