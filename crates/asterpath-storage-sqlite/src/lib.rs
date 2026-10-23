//! SQLite backend: one file holds every session, and one module implements
//! `SessionStore`.
//!
//! A single writer connection owns the file: `append` writes the log row and
//! its `sessions` catalog row in one transaction. The process takes a
//! non-blocking `flock` at open, so a second process gets `WriterBusy`.
//! `retire` folds the session facts, writes the tombstone and deletes the
//! log and catalog row in one immediate transaction.

use std::fs::OpenOptions as FsOpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use asterpath_protocol::{
    Clock, DedupKey, LogEntry, LogId, LogQuery, NewEntry, RetirementError, SessionId, SessionStore,
    StorageError, SystemClock,
};
use asterpath_protocol::{SessionEvent, SessionFacts, decode_payload};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;

/// Physical schema version tracked in `PRAGMA user_version`.
pub const SUPPORTED_SCHEMA_VERSION: i64 = 2;

/// Durability of committed appends.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Synchronous {
    /// Survives process crash and power loss; the production default.
    #[default]
    Full,
    /// Survives process crash only. Tests and development.
    Normal,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OpenOptions {
    pub synchronous: Synchronous,
}

const SCHEMA_V1: &str = "
CREATE TABLE sessions (
    session_id  BLOB    PRIMARY KEY,
    parent_id   BLOB,
    created_at  INTEGER NOT NULL
);
CREATE TABLE log (
    log_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id      BLOB    NOT NULL,
    seq             INTEGER NOT NULL,
    recorded_at     INTEGER NOT NULL,
    schema_version  INTEGER NOT NULL,
    event_kind      TEXT    NOT NULL,
    dedup_key       TEXT,
    payload         TEXT    NOT NULL,
    UNIQUE (session_id, seq),
    UNIQUE (session_id, event_kind, dedup_key)
);
CREATE TABLE retired_sessions (
    session_id   BLOB    PRIMARY KEY,
    parent_id    BLOB,
    last_log_id  INTEGER NOT NULL,
    retired_at   INTEGER NOT NULL
);
";

/// v2 (ADR-0023): wake discovery folds session facts, so the derived wake
/// tables go. The log is untouched.
const MIGRATE_V2: &str = "
DROP TABLE IF EXISTS open_intents;
DROP INDEX IF EXISTS timers_due;
DROP TABLE IF EXISTS timers;
DROP TABLE IF EXISTS pending_inputs;
";

/// Open the database with the production defaults.
pub fn open(path: impl AsRef<Path>) -> Result<Arc<SqliteStorage>, StorageError> {
    open_with(path, OpenOptions::default())
}

/// Open the database, configuring durability explicitly.
pub fn open_with(
    path: impl AsRef<Path>,
    options: OpenOptions,
) -> Result<Arc<SqliteStorage>, StorageError> {
    let path = path.as_ref();
    create_private_file(path)?;
    let conn = Connection::open(path).map_err(backend)?;
    configure(&conn, options.synchronous)?;
    let lock = acquire_writer_lock(path)?;
    migrate(&conn)?;
    Ok(Arc::new(SqliteStorage {
        conn: Mutex::new(conn),
        _lock: lock,
        clock: SystemClock,
    }))
}

pub struct SqliteStorage {
    conn: Mutex<Connection>,
    _lock: WriterLock,
    clock: SystemClock,
}

impl SqliteStorage {
    fn conn(&self) -> MutexGuard<'_, Connection> {
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn append_sync(&self, entry: NewEntry) -> Result<LogId, StorageError> {
        let payload = serde_json::to_string(&entry.payload).map_err(backend)?;

        let mut conn = self.conn();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;

        // Decode inside the transaction and before dedup: a bad payload is a
        // hard error, and the decoded event yields the `event_kind`
        // discriminator and the catalog row.
        let event = decode_payload(&entry.payload, entry.schema_version)
            .map_err(|error| StorageError::InvalidEntry(error.to_string()))?;
        let kind = event.kind().to_owned();

        if let Some(key) = &entry.dedup_key {
            let existing = tx
                .query_row(
                    "SELECT log_id FROM log \
                     WHERE session_id = ?1 AND event_kind = ?2 AND dedup_key = ?3",
                    params![entry.session.as_bytes(), kind, key.as_str()],
                    |row| row.get::<_, i64>(0),
                )
                .optional()
                .map_err(backend)?;
            if let Some(log_id) = existing {
                return Ok(LogId::from_i64(log_id));
            }
        }

        let seq: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(seq), 0) + 1 FROM log WHERE session_id = ?1",
                [entry.session.as_bytes()],
                |row| row.get(0),
            )
            .map_err(backend)?;
        let recorded_at = self.clock.now();
        tx.execute(
            "INSERT INTO log \
             (session_id, seq, recorded_at, schema_version, event_kind, dedup_key, payload) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                entry.session.as_bytes(),
                seq,
                recorded_at,
                entry.schema_version,
                kind,
                entry.dedup_key.as_ref().map(DedupKey::as_str),
                payload
            ],
        )
        .map_err(backend)?;
        let log_id = LogId::from_i64(tx.last_insert_rowid());
        if let SessionEvent::SessionCreated { parent, .. } = &event {
            let parent = parent
                .as_ref()
                .map(|parent| parent.session.as_bytes().to_vec());
            tx.execute(
                "INSERT OR REPLACE INTO sessions (session_id, parent_id, created_at) \
                 VALUES (?1, ?2, ?3)",
                params![entry.session.as_bytes(), parent, recorded_at],
            )
            .map_err(backend)?;
        }
        tx.commit().map_err(backend)?;
        Ok(log_id)
    }

    fn scan_sync(&self, query: LogQuery) -> Vec<Result<LogEntry, StorageError>> {
        let conn = self.conn();
        let limit = query.limit.map(|limit| limit as i64).unwrap_or(-1);
        let after = query.after.map(LogId::as_i64).unwrap_or(0);
        let mut statement = match conn.prepare(
            "SELECT log_id, session_id, seq, recorded_at, schema_version, dedup_key, payload \
             FROM log WHERE session_id = ?1 AND log_id > ?2 \
             AND NOT EXISTS (SELECT 1 FROM retired_sessions WHERE session_id = ?1) \
             ORDER BY seq ASC LIMIT ?3",
        ) {
            Ok(statement) => statement,
            Err(error) => return vec![Err(backend(error))],
        };
        let rows = statement.query_map(params![query.session.as_bytes(), after, limit], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, String>(6)?,
            ))
        });
        let rows = match rows {
            Ok(rows) => rows,
            Err(error) => return vec![Err(backend(error))],
        };

        let mut entries = Vec::new();
        for row in rows {
            match row
                .map_err(backend)
                .and_then(|row| to_entry(query.session, row))
            {
                Ok(entry) => entries.push(Ok(entry)),
                Err(error) => {
                    entries.push(Err(error));
                    break;
                }
            }
        }
        entries
    }

    fn frontier_sync(&self, session: SessionId) -> Result<Option<LogId>, StorageError> {
        let conn = self.conn();
        let frontier: Option<i64> = conn
            .query_row(
                "SELECT MAX(log_id) FROM log WHERE session_id = ?1 \
                 AND NOT EXISTS (SELECT 1 FROM retired_sessions WHERE session_id = ?1)",
                [session.as_bytes()],
                |row| row.get(0),
            )
            .map_err(backend)?;
        Ok(frontier.map(LogId::from_i64))
    }

    /// Session retirement; the check order is the contract in retention.md §5.
    fn retire_sync(
        &self,
        session: SessionId,
        expected_frontier: LogId,
    ) -> Result<(), RetirementError> {
        let mut conn = self.conn();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(backend)?;

        // An existing tombstone is the truth: the log is already gone.
        let tombstone: Option<i64> = tx
            .query_row(
                "SELECT last_log_id FROM retired_sessions WHERE session_id = ?1",
                [session.as_bytes()],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)?;
        if let Some(last_log_id) = tombstone {
            return if last_log_id == expected_frontier.as_i64() {
                Ok(())
            } else {
                Err(RetirementError::FrontierMoved {
                    expected: expected_frontier,
                    actual: Some(LogId::from_i64(last_log_id)),
                })
            };
        }

        let parent: Option<Option<Vec<u8>>> = tx
            .query_row(
                "SELECT parent_id FROM sessions WHERE session_id = ?1",
                [session.as_bytes()],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)?;
        let Some(parent) = parent else {
            return Err(RetirementError::NotFound);
        };

        if !is_idle(&tx, session)? {
            return Err(RetirementError::NotIdle);
        }

        let actual: Option<i64> = tx
            .query_row(
                "SELECT MAX(log_id) FROM log WHERE session_id = ?1",
                [session.as_bytes()],
                |row| row.get(0),
            )
            .map_err(backend)?;
        if actual != Some(expected_frontier.as_i64()) {
            return Err(RetirementError::FrontierMoved {
                expected: expected_frontier,
                actual: actual.map(LogId::from_i64),
            });
        }

        // Live children: a sessions row whose parent is this session and that
        // has not itself been retired.
        let children: i64 = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sessions s WHERE s.parent_id = ?1 \
                 AND NOT EXISTS \
                     (SELECT 1 FROM retired_sessions r WHERE r.session_id = s.session_id))",
                [session.as_bytes()],
                |row| row.get(0),
            )
            .map_err(backend)?;
        if children != 0 {
            return Err(RetirementError::HasChildren);
        }

        if let Some(parent_bytes) = parent.as_deref() {
            let parent = session_from_blob(parent_bytes.to_vec())?;
            if !is_idle(&tx, parent)? {
                return Err(RetirementError::ParentNotIdle);
            }
        }

        let retired_at = self.clock.now();
        tx.execute(
            "INSERT INTO retired_sessions (session_id, parent_id, last_log_id, retired_at) \
             VALUES (?1, ?2, ?3, ?4)",
            params![
                session.as_bytes(),
                parent.as_deref(),
                expected_frontier.as_i64(),
                retired_at
            ],
        )
        .map_err(backend)?;
        tx.execute(
            "DELETE FROM log WHERE session_id = ?1",
            [session.as_bytes()],
        )
        .map_err(backend)?;
        tx.execute(
            "DELETE FROM sessions WHERE session_id = ?1",
            [session.as_bytes()],
        )
        .map_err(backend)?;
        tx.commit().map_err(backend)?;
        Ok(())
    }

    fn sessions_sync(&self) -> Result<Vec<SessionId>, StorageError> {
        let conn = self.conn();
        let mut statement = conn
            .prepare(
                "SELECT session_id FROM sessions \
                 WHERE session_id NOT IN (SELECT session_id FROM retired_sessions) \
                 ORDER BY session_id",
            )
            .map_err(backend)?;
        let rows = statement
            .query_map([], |row| row.get::<_, Vec<u8>>(0))
            .map_err(backend)?;
        rows.map(|row| row.map_err(backend).and_then(session_from_blob))
            .collect()
    }

    fn parent_sync(&self, session: SessionId) -> Result<Option<SessionId>, StorageError> {
        let conn = self.conn();
        let parent: Option<Option<Vec<u8>>> = conn
            .query_row(
                "SELECT parent_id FROM sessions WHERE session_id = ?1 \
                 AND NOT EXISTS (SELECT 1 FROM retired_sessions WHERE session_id = ?1)",
                [session.as_bytes()],
                |row| row.get(0),
            )
            .optional()
            .map_err(backend)?;
        match parent {
            None | Some(None) => Ok(None),
            Some(Some(bytes)) => session_from_blob(bytes).map(Some),
        }
    }

    fn children_sync(&self, session: SessionId) -> Vec<Result<SessionId, StorageError>> {
        let conn = self.conn();
        let mut statement = match conn.prepare(
            "SELECT session_id FROM sessions WHERE parent_id = ?1 \
             AND session_id NOT IN (SELECT session_id FROM retired_sessions) \
             ORDER BY session_id",
        ) {
            Ok(statement) => statement,
            Err(error) => return vec![Err(backend(error))],
        };
        let rows = match statement.query_map([session.as_bytes()], |row| row.get::<_, Vec<u8>>(0)) {
            Ok(rows) => rows,
            Err(error) => return vec![Err(backend(error))],
        };
        rows.map(|row| row.map_err(backend).and_then(session_from_blob))
            .collect()
    }
}

impl SessionStore for SqliteStorage {
    fn append(&self, entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>> {
        Box::pin(async move { self.append_sync(entry) })
    }

    fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>> {
        stream::iter(self.scan_sync(query)).boxed()
    }

    fn frontier(&self, session: SessionId) -> BoxFuture<'_, Result<Option<LogId>, StorageError>> {
        Box::pin(async move { self.frontier_sync(session) })
    }

    fn sessions(&self) -> BoxFuture<'_, Result<Vec<SessionId>, StorageError>> {
        Box::pin(async move { self.sessions_sync() })
    }

    fn parent(&self, session: SessionId) -> BoxFuture<'_, Result<Option<SessionId>, StorageError>> {
        Box::pin(async move { self.parent_sync(session) })
    }

    fn children(&self, session: SessionId) -> BoxStream<'_, Result<SessionId, StorageError>> {
        stream::iter(self.children_sync(session)).boxed()
    }

    fn retire(
        &self,
        session: SessionId,
        expected_frontier: LogId,
    ) -> BoxFuture<'_, Result<(), RetirementError>> {
        Box::pin(async move { self.retire_sync(session, expected_frontier) })
    }
}

struct WriterLock {
    _file: std::fs::File,
}

fn create_private_file(path: &Path) -> Result<(), StorageError> {
    FsOpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .map(|_| ())
        .map_err(backend)
}

fn configure(conn: &Connection, synchronous: Synchronous) -> Result<(), StorageError> {
    let mode: String = conn
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .map_err(backend)?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(StorageError::Backend(format!(
            "could not enable WAL (journal_mode={mode})"
        )));
    }
    let synchronous = match synchronous {
        Synchronous::Full => "FULL",
        Synchronous::Normal => "NORMAL",
    };
    conn.pragma_update(None, "synchronous", synchronous)
        .map_err(backend)?;
    conn.pragma_update(None, "busy_timeout", 0)
        .map_err(backend)?;
    Ok(())
}

/// How long to keep retrying a contended writer lock (1 ms per attempt).
/// A second *process* holds the lock indefinitely and still gets
/// `WriterBusy`; this only absorbs the fork/exec window in which a child
/// transiently inherits the lock descriptor before `CLOEXEC` closes it.
const WRITER_LOCK_RETRIES: usize = 50;

fn acquire_writer_lock(path: &Path) -> Result<WriterLock, StorageError> {
    let mut lock_path = path.as_os_str().to_os_string();
    lock_path.push(".lock");
    let file = FsOpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .mode(0o600)
        .open(PathBuf::from(lock_path))
        .map_err(backend)?;
    for _ in 0..WRITER_LOCK_RETRIES {
        // SAFETY: `file` owns a valid descriptor for the duration of the call.
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc == 0 {
            return Ok(WriterLock { _file: file });
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(code) if code == libc::EWOULDBLOCK || code == libc::EAGAIN => {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            _ => return Err(backend(error)),
        }
    }
    Err(StorageError::WriterBusy)
}

fn migrate(conn: &Connection) -> Result<(), StorageError> {
    let mut version: i64 = conn
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(backend)?;
    if version > SUPPORTED_SCHEMA_VERSION {
        return Err(StorageError::SchemaTooNew {
            found: version,
            supported: SUPPORTED_SCHEMA_VERSION,
        });
    }
    while version < SUPPORTED_SCHEMA_VERSION {
        match version {
            0 => migrate_v1(conn)?,
            1 => migrate_v2(conn)?,
            _ => {
                return Err(StorageError::Backend(format!(
                    "no migration from schema version {version}"
                )));
            }
        }
        version += 1;
    }
    Ok(())
}

fn migrate_v1(conn: &Connection) -> Result<(), StorageError> {
    let tx = conn.unchecked_transaction().map_err(backend)?;
    tx.execute_batch(SCHEMA_V1).map_err(backend)?;
    tx.pragma_update(None, "user_version", 1).map_err(backend)?;
    tx.commit().map_err(backend)
}

fn migrate_v2(conn: &Connection) -> Result<(), StorageError> {
    let tx = conn.unchecked_transaction().map_err(backend)?;
    tx.execute_batch(MIGRATE_V2).map_err(backend)?;
    tx.pragma_update(None, "user_version", 2).map_err(backend)?;
    tx.commit().map_err(backend)
}

type RawEntry = (i64, Vec<u8>, i64, i64, i64, Option<String>, String);

fn to_entry(session: SessionId, raw: RawEntry) -> Result<LogEntry, StorageError> {
    let (log_id, blob, seq, recorded_at, schema_version, dedup_key, payload) = raw;
    let stored_session = session_from_blob(blob)?;
    if stored_session != session {
        return Err(StorageError::Corrupt {
            session: Some(session),
            detail: "log row session does not match its query".to_owned(),
        });
    }
    let seq = u64::try_from(seq).map_err(|_| StorageError::Corrupt {
        session: Some(session),
        detail: format!("negative seq {seq}"),
    })?;
    let schema_version = u16::try_from(schema_version).map_err(|_| StorageError::Corrupt {
        session: Some(session),
        detail: format!("schema version {schema_version} out of range"),
    })?;
    let payload = serde_json::from_str(&payload).map_err(|error| StorageError::Corrupt {
        session: Some(session),
        detail: error.to_string(),
    })?;
    Ok(LogEntry {
        log_id: LogId::from_i64(log_id),
        session,
        seq,
        recorded_at,
        schema_version,
        dedup_key: dedup_key.map(DedupKey::new),
        payload,
    })
}

fn session_from_blob(bytes: Vec<u8>) -> Result<SessionId, StorageError> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| StorageError::Corrupt {
        session: None,
        detail: "session id must be 16 bytes".to_owned(),
    })?;
    Ok(SessionId::from_bytes(bytes))
}

/// Session facts over the session's committed log, read inside `tx`
/// (storage.md §5): idle is the absence of a non-compaction open intent.
fn is_idle(tx: &Transaction<'_>, session: SessionId) -> Result<bool, StorageError> {
    let mut statement = tx
        .prepare(
            "SELECT log_id, session_id, seq, recorded_at, schema_version, dedup_key, payload \
             FROM log WHERE session_id = ?1 ORDER BY seq ASC",
        )
        .map_err(backend)?;
    let rows = statement
        .query_map([session.as_bytes()], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ))
        })
        .map_err(backend)?;
    let entries = rows
        .map(|row| row.map_err(backend).and_then(|row| to_entry(session, row)))
        .collect::<Result<Vec<_>, _>>()?;
    let facts = SessionFacts::fold(&entries).map_err(|error| StorageError::Corrupt {
        session: Some(session),
        detail: error.to_string(),
    })?;
    Ok(facts.is_idle())
}

fn backend(error: impl std::fmt::Display) -> StorageError {
    StorageError::Backend(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_applies_wal_full_and_no_busy_wait() {
        for (options, expected_synchronous) in [
            (OpenOptions::default(), 2_i64),
            (
                OpenOptions {
                    synchronous: Synchronous::Normal,
                },
                1,
            ),
        ] {
            let dir = std::env::temp_dir().join(format!(
                "asterpath-sqlite-pragmas-{}-{expected_synchronous}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).expect("create temp dir");
            let path = dir.join("db.sqlite");
            let storage = open_with(&path, options).expect("open");
            {
                let conn = storage.conn();
                let journal: String = conn
                    .query_row("PRAGMA journal_mode", [], |row| row.get(0))
                    .expect("journal_mode");
                assert_eq!(journal.to_lowercase(), "wal");
                let synchronous: i64 = conn
                    .query_row("PRAGMA synchronous", [], |row| row.get(0))
                    .expect("synchronous");
                assert_eq!(synchronous, expected_synchronous);
                let busy_timeout: i64 = conn
                    .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
                    .expect("busy_timeout");
                assert_eq!(busy_timeout, 0);
            }
            drop(storage);
            std::fs::remove_dir_all(&dir).ok();
        }
    }
}
