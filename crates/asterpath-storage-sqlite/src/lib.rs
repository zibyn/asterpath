//! SQLite backend: one file holds every session, and one module implements
//! `SessionStore`.
//!
//! A dedicated writer thread owns the write connection; `append` and `retire`
//! are commands on an unbounded queue, answered over a oneshot. The thread
//! group-commits whatever is already queued (no wait window, at most
//! `MAX_BATCH`) in one immediate transaction, each command in its own
//! savepoint, and replies only after `COMMIT`. `append` writes the log row and
//! its `sessions` catalog row; `retire` folds the session facts, writes the
//! tombstone and deletes the log and catalog row. Reads run on the caller
//! over a separate read-only connection, so they never queue behind a fsync.
//! The process takes a non-blocking `flock` at open, so a second process gets
//! `WriterBusy`.

use std::fs::OpenOptions as FsOpenOptions;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, mpsc};
use std::thread::JoinHandle;

use asterpath_protocol::{
    CatalogEntry, Clock, DedupKey, LogEntry, LogId, LogQuery, NewEntry, ParentFilter,
    RetirementError, SessionId, SessionQuery, SessionStore, StorageError, SystemClock,
};
use asterpath_protocol::{SessionEvent, SessionFacts, decode_payload};
use futures_channel::oneshot;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;

/// Physical schema version tracked in `PRAGMA user_version`.
pub const SUPPORTED_SCHEMA_VERSION: i64 = 1;

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

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS sessions (
    session_id  BLOB    PRIMARY KEY,
    parent_id   BLOB,
    created_at  INTEGER NOT NULL,
    metadata    TEXT    NOT NULL
);
CREATE TABLE IF NOT EXISTS log (
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
CREATE TABLE IF NOT EXISTS retired_sessions (
    session_id   BLOB    PRIMARY KEY,
    parent_id    BLOB,
    last_log_id  INTEGER NOT NULL,
    retired_at   INTEGER NOT NULL
);
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
    open_with_clock(path, options, Arc::new(SystemClock))
}

/// Contract-test seam: a fixed clock makes `created_at` ties deterministic.
#[doc(hidden)]
pub fn open_with_clock(
    path: impl AsRef<Path>,
    options: OpenOptions,
    clock: Arc<dyn Clock>,
) -> Result<Arc<SqliteStorage>, StorageError> {
    let path = path.as_ref();
    create_private_file(path)?;
    let conn = Connection::open(path).map_err(backend)?;
    let lock = acquire_writer_lock(path)?;
    prepare(&conn, options.synchronous)?;
    let reader = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(backend)?;
    // shortcut: unbounded queue, no backpressure when appends outrun fsync; switch to a
    // bounded channel with an async send when a host can outrun the disk.
    let (commands, queue) = mpsc::channel();
    let thread = std::thread::Builder::new()
        .name("asterpath-sqlite-writer".to_owned())
        .spawn(move || run_writer(conn, queue, clock))
        .map_err(backend)?;
    Ok(Arc::new(SqliteStorage {
        writer: Writer {
            commands: Some(commands),
            thread: Some(thread),
        },
        reader: Mutex::new(reader),
        _lock: lock,
    }))
}

/// Field order is drop order: the writer drains and joins before the reader
/// closes, and the `flock` is released last, so a same-process reopen works.
pub struct SqliteStorage {
    writer: Writer,
    reader: Mutex<Connection>,
    _lock: WriterLock,
}

/// Most writes group-committed in one transaction.
const MAX_BATCH: usize = 64;

#[cfg(test)]
type WriterJob = Box<dyn FnOnce(&Connection) + Send>;

enum Command {
    Write(Write),
    #[cfg(test)]
    Run(WriterJob),
}

enum Write {
    Append {
        entry: NewEntry,
        payload: String,
        reply: oneshot::Sender<Result<LogId, StorageError>>,
    },
    Retire {
        session: SessionId,
        expected_frontier: LogId,
        reply: oneshot::Sender<Result<(), RetirementError>>,
    },
}

/// A write executed in the batch, waiting for `COMMIT` before its reply.
enum Done {
    Append(
        oneshot::Sender<Result<LogId, StorageError>>,
        Result<LogId, StorageError>,
    ),
    Retire(
        oneshot::Sender<Result<(), RetirementError>>,
        Result<(), RetirementError>,
    ),
}

impl Done {
    /// A write that never ran because the transaction did not begin.
    fn skipped(write: Write) -> Self {
        match write {
            Write::Append { reply, .. } => Done::Append(reply, Err(writer_stopped())),
            Write::Retire { reply, .. } => Done::Retire(reply, Err(writer_stopped().into())),
        }
    }

    /// Callers whose future was dropped are gone; their send error is moot.
    fn reply(self, commit: &Result<(), StorageError>) {
        match (self, commit) {
            (Done::Append(reply, result), Ok(())) => drop(reply.send(result)),
            (Done::Append(reply, _), Err(error)) => drop(reply.send(Err(backend(error)))),
            (Done::Retire(reply, result), Ok(())) => drop(reply.send(result)),
            (Done::Retire(reply, _), Err(error)) => drop(reply.send(Err(backend(error).into()))),
        }
    }
}

struct Writer {
    commands: Option<mpsc::Sender<Command>>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for Writer {
    fn drop(&mut self) {
        // Closing the queue ends the thread once it has drained it.
        drop(self.commands.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn writer_stopped() -> StorageError {
    StorageError::Backend("sqlite writer stopped".to_owned())
}

fn run_writer(mut conn: Connection, queue: mpsc::Receiver<Command>, clock: Arc<dyn Clock>) {
    while let Ok(first) = queue.recv() {
        let mut batch = match first {
            #[cfg(test)]
            Command::Run(job) => {
                job(&conn);
                continue;
            }
            Command::Write(write) => vec![write],
        };
        // Take only what is already queued: no timer, no added latency.
        #[cfg(test)]
        let mut after = None;
        while batch.len() < MAX_BATCH {
            match queue.try_recv() {
                Ok(Command::Write(write)) => batch.push(write),
                #[cfg(test)]
                Ok(Command::Run(job)) => {
                    after = Some(job);
                    break;
                }
                Err(_) => break,
            }
        }
        commit_batch(&mut conn, batch, clock.as_ref());
        #[cfg(test)]
        if let Some(job) = after {
            job(&conn);
        }
    }
}

fn commit_batch(conn: &mut Connection, batch: Vec<Write>, clock: &dyn Clock) {
    let (done, commit): (Vec<_>, _) =
        match conn.transaction_with_behavior(TransactionBehavior::Immediate) {
            Ok(mut tx) => {
                let done = batch
                    .into_iter()
                    .map(|write| execute(&mut tx, write, clock))
                    .collect();
                (done, tx.commit().map_err(backend))
            }
            Err(error) => (
                batch.into_iter().map(Done::skipped).collect(),
                Err(backend(error)),
            ),
        };
    for done in done {
        done.reply(&commit);
    }
}

fn execute(tx: &mut Transaction<'_>, write: Write, clock: &dyn Clock) -> Done {
    match write {
        Write::Append {
            entry,
            payload,
            reply,
        } => Done::Append(
            reply,
            in_savepoint(tx, |conn| append_in(conn, &entry, payload, clock)),
        ),
        Write::Retire {
            session,
            expected_frontier,
            reply,
        } => Done::Retire(
            reply,
            in_savepoint(tx, |conn| {
                retire_in(conn, session, expected_frontier, clock)
            }),
        ),
    }
}

/// A failed write rolls back only its own savepoint; the batch goes on.
fn in_savepoint<T, E: From<StorageError>>(
    tx: &mut Transaction<'_>,
    op: impl FnOnce(&Connection) -> Result<T, E>,
) -> Result<T, E> {
    let savepoint = tx.savepoint().map_err(backend)?;
    let value = op(&savepoint)?;
    savepoint.commit().map_err(backend)?;
    Ok(value)
}

fn append_in(
    tx: &Connection,
    entry: &NewEntry,
    payload: String,
    clock: &dyn Clock,
) -> Result<LogId, StorageError> {
    // Decode inside the transaction and before dedup: a bad payload is a
    // hard error, and the decoded event yields the `event_kind`
    // discriminator and the catalog row.
    let event = decode_payload(&entry.payload, entry.schema_version)
        .map_err(|error| StorageError::InvalidEntry(error.to_string()))?;
    let kind = event.kind().to_owned();

    let retired = tx
        .query_row(
            "SELECT 1 FROM retired_sessions WHERE session_id = ?1",
            [entry.session.as_bytes()],
            |_| Ok(()),
        )
        .optional()
        .map_err(backend)?;
    if retired.is_some() {
        return Err(StorageError::Retired {
            session: entry.session,
        });
    }

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
    let recorded_at = clock.now();
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
    if let SessionEvent::SessionCreated {
        parent, metadata, ..
    } = &event
    {
        let parent = parent
            .as_ref()
            .map(|parent| parent.session.as_bytes().to_vec());
        let metadata = serde_json::to_string(metadata).map_err(backend)?;
        tx.execute(
            "INSERT OR REPLACE INTO sessions (session_id, parent_id, created_at, metadata) \
                 VALUES (?1, ?2, ?3, ?4)",
            params![entry.session.as_bytes(), parent, recorded_at, metadata],
        )
        .map_err(backend)?;
    }
    Ok(log_id)
}

/// Session retirement; the check order is the contract in retention.md §5.
fn retire_in(
    tx: &Connection,
    session: SessionId,
    expected_frontier: LogId,
    clock: &dyn Clock,
) -> Result<(), RetirementError> {
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

    if !is_idle(tx, session)? {
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
        if !is_idle(tx, parent)? {
            return Err(RetirementError::ParentNotIdle);
        }
    }

    let retired_at = clock.now();
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
    Ok(())
}

impl SqliteStorage {
    fn reader(&self) -> MutexGuard<'_, Connection> {
        self.reader.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn send(&self, command: Command) -> Result<(), StorageError> {
        self.writer
            .commands
            .as_ref()
            .ok_or_else(writer_stopped)?
            .send(command)
            .map_err(|_| writer_stopped())
    }

    /// Test seam: run `job` on the writer thread between batches (inspect
    /// the write connection, park the writer, or crash it).
    #[cfg(test)]
    fn run_on_writer(
        &self,
        job: impl FnOnce(&Connection) + Send + 'static,
    ) -> Result<(), StorageError> {
        self.send(Command::Run(Box::new(job)))
    }

    fn scan_sync(&self, query: LogQuery) -> Vec<Result<LogEntry, StorageError>> {
        let conn = self.reader();
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
        let conn = self.reader();
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

    /// `last_activity` reads the last row through the `(session_id, seq)`
    /// unique index; no derived column (ADR-0028).
    fn sessions_sync(&self, query: SessionQuery) -> Result<Vec<CatalogEntry>, StorageError> {
        let (mode, of) = match query.parent {
            ParentFilter::Any => (0, None),
            ParentFilter::Roots => (1, None),
            ParentFilter::ChildrenOf(of) => (2, Some(of.as_bytes().to_vec())),
        };
        let after_created = query.after.map(|after| after.created_at);
        let after_session = query.after.map(|after| after.session.as_bytes().to_vec());
        let limit = query
            .limit
            .map_or(-1, |limit| i64::try_from(limit).unwrap_or(i64::MAX));
        let conn = self.reader();
        let mut statement = conn
            .prepare(
                "SELECT s.session_id, s.parent_id, s.created_at, \
                 COALESCE((SELECT l.recorded_at FROM log l WHERE l.session_id = s.session_id \
                           ORDER BY l.seq DESC LIMIT 1), s.created_at), s.metadata \
                 FROM sessions s \
                 WHERE s.session_id NOT IN (SELECT session_id FROM retired_sessions) \
                 AND (?1 = 0 OR (?1 = 1 AND s.parent_id IS NULL) OR (?1 = 2 AND s.parent_id = ?2)) \
                 AND (?3 IS NULL OR s.created_at < ?3 OR (s.created_at = ?3 AND s.session_id < ?4)) \
                 ORDER BY s.created_at DESC, s.session_id DESC \
                 LIMIT ?5",
            )
            .map_err(backend)?;
        let rows = statement
            .query_map(
                params![mode, of, after_created, after_session, limit],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, Option<Vec<u8>>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .map_err(backend)?;
        rows.map(|row| {
            let (session, parent, created_at, last_activity, metadata) = row.map_err(backend)?;
            let session = session_from_blob(session)?;
            Ok(CatalogEntry::new(
                session,
                parent.map(session_from_blob).transpose()?,
                created_at,
                last_activity,
                serde_json::from_str(&metadata).map_err(|error| StorageError::Corrupt {
                    session: Some(session),
                    detail: format!("catalog metadata: {error}"),
                })?,
            ))
        })
        .collect()
    }

    fn parent_sync(&self, session: SessionId) -> Result<Option<SessionId>, StorageError> {
        let conn = self.reader();
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
}

impl SessionStore for SqliteStorage {
    fn append(&self, entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>> {
        Box::pin(async move {
            let payload = serde_json::to_string(&entry.payload).map_err(backend)?;
            let (reply, response) = oneshot::channel();
            self.send(Command::Write(Write::Append {
                entry,
                payload,
                reply,
            }))?;
            response.await.unwrap_or_else(|_| Err(writer_stopped()))
        })
    }

    fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>> {
        stream::iter(self.scan_sync(query)).boxed()
    }

    fn frontier(&self, session: SessionId) -> BoxFuture<'_, Result<Option<LogId>, StorageError>> {
        Box::pin(async move { self.frontier_sync(session) })
    }

    fn sessions(
        &self,
        query: SessionQuery,
    ) -> BoxFuture<'_, Result<Vec<CatalogEntry>, StorageError>> {
        Box::pin(async move { self.sessions_sync(query) })
    }

    fn parent(&self, session: SessionId) -> BoxFuture<'_, Result<Option<SessionId>, StorageError>> {
        Box::pin(async move { self.parent_sync(session) })
    }

    fn retire(
        &self,
        session: SessionId,
        expected_frontier: LogId,
    ) -> BoxFuture<'_, Result<(), RetirementError>> {
        Box::pin(async move {
            let (reply, response) = oneshot::channel();
            self.send(Command::Write(Write::Retire {
                session,
                expected_frontier,
                reply,
            }))?;
            response
                .await
                .unwrap_or_else(|_| Err(writer_stopped().into()))
        })
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

/// Connection setup and schema migration every opener runs before serving.
fn prepare(conn: &Connection, synchronous: Synchronous) -> Result<(), StorageError> {
    configure(conn, synchronous)?;
    migrate(conn)?;
    // Setup above waits out other openers (rusqlite's default busy timeout);
    // serving never waits.
    conn.pragma_update(None, "busy_timeout", 0).map_err(backend)
}

fn configure(conn: &Connection, synchronous: Synchronous) -> Result<(), StorageError> {
    // Switching a fresh file to WAL can return BUSY without consulting the busy
    // handler (lock-upgrade deadlock between concurrent first openers), so retry.
    let mut attempts = 0;
    let mode: String = loop {
        match conn.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0)) {
            Err(rusqlite::Error::SqliteFailure(error, _))
                if error.code == rusqlite::ErrorCode::DatabaseBusy && attempts < 5_000 =>
            {
                attempts += 1;
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            result => break result.map_err(backend)?,
        }
    };
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

/// Concurrent first opens race here: the version is read under the write lock
/// (`BEGIN IMMEDIATE`) so exactly one opener builds the schema.
fn migrate(conn: &Connection) -> Result<(), StorageError> {
    let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate).map_err(backend)?;
    let version: i64 = tx
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(backend)?;
    if version > SUPPORTED_SCHEMA_VERSION {
        return Err(StorageError::SchemaTooNew {
            found: version,
            supported: SUPPORTED_SCHEMA_VERSION,
        });
    }
    // Pre-release: no DDL migrations; a database from an older layout is rebuilt.
    if version == 0 {
        tx.execute_batch(SCHEMA).map_err(backend)?;
        tx.pragma_update(None, "user_version", SUPPORTED_SCHEMA_VERSION)
            .map_err(backend)?;
    }
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
fn is_idle(tx: &Connection, session: SessionId) -> Result<bool, StorageError> {
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
            let (sender, pragmas) = std::sync::mpsc::channel();
            storage
                .run_on_writer(move |conn| {
                    let pragma = |name: &str| -> String {
                        conn.query_row(&format!("PRAGMA {name}"), [], |row| {
                            row.get::<_, rusqlite::types::Value>(0)
                        })
                        .map(|value| format!("{value:?}"))
                        .expect("pragma")
                    };
                    sender
                        .send((
                            pragma("journal_mode"),
                            pragma("synchronous"),
                            pragma("busy_timeout"),
                            conn.is_readonly(rusqlite::MAIN_DB).expect("readonly"),
                        ))
                        .expect("send pragmas");
                })
                .expect("run on writer");
            assert_eq!(
                pragmas.recv().expect("writer pragmas"),
                (
                    "Text(\"wal\")".to_owned(),
                    format!("Integer({expected_synchronous})"),
                    "Integer(0)".to_owned(),
                    false,
                )
            );
            assert!(
                storage
                    .reader()
                    .is_readonly(rusqlite::MAIN_DB)
                    .expect("readonly"),
                "reads use a read-only connection"
            );
            drop(storage);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn concurrent_first_opens_all_migrate_to_one_schema() {
        // The writer lock serializes `open` today; ADR-0036 removes it, so race the
        // connection setup and migration that every opener runs (#143).
        for round in 0..20 {
            let dir = std::env::temp_dir().join(format!(
                "asterpath-sqlite-migrate-race-{}-{round}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).expect("create temp dir");
            let path = dir.join("db.sqlite");
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
            let openers: Vec<_> = (0..8)
                .map(|_| {
                    let (path, barrier) = (path.clone(), barrier.clone());
                    std::thread::spawn(move || {
                        let conn = Connection::open(&path).expect("open connection");
                        barrier.wait();
                        prepare(&conn, Synchronous::Normal)?;
                        let schema: Vec<String> = conn
                            .prepare(
                                "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name",
                            )
                            .and_then(|mut statement| {
                                statement
                                    .query_map([], |row| row.get(0))?
                                    .collect::<Result<_, _>>()
                            })
                            .map_err(backend)?;
                        let version: i64 = conn
                            .query_row("PRAGMA user_version", [], |row| row.get(0))
                            .map_err(backend)?;
                        Ok::<_, StorageError>((version, schema))
                    })
                })
                .collect();
            let results: Vec<_> = openers
                .into_iter()
                .map(|opener| opener.join().expect("opener thread"))
                .collect();
            let first = results[0].as_ref().expect("first opener").clone();
            assert_eq!(first.0, SUPPORTED_SCHEMA_VERSION);
            for result in &results {
                assert_eq!(result.as_ref().expect("every opener succeeds"), &first);
            }
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    // --- writer seam: group commit with a savepoint per record (#97) -------

    use asterpath_protocol::LOG_SCHEMA_VERSION;
    use std::sync::mpsc;

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        futures_executor::block_on(future)
    }

    fn session(byte: u8) -> SessionId {
        SessionId::from_bytes([byte; 16])
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

    /// A fresh database under the temp dir; the directory is removed on drop.
    struct TempDb(PathBuf);

    impl TempDb {
        fn new(label: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "asterpath-sqlite-unit-{}-{label}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("create temp dir");
            Self(dir)
        }

        fn db(&self) -> PathBuf {
            self.0.join("test.sqlite")
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Run `appends` as one writer batch: the writer is parked until every
    /// append has been sent, so they are all queued when it drains.
    fn append_batch(
        storage: &SqliteStorage,
        appends: Vec<NewEntry>,
    ) -> Vec<Result<LogId, StorageError>> {
        let (release, held) = mpsc::channel::<()>();
        storage
            .run_on_writer(move |_| {
                let _ = held.recv();
            })
            .expect("park writer");
        let appends =
            futures_util::future::join_all(appends.into_iter().map(|entry| storage.append(entry)));
        // `join` polls `appends` first, so every command is queued before release.
        let (results, ()) = block_on(futures_util::future::join(
            appends,
            async move { drop(release) },
        ));
        results
    }

    #[test]
    fn a_bad_append_in_a_batch_fails_alone() {
        let dir = TempDb::new("batch-bad");
        let storage = open(dir.db()).expect("open");
        let bad = NewEntry {
            session: session(3),
            schema_version: LOG_SCHEMA_VERSION,
            dedup_key: None,
            payload: serde_json::json!({"NoSuchEvent": {}}),
        };
        let results = append_batch(
            &storage,
            vec![
                entry(session(3), &input_recorded(), None),
                bad,
                entry(session(3), &input_recorded(), None),
            ],
        );
        assert!(matches!(results[1], Err(StorageError::InvalidEntry(_))));
        let first = *results[0].as_ref().expect("first append");
        let third = *results[2].as_ref().expect("third append");

        let entries: Vec<_> = scan(storage.as_ref(), session(3))
            .into_iter()
            .map(|entry| entry.expect("scan"))
            .collect();
        let seqs: Vec<_> = entries.iter().map(|entry| entry.seq).collect();
        assert_eq!(seqs, [1, 2], "seq has no hole for the rejected append");
        let ids: Vec<_> = entries.iter().map(|entry| entry.log_id).collect();
        assert_eq!(ids, [first, third]);
    }

    #[test]
    fn duplicate_appends_in_a_batch_share_one_row() {
        let dir = TempDb::new("batch-dedup");
        let storage = open(dir.db()).expect("open");
        let results = append_batch(
            &storage,
            vec![
                entry(session(4), &input_recorded(), Some(DedupKey::new("k"))),
                entry(session(4), &input_recorded(), Some(DedupKey::new("k"))),
            ],
        );
        let first = results[0].as_ref().expect("first append");
        let second = results[1].as_ref().expect("second append");
        assert_eq!(first, second);
        assert_eq!(scan(storage.as_ref(), session(4)).len(), 1);
    }

    #[test]
    fn a_stopped_writer_fails_appends_instead_of_hanging() {
        let dir = TempDb::new("writer-stopped");
        let storage = open(dir.db()).expect("open");
        storage
            .run_on_writer(|_| panic!("simulated writer crash"))
            .expect("send to writer");
        for _ in 0..2 {
            let error = append(storage.as_ref(), entry(session(6), &input_recorded(), None))
                .expect_err("writer is gone");
            assert!(matches!(error, StorageError::Backend(_)), "{error:?}");
        }
    }
}
