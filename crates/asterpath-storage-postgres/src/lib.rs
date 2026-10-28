//! Postgres backend: `SessionStore` over a connection pool the application
//! builds (TLS, auth, pool size and `search_path` are the application's).
//!
//! Postgres is persistence only (ADR-0036): reads, writes and conditional
//! writes; no advisory lock, no `LISTEN/NOTIFY`, nothing tied to a
//! connection's lifetime. Each `append` is one read-committed transaction
//! that locks the session's head row before it allocates `seq`, so within a
//! session `seq`, `log_id` and commit order agree. Design notes:
//! docs/design/storage-postgres.md.
//!
//! The store runs every statement on a runtime of its own, so a claim's
//! `Drop` and the handle's `Drop` can wait for their release to commit
//! without needing the caller's runtime. Claims, releases and renewals go
//! through one queue, in order; appends, retirements and reads run
//! concurrently.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak, mpsc};
use std::time::Duration;

use asterpath_protocol::{
    CatalogEntry, Claim, Clock, DEFAULT_LEASE_TTL, DedupKey, LogEntry, LogId, LogQuery, NewEntry,
    OwnerToken, ParentFilter, RetirementError, SessionEvent, SessionFacts, SessionId, SessionQuery,
    SessionStore, StorageError, SystemClock, Timestamp, decode_payload,
};
use deadpool_postgres::Pool;
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, stream};
use tokio::runtime::Runtime;
use tokio::sync::{mpsc as queue, oneshot};
use tokio_postgres::GenericClient;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::ToSql;

/// The pool type `from_pool` takes, re-exported so applications build it
/// with the same version.
pub use deadpool_postgres;

/// Physical schema version in the `schema_version` table.
pub const SUPPORTED_SCHEMA_VERSION: i64 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenOptions {
    /// Open even when the connection's `synchronous_commit` is `off`, so a
    /// returned append may be lost if the server crashes. Tests and
    /// development only, like SQLite's `Synchronous::Normal`.
    pub allow_async_commit: bool,
    /// How long a renewal of this process's lease lasts; a killed process
    /// keeps its trees this long. Renew well within it (ADR-0036).
    pub lease_ttl: Duration,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            allow_async_commit: false,
            lease_ttl: DEFAULT_LEASE_TTL,
        }
    }
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS session_heads (
    session_id  bytea   PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS sessions (
    session_id  bytea   PRIMARY KEY,
    parent_id   bytea,
    created_at  bigint  NOT NULL,
    metadata    text    NOT NULL
);
CREATE TABLE IF NOT EXISTS log (
    log_id          bigint  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id      bytea   NOT NULL,
    seq             bigint  NOT NULL,
    recorded_at     bigint  NOT NULL,
    schema_version  integer NOT NULL,
    event_kind      text    NOT NULL,
    dedup_key       text,
    payload         text    NOT NULL,
    UNIQUE (session_id, seq),
    UNIQUE (session_id, event_kind, dedup_key)
);
CREATE TABLE IF NOT EXISTS retired_sessions (
    session_id   bytea   PRIMARY KEY,
    parent_id    bytea,
    last_log_id  bigint  NOT NULL,
    retired_at   bigint  NOT NULL
);
CREATE TABLE IF NOT EXISTS tree_owners (
    root     bytea   PRIMARY KEY,
    epoch    bigint  NOT NULL,
    process  bytea
);
CREATE TABLE IF NOT EXISTS process_leases (
    process     bytea   PRIMARY KEY,
    expires_at  bigint  NOT NULL
);
";

/// Open the store on `pool` with the production defaults: create the tables
/// if missing, check the schema version and `synchronous_commit`.
pub async fn from_pool(
    pool: Pool,
    options: OpenOptions,
) -> Result<Arc<PostgresStorage>, StorageError> {
    from_pool_with_clock(pool, options, Arc::new(SystemClock)).await
}

/// Contract-test seam: a fixed clock makes `created_at` ties and lease
/// expiry deterministic.
#[doc(hidden)]
pub async fn from_pool_with_clock(
    pool: Pool,
    options: OpenOptions,
    clock: Arc<dyn Clock>,
) -> Result<Arc<PostgresStorage>, StorageError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("asterpath-postgres")
        .enable_all()
        .build()
        .map_err(backend)?;
    let prepared = runtime.spawn({
        let pool = pool.clone();
        async move { prepare(&pool, options).await }
    });
    let process = match prepared.await.map_err(backend).and_then(|result| result) {
        Ok(process) => process,
        Err(error) => {
            // Dropping a runtime inside the caller's async context panics.
            runtime.shutdown_background();
            return Err(error);
        }
    };
    let lease = Lease {
        process,
        ttl: i64::try_from(options.lease_ttl.as_millis()).unwrap_or(i64::MAX),
    };
    let (commands, received) = queue::unbounded_channel();
    let (exited, exit) = mpsc::channel();
    runtime.spawn(run_control(
        pool.clone(),
        received,
        Arc::clone(&clock),
        lease,
        exited,
    ));
    Ok(Arc::new_cyclic(|me| PostgresStorage {
        runtime: Some(runtime),
        commands: Some(commands),
        exit: Mutex::new(exit),
        pool,
        me: me.clone(),
        lease,
        clock,
        held: Mutex::default(),
    }))
}

pub struct PostgresStorage {
    runtime: Option<Runtime>,
    commands: Option<queue::UnboundedSender<Control>>,
    /// Disconnects once the control task has freed this process's trees.
    exit: Mutex<mpsc::Receiver<()>>,
    pool: Pool,
    /// For claims, which release through the store if it is still open.
    me: Weak<PostgresStorage>,
    lease: Lease,
    clock: Arc<dyn Clock>,
    /// Root -> (epoch, live claims) of the trees held through this handle.
    held: Mutex<HashMap<SessionId, (u64, usize)>>,
}

impl Drop for PostgresStorage {
    fn drop(&mut self) {
        // Closing the queue ends the control task once it has drained it and
        // freed this process's trees (a normal exit).
        drop(self.commands.take());
        let _ = lock(&self.exit).recv();
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

/// This handle's process: its id in `process_leases` and how long, in
/// milliseconds, a renewal lasts.
#[derive(Clone, Copy)]
struct Lease {
    process: [u8; 16],
    ttl: i64,
}

enum Control {
    Claim {
        root: SessionId,
        reply: oneshot::Sender<Result<(u64, bool), StorageError>>,
    },
    /// `done` drops once the release is committed.
    Release {
        token: OwnerToken,
        done: mpsc::Sender<()>,
    },
    Renew {
        held: Vec<OwnerToken>,
        reply: oneshot::Sender<Result<Vec<OwnerToken>, StorageError>>,
    },
}

async fn run_control(
    pool: Pool,
    mut commands: queue::UnboundedReceiver<Control>,
    clock: Arc<dyn Clock>,
    lease: Lease,
    exited: mpsc::Sender<()>,
) {
    while let Some(command) = commands.recv().await {
        match command {
            Control::Claim { root, reply } => {
                let claimed = claim_in(&pool, root, lease, clock.now()).await;
                // A caller that gave up does not keep a tree it just took.
                if let Err(Ok((epoch, true))) = reply.send(claimed) {
                    let _ = release_in(&pool, OwnerToken { root, epoch }, lease).await;
                }
            }
            Control::Release { token, done } => {
                // A failed release leaves the tree to the lease's expiry.
                let _ = release_in(&pool, token, lease).await;
                drop(done);
            }
            Control::Renew { held, reply } => {
                let _ = reply.send(renew_in(&pool, &held, lease, clock.now()).await);
            }
        }
    }
    // Nobody is left to report a failure to (the handle is dropping); the
    // lease then expires after one TTL, as for a killed process.
    let _ = exit_in(&pool, lease).await;
    drop(exited);
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn stopped() -> StorageError {
    StorageError::Backend("postgres store stopped".to_owned())
}

fn backend(error: impl std::fmt::Display) -> StorageError {
    StorageError::Backend(error.to_string())
}

/// The server's message when there is one; `tokio_postgres::Error` alone
/// prints only "db error".
fn describe(error: &tokio_postgres::Error) -> String {
    error
        .as_db_error()
        .map_or_else(|| error.to_string(), ToString::to_string)
}

/// Serialization failures, deadlocks and lock timeouts are conflicts worth
/// retrying; anything else is a real failure.
fn sql(error: tokio_postgres::Error) -> StorageError {
    match error.code() {
        Some(
            &SqlState::T_R_SERIALIZATION_FAILURE
            | &SqlState::T_R_DEADLOCK_DETECTED
            | &SqlState::LOCK_NOT_AVAILABLE,
        ) => StorageError::Conflict(describe(&error)),
        _ => StorageError::Backend(describe(&error)),
    }
}

/// `sql`, plus the catalog collisions of two first openers creating one
/// table at once.
fn ddl(error: tokio_postgres::Error) -> StorageError {
    match error.code() {
        Some(
            &SqlState::UNIQUE_VIOLATION | &SqlState::DUPLICATE_TABLE | &SqlState::DUPLICATE_OBJECT,
        ) => StorageError::Conflict(describe(&error)),
        _ => sql(error),
    }
}

/// Retries of a conflicting control transaction, 1 ms apart.
const CONFLICT_RETRIES: usize = 1_000;

/// Retry a whole transaction while it conflicts. Appends and retirements
/// are not retried here: their `Conflict` goes back to the caller.
async fn retry<T, F, Fut>(mut op: F) -> Result<T, StorageError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StorageError>>,
{
    let mut attempts = 0;
    loop {
        match op().await {
            Err(error) if error.is_retryable() && attempts < CONFLICT_RETRIES => {
                attempts += 1;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            result => return result,
        }
    }
}

// shortcut: every statement is prepared per call (one extra round trip each);
// switch to `prepare_cached` when append latency matters.
async fn connect(pool: &Pool) -> Result<deadpool_postgres::Object, StorageError> {
    pool.get().await.map_err(backend)
}

/// Durability check, schema creation and this process's random id.
async fn prepare(pool: &Pool, options: OpenOptions) -> Result<[u8; 16], StorageError> {
    let client = connect(pool).await?;
    if !options.allow_async_commit {
        let row = client
            .query_one("SHOW synchronous_commit", &[])
            .await
            .map_err(sql)?;
        if row.get::<_, String>(0) == "off" {
            return Err(StorageError::Backend(
                "synchronous_commit is off: a returned append could be lost; \
                 turn it on or set OpenOptions::allow_async_commit"
                    .to_owned(),
            ));
        }
    }
    drop(client);
    retry(|| migrate(pool)).await?;
    let client = connect(pool).await?;
    let process: Vec<u8> = client
        .query_one("SELECT uuid_send(gen_random_uuid())", &[])
        .await
        .map_err(sql)?
        .get(0);
    process
        .try_into()
        .map_err(|_| backend("gen_random_uuid() is not 16 bytes"))
}

/// One transaction; concurrent first openers collide in the catalog and
/// retry until the tables exist.
async fn migrate(pool: &Pool) -> Result<(), StorageError> {
    let mut client = connect(pool).await?;
    let tx = client.transaction().await.map_err(sql)?;
    tx.batch_execute(
        "CREATE TABLE IF NOT EXISTS schema_version (
            singleton  boolean  PRIMARY KEY DEFAULT true CHECK (singleton),
            version    bigint   NOT NULL
        )",
    )
    .await
    .map_err(ddl)?;
    let version: Option<i64> = tx
        .query_opt("SELECT version FROM schema_version", &[])
        .await
        .map_err(ddl)?
        .map(|row| row.get(0));
    if let Some(found) = version
        && found > SUPPORTED_SCHEMA_VERSION
    {
        return Err(StorageError::SchemaTooNew {
            found,
            supported: SUPPORTED_SCHEMA_VERSION,
        });
    }
    tx.batch_execute(SCHEMA).await.map_err(ddl)?;
    tx.execute(
        "INSERT INTO schema_version (version) VALUES ($1) ON CONFLICT DO NOTHING",
        &[&SUPPORTED_SCHEMA_VERSION],
    )
    .await
    .map_err(ddl)?;
    tx.commit().await.map_err(ddl)
}

/// Lock `session`'s head row until the transaction ends, creating it first
/// if missing; appends and retirements of one session queue here.
async fn lock_head(tx: &impl GenericClient, session: SessionId) -> Result<(), StorageError> {
    tx.execute(
        "INSERT INTO session_heads (session_id) VALUES ($1) ON CONFLICT DO NOTHING",
        &[&session.as_bytes().as_slice()],
    )
    .await
    .map_err(sql)?;
    tx.execute(
        "SELECT 1 FROM session_heads WHERE session_id = $1 FOR UPDATE",
        &[&session.as_bytes().as_slice()],
    )
    .await
    .map_err(sql)?;
    Ok(())
}

async fn is_retired(tx: &impl GenericClient, session: SessionId) -> Result<bool, StorageError> {
    Ok(tx
        .query_opt(
            "SELECT 1 FROM retired_sessions WHERE session_id = $1",
            &[&session.as_bytes().as_slice()],
        )
        .await
        .map_err(sql)?
        .is_some())
}

async fn append_in(
    pool: &Pool,
    entry: NewEntry,
    clock: &dyn Clock,
    lease: Lease,
) -> Result<LogId, StorageError> {
    // A bad payload is a hard error; the decoded event yields the
    // `event_kind` discriminator and the catalog row.
    let event = decode_payload(&entry.payload, entry.schema_version)
        .map_err(|error| StorageError::InvalidEntry(error.to_string()))?;
    let kind = event.kind();
    let payload = serde_json::to_string(&entry.payload).map_err(backend)?;
    let session = entry.session.as_bytes().as_slice();

    let mut client = connect(pool).await?;
    let tx = client.transaction().await.map_err(sql)?;
    lock_head(&*tx, entry.session).await?;
    // A child's creation and its parent's retirement exclude each other.
    if let SessionEvent::SessionCreated {
        parent: Some(parent),
        ..
    } = &event
    {
        lock_head(&*tx, parent.session).await?;
    }
    if is_retired(&*tx, entry.session).await? {
        return Err(StorageError::Retired {
            session: entry.session,
        });
    }
    // The fence: an owner write lands only under the claim this process
    // still holds, and the share lock keeps a takeover out until commit.
    if let Some(owner) = entry.owner
        && !holds(&*tx, owner, lease, " FOR SHARE").await?
    {
        return Err(StorageError::OwnedElsewhere { root: owner.root });
    }

    let seq: i64 = tx
        .query_one(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM log WHERE session_id = $1",
            &[&session],
        )
        .await
        .map_err(sql)?
        .get(0);
    let recorded_at = clock.now();
    let dedup_key = entry.dedup_key.as_ref().map(DedupKey::as_str);
    let inserted = tx
        .query_opt(
            "INSERT INTO log \
             (session_id, seq, recorded_at, schema_version, event_kind, dedup_key, payload) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (session_id, event_kind, dedup_key) DO NOTHING RETURNING log_id",
            &[
                &session,
                &seq,
                &recorded_at,
                &i32::from(entry.schema_version),
                &kind,
                &dedup_key,
                &payload,
            ],
        )
        .await
        .map_err(sql)?;
    let Some(inserted) = inserted else {
        // A duplicate: the id of the record it repeats; nothing is written.
        let existing = tx
            .query_one(
                "SELECT log_id FROM log \
                 WHERE session_id = $1 AND event_kind = $2 AND dedup_key = $3",
                &[&session, &kind, &dedup_key],
            )
            .await
            .map_err(sql)?;
        return Ok(LogId::from_i64(existing.get(0)));
    };

    match &event {
        SessionEvent::MetadataChanged { set, remove } => {
            let current = tx
                .query_opt(
                    "SELECT metadata FROM sessions WHERE session_id = $1",
                    &[&session],
                )
                .await
                .map_err(sql)?;
            let Some(current) = current else {
                return Err(StorageError::InvalidEntry(
                    "MetadataChanged for a session without SessionCreated".to_owned(),
                ));
            };
            let merged = asterpath_protocol::patched_metadata(
                catalog_metadata(entry.session, current.get(0))?,
                set,
                remove,
            )?;
            tx.execute(
                "UPDATE sessions SET metadata = $2 WHERE session_id = $1",
                &[&session, &serde_json::to_string(&merged).map_err(backend)?],
            )
            .await
            .map_err(sql)?;
        }
        SessionEvent::SessionCreated {
            parent, metadata, ..
        } => {
            let parent = parent
                .as_ref()
                .map(|parent| parent.session.as_bytes().to_vec());
            tx.execute(
                "INSERT INTO sessions (session_id, parent_id, created_at, metadata) \
                 VALUES ($1, $2, $3, $4) ON CONFLICT (session_id) DO UPDATE SET \
                 parent_id = excluded.parent_id, created_at = excluded.created_at, \
                 metadata = excluded.metadata",
                &[
                    &session,
                    &parent,
                    &recorded_at,
                    &serde_json::to_string(metadata).map_err(backend)?,
                ],
            )
            .await
            .map_err(sql)?;
        }
        _ => {}
    }
    tx.commit().await.map_err(sql)?;
    Ok(LogId::from_i64(inserted.get(0)))
}

/// Session retirement; the check order is the contract in retention.md §5.
async fn retire_in(
    pool: &Pool,
    session: SessionId,
    expected_frontier: LogId,
    clock: &dyn Clock,
) -> Result<(), RetirementError> {
    let id = session.as_bytes().as_slice();
    let mut client = connect(pool).await?;
    let tx = client.transaction().await.map_err(sql)?;
    // No head row: the session never had a record, so it is not found below.
    tx.execute(
        "SELECT 1 FROM session_heads WHERE session_id = $1 FOR UPDATE",
        &[&id],
    )
    .await
    .map_err(sql)?;

    // An existing tombstone is the truth: the log is already gone.
    let tombstone = tx
        .query_opt(
            "SELECT last_log_id FROM retired_sessions WHERE session_id = $1",
            &[&id],
        )
        .await
        .map_err(sql)?;
    if let Some(tombstone) = tombstone {
        let last_log_id: i64 = tombstone.get(0);
        return if last_log_id == expected_frontier.as_i64() {
            Ok(())
        } else {
            Err(RetirementError::FrontierMoved {
                expected: expected_frontier,
                actual: Some(LogId::from_i64(last_log_id)),
            })
        };
    }

    let Some(row) = tx
        .query_opt(
            "SELECT parent_id FROM sessions WHERE session_id = $1",
            &[&id],
        )
        .await
        .map_err(sql)?
    else {
        return Err(RetirementError::NotFound);
    };
    let parent: Option<Vec<u8>> = row.get(0);

    if !is_idle(&*tx, session).await? {
        return Err(RetirementError::NotIdle);
    }

    let actual: Option<i64> = tx
        .query_one("SELECT MAX(log_id) FROM log WHERE session_id = $1", &[&id])
        .await
        .map_err(sql)?
        .get(0);
    if actual != Some(expected_frontier.as_i64()) {
        return Err(RetirementError::FrontierMoved {
            expected: expected_frontier,
            actual: actual.map(LogId::from_i64),
        });
    }

    // Live children: a sessions row whose parent is this session and that
    // has not itself been retired. A child's creation holds this head row.
    let children: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM sessions s WHERE s.parent_id = $1 \
             AND NOT EXISTS \
                 (SELECT 1 FROM retired_sessions r WHERE r.session_id = s.session_id))",
            &[&id],
        )
        .await
        .map_err(sql)?
        .get(0);
    if children {
        return Err(RetirementError::HasChildren);
    }

    if let Some(parent) = parent.clone() {
        let parent = session_from_bytes(parent)?;
        lock_head(&*tx, parent).await?;
        if !is_idle(&*tx, parent).await? {
            return Err(RetirementError::ParentNotIdle);
        }
    }

    tx.execute(
        "INSERT INTO retired_sessions (session_id, parent_id, last_log_id, retired_at) \
         VALUES ($1, $2, $3, $4)",
        &[&id, &parent, &expected_frontier.as_i64(), &clock.now()],
    )
    .await
    .map_err(sql)?;
    // The head row stays: appends queued on it then find the tombstone.
    tx.execute("DELETE FROM log WHERE session_id = $1", &[&id])
        .await
        .map_err(sql)?;
    tx.execute("DELETE FROM sessions WHERE session_id = $1", &[&id])
        .await
        .map_err(sql)?;
    tx.commit().await.map_err(sql)?;
    Ok(())
}

const ENTRY_COLUMNS: &str =
    "log_id, session_id, seq, recorded_at, schema_version, dedup_key, payload";

/// Session facts over the session's committed log, read inside `tx`
/// (storage.md §5): idle is the absence of a non-compaction open intent.
async fn is_idle(tx: &impl GenericClient, session: SessionId) -> Result<bool, StorageError> {
    let rows = tx
        .query(
            &format!("SELECT {ENTRY_COLUMNS} FROM log WHERE session_id = $1 ORDER BY seq ASC"),
            &[&session.as_bytes().as_slice()],
        )
        .await
        .map_err(sql)?;
    let entries = rows
        .iter()
        .map(|row| to_entry(session, row))
        .collect::<Result<Vec<_>, _>>()?;
    let facts = SessionFacts::fold(&entries).map_err(|error| StorageError::Corrupt {
        session: Some(session),
        detail: error.to_string(),
    })?;
    Ok(facts.is_idle())
}

fn to_entry(session: SessionId, row: &tokio_postgres::Row) -> Result<LogEntry, StorageError> {
    let corrupt = |detail: String| StorageError::Corrupt {
        session: Some(session),
        detail,
    };
    let get = |error: tokio_postgres::Error| corrupt(describe(&error));
    if session_from_bytes(row.try_get(1).map_err(get)?)? != session {
        return Err(corrupt(
            "log row session does not match its query".to_owned(),
        ));
    }
    let seq: i64 = row.try_get(2).map_err(get)?;
    let schema_version: i32 = row.try_get(4).map_err(get)?;
    let payload: &str = row.try_get(6).map_err(get)?;
    Ok(LogEntry {
        log_id: LogId::from_i64(row.try_get(0).map_err(get)?),
        session,
        seq: u64::try_from(seq).map_err(|_| corrupt(format!("negative seq {seq}")))?,
        recorded_at: row.try_get(3).map_err(get)?,
        schema_version: u16::try_from(schema_version)
            .map_err(|_| corrupt(format!("schema version {schema_version} out of range")))?,
        dedup_key: row
            .try_get::<_, Option<String>>(5)
            .map_err(get)?
            .map(DedupKey::new),
        payload: serde_json::from_str(payload).map_err(|error| corrupt(error.to_string()))?,
    })
}

fn session_from_bytes(bytes: Vec<u8>) -> Result<SessionId, StorageError> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| StorageError::Corrupt {
        session: None,
        detail: "session id must be 16 bytes".to_owned(),
    })?;
    Ok(SessionId::from_bytes(bytes))
}

fn catalog_metadata(
    session: SessionId,
    metadata: &str,
) -> Result<std::collections::BTreeMap<String, String>, StorageError> {
    serde_json::from_str(metadata).map_err(|error| StorageError::Corrupt {
        session: Some(session),
        detail: format!("catalog metadata: {error}"),
    })
}

async fn scan_in(pool: &Pool, query: LogQuery) -> Result<Vec<LogEntry>, StorageError> {
    let client = connect(pool).await?;
    let limit = query
        .limit
        .map(|limit| i64::try_from(limit).unwrap_or(i64::MAX));
    let rows = client
        .query(
            &format!(
                "SELECT {ENTRY_COLUMNS} FROM log WHERE session_id = $1 AND log_id > $2 \
                 AND NOT EXISTS (SELECT 1 FROM retired_sessions WHERE session_id = $1) \
                 ORDER BY seq ASC LIMIT $3"
            ),
            &[
                &query.session.as_bytes().as_slice(),
                &query.after.map(LogId::as_i64).unwrap_or(0),
                &limit,
            ],
        )
        .await
        .map_err(sql)?;
    rows.iter()
        .map(|row| to_entry(query.session, row))
        .collect()
}

async fn frontier_in(pool: &Pool, session: SessionId) -> Result<Option<LogId>, StorageError> {
    let client = connect(pool).await?;
    let frontier: Option<i64> = client
        .query_one(
            "SELECT MAX(log_id) FROM log WHERE session_id = $1 \
             AND NOT EXISTS (SELECT 1 FROM retired_sessions WHERE session_id = $1)",
            &[&session.as_bytes().as_slice()],
        )
        .await
        .map_err(sql)?
        .get(0);
    Ok(frontier.map(LogId::from_i64))
}

/// `last_activity` reads the last row through the `(session_id, seq)`
/// unique index; no derived column (ADR-0028).
async fn sessions_in(pool: &Pool, query: SessionQuery) -> Result<Vec<CatalogEntry>, StorageError> {
    let (mode, of) = match query.parent {
        ParentFilter::Any => (0_i32, None),
        ParentFilter::Roots => (1, None),
        ParentFilter::ChildrenOf(of) => (2, Some(of.as_bytes().to_vec())),
    };
    let after_created = query.after.map(|after| after.created_at);
    let after_session = query.after.map(|after| after.session.as_bytes().to_vec());
    let limit = query
        .limit
        .map(|limit| i64::try_from(limit).unwrap_or(i64::MAX));
    let metadata = serde_json::to_string(&query.metadata).map_err(backend)?;
    let params: [&(dyn ToSql + Sync); 6] = [
        &mode,
        &of,
        &after_created,
        &after_session,
        &limit,
        &metadata,
    ];
    let client = connect(pool).await?;
    let rows = client
        .query(
            "SELECT s.session_id, s.parent_id, s.created_at, \
             COALESCE((SELECT l.recorded_at FROM log l WHERE l.session_id = s.session_id \
                       ORDER BY l.seq DESC LIMIT 1), s.created_at), s.metadata \
             FROM sessions s \
             WHERE NOT EXISTS (SELECT 1 FROM retired_sessions r WHERE r.session_id = s.session_id) \
             AND ($1::int4 = 0 OR ($1 = 1 AND s.parent_id IS NULL) \
                  OR ($1 = 2 AND s.parent_id = $2::bytea)) \
             AND ($3::int8 IS NULL OR s.created_at < $3 \
                  OR (s.created_at = $3 AND s.session_id < $4::bytea)) \
             AND s.metadata::jsonb @> $6::text::jsonb \
             ORDER BY s.created_at DESC, s.session_id DESC \
             LIMIT $5::int8",
            &params,
        )
        .await
        .map_err(sql)?;
    rows.iter()
        .map(|row| {
            let session = session_from_bytes(row.get(0))?;
            Ok(CatalogEntry::new(
                session,
                row.get::<_, Option<Vec<u8>>>(1)
                    .map(session_from_bytes)
                    .transpose()?,
                row.get(2),
                row.get(3),
                catalog_metadata(session, row.get(4))?,
            ))
        })
        .collect()
}

async fn parent_in(pool: &Pool, session: SessionId) -> Result<Option<SessionId>, StorageError> {
    let client = connect(pool).await?;
    let parent = client
        .query_opt(
            "SELECT parent_id FROM sessions WHERE session_id = $1 \
             AND NOT EXISTS (SELECT 1 FROM retired_sessions WHERE session_id = $1)",
            &[&session.as_bytes().as_slice()],
        )
        .await
        .map_err(sql)?;
    parent
        .and_then(|row| row.get::<_, Option<Vec<u8>>>(0))
        .map(session_from_bytes)
        .transpose()
}

/// `(epoch, process, its lease expiry)` of `root`'s owner row; `lock`
/// appends a locking clause.
type OwnerRow = (u64, Option<Vec<u8>>, Option<Timestamp>);

async fn owner_row(
    client: &impl GenericClient,
    root: SessionId,
    lock: &str,
) -> Result<Option<OwnerRow>, StorageError> {
    let row = client
        .query_opt(
            &format!(
                "SELECT o.epoch, o.process, l.expires_at FROM tree_owners o \
                 LEFT JOIN process_leases l ON l.process = o.process WHERE o.root = $1{lock}"
            ),
            &[&root.as_bytes().as_slice()],
        )
        .await
        .map_err(sql)?;
    Ok(row.map(|row| (row.get::<_, i64>(0) as u64, row.get(1), row.get(2))))
}

/// Whether this process still holds `owner`'s epoch; `lock` appends a
/// locking clause.
async fn holds(
    client: &impl GenericClient,
    owner: OwnerToken,
    lease: Lease,
    lock: &str,
) -> Result<bool, StorageError> {
    Ok(client
        .query_opt(
            &format!(
                "SELECT 1 FROM tree_owners WHERE root = $1 AND epoch = $2 AND process = $3{lock}"
            ),
            &[
                &owner.root.as_bytes().as_slice(),
                &(owner.epoch as i64),
                &lease.process.as_slice(),
            ],
        )
        .await
        .map_err(sql)?
        .is_some())
}

async fn renew_lease(
    client: &impl GenericClient,
    lease: Lease,
    now: Timestamp,
) -> Result<(), StorageError> {
    client
        .execute(
            "INSERT INTO process_leases (process, expires_at) VALUES ($1, $2) \
             ON CONFLICT (process) DO UPDATE SET expires_at = excluded.expires_at",
            &[&lease.process.as_slice(), &now.saturating_add(lease.ttl)],
        )
        .await
        .map_err(sql)?;
    Ok(())
}

/// Renew the lease and take `root` unless another process with a live
/// lease holds it: the epoch, and whether this claim took the tree rather
/// than sharing this process's claim. The owner row is locked, so two
/// claimers of one root take turns.
async fn claim_in(
    pool: &Pool,
    root: SessionId,
    lease: Lease,
    now: Timestamp,
) -> Result<(u64, bool), StorageError> {
    retry(|| async move {
        let mut client = connect(pool).await?;
        let tx = client.transaction().await.map_err(sql)?;
        renew_lease(&*tx, lease, now).await?;
        let id = root.as_bytes().as_slice();
        let claimed = match owner_row(&*tx, root, " FOR UPDATE OF o").await? {
            Some((epoch, Some(process), _)) if process == lease.process => (epoch, false),
            Some((_, Some(_), Some(until))) if now < until => {
                return Err(StorageError::OwnedElsewhere { root });
            }
            Some(_) => {
                let epoch: i64 = tx
                    .query_one(
                        "UPDATE tree_owners SET epoch = epoch + 1, process = $2 \
                         WHERE root = $1 RETURNING epoch",
                        &[&id, &lease.process.as_slice()],
                    )
                    .await
                    .map_err(sql)?
                    .get(0);
                (epoch as u64, true)
            }
            None => {
                let inserted = tx
                    .query_opt(
                        "INSERT INTO tree_owners (root, epoch, process) VALUES ($1, 1, $2) \
                         ON CONFLICT (root) DO NOTHING RETURNING epoch",
                        &[&id, &lease.process.as_slice()],
                    )
                    .await
                    .map_err(sql)?;
                let Some(inserted) = inserted else {
                    return Err(StorageError::Conflict(
                        "another process claimed the root first".to_owned(),
                    ));
                };
                (inserted.get::<_, i64>(0) as u64, true)
            }
        };
        tx.commit().await.map_err(sql)?;
        Ok(claimed)
    })
    .await
}

/// Free `token`'s tree unless another process has taken it since.
async fn release_in(pool: &Pool, token: OwnerToken, lease: Lease) -> Result<(), StorageError> {
    retry(|| async move {
        connect(pool)
            .await?
            .execute(
                "UPDATE tree_owners SET process = NULL \
                 WHERE root = $1 AND epoch = $2 AND process = $3",
                &[
                    &token.root.as_bytes().as_slice(),
                    &(token.epoch as i64),
                    &lease.process.as_slice(),
                ],
            )
            .await
            .map_err(sql)
    })
    .await?;
    Ok(())
}

/// Renew the lease; the claims in `held` this process no longer holds.
async fn renew_in(
    pool: &Pool,
    held: &[OwnerToken],
    lease: Lease,
    now: Timestamp,
) -> Result<Vec<OwnerToken>, StorageError> {
    retry(|| async move {
        let mut client = connect(pool).await?;
        let tx = client.transaction().await.map_err(sql)?;
        renew_lease(&*tx, lease, now).await?;
        // Leases of killed processes would otherwise stay forever; an
        // expired lease already owns nothing.
        tx.execute("DELETE FROM process_leases WHERE expires_at <= $1", &[&now])
            .await
            .map_err(sql)?;
        let mut lost = Vec::new();
        for token in held {
            if !holds(&*tx, *token, lease, "").await? {
                lost.push(*token);
            }
        }
        tx.commit().await.map_err(sql)?;
        Ok(lost)
    })
    .await
}

/// A normal exit: drop the lease and free every tree this process holds.
async fn exit_in(pool: &Pool, lease: Lease) -> Result<(), StorageError> {
    retry(|| async move {
        let mut client = connect(pool).await?;
        let tx = client.transaction().await.map_err(sql)?;
        tx.execute(
            "DELETE FROM process_leases WHERE process = $1",
            &[&lease.process.as_slice()],
        )
        .await
        .map_err(sql)?;
        tx.execute(
            "UPDATE tree_owners SET process = NULL WHERE process = $1",
            &[&lease.process.as_slice()],
        )
        .await
        .map_err(sql)?;
        tx.commit().await.map_err(sql)
    })
    .await
}

impl PostgresStorage {
    /// Run `op` on the store's runtime and wait for it.
    async fn run<T, E>(
        &self,
        op: impl FnOnce(Pool, Arc<dyn Clock>) -> BoxFuture<'static, Result<T, E>>,
    ) -> Result<T, E>
    where
        T: Send + 'static,
        E: From<StorageError> + Send + 'static,
    {
        let runtime = self.runtime.as_ref().ok_or_else(stopped)?;
        let task = runtime.spawn(op(self.pool.clone(), Arc::clone(&self.clock)));
        task.await.map_err(|_| stopped())?
    }

    fn send(&self, command: Control) -> Result<(), StorageError> {
        self.commands
            .as_ref()
            .ok_or_else(stopped)?
            .send(command)
            .map_err(|_| stopped())
    }

    fn held(&self) -> MutexGuard<'_, HashMap<SessionId, (u64, usize)>> {
        lock(&self.held)
    }

    fn claim_of(&self, token: OwnerToken) -> Claim {
        let release = Release {
            store: self.me.clone(),
            token,
        };
        Claim::new(token, release)
    }
}

impl SessionStore for PostgresStorage {
    fn append(&self, entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>> {
        let lease = self.lease;
        Box::pin(self.run(move |pool, clock| {
            Box::pin(async move { append_in(&pool, entry, clock.as_ref(), lease).await })
        }))
    }

    fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>> {
        let entries = self.run(|pool, _| Box::pin(async move { scan_in(&pool, query).await }));
        stream::once(entries)
            .flat_map(|entries| match entries {
                Ok(entries) => stream::iter(entries.into_iter().map(Ok)).boxed(),
                Err(error) => stream::iter([Err(error)]).boxed(),
            })
            .boxed()
    }

    fn frontier(&self, session: SessionId) -> BoxFuture<'_, Result<Option<LogId>, StorageError>> {
        Box::pin(
            self.run(move |pool, _| Box::pin(async move { frontier_in(&pool, session).await })),
        )
    }

    fn sessions(
        &self,
        query: SessionQuery,
    ) -> BoxFuture<'_, Result<Vec<CatalogEntry>, StorageError>> {
        Box::pin(self.run(move |pool, _| Box::pin(async move { sessions_in(&pool, query).await })))
    }

    fn parent(&self, session: SessionId) -> BoxFuture<'_, Result<Option<SessionId>, StorageError>> {
        Box::pin(self.run(move |pool, _| Box::pin(async move { parent_in(&pool, session).await })))
    }

    fn claim(&self, root: SessionId) -> BoxFuture<'_, Result<Claim, StorageError>> {
        Box::pin(async move {
            let response = {
                let mut held = self.held();
                if let Some((epoch, claims)) = held.get_mut(&root) {
                    *claims += 1;
                    let epoch = *epoch;
                    return Ok(self.claim_of(OwnerToken { root, epoch }));
                }
                let (reply, response) = oneshot::channel();
                // Sent under the lock, so a release of this root queued
                // before it commits first.
                self.send(Control::Claim { root, reply })?;
                response
            };
            let (epoch, _) = response.await.unwrap_or_else(|_| Err(stopped()))?;
            let mut held = self.held();
            let entry = held.entry(root).or_insert((epoch, 0));
            if entry.0 != epoch {
                *entry = (epoch, 0);
            }
            entry.1 += 1;
            drop(held);
            Ok(self.claim_of(OwnerToken { root, epoch }))
        })
    }

    fn owner(&self, root: SessionId) -> BoxFuture<'_, Result<Option<OwnerToken>, StorageError>> {
        let process = self.lease.process;
        Box::pin(self.run(move |pool, clock| {
            Box::pin(async move {
                let row = owner_row(&**connect(&pool).await?, root, "").await?;
                let now = clock.now();
                Ok(match row {
                    Some((epoch, Some(owner), until))
                        if owner == process || until.is_some_and(|until| now < until) =>
                    {
                        Some(OwnerToken { root, epoch })
                    }
                    _ => None,
                })
            })
        }))
    }

    fn renew(&self) -> BoxFuture<'_, Result<Vec<OwnerToken>, StorageError>> {
        Box::pin(async move {
            let held = self
                .held()
                .iter()
                .map(|(root, (epoch, _))| OwnerToken {
                    root: *root,
                    epoch: *epoch,
                })
                .collect();
            let (reply, response) = oneshot::channel();
            self.send(Control::Renew { held, reply })?;
            let lost = response.await.unwrap_or_else(|_| Err(stopped()))?;
            let mut held = self.held();
            for token in &lost {
                if held
                    .get(&token.root)
                    .is_some_and(|(epoch, _)| *epoch == token.epoch)
                {
                    held.remove(&token.root);
                }
            }
            Ok(lost)
        })
    }

    fn retire(
        &self,
        session: SessionId,
        expected_frontier: LogId,
    ) -> BoxFuture<'_, Result<(), RetirementError>> {
        Box::pin(self.run(move |pool, clock| {
            Box::pin(
                async move { retire_in(&pool, session, expected_frontier, clock.as_ref()).await },
            )
        }))
    }
}

/// Drops one claim; the last one of its epoch releases the tree, committed
/// before `drop` returns so another process may claim it at once. A store
/// already closed has freed the tree on exit.
struct Release {
    store: Weak<PostgresStorage>,
    token: OwnerToken,
}

impl Drop for Release {
    fn drop(&mut self) {
        let Some(store) = self.store.upgrade() else {
            return;
        };
        let committed = {
            let mut held = store.held();
            match held.get_mut(&self.token.root) {
                Some((epoch, claims)) if *epoch == self.token.epoch => {
                    *claims -= 1;
                    if *claims > 0 {
                        return;
                    }
                }
                _ => return,
            }
            held.remove(&self.token.root);
            let (done, committed) = mpsc::channel();
            let release = Control::Release {
                token: self.token,
                done,
            };
            if store.send(release).is_err() {
                return;
            }
            committed
        };
        let _ = committed.recv();
    }
}
