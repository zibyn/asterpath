//! Postgres runs the shared contract suite plus its own cases: concurrent
//! writers on one session, concurrent first opens, the `synchronous_commit`
//! check, payload text round trips and the schema version.
//!
//! Every test needs `ASTERPATH_TEST_POSTGRES_URL` (a `postgres://` URL whose
//! user may create schemas) and is skipped without it. Each store gets a
//! random schema through `search_path`, dropped afterwards.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};

use asterpath_protocol::{
    Clock, ConfigSnapshot, JsonString, LOG_SCHEMA_VERSION, LogEntry, LogId, LogQuery, ModelId,
    ModelSpec, NewEntry, OwnerToken, Provenance, ProviderId, ReplaySafety, SessionEvent, SessionId,
    SessionStore, StepId, StorageError, ToolCallId, ToolName, TurnId, dedup,
};
use asterpath_storage_postgres::deadpool_postgres::{Manager, ManagerConfig, Pool};
use asterpath_storage_postgres::{
    OpenOptions, PostgresStorage, SUPPORTED_SCHEMA_VERSION, from_pool, from_pool_with_clock,
};
use asterpath_testing::{ManualClock, StoreFactory, session_store_contract};
use futures_util::{FutureExt, StreamExt};
use tokio_postgres::NoTls;

const URL_VAR: &str = "ASTERPATH_TEST_POSTGRES_URL";

/// The test database, or `None` (with a note) when the URL is not set.
fn database() -> Option<Arc<Db>> {
    let Ok(url) = std::env::var(URL_VAR) else {
        eprintln!("skipped: set {URL_VAR} to a postgres:// URL to run the Postgres tests");
        return None;
    };
    let admin = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("admin runtime");
    Some(Arc::new(Db { url, admin }))
}

struct Db {
    url: String,
    /// Runs schema setup and raw checks, apart from the stores' runtimes.
    admin: tokio::runtime::Runtime,
}

impl Db {
    fn execute(&self, sql: &str) -> Vec<tokio_postgres::Row> {
        self.admin.block_on(async {
            let (client, connection) = tokio_postgres::connect(&self.url, NoTls)
                .await
                .expect("connect to the test database");
            let connection = tokio::spawn(connection);
            let rows = client.query(sql, &[]).await.expect(sql);
            drop(client);
            let _ = connection.await;
            rows
        })
    }

    fn schema(self: &Arc<Self>) -> Schema {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let name = format!(
            "asterpath_test_{}_{nanos}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        self.execute(&format!("CREATE SCHEMA {name}"));
        Schema {
            db: Arc::clone(self),
            name,
        }
    }

    /// A pool whose connections use `schema`, with extra server settings.
    fn pool(&self, schema: &Schema, settings: &str) -> Pool {
        let mut config: tokio_postgres::Config = self.url.parse().expect("parse the test URL");
        config.options(format!("-c search_path={} {settings}", schema.name));
        let manager = Manager::from_config(config, NoTls, ManagerConfig::default());
        Pool::builder(manager)
            .max_size(8)
            .build()
            .expect("build a pool")
    }

    fn open(&self, schema: &Schema, clock: Arc<dyn Clock>) -> Arc<PostgresStorage> {
        self.try_open(self.pool(schema, ""), OpenOptions::default(), clock)
            .expect("open the postgres store")
    }

    fn try_open(
        &self,
        pool: Pool,
        options: OpenOptions,
        clock: Arc<dyn Clock>,
    ) -> Result<Arc<PostgresStorage>, StorageError> {
        self.admin
            .block_on(from_pool_with_clock(pool, options, clock))
    }
}

/// A random schema, dropped with everything in it.
struct Schema {
    db: Arc<Db>,
    name: String,
}

impl Drop for Schema {
    fn drop(&mut self) {
        self.db
            .execute(&format!("DROP SCHEMA IF EXISTS {} CASCADE", self.name));
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    futures_executor::block_on(future)
}

struct PostgresFactory {
    db: Arc<Db>,
    main: Arc<PostgresStorage>,
    /// Fresh schemas handed out so far, dropped with the factory.
    schemas: Mutex<Vec<Schema>>,
}

impl PostgresFactory {
    fn fresh(&self) -> Schema {
        self.db.schema()
    }
}

impl StoreFactory for PostgresFactory {
    fn open(&self) -> Arc<dyn SessionStore> {
        self.main.clone()
    }

    fn open_with_clock(&self, clock: Arc<dyn Clock>) -> Arc<dyn SessionStore> {
        let schema = self.fresh();
        let store = self.db.open(&schema, clock);
        self.schemas.lock().unwrap().push(schema);
        store
    }

    fn open_processes(&self, clock: Arc<dyn Clock>) -> [Arc<dyn SessionStore>; 2] {
        let schema = self.fresh();
        // Two pools, as two processes would build.
        let first = self.db.open(&schema, clock.clone());
        let second = self.db.open(&schema, clock);
        self.schemas.lock().unwrap().push(schema);
        [first, second]
    }
}

#[test]
fn postgres_satisfies_the_shared_session_store_contract() {
    let Some(db) = database() else { return };
    let schema = db.schema();
    let factory = PostgresFactory {
        main: db.open(&schema, Arc::new(asterpath_protocol::SystemClock)),
        db,
        schemas: Mutex::default(),
    };
    block_on(session_store_contract(&factory));
    drop(factory);
}

fn session(byte: u8) -> SessionId {
    SessionId::from_bytes([byte; 16])
}

fn config() -> ConfigSnapshot {
    ConfigSnapshot {
        system_sections: Vec::new(),
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

fn entry(session: SessionId, event: &SessionEvent, owner: Option<OwnerToken>) -> NewEntry {
    NewEntry {
        session,
        schema_version: LOG_SCHEMA_VERSION,
        dedup_key: None,
        owner,
        payload: serde_json::to_value(event).expect("serialize event"),
    }
}

fn create(store: &dyn SessionStore, root: SessionId) {
    let created = SessionEvent::SessionCreated {
        config: config(),
        parent: None,
        metadata: Default::default(),
    };
    let mut created = entry(root, &created, None);
    created.dedup_key = Some(dedup::session_created(root));
    block_on(store.append(created)).expect("create the session");
}

fn input() -> SessionEvent {
    SessionEvent::InputRecorded { input: vec![] }
}

async fn scan_after(
    store: &dyn SessionStore,
    session: SessionId,
    after: Option<LogId>,
) -> Vec<LogEntry> {
    store
        .scan(LogQuery {
            session,
            after,
            limit: None,
        })
        .map(|entry| entry.expect("scan"))
        .collect()
        .await
}

#[test]
fn owner_and_external_writers_on_one_session_keep_a_stable_prefix() {
    let Some(db) = database() else { return };
    let schema = db.schema();
    let clock: Arc<dyn Clock> = Arc::new(asterpath_protocol::SystemClock);
    let owner = db.open(&schema, clock.clone());
    let external = db.open(&schema, clock);
    let root = session(0x51);
    create(&*owner, root);
    let claim = block_on(owner.claim(root)).expect("claim");
    let token = claim.token();
    const WRITES: usize = 60;
    let total = 2 * WRITES + 1;

    let writing = AtomicUsize::new(2);
    let finish = |results: Vec<Result<LogId, StorageError>>| {
        writing.fetch_sub(1, Ordering::SeqCst);
        results
    };
    let owner_writes = futures_util::future::join_all(
        (0..WRITES).map(|_| owner.append(entry(root, &input(), Some(token)))),
    )
    .map(finish);
    let external_writes = futures_util::future::join_all(
        (0..WRITES).map(|_| external.append(entry(root, &input(), None))),
    )
    .map(finish);
    // A reader following the session with a cursor while both write: every
    // page continues the last one, with no hole and no repeat.
    let reader = async {
        let mut seen: Vec<LogEntry> = Vec::new();
        loop {
            let done = writing.load(Ordering::SeqCst) == 0;
            let page = scan_after(&*external, root, seen.last().map(|entry| entry.log_id)).await;
            if done && page.is_empty() {
                break;
            }
            for entry in page {
                assert_eq!(entry.seq, seen.len() as u64 + 1, "seq continues the prefix");
                seen.push(entry);
            }
        }
        seen
    };
    let (owned, outside, followed) =
        block_on(async { futures_util::join!(owner_writes, external_writes, reader) });
    for result in owned.into_iter().chain(outside) {
        result.expect("append");
    }

    let all = block_on(scan_after(&*owner, root, None));
    assert_eq!(all, followed, "the follower saw the committed log");
    let seqs: Vec<u64> = all.iter().map(|entry| entry.seq).collect();
    assert_eq!(seqs, (1..=total as u64).collect::<Vec<_>>());
    assert!(
        all.windows(2).all(|pair| pair[0].log_id < pair[1].log_id),
        "log ids follow seq"
    );
    for (index, entry) in all.iter().enumerate() {
        let rest = block_on(scan_after(&*external, root, Some(entry.log_id)));
        assert_eq!(rest, all[index + 1..], "scan after {:?}", entry.log_id);
    }
    drop(claim);
}

#[test]
fn concurrent_first_opens_of_an_empty_schema_all_succeed() {
    let Some(db) = database() else { return };
    for _ in 0..5 {
        let schema = db.schema();
        let openers = 8;
        let barrier = Arc::new(Barrier::new(openers));
        let pools: Vec<Pool> = (0..openers).map(|_| db.pool(&schema, "")).collect();
        let opened: Vec<_> = pools
            .into_iter()
            .map(|pool| {
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    block_on(from_pool(pool, OpenOptions::default()))
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|opener| opener.join().expect("opener thread"))
            .collect();
        for store in &opened {
            assert!(store.is_ok(), "{:?}", store.as_ref().err());
        }
        let version = db.execute(&format!(
            "SELECT version FROM {}.schema_version",
            schema.name
        ));
        assert_eq!(version.len(), 1);
        assert_eq!(version[0].get::<_, i64>(0), SUPPORTED_SCHEMA_VERSION);
        drop(opened);
    }
}

#[test]
fn asynchronous_commit_is_refused_unless_allowed() {
    let Some(db) = database() else { return };
    let schema = db.schema();
    let clock: Arc<dyn Clock> = Arc::new(asterpath_protocol::SystemClock);
    let refused = db
        .try_open(
            db.pool(&schema, "-c synchronous_commit=off"),
            OpenOptions::default(),
            clock.clone(),
        )
        .err()
        .expect("synchronous_commit=off is refused by default");
    assert!(
        matches!(&refused, StorageError::Backend(message) if message.contains("synchronous_commit")),
        "{refused:?}"
    );
    db.try_open(
        db.pool(&schema, "-c synchronous_commit=off"),
        OpenOptions {
            allow_async_commit: true,
            ..OpenOptions::default()
        },
        clock,
    )
    .expect("allowed explicitly");
}

#[test]
fn payload_text_round_trips_byte_for_byte() {
    let Some(db) = database() else { return };
    let schema = db.schema();
    let store = db.open(&schema, Arc::new(asterpath_protocol::SystemClock));
    let raw_args = "{ \"z\" : 1,\n  \"a\": 123456789012345678901234567890.000, \"m\": [ ] }";
    let event = SessionEvent::ToolCallRequested {
        turn_id: TurnId::from_bytes([1; 16]),
        step_id: StepId::from_bytes([2; 16]),
        tool_call_id: ToolCallId::from_bytes([3; 16]),
        provider_call_id: "call".to_owned(),
        name: ToolName::new("echo"),
        raw_args: JsonString::new(raw_args),
        replay: ReplaySafety::Safe,
        provenance: Provenance::InProcess,
    };
    let appended = entry(session(0x61), &event, None);
    let text = serde_json::to_string(&appended.payload).expect("payload text");
    block_on(store.append(appended.clone())).expect("append");

    let stored = db.execute(&format!("SELECT payload FROM {}.log", schema.name));
    assert_eq!(
        stored[0].get::<_, String>(0),
        text,
        "the column keeps the text"
    );
    let read = block_on(scan_after(&*store, session(0x61), None));
    assert_eq!(read[0].payload, appended.payload);
    assert_eq!(
        serde_json::to_string(&read[0].payload).expect("payload text"),
        text
    );
    let SessionEvent::ToolCallRequested {
        raw_args: read_args,
        ..
    } = serde_json::from_value(read[0].payload.clone()).expect("decode")
    else {
        panic!("not a tool call");
    };
    assert_eq!(read_args.as_str(), raw_args);
}

#[test]
fn a_newer_schema_version_is_rejected() {
    let Some(db) = database() else { return };
    let schema = db.schema();
    drop(db.open(&schema, Arc::new(ManualClock::new())));
    db.execute(&format!(
        "UPDATE {}.schema_version SET version = 99",
        schema.name
    ));
    let refused = db
        .try_open(
            db.pool(&schema, ""),
            OpenOptions::default(),
            Arc::new(ManualClock::new()),
        )
        .err()
        .expect("a newer schema is refused");
    assert!(
        matches!(
            refused,
            StorageError::SchemaTooNew {
                found: 99,
                supported: SUPPORTED_SCHEMA_VERSION
            }
        ),
        "{refused:?}"
    );
}
