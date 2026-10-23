use asterpath_protocol::{
    ApprovalGate, AwaitSignal, Clock, DedupKey, EventLog, LogEntry, LogId, LogQuery, NewEntry,
    RetirementError, SessionCatalog, SessionId, SessionRetirement, SignalId, SignalName, Storage,
    StorageError, SystemClock, Timestamp,
};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use futures_util::stream;

struct DummyLog;

impl EventLog for DummyLog {
    fn append(&self, _entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>> {
        Box::pin(async { Ok(LogId::from_i64(1)) })
    }

    fn scan(&self, _query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>> {
        stream::empty().boxed()
    }

    fn frontier(&self, _session: SessionId) -> BoxFuture<'_, Result<Option<LogId>, StorageError>> {
        Box::pin(async { Ok(None) })
    }
}

struct DummyStorage;

impl Storage for DummyStorage {
    fn wakeable(&self, _now: Timestamp) -> BoxFuture<'_, Result<Vec<SessionId>, StorageError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

struct DummyCatalog;

impl SessionCatalog for DummyCatalog {
    fn parent(
        &self,
        _session: SessionId,
    ) -> BoxFuture<'_, Result<Option<SessionId>, StorageError>> {
        Box::pin(async { Ok(None) })
    }

    fn children(&self, _session: SessionId) -> BoxStream<'_, Result<SessionId, StorageError>> {
        stream::empty().boxed()
    }
}

struct DummyRetirement;

impl SessionRetirement for DummyRetirement {
    fn retire(
        &self,
        _session: SessionId,
        _expected_frontier: LogId,
    ) -> BoxFuture<'_, Result<(), RetirementError>> {
        Box::pin(async { Ok(()) })
    }
}

#[test]
fn contract_traits_are_object_safe_and_values_have_the_documented_shape() {
    let _: &dyn EventLog = &DummyLog;
    let _: &dyn Storage = &DummyStorage;
    let _: &dyn SessionCatalog = &DummyCatalog;
    let _: &dyn SessionRetirement = &DummyRetirement;

    let session = SessionId::from_bytes([7; 16]);
    let _new = NewEntry {
        session,
        schema_version: 1,
        dedup_key: Some(DedupKey::new("dedup")),
        payload: serde_json::json!({ "SessionCreated": {} }),
    };
    let _entry = LogEntry {
        log_id: LogId::from_i64(3),
        session,
        seq: 1,
        recorded_at: 0,
        schema_version: 1,
        dedup_key: None,
        payload: serde_json::Value::Null,
    };
    let _query = LogQuery {
        session,
        after: Some(LogId::from_i64(2)),
        limit: Some(10),
    };
    let _await = AwaitSignal {
        session,
        signal: SignalId::from_bytes([3; 16]),
        name: SignalName::new("tool.approval.c1"),
    };
}

#[test]
fn system_clock_returns_epoch_milliseconds() {
    assert!(SystemClock.now() > 1_600_000_000_000);
}

#[test]
fn approval_gate_tracks_registered_endpoints() {
    let gate = ApprovalGate::new();
    assert!(!gate.is_open());

    let endpoint = gate.register();
    assert!(gate.is_open());

    let second = gate.register();
    drop(endpoint);
    assert!(gate.is_open(), "one endpoint still registered");
    drop(second);
    assert!(!gate.is_open());
}
