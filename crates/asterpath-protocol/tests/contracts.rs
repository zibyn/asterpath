use asterpath_protocol::{
    ApprovalGate, CatalogEntry, Claim, Clock, DedupKey, LogEntry, LogId, LogQuery, NewEntry,
    OwnerToken, RetirementError, SessionId, SessionQuery, SessionStore, StorageError, SystemClock,
};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::stream;
use futures_util::{FutureExt, StreamExt};

struct DummyStore;

impl SessionStore for DummyStore {
    fn append(&self, _entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>> {
        Box::pin(async { Ok(LogId::from_i64(1)) })
    }

    fn scan(&self, _query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>> {
        stream::empty().boxed()
    }

    fn frontier(&self, _session: SessionId) -> BoxFuture<'_, Result<Option<LogId>, StorageError>> {
        Box::pin(async { Ok(None) })
    }

    fn sessions(
        &self,
        _query: SessionQuery,
    ) -> BoxFuture<'_, Result<Vec<CatalogEntry>, StorageError>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn parent(
        &self,
        _session: SessionId,
    ) -> BoxFuture<'_, Result<Option<SessionId>, StorageError>> {
        Box::pin(async { Ok(None) })
    }

    fn claim(&self, root: SessionId) -> BoxFuture<'_, Result<Claim, StorageError>> {
        Box::pin(async move { Ok(Claim::new(OwnerToken { root, epoch: 1 }, ())) })
    }

    fn owner(&self, _root: SessionId) -> BoxFuture<'_, Result<Option<OwnerToken>, StorageError>> {
        Box::pin(async { Ok(None) })
    }
}

#[test]
fn contract_traits_are_object_safe_and_values_have_the_documented_shape() {
    let _: &dyn SessionStore = &DummyStore;

    let session = SessionId::from_bytes([7; 16]);
    let _new = NewEntry {
        session,
        schema_version: 1,
        dedup_key: Some(DedupKey::new("dedup")),
        owner: None,
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
}

#[test]
fn retirement_defaults_to_unsupported() {
    let error = DummyStore
        .retire(SessionId::from_bytes([1; 16]), LogId::from_i64(1))
        .now_or_never()
        .expect("ready")
        .expect_err("default retire");
    assert!(matches!(error, RetirementError::Unsupported));
}

#[test]
fn system_clock_returns_epoch_milliseconds() {
    assert!(SystemClock.now() > 1_600_000_000_000);
}

#[test]
fn approval_gate_tracks_registered_endpoints_per_tree() {
    let (tree, other) = (
        SessionId::from_bytes([1; 16]),
        SessionId::from_bytes([2; 16]),
    );
    let gate = ApprovalGate::new();
    assert!(!gate.is_open(tree));

    let endpoint = gate.register(tree);
    assert!(gate.is_open(tree));
    assert!(!gate.is_open(other), "an endpoint attends only its tree");

    let second = gate.register(tree);
    drop(endpoint);
    assert!(gate.is_open(tree), "one endpoint still registered");
    drop(second);
    assert!(!gate.is_open(tree));
}
