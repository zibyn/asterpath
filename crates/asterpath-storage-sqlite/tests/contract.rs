//! SQLite runs the same backend-neutral suite as the in-memory backend.

mod common;

use std::sync::Arc;

use asterpath_protocol::SessionStore;
use asterpath_storage_sqlite::{SqliteStorage, open};
use asterpath_testing::{StoreFactory, session_store_contract};
use common::TempDir;

struct SqliteFactory {
    backend: Arc<SqliteStorage>,
}

impl StoreFactory for SqliteFactory {
    fn open(&self) -> Arc<dyn SessionStore> {
        self.backend.clone()
    }
}

#[tokio::test]
async fn sqlite_satisfies_the_shared_session_store_contract() {
    let dir = TempDir::new("store-contract");
    let factory = SqliteFactory {
        backend: open(dir.db()).expect("open sqlite backend"),
    };
    session_store_contract(&factory).await;
}
