//! SQLite runs the same backend-neutral suite as the in-memory backend.

mod common;

use std::sync::Arc;

use asterpath_protocol::{Clock, SessionStore};
use asterpath_storage_sqlite::{OpenOptions, SqliteStorage, open, open_with_clock};
use asterpath_testing::{StoreFactory, session_store_contract};
use common::TempDir;

struct SqliteFactory {
    dir: TempDir,
    backend: Arc<SqliteStorage>,
}

impl StoreFactory for SqliteFactory {
    fn open(&self) -> Arc<dyn SessionStore> {
        self.backend.clone()
    }

    fn open_with_clock(&self, clock: Arc<dyn Clock>) -> Arc<dyn SessionStore> {
        let path = self.dir.db().with_file_name("clocked.sqlite");
        open_with_clock(path, OpenOptions::default(), clock).expect("open clocked sqlite backend")
    }

    fn open_other_process(&self) -> Arc<dyn SessionStore> {
        open(self.dir.db()).expect("open the database again")
    }
}

#[tokio::test]
async fn sqlite_satisfies_the_shared_session_store_contract() {
    let dir = TempDir::new("store-contract");
    let factory = SqliteFactory {
        backend: open(dir.db()).expect("open sqlite backend"),
        dir,
    };
    session_store_contract(&factory).await;
}
