//! SQLite runs the same backend-neutral suite as the in-memory backend.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use asterpath_protocol::{Clock, SessionStore};
use asterpath_storage_sqlite::{OpenOptions, SqliteStorage, open, open_with_clock};
use asterpath_testing::{StoreFactory, session_store_contract};
use common::TempDir;

struct SqliteFactory {
    dir: TempDir,
    backend: Arc<SqliteStorage>,
    /// Files opened so far, so each fresh store gets its own.
    opened: AtomicUsize,
}

impl SqliteFactory {
    fn fresh(&self, clock: &Arc<dyn Clock>) -> (Arc<SqliteStorage>, std::path::PathBuf) {
        let n = self.opened.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.db().with_file_name(format!("fresh-{n}.sqlite"));
        let store = open_with_clock(&path, OpenOptions::default(), clock.clone())
            .expect("open a fresh sqlite backend");
        (store, path)
    }
}

impl StoreFactory for SqliteFactory {
    fn open(&self) -> Arc<dyn SessionStore> {
        self.backend.clone()
    }

    fn open_with_clock(&self, clock: Arc<dyn Clock>) -> Arc<dyn SessionStore> {
        self.fresh(&clock).0
    }

    fn open_processes(&self, clock: Arc<dyn Clock>) -> [Arc<dyn SessionStore>; 2] {
        let (first, path) = self.fresh(&clock);
        let second =
            open_with_clock(path, OpenOptions::default(), clock).expect("open the database again");
        [first, second]
    }
}

#[tokio::test]
async fn sqlite_satisfies_the_shared_session_store_contract() {
    let dir = TempDir::new("store-contract");
    let factory = SqliteFactory {
        backend: open(dir.db()).expect("open sqlite backend"),
        dir,
        opened: AtomicUsize::new(0),
    };
    session_store_contract(&factory).await;
}
