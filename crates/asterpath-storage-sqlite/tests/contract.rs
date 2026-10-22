//! SQLite runs the same backend-neutral suites as the in-memory backend.

mod common;

use std::sync::Arc;

use asterpath_core::{EventLog, Storage};
use asterpath_storage_sqlite::{SqliteStorage, open};
use asterpath_testing::{BackendFactory, BackendHandle, event_log_contract, storage_contract};
use common::TempDir;

struct SqliteFactory {
    backend: Arc<SqliteStorage>,
}

impl BackendFactory for SqliteFactory {
    fn open(&self) -> BackendHandle {
        let log: Arc<dyn EventLog> = self.backend.clone();
        let storage: Arc<dyn Storage> = self.backend.clone();
        BackendHandle { log, storage }
    }
}

fn factory(dir: &TempDir) -> SqliteFactory {
    SqliteFactory {
        backend: open(dir.db()).expect("open sqlite backend"),
    }
}

#[test]
fn sqlite_satisfies_the_shared_event_log_contract() {
    let dir = TempDir::new("event-log-contract");
    event_log_contract(&factory(&dir));
}

#[test]
fn sqlite_satisfies_the_shared_storage_contract() {
    let dir = TempDir::new("storage-contract");
    storage_contract(&factory(&dir));
}
