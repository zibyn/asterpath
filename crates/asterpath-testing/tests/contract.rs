//! The in-memory backend runs the same backend-neutral suites as SQLite.

use asterpath_testing::{MemoryBackend, event_log_contract, storage_contract};

#[test]
fn in_memory_backend_satisfies_the_shared_event_log_contract() {
    event_log_contract(&MemoryBackend::new());
}

#[test]
fn in_memory_backend_satisfies_the_shared_storage_contract() {
    storage_contract(&MemoryBackend::new());
}
