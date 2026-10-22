//! The in-memory backend runs the same backend-neutral suite as SQLite will.

use asterpath_testing::{MemoryBackend, event_log_contract};

#[test]
fn in_memory_backend_satisfies_the_shared_event_log_contract() {
    event_log_contract(&MemoryBackend::new());
}
