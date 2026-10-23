//! The in-memory backend runs the same backend-neutral suites as SQLite.

use asterpath_protocol::{
    ConfigSnapshot, EventLog, ModelId, ModelSpec, ParentRef, ProviderId, SessionCatalog,
    SessionEvent, SessionId, ToolCallId, TurnId, encode_entry, scan_all,
};
use asterpath_testing::{InMemoryEventLog, MemoryBackend, event_log_contract, storage_contract};
use futures_util::StreamExt;

#[test]
fn in_memory_backend_satisfies_the_shared_event_log_contract() {
    event_log_contract(&MemoryBackend::new());
}

#[test]
fn in_memory_backend_satisfies_the_shared_storage_contract() {
    storage_contract(&MemoryBackend::new());
}

fn created(parent: Option<SessionId>) -> SessionEvent {
    SessionEvent::SessionCreated {
        config: ConfigSnapshot {
            model: ModelSpec {
                provider: ProviderId::new("p"),
                model: ModelId::new("m"),
                context_window: None,
            },
            system: vec![],
            tools: vec![],
            request: Default::default(),
            turn: Default::default(),
            compaction: Default::default(),
            subagents: Default::default(),
        },
        parent: parent.map(|session| ParentRef {
            session,
            turn: TurnId::from_bytes([9; 16]),
            tool_call: ToolCallId::from_bytes([8; 16]),
        }),
    }
}

#[tokio::test]
async fn in_memory_log_is_a_session_catalog_over_session_created_parents() {
    let root = SessionId::from_bytes([1; 16]);
    let first = SessionId::from_bytes([3; 16]);
    let second = SessionId::from_bytes([2; 16]);
    let log = InMemoryEventLog::new();
    for (session, parent) in [(root, None), (first, Some(root)), (second, Some(root))] {
        log.append(encode_entry(session, None, &created(parent)).unwrap())
            .await
            .unwrap();
    }

    assert_eq!(log.parent(root).await.unwrap(), None);
    assert_eq!(log.parent(first).await.unwrap(), Some(root));
    assert_eq!(
        log.parent(SessionId::from_bytes([7; 16])).await.unwrap(),
        None
    );
    let children: Vec<SessionId> = log
        .children(root)
        .map(|child| child.unwrap())
        .collect()
        .await;
    assert_eq!(
        children,
        vec![second, first],
        "ordered by session id, like SQLite"
    );
    assert!(log.children(first).next().await.is_none());
}

#[tokio::test]
async fn scan_all_reads_one_whole_session_in_seq_order() {
    let root = SessionId::from_bytes([1; 16]);
    let other = SessionId::from_bytes([2; 16]);
    let log = InMemoryEventLog::new();
    let first = log
        .append(encode_entry(root, None, &created(None)).unwrap())
        .await
        .unwrap();
    log.append(encode_entry(other, None, &created(Some(root))).unwrap())
        .await
        .unwrap();
    let input = SessionEvent::InputRecorded { input: vec![] };
    let second = log
        .append(encode_entry(root, None, &input).unwrap())
        .await
        .unwrap();

    let entries = scan_all(&log, root).await.unwrap();
    let ids: Vec<_> = entries.iter().map(|entry| entry.log_id).collect();
    assert_eq!(ids, vec![first, second]);
    assert!(
        scan_all(&log, SessionId::from_bytes([9; 16]))
            .await
            .unwrap()
            .is_empty()
    );
}
