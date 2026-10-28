//! The in-memory backend runs the same backend-neutral suites as SQLite.

use asterpath_protocol::{
    ConfigSnapshot, ModelId, ModelSpec, ParentRef, ProviderId, SessionEvent, SessionId,
    SessionStore, ToolCallId, TurnId, encode_entry, scan_all,
};
use asterpath_testing::{InMemoryEventLog, MemoryBackend, session_store_contract};

#[tokio::test]
async fn in_memory_backend_satisfies_the_shared_session_store_contract() {
    session_store_contract(&MemoryBackend::new()).await;
}

fn created(parent: Option<SessionId>) -> SessionEvent {
    SessionEvent::SessionCreated {
        config: ConfigSnapshot {
            system_sections: Vec::new(),
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
        metadata: Default::default(),
    }
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
