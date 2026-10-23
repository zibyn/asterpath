//! Approval signal and wake behaviour through the public `DurableExecutor` seam.

use std::sync::Arc;
use std::time::Duration;

use std::sync::atomic::{AtomicBool, Ordering};

use asterpath_core::{
    AwaitSignal, DURABLE_EXECUTOR_SERVICE, DurableExecutor, EventLog, LifecycleError, LogQuery,
    Module, ModuleCtx, ModuleHost, ServiceId, SessionId, SignalDelivery, SignalId, SignalName,
    ToolCallId, WakeCause,
};
use asterpath_protocol::{SessionEvent, decode_entry};
use asterpath_runtime::{RuntimeExecutor, RuntimeModule};
use asterpath_testing::InMemoryEventLog;
use futures_util::StreamExt;

const SESSION: SessionId = SessionId::from_bytes([7; 16]);

struct Fixture {
    log: Arc<InMemoryEventLog>,
    executor: Arc<RuntimeExecutor>,
}

fn fixture() -> Fixture {
    let log = Arc::new(InMemoryEventLog::new());
    let executor = Arc::new(RuntimeExecutor::new(log.clone()));
    Fixture { log, executor }
}

async fn events(log: &InMemoryEventLog) -> Vec<SessionEvent> {
    let mut stream = log.scan(LogQuery {
        session: SESSION,
        after: None,
        limit: None,
    });
    let mut entries = Vec::new();
    while let Some(entry) = stream.next().await {
        entries.push(entry.expect("scan"));
    }
    entries
        .iter()
        .map(|entry| decode_entry(entry).expect("decodes"))
        .collect()
}

fn count(events: &[SessionEvent], kind: &str) -> usize {
    events.iter().filter(|event| event.kind() == kind).count()
}

#[tokio::test]
async fn approval_signal_uses_tool_call_id_and_writes_no_intent() {
    let fixture = fixture();
    let tool_call = [3u8; 16];
    let name = SignalName::new(format!(
        "tool.approval.{}",
        ToolCallId::from_bytes(tool_call).to_hex()
    ));

    let waiting = Arc::clone(&fixture.executor);
    let awaited_name = name.clone();
    let handle = tokio::spawn(async move {
        waiting
            .await_signal(AwaitSignal {
                session: SESSION,
                signal: SignalId::from_bytes(tool_call),
                name: awaited_name,
            })
            .await
    });

    tokio::time::sleep(Duration::from_millis(10)).await;
    fixture
        .executor
        .deliver_signal(SignalDelivery {
            session: SESSION,
            signal: SignalId::from_bytes(tool_call),
            name,
            payload: serde_json::json!("Allow"),
        })
        .await
        .expect("deliver");

    let payload = handle.await.expect("join").expect("signal");
    assert_eq!(payload, serde_json::json!("Allow"));
    let received = events(&fixture.log)
        .await
        .into_iter()
        .find_map(|event| match event {
            SessionEvent::SignalReceived { signal_id, .. } => Some(signal_id),
            _ => None,
        })
        .expect("received");
    assert_eq!(received, SignalId::from_bytes(tool_call));
}

#[tokio::test]
async fn duplicate_delivery_and_repeat_waits_collapse() {
    let fixture = fixture();
    let name = SignalName::new("tool.approval.00");
    let delivery = SignalDelivery {
        session: SESSION,
        signal: SignalId::from_bytes([0; 16]),
        name: name.clone(),
        payload: serde_json::json!({"n": 1}),
    };
    fixture
        .executor
        .deliver_signal(delivery.clone())
        .await
        .expect("deliver");
    // The second delivery must not write a second record.
    fixture
        .executor
        .deliver_signal(SignalDelivery {
            payload: serde_json::json!({"n": 2}),
            ..delivery
        })
        .await
        .expect("duplicate deliver");

    let request = AwaitSignal {
        session: SESSION,
        signal: SignalId::from_bytes([0; 16]),
        name,
    };
    let first = fixture
        .executor
        .await_signal(request.clone())
        .await
        .expect("first wait");
    let second = fixture
        .executor
        .await_signal(request)
        .await
        .expect("second wait");

    assert_eq!(first, serde_json::json!({"n": 1}));
    assert_eq!(second, first);
    let state = events(&fixture.log).await;
    assert_eq!(count(&state, "SignalReceived"), 1);
}

#[tokio::test]
async fn module_provides_the_executor_through_the_host() {
    let fixture = fixture();
    let seen = Arc::new(AtomicBool::new(false));

    struct Probe(Arc<AtomicBool>);

    impl Module for Probe {
        fn name(&self) -> &'static str {
            "probe"
        }

        fn dependencies(&self) -> &[ServiceId] {
            static IDS: [ServiceId; 1] = [DURABLE_EXECUTOR_SERVICE.id()];
            &IDS
        }

        fn init<'a>(
            &'a self,
            ctx: &'a ModuleCtx,
        ) -> futures_core::future::BoxFuture<'a, Result<(), LifecycleError>> {
            let seen = Arc::clone(&self.0);
            Box::pin(async move {
                let executor = ctx.require(DURABLE_EXECUTOR_SERVICE)?;
                // The service is the real durable executor.
                let signal = SignalId::from_bytes([5; 16]);
                executor
                    .deliver_signal(SignalDelivery {
                        session: SESSION,
                        signal,
                        name: SignalName::new("tool.approval.05"),
                        payload: serde_json::json!("Allow"),
                    })
                    .await
                    .expect("deliver");
                assert!(
                    executor
                        .await_wake(SESSION, WakeCause::Signal(signal))
                        .await
                        .is_ok()
                );
                seen.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    let mut host = ModuleHost::new();
    host.register(Arc::new(RuntimeModule::new(Arc::clone(&fixture.executor))));
    host.register(Arc::new(Probe(Arc::clone(&seen))));
    host.init().await.expect("host init");
    assert!(seen.load(Ordering::SeqCst));
    host.shutdown().await;
}
