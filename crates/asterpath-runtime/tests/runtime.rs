//! Durable timer and signal behaviour through the public `DurableExecutor` seam.

use std::sync::Arc;
use std::time::Duration;

use std::sync::atomic::{AtomicBool, Ordering};

use asterpath_core::{
    AwaitSignal, DURABLE_EXECUTOR_SERVICE, DurableExecutor, EventLog, ExecutionError,
    LifecycleError, LogQuery, Module, ModuleCtx, ModuleHost, ServiceId, SessionId, SignalDelivery,
    SignalId, SignalIntent, SignalName, Storage, TimerId, TimerRequest, ToolCallId, WakeCause,
};
use asterpath_protocol::{SessionEvent, decode_entry};
use asterpath_runtime::{RuntimeExecutor, RuntimeModule};
use asterpath_testing::{InMemoryEventLog, InMemoryStorage, ManualClock};
use futures_util::StreamExt;

const SESSION: SessionId = SessionId::from_bytes([7; 16]);
const CAUSED_BY: asterpath_core::LogId = asterpath_core::LogId::from_i64(1);

struct Fixture {
    log: Arc<InMemoryEventLog>,
    executor: Arc<RuntimeExecutor>,
    clock: Arc<ManualClock>,
}

fn fixture() -> Fixture {
    let log = Arc::new(InMemoryEventLog::new());
    let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new(Arc::clone(&log)));
    let clock = Arc::new(ManualClock::new());
    let executor = Arc::new(RuntimeExecutor::new(log.clone(), storage, clock.clone()));
    Fixture {
        log,
        executor,
        clock,
    }
}

fn timer_request(slot: u8, deadline: i64) -> TimerRequest {
    TimerRequest {
        session: SESSION,
        timer: TimerId::from_bytes([slot; 16]),
        caused_by: CAUSED_BY,
        deadline,
    }
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
async fn timer_ids_are_deterministic_and_registration_is_idempotent() {
    let fixture = fixture();
    let request = timer_request(1, ManualClock::BASE + 50);

    let first = fixture
        .executor
        .schedule_timer(timer_request(1, ManualClock::BASE + 50))
        .await
        .expect("schedule");
    let second = fixture
        .executor
        .schedule_timer(request)
        .await
        .expect("schedule");

    assert_eq!(first, second);
    assert_eq!(count(&events(&fixture.log).await, "TimerScheduled"), 1);
}

#[tokio::test]
async fn distinct_slots_get_distinct_timer_ids() {
    let fixture = fixture();
    let a = fixture
        .executor
        .schedule_timer(timer_request(1, ManualClock::BASE + 50))
        .await
        .expect("schedule");
    let b = fixture
        .executor
        .schedule_timer(timer_request(2, ManualClock::BASE + 50))
        .await
        .expect("schedule");
    assert_ne!(a, b);
}

#[tokio::test]
async fn timer_due_while_down_fires_exactly_once() {
    let fixture = fixture();
    let timer = fixture
        .executor
        .schedule_timer(timer_request(1, ManualClock::BASE + 100))
        .await
        .expect("schedule");

    // A new executor instance over the same log models a restarted process.
    let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new(Arc::clone(&fixture.log)));
    let restarted = Arc::new(RuntimeExecutor::new(
        fixture.log.clone(),
        storage,
        fixture.clock.clone(),
    ));

    fixture.clock.advance(1_000);
    restarted
        .await_wake(SESSION, WakeCause::Timer(timer))
        .await
        .expect("due timer fires");
    restarted
        .await_wake(SESSION, WakeCause::Timer(timer))
        .await
        .expect("already fired");

    assert_eq!(count(&events(&fixture.log).await, "TimerFired"), 1);
    assert_eq!(count(&events(&fixture.log).await, "TimerCancelled"), 0);
}

#[tokio::test]
async fn cancelled_timer_never_fires_and_is_not_resurrected() {
    let fixture = fixture();
    let timer = fixture
        .executor
        .schedule_timer(timer_request(1, ManualClock::BASE + 100))
        .await
        .expect("schedule");

    // Cancel while a waiter is parked: the waiter observes the cancellation.
    let waiting = Arc::clone(&fixture.executor);
    let handle =
        tokio::spawn(async move { waiting.await_wake(SESSION, WakeCause::Timer(timer)).await });
    tokio::time::sleep(Duration::from_millis(10)).await;

    fixture
        .executor
        .cancel_timer(SESSION, timer)
        .await
        .expect("cancel");
    assert!(matches!(
        handle.await.expect("join"),
        Err(ExecutionError::Cancelled)
    ));

    fixture.clock.advance(10_000);
    assert!(matches!(
        fixture
            .executor
            .await_wake(SESSION, WakeCause::Timer(timer))
            .await,
        Err(ExecutionError::Cancelled)
    ));

    // Re-registering the same slot must not resurrect the timer.
    let again = fixture
        .executor
        .schedule_timer(timer_request(1, ManualClock::BASE + 100))
        .await
        .expect("schedule");
    assert_eq!(again, timer);
    assert_eq!(count(&events(&fixture.log).await, "TimerFired"), 0);
    assert_eq!(count(&events(&fixture.log).await, "TimerCancelled"), 1);
    assert_eq!(count(&events(&fixture.log).await, "TimerScheduled"), 1);
}

#[tokio::test]
async fn generic_signal_await_records_the_intent() {
    let fixture = fixture();
    let name = SignalName::new("child.done");

    let waiting = Arc::clone(&fixture.executor);
    let awaited_name = name.clone();
    let handle = tokio::spawn(async move {
        waiting
            .await_signal(AwaitSignal {
                session: SESSION,
                signal: SignalId::from_bytes([0; 16]),
                name: awaited_name,
                caused_by: CAUSED_BY,
                intent: SignalIntent::Record,
            })
            .await
    });

    wait_for(&fixture.log, "SignalAwaited").await;
    fixture
        .executor
        .deliver_signal(SignalDelivery {
            session: SESSION,
            signal: SignalId::from_bytes([0; 16]),
            name,
            payload: serde_json::json!({"ok": true}),
        })
        .await
        .expect("deliver");

    let payload = handle.await.expect("join").expect("signal");
    assert_eq!(payload, serde_json::json!({"ok": true}));
    assert_eq!(count(&events(&fixture.log).await, "SignalAwaited"), 1);
    assert_eq!(count(&events(&fixture.log).await, "SignalReceived"), 1);
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
                caused_by: CAUSED_BY,
                intent: SignalIntent::AlreadyRecorded,
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
    assert_eq!(count(&events(&fixture.log).await, "SignalAwaited"), 0);
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
    let name = SignalName::new("child.done");
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
        caused_by: CAUSED_BY,
        intent: SignalIntent::Record,
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
    assert_eq!(count(&state, "SignalAwaited"), 0);
}

#[tokio::test]
async fn fire_and_cancel_race_records_only_one_terminal() {
    let fixture = fixture();
    let timer = fixture
        .executor
        .schedule_timer(timer_request(1, ManualClock::BASE))
        .await
        .expect("schedule");

    let firing = Arc::clone(&fixture.executor);
    let cancelling = Arc::clone(&fixture.executor);
    let (_, _) = tokio::join!(
        tokio::spawn(async move { firing.await_wake(SESSION, WakeCause::Timer(timer)).await }),
        tokio::spawn(async move { cancelling.cancel_timer(SESSION, timer).await })
    );

    let state = events(&fixture.log).await;
    let terminals = count(&state, "TimerFired") + count(&state, "TimerCancelled");
    assert_eq!(terminals, 1);
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
                assert!(
                    executor
                        .await_wake(SESSION, WakeCause::Startup)
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

/// Blocks until an event of `kind` is in the log, or fails after a second.
async fn wait_for(log: &InMemoryEventLog, kind: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    loop {
        if events(log).await.iter().any(|event| event.kind() == kind) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {kind}"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}
