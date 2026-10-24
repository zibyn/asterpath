//! Concurrency smoke benchmark for the SQLite backend (#86): how much do N
//! sessions appending and reading at once stall the tokio workers?
//!
//! Run (release, prints a table; ignored by default `cargo test`). Point
//! TMPDIR at a real disk: on tmpfs fsync is free and Full == Normal.
//!
//!     TMPDIR=$PWD/target cargo test --release -p asterpath-storage-sqlite --test concurrency_bench -- --ignored --nocapture
//!
//! Drives only the public `SessionStore` trait. Each session loops
//! `append` -> `frontier` -> incremental `scan` -> `yield_now` (stand-in for
//! the LLM/tool await between real appends); a storage-free heartbeat task
//! on the same runtime sleeps 1ms and records how late it wakes (worker stall
//! proxy). Repeated for `Synchronous::Full` and `Synchronous::Normal` to split
//! fsync cost from lock contention.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use asterpath_protocol::{
    LOG_SCHEMA_VERSION, LogQuery, NewEntry, SessionEvent, SessionId, SessionStore,
};
use asterpath_storage_sqlite::{OpenOptions, Synchronous, open_with};
use common::TempDir;
use futures_util::StreamExt;

const WORKERS: usize = 4;
const APPENDS_PER_SESSION: usize = 100;
const SESSION_COUNTS: [usize; 3] = [1, 8, 32];
const HEARTBEAT: Duration = Duration::from_millis(1);

#[test]
#[ignore = "benchmark; run explicitly in release mode (see file header)"]
fn concurrency_bench() {
    println!(
        "workers={WORKERS} appends/session={APPENDS_PER_SESSION} heartbeat={}ms",
        HEARTBEAT.as_millis()
    );
    println!(
        "| synchronous | N | appends | append p50 (ms) | append p99 (ms) | heartbeat lag p50 (ms) | heartbeat lag p99 (ms) | heartbeat lag max (ms) | heartbeats | wall (s) |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|");
    for synchronous in [Synchronous::Full, Synchronous::Normal] {
        for n in SESSION_COUNTS {
            run(synchronous, n);
        }
    }
}

fn run(synchronous: Synchronous, n: usize) {
    let dir = TempDir::new("bench");
    let store: Arc<dyn SessionStore> =
        open_with(dir.db(), OpenOptions { synchronous }).expect("open store");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .expect("runtime");

    let (mut appends, mut lags, wall) = runtime.block_on(async {
        let stop = Arc::new(AtomicBool::new(false));
        let heartbeat = tokio::spawn({
            let stop = stop.clone();
            async move {
                let mut lags = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let start = Instant::now();
                    tokio::time::sleep(HEARTBEAT).await;
                    lags.push(start.elapsed().saturating_sub(HEARTBEAT));
                }
                lags
            }
        });

        let started = Instant::now();
        let sessions: Vec<_> = (0..n)
            .map(|i| tokio::spawn(drive_session(store.clone(), session(i))))
            .collect();
        let mut appends = Vec::with_capacity(n * APPENDS_PER_SESSION);
        for handle in sessions {
            appends.extend(handle.await.expect("session task"));
        }
        let wall = started.elapsed();
        stop.store(true, Ordering::Relaxed);
        (appends, heartbeat.await.expect("heartbeat task"), wall)
    });

    println!(
        "| {synchronous:?} | {n} | {} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} | {} | {:.2} |",
        appends.len(),
        ms(percentile(&mut appends, 0.50)),
        ms(percentile(&mut appends, 0.99)),
        ms(percentile(&mut lags, 0.50)),
        ms(percentile(&mut lags, 0.99)),
        ms(percentile(&mut lags, 1.0)),
        lags.len(),
        wall.as_secs_f64(),
    );
}

async fn drive_session(store: Arc<dyn SessionStore>, session: SessionId) -> Vec<Duration> {
    let payload = serde_json::to_value(SessionEvent::InputRecorded { input: vec![] })
        .expect("serialize event");
    let mut latencies = Vec::with_capacity(APPENDS_PER_SESSION);
    let mut cursor = None;
    for _ in 0..APPENDS_PER_SESSION {
        let start = Instant::now();
        store
            .append(NewEntry {
                session,
                schema_version: LOG_SCHEMA_VERSION,
                dedup_key: None,
                payload: payload.clone(),
            })
            .await
            .expect("append");
        latencies.push(start.elapsed());

        store.frontier(session).await.expect("frontier");
        let mut entries = store.scan(LogQuery {
            session,
            after: cursor,
            limit: None,
        });
        while let Some(entry) = entries.next().await {
            cursor = Some(entry.expect("scan").log_id);
        }
        tokio::task::yield_now().await;
    }
    latencies
}

fn session(i: usize) -> SessionId {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&(i as u64 + 1).to_be_bytes());
    SessionId::from_bytes(bytes)
}

fn percentile(samples: &mut [Duration], p: f64) -> Duration {
    samples.sort_unstable();
    samples[((samples.len() - 1) as f64 * p).round() as usize]
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}
