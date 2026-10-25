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
//!
//! `multi_process_bench` (same command) spreads the sessions over P
//! processes sharing the file (ADR-0036): each claims its own trees and
//! appends as their owner, so writers contend for the WAL lock across
//! processes without a shared group commit. It prints throughput and the
//! append latency tail.

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
                owner: None,
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

// --- several processes on one file (ADR-0036) --------------------------------

const PROCESS_COUNTS: [usize; 3] = [1, 2, 4];
const SESSIONS_PER_PROCESS: usize = 8;
const CHILD_DB: &str = "ASTERPATH_BENCH_DB";
const CHILD_INDEX: &str = "ASTERPATH_BENCH_PROCESS";
const CHILD_SYNC: &str = "ASTERPATH_BENCH_SYNC";

#[test]
#[ignore = "benchmark; run explicitly in release mode (see file header)"]
fn multi_process_bench() {
    println!(
        "sessions/process={SESSIONS_PER_PROCESS} appends/session={APPENDS_PER_SESSION}, owner writes"
    );
    println!(
        "| synchronous | processes | appends | appends/s | append p50 (ms) | append p99 (ms) | append max (ms) | wall (s) |"
    );
    println!("|---|---|---|---|---|---|---|---|");
    for synchronous in [Synchronous::Full, Synchronous::Normal] {
        for processes in PROCESS_COUNTS {
            run_processes(synchronous, processes);
        }
    }
}

fn run_processes(synchronous: Synchronous, processes: usize) {
    let dir = TempDir::new("bench-processes");
    // Create the schema once, outside the timed part.
    drop(open_with(dir.db(), OpenOptions { synchronous }).expect("open store"));
    let started = Instant::now();
    let children: Vec<_> = (0..processes)
        .map(|index| {
            std::process::Command::new(std::env::current_exe().expect("bench binary"))
                .args(["--exact", "bench_child", "--nocapture", "--ignored"])
                .env(CHILD_DB, dir.db())
                .env(CHILD_INDEX, index.to_string())
                .env(CHILD_SYNC, format!("{synchronous:?}"))
                .stdout(std::process::Stdio::piped())
                .spawn()
                .expect("spawn bench process")
        })
        .collect();
    let mut appends: Vec<Duration> = Vec::new();
    for child in children {
        let output = child.wait_with_output().expect("bench process");
        assert!(output.status.success(), "bench process failed");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let line = stdout
            .lines()
            .find_map(|line| line.strip_prefix("latencies_us:"))
            .expect("latency line");
        appends.extend(
            line.split_whitespace()
                .map(|us| Duration::from_micros(us.parse().expect("micros"))),
        );
    }
    let wall = started.elapsed();
    println!(
        "| {synchronous:?} | {processes} | {} | {:.0} | {:.2} | {:.2} | {:.2} | {:.2} |",
        appends.len(),
        appends.len() as f64 / wall.as_secs_f64(),
        ms(percentile(&mut appends, 0.50)),
        ms(percentile(&mut appends, 0.99)),
        ms(percentile(&mut appends, 1.0)),
        wall.as_secs_f64(),
    );
}

/// One bench process: claims its sessions and appends as their owner.
#[test]
#[ignore = "re-entered by multi_process_bench"]
fn bench_child() {
    let Ok(db) = std::env::var(CHILD_DB) else {
        return;
    };
    let index: usize = std::env::var(CHILD_INDEX)
        .expect("process index")
        .parse()
        .expect("index");
    let synchronous = match std::env::var(CHILD_SYNC).as_deref() {
        Ok("Normal") => Synchronous::Normal,
        _ => Synchronous::Full,
    };
    let store: Arc<dyn SessionStore> =
        open_with(db, OpenOptions { synchronous }).expect("open store");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .expect("runtime");
    let latencies = runtime.block_on(async {
        let sessions: Vec<_> = (0..SESSIONS_PER_PROCESS)
            .map(|i| {
                let store = store.clone();
                tokio::spawn(async move {
                    let session = session(index * SESSIONS_PER_PROCESS + i);
                    let claim = store.claim(session).await.expect("claim");
                    owner_appends(&*store, session, claim.token()).await
                })
            })
            .collect();
        let mut latencies = Vec::new();
        for handle in sessions {
            latencies.extend(handle.await.expect("session task"));
        }
        latencies
    });
    let micros: Vec<String> = latencies
        .iter()
        .map(|latency| latency.as_micros().to_string())
        .collect();
    println!("latencies_us:{}", micros.join(" "));
}

async fn owner_appends(
    store: &dyn SessionStore,
    session: SessionId,
    owner: asterpath_protocol::OwnerToken,
) -> Vec<Duration> {
    let payload = serde_json::to_value(SessionEvent::InputRecorded { input: vec![] })
        .expect("serialize event");
    let mut latencies = Vec::with_capacity(APPENDS_PER_SESSION);
    for _ in 0..APPENDS_PER_SESSION {
        let start = Instant::now();
        store
            .append(NewEntry {
                session,
                schema_version: LOG_SCHEMA_VERSION,
                dedup_key: None,
                owner: Some(owner),
                payload: payload.clone(),
            })
            .await
            .expect("append");
        latencies.push(start.elapsed());
        tokio::task::yield_now().await;
    }
    latencies
}
