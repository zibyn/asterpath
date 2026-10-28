//! The claim table over fake row operations that record what the backend
//! was asked to do, in order.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::task::Poll;

use asterpath_protocol::{ClaimRows, ClaimTable, OwnerToken, ReleaseWait, SessionId, StorageError};
use futures_core::future::BoxFuture;
use futures_util::FutureExt;

/// A claim reply the test fills in; polled, not woken.
type Slot = Arc<Mutex<Option<u64>>>;

#[derive(Clone, Debug, PartialEq)]
enum Call {
    Claim(SessionId),
    Release(OwnerToken),
    Renew(Vec<OwnerToken>),
}

/// Row operations whose replies the test hands out: each claim takes the
/// next queued epoch, each renewal the next queued loss.
#[derive(Clone, Default)]
struct Rows {
    calls: Arc<Mutex<Vec<Call>>>,
    epochs: Arc<Mutex<VecDeque<u64>>>,
    lost: Arc<Mutex<VecDeque<Vec<OwnerToken>>>>,
    /// Pending claims, answered by `answer` instead of `epochs`.
    pending: Arc<Mutex<VecDeque<Slot>>>,
    hold_claims: bool,
}

impl Rows {
    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }

    fn epochs(self, epochs: &[u64]) -> Self {
        self.epochs.lock().unwrap().extend(epochs);
        self
    }

    fn answer(&self, epoch: u64) {
        let slot = self
            .pending
            .lock()
            .unwrap()
            .pop_front()
            .expect("a pending claim");
        *slot.lock().unwrap() = Some(epoch);
    }
}

impl ClaimRows for Rows {
    fn claim(&self, root: SessionId) -> BoxFuture<'static, Result<u64, StorageError>> {
        self.calls.lock().unwrap().push(Call::Claim(root));
        if self.hold_claims {
            let slot = Slot::default();
            self.pending.lock().unwrap().push_back(Arc::clone(&slot));
            return Box::pin(std::future::poll_fn(move |_| match *slot.lock().unwrap() {
                Some(epoch) => Poll::Ready(Ok(epoch)),
                None => Poll::Pending,
            }));
        }
        let epoch = self.epochs.lock().unwrap().pop_front().expect("an epoch");
        Box::pin(async move { Ok(epoch) })
    }

    fn release(&self, token: OwnerToken) -> ReleaseWait {
        self.calls.lock().unwrap().push(Call::Release(token));
        Box::new(|| {})
    }

    fn renew(
        &self,
        held: Vec<OwnerToken>,
    ) -> BoxFuture<'static, Result<Vec<OwnerToken>, StorageError>> {
        let mut held = held;
        held.sort_by_key(|token| token.root);
        self.calls.lock().unwrap().push(Call::Renew(held));
        let lost = self.lost.lock().unwrap().pop_front().unwrap_or_default();
        Box::pin(async move { Ok(lost) })
    }
}

fn root(byte: u8) -> SessionId {
    SessionId::from_bytes([byte; 16])
}

fn token(root: SessionId, epoch: u64) -> OwnerToken {
    OwnerToken { root, epoch }
}

fn now<T>(future: impl std::future::Future<Output = T>) -> T {
    future.now_or_never().expect("ready")
}

#[test]
fn claims_of_one_root_share_the_epoch_and_the_last_drop_releases() {
    let rows = Rows::default().epochs(&[3]);
    let table = ClaimTable::new(rows.clone());

    let first = now(table.claim(root(1))).unwrap();
    let second = now(table.claim(root(1))).unwrap();
    assert_eq!(first.token(), token(root(1), 3));
    assert_eq!(second.token(), first.token(), "shared epoch");
    assert_eq!(rows.calls(), vec![Call::Claim(root(1))], "one row claim");

    drop(first);
    assert_eq!(rows.calls(), vec![Call::Claim(root(1))], "still held");
    drop(second);
    assert_eq!(
        rows.calls(),
        vec![Call::Claim(root(1)), Call::Release(token(root(1), 3))]
    );
}

#[test]
fn a_claim_while_the_release_commits_is_queued_after_it() {
    let rows = Rows::default().epochs(&[1, 2]);
    let (commit, committed) = std::sync::mpsc::channel::<()>();
    let table = ClaimTable::new(Gated {
        rows: rows.clone(),
        committed: Arc::new(Mutex::new(committed)),
    });
    let claim = now(table.claim(root(1))).unwrap();

    let dropping = std::thread::spawn(move || drop(claim));
    while !rows.calls().contains(&Call::Release(token(root(1), 1))) {
        std::thread::yield_now();
    }
    let again = now(table.claim(root(1))).unwrap();
    assert_eq!(again.token(), token(root(1), 2), "not the releasing epoch");
    assert_eq!(
        rows.calls(),
        vec![
            Call::Claim(root(1)),
            Call::Release(token(root(1), 1)),
            Call::Claim(root(1)),
        ]
    );
    commit.send(()).unwrap();
    dropping.join().unwrap();
    drop(commit); // `again` releases as the test ends.
}

/// `Rows` whose releases commit when the test says so.
struct Gated {
    rows: Rows,
    committed: Arc<Mutex<std::sync::mpsc::Receiver<()>>>,
}

impl ClaimRows for Gated {
    fn claim(&self, root: SessionId) -> BoxFuture<'static, Result<u64, StorageError>> {
        self.rows.claim(root)
    }

    fn release(&self, token: OwnerToken) -> ReleaseWait {
        let recorded = self.rows.release(token);
        let committed = Arc::clone(&self.committed);
        Box::new(move || {
            recorded();
            let _ = committed.lock().unwrap().recv();
        })
    }

    fn renew(
        &self,
        held: Vec<OwnerToken>,
    ) -> BoxFuture<'static, Result<Vec<OwnerToken>, StorageError>> {
        self.rows.renew(held)
    }
}

#[test]
fn renewal_forgets_only_lost_claims_of_the_held_epoch() {
    let rows = Rows::default().epochs(&[3, 5, 6]);
    let table = ClaimTable::new(rows.clone());
    let _first = now(table.claim(root(1))).unwrap();
    let second = now(table.claim(root(2))).unwrap();
    // Root 2's loss names an epoch this table no longer holds.
    rows.lost
        .lock()
        .unwrap()
        .push_back(vec![token(root(1), 3), token(root(2), 4)]);

    let lost = now(table.renew()).unwrap();
    assert_eq!(lost, vec![token(root(1), 3), token(root(2), 4)]);
    assert_eq!(
        rows.calls().last(),
        Some(&Call::Renew(vec![token(root(1), 3), token(root(2), 5)]))
    );

    let shared = now(table.claim(root(2))).unwrap();
    assert_eq!(shared.token(), second.token(), "root 2 is still held");
    let fresh = now(table.claim(root(1))).unwrap();
    assert_eq!(fresh.token(), token(root(1), 6), "root 1 is claimed anew");
}

#[test]
fn a_different_epoch_from_the_backend_restarts_the_count() {
    let rows = Rows {
        hold_claims: true,
        ..Rows::default()
    };
    let table = ClaimTable::new(rows.clone());
    let mut early = Box::pin(table.claim(root(1)));
    let mut late = Box::pin(table.claim(root(1)));
    let mut cx = std::task::Context::from_waker(futures_util::task::noop_waker_ref());
    assert!(early.poll_unpin(&mut cx).is_pending());
    assert!(late.poll_unpin(&mut cx).is_pending());

    rows.answer(7);
    rows.answer(8);
    let early = now(early).unwrap();
    let late = now(late).unwrap();
    assert_eq!(early.token(), token(root(1), 7));
    assert_eq!(late.token(), token(root(1), 8));

    drop(early);
    assert!(
        !rows
            .calls()
            .iter()
            .any(|call| matches!(call, Call::Release(_))),
        "the superseded epoch releases nothing"
    );
    drop(late);
    assert_eq!(rows.calls().last(), Some(&Call::Release(token(root(1), 8))));
}
