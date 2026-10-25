//! The claim an owner operation runs under (ADR-0036). A drive or manual
//! compaction polls its work inside [`within`], so every append it makes,
//! child drives included, carries the claim's token without threading it
//! through the agent, pipeline and scheduler. Appends made outside one, the
//! external writes, carry none.
//!
//! [`Ownership`] keeps this process's claims and renews its process lease.

use std::cell::Cell;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use asterpath_protocol::{Claim, Hooks, OwnerToken, SessionId, SessionStore, StorageError};
use tokio::sync::watch;

thread_local! {
    static OWNER: Cell<Option<OwnerToken>> = const { Cell::new(None) };
}

/// The claim of the owner operation being polled on this thread.
pub(crate) fn current() -> Option<OwnerToken> {
    OWNER.get()
}

/// Run `f` as `owner`; `None` keeps the enclosing claim (a child drive
/// polled inside its parent's).
pub(crate) fn within<R>(owner: Option<OwnerToken>, f: impl FnOnce() -> R) -> R {
    struct Restore(Option<OwnerToken>);
    impl Drop for Restore {
        fn drop(&mut self) {
            OWNER.set(self.0);
        }
    }
    let Some(owner) = owner else {
        return f();
    };
    let _restore = Restore(OWNER.replace(Some(owner)));
    f()
}

/// A future polled as `owner`.
pub(crate) struct Owned<F> {
    pub(crate) owner: OwnerToken,
    pub(crate) future: F,
}

impl<F: Future + Unpin> Future for Owned<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let owner = self.owner;
        within(Some(owner), || Pin::new(&mut self.future).poll(cx))
    }
}

/// One claim of a tree in this process, shared by every `OwnedSession` of
/// the tree, until it is lost.
pub(crate) struct Held {
    token: OwnerToken,
    /// Taken and dropped, releasing the tree, when the claim is lost.
    claim: Mutex<Option<Claim>>,
    lost: watch::Sender<bool>,
}

impl Held {
    pub(crate) fn token(&self) -> OwnerToken {
        self.token
    }

    pub(crate) fn is_lost(&self) -> bool {
        *self.lost.borrow()
    }

    /// Resolves once the claim is lost.
    pub(crate) fn lost(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut lost = self.lost.subscribe();
        async move {
            let _ = lost.wait_for(|lost| *lost).await;
        }
    }
}

/// This process's claims and its process lease (ADR-0036). A claim is lost
/// when the lease cannot be renewed or another process takes the tree over
/// (a renewal reports it, or an owner write is refused): its drives stop,
/// its owner writes are refused here, the tree is released and the hosts
/// are told.
pub(crate) struct Ownership {
    /// The backend, not the commit log: claims and renewals are not records.
    store: Arc<dyn SessionStore>,
    hooks: Vec<Arc<dyn Hooks>>,
    held: Mutex<HashMap<SessionId, Weak<Held>>>,
}

impl Ownership {
    pub(crate) fn new(store: Arc<dyn SessionStore>, hooks: Vec<Arc<dyn Hooks>>) -> Self {
        Self {
            store,
            hooks,
            held: Mutex::default(),
        }
    }

    fn live(&self, root: SessionId) -> Option<Arc<Held>> {
        lock(&self.held).get(&root).and_then(Weak::upgrade)
    }

    /// Claim `root`, sharing this process's live claim of it.
    pub(crate) async fn claim(&self, root: SessionId) -> Result<Arc<Held>, StorageError> {
        let claim = self.store.claim(root).await?;
        let token = claim.token();
        let mut held = lock(&self.held);
        if let Some(live) = held.get(&root).and_then(Weak::upgrade)
            && live.token == token
            && !live.is_lost()
        {
            // The store counted this claim on top of the shared one.
            drop(held);
            drop(claim);
            return Ok(live);
        }
        let live = Arc::new(Held {
            token,
            claim: Mutex::new(Some(claim)),
            lost: watch::Sender::new(false),
        });
        held.retain(|_, held| held.strong_count() > 0);
        held.insert(root, Arc::downgrade(&live));
        Ok(live)
    }

    /// Whether an owner write as `owner` must not reach the store: its claim
    /// here is lost or superseded.
    pub(crate) fn revoked(&self, owner: OwnerToken) -> bool {
        self.live(owner.root)
            .is_some_and(|live| live.token != owner || live.is_lost())
    }

    /// Lose `owner`'s claim, once.
    pub(crate) fn lose(&self, owner: OwnerToken) {
        let Some(live) = self.live(owner.root) else {
            return;
        };
        if live.token != owner || live.lost.send_replace(true) {
            return;
        }
        tracing::warn!(root = ?owner.root, epoch = owner.epoch, "session tree ownership lost");
        // Unless another process has taken the tree, this frees it now.
        drop(lock(&live.claim).take());
        for hook in &self.hooks {
            if std::panic::catch_unwind(AssertUnwindSafe(|| hook.on_ownership_lost(owner))).is_err()
            {
                tracing::warn!(root = ?owner.root, "on_ownership_lost hook panicked");
            }
        }
    }

    /// Renew the process lease; lose what another process took, or every
    /// claim when the lease could not be renewed.
    async fn renew(&self) {
        let lost = match self.store.renew().await {
            Ok(lost) => lost,
            Err(error) => {
                tracing::warn!(%error, "renewing the process lease failed");
                lock(&self.held)
                    .values()
                    .filter_map(Weak::upgrade)
                    .map(|live| live.token)
                    .collect()
            }
        };
        for owner in lost {
            self.lose(owner);
        }
    }

    /// Renew every `every` on the current tokio runtime for as long as
    /// `ownership` lives. Without a runtime nothing renews, and other
    /// processes may take this one's trees once its lease expires.
    pub(crate) fn keep_renewed(ownership: &Arc<Self>, every: Duration) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::warn!("no tokio runtime: the process lease is not renewed");
            return;
        };
        let ownership = Arc::downgrade(ownership);
        runtime.spawn(async move {
            let mut ticks = tokio::time::interval(every);
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticks.tick().await;
            loop {
                ticks.tick().await;
                let Some(ownership) = ownership.upgrade() else {
                    return;
                };
                ownership.renew().await;
            }
        });
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
