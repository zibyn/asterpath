//! The claim an owner operation runs under (ADR-0036). A drive or manual
//! compaction polls its work inside [`within`], so every append it makes,
//! child drives included, carries the claim's token without threading it
//! through the agent, pipeline and scheduler. Appends made outside one, the
//! external writes, carry none.

use std::cell::Cell;
use std::pin::Pin;
use std::task::{Context, Poll};

use asterpath_protocol::OwnerToken;

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
