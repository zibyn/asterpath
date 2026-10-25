//! Values a drive hands to the tool pipeline through the poll (the owner
//! token, the session environment): set on this thread while the drive's
//! future is polled, so they need not be threaded through the agent,
//! pipeline and scheduler.

use std::cell::RefCell;
use std::thread::LocalKey;

/// Run `f` with `key` set to `value`; `None` keeps the enclosing value (a
/// child drive polled inside its parent's).
pub(crate) fn within<T: 'static, R>(
    key: &'static LocalKey<RefCell<Option<T>>>,
    value: Option<T>,
    f: impl FnOnce() -> R,
) -> R {
    struct Restore<T: 'static>(&'static LocalKey<RefCell<Option<T>>>, Option<T>);
    impl<T> Drop for Restore<T> {
        fn drop(&mut self) {
            self.0.set(self.1.take());
        }
    }
    let Some(value) = value else {
        return f();
    };
    let _restore = Restore(key, key.replace(Some(value)));
    f()
}
