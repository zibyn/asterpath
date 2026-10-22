use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures_core::future::BoxFuture;
use futures_util::FutureExt;

/// A typed notification passed between modules.
pub trait Event: Send + Sync + 'static {}

/// An event dispatched through the waterfall (intercepting) bus.
pub trait WaterfallEvent: Event {
    type Error: std::error::Error + Send + Sync + 'static;
}

/// Observing participant of the typed event bus.
pub trait Handler<E: Event>: Send + Sync + 'static {
    fn handle<'a>(&'a self, event: &'a E) -> BoxFuture<'a, ()>;
}

/// Intercepting participant of the typed event bus.
pub trait Middleware<E: WaterfallEvent>: Send + Sync + 'static {
    fn handle<'a>(&'a self, event: E, next: Next<'a, E>) -> BoxFuture<'a, Result<E, E::Error>>;
}

impl<E: Event, F> Handler<E> for F
where
    F: for<'a> Fn(&'a E) -> BoxFuture<'a, ()> + Send + Sync + 'static,
{
    fn handle<'a>(&'a self, event: &'a E) -> BoxFuture<'a, ()> {
        (self)(event)
    }
}

impl<E, F> Middleware<E> for F
where
    E: WaterfallEvent,
    F: for<'a> Fn(E, Next<'a, E>) -> BoxFuture<'a, Result<E, E::Error>> + Send + Sync + 'static,
{
    fn handle<'a>(&'a self, event: E, next: Next<'a, E>) -> BoxFuture<'a, Result<E, E::Error>> {
        (self)(event, next)
    }
}

/// Opaque continuation handle for the remaining waterfall middleware chain.
pub struct Next<'a, E: WaterfallEvent> {
    rest: &'a [Arc<dyn Middleware<E>>],
}

impl<'a, E: WaterfallEvent> Next<'a, E> {
    /// Invoke the next middleware (or return the event unchanged at the end of the chain).
    pub fn run(self, event: E) -> BoxFuture<'a, Result<E, E::Error>> {
        Box::pin(async move {
            let Some((middleware, rest)) = self.rest.split_first() else {
                return Ok(event);
            };
            middleware.handle(event, Next { rest }).await
        })
    }
}

type Erased = Arc<dyn Any + Send + Sync>;
type HandlerList = Vec<(u64, Erased)>;
type MiddlewareList = Vec<(u64, Erased)>;

#[derive(Default)]
pub(crate) struct EventBus {
    handlers: Mutex<HashMap<TypeId, HandlerList>>,
    middlewares: Mutex<HashMap<TypeId, MiddlewareList>>,
    next_registration: AtomicU64,
}

impl EventBus {
    pub(crate) fn register_handler<E: Event>(&self, handler: Arc<dyn Handler<E>>) -> u64 {
        let id = self.next_registration.fetch_add(1, Ordering::Relaxed);
        let stored: Erased = Arc::new(handler);
        self.handlers
            .lock()
            .unwrap()
            .entry(TypeId::of::<E>())
            .or_default()
            .push((id, stored));
        id
    }

    pub(crate) fn remove_handler<E: Event>(&self, registration: u64) {
        remove_registration(
            self.handlers.lock().unwrap().get_mut(&TypeId::of::<E>()),
            registration,
        );
    }

    pub(crate) fn register_middleware<E: WaterfallEvent>(
        &self,
        middleware: Arc<dyn Middleware<E>>,
    ) -> u64 {
        let id = self.next_registration.fetch_add(1, Ordering::Relaxed);
        let stored: Erased = Arc::new(middleware);
        self.middlewares
            .lock()
            .unwrap()
            .entry(TypeId::of::<E>())
            .or_default()
            .push((id, stored));
        id
    }

    pub(crate) fn remove_middleware<E: WaterfallEvent>(&self, registration: u64) {
        remove_registration(
            self.middlewares.lock().unwrap().get_mut(&TypeId::of::<E>()),
            registration,
        );
    }

    /// Notify handlers in registration order.
    ///
    /// A handler that panics is swallowed so later handlers still run; handler
    /// failures are never propagated to the emitter.
    pub(crate) fn emit<E: Event>(&self, event: E) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let handlers = {
                let map = self.handlers.lock().unwrap();
                map.get(&TypeId::of::<E>())
                    .map(|entries| {
                        entries
                            .iter()
                            .map(|(_, stored)| {
                                stored
                                    .clone()
                                    .downcast::<Arc<dyn Handler<E>>>()
                                    .expect("handler registered under its event type")
                                    .as_ref()
                                    .clone()
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            };

            for handler in handlers {
                let outcome = AssertUnwindSafe(handler.handle(&event))
                    .catch_unwind()
                    .await;
                let _ = outcome;
            }
        })
    }

    /// Run the middleware chain in registration order; either layer may rewrite,
    /// short-circuit or fail the dispatch.
    pub(crate) fn waterfall<E: WaterfallEvent>(
        &self,
        event: E,
    ) -> BoxFuture<'_, Result<E, E::Error>> {
        Box::pin(async move {
            let middlewares = {
                let map = self.middlewares.lock().unwrap();
                map.get(&TypeId::of::<E>())
                    .map(|entries| {
                        entries
                            .iter()
                            .map(|(_, stored)| {
                                stored
                                    .clone()
                                    .downcast::<Arc<dyn Middleware<E>>>()
                                    .expect("middleware registered under its event type")
                                    .as_ref()
                                    .clone()
                            })
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            };

            Next { rest: &middlewares }.run(event).await
        })
    }
}

fn remove_registration(list: Option<&mut Vec<(u64, Erased)>>, registration: u64) {
    if let Some(list) = list {
        list.retain(|(id, _)| *id != registration);
    }
}
