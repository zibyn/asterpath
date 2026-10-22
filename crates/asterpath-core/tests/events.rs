use std::fmt;
use std::sync::{Arc, Mutex};

use asterpath_core::{
    Event, Handler, LifecycleError, Middleware, Module, ModuleCtx, ModuleHost, Next, WaterfallEvent,
};
use futures_util::future::BoxFuture;

#[derive(Clone, Debug, PartialEq)]
struct Ping(u32);

impl Event for Ping {}

#[derive(Clone, Debug, PartialEq)]
struct Chain(&'static str);

impl Event for Chain {}

#[derive(Debug, PartialEq)]
struct ChainError(&'static str);

impl fmt::Display for ChainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for ChainError {}

impl WaterfallEvent for Chain {
    type Error = ChainError;
}

struct Recorder {
    tag: &'static str,
    panics: bool,
    seen: Arc<Mutex<Vec<(&'static str, u32)>>>,
}

impl Handler<Ping> for Recorder {
    fn handle<'a>(&'a self, event: &'a Ping) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.seen.lock().unwrap().push((self.tag, event.0));
            if self.panics {
                panic!("handler failed");
            }
        })
    }
}

struct Scripted {
    tag: &'static str,
    replacement: Option<&'static str>,
    call_next: bool,
    fail: bool,
    seen: Arc<Mutex<Vec<&'static str>>>,
}

impl Middleware<Chain> for Scripted {
    fn handle<'a>(
        &'a self,
        event: Chain,
        next: Next<'a, Chain>,
    ) -> BoxFuture<'a, Result<Chain, ChainError>> {
        Box::pin(async move {
            self.seen.lock().unwrap().push(self.tag);
            if self.fail {
                return Err(ChainError("rejected"));
            }
            let event = match self.replacement {
                Some(replacement) => Chain(replacement),
                None => event,
            };
            if !self.call_next {
                return Ok(event);
            }
            next.run(event).await
        })
    }
}

struct FnModule<F> {
    name: &'static str,
    init: F,
}

impl<F> Module for FnModule<F>
where
    F: for<'a> Fn(&'a ModuleCtx) -> BoxFuture<'a, Result<(), LifecycleError>>
        + Send
        + Sync
        + 'static,
{
    fn name(&self) -> &'static str {
        self.name
    }

    fn init<'a>(&'a self, ctx: &'a ModuleCtx) -> BoxFuture<'a, Result<(), LifecycleError>> {
        (self.init)(ctx)
    }
}

async fn run<F>(init: F) -> Result<(), LifecycleError>
where
    F: for<'a> Fn(&'a ModuleCtx) -> BoxFuture<'a, Result<(), LifecycleError>>
        + Send
        + Sync
        + 'static,
{
    let mut host = ModuleHost::new();
    host.register(Arc::new(FnModule { name: "bus", init }));
    host.init().await
}

#[tokio::test]
async fn emit_notifies_handlers_in_registration_order() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let first_seen = seen.clone();
    let second_seen = seen.clone();

    run(move |ctx: &ModuleCtx| {
        let first_seen = first_seen.clone();
        let second_seen = second_seen.clone();
        Box::pin(async move {
            ctx.on(Recorder {
                tag: "first",
                panics: false,
                seen: first_seen,
            });
            ctx.on(Recorder {
                tag: "second",
                panics: false,
                seen: second_seen,
            });

            ctx.emit(Ping(7)).await;
            Ok(())
        })
    })
    .await
    .unwrap();

    assert_eq!(*seen.lock().unwrap(), [("first", 7), ("second", 7)]);
}

#[tokio::test]
async fn emit_continues_after_a_failing_handler() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let before_seen = seen.clone();
    let after_seen = seen.clone();

    run(move |ctx: &ModuleCtx| {
        let before_seen = before_seen.clone();
        let after_seen = after_seen.clone();
        Box::pin(async move {
            ctx.on(Recorder {
                tag: "before",
                panics: true,
                seen: before_seen,
            });
            ctx.on(Recorder {
                tag: "after",
                panics: false,
                seen: after_seen,
            });

            ctx.emit(Ping(1)).await;
            Ok(())
        })
    })
    .await
    .unwrap();

    assert_eq!(
        *seen.lock().unwrap(),
        [("before", 1), ("after", 1)],
        "a failing handler must not stop later handlers"
    );
}

#[tokio::test]
async fn waterfall_lets_middleware_rewrite_the_event() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let middleware_seen = seen.clone();
    let result_seen = seen.clone();

    run(move |ctx: &ModuleCtx| {
        let middleware_seen = middleware_seen.clone();
        let result_seen = result_seen.clone();
        Box::pin(async move {
            ctx.intercept(Scripted {
                tag: "rewriter",
                replacement: Some("rewritten"),
                call_next: true,
                fail: false,
                seen: middleware_seen,
            });

            let result = ctx.waterfall(Chain("original")).await.unwrap();
            result_seen.lock().unwrap().push(result.0);
            Ok(())
        })
    })
    .await
    .unwrap();

    assert_eq!(*seen.lock().unwrap(), ["rewriter", "rewritten"]);
}

#[tokio::test]
async fn waterfall_short_circuits_when_middleware_does_not_call_next() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let outer_seen = seen.clone();
    let inner_seen = seen.clone();
    let result_seen = seen.clone();

    run(move |ctx: &ModuleCtx| {
        let outer_seen = outer_seen.clone();
        let inner_seen = inner_seen.clone();
        let result_seen = result_seen.clone();
        Box::pin(async move {
            ctx.intercept(Scripted {
                tag: "outer",
                replacement: Some("stopped"),
                call_next: false,
                fail: false,
                seen: outer_seen,
            });
            ctx.intercept(Scripted {
                tag: "inner",
                replacement: None,
                call_next: true,
                fail: false,
                seen: inner_seen,
            });

            let result = ctx.waterfall(Chain("original")).await.unwrap();
            result_seen.lock().unwrap().push(result.0);
            Ok(())
        })
    })
    .await
    .unwrap();

    assert_eq!(*seen.lock().unwrap(), ["outer", "stopped"]);
}

#[tokio::test]
async fn waterfall_propagates_middleware_errors() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let middleware_seen = seen.clone();
    let result_seen = seen.clone();

    run(move |ctx: &ModuleCtx| {
        let middleware_seen = middleware_seen.clone();
        let result_seen = result_seen.clone();
        Box::pin(async move {
            ctx.intercept(Scripted {
                tag: "rejecting",
                replacement: None,
                call_next: true,
                fail: true,
                seen: middleware_seen,
            });

            let result = ctx.waterfall(Chain("original")).await;
            result_seen.lock().unwrap().push(match result {
                Ok(_) => "ok",
                Err(_) => "err",
            });
            Ok(())
        })
    })
    .await
    .unwrap();

    assert_eq!(*seen.lock().unwrap(), ["rejecting", "err"]);
}
