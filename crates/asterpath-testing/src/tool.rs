//! A scripted in-process tool that records the arguments it receives.

use std::sync::{Arc, Mutex, PoisonError};

use asterpath_protocol::{
    Part, Tool, ToolArgs, ToolContext, ToolDescriptor, ToolError, ToolOutput,
};
use futures_core::future::BoxFuture;

type InvokeFn = Arc<
    dyn Fn(ToolArgs, ToolContext) -> BoxFuture<'static, Result<ToolOutput, ToolError>>
        + Send
        + Sync,
>;

/// Shared record of every `ToolArgs` a [`TestTool`] was invoked with.
pub type ToolCalls = Arc<Mutex<Vec<ToolArgs>>>;

/// A tool whose behaviour is a closure; every invocation's args are pushed to
/// `calls` before the closure runs.
pub struct TestTool {
    descriptor: ToolDescriptor,
    calls: ToolCalls,
    invoke: InvokeFn,
}

impl TestTool {
    pub fn new(
        descriptor: ToolDescriptor,
        calls: ToolCalls,
        invoke: impl Fn(ToolArgs, ToolContext) -> BoxFuture<'static, Result<ToolOutput, ToolError>>
        + Send
        + Sync
        + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            descriptor,
            calls,
            invoke: Arc::new(invoke),
        })
    }

    /// Returns its args as one text part.
    pub fn echo(descriptor: ToolDescriptor) -> (Arc<Self>, ToolCalls) {
        let calls = ToolCalls::default();
        let tool = Self::new(descriptor, Arc::clone(&calls), |args, _ctx| {
            Box::pin(async move { Ok(ToolOutput::new(vec![text(args.as_str())])) })
        });
        (tool, calls)
    }

    /// Returns `value` as one text part.
    pub fn text(descriptor: ToolDescriptor, value: &str) -> (Arc<Self>, ToolCalls) {
        let value = value.to_owned();
        let calls = ToolCalls::default();
        let tool = Self::new(descriptor, Arc::clone(&calls), move |_args, _ctx| {
            let value = value.clone();
            Box::pin(async move { Ok(ToolOutput::new(vec![text(&value)])) })
        });
        (tool, calls)
    }
}

impl Tool for TestTool {
    fn descriptor(&self) -> &ToolDescriptor {
        &self.descriptor
    }

    fn invoke(
        &self,
        args: ToolArgs,
        ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(args.clone());
        (self.invoke)(args, ctx)
    }
}

fn text(value: &str) -> Part {
    Part::Text {
        text: value.to_owned(),
    }
}
