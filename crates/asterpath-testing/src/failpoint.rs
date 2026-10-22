//! Crash-injection decorators at trait boundaries.
//!
//! There is one decorator per injection point, and nesting covers scenarios
//! that need more than one. `Halt` is the process-level action: it writes the
//! canonical marker, flushes stdout, then never returns until the parent test
//! sends `SIGKILL`. `Panic` exists only for in-process tests of intermediate
//! state; it is never a substitute for a real crash.

use std::io::Write;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use asterpath_core::{EventLog, LogEntry, LogId, LogQuery, NewEntry, StorageError};
use asterpath_protocol::{Tool, ToolArgs, ToolContext, ToolDescriptor, ToolError, ToolOutput};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;

/// The four decorator-backed injection points. `clock:advance` is
/// `ManualClock::advance` and is not represented here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Point {
    AppendBeforeCommit,
    AppendAfterCommit,
    ToolBeforeEffect,
    ToolAfterEffect,
}

impl Point {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AppendBeforeCommit => "append:before_commit",
            Self::AppendAfterCommit => "append:after_commit",
            Self::ToolBeforeEffect => "tool:before_effect",
            Self::ToolAfterEffect => "tool:after_effect",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "append:before_commit" => Some(Self::AppendBeforeCommit),
            "append:after_commit" => Some(Self::AppendAfterCommit),
            "tool:before_effect" => Some(Self::ToolBeforeEffect),
            "tool:after_effect" => Some(Self::ToolAfterEffect),
            _ => None,
        }
    }

    pub const fn is_append(self) -> bool {
        matches!(self, Self::AppendBeforeCommit | Self::AppendAfterCommit)
    }

    pub const fn is_tool(self) -> bool {
        matches!(self, Self::ToolBeforeEffect | Self::ToolAfterEffect)
    }
}

/// What the decorator does when the configured occurrence is reached.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    /// Process-level only: marker, flush, then hang forever.
    Halt,
    /// In-process only: panics with `failpoint <point>`.
    Panic,
}

/// One decorator configuration.
#[derive(Clone, Debug)]
pub struct Failpoint {
    pub point: Point,
    /// One-based hit number; defaults to the first hit.
    pub occurrence: NonZeroUsize,
    pub action: Action,
}

impl Failpoint {
    pub fn halt(point: Point) -> Self {
        Self {
            point,
            occurrence: NonZeroUsize::MIN,
            action: Action::Halt,
        }
    }

    pub fn panic(point: Point) -> Self {
        Self {
            point,
            occurrence: NonZeroUsize::MIN,
            action: Action::Panic,
        }
    }

    #[must_use]
    pub fn with_occurrence(mut self, occurrence: NonZeroUsize) -> Self {
        self.occurrence = occurrence;
        self
    }

    fn hit(&self, hits: &AtomicUsize) -> bool {
        hits.fetch_add(1, Ordering::SeqCst) + 1 == self.occurrence.get()
    }

    async fn fire(&self) {
        match self.action {
            Action::Halt => {
                {
                    let mut stdout = std::io::stdout().lock();
                    // The marker format is a test contract; point names are ASCII.
                    let _ = writeln!(
                        stdout,
                        "{{\"event\":\"failpoint\",\"point\":\"{}\"}}",
                        self.point.as_str()
                    );
                    let _ = stdout.flush();
                }
                std::future::pending::<()>().await;
            }
            Action::Panic => panic!("failpoint {}", self.point.as_str()),
        }
    }
}

/// `EventLog` decorated with one append failpoint.
pub struct FailpointEventLog {
    inner: Arc<dyn EventLog>,
    failpoint: Failpoint,
    hits: AtomicUsize,
}

impl FailpointEventLog {
    pub fn new(inner: Arc<dyn EventLog>, failpoint: Failpoint) -> Self {
        assert!(
            failpoint.point.is_append(),
            "{} is not an append failpoint",
            failpoint.point.as_str()
        );
        Self {
            inner,
            failpoint,
            hits: AtomicUsize::new(0),
        }
    }
}

impl EventLog for FailpointEventLog {
    fn append(&self, entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>> {
        Box::pin(async move {
            if self.failpoint.point == Point::AppendBeforeCommit && self.failpoint.hit(&self.hits) {
                self.failpoint.fire().await;
            }
            let log_id = self.inner.append(entry).await?;
            if self.failpoint.point == Point::AppendAfterCommit && self.failpoint.hit(&self.hits) {
                self.failpoint.fire().await;
            }
            Ok(log_id)
        })
    }

    fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>> {
        self.inner.scan(query)
    }

    fn frontier(
        &self,
        session: asterpath_core::SessionId,
    ) -> BoxFuture<'_, Result<Option<LogId>, StorageError>> {
        self.inner.frontier(session)
    }
}

/// `Tool` decorated with one effect-boundary failpoint.
pub struct FailpointTool {
    inner: Arc<dyn Tool>,
    failpoint: Failpoint,
    hits: AtomicUsize,
}

impl FailpointTool {
    pub fn new(inner: Arc<dyn Tool>, failpoint: Failpoint) -> Self {
        assert!(
            failpoint.point.is_tool(),
            "{} is not a tool failpoint",
            failpoint.point.as_str()
        );
        Self {
            inner,
            failpoint,
            hits: AtomicUsize::new(0),
        }
    }
}

impl Tool for FailpointTool {
    fn descriptor(&self) -> &ToolDescriptor {
        self.inner.descriptor()
    }

    fn invoke(
        &self,
        args: ToolArgs,
        ctx: ToolContext,
    ) -> BoxFuture<'_, Result<ToolOutput, ToolError>> {
        Box::pin(async move {
            if self.failpoint.point == Point::ToolBeforeEffect && self.failpoint.hit(&self.hits) {
                self.failpoint.fire().await;
            }
            let output = self.inner.invoke(args, ctx).await;
            if self.failpoint.point == Point::ToolAfterEffect && self.failpoint.hit(&self.hits) {
                self.failpoint.fire().await;
            }
            output
        })
    }
}
