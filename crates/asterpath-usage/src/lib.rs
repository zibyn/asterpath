//! Native usage accounting: a deterministic fold over the event log.
//!
//! Usage is never stored separately. This module reads the three terminal
//! records that carry it (`AssistantMessage`, `CompactionApplied`,
//! `ModelCallAborted`) and folds them into buckets. Descendants come from the
//! injected `SessionCatalog`; a subtree report keeps `own` and `descendants`
//! apart so nothing is double-counted.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use asterpath_core::{
    DedupKey, EventLog, LogEntry, LogQuery, SessionCatalog, SessionId, StepId, StorageError, TurnId,
};
use asterpath_protocol::{
    Usage, UsageBucket, UsageEntry, UsageError, UsageFilter, UsageQuery, UsageRecord, UsageReport,
    UsageScope, UsageTotals, decode_entry, usage_of,
};
use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use futures_util::stream;

/// Read-side implementation over one `EventLog` and the session tree.
pub struct UsageService {
    log: Arc<dyn EventLog>,
    catalog: Arc<dyn SessionCatalog>,
}

impl UsageService {
    pub fn new(log: Arc<dyn EventLog>, catalog: Arc<dyn SessionCatalog>) -> Self {
        Self { log, catalog }
    }

    async fn live(&self, session: SessionId) -> Result<bool, UsageError> {
        match self.log.frontier(session).await {
            Ok(frontier) => Ok(frontier.is_some()),
            Err(error) => Err(storage_error(session, error)),
        }
    }

    /// The root session and, for `SessionSubtree`, surviving descendants.
    async fn sessions_for(&self, scope: &UsageScope) -> Result<Vec<SessionId>, UsageError> {
        let root = match *scope {
            UsageScope::Session(session)
            | UsageScope::Turn { session, .. }
            | UsageScope::Step { session, .. } => {
                if !self.live(session).await? {
                    return Err(UsageError::NotFound { session });
                }
                return Ok(vec![session]);
            }
            UsageScope::SessionSubtree(root) => root,
        };
        if !self.live(root).await? {
            return Err(UsageError::NotFound { session: root });
        }

        let mut sessions = vec![root];
        let mut queue = VecDeque::from([root]);
        while let Some(parent) = queue.pop_front() {
            let mut children = self.catalog.children(parent);
            while let Some(child) = children.next().await {
                let child = child.map_err(|error| UsageError::Backend(error.to_string()))?;
                if self.live(child).await? {
                    sessions.push(child);
                    queue.push_back(child);
                }
            }
        }
        Ok(sessions)
    }

    async fn next_entry(
        &self,
        session: SessionId,
        after: Option<asterpath_core::LogId>,
    ) -> Result<Option<LogEntry>, UsageError> {
        let mut entries = self.log.scan(LogQuery {
            session,
            after,
            limit: Some(1),
        });
        match entries.next().await {
            None => Ok(None),
            Some(Ok(entry)) => Ok(Some(entry)),
            Some(Err(error)) => Err(storage_error(session, error)),
        }
    }

    async fn next_record(
        &self,
        scan: &mut SessionScan,
        filter: &UsageFilter,
    ) -> Result<Option<UsageRecord>, UsageError> {
        loop {
            let Some(entry) = self.next_entry(scan.session, scan.after).await? else {
                return Ok(None);
            };
            scan.after = Some(entry.log_id);
            if let Some(record) = evaluate(scan, &entry, filter)? {
                return Ok(Some(record));
            }
        }
    }
}

impl UsageQuery for UsageService {
    fn records(&self, filter: UsageFilter) -> BoxStream<'_, Result<UsageRecord, UsageError>> {
        let state = Records {
            filter,
            phase: Phase::Start,
            sessions: VecDeque::new(),
            scan: None,
            emitted: 0,
        };
        stream::unfold(state, move |mut state| async move {
            loop {
                match state.phase {
                    Phase::Start => match self.sessions_for(&state.filter.scope).await {
                        Ok(sessions) => {
                            state.sessions = sessions.into();
                            state.phase = Phase::Scan;
                        }
                        Err(error) => {
                            state.phase = Phase::Done;
                            return Some((Err(error), state));
                        }
                    },
                    Phase::Scan => {
                        if state
                            .filter
                            .limit
                            .is_some_and(|limit| state.emitted >= limit)
                        {
                            return None;
                        }
                        let session = state.sessions.front().copied()?;
                        let mut scan = state
                            .scan
                            .take()
                            .unwrap_or_else(|| SessionScan::new(session));
                        match self.next_record(&mut scan, &state.filter).await {
                            Ok(Some(record)) => {
                                state.emitted += 1;
                                state.scan = Some(scan);
                                return Some((Ok(record), state));
                            }
                            Ok(None) => {
                                state.sessions.pop_front();
                                state.scan = None;
                            }
                            Err(error) => {
                                state.phase = Phase::Done;
                                return Some((Err(error), state));
                            }
                        }
                    }
                    Phase::Done => return None,
                }
            }
        })
        .boxed()
    }

    fn totals(&self, filter: UsageFilter) -> BoxFuture<'_, Result<UsageReport, UsageError>> {
        Box::pin(async move {
            let sessions = self.sessions_for(&filter.scope).await?;
            let mut report = UsageReport::default();
            for (index, session) in sessions.iter().enumerate() {
                let mut scan = SessionScan::new(*session);
                loop {
                    let Some(entry) = self.next_entry(*session, scan.after).await? else {
                        break;
                    };
                    scan.after = Some(entry.log_id);
                    if let Some(record) = evaluate(&mut scan, &entry, &filter)? {
                        let totals = if index == 0 {
                            &mut report.own
                        } else {
                            &mut report.descendants
                        };
                        add_usage(totals, record.bucket, record.usage);
                    }
                }
            }
            Ok(report)
        })
    }
}

enum Phase {
    Start,
    Scan,
    Done,
}

struct Records {
    filter: UsageFilter,
    phase: Phase,
    sessions: VecDeque<SessionId>,
    scan: Option<SessionScan>,
    emitted: usize,
}

struct SessionScan {
    session: SessionId,
    after: Option<asterpath_core::LogId>,
    /// `CompactionStarted` dedup key -> the turn it recorded.
    started: HashMap<DedupKey, Option<TurnId>>,
}

impl SessionScan {
    fn new(session: SessionId) -> Self {
        Self {
            session,
            after: None,
            started: HashMap::new(),
        }
    }
}

/// Fold one entry into state and, if it carries usage that the scope and
/// filter accept, return the sample.
fn evaluate(
    scan: &mut SessionScan,
    entry: &LogEntry,
    filter: &UsageFilter,
) -> Result<Option<UsageRecord>, UsageError> {
    let event = decode_entry(entry).map_err(|error| UsageError::Corrupt {
        session: scan.session,
        detail: error.to_string(),
    })?;

    if let (asterpath_protocol::SessionEvent::CompactionStarted { turn, .. }, Some(key)) =
        (&event, &entry.dedup_key)
    {
        scan.started.insert(key.clone(), *turn);
    }
    let compaction_turn = entry
        .dedup_key
        .as_ref()
        .and_then(|key| scan.started.get(key))
        .copied()
        .flatten();
    let Some(UsageEntry {
        bucket,
        usage,
        model,
        turn,
        step,
    }) = usage_of(&event, compaction_turn)
    else {
        return Ok(None);
    };

    if !scope_accepts(&filter.scope, bucket, turn, step) {
        return Ok(None);
    }
    if filter.bucket.is_some_and(|wanted| wanted != bucket) {
        return Ok(None);
    }
    if filter
        .model
        .as_ref()
        .is_some_and(|wanted| wanted != &model.model)
    {
        return Ok(None);
    }
    if filter.since.is_some_and(|since| entry.recorded_at < since) {
        return Ok(None);
    }
    if filter.until.is_some_and(|until| entry.recorded_at >= until) {
        return Ok(None);
    }

    Ok(Some(UsageRecord {
        session: scan.session,
        turn,
        step,
        model,
        bucket,
        usage,
        log: entry.log_id,
        recorded_at: entry.recorded_at,
    }))
}

/// Turn and step scopes are narrower than the log: a step is model calls only,
/// and a turn is model calls plus compactions attributed to it.
fn scope_accepts(
    scope: &UsageScope,
    bucket: UsageBucket,
    turn: Option<TurnId>,
    step: Option<StepId>,
) -> bool {
    match scope {
        UsageScope::Session(_) | UsageScope::SessionSubtree(_) => true,
        UsageScope::Turn { turn: wanted, .. } => {
            matches!(bucket, UsageBucket::ModelCall | UsageBucket::Compaction)
                && turn == Some(*wanted)
        }
        UsageScope::Step {
            turn: wanted,
            step: wanted_step,
            ..
        } => {
            bucket == UsageBucket::ModelCall && turn == Some(*wanted) && step == Some(*wanted_step)
        }
    }
}

fn add_usage(totals: &mut UsageTotals, bucket: UsageBucket, usage: Usage) {
    let target = match bucket {
        UsageBucket::ModelCall => &mut totals.model_calls,
        UsageBucket::Compaction => &mut totals.compaction,
        UsageBucket::Partial => &mut totals.partial,
    };
    target.input.uncached += usage.input.uncached;
    target.input.cache_read += usage.input.cache_read;
    target.input.cache_write += usage.input.cache_write;
    target.output.total += usage.output.total;
    target.output.reasoning += usage.output.reasoning;
}

fn storage_error(session: SessionId, error: StorageError) -> UsageError {
    match error {
        StorageError::Corrupt { detail, .. } => UsageError::Corrupt { session, detail },
        other => UsageError::Backend(other.to_string()),
    }
}
