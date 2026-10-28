//! Whole-log reads: scan a session and decode every record once into a
//! decoded log, with the error mapping each engine part needs.

use std::ops::Deref;

use asterpath_protocol::{
    AgentError, ConfigSnapshot, ExecutionError, LogEntry, LogId, SessionEvent, SessionFacts,
    SessionId, SessionStore, StepId, StorageError, TraceError, TurnId, decode_entry,
    effective_config, scan_all,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ScanError {
    #[error("{source}")]
    Storage {
        session: SessionId,
        source: StorageError,
    },
    #[error("session {session:?} has an undecodable record: {detail}")]
    Decode { session: SessionId, detail: String },
}

/// Every committed record of `session`, decoded, in log order.
pub(crate) async fn scan_decoded(
    log: &dyn SessionStore,
    session: SessionId,
) -> Result<DecodedLog, ScanError> {
    let records = scan_all(log, session)
        .await
        .map_err(|source| ScanError::Storage { session, source })?
        .into_iter()
        .map(|entry| {
            let event = decode_entry(&entry).map_err(|error| ScanError::Decode {
                session,
                detail: error.to_string(),
            })?;
            Ok((entry, event))
        })
        .collect::<Result<_, ScanError>>()?;
    Ok(DecodedLog { session, records })
}

/// One read of a session: every record decoded once, in log order. It
/// answers the questions several engine parts share; a lookup only one
/// caller needs stays with that caller, over these records.
#[derive(Debug)]
pub(crate) struct DecodedLog {
    session: SessionId,
    records: Vec<(LogEntry, SessionEvent)>,
}

impl DecodedLog {
    pub(crate) fn session(&self) -> SessionId {
        self.session
    }

    pub(crate) fn facts(&self) -> SessionFacts {
        SessionFacts::fold_decoded(self.records.iter().map(|(entry, event)| (entry, event)))
    }

    /// Every record, undecoded.
    pub(crate) fn entries(&self) -> Vec<LogEntry> {
        self.records
            .iter()
            .map(|(entry, _)| entry.clone())
            .collect()
    }

    pub(crate) fn seq_of(&self, log_id: LogId) -> Option<u64> {
        self.records
            .iter()
            .find(|(entry, _)| entry.log_id == log_id)
            .map(|(entry, _)| entry.seq)
    }

    /// The `StepStarted` of `step`.
    pub(crate) fn step_started(&self, step: StepId) -> Option<&LogEntry> {
        self.records.iter().find_map(|(entry, event)| match event {
            SessionEvent::StepStarted { step_id, .. } if *step_id == step => Some(entry),
            _ => None,
        })
    }

    /// The effective config before the record `at`: `SessionCreated.config`
    /// with every earlier `ConfigChanged` applied. `None` without
    /// `SessionCreated` or when `at` is not in this log.
    pub(crate) fn config_at(&self, at: LogId) -> Option<ConfigSnapshot> {
        effective_config(&self.records, self.seq_of(at)?)
    }

    /// The effective config after every recorded change.
    pub(crate) fn config(&self) -> Option<ConfigSnapshot> {
        effective_config(&self.records, u64::MAX)
    }

    /// The history a step sees, anchored at its `StepStarted` `at`: every
    /// record before it plus every `CompactionApplied`, which rewrites history
    /// wherever it lands. Shared by the agent's request and compaction input
    /// and by spawn forks. `None` when `at` is not in this log.
    pub(crate) fn step_prefix(&self, at: LogId) -> Option<Vec<LogEntry>> {
        let at = self.seq_of(at)?;
        Some(
            self.records
                .iter()
                .filter(|(entry, event)| {
                    entry.seq < at || matches!(event, SessionEvent::CompactionApplied { .. })
                })
                .map(|(entry, _)| entry.clone())
                .collect(),
        )
    }

    /// A session fork's waterline and prefix (ADR-0033, ADR-0045). The
    /// waterline is `at`'s `TurnCommitted`, or without `at` the last one, or
    /// `SessionCreated` when none has committed; nothing after it is copied,
    /// compactions included, so a turn in progress or a later compaction
    /// never leaks. Inputs recorded after the waterline turn's last
    /// `StepStarted` belong to the next turn. `None` for an empty log or an
    /// `at` that is not a committed turn of this log.
    pub(crate) fn fork_prefix(&self, at: Option<TurnId>) -> Option<(LogEntry, Vec<LogEntry>)> {
        let entries = &self.records;
        let committed = |event: &SessionEvent| match event {
            SessionEvent::TurnCommitted { turn_id, .. } => at.is_none_or(|at| *turn_id == at),
            _ => false,
        };
        let found = entries.iter().rev().find(|(_, event)| committed(event));
        let waterline = match at {
            Some(_) => found?,
            None => found.or_else(|| entries.first())?,
        }
        .0
        .clone();
        let last_step = entries
            .iter()
            .filter(|(entry, event)| {
                entry.seq <= waterline.seq && matches!(event, SessionEvent::StepStarted { .. })
            })
            .map(|(entry, _)| entry.seq)
            .max()
            .unwrap_or(0);
        let prefix = entries
            .iter()
            .filter(|(entry, event)| match event {
                SessionEvent::InputRecorded { .. } => entry.seq <= last_step,
                _ => entry.seq <= waterline.seq,
            })
            .map(|(entry, _)| entry.clone())
            .collect();
        Some((waterline, prefix))
    }
}

impl Deref for DecodedLog {
    type Target = [(LogEntry, SessionEvent)];

    fn deref(&self) -> &Self::Target {
        &self.records
    }
}

impl<'a> IntoIterator for &'a DecodedLog {
    type Item = &'a (LogEntry, SessionEvent);
    type IntoIter = std::slice::Iter<'a, (LogEntry, SessionEvent)>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.iter()
    }
}

impl IntoIterator for DecodedLog {
    type Item = (LogEntry, SessionEvent);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.records.into_iter()
    }
}

pub(crate) fn backend(error: impl std::fmt::Display) -> ExecutionError {
    ExecutionError::Backend(error.to_string())
}

impl From<ScanError> for AgentError {
    fn from(error: ScanError) -> Self {
        match error {
            ScanError::Storage { source, .. } => Self::Storage(source),
            ScanError::Decode { session, detail } => Self::Corrupt { session, detail },
        }
    }
}

impl From<ScanError> for ExecutionError {
    fn from(error: ScanError) -> Self {
        backend(error)
    }
}

impl From<ScanError> for TraceError {
    fn from(error: ScanError) -> Self {
        match error {
            ScanError::Storage {
                session,
                source: StorageError::Corrupt { detail, .. },
            } => Self::Corrupt { session, detail },
            ScanError::Storage { source, .. } => Self::Backend(source.to_string()),
            ScanError::Decode { session, detail } => Self::Corrupt { session, detail },
        }
    }
}

impl From<ScanError> for crate::scheduler::WaitError {
    fn from(error: ScanError) -> Self {
        Self::Execution(error.into())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use asterpath_protocol::{
        AgentError, CatalogEntry, Claim, LogEntry, LogId, LogIdRange, LogQuery, ModelId, ModelSpec,
        NewEntry, OwnerToken, Part, ProviderId, RetirementError, SessionEvent, SessionId,
        SessionQuery, SessionStore, StepId, StorageError, TurnEndReason, TurnId, Usage,
        decode_entry,
    };
    use asterpath_testing::{InMemoryEventLog, ScriptedProvider};
    use futures_core::future::BoxFuture;
    use futures_core::stream::BoxStream;
    use futures_util::StreamExt;

    use crate::commit::CommitLog;

    const SESSION: SessionId = SessionId::from_bytes([5; 16]);
    const TURN: TurnId = TurnId::from_bytes([6; 16]);
    const STEP: StepId = StepId::from_bytes([7; 16]);

    /// The facade's commit log over `store`.
    fn commit_log(store: Arc<dyn SessionStore>) -> Arc<CommitLog> {
        crate::Asterpath::from_config(crate::AsterpathConfig {
            store: Some(store),
            providers: vec![Arc::new(ScriptedProvider::new("p"))],
            ..crate::AsterpathConfig::default()
        })
        .expect("assemble")
        .store
    }

    fn kinds(prefix: &[LogEntry]) -> Vec<&'static str> {
        prefix
            .iter()
            .map(|entry| decode_entry(entry).expect("decodes").kind())
            .collect()
    }

    /// Input, a turn with one step that commits, then a compaction after the
    /// turn's waterline. Returns the step's `StepStarted` id.
    async fn seeded(log: &CommitLog) -> LogId {
        let append = |event| log.append_event(SESSION, event, None);
        let input = append(SessionEvent::InputRecorded { input: Vec::new() })
            .await
            .expect("input");
        append(SessionEvent::TurnStarted { turn_id: TURN })
            .await
            .expect("turn");
        let step = append(SessionEvent::StepStarted {
            turn_id: TURN,
            step_id: STEP,
        })
        .await
        .expect("step");
        append(SessionEvent::StepCommitted {
            turn_id: TURN,
            step_id: STEP,
        })
        .await
        .expect("step committed");
        append(SessionEvent::TurnCommitted {
            turn_id: TURN,
            reason: TurnEndReason::Completed,
        })
        .await
        .expect("turn committed");
        append(SessionEvent::CompactionApplied {
            first_kept: step,
            summary: vec![Part::Text {
                text: "summary".to_owned(),
            }],
            replaces: LogIdRange {
                first: input,
                last: input,
            },
            model: ModelSpec {
                provider: ProviderId::new("p"),
                model: ModelId::new("m"),
                context_window: None,
            },
            usage: Usage::default(),
            raw: None,
        })
        .await
        .expect("compaction");
        step
    }

    #[tokio::test]
    async fn the_step_prefix_carries_compactions_recorded_after_the_step() {
        let log = commit_log(Arc::new(InMemoryEventLog::new()));
        let step = seeded(&log).await;

        let decoded = log.read(SESSION).await.expect("read");
        let prefix = decoded.step_prefix(step).expect("step is in the log");

        assert_eq!(
            kinds(&prefix),
            ["InputRecorded", "TurnStarted", "CompactionApplied"]
        );
    }

    #[tokio::test]
    async fn the_fork_prefix_leaves_out_compactions_after_the_waterline() {
        let log = commit_log(Arc::new(InMemoryEventLog::new()));
        seeded(&log).await;

        let decoded = log.read(SESSION).await.expect("read");
        let (waterline, prefix) = decoded.fork_prefix(Some(TURN)).expect("turn committed");

        assert_eq!(
            decode_entry(&waterline).expect("decodes").kind(),
            "TurnCommitted"
        );
        assert_eq!(
            kinds(&prefix),
            [
                "InputRecorded",
                "TurnStarted",
                "StepStarted",
                "StepCommitted",
                "TurnCommitted"
            ]
        );
    }

    #[tokio::test]
    async fn an_undecodable_record_reads_as_corrupt() {
        let log = commit_log(Arc::new(Undecodable(InMemoryEventLog::new())));

        let error = AgentError::from(log.read(SESSION).await.unwrap_err());

        assert!(
            matches!(error, AgentError::Corrupt { session, .. } if session == SESSION),
            "{error:?}"
        );
    }

    /// A store whose every session holds one record no event decodes from.
    struct Undecodable(InMemoryEventLog);

    impl SessionStore for Undecodable {
        fn append(&self, entry: NewEntry) -> BoxFuture<'_, Result<LogId, StorageError>> {
            self.0.append(entry)
        }

        fn scan(&self, query: LogQuery) -> BoxStream<'_, Result<LogEntry, StorageError>> {
            let entry = LogEntry {
                log_id: LogId::from_i64(1),
                session: query.session,
                seq: 1,
                recorded_at: 0,
                schema_version: asterpath_protocol::LOG_SCHEMA_VERSION,
                dedup_key: None,
                payload: serde_json::json!({"NoSuchEvent": {}}),
            };
            futures_util::stream::once(async move { Ok(entry) }).boxed()
        }

        fn frontier(
            &self,
            session: SessionId,
        ) -> BoxFuture<'_, Result<Option<LogId>, StorageError>> {
            self.0.frontier(session)
        }

        fn sessions(
            &self,
            query: SessionQuery,
        ) -> BoxFuture<'_, Result<Vec<CatalogEntry>, StorageError>> {
            self.0.sessions(query)
        }

        fn parent(
            &self,
            session: SessionId,
        ) -> BoxFuture<'_, Result<Option<SessionId>, StorageError>> {
            self.0.parent(session)
        }

        fn claim(&self, root: SessionId) -> BoxFuture<'_, Result<Claim, StorageError>> {
            self.0.claim(root)
        }

        fn owner(
            &self,
            root: SessionId,
        ) -> BoxFuture<'_, Result<Option<OwnerToken>, StorageError>> {
            self.0.owner(root)
        }

        fn renew(&self) -> BoxFuture<'_, Result<Vec<OwnerToken>, StorageError>> {
            self.0.renew()
        }

        fn retire(
            &self,
            session: SessionId,
            expected_frontier: LogId,
        ) -> BoxFuture<'_, Result<(), RetirementError>> {
            self.0.retire(session, expected_frontier)
        }
    }
}
