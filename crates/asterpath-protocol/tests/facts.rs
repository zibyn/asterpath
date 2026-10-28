//! Session facts: hand-written event sequences folded once, then every
//! question recovery, admission, wake and inspection ask is checked.

use asterpath_protocol::*;

fn session(byte: u8) -> SessionId {
    SessionId::from_bytes([byte; 16])
}

fn turn(byte: u8) -> TurnId {
    TurnId::from_bytes([byte; 16])
}

fn step(byte: u8) -> StepId {
    StepId::from_bytes([byte; 16])
}

fn call(byte: u8) -> ToolCallId {
    ToolCallId::from_bytes([byte; 16])
}

fn config() -> ConfigSnapshot {
    ConfigSnapshot {
        system_sections: Vec::new(),
        model: ModelSpec {
            provider: ProviderId::new("test"),
            model: ModelId::new("model"),
            context_window: Some(1000),
        },
        system: vec![],
        tools: vec![],
        request: RequestConfig::default(),
        turn: TurnConfig::default(),
        compaction: CompactionConfig::default(),
        subagents: SubagentConfig::default(),
    }
}

fn created(parent: Option<ParentRef>) -> SessionEvent {
    SessionEvent::SessionCreated {
        config: config(),
        parent,
        metadata: Default::default(),
    }
}

fn input() -> SessionEvent {
    SessionEvent::InputRecorded {
        input: vec![Part::Text {
            text: "hi".to_owned(),
        }],
    }
}

fn turn_started(t: u8) -> SessionEvent {
    SessionEvent::TurnStarted { turn_id: turn(t) }
}

fn turn_committed(t: u8) -> SessionEvent {
    SessionEvent::TurnCommitted {
        turn_id: turn(t),
        reason: TurnEndReason::Completed,
    }
}

fn step_started(t: u8, s: u8) -> SessionEvent {
    SessionEvent::StepStarted {
        turn_id: turn(t),
        step_id: step(s),
    }
}

fn step_committed(t: u8, s: u8) -> SessionEvent {
    SessionEvent::StepCommitted {
        turn_id: turn(t),
        step_id: step(s),
    }
}

fn requested(t: u8, s: u8, c: u8, name: &str) -> SessionEvent {
    SessionEvent::ToolCallRequested {
        turn_id: turn(t),
        step_id: step(s),
        tool_call_id: call(c),
        provider_call_id: format!("p{c}"),
        name: ToolName::new(name),
        raw_args: JsonString::new("{}"),
        replay: ReplaySafety::Safe,
        provenance: Provenance::InProcess,
    }
}

fn result(c: u8) -> SessionEvent {
    SessionEvent::ToolResult {
        tool_call_id: call(c),
        status: ToolResultStatus::Completed,
        output: None,
        error: None,
        truncated: false,
    }
}

fn spawned(child: u8, parent_call: u8, deadline: Option<Timestamp>) -> SessionEvent {
    SessionEvent::ChildSessionSpawned {
        child: session(child),
        task: vec![],
        config: config(),
        context: ContextPolicy::Fresh,
        parent_tool_call_id: call(parent_call),
        caused_by: LogId::from_i64(1),
        deadline,
    }
}

fn completed(child: u8) -> SessionEvent {
    SessionEvent::ChildSessionCompleted {
        child: session(child),
        outcome: ChildOutcome::Cancelled {
            reason: CancelReason::Parent,
        },
    }
}

fn compaction_started(trigger: CompactionTrigger) -> SessionEvent {
    SessionEvent::CompactionStarted {
        trigger,
        replaces_from: LogId::from_i64(1),
        caused_by: LogId::from_i64(1),
        turn: None,
    }
}

fn compaction_aborted() -> SessionEvent {
    SessionEvent::CompactionAborted {
        reason: CompactionAbortReason::SummaryFailed,
    }
}

/// Every answer the facts give, in comparable form.
#[derive(Debug, PartialEq)]
struct Answers {
    state: SessionState,
    idle: bool,
    open_step: Option<(TurnId, StepId, u64)>,
    tools: Vec<(ToolCallId, TurnId, StepId, String, u64, Option<SessionId>)>,
    children: Vec<(SessionId, ToolCallId, Option<Timestamp>, u64)>,
    compactions: Vec<(String, CompactionTrigger, u64)>,
    pending_input: bool,
    parent: Option<SessionId>,
    last: Option<(u64, LogId)>,
}

impl Default for Answers {
    fn default() -> Self {
        Self {
            state: SessionState::Idle,
            idle: true,
            open_step: None,
            tools: vec![],
            children: vec![],
            compactions: vec![],
            pending_input: false,
            parent: None,
            last: None,
        }
    }
}

fn answers(facts: &SessionFacts) -> Answers {
    Answers {
        state: facts.state(),
        idle: facts.is_idle(),
        open_step: facts
            .open_step()
            .map(|open| (open.turn, open.step, open.seq)),
        tools: facts
            .open_tools()
            .map(|(id, tool)| {
                (
                    id,
                    tool.turn,
                    tool.step,
                    tool.name.as_str().to_owned(),
                    tool.seq,
                    tool.spawned,
                )
            })
            .collect(),
        children: facts
            .open_children()
            .map(|(id, child)| (id, child.parent_tool_call, child.deadline, child.seq))
            .collect(),
        compactions: facts
            .open_compactions()
            .map(|open| (open.key.as_str().to_owned(), open.trigger, open.seq))
            .collect(),
        pending_input: facts.has_pending_input(),
        parent: facts.parent().map(|parent| parent.session),
        last: facts.last(),
    }
}

type Keyed = (SessionEvent, Option<DedupKey>);

/// Fold through the per-event entry: seq is 1-based position, log id is 10x.
fn fold(events: &[Keyed]) -> SessionFacts {
    let mut facts = SessionFacts::default();
    for (index, (event, key)) in events.iter().enumerate() {
        let seq = index as u64 + 1;
        let entry = LogEntry {
            log_id: LogId::from_i64(seq as i64 * 10),
            session: session(0),
            seq,
            recorded_at: 0,
            schema_version: LOG_SCHEMA_VERSION,
            dedup_key: key.clone(),
            payload: serde_json::Value::Null,
        };
        facts.apply(&entry, event);
    }
    facts
}

fn plain(events: Vec<SessionEvent>) -> Vec<Keyed> {
    events.into_iter().map(|event| (event, None)).collect()
}

#[test]
fn session_facts_answer_every_question() {
    let parent = ParentRef {
        session: session(7),
        turn: turn(7),
        tool_call: call(7),
    };
    let compaction = dedup::compaction(LogId::from_i64(4));
    let cases: Vec<(&str, Vec<Keyed>, Answers)> = vec![
        ("empty log", vec![], Answers::default()),
        (
            "parent and pending input",
            plain(vec![created(Some(parent.clone())), input()]),
            Answers {
                pending_input: true,
                parent: Some(session(7)),
                last: Some((2, LogId::from_i64(20))),
                ..Answers::default()
            },
        ),
        (
            "StepStarted clears pending input; open turn and step with seq",
            plain(vec![
                created(None),
                input(),
                turn_started(1),
                step_started(1, 1),
            ]),
            Answers {
                state: SessionState::TurnOpen {
                    turn: turn(1),
                    step: Some(StepState::InProgress),
                },
                idle: false,
                open_step: Some((turn(1), step(1), 4)),
                last: Some((4, LogId::from_i64(40))),
                ..Answers::default()
            },
        ),
        (
            "input after the step starts is pending again",
            plain(vec![turn_started(1), step_started(1, 1), input()]),
            Answers {
                state: SessionState::TurnOpen {
                    turn: turn(1),
                    step: Some(StepState::InProgress),
                },
                idle: false,
                open_step: Some((turn(1), step(1), 2)),
                pending_input: true,
                last: Some((3, LogId::from_i64(30))),
                ..Answers::default()
            },
        ),
        (
            "committed step closes; turn stays open between steps",
            plain(vec![
                turn_started(1),
                step_started(1, 1),
                step_committed(1, 1),
            ]),
            Answers {
                state: SessionState::TurnOpen {
                    turn: turn(1),
                    step: None,
                },
                idle: false,
                last: Some((3, LogId::from_i64(30))),
                ..Answers::default()
            },
        ),
        (
            "open tools with turn, step, name and seq; results close them",
            plain(vec![
                turn_started(1),
                step_started(1, 2),
                requested(1, 2, 1, "read"),
                requested(1, 2, 2, "spawn_agent"),
                requested(1, 2, 3, "write"),
                result(1),
            ]),
            Answers {
                state: SessionState::TurnOpen {
                    turn: turn(1),
                    step: Some(StepState::InProgress),
                },
                idle: false,
                open_step: Some((turn(1), step(2), 2)),
                tools: vec![
                    (call(2), turn(1), step(2), "spawn_agent".to_owned(), 4, None),
                    (call(3), turn(1), step(2), "write".to_owned(), 5, None),
                ],
                last: Some((6, LogId::from_i64(60))),
                ..Answers::default()
            },
        ),
        (
            "open children carry parent tool call and deadline; the tool records its child",
            plain(vec![
                turn_started(1),
                step_started(1, 1),
                requested(1, 1, 1, "spawn_agent"),
                requested(1, 1, 2, "spawn_agent"),
                spawned(8, 1, Some(500)),
                spawned(9, 2, None),
                completed(9),
            ]),
            Answers {
                state: SessionState::TurnOpen {
                    turn: turn(1),
                    step: Some(StepState::Waiting),
                },
                idle: false,
                open_step: Some((turn(1), step(1), 2)),
                tools: vec![
                    (
                        call(1),
                        turn(1),
                        step(1),
                        "spawn_agent".to_owned(),
                        3,
                        Some(session(8)),
                    ),
                    (
                        call(2),
                        turn(1),
                        step(1),
                        "spawn_agent".to_owned(),
                        4,
                        Some(session(9)),
                    ),
                ],
                children: vec![(session(8), call(1), Some(500), 5)],
                last: Some((7, LogId::from_i64(70))),
                ..Answers::default()
            },
        ),
        (
            "a dangling compaction is open but never makes the session busy",
            vec![(
                compaction_started(CompactionTrigger::Manual),
                Some(compaction.clone()),
            )],
            Answers {
                compactions: vec![("compaction:4".to_owned(), CompactionTrigger::Manual, 1)],
                last: Some((1, LogId::from_i64(10))),
                ..Answers::default()
            },
        ),
        (
            "only the terminal with the same dedup key closes a compaction",
            vec![
                (
                    compaction_started(CompactionTrigger::Threshold),
                    Some(compaction.clone()),
                ),
                (compaction_aborted(), Some(DedupKey::new("compaction:999"))),
                (compaction_started(CompactionTrigger::Manual), None),
            ],
            Answers {
                compactions: vec![("compaction:4".to_owned(), CompactionTrigger::Threshold, 1)],
                last: Some((3, LogId::from_i64(30))),
                ..Answers::default()
            },
        ),
        (
            "a full turn folds back to idle",
            vec![
                (turn_started(1), None),
                (step_started(1, 1), None),
                (requested(1, 1, 1, "spawn_agent"), None),
                (spawned(8, 1, None), None),
                (
                    compaction_started(CompactionTrigger::Threshold),
                    Some(compaction.clone()),
                ),
                (compaction_aborted(), Some(compaction.clone())),
                (completed(8), None),
                (result(1), None),
                (step_committed(1, 1), None),
                (turn_committed(1), None),
            ],
            Answers {
                last: Some((10, LogId::from_i64(100))),
                ..Answers::default()
            },
        ),
    ];

    for (name, events, expected) in cases {
        assert_eq!(answers(&fold(&events)), expected, "{name}");
    }
}

#[test]
fn batch_fold_sorts_by_seq_and_matches_the_per_event_fold() {
    let events = plain(vec![
        created(None),
        input(),
        turn_started(1),
        step_started(1, 1),
        requested(1, 1, 1, "spawn_agent"),
        spawned(8, 1, Some(9)),
    ]);
    let mut entries: Vec<LogEntry> = events
        .iter()
        .enumerate()
        .map(|(index, (event, key))| {
            let new = encode_entry(session(1), key.clone(), event).expect("encode");
            let seq = index as u64 + 1;
            LogEntry {
                log_id: LogId::from_i64(seq as i64 * 10),
                session: new.session,
                seq,
                recorded_at: 0,
                schema_version: new.schema_version,
                dedup_key: new.dedup_key,
                payload: new.payload,
            }
        })
        .collect();
    entries.reverse();

    let facts = SessionFacts::fold(&entries).expect("fold");
    assert_eq!(answers(&facts), answers(&fold(&events)));
}

#[test]
fn batch_fold_reports_an_undecodable_record() {
    let entry = LogEntry {
        log_id: LogId::from_i64(1),
        session: session(1),
        seq: 1,
        recorded_at: 0,
        schema_version: LOG_SCHEMA_VERSION,
        dedup_key: None,
        payload: serde_json::json!({"type": "NoSuchEvent"}),
    };
    assert!(SessionFacts::fold(&[entry]).is_err());
}

#[test]
fn open_intents_list_what_is_still_open() {
    let facts = fold(&[
        (turn_started(1), None),
        (step_started(1, 1), None),
        (requested(1, 1, 1, "spawn_agent"), None),
        (spawned(8, 1, None), None),
        (
            compaction_started(CompactionTrigger::Manual),
            Some(dedup::compaction(LogId::from_i64(4))),
        ),
    ]);
    assert_eq!(
        facts.open_intents(),
        vec![
            OpenIntent::Turn(turn(1)),
            OpenIntent::Step(step(1)),
            OpenIntent::Tool(call(1)),
            OpenIntent::Child(session(8)),
            OpenIntent::Compaction(LogId::from_i64(4)),
        ]
    );
}

#[test]
fn wakeable_is_an_open_non_compaction_intent_or_pending_input() {
    let compaction = || {
        (
            compaction_started(CompactionTrigger::Manual),
            Some(dedup::compaction(LogId::from_i64(4))),
        )
    };
    let cases: Vec<(&str, Vec<Keyed>, bool)> = vec![
        ("empty log", vec![], false),
        ("created only", plain(vec![created(None)]), false),
        ("pending input", plain(vec![created(None), input()]), true),
        ("open turn", plain(vec![turn_started(1)]), true),
        (
            "committed turn",
            plain(vec![turn_started(1), turn_committed(1)]),
            false,
        ),
        ("only a dangling compaction", vec![compaction()], false),
        (
            "steering input after the step started",
            plain(vec![
                turn_started(1),
                step_started(1, 1),
                step_committed(1, 1),
                turn_committed(1),
                input(),
            ]),
            true,
        ),
    ];
    for (name, events, wakeable) in cases {
        assert_eq!(fold(&events).is_wakeable(), wakeable, "{name}");
    }
}
