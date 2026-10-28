//! The session driver (GLOSSARY「会话驱动」): one loop drives a session tree
//! for every frontend. The core opens the tree's approval endpoint (if the
//! frontend has an approval channel), drives to idle, drives again while
//! input is pending after a drive ended normally, and stops on an error. The
//! optional operator layer hands every approval of the tree, its children's
//! included, to an [`Operator`] and cancels the turn when nobody answers.
// `AsterError` is the facade's error, returned as is.
#![allow(clippy::result_large_err)]

use std::collections::HashSet;
use std::time::Duration;

use asterpath::{AsterError, Asterpath, OwnedSession, SessionHandle};
use asterpath_protocol::{
    AgentEvent, ApprovalDecision, ApprovalRequest, Clock, InputId, Part, SystemClock,
    TurnEndReason, Usage, UserInput, unique_bytes,
};
use futures_util::StreamExt;

#[derive(Default)]
pub struct DriveOutcome {
    /// Whether anything was driven.
    pub ran: bool,
    pub idle: bool,
    pub reason: Option<TurnEndReason>,
    /// The usage of the last committed turn.
    pub usage: Option<Usage>,
}

impl DriveOutcome {
    /// Fold one live event into the outcome.
    pub fn observe(&mut self, event: &AgentEvent) {
        match event {
            AgentEvent::TurnCommitted { reason, usage, .. } => {
                self.reason = Some(reason.clone());
                self.usage = Some(*usage);
            }
            AgentEvent::Idle { .. } => self.idle = true,
            _ => {}
        }
    }
}

/// Who answers the tree's approvals while it is driven.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Approvals {
    /// No channel: no endpoint is registered, so the framework denies every `Ask`.
    Denied,
    /// The endpoint is open and the application pulls the requests itself
    /// (`ApprovalGate::pending_in`); the operator is never asked.
    Pulled,
    /// The endpoint is open and every request goes to [`Operator::decide`].
    Asked,
}

/// What an operator does in the middle of a drive.
#[derive(Debug, PartialEq)]
pub enum Interrupt {
    /// Cancel the turn; the drive runs on to its end.
    Cancel,
    /// Log input; it steers the turn or starts the next drive.
    Input(Vec<Part>),
}

/// Who watches a drive and answers its approvals.
pub trait Operator {
    fn approvals(&self) -> Approvals {
        Approvals::Asked
    }

    /// A live event to show; approval requests go to [`Operator::decide`].
    fn show(&mut self, _event: &AgentEvent) {}

    /// The answer to an approval request of any session in the tree; `None`
    /// cancels the turn.
    fn decide(
        &mut self,
        _request: &ApprovalRequest,
    ) -> impl Future<Output = Option<ApprovalDecision>> {
        std::future::ready(Some(ApprovalDecision::Deny { reason: None }))
    }

    /// Resolves when the operator interrupts the drive; never by default.
    /// Raced against the drive, so it must be cancel-safe.
    fn interrupted(&mut self) -> impl Future<Output = Interrupt> {
        std::future::pending()
    }
}

/// No operator: nothing is shown and the framework denies every approval.
pub struct Headless;

impl Operator for Headless {
    fn approvals(&self) -> Approvals {
        Approvals::Denied
    }
}

/// How often a drive looks for child sessions' approval requests.
const CHILD_APPROVAL_POLL: Duration = Duration::from_millis(100);

/// Drive `handle`'s tree until it is idle with no input pending. A drive
/// that fails is not repeated: old pending input would fail again.
pub async fn drive(
    asterpath: &Asterpath,
    handle: &OwnedSession,
    operator: &mut impl Operator,
) -> Result<DriveOutcome, AsterError> {
    let approvals = operator.approvals();
    let _endpoint = (approvals != Approvals::Denied)
        .then(|| asterpath.approval_gate().register(handle.owner().root));
    let mut outcome = DriveOutcome {
        ran: true,
        ..DriveOutcome::default()
    };
    loop {
        drive_once(asterpath, handle, operator, approvals, &mut outcome).await?;
        if !handle.facts().await?.has_pending_input() {
            return Ok(outcome);
        }
    }
}

/// One drive to `TurnCommitted` or `Idle`. Child sessions' requests never
/// reach this stream, so the tree's pending list is polled for them.
async fn drive_once(
    asterpath: &Asterpath,
    handle: &OwnedSession,
    operator: &mut impl Operator,
    approvals: Approvals,
    outcome: &mut DriveOutcome,
) -> Result<(), AsterError> {
    let asked = approvals == Approvals::Asked;
    let gate = asterpath.approval_gate();
    let mut children = HashSet::new();
    let mut poll = tokio::time::interval(CHILD_APPROVAL_POLL);
    let mut stream = handle.drive();
    loop {
        let event = tokio::select! {
            event = stream.next() => event,
            interrupt = operator.interrupted() => {
                match interrupt {
                    Interrupt::Cancel => handle.cancel().await?,
                    Interrupt::Input(parts) => submit(handle, parts).await?,
                }
                continue;
            }
            _ = poll.tick(), if asked => {
                for request in gate.pending_in(handle.owner().root) {
                    if request.session != handle.id()
                        && children.insert((request.session, request.tool_call_id))
                    {
                        let child = asterpath.open_session(request.session).await?;
                        respond(&child, handle, operator, &request).await?;
                    }
                }
                continue;
            }
        };
        let Some(event) = event else { return Ok(()) };
        let event = event?;
        outcome.observe(&event);
        match &event {
            AgentEvent::ApprovalRequested(request) if asked => {
                respond(handle, handle, operator, request).await?;
            }
            _ => operator.show(&event),
        }
    }
}

/// Ask the operator and record the answer in `session`; no answer cancels
/// the driven turn.
async fn respond(
    session: &SessionHandle,
    driven: &OwnedSession,
    operator: &mut impl Operator,
    request: &ApprovalRequest,
) -> Result<(), AsterError> {
    match operator.decide(request).await {
        Some(decision) => session.approve(request.tool_call_id, decision).await,
        None => driven.cancel().await,
    }
}

/// Log `parts` as one user input.
pub async fn submit(handle: &SessionHandle, parts: Vec<Part>) -> Result<(), AsterError> {
    handle
        .submit_input(UserInput {
            id: InputId::from_bytes(unique_bytes(SystemClock.now())),
            parts,
        })
        .await
        .map(drop)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use asterpath_protocol::{
        ApprovalPolicy, ApprovalVerdict, CompactionConfig, Item, JsonString, ModelId, ModelSpec,
        Parallelism, Provider, ProviderId, ReplaySafety, RequestConfig, Role, SessionId,
        SessionSpec, SideEffect, StopReason, SubagentConfig, ToolDescriptor, ToolName,
        ToolSelection, TurnConfig,
    };
    use asterpath_testing::{InMemoryEventLog, ScriptedProvider, TestTool, ToolCalls};

    use super::*;

    struct AlwaysAsk;

    impl ApprovalPolicy for AlwaysAsk {
        fn classify(&self, _request: &ApprovalRequest) -> ApprovalVerdict {
            ApprovalVerdict::Ask
        }
    }

    fn call(name: &str, args: &str) -> Item {
        Item::ToolCall {
            call_id: format!("call-{name}"),
            provider_item_id: None,
            name: ToolName::new(name),
            raw_args: JsonString::new(args),
            raw: None,
        }
    }

    fn text(text: &str) -> Item {
        Item::Message {
            role: Role::Assistant,
            parts: vec![Part::Text { text: text.into() }],
            raw: None,
        }
    }

    fn spec(provider: &str) -> SessionSpec {
        let mut turn = TurnConfig::default();
        turn.initial_backoff = Duration::ZERO;
        turn.max_backoff = Duration::ZERO;
        SessionSpec {
            system_sections: Vec::new(),
            model: ModelSpec {
                provider: ProviderId::new(provider),
                model: ModelId::new("model-1"),
                context_window: None,
            },
            system: Vec::new(),
            tools: ToolSelection::All,
            request: RequestConfig::default(),
            turn,
            compaction: CompactionConfig::default(),
            subagents: SubagentConfig::default(),
            metadata: Default::default(),
        }
    }

    /// An engine whose policy asks for every call, with an `echo` tool.
    async fn engine(replies: Vec<Item>) -> (Asterpath, ToolCalls) {
        engine_on(Arc::new(InMemoryEventLog::new()), "p", replies).await
    }

    async fn engine_on(
        store: Arc<InMemoryEventLog>,
        provider: &str,
        replies: Vec<Item>,
    ) -> (Asterpath, ToolCalls) {
        let mut provider = ScriptedProvider::new(provider);
        for reply in replies {
            let stop = if matches!(reply, Item::ToolCall { .. }) {
                StopReason::ToolUse
            } else {
                StopReason::EndTurn
            };
            provider = provider.respond(vec![reply], stop, Usage::default());
        }
        let (echo, calls) = TestTool::echo(
            ToolDescriptor::new(ToolName::new("echo"), String::new(), serde_json::json!({}))
                .with_parallelism(Parallelism::Parallel)
                .with_replay(ReplaySafety::Safe)
                .with_side_effect(SideEffect::ReadOnly),
        );
        let asterpath = Asterpath::builder()
            .store(store)
            .providers(vec![Arc::new(provider) as Arc<dyn Provider>])
            .tools(vec![echo])
            .approval(Arc::new(AlwaysAsk))
            .build()
            .await
            .expect("build");
        (asterpath, calls)
    }

    async fn session(asterpath: &Asterpath, spec: SessionSpec) -> OwnedSession {
        let handle = asterpath
            .create_session(spec)
            .await
            .unwrap()
            .claim()
            .await
            .unwrap();
        submit(&handle, vec![Part::Text { text: "go".into() }])
            .await
            .unwrap();
        handle
    }

    /// Answers with `answer`, records which sessions asked for which tool, and
    /// logs `input` while it answers.
    struct Scripted {
        answer: Option<ApprovalDecision>,
        asked: Vec<(SessionId, String)>,
        input: Option<(OwnedSession, Vec<Part>)>,
    }

    impl Scripted {
        fn new(answer: Option<ApprovalDecision>) -> Self {
            Self {
                answer,
                asked: Vec::new(),
                input: None,
            }
        }
    }

    impl Operator for Scripted {
        async fn decide(&mut self, request: &ApprovalRequest) -> Option<ApprovalDecision> {
            self.asked
                .push((request.session, request.tool.as_str().to_owned()));
            if let Some((handle, parts)) = self.input.take() {
                submit(&handle, parts).await.unwrap();
            }
            self.answer.clone()
        }
    }

    async fn run(
        asterpath: &Asterpath,
        handle: &OwnedSession,
        operator: &mut impl Operator,
    ) -> Result<DriveOutcome, AsterError> {
        tokio::time::timeout(Duration::from_secs(10), drive(asterpath, handle, operator))
            .await
            .expect("the drive ends")
    }

    #[tokio::test]
    async fn input_pending_after_a_drive_starts_another() {
        let (asterpath, _) = engine(vec![call("echo", "{}"), text("second")]).await;
        let mut one_step = spec("p");
        one_step.turn.max_steps_per_turn = Some(1);
        let handle = session(&asterpath, one_step).await;
        let mut operator = Scripted::new(Some(ApprovalDecision::Allow));
        // Logged during the only step of the first turn: it waits for a new turn.
        operator.input = Some((handle.clone(), vec![Part::Text { text: "two".into() }]));

        let outcome = run(&asterpath, &handle, &mut operator).await.unwrap();

        assert!(matches!(outcome.reason, Some(TurnEndReason::Completed)));
        assert!(!handle.facts().await.unwrap().has_pending_input());
    }

    #[tokio::test]
    async fn a_drive_error_ends_the_drive() {
        let store = Arc::new(InMemoryEventLog::new());
        let (other, _) = engine_on(Arc::clone(&store), "q", Vec::new()).await;
        let session_id = other.create_session(spec("q")).await.unwrap().id();
        // No provider `q` here: the drive fails.
        let (asterpath, _) = engine_on(store, "p", Vec::new()).await;
        let handle = asterpath
            .open_session(session_id)
            .await
            .unwrap()
            .claim()
            .await
            .unwrap();
        submit(&handle, vec![Part::Text { text: "go".into() }])
            .await
            .unwrap();

        let error = run(&asterpath, &handle, &mut Headless).await.err();

        assert!(error.is_some_and(|error| error.to_string().contains('q')));
    }

    #[tokio::test]
    async fn a_childs_approval_goes_to_the_operator_and_lands_in_the_child() {
        let (asterpath, calls) = engine(vec![
            call("spawn_agent", r#"{"task":"echo"}"#),
            call("echo", "{}"),
            text("child done"),
            text("done"),
        ])
        .await;
        let handle = session(&asterpath, spec("p")).await;
        let mut operator = Scripted::new(Some(ApprovalDecision::Allow));

        let outcome = run(&asterpath, &handle, &mut operator).await.unwrap();

        assert!(matches!(outcome.reason, Some(TurnEndReason::Completed)));
        assert_eq!(calls.lock().unwrap().len(), 1, "the child ran echo");
        let [(parent, first), (child, second)] = operator.asked.as_slice() else {
            panic!("{:?}", operator.asked);
        };
        assert_eq!((*parent, first.as_str()), (handle.id(), "spawn_agent"));
        assert_ne!(*child, handle.id());
        assert_eq!(second, "echo");
    }

    #[tokio::test]
    async fn no_answer_cancels_the_turn() {
        let (asterpath, calls) = engine(vec![call("echo", "{}"), text("done")]).await;
        let handle = session(&asterpath, spec("p")).await;

        let outcome = run(&asterpath, &handle, &mut Scripted::new(None))
            .await
            .unwrap();

        assert!(matches!(outcome.reason, Some(TurnEndReason::Cancelled)));
        assert!(calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn headless_opens_no_endpoint_so_the_framework_denies() {
        let (asterpath, calls) = engine(vec![call("echo", "{}"), text("done")]).await;
        let handle = session(&asterpath, spec("p")).await;

        let outcome = run(&asterpath, &handle, &mut Headless).await.unwrap();

        assert!(matches!(outcome.reason, Some(TurnEndReason::Completed)));
        assert!(calls.lock().unwrap().is_empty());
    }
}
