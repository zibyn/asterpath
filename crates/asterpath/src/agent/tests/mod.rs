mod compaction_turn;
mod crash_points;
mod pipeline;
mod recovery;
mod support;
mod tool_pipeline;

#[test]
fn a_refusal_ends_the_turn_whatever_else_is_pending() {
    use asterpath_protocol::{StopReason, TurnEndReason};
    for (had_calls, pending_input) in [(false, false), (true, false), (false, true), (true, true)] {
        assert!(matches!(
            super::step_outcome(&StopReason::Refusal, had_calls, pending_input),
            super::StepOutcome::Turn(TurnEndReason::Refused)
        ));
    }
}
