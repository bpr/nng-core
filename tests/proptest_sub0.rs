//! State-machine property test for Sub0State.
//!
//! Uses proptest-state-machine to verify that Sub0State's subscribe /
//! unsubscribe / matches behaviour exactly tracks a simple BTreeSet reference
//! over arbitrary sequences of operations.
//!
//! The reference model is intentionally trivial — a BTreeSet of topic prefixes
//! with a linear scan for matches.  Any divergence between it and Sub0State is
//! a bug in Sub0State.

use std::collections::BTreeSet;

use nng_core::{Message, protocols::pubsub::Sub0State};
use proptest::prelude::*;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};

// ── Reference state ───────────────────────────────────────────────────────────

#[derive(Clone, Debug, Default)]
struct RefState {
    subs: BTreeSet<Vec<u8>>,
}

impl RefState {
    fn matches(&self, body: &[u8]) -> bool {
        self.subs
            .iter()
            .any(|prefix| body.starts_with(prefix.as_slice()))
    }
}

// ── Transition enum ───────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
enum Transition {
    Subscribe(Vec<u8>),
    Unsubscribe(Vec<u8>),
    CheckMatches(Vec<u8>),
}

// ── ReferenceStateMachine ─────────────────────────────────────────────────────

struct Sub0Machine;

impl ReferenceStateMachine for Sub0Machine {
    type State = RefState;
    type Transition = Transition;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(RefState::default()).boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        // Short prefixes and bodies keep the search space manageable while
        // still exercising prefix-vs-body length edge cases.
        let prefix = proptest::collection::vec(any::<u8>(), 0..=8);
        let body = proptest::collection::vec(any::<u8>(), 0..=16);

        if state.subs.is_empty() {
            prop_oneof![
                prefix.prop_map(Transition::Subscribe),
                body.prop_map(Transition::CheckMatches),
            ]
            .boxed()
        } else {
            let existing: Vec<Vec<u8>> = state.subs.iter().cloned().collect();
            prop_oneof![
                prefix.prop_map(Transition::Subscribe),
                // Bias toward unsubscribing an existing topic so the test also
                // exercises the empty-subscriptions path.
                proptest::sample::select(existing).prop_map(Transition::Unsubscribe),
                body.prop_map(Transition::CheckMatches),
            ]
            .boxed()
        }
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        match transition {
            Transition::Subscribe(prefix) => {
                state.subs.insert(prefix.clone());
            }
            Transition::Unsubscribe(prefix) => {
                state.subs.remove(prefix);
            }
            Transition::CheckMatches(_) => {}
        }
        state
    }
}

// ── StateMachineTest ──────────────────────────────────────────────────────────

struct Sub0Test;

impl StateMachineTest for Sub0Test {
    type SystemUnderTest = Sub0State;
    type Reference = Sub0Machine;

    fn init_test(_ref_state: &RefState) -> Sub0State {
        Sub0State::new()
    }

    fn apply(mut sut: Sub0State, ref_state: &RefState, transition: Transition) -> Sub0State {
        match &transition {
            Transition::Subscribe(prefix) => sut.subscribe(prefix),
            Transition::Unsubscribe(prefix) => sut.unsubscribe(prefix),
            Transition::CheckMatches(body) => {
                let mut msg = Message::new();
                msg.push_back(body);
                let got = sut.matches(&msg);
                let expected = ref_state.matches(body);
                assert_eq!(
                    got, expected,
                    "matches({body:?}): sut={got}, ref={expected} with subs={:?}",
                    ref_state.subs,
                );
            }
        }
        sut
    }

    fn check_invariants(sut: &Sub0State, ref_state: &RefState) {
        assert_eq!(
            sut.is_empty(),
            ref_state.subs.is_empty(),
            "is_empty() disagrees: sut={}, ref={}",
            sut.is_empty(),
            ref_state.subs.is_empty(),
        );
    }
}

prop_state_machine! {
    #[test]
    fn sub0_state_machine(sequential 1..20 => Sub0Test);
}
