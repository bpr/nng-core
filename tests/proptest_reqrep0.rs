//! Property-based tests for Req0State / Rep0State.
//!
//! These go beyond the fixed-example unit tests in tests/protocols.rs by
//! exercising the same invariants with arbitrary inputs.

use nng_core::{
    Message,
    protocols::reqrep::{Rep0State, Req0State, ReqRepError},
};
use proptest::prelude::*;

/// Simulate the on-wire encoding: the sender puts header bytes first in the
/// payload, so the receiver sees them at the front of the message body.
fn to_wire(msg: &Message) -> Message {
    let mut wire = Message::new();
    wire.push_back(msg.header());
    wire.push_back(msg.body());
    wire
}

proptest! {
    /// Any body survives a full REQ → REP → REQ round-trip intact.
    #[test]
    fn req_rep_roundtrip(body in proptest::collection::vec(any::<u8>(), 0..256)) {
        let mut req = Req0State::new();
        let rep = Rep0State::new();

        let mut out = Message::new();
        out.push_back(&body);
        let id = req.prepare_outgoing(&mut out);

        let mut wire_req = to_wire(&out);
        let routing = rep.process_incoming(&mut wire_req).unwrap();
        prop_assert_eq!(wire_req.body(), body.as_slice());

        let mut reply = Message::new();
        reply.push_back(&body);
        rep.prepare_reply(&mut reply, &routing);

        let mut wire_reply = to_wire(&reply);
        req.process_incoming(&mut wire_reply, id).unwrap();
        prop_assert_eq!(wire_reply.body(), body.as_slice());
    }

    /// The wire request ID (4 bytes in the message header after prepare_outgoing)
    /// must always have the high bit set.  This bit is the end-of-backtrace-chain
    /// marker required by NNG REP for multi-hop device routing.
    #[test]
    fn req_wire_id_high_bit_always_set(body in proptest::collection::vec(any::<u8>(), 0..64)) {
        let mut req = Req0State::new();
        let mut msg = Message::new();
        msg.push_back(&body);
        req.prepare_outgoing(&mut msg);
        let wire_id = u32::from_be_bytes(msg.header()[..4].try_into().unwrap());
        prop_assert_ne!(
            wire_id & 0x8000_0000, 0,
            "wire ID {:#010x} must have high bit set", wire_id
        );
    }

    /// A reply whose leading 4-byte ID doesn't match the sent ID is always
    /// rejected, regardless of body content.
    #[test]
    fn req_wrong_id_always_rejected(
        sent_id in 1u32..=0x7FFF_FFFFu32,
        wire_id_bytes in any::<u32>(),
        body in proptest::collection::vec(any::<u8>(), 0..64),
    ) {
        // The implementation strips the high bit from the wire value before
        // comparing.  Filter cases where stripped wire value == sent_id.
        prop_assume!((wire_id_bytes & 0x7FFF_FFFF) != sent_id);

        let req = Req0State::new();
        let mut msg = Message::new();
        msg.push_back(&wire_id_bytes.to_be_bytes());
        msg.push_back(&body);
        let err = req.process_incoming(&mut msg, sent_id).unwrap_err();
        prop_assert!(
            matches!(err, ReqRepError::IdMismatch { .. }),
            "expected IdMismatch, got {err:?}"
        );
    }

    /// Across N consecutive requests, every assigned ID is unique and nonzero.
    #[test]
    fn req_ids_unique_and_nonzero(n in 1usize..200) {
        let mut req = Req0State::new();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..n {
            let mut msg = Message::new();
            msg.push_back(b"x");
            let id = req.prepare_outgoing(&mut msg);
            prop_assert_ne!(id, 0, "req ID must never be zero");
            prop_assert!(seen.insert(id), "req ID {id} was issued twice");
        }
    }

    /// Surveyor0State: across N surveys, IDs are unique and nonzero.
    #[test]
    fn surveyor_ids_unique_and_nonzero(n in 1usize..200) {
        use nng_core::protocols::survey::Surveyor0State;
        let mut surveyor = Surveyor0State::new();
        let mut seen = std::collections::HashSet::new();
        for _ in 0..n {
            let mut msg = Message::new();
            msg.push_back(b"?");
            let id = surveyor.prepare_survey(&mut msg);
            prop_assert_ne!(id, 0, "survey ID must never be zero");
            prop_assert!(seen.insert(id), "survey ID {id} was issued twice");
        }
    }

    /// Surveyor0State: a response carrying any ID other than the current survey
    /// ID is always rejected.
    #[test]
    fn surveyor_stale_id_always_rejected(
        stale_id in any::<u32>(),
        body in proptest::collection::vec(any::<u8>(), 0..64),
    ) {
        use nng_core::protocols::survey::{SurveyError, Surveyor0State};
        let mut surveyor = Surveyor0State::new();
        let mut survey = Message::new();
        survey.push_back(b"?");
        let current_id = surveyor.prepare_survey(&mut survey);
        prop_assume!(stale_id != current_id);

        let mut response = Message::new();
        response.push_back(&stale_id.to_be_bytes());
        response.push_back(&body);
        let err = surveyor.process_response(&mut response).unwrap_err();
        prop_assert!(
            matches!(err, SurveyError::StaleSurveyId { .. }),
            "expected StaleSurveyId, got {err:?}"
        );
    }
}
