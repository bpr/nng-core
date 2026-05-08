use nng_core::{
    Message,
    protocols::{
        pubsub::Sub0State,
        reqrep::{Rep0State, Req0State, ReqRepError},
        survey::{Respondent0State, Surveyor0State, SurveyError},
    },
};
use proptest::prelude::*;

// ── REQ0 / REP0 ──────────────────────────────────────────────────────────────

proptest! {
    /// N sequential prepare_outgoing calls produce N distinct, nonzero IDs.
    #[test]
    fn req_ids_nonzero_and_unique(n in 1usize..50) {
        let mut req = Req0State::new();
        let mut ids = Vec::new();
        for _ in 0..n {
            let mut m = Message::new();
            ids.push(req.prepare_outgoing(&mut m));
        }
        prop_assert!(ids.iter().all(|&id| id != 0));
        let unique: std::collections::HashSet<_> = ids.iter().copied().collect();
        prop_assert_eq!(unique.len(), ids.len());
    }

    /// The wire ID written by prepare_outgoing always has the high (backtrace marker) bit set.
    #[test]
    fn req_wire_id_always_has_high_bit(n in 1usize..20) {
        let mut req = Req0State::new();
        for _ in 0..n {
            let mut m = Message::new();
            req.prepare_outgoing(&mut m);
            let wire_id = u32::from_be_bytes(m.header()[..4].try_into().unwrap());
            prop_assert!(
                wire_id & 0x8000_0000u32 != 0,
                "wire ID {:#010x} missing high bit", wire_id
            );
        }
    }

    /// For any ID in the range produced by the state machine (< 0x8000_0000),
    /// stripping the high bit after ORing it back in is the identity.
    #[test]
    fn req_high_bit_strip_roundtrip(id in 1u32..0x8000_0000u32) {
        let stripped = (id | 0x8000_0000u32) & 0x7fff_ffffu32;
        prop_assert_eq!(stripped, id);
    }

    /// prepare_outgoing places exactly 4 bytes in the header.
    /// Rep0State::process_incoming strips exactly 4 bytes from the body front.
    #[test]
    fn req_rep_header_is_4_bytes(payload in prop::collection::vec(any::<u8>(), 0..128)) {
        let mut req = Req0State::new();
        let rep = Rep0State::new();

        let mut msg = Message::new();
        msg.push_back(&payload);
        req.prepare_outgoing(&mut msg);
        prop_assert_eq!(msg.header().len(), 4);

        // Simulate wire: header ++ body arrives as body of received message.
        let mut wire = Message::new();
        wire.push_back(msg.header());
        wire.push_back(msg.body());
        let len_before = wire.body().len();
        rep.process_incoming(&mut wire).unwrap();
        prop_assert_eq!(wire.body().len(), len_before - 4);
    }

    /// Full roundtrip: REQ prepares, REP processes and replies, REQ validates.
    /// The application payload is preserved exactly through both wire hops.
    #[test]
    fn req_rep_full_roundtrip(payload in prop::collection::vec(any::<u8>(), 0..256)) {
        let mut req = Req0State::new();
        let rep = Rep0State::new();

        let mut req_msg = Message::new();
        req_msg.push_back(&payload);
        let id = req.prepare_outgoing(&mut req_msg);

        let mut wire_req = Message::new();
        wire_req.push_back(req_msg.header());
        wire_req.push_back(req_msg.body());

        let routing = rep.process_incoming(&mut wire_req).unwrap();
        prop_assert_eq!(wire_req.body(), payload.as_slice());

        let mut reply = Message::new();
        reply.push_back(&payload);
        rep.prepare_reply(&mut reply, &routing);

        let mut wire_reply = Message::new();
        wire_reply.push_back(reply.header());
        wire_reply.push_back(reply.body());

        req.process_incoming(&mut wire_reply, id).unwrap();
        prop_assert_eq!(wire_reply.body(), payload.as_slice());
    }

    /// When the low-31-bit portion of the wire ID doesn't match sent_id,
    /// process_incoming returns IdMismatch with the correct got/expected values.
    #[test]
    fn req_id_mismatch_on_wrong_wire_id(
        sent_id in 1u32..0x8000_0000u32,
        wire_id  in any::<u32>(),
    ) {
        let stripped = wire_id & 0x7fff_ffff;
        prop_assume!(stripped != sent_id);

        let req = Req0State::new();
        let mut msg = Message::new();
        msg.push_back(&wire_id.to_be_bytes());
        msg.push_back(b"payload");

        prop_assert_eq!(
            req.process_incoming(&mut msg, sent_id),
            Err(ReqRepError::IdMismatch { got: stripped, expected: sent_id }),
        );
    }

    /// Any body shorter than 4 bytes yields MessageTooShort on both sides.
    #[test]
    fn req_rep_too_short_rejected(bytes in prop::collection::vec(any::<u8>(), 0..4)) {
        let req = Req0State::new();
        let rep = Rep0State::new();

        let mut m1 = Message::new();
        m1.push_back(&bytes);
        prop_assert_eq!(req.process_incoming(&mut m1, 1), Err(ReqRepError::MessageTooShort));

        let mut m2 = Message::new();
        m2.push_back(&bytes);
        prop_assert_eq!(rep.process_incoming(&mut m2), Err(ReqRepError::MessageTooShort));
    }
}

// ── PUB0 / SUB0 ──────────────────────────────────────────────────────────────

proptest! {
    /// An empty subscription (b"") matches every possible body.
    #[test]
    fn sub_empty_prefix_matches_all(body in prop::collection::vec(any::<u8>(), 0..256)) {
        let mut sub = Sub0State::new();
        sub.subscribe(b"");
        let mut msg = Message::new();
        msg.push_back(&body);
        prop_assert!(sub.matches(&msg));
    }

    /// With no subscriptions, no message ever matches.
    #[test]
    fn sub_no_subscriptions_never_matches(body in prop::collection::vec(any::<u8>(), 0..256)) {
        let sub = Sub0State::new();
        let mut msg = Message::new();
        msg.push_back(&body);
        prop_assert!(!sub.matches(&msg));
    }

    /// matches() is definitionally equivalent to "body starts_with any subscribed prefix".
    #[test]
    fn sub_matches_iff_starts_with_any_prefix(
        prefixes in prop::collection::vec(
            prop::collection::vec(any::<u8>(), 0..16), 1..6,
        ),
        body in prop::collection::vec(any::<u8>(), 0..64),
    ) {
        let mut sub = Sub0State::new();
        for p in &prefixes {
            sub.subscribe(p);
        }
        let mut msg = Message::new();
        msg.push_back(&body);

        let expected = prefixes.iter().any(|p| body.starts_with(p.as_slice()));
        prop_assert_eq!(sub.matches(&msg), expected);
    }

    /// Subscribing the same prefix N times is the same as subscribing once:
    /// a single unsubscribe removes it completely.
    #[test]
    fn sub_subscribe_is_idempotent(
        prefix in prop::collection::vec(any::<u8>(), 0..16),
        n in 1usize..10,
    ) {
        let mut sub = Sub0State::new();
        for _ in 0..n {
            sub.subscribe(&prefix);
        }
        sub.unsubscribe(&prefix);
        prop_assert!(sub.is_empty());
    }

    /// Calling unsubscribe more than once for the same prefix is a no-op after the first.
    #[test]
    fn sub_unsubscribe_is_idempotent(
        prefix in prop::collection::vec(any::<u8>(), 0..16),
        body   in prop::collection::vec(any::<u8>(), 0..64),
    ) {
        let mut sub = Sub0State::new();
        sub.subscribe(&prefix);
        sub.unsubscribe(&prefix);
        sub.unsubscribe(&prefix); // second call must be a no-op
        prop_assert!(sub.is_empty());

        let mut msg = Message::new();
        msg.push_back(&body);
        prop_assert!(!sub.matches(&msg));
    }

    /// Subscribing to a prefix then checking a body that starts with that prefix always matches.
    #[test]
    fn sub_subscribed_prefix_always_matches(
        prefix in prop::collection::vec(any::<u8>(), 0..32),
        suffix in prop::collection::vec(any::<u8>(), 0..32),
    ) {
        let mut body = prefix.clone();
        body.extend_from_slice(&suffix);

        let mut sub = Sub0State::new();
        sub.subscribe(&prefix);

        let mut msg = Message::new();
        msg.push_back(&body);
        prop_assert!(sub.matches(&msg));
    }

    /// Adding a new subscription never removes an existing match.
    #[test]
    fn sub_adding_subscription_preserves_existing_matches(
        p1     in prop::collection::vec(any::<u8>(), 1..16),
        p2     in prop::collection::vec(any::<u8>(), 1..16),
        suffix in prop::collection::vec(any::<u8>(), 0..16),
    ) {
        // Construct a body guaranteed to match p1.
        let mut body = p1.clone();
        body.extend_from_slice(&suffix);

        let mut sub = Sub0State::new();
        sub.subscribe(&p1);

        let mut msg = Message::new();
        msg.push_back(&body);

        prop_assert!(sub.matches(&msg)); // matches before
        sub.subscribe(&p2);
        prop_assert!(sub.matches(&msg)); // still matches after adding p2
    }
}

// ── SURVEYOR0 / RESPONDENT0 ──────────────────────────────────────────────────

proptest! {
    /// N sequential prepare_survey calls produce N distinct, nonzero IDs.
    #[test]
    fn surveyor_ids_nonzero_and_unique(n in 1usize..50) {
        let mut s = Surveyor0State::new();
        let mut ids = Vec::new();
        for _ in 0..n {
            let mut m = Message::new();
            ids.push(s.prepare_survey(&mut m));
        }
        prop_assert!(ids.iter().all(|&id| id != 0));
        let unique: std::collections::HashSet<_> = ids.iter().copied().collect();
        prop_assert_eq!(unique.len(), ids.len());
    }

    /// current_survey_id() always reflects the ID from the most recent prepare_survey.
    #[test]
    fn surveyor_current_id_tracks_last_survey(n in 1usize..20) {
        let mut s = Surveyor0State::new();
        for _ in 0..n {
            let mut m = Message::new();
            let id = s.prepare_survey(&mut m);
            prop_assert_eq!(s.current_survey_id(), id);
        }
    }

    /// prepare_survey places exactly 4 bytes in the header.
    /// Respondent0State::process_incoming strips exactly 4 bytes from the body front.
    #[test]
    fn survey_header_is_4_bytes(payload in prop::collection::vec(any::<u8>(), 0..128)) {
        let mut surveyor = Surveyor0State::new();
        let respondent = Respondent0State::new();

        let mut survey = Message::new();
        survey.push_back(&payload);
        surveyor.prepare_survey(&mut survey);
        prop_assert_eq!(survey.header().len(), 4);

        let mut wire = Message::new();
        wire.push_back(survey.header());
        wire.push_back(survey.body());
        let len_before = wire.body().len();
        respondent.process_incoming(&mut wire).unwrap();
        prop_assert_eq!(wire.body().len(), len_before - 4);
    }

    /// Full roundtrip: surveyor sends, respondent processes and replies, surveyor validates.
    /// The application payload is preserved exactly through both wire hops.
    #[test]
    fn survey_full_roundtrip(payload in prop::collection::vec(any::<u8>(), 0..256)) {
        let mut surveyor = Surveyor0State::new();
        let respondent = Respondent0State::new();

        let mut survey = Message::new();
        survey.push_back(&payload);
        surveyor.prepare_survey(&mut survey);

        let mut wire_survey = Message::new();
        wire_survey.push_back(survey.header());
        wire_survey.push_back(survey.body());

        let routing = respondent.process_incoming(&mut wire_survey).unwrap();
        prop_assert_eq!(wire_survey.body(), payload.as_slice());

        let mut response = Message::new();
        response.push_back(&payload);
        respondent.prepare_response(&mut response, &routing);

        let mut wire_response = Message::new();
        wire_response.push_back(response.header());
        wire_response.push_back(response.body());

        surveyor.process_response(&mut wire_response).unwrap();
        prop_assert_eq!(wire_response.body(), payload.as_slice());
    }

    /// Any response whose ID doesn't match current_id is rejected with StaleSurveyId.
    #[test]
    fn surveyor_non_current_id_is_stale(stale_id in any::<u32>()) {
        let mut s = Surveyor0State::new();
        let mut m = Message::new();
        let current_id = s.prepare_survey(&mut m);
        prop_assume!(stale_id != current_id);

        let mut response = Message::new();
        response.push_back(&stale_id.to_be_bytes());
        response.push_back(b"body");

        prop_assert_eq!(
            s.process_response(&mut response),
            Err(SurveyError::StaleSurveyId { got: stale_id, expected: current_id }),
        );
    }

    /// After N surveys, every ID except the last is stale.
    #[test]
    fn surveyor_only_latest_id_accepted(n in 2usize..10) {
        let mut s = Surveyor0State::new();
        let mut ids = Vec::new();
        for _ in 0..n {
            let mut m = Message::new();
            ids.push(s.prepare_survey(&mut m));
        }
        let current = *ids.last().unwrap();
        for &old_id in &ids[..ids.len() - 1] {
            let mut resp = Message::new();
            resp.push_back(&old_id.to_be_bytes());
            resp.push_back(b"late");
            prop_assert_eq!(
                s.process_response(&mut resp),
                Err(SurveyError::StaleSurveyId { got: old_id, expected: current }),
            );
        }
    }

    /// Any body shorter than 4 bytes yields MessageTooShort on both sides.
    #[test]
    fn survey_too_short_rejected(bytes in prop::collection::vec(any::<u8>(), 0..4)) {
        let mut s = Surveyor0State::new();
        let mut m = Message::new();
        s.prepare_survey(&mut m); // establish current_id

        let respondent = Respondent0State::new();

        let mut m1 = Message::new();
        m1.push_back(&bytes);
        prop_assert_eq!(s.process_response(&mut m1), Err(SurveyError::MessageTooShort));

        let mut m2 = Message::new();
        m2.push_back(&bytes);
        prop_assert_eq!(respondent.process_incoming(&mut m2), Err(SurveyError::MessageTooShort));
    }
}
