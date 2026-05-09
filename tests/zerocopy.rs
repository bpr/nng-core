use nng_core::{
    MessageBuf, ZeroCopyMessage,
    protocols::{
        pair::{Pair1State, PairError},
        pubsub::Sub0State,
        reqrep::{Rep0State, Req0State, ReqRepError},
        survey::{Respondent0State, SurveyError, Surveyor0State},
    },
};

// ── ZeroCopyMessage basics ──

#[test]
fn zcm_new_is_empty() {
    let m = ZeroCopyMessage::<128>::new();
    assert!(m.body().is_empty());
    assert!(m.header().is_empty());
}

#[test]
fn zcm_push_back_and_read() {
    let mut m = ZeroCopyMessage::<128>::new();
    m.push_back(b"hello");
    assert_eq!(m.body(), b"hello");
    m.push_back(b" world");
    assert_eq!(m.body(), b"hello world");
}

#[test]
fn zcm_header_push_back_and_read() {
    let mut m = ZeroCopyMessage::<128>::new();
    m.header_push_back(&[0x01, 0x02, 0x03, 0x04]);
    assert_eq!(m.header(), &[0x01, 0x02, 0x03, 0x04]);
}

#[test]
fn zcm_trim_front_is_o1() {
    let mut m = ZeroCopyMessage::<128>::new();
    m.push_back(&[0xAA, 0xBB, 0xCC, 0xDD, 0xEE]);
    m.trim_front(2);
    assert_eq!(m.body(), &[0xCC, 0xDD, 0xEE]);
    // trim again
    m.trim_front(1);
    assert_eq!(m.body(), &[0xDD, 0xEE]);
}

#[test]
fn zcm_trim_all_leaves_empty_body() {
    let mut m = ZeroCopyMessage::<128>::new();
    m.push_back(b"data");
    m.trim_front(4);
    assert!(m.body().is_empty());
}

#[test]
fn zcm_header_and_body_independent() {
    let mut m = ZeroCopyMessage::<128>::new();
    m.push_back(b"payload");
    m.header_push_back(&1u32.to_be_bytes());
    assert_eq!(m.header(), &[0, 0, 0, 1]);
    assert_eq!(m.body(), b"payload");
}

#[test]
fn zcm_body_capacity() {
    // N=64 → HEADER_CAP=16, body_capacity=48
    let m = ZeroCopyMessage::<64>::new();
    assert_eq!(ZeroCopyMessage::<64>::body_capacity(), 48);
    assert_eq!(m.body_remaining(), 48);
}

#[test]
#[should_panic(expected = "body overflow")]
fn zcm_body_overflow_panics() {
    let mut m = ZeroCopyMessage::<8>::new();
    // HEADER_CAP = 8/4 = 2, body capacity = 6
    m.push_back(&[0u8; 7]); // one byte too many
}

#[test]
#[should_panic(expected = "header overflow")]
fn zcm_header_overflow_panics() {
    let mut m = ZeroCopyMessage::<8>::new();
    // HEADER_CAP = 8/4 = 2, only 2 bytes for header
    m.header_push_back(&[0u8; 3]); // one byte too many
}

#[test]
#[should_panic(expected = "trim_front out of bounds")]
fn zcm_trim_front_oob_panics() {
    let mut m = ZeroCopyMessage::<64>::new();
    m.push_back(b"hi");
    m.trim_front(5); // more than body length
}

// ── Protocol state machines with ZeroCopyMessage ──

#[test]
fn req_rep_roundtrip_zerocopy() {
    let mut req = Req0State::new();
    let rep = Rep0State::new();

    let mut req_msg = ZeroCopyMessage::<128>::new();
    req_msg.push_back(b"ping");
    let id = req.prepare_outgoing(&mut req_msg);
    assert_eq!(req_msg.header().len(), 4);
    assert_eq!(req_msg.body(), b"ping");

    // Simulate wire: header bytes arrive as body prefix
    let mut wire_msg = ZeroCopyMessage::<128>::new();
    wire_msg.push_back(req_msg.header());
    wire_msg.push_back(req_msg.body());

    let routing = rep.process_incoming(&mut wire_msg).unwrap();
    assert_eq!(wire_msg.body(), b"ping");
    assert_eq!(u32::from_be_bytes(routing.0), id | 0x8000_0000u32);

    let mut reply = ZeroCopyMessage::<128>::new();
    reply.push_back(b"pong");
    rep.prepare_reply(&mut reply, &routing);
    assert_eq!(reply.header(), &routing.0);

    let mut wire_reply = ZeroCopyMessage::<128>::new();
    wire_reply.push_back(reply.header());
    wire_reply.push_back(reply.body());

    req.process_incoming(&mut wire_reply, id).unwrap();
    assert_eq!(wire_reply.body(), b"pong");
}

#[test]
fn req_id_mismatch_zerocopy() {
    let req = Req0State::new();
    let mut msg = ZeroCopyMessage::<64>::new();
    msg.push_back(&42u32.to_be_bytes());
    msg.push_back(b"data");
    let err = req.process_incoming(&mut msg, 1).unwrap_err();
    assert!(matches!(
        err,
        ReqRepError::IdMismatch {
            got: 42,
            expected: 1
        }
    ));
}

#[test]
fn req_message_too_short_zerocopy() {
    let req = Req0State::new();
    let mut msg = ZeroCopyMessage::<64>::new();
    msg.push_back(&[0x00, 0x01]);
    assert_eq!(
        req.process_incoming(&mut msg, 1),
        Err(ReqRepError::MessageTooShort)
    );
}

#[test]
fn pair1_attach_and_process_ttl_zerocopy() {
    let state = Pair1State::new(8);

    let mut msg = ZeroCopyMessage::<128>::new();
    msg.push_back(b"hello");
    state.attach_ttl(&mut msg);
    assert_eq!(msg.header().len(), 4);
    let raw = u32::from_be_bytes(msg.header()[..4].try_into().unwrap());
    assert_eq!(raw & 0x0F, 8);
    assert_ne!(raw & 0x8000_0000, 0);

    let mut wire_msg = ZeroCopyMessage::<128>::new();
    wire_msg.push_back(msg.header());
    wire_msg.push_back(msg.body());

    let ttl = state.process_incoming(&mut wire_msg).unwrap();
    assert_eq!(ttl, 8);
    assert_eq!(wire_msg.body(), b"hello");
}

#[test]
fn pair1_ttl_zero_rejected_zerocopy() {
    let state = Pair1State::new(8);
    let mut msg = ZeroCopyMessage::<64>::new();
    msg.push_back(&0u32.to_be_bytes());
    msg.push_back(b"data");
    assert_eq!(state.process_incoming(&mut msg), Err(PairError::TtlExpired));
}

#[test]
fn survey_roundtrip_zerocopy() {
    let mut surveyor = Surveyor0State::new();
    let respondent = Respondent0State::new();

    let mut survey = ZeroCopyMessage::<128>::new();
    survey.push_back(b"who is alive?");
    let survey_id = surveyor.prepare_survey(&mut survey);

    let mut wire_survey = ZeroCopyMessage::<128>::new();
    wire_survey.push_back(survey.header());
    wire_survey.push_back(survey.body());

    let routing = respondent.process_incoming(&mut wire_survey).unwrap();
    assert_eq!(u32::from_be_bytes(routing.0), survey_id);
    assert_eq!(wire_survey.body(), b"who is alive?");

    let mut response = ZeroCopyMessage::<128>::new();
    response.push_back(b"I am");
    respondent.prepare_response(&mut response, &routing);

    let mut wire_response = ZeroCopyMessage::<128>::new();
    wire_response.push_back(response.header());
    wire_response.push_back(response.body());

    surveyor.process_response(&mut wire_response).unwrap();
    assert_eq!(wire_response.body(), b"I am");
}

#[test]
fn survey_stale_response_rejected_zerocopy() {
    let mut surveyor = Surveyor0State::new();

    let mut old_survey = ZeroCopyMessage::<64>::new();
    let old_id = surveyor.prepare_survey(&mut old_survey);

    let mut new_survey = ZeroCopyMessage::<64>::new();
    surveyor.prepare_survey(&mut new_survey);

    let mut stale = ZeroCopyMessage::<64>::new();
    stale.push_back(&old_id.to_be_bytes());
    stale.push_back(b"late");

    let err = surveyor.process_response(&mut stale).unwrap_err();
    assert!(matches!(err, SurveyError::StaleSurveyId { .. }));
}

#[test]
fn sub_matches_zerocopy() {
    let mut sub = Sub0State::new();
    sub.subscribe(b"news:");

    let mut yes = ZeroCopyMessage::<64>::new();
    yes.push_back(b"news: headline");
    let mut no = ZeroCopyMessage::<64>::new();
    no.push_back(b"weather: sunny");

    assert!(sub.matches(&yes));
    assert!(!sub.matches(&no));
}
