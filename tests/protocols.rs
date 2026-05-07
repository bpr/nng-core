use nng_pure::Message;
use nng_pure::protocols::{
    pair::{Pair1State, PairError},
    pubsub::{Pub0State, Sub0State},
    reqrep::{Rep0State, Req0State, ReqRepError},
    survey::{Respondent0State, Surveyor0State, SurveyError},
};

// ── REQ0 / REP0 ──

#[test]
fn req_rep_roundtrip() {
    let mut req = Req0State::new();
    let rep = Rep0State::new();

    // REQ prepares outgoing request
    let mut req_msg = Message::new();
    req_msg.push_back(b"ping");
    let id = req.prepare_outgoing(&mut req_msg);
    assert_eq!(req_msg.header().len(), 4); // ID in header
    assert_eq!(req_msg.body(), b"ping");

    // Simulate wire: encode_frame puts header before body, receive puts all in body
    let mut wire_msg = Message::new();
    wire_msg.push_back(req_msg.header()); // header bytes arrive as body prefix
    wire_msg.push_back(req_msg.body());

    // REP receives: strips ID from body front
    let routing = rep.process_incoming(&mut wire_msg).unwrap();
    assert_eq!(wire_msg.body(), b"ping"); // just the payload
    // routing.0 contains the wire ID (high bit set = end-of-backtrace marker).
    assert_eq!(u32::from_be_bytes(routing.0), id | 0x8000_0000u32);

    // REP prepares reply
    let mut reply = Message::new();
    reply.push_back(b"pong");
    rep.prepare_reply(&mut reply, &routing);
    assert_eq!(reply.header(), &routing.0); // ID in header

    // Simulate wire again (header bytes first in body)
    let mut wire_reply = Message::new();
    wire_reply.push_back(reply.header());
    wire_reply.push_back(reply.body());

    // REQ validates reply
    req.process_incoming(&mut wire_reply, id).unwrap();
    assert_eq!(wire_reply.body(), b"pong");
}

#[test]
fn req_id_mismatch() {
    let req = Req0State::new();
    let mut msg = Message::new();
    msg.push_back(&42u32.to_be_bytes()); // wrong ID
    msg.push_back(b"data");
    let err = req.process_incoming(&mut msg, 1).unwrap_err();
    assert!(matches!(err, ReqRepError::IdMismatch { got: 42, expected: 1 }));
}

#[test]
fn req_message_too_short() {
    let req = Req0State::new();
    let mut msg = Message::new();
    msg.push_back(&[0x00, 0x01]); // only 2 bytes
    assert_eq!(req.process_incoming(&mut msg, 1), Err(ReqRepError::MessageTooShort));
}

#[test]
fn req_id_increments_and_skips_zero() {
    let mut req = Req0State::new();
    let mut ids = Vec::new();
    for _ in 0..5 {
        let mut m = Message::new();
        ids.push(req.prepare_outgoing(&mut m));
    }
    // All IDs unique, none zero
    let unique: std::collections::HashSet<_> = ids.iter().copied().collect();
    assert_eq!(unique.len(), 5);
    assert!(!ids.contains(&0));
}

#[test]
fn rep_message_too_short() {
    let rep = Rep0State::new();
    let mut msg = Message::new();
    msg.push_back(&[0x00]); // only 1 byte
    assert_eq!(rep.process_incoming(&mut msg), Err(ReqRepError::MessageTooShort));
}

// ── PUB0 / SUB0 ──

#[test]
fn sub_empty_subscription_matches_all() {
    let mut sub = Sub0State::new();
    sub.subscribe(b""); // empty prefix = match everything

    let mut m1 = Message::new();
    m1.push_back(b"news: breaking");
    let mut m2 = Message::new();
    m2.push_back(b"weather: sunny");

    assert!(sub.matches(&m1));
    assert!(sub.matches(&m2));
}

#[test]
fn sub_prefix_matching() {
    let mut sub = Sub0State::new();
    sub.subscribe(b"news:");

    let mut yes = Message::new();
    yes.push_back(b"news: headline");
    let mut no = Message::new();
    no.push_back(b"weather: sunny");

    assert!(sub.matches(&yes));
    assert!(!sub.matches(&no));
}

#[test]
fn sub_multiple_subscriptions() {
    let mut sub = Sub0State::new();
    sub.subscribe(b"a:");
    sub.subscribe(b"b:");

    let mut ma = Message::new();
    ma.push_back(b"a: stuff");
    let mut mb = Message::new();
    mb.push_back(b"b: things");
    let mut mc = Message::new();
    mc.push_back(b"c: other");

    assert!(sub.matches(&ma));
    assert!(sub.matches(&mb));
    assert!(!sub.matches(&mc));
}

#[test]
fn sub_unsubscribe() {
    let mut sub = Sub0State::new();
    sub.subscribe(b"a:");
    sub.unsubscribe(b"a:");
    assert!(sub.is_empty());

    let mut m = Message::new();
    m.push_back(b"a: data");
    assert!(!sub.matches(&m));
}

#[test]
fn sub_no_duplicate_subscriptions() {
    let mut sub = Sub0State::new();
    sub.subscribe(b"topic:");
    sub.subscribe(b"topic:"); // duplicate
    sub.unsubscribe(b"topic:");
    assert!(sub.is_empty()); // only one removal needed
}

#[test]
fn pub_state_is_stateless() {
    let _pub = Pub0State::new();
    // nothing to test, just confirm it compiles and is unit-constructible
}

// ── PAIR1 ──

#[test]
fn pair1_attach_and_process_ttl() {
    let state = Pair1State::new(8);

    let mut msg = Message::new();
    msg.push_back(b"hello");
    state.attach_ttl(&mut msg);
    // TTL header is in message header
    assert_eq!(msg.header().len(), 4);
    let raw = u32::from_be_bytes(msg.header()[..4].try_into().unwrap());
    assert_eq!(raw & 0x0F, 8); // TTL = 8
    assert_ne!(raw & 0x8000_0000, 0); // high bit set

    // Simulate wire: header bytes arrive in front of body
    let mut wire_msg = Message::new();
    wire_msg.push_back(msg.header());
    wire_msg.push_back(msg.body());

    let ttl = state.process_incoming(&mut wire_msg).unwrap();
    assert_eq!(ttl, 8);
    assert_eq!(wire_msg.body(), b"hello");
}

#[test]
fn pair1_ttl_zero_rejected() {
    let state = Pair1State::new(8);
    let mut msg = Message::new();
    // Manually craft a message with TTL=0 in front of body
    msg.push_back(&0u32.to_be_bytes()); // TTL field = 0
    msg.push_back(b"data");
    assert_eq!(state.process_incoming(&mut msg), Err(PairError::TtlExpired));
}

// ── SURVEYOR0 / RESPONDENT0 ──

#[test]
fn survey_roundtrip() {
    let mut surveyor = Surveyor0State::new();
    let respondent = Respondent0State::new();

    // Surveyor sends survey
    let mut survey = Message::new();
    survey.push_back(b"who is alive?");
    let survey_id = surveyor.prepare_survey(&mut survey);
    assert_eq!(survey.header().len(), 4);

    // Simulate wire
    let mut wire_survey = Message::new();
    wire_survey.push_back(survey.header());
    wire_survey.push_back(survey.body());

    // Respondent receives
    let routing = respondent.process_incoming(&mut wire_survey).unwrap();
    assert_eq!(u32::from_be_bytes(routing.0), survey_id);
    assert_eq!(wire_survey.body(), b"who is alive?");

    // Respondent sends response
    let mut response = Message::new();
    response.push_back(b"I am");
    respondent.prepare_response(&mut response, &routing);
    assert_eq!(response.header(), &routing.0);

    // Simulate wire back to surveyor
    let mut wire_response = Message::new();
    wire_response.push_back(response.header());
    wire_response.push_back(response.body());

    // Surveyor validates
    surveyor.process_response(&mut wire_response).unwrap();
    assert_eq!(wire_response.body(), b"I am");
}

#[test]
fn survey_stale_response_rejected() {
    let mut surveyor = Surveyor0State::new();

    let mut old_survey = Message::new();
    let old_id = surveyor.prepare_survey(&mut old_survey);

    // Issue a new survey (old_id is now stale)
    let mut new_survey = Message::new();
    surveyor.prepare_survey(&mut new_survey);

    // Fabricate a response with old_id
    let mut stale = Message::new();
    stale.push_back(&old_id.to_be_bytes());
    stale.push_back(b"late response");

    let err = surveyor.process_response(&mut stale).unwrap_err();
    assert!(matches!(err, SurveyError::StaleSurveyId { .. }));
}
