//! SURVEYOR0 / RESPONDENT0 protocol state machines.
//!
//! # Survey pattern
//!
//! The surveyor broadcasts a query to all connected respondents and collects
//! their replies within a deadline. This is a fan-out/fan-in pattern: one
//! sender, many receivers, many replies.
//!
//! # Wire encoding
//!
//! Every survey carries a 4-byte **survey ID** (u32 big-endian) prepended to
//! the message. The ID travels in the message header on send and is stripped
//! from the front of the body on receive — same convention as REQ/REP.
//!
//! ```text
//! ┌────────────────────┬──────────────────────┐
//! │  survey ID (4 B)   │   body (variable)    │
//! └────────────────────┴──────────────────────┘
//! ```
//!
//! Respondents echo the survey ID back in their replies so the surveyor can:
//! 1. Match the reply to the current survey.
//! 2. Detect and discard replies to *stale* (already-expired) surveys.
//!
//! Unlike REQ/REP, the survey ID does **not** use the high-bit backtrace
//! mechanism. There is no device-forwarding for surveys in the current spec.
//!
//! # Stale reply detection
//!
//! The surveyor keeps a single `current_id`. Any response whose ID does not
//! match is silently discarded with [`SurveyError::StaleSurveyId`]. This
//! handles network re-ordering, slow respondents, and spurious retransmissions.

use crate::codec::ProtocolId;
use crate::message::MessageBuf;

pub const PROTOCOL_ID_SURVEYOR: ProtocolId = ProtocolId::SURVEYOR0;
pub const PROTOCOL_ID_RESPONDENT: ProtocolId = ProtocolId::RESPONDENT0;

/// Errors produced by the SURVEYOR0 / RESPONDENT0 state machines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SurveyError {
    /// Received a response for a different (older) survey.
    ///
    /// The surveyor has already moved on; this reply should be discarded.
    StaleSurveyId { got: u32, expected: u32 },
    /// Message body was shorter than 4 bytes — too short to hold a survey ID.
    MessageTooShort,
}

impl core::fmt::Display for SurveyError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::StaleSurveyId { got, expected } => write!(
                f,
                "stale survey ID: got {got:#010x}, current {expected:#010x}"
            ),
            Self::MessageTooShort => write!(f, "message too short to contain survey ID"),
        }
    }
}

// ── Surveyor0 ─────────────────────────────────────────────────────────────────

/// State machine for the SURVEYOR0 (query) side.
///
/// Maintains two pieces of state:
/// - `next_id` — the ID to assign to the next survey (monotonically increasing,
///   skips zero, wraps via `wrapping_add(1).max(1)`).
/// - `current_id` — the ID of the currently active survey. Any response whose
///   ID does not match is treated as stale and rejected.
pub struct Surveyor0State {
    next_id: u32,
    current_id: u32,
}

impl Surveyor0State {
    pub fn new() -> Self {
        Self {
            next_id: 1,
            current_id: 0,
        }
    }

    /// Assign a survey ID to an outgoing survey message and return that ID.
    ///
    /// Writes the 4-byte ID into the message header. Advances `current_id`
    /// so that any in-flight responses to previous surveys are now considered
    /// stale.
    pub fn prepare_survey<M: MessageBuf>(&mut self, msg: &mut M) -> u32 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1);
        self.current_id = id;
        msg.header_push_back(&id.to_be_bytes());
        id
    }

    /// Validate an incoming response and strip the survey ID from the body.
    ///
    /// Returns `Ok(())` if the ID matches `current_id`; returns
    /// [`SurveyError::StaleSurveyId`] if it does not. On success the 4-byte
    /// ID is consumed and `msg.body()` holds only the response payload.
    pub fn process_response<M: MessageBuf>(&self, msg: &mut M) -> Result<(), SurveyError> {
        if msg.body().len() < 4 {
            return Err(SurveyError::MessageTooShort);
        }
        let id = u32::from_be_bytes(msg.body()[..4].try_into().unwrap());
        msg.trim_front(4);
        if id != self.current_id {
            return Err(SurveyError::StaleSurveyId {
                got: id,
                expected: self.current_id,
            });
        }
        Ok(())
    }

    /// The ID of the currently active survey (0 if no survey has been started).
    pub fn current_survey_id(&self) -> u32 {
        self.current_id
    }
}

impl Default for Surveyor0State {
    fn default() -> Self {
        Self::new()
    }
}

// ── Respondent0 ───────────────────────────────────────────────────────────────

/// Opaque routing information extracted from an incoming survey.
///
/// Contains the 4-byte survey ID. The respondent must pass this back to
/// [`Respondent0State::prepare_response`] so the reply is tagged with the
/// correct ID for the surveyor to correlate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SurveyRoutingInfo(pub [u8; 4]);

/// State machine for the RESPONDENT0 (reply) side. Stateless between surveys.
///
/// All per-survey state lives in the [`SurveyRoutingInfo`] returned by
/// [`process_incoming`](Self::process_incoming), making it easy to handle
/// surveys in separate tasks or to hold multiple in-flight surveys.
pub struct Respondent0State;

impl Respondent0State {
    pub fn new() -> Self {
        Self
    }

    /// Strip the survey ID from the front of an incoming survey message.
    ///
    /// Returns the [`SurveyRoutingInfo`] needed to send a response. On success
    /// `msg.body()` holds only the survey payload.
    pub fn process_incoming<M: MessageBuf>(
        &self,
        msg: &mut M,
    ) -> Result<SurveyRoutingInfo, SurveyError> {
        if msg.body().len() < 4 {
            return Err(SurveyError::MessageTooShort);
        }
        let id_bytes: [u8; 4] = msg.body()[..4].try_into().unwrap();
        msg.trim_front(4);
        Ok(SurveyRoutingInfo(id_bytes))
    }

    /// Attach the saved survey ID to the response header.
    ///
    /// Places the 4-byte ID into the message header so that the surveyor can
    /// match this response to its active survey.
    pub fn prepare_response<M: MessageBuf>(&self, msg: &mut M, routing: &SurveyRoutingInfo) {
        msg.header_push_back(&routing.0);
    }
}

impl Default for Respondent0State {
    fn default() -> Self {
        Self::new()
    }
}
