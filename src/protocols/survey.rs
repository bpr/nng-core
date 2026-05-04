//! SURVEYOR0 / RESPONDENT0 protocol state machines.
//!
//! ## Wire encoding
//! Each survey carries a 4-byte survey ID (u32 big-endian) in the header
//! (so it precedes the body on the wire).  Responses carry back the same
//! survey ID so the surveyor can correlate them.
//!
//! SURVEYOR:
//!   - send: prepend survey_id as u32 BE to header
//!   - recv: strip 4-byte survey ID from front of body, validate it matches
//!     current survey; on mismatch the response is stale — discard.
//!
//! RESPONDENT:
//!   - recv: strip survey ID from front of body, save for reply
//!   - send (respond): prepend saved survey ID to header

use crate::Message;
use crate::codec::ProtocolId;

pub const PROTOCOL_ID_SURVEYOR: ProtocolId = ProtocolId::SURVEYOR0;
pub const PROTOCOL_ID_RESPONDENT: ProtocolId = ProtocolId::RESPONDENT0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SurveyError {
    /// Received response for a different (stale) survey.
    StaleSurveyId { got: u32, expected: u32 },
    /// Message too short to contain a survey ID.
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

// ── Surveyor0 ──

/// State machine for the SURVEYOR0 side.
pub struct Surveyor0State {
    next_id: u32,
    /// ID of the currently active survey (0 = none).
    current_id: u32,
}

impl Surveyor0State {
    pub fn new() -> Self {
        Self { next_id: 1, current_id: 0 }
    }

    /// Prepare a survey message and return the survey ID.
    pub fn prepare_survey(&mut self, msg: &mut Message) -> u32 {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1).max(1);
        self.current_id = id;
        msg.header_push_back(&id.to_be_bytes());
        id
    }

    /// Process an incoming response.  Returns `Ok(())` if the survey ID
    /// matches the current survey; discards stale responses.
    pub fn process_response(&self, msg: &mut Message) -> Result<(), SurveyError> {
        if msg.body().len() < 4 {
            return Err(SurveyError::MessageTooShort);
        }
        let id = u32::from_be_bytes(msg.body()[..4].try_into().unwrap());
        msg.trim_front(4);
        if id != self.current_id {
            return Err(SurveyError::StaleSurveyId { got: id, expected: self.current_id });
        }
        Ok(())
    }

    pub fn current_survey_id(&self) -> u32 {
        self.current_id
    }
}

impl Default for Surveyor0State {
    fn default() -> Self {
        Self::new()
    }
}

// ── Respondent0 ──

/// Opaque routing info for a survey, needed to send a response.
#[derive(Debug, Clone)]
pub struct SurveyRoutingInfo(pub [u8; 4]);

/// State machine for the RESPONDENT0 side.  Stateless between requests.
pub struct Respondent0State;

impl Respondent0State {
    pub fn new() -> Self {
        Self
    }

    /// Process an incoming survey: extract survey ID from front of body.
    /// Returns routing info needed to send the response.
    pub fn process_incoming(&self, msg: &mut Message) -> Result<SurveyRoutingInfo, SurveyError> {
        if msg.body().len() < 4 {
            return Err(SurveyError::MessageTooShort);
        }
        let id_bytes: [u8; 4] = msg.body()[..4].try_into().unwrap();
        msg.trim_front(4);
        Ok(SurveyRoutingInfo(id_bytes))
    }

    /// Prepare a response: attach the saved survey ID to the reply header.
    pub fn prepare_response(&self, msg: &mut Message, routing: &SurveyRoutingInfo) {
        msg.header_push_back(&routing.0);
    }
}

impl Default for Respondent0State {
    fn default() -> Self {
        Self::new()
    }
}
