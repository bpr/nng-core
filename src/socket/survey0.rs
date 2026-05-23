//! SURVEYOR0 / RESPONDENT0 socket API.
//!
//! The surveyor broadcasts a question to all connected respondents and
//! collects answers within a deadline.  The respondent receives surveys
//! and may reply via the returned `SurveyHandle`.

use std::time::Duration;

use crate::{
    Message,
    codec::ProtocolId,
    protocols::survey::{Respondent0State, SurveyRoutingInfo, Surveyor0State},
};

use super::{
    AnyListener, AnyTransport, NngError, ReconnectOptions, bind_listener, connect_transport,
    reconnect_transport,
};

/// Surveyor socket: broadcasts surveys to multiple respondents.
pub struct Surveyor0 {
    listener: AnyListener,
    respondents: Vec<AnyTransport>,
    state: Surveyor0State,
    /// Deadline given to each `survey` call.  Matches NNG's `NNG_OPT_SURVEYOR_SURVEYTIME`.
    survey_time: Duration,
}

impl Surveyor0 {
    /// Bind and start accepting respondent connections.
    pub async fn listen(addr: &str) -> Result<Self, NngError> {
        let listener = bind_listener(addr).await?;
        Ok(Self {
            listener,
            respondents: Vec::new(),
            state: Surveyor0State::new(),
            survey_time: Duration::from_secs(1),
        })
    }

    /// Set the default survey deadline used by `survey()`.
    ///
    /// Equivalent to NNG's `NNG_OPT_SURVEYOR_SURVEYTIME`.  Defaults to 1 second.
    pub fn set_survey_time(&mut self, d: Duration) {
        self.survey_time = d;
    }

    /// Block until at least `n` respondents have connected.
    pub async fn wait_for_respondents(&mut self, n: usize) -> Result<(), NngError> {
        while self.respondents.len() < n {
            if let Ok(t) = self
                .listener
                .accept_as_transport(ProtocolId::SURVEYOR0)
                .await
            {
                self.respondents.push(t);
            }
        }
        Ok(())
    }

    /// Accept any respondents that connected since the last call.
    ///
    /// Returns immediately when the kernel's accept queue is empty.
    pub async fn accept_pending(&mut self) {
        loop {
            let raw = tokio::select! {
                biased;
                result = self.listener.accept_raw() => match result {
                    Ok(raw) => raw,
                    Err(_) => break,
                },
                _ = std::future::ready(()) => break,
            };
            if let Ok(t) = raw.into_transport(ProtocolId::SURVEYOR0).await {
                self.respondents.push(t);
            }
        }
    }

    /// Broadcast `msg` as a survey; collect all responses arriving within
    /// `self.survey_time`.  Returns the application-level response messages.
    pub async fn survey(&mut self, msg: Message) -> Result<Vec<Message>, NngError> {
        self.survey_with_timeout(msg, self.survey_time).await
    }

    /// Broadcast `msg` as a survey; collect all responses arriving within
    /// `timeout`.  All respondents are polled **concurrently** — a slow
    /// respondent does not starve the others.
    pub async fn survey_with_timeout(
        &mut self,
        msg: Message,
        timeout: Duration,
    ) -> Result<Vec<Message>, NngError> {
        let mut outgoing = msg;
        self.state.prepare_survey(&mut outgoing);

        let deadline = tokio::time::Instant::now() + timeout;

        let mut pending: Vec<usize> = Vec::new();
        for (i, resp) in self.respondents.iter_mut().enumerate() {
            if resp.send(&outgoing).await.is_ok() {
                pending.push(i);
            }
        }

        let mut responses = Vec::new();

        // Poll all pending respondents concurrently within the shared deadline.
        // FramedTransport::recv is cancellation-safe (state stored in RecvBuf),
        // so dropping a half-polled future and retrying is correct.
        'outer: loop {
            if pending.is_empty() {
                break;
            }

            let mut still_pending = Vec::new();
            for &i in &pending {
                let result = tokio::select! {
                    biased;
                    r = self.respondents[i].recv() => Some(r),
                    _ = std::future::ready(()) => None,
                };
                match result {
                    Some(Ok(mut raw)) => {
                        if self.state.process_response(&mut raw).is_ok() {
                            responses.push(raw);
                        }
                    }
                    Some(Err(_)) => {}
                    None => still_pending.push(i),
                }
            }
            pending = still_pending;

            if pending.is_empty() {
                break;
            }

            // Yield once so the runtime can service I/O, then check deadline.
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => break 'outer,
                _ = tokio::task::yield_now() => continue,
            }
        }

        Ok(responses)
    }
}

/// One-shot handle that allows sending a single response to the active survey.
pub struct SurveyHandle<'a> {
    transport: &'a mut AnyTransport,
    routing: SurveyRoutingInfo,
}

impl<'a> SurveyHandle<'a> {
    /// Send a response to the surveyor.
    pub async fn respond(self, msg: Message) -> Result<(), NngError> {
        let state = Respondent0State::new();
        let mut outgoing = msg;
        state.prepare_response(&mut outgoing, &self.routing);
        self.transport.send(&outgoing).await
    }
}

/// Respondent socket: dials a surveyor, receives surveys, sends responses.
pub struct Respondent0 {
    transport: AnyTransport,
    state: Respondent0State,
    dial_addr: Option<String>,
    reconnect: Option<ReconnectOptions>,
}

impl Respondent0 {
    /// Connect to a surveyor at `addr`.
    pub async fn dial(addr: &str) -> Result<Self, NngError> {
        let transport = connect_transport(addr, ProtocolId::RESPONDENT0).await?;
        Ok(Self {
            transport,
            state: Respondent0State::new(),
            dial_addr: None,
            reconnect: None,
        })
    }

    /// Dial with automatic reconnect using default backoff (100 ms → 30 s).
    pub async fn dial_reconnecting(addr: &str) -> Result<Self, NngError> {
        Self::dial_with_reconnect(addr, ReconnectOptions::default()).await
    }

    /// Dial with automatic reconnect using custom `ReconnectOptions`.
    pub async fn dial_with_reconnect(addr: &str, opts: ReconnectOptions) -> Result<Self, NngError> {
        let transport = connect_transport(addr, ProtocolId::RESPONDENT0).await?;
        Ok(Self {
            transport,
            state: Respondent0State::new(),
            dial_addr: Some(addr.to_owned()),
            reconnect: Some(opts),
        })
    }

    /// Receive the next survey.  Returns the application message and a
    /// `SurveyHandle` that must be used to respond (or dropped to skip).
    pub async fn receive(&mut self) -> Result<(Message, SurveyHandle<'_>), NngError> {
        let mut msg = loop {
            match self.transport.recv().await {
                Ok(m) => break m,
                Err(e) => {
                    if let (Some(addr), Some(opts)) = (&self.dial_addr, &self.reconnect) {
                        let addr = addr.clone();
                        let opts = opts.clone();
                        reconnect_transport(
                            &mut self.transport,
                            &addr,
                            ProtocolId::RESPONDENT0,
                            &opts,
                        )
                        .await?;
                    } else {
                        return Err(e);
                    }
                }
            }
        };
        let routing = self
            .state
            .process_incoming(&mut msg)
            .map_err(|e| NngError::ProtocolViolation(e.to_string()))?;
        Ok((
            msg,
            SurveyHandle {
                transport: &mut self.transport,
                routing,
            },
        ))
    }
}
