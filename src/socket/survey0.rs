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
///
/// Constructed via [`listen`](Self::listen) (classic API, keeps the
/// listener internally so [`wait_for_respondents`](Self::wait_for_respondents)
/// and [`accept_pending`](Self::accept_pending) can admit peers) or via
/// [`bind`](Self::bind) (stream-based API, returns an [`AcceptStream`]
/// that the caller drives explicitly).
pub struct Surveyor0 {
    /// `Some` when constructed via [`listen`](Self::listen); `None` when
    /// constructed via [`bind`](Self::bind) (the listener moved into the
    /// returned [`AcceptStream`]).
    listener: Option<AnyListener>,
    respondents: Vec<AnyTransport>,
    state: Surveyor0State,
    /// Deadline given to each `survey` call.  Matches NNG's `NNG_OPT_SURVEYOR_SURVEYTIME`.
    survey_time: Duration,
}

impl Surveyor0 {
    /// Bind and start accepting respondent connections.
    ///
    /// The listener is kept internally so
    /// [`wait_for_respondents`](Self::wait_for_respondents) and
    /// [`accept_pending`](Self::accept_pending) can admit peers.  For a
    /// stream-based accept API instead, see [`bind`](Self::bind).
    pub async fn listen(addr: &str) -> Result<Self, NngError> {
        let listener = bind_listener(addr).await?;
        Ok(Self {
            listener: Some(listener),
            respondents: Vec::new(),
            state: Surveyor0State::new(),
            survey_time: Duration::from_secs(1),
        })
    }

    /// Bind to `addr` and split the result into an empty surveyor plus an
    /// [`AcceptStream`] yielding incoming respondents.  Same shape as
    /// [`Bus0::bind`](crate::socket::bus0::Bus0::bind): the caller decides
    /// when and whether to admit each incoming respondent.
    ///
    /// ```ignore
    /// let (mut surv, mut accepts) = Surveyor0::bind("tcp://127.0.0.1:5555").await?;
    /// for _ in 0..3 {
    ///     let r = accepts.accept().await?;
    ///     surv.add_respondent(r);
    /// }
    /// // drop `accepts` to stop admitting; surv.survey(...) now polls just the three.
    /// ```
    ///
    /// The returned `Surveyor0` has `listener: None`, so
    /// [`wait_for_respondents`](Self::wait_for_respondents) and
    /// [`accept_pending`](Self::accept_pending) become no-ops on it.
    pub async fn bind(addr: &str) -> Result<(Self, AcceptStream), NngError> {
        let listener = bind_listener(addr).await?;
        let surv = Self {
            listener: None,
            respondents: Vec::new(),
            state: Surveyor0State::new(),
            survey_time: Duration::from_secs(1),
        };
        Ok((surv, AcceptStream { listener }))
    }

    /// Add a respondent accepted via [`AcceptStream::accept`].
    pub fn add_respondent(&mut self, respondent: AcceptedRespondent) {
        self.respondents.push(respondent.0);
    }

    /// Set the default survey deadline used by `survey()`.
    ///
    /// Equivalent to NNG's `NNG_OPT_SURVEYOR_SURVEYTIME`.  Defaults to 1 second.
    pub fn set_survey_time(&mut self, d: Duration) {
        self.survey_time = d;
    }

    /// Block until at least `n` respondents have connected.
    ///
    /// No-op for `Surveyor0` constructed via [`bind`](Self::bind) — that
    /// path has no internal listener; use the returned [`AcceptStream`]
    /// instead.
    pub async fn wait_for_respondents(&mut self, n: usize) -> Result<(), NngError> {
        let Some(listener) = &self.listener else {
            return Ok(());
        };
        while self.respondents.len() < n {
            if let Ok(t) = listener.accept_as_transport(ProtocolId::SURVEYOR0).await {
                self.respondents.push(t);
            }
        }
        Ok(())
    }

    /// Accept any respondents that connected since the last call.
    ///
    /// Returns immediately when the kernel's accept queue is empty.
    /// No-op for `Surveyor0` constructed via [`bind`](Self::bind).
    pub async fn accept_pending(&mut self) {
        let Some(listener) = &self.listener else {
            return;
        };
        loop {
            let raw = tokio::select! {
                biased;
                result = listener.accept_raw() => match result {
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

/// A respondent accepted via [`AcceptStream::accept`], ready to be handed
/// to [`Surveyor0::add_respondent`].  Opaque newtype around an internal
/// transport.
pub struct AcceptedRespondent(pub(crate) AnyTransport);

/// Stream of incoming respondent connections produced by
/// [`Surveyor0::bind`].
pub struct AcceptStream {
    listener: AnyListener,
}

impl AcceptStream {
    /// Await the next incoming respondent and complete its SP handshake.
    pub async fn accept(&mut self) -> Result<AcceptedRespondent, NngError> {
        self.listener
            .accept_as_transport(ProtocolId::SURVEYOR0)
            .await
            .map(AcceptedRespondent)
    }
}
