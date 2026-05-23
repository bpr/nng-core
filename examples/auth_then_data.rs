//! Typestate-protected PAIR0 protocol.
//!
//! Two states per side, with `self`-consuming transitions:
//!
//! ```text
//! Client:  AuthSession<Unauth>        ──login──▶  AuthSession<Auth>
//! Server:  ServerSession<AwaitLogin>  ──accept_login──▶  ServerSession<Authed>
//! ```
//!
//! Methods exist only on the state where they make sense, so invalid use is
//! a *compile* error, not a runtime one:
//!
//! ```ignore
//! let conn = AuthSession::dial(addr).await?;
//! conn.send_data(b"hi").await?;            // ❌ no `send_data` on <Unauth>
//!
//! let mut conn = conn.login("u", "p").await?;
//! let conn = conn.login("u", "p").await?;  // ❌ no `login` on <Auth>
//! ```
//!
//! The trick is the phantom state parameter on the session struct: each
//! transition method is implemented for exactly one state and returns the
//! next one, so the type system tracks which protocol-state the connection
//! is in.  Existing examples of this pattern in nng-core itself include
//! `Responder` (one-shot reply after `Rep0::receive`) and `SurveyHandle`
//! (one-shot survey response).
//!
//! Run with two terminals:
//! ```text
//! cargo run --example auth_then_data -- --server
//! cargo run --example auth_then_data -- --client
//! ```

use nng_core::{Message, socket::pair0::Pair0};
use std::{env, marker::PhantomData};

const ADDR: &str = "tcp://127.0.0.1:5570";

type Err = Box<dyn std::error::Error + Send + Sync>;

// ── State markers (zero-sized, exist only at the type level) ─────────────────

/// Client: credentials not yet accepted by the server.
pub struct Unauth;
/// Client: credentials accepted; data exchange permitted.
pub struct Auth;
/// Server: waiting for the first LOGIN.
pub struct AwaitLogin;
/// Server: client authenticated; data exchange permitted.
pub struct Authed;

// ── Client session ───────────────────────────────────────────────────────────

pub struct AuthSession<S = Unauth> {
    pair: Pair0,
    _state: PhantomData<S>,
}

impl AuthSession<Unauth> {
    pub async fn dial(addr: &str) -> Result<Self, Err> {
        Ok(Self {
            pair: Pair0::dial(addr).await?,
            _state: PhantomData,
        })
    }

    /// Send LOGIN and await the verdict.  Consumes `self`: the **only**
    /// way to reach `AuthSession<Auth>` is via this transition.
    pub async fn login(mut self, user: &str, pass: &str) -> Result<AuthSession<Auth>, Err> {
        let mut msg = Message::new();
        msg.push_back(format!("LOGIN {user} {pass}").as_bytes());
        self.pair.send(msg).await?;

        match self.pair.recv().await?.body() {
            b"OK" => Ok(AuthSession {
                pair: self.pair,
                _state: PhantomData,
            }),
            b"NO" => Err("server rejected login".into()),
            other => Err(format!("unexpected reply: {other:?}").into()),
        }
    }
}

impl AuthSession<Auth> {
    pub async fn send_data(&mut self, payload: &[u8]) -> Result<(), Err> {
        let mut msg = Message::new();
        msg.push_back(b"DATA ");
        msg.push_back(payload);
        self.pair.send(msg).await?;
        Ok(())
    }

    pub async fn recv_data(&mut self) -> Result<Vec<u8>, Err> {
        let msg = self.pair.recv().await?;
        msg.body()
            .strip_prefix(b"DATA ")
            .map(|p| p.to_vec())
            .ok_or_else(|| format!("unexpected frame: {:?}", msg.body()).into())
    }
}

// ── Server session ───────────────────────────────────────────────────────────

pub struct ServerSession<S = AwaitLogin> {
    pair: Pair0,
    user: Option<String>,
    _state: PhantomData<S>,
}

impl ServerSession<AwaitLogin> {
    pub async fn listen(addr: &str) -> Result<Self, Err> {
        Ok(Self {
            pair: Pair0::listen(addr).await?,
            user: None,
            _state: PhantomData,
        })
    }

    /// Read the first LOGIN, validate via `auth`, reply OK/NO.
    ///
    /// On rejection returns the `<AwaitLogin>` session back to the caller
    /// (paired with a reason string) so they can try again on the same
    /// connection or close — illustrating a typestate **failure-path
    /// transition** that keeps the resource recoverable.
    pub async fn accept_login(
        mut self,
        auth: impl Fn(&str, &str) -> bool,
    ) -> Result<ServerSession<Authed>, (Self, String)> {
        let msg = match self.pair.recv().await {
            Ok(m) => m,
            Err(e) => return Err((self, format!("recv failed: {e}"))),
        };
        let body = std::str::from_utf8(msg.body()).unwrap_or("");
        let parts: Vec<&str> = body.splitn(3, ' ').collect();
        let (verdict, user) = match parts.as_slice() {
            ["LOGIN", u, p] if auth(u, p) => (b"OK".as_slice(), Some(u.to_string())),
            _ => (b"NO".as_slice(), None),
        };

        let mut reply = Message::new();
        reply.push_back(verdict);
        if let Err(e) = self.pair.send(reply).await {
            return Err((self, format!("send failed: {e}")));
        }
        match user {
            Some(u) => Ok(ServerSession {
                pair: self.pair,
                user: Some(u),
                _state: PhantomData,
            }),
            None => Err((self, "invalid credentials".into())),
        }
    }
}

impl ServerSession<Authed> {
    pub fn user(&self) -> &str {
        self.user.as_deref().unwrap()
    }

    pub async fn recv_data(&mut self) -> Result<Vec<u8>, Err> {
        let msg = self.pair.recv().await?;
        msg.body()
            .strip_prefix(b"DATA ")
            .map(|p| p.to_vec())
            .ok_or_else(|| format!("unexpected frame: {:?}", msg.body()).into())
    }

    pub async fn reply(&mut self, payload: &[u8]) -> Result<(), Err> {
        let mut msg = Message::new();
        msg.push_back(b"DATA ");
        msg.push_back(payload);
        self.pair.send(msg).await?;
        Ok(())
    }
}

// ── Demo driver ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Err> {
    match env::args().nth(1).as_deref() {
        Some("--server") => run_server().await,
        Some("--client") => run_client().await,
        _ => {
            eprintln!("usage: auth_then_data --server | --client");
            Ok(())
        }
    }
}

async fn run_server() -> Result<(), Err> {
    let conn = ServerSession::listen(ADDR).await?;
    println!("server: waiting for login on {ADDR}");

    let mut conn = match conn
        .accept_login(|u, p| u == "alice" && p == "hunter2")
        .await
    {
        Ok(c) => c,
        Err((_, why)) => {
            eprintln!("server: login rejected: {why}");
            return Ok(());
        }
    };
    println!("server: client `{}` logged in", conn.user());

    while let Ok(payload) = conn.recv_data().await {
        let text = String::from_utf8_lossy(&payload);
        println!("server: <- {text:?}");
        conn.reply(&payload).await?;
    }
    Ok(())
}

async fn run_client() -> Result<(), Err> {
    let conn = AuthSession::dial(ADDR).await?;
    let mut conn = conn.login("alice", "hunter2").await?;
    println!("client: authenticated");

    for line in ["hello", "world", "typestate is nice"] {
        conn.send_data(line.as_bytes()).await?;
        let echo = conn.recv_data().await?;
        println!("client: <- {}", String::from_utf8_lossy(&echo));
    }
    Ok(())
}
