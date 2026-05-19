//! REQ/REP over QUIC transport.
//!
//! Generates a self-signed TLS certificate at startup, then runs a server and
//! client in the same process via `tokio::spawn` and exchanges five requests.
//!
//! ```bash
//! cargo run --example quic_reqrep --features quic
//! ```
//!
//! # Production use
//!
//! This example uses a self-signed certificate so it is fully self-contained.
//! With a real CA-signed certificate the server is the same, and the client
//! simply dials by URL — the system's native root certificate store is used
//! automatically:
//!
//! ```rust,ignore
//! // Server (unchanged — just swap in real cert / key files).
//! let mut rep = Rep0::listen_quic("quic://0.0.0.0:5555", &cert_pem, &key_pem).await?;
//!
//! // Client — native roots, no rustls dependency needed.
//! let mut req = Req0::dial("quic://myserver.example.com:5555").await?;
//! ```

use nng_core::{Message, socket::reqrep0};
use rcgen::CertifiedKey;
use rustls::pki_types::CertificateDer;
use std::{io::Write, sync::Arc, time::Duration};

// Port 15000 is reserved for this example.
const ADDR: &str = "quic://localhost:15000";

/// Generate a self-signed certificate for "localhost".
///
/// Returns temp files for the PEM cert and key (kept alive so files persist),
/// plus the raw DER bytes that the client uses to trust the server cert.
fn make_self_signed_cert() -> (
    tempfile::NamedTempFile,
    tempfile::NamedTempFile,
    CertificateDer<'static>,
) {
    let CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();

    let cert_der = CertificateDer::from(cert.der().to_vec());

    let mut cert_file = tempfile::NamedTempFile::new().unwrap();
    cert_file.write_all(cert.pem().as_bytes()).unwrap();
    cert_file.flush().unwrap();

    let mut key_file = tempfile::NamedTempFile::new().unwrap();
    key_file
        .write_all(signing_key.serialize_pem().as_bytes())
        .unwrap();
    key_file.flush().unwrap();

    (cert_file, key_file, cert_der)
}

#[tokio::main]
async fn main() {
    // cert_file and key_file are kept alive in main so the temp files persist
    // for the duration of the example.  Only the paths are moved into the
    // server task; cert_der stays in main for the client to use.
    let (cert_file, key_file, cert_der) = make_self_signed_cert();
    let cert_path = cert_file.path().to_path_buf();
    let key_path = key_file.path().to_path_buf();

    let server = tokio::spawn(async move {
        // listen_quic binds the UDP socket immediately, then blocks until the
        // first client connects.  The 50 ms sleep below gives the OS time to
        // complete the bind before the client dials.
        let mut rep = reqrep0::Rep0::listen_quic(ADDR, &cert_path, &key_path)
            .await
            .expect("listen_quic failed");
        println!("Server ready");

        loop {
            match rep.receive().await {
                Ok((request, responder)) => {
                    let text = String::from_utf8_lossy(request.body()).into_owned();
                    println!("Server received: {text}");
                    let mut reply = Message::new();
                    reply.push_back(format!("Pong: {text}").as_bytes());
                    responder.reply(reply).await.expect("reply failed");
                }
                Err(_) => break,
            }
        }
    });

    // Give the server task time to bind the QUIC endpoint.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Build a TLS config that trusts the server's self-signed certificate.
    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(cert_der.into_owned()).unwrap();
    let client_config = Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth(),
    );

    let mut req = reqrep0::Req0::dial_quic(ADDR, client_config)
        .await
        .expect("dial_quic failed");

    for i in 0..5u32 {
        let mut msg = Message::new();
        msg.push_back(format!("Ping {i}").as_bytes());
        println!("Client sent: Ping {i}");
        let reply = req.request(msg).await.expect("request failed");
        println!("Client received: {}", String::from_utf8_lossy(reply.body()));
    }

    // Dropping `req` sends QUIC CONNECTION_CLOSE, which lets the server's
    // pending receive return so the server task exits cleanly.
    drop(req);
    server.await.unwrap();
}
