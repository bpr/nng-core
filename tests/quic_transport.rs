/// QUIC transport tests (require `--features quic`).
///
/// Note on connection lifecycle: unlike TCP (where the OS kernel handles
/// graceful drain), QUIC sends CONNECTION_CLOSE when the last Connection
/// handle is dropped.  If the server drops its socket immediately after
/// sending a reply, the client may receive CONNECTION_CLOSE before the
/// reply data.  The server task therefore keeps its socket alive (with a
/// final receive that returns ConnectionClosed) until the client explicitly
/// drops its socket.

#[cfg(feature = "quic")]
mod quic {
    use nng_core::{
        Message,
        socket::{pair0, pipeline0, reqrep0},
    };
    use rcgen::CertifiedKey;
    use rustls::pki_types::CertificateDer;
    use std::{io::Write, sync::Arc, time::Duration};
    use tokio::net::UdpSocket;

    struct TestCerts {
        cert_file: tempfile::NamedTempFile,
        key_file: tempfile::NamedTempFile,
        cert_der: CertificateDer<'static>,
    }

    fn make_test_certs() -> TestCerts {
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

        TestCerts {
            cert_file,
            key_file,
            cert_der,
        }
    }

    fn trusting_client_config(cert_der: &CertificateDer<'_>) -> Arc<rustls::ClientConfig> {
        let mut root_store = rustls::RootCertStore::empty();
        root_store.add(cert_der.clone().into_owned()).unwrap();
        Arc::new(
            rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth(),
        )
    }

    async fn free_quic_addr() -> String {
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = sock.local_addr().unwrap().port();
        drop(sock);
        format!("quic://localhost:{port}")
    }

    #[tokio::test]
    async fn req_rep_quic() {
        let certs = make_test_certs();
        let addr = free_quic_addr().await;
        let server_addr = addr.clone();

        let cert_path = certs.cert_file.path().to_path_buf();
        let key_path = certs.key_file.path().to_path_buf();

        let server = tokio::spawn(async move {
            let mut rep = reqrep0::Rep0::listen_quic(&server_addr, &cert_path, &key_path)
                .await
                .unwrap();
            let (msg, responder) = rep.receive().await.unwrap();
            assert_eq!(msg.body(), b"quic ping");
            let mut reply = Message::new();
            reply.push_back(b"quic pong");
            responder.reply(reply).await.unwrap();
            // Keep connection alive until client disconnects, so the reply
            // is not aborted by a premature CONNECTION_CLOSE.
            let _ = rep.receive().await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let client_config = trusting_client_config(&certs.cert_der);
        let mut req = reqrep0::Req0::dial_quic(&addr, client_config)
            .await
            .unwrap();
        let mut msg = Message::new();
        msg.push_back(b"quic ping");
        let reply = req.request(msg).await.unwrap();
        assert_eq!(reply.body(), b"quic pong");

        // Explicitly close the client so the server's extra receive returns.
        drop(req);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn req_rep_quic_multiple_roundtrips() {
        let certs = make_test_certs();
        let addr = free_quic_addr().await;
        let server_addr = addr.clone();

        let cert_path = certs.cert_file.path().to_path_buf();
        let key_path = certs.key_file.path().to_path_buf();

        const N: u32 = 20;
        let server = tokio::spawn(async move {
            let mut rep = reqrep0::Rep0::listen_quic(&server_addr, &cert_path, &key_path)
                .await
                .unwrap();
            for i in 0..N {
                let (msg, responder) = rep.receive().await.unwrap();
                assert_eq!(msg.body(), &i.to_be_bytes());
                let mut reply = Message::new();
                reply.push_back(&(i * 2).to_be_bytes());
                responder.reply(reply).await.unwrap();
            }
            // Keep connection alive until client disconnects.
            let _ = rep.receive().await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let client_config = trusting_client_config(&certs.cert_der);
        let mut req = reqrep0::Req0::dial_quic(&addr, client_config)
            .await
            .unwrap();
        for i in 0..N {
            let mut msg = Message::new();
            msg.push_back(&i.to_be_bytes());
            let reply = req.request(msg).await.unwrap();
            assert_eq!(reply.body(), &(i * 2).to_be_bytes());
        }

        drop(req);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn push_pull_quic() {
        let certs = make_test_certs();
        let addr = free_quic_addr().await;
        let server_addr = addr.clone();

        let cert_path = certs.cert_file.path().to_path_buf();
        let key_path = certs.key_file.path().to_path_buf();

        const N: u32 = 20;
        let server = tokio::spawn(async move {
            let mut pull = pipeline0::Pull0::listen_quic(&server_addr, &cert_path, &key_path)
                .await
                .unwrap();
            for i in 0..N {
                let msg = pull.pull().await.unwrap();
                assert_eq!(msg.body(), &i.to_be_bytes());
            }
            // Server only receives; no reply to worry about delivering.
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let client_config = trusting_client_config(&certs.cert_der);
        let mut push = pipeline0::Push0::dial_quic(&addr, client_config)
            .await
            .unwrap();
        for i in 0..N {
            let mut msg = Message::new();
            msg.push_back(&i.to_be_bytes());
            push.push(msg).await.unwrap();
        }

        server.await.unwrap();
    }

    #[tokio::test]
    async fn pair_quic_bidirectional() {
        let certs = make_test_certs();
        let addr = free_quic_addr().await;
        let server_addr = addr.clone();

        let cert_path = certs.cert_file.path().to_path_buf();
        let key_path = certs.key_file.path().to_path_buf();

        let server = tokio::spawn(async move {
            let mut pair = pair0::Pair0::listen_quic(&server_addr, &cert_path, &key_path)
                .await
                .unwrap();
            for i in 0u32..10 {
                let msg = pair.recv().await.unwrap();
                assert_eq!(msg.body(), &i.to_be_bytes());
                let mut reply = Message::new();
                reply.push_back(&(i + 100).to_be_bytes());
                pair.send(reply).await.unwrap();
            }
            // Keep connection alive until client disconnects.
            let _ = pair.recv().await;
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let client_config = trusting_client_config(&certs.cert_der);
        let mut pair = pair0::Pair0::dial_quic(&addr, client_config).await.unwrap();
        for i in 0u32..10 {
            let mut msg = Message::new();
            msg.push_back(&i.to_be_bytes());
            pair.send(msg).await.unwrap();
            let reply = pair.recv().await.unwrap();
            assert_eq!(reply.body(), &(i + 100).to_be_bytes());
        }

        drop(pair);
        server.await.unwrap();
    }
}
