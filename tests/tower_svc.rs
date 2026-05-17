#![cfg(feature = "tower")]

use std::time::Duration;

use nng_core::{Message, Req0Service, socket::reqrep0};

async fn free_addr() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("tcp://127.0.0.1:{port}")
}

async fn echo_server(addr: String) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut rep = reqrep0::Rep0::listen(&addr).await.unwrap();
        loop {
            let Ok((msg, responder)) = rep.receive().await else {
                break;
            };
            let mut reply = Message::new();
            reply.push_back(msg.body());
            let _ = responder.reply(reply).await;
        }
    })
}

#[tokio::test]
async fn req0_service_basic_roundtrip() {
    let addr = free_addr().await;
    let _server = echo_server(addr.clone()).await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    let req = reqrep0::Req0::dial(&addr).await.unwrap();
    let mut svc = Req0Service::new(req);

    let mut msg = Message::new();
    msg.push_back(b"hello");
    let reply = tower_service::Service::call(&mut svc, msg).await.unwrap();
    assert_eq!(reply.body(), b"hello");
}

#[tokio::test]
async fn req0_service_multiple_sequential_requests() {
    let addr = free_addr().await;
    let _server = echo_server(addr.clone()).await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    let req = reqrep0::Req0::dial(&addr).await.unwrap();
    let mut svc = Req0Service::new(req);

    for i in 0u32..10 {
        let mut msg = Message::new();
        msg.push_back(&i.to_be_bytes());
        let reply = tower_service::Service::call(&mut svc, msg).await.unwrap();
        assert_eq!(reply.body(), &i.to_be_bytes());
    }
}

/// Cloned services share the same underlying socket.  Both work when used
/// sequentially (REQ0 is single-inflight).
#[tokio::test]
async fn req0_service_clone_shares_socket() {
    let addr = free_addr().await;
    let _server = echo_server(addr.clone()).await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    let req = reqrep0::Req0::dial(&addr).await.unwrap();
    let mut svc1 = Req0Service::new(req);
    let mut svc2 = svc1.clone();

    let mut msg1 = Message::new();
    msg1.push_back(b"from-svc1");
    let reply1 = tower_service::Service::call(&mut svc1, msg1).await.unwrap();
    assert_eq!(reply1.body(), b"from-svc1");

    let mut msg2 = Message::new();
    msg2.push_back(b"from-svc2");
    let reply2 = tower_service::Service::call(&mut svc2, msg2).await.unwrap();
    assert_eq!(reply2.body(), b"from-svc2");
}

/// `poll_ready` always returns `Ready(Ok(()))` — REQ0 has no backpressure.
#[tokio::test]
async fn req0_service_poll_ready_always_ready() {
    use std::task::Poll;
    let addr = free_addr().await;
    let _server = echo_server(addr.clone()).await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    let req = reqrep0::Req0::dial(&addr).await.unwrap();
    let mut svc = Req0Service::new(req);

    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(matches!(
        tower_service::Service::poll_ready(&mut svc, &mut cx),
        Poll::Ready(Ok(()))
    ));
}
