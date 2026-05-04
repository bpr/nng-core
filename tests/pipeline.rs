use std::time::Duration;

use nng_pure::{Message, socket::pipeline0};

async fn free_addr() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("tcp://127.0.0.1:{port}")
}

#[tokio::test]
async fn pipeline_push_pull_10_messages() {
    let addr = free_addr().await;

    let push_addr = addr.clone();
    let pusher = tokio::spawn(async move {
        let mut push = pipeline0::Push0::listen(&push_addr).await.unwrap();
        for i in 0u32..10 {
            let mut msg = Message::new();
            msg.push_back(&i.to_be_bytes());
            push.push(msg).await.unwrap();
        }
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut pull = pipeline0::Pull0::dial(&addr).await.unwrap();
    for i in 0u32..10 {
        let msg = pull.pull().await.unwrap();
        assert_eq!(msg.body(), &i.to_be_bytes());
    }

    pusher.await.unwrap();
}

#[tokio::test]
async fn pipeline_pull_listens_push_dials() {
    let addr = free_addr().await;

    let pull_addr = addr.clone();
    let puller = tokio::spawn(async move {
        let mut pull = pipeline0::Pull0::listen(&pull_addr).await.unwrap();
        for i in 0u32..5 {
            let msg = pull.pull().await.unwrap();
            assert_eq!(msg.body(), &i.to_be_bytes());
        }
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut push = pipeline0::Push0::dial(&addr).await.unwrap();
    for i in 0u32..5 {
        let mut msg = Message::new();
        msg.push_back(&i.to_be_bytes());
        push.push(msg).await.unwrap();
    }

    puller.await.unwrap();
}
