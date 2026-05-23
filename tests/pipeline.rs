use std::time::Duration;

use nng_core::{Message, socket::pipeline0};

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

/// `Pull0Fan::bind` returns an empty fan plus an `AcceptStream`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pull0fan_bind_admits_pushers_via_stream() {
    const N_PUSHERS: usize = 3;
    const N_MSGS: usize = 5;

    let addr = free_addr().await;
    let server_addr = addr.clone();

    let server = tokio::spawn(async move {
        let (mut fan, mut accepts) = pipeline0::Pull0Fan::bind(&server_addr).await.unwrap();

        for _ in 0..N_PUSHERS {
            let p = accepts.accept().await.unwrap();
            fan.add_pusher(p);
        }
        assert_eq!(fan.sender_count(), N_PUSHERS);
        drop(accepts);

        let mut received = 0;
        while received < N_PUSHERS * N_MSGS {
            let _ = fan.pull_any().await.unwrap();
            received += 1;
        }
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    let mut push_tasks = Vec::new();
    for sender in 0..N_PUSHERS {
        let dial_addr = addr.clone();
        push_tasks.push(tokio::spawn(async move {
            let mut push = pipeline0::Push0::dial(&dial_addr).await.unwrap();
            for i in 0..N_MSGS {
                let mut msg = Message::new();
                msg.push_back(format!("s{sender}-m{i}").as_bytes());
                push.push(msg).await.unwrap();
            }
        }));
    }

    for t in push_tasks {
        t.await.unwrap();
    }
    server.await.unwrap();
}
