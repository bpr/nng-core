#![cfg(feature = "streams")]

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use nng_core::{
    Message,
    socket::{pair0, pipeline0, pubsub0},
};

async fn free_addr() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("tcp://127.0.0.1:{port}")
}

fn msg(body: &[u8]) -> Message {
    let mut m = Message::new();
    m.push_back(body);
    m
}

// ── Pull0 / Push0 ────────────────────────────────────────────────────────────

/// Listener side (Pull0) runs in a spawned task using `into_stream()`.
/// Main task dials Push0 and uses `into_sink()` to send 5 messages.
#[tokio::test]
async fn push_into_sink_pull_into_stream() {
    let addr = free_addr().await;

    let addr2 = addr.clone();
    let collector = tokio::spawn(async move {
        let pull = pipeline0::Pull0::listen(&addr2).await.unwrap();
        pull.into_stream()
            .take(5)
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()
            .await
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let push = pipeline0::Push0::dial(&addr).await.unwrap();
    let mut sink = push.into_sink();
    for i in 0u32..5 {
        sink.send(msg(&i.to_be_bytes())).await.unwrap();
    }

    let received = collector.await.unwrap();
    assert_eq!(received.len(), 5);
    for (i, m) in received.iter().enumerate() {
        assert_eq!(m.body(), &(i as u32).to_be_bytes());
    }
}

/// Use `futures::stream::iter` → `forward` to pipe an owned message list
/// into the push sink.
#[tokio::test]
async fn push_sink_forward_from_iter_stream() {
    let addr = free_addr().await;

    let addr2 = addr.clone();
    let collector = tokio::spawn(async move {
        let pull = pipeline0::Pull0::listen(&addr2).await.unwrap();
        pull.into_stream()
            .take(8)
            .map(|r| r.unwrap().body()[0])
            .collect::<Vec<u8>>()
            .await
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let push = pipeline0::Push0::dial(&addr).await.unwrap();
    let sink = push.into_sink();
    let payloads: Vec<Result<Message, nng_core::NngError>> =
        (0u8..8).map(|i| Ok(msg(&[i]))).collect();
    futures_util::stream::iter(payloads)
        .forward(sink)
        .await
        .unwrap();

    let received = collector.await.unwrap();
    assert_eq!(received, (0u8..8).collect::<Vec<_>>());
}

// ── Pull0Fan ─────────────────────────────────────────────────────────────────

/// Spawn the two pushers with a brief sleep so `listen_and_accept` can
/// bind before they dial.  Then collect 8 messages from the fan as a Stream.
#[tokio::test]
async fn pull0fan_stream_collect() {
    let addr = free_addr().await;

    // Pre-spawn pushers; sleep gives the listener time to bind.
    for _ in 0..2 {
        let a = addr.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let mut push = pipeline0::Push0::dial(&a).await.unwrap();
            for i in 0u8..4 {
                push.push(msg(&[i])).await.unwrap();
            }
        });
    }

    let fan = pipeline0::Pull0Fan::listen_and_accept(&addr, 2)
        .await
        .unwrap();

    let msgs: Vec<Message> = fan.take(8).collect().await;
    assert_eq!(msgs.len(), 8);
}

// ── Sub0 ─────────────────────────────────────────────────────────────────────

/// Publisher (spawned task) waits for one subscriber, then publishes 5 messages.
/// Subscriber converts to a stream and collects all 5.
#[tokio::test]
async fn sub_into_stream() {
    let addr = free_addr().await;

    let addr2 = addr.clone();
    let publisher = tokio::spawn(async move {
        let mut pub0 = pubsub0::Pub0::listen(&addr2).await.unwrap();
        pub0.wait_for_subscribers(1).await.unwrap();
        for i in 0u8..5 {
            tokio::time::sleep(Duration::from_millis(5)).await;
            pub0.publish(msg(&[i])).await.unwrap();
        }
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut sub = pubsub0::Sub0::dial(&addr).await.unwrap();
    sub.subscribe_to(b"");

    let received: Vec<u8> = sub
        .into_stream()
        .take(5)
        .map(|r| r.unwrap().body()[0])
        .collect()
        .await;

    publisher.await.unwrap();
    assert_eq!(received, (0u8..5).collect::<Vec<_>>());
}

// ── Pair0 ────────────────────────────────────────────────────────────────────

/// Listener side converts to a stream; dialer sends 4 messages.
#[tokio::test]
async fn pair_into_stream() {
    let addr = free_addr().await;

    let addr2 = addr.clone();
    let listener_task = tokio::spawn(async move {
        let listener = pair0::Pair0::listen(&addr2).await.unwrap();
        listener
            .into_stream()
            .take(4)
            .map(|r| r.unwrap().body()[0])
            .collect::<Vec<u8>>()
            .await
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut dialer = pair0::Pair0::dial(&addr).await.unwrap();
    for i in 0u8..4 {
        dialer.send(msg(&[i])).await.unwrap();
    }

    let received = listener_task.await.unwrap();
    assert_eq!(received, vec![0, 1, 2, 3]);
}

/// Dialer side converts to a sink; listener receives 4 messages via `recv()`.
#[tokio::test]
async fn pair_into_sink() {
    let addr = free_addr().await;

    let addr2 = addr.clone();
    let listener_task = tokio::spawn(async move {
        let mut listener = pair0::Pair0::listen(&addr2).await.unwrap();
        let mut results = Vec::new();
        for _ in 0..4 {
            results.push(listener.recv().await.unwrap().body()[0]);
        }
        results
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let dialer = pair0::Pair0::dial(&addr).await.unwrap();
    let mut sink = dialer.into_sink();
    for i in 0u8..4 {
        sink.send(msg(&[i])).await.unwrap();
    }
    sink.close().await.unwrap();

    let received = listener_task.await.unwrap();
    assert_eq!(received, vec![0, 1, 2, 3]);
}
