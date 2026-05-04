use std::time::Duration;

use nng_pure::{Message, socket::pubsub0};

async fn free_addr() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("tcp://127.0.0.1:{port}")
}

#[tokio::test]
async fn pubsub_single_subscriber_all_messages() {
    let addr = free_addr().await;

    let pub_addr = addr.clone();
    let publisher = tokio::spawn(async move {
        let mut pub0 = pubsub0::Pub0::listen(&pub_addr).await.unwrap();
        pub0.wait_for_subscribers(1).await.unwrap();

        for i in 0u32..5 {
            let mut msg = Message::new();
            msg.push_back(&i.to_be_bytes());
            pub0.publish(msg).await.unwrap();
        }
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut sub = pubsub0::Sub0::dial(&addr).await.unwrap();
    sub.subscribe_to(b""); // empty prefix matches everything

    for i in 0u32..5 {
        let msg = sub.next().await.unwrap();
        assert_eq!(msg.body(), &i.to_be_bytes());
    }

    publisher.await.unwrap();
}

#[tokio::test]
async fn pubsub_topic_filtering() {
    let addr = free_addr().await;

    let pub_addr = addr.clone();
    let publisher = tokio::spawn(async move {
        let mut pub0 = pubsub0::Pub0::listen(&pub_addr).await.unwrap();
        pub0.wait_for_subscribers(2).await.unwrap();

        let messages: &[&[u8]] = &[
            b"sports:goal!",
            b"weather:sunny",
            b"sports:penalty!",
            b"weather:rain",
        ];
        for body in messages {
            let mut msg = Message::new();
            msg.push_back(body);
            pub0.publish(msg).await.unwrap();
        }
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let sports_addr = addr.clone();
    let sports_task = tokio::spawn(async move {
        let mut sub = pubsub0::Sub0::dial(&sports_addr).await.unwrap();
        sub.subscribe_to(b"sports:");
        let m1 = sub.next().await.unwrap();
        let m2 = sub.next().await.unwrap();
        assert_eq!(m1.body(), b"sports:goal!");
        assert_eq!(m2.body(), b"sports:penalty!");
    });

    let weather_addr = addr.clone();
    let weather_task = tokio::spawn(async move {
        let mut sub = pubsub0::Sub0::dial(&weather_addr).await.unwrap();
        sub.subscribe_to(b"weather:");
        let m1 = sub.next().await.unwrap();
        let m2 = sub.next().await.unwrap();
        assert_eq!(m1.body(), b"weather:sunny");
        assert_eq!(m2.body(), b"weather:rain");
    });

    publisher.await.unwrap();
    sports_task.await.unwrap();
    weather_task.await.unwrap();
}

#[tokio::test]
async fn pubsub_non_matching_messages_are_skipped() {
    let addr = free_addr().await;

    let pub_addr = addr.clone();
    let publisher = tokio::spawn(async move {
        let mut pub0 = pubsub0::Pub0::listen(&pub_addr).await.unwrap();
        pub0.wait_for_subscribers(1).await.unwrap();

        // Non-matching first, then matching — subscriber should skip the first.
        for body in [b"other:ignored" as &[u8], b"prefix:keep-me"] {
            let mut msg = Message::new();
            msg.push_back(body);
            pub0.publish(msg).await.unwrap();
        }
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut sub = pubsub0::Sub0::dial(&addr).await.unwrap();
    sub.subscribe_to(b"prefix:");

    // The "other:ignored" frame must be silently discarded.
    let msg = sub.next().await.unwrap();
    assert_eq!(msg.body(), b"prefix:keep-me");

    publisher.await.unwrap();
}
