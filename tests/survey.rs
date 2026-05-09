use std::time::Duration;

use nng_core::{Message, socket::survey0};

async fn free_addr() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    format!("tcp://127.0.0.1:{port}")
}

#[tokio::test]
async fn survey_single_respondent() {
    let addr = free_addr().await;

    // Surveyor must bind BEFORE respondents dial.
    let mut surveyor = survey0::Surveyor0::listen(&addr).await.unwrap();

    let resp_addr = addr.clone();
    let respondent = tokio::spawn(async move {
        let mut resp = survey0::Respondent0::dial(&resp_addr).await.unwrap();
        let (survey, handle) = resp.receive().await.unwrap();
        assert_eq!(survey.body(), b"ping");
        let mut reply = Message::new();
        reply.push_back(b"pong");
        handle.respond(reply).await.unwrap();
    });

    surveyor.wait_for_respondents(1).await.unwrap();

    let mut question = Message::new();
    question.push_back(b"ping");
    let responses = surveyor
        .survey(question, Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].body(), b"pong");

    respondent.await.unwrap();
}

#[tokio::test]
async fn survey_multiple_respondents() {
    let addr = free_addr().await;

    let mut surveyor = survey0::Surveyor0::listen(&addr).await.unwrap();

    let mut handles = Vec::new();
    for i in 0u32..3 {
        let resp_addr = addr.clone();
        handles.push(tokio::spawn(async move {
            let mut resp = survey0::Respondent0::dial(&resp_addr).await.unwrap();
            let (_, handle) = resp.receive().await.unwrap();
            let mut reply = Message::new();
            reply.push_back(&i.to_be_bytes());
            handle.respond(reply).await.unwrap();
        }));
    }

    surveyor.wait_for_respondents(3).await.unwrap();

    let mut question = Message::new();
    question.push_back(b"query");
    let mut responses = surveyor
        .survey(question, Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!(responses.len(), 3);

    // Sort by body so assertion is order-independent.
    responses.sort_by_key(|m| m.body().to_vec());
    assert_eq!(responses[0].body(), &0u32.to_be_bytes());
    assert_eq!(responses[1].body(), &1u32.to_be_bytes());
    assert_eq!(responses[2].body(), &2u32.to_be_bytes());

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn survey_timeout_with_no_respondents() {
    let addr = free_addr().await;

    let mut surveyor = survey0::Surveyor0::listen(&addr).await.unwrap();

    let mut question = Message::new();
    question.push_back(b"anyone?");
    let responses = surveyor
        .survey(question, Duration::from_millis(50))
        .await
        .unwrap();

    assert!(responses.is_empty());
}
