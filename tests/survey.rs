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
        .survey_with_timeout(question, Duration::from_secs(1))
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
        .survey_with_timeout(question, Duration::from_secs(1))
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
        .survey_with_timeout(question, Duration::from_millis(50))
        .await
        .unwrap();

    assert!(responses.is_empty());
}

/// Verify that all respondents are polled concurrently within the shared deadline.
///
/// Respondent 0 sleeps for 100 ms before replying; respondents 1 and 2 reply
/// immediately.  With sequential polling the deadline would expire after
/// respondent 0 consumed it, starving respondents 1 and 2.  With concurrent
/// polling all three replies arrive within the 500 ms window.
#[tokio::test]
async fn survey_concurrent_collection() {
    let addr = free_addr().await;

    let mut surveyor = survey0::Surveyor0::listen(&addr).await.unwrap();

    let mut handles = Vec::new();
    for i in 0u32..3 {
        let resp_addr = addr.clone();
        handles.push(tokio::spawn(async move {
            let mut resp = survey0::Respondent0::dial(&resp_addr).await.unwrap();
            let (_, handle) = resp.receive().await.unwrap();
            // Respondent 0 is deliberately slow.
            if i == 0 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let mut reply = Message::new();
            reply.push_back(&i.to_be_bytes());
            handle.respond(reply).await.unwrap();
        }));
    }

    surveyor.wait_for_respondents(3).await.unwrap();

    let mut question = Message::new();
    question.push_back(b"concurrent?");
    let mut responses = surveyor
        .survey_with_timeout(question, Duration::from_millis(500))
        .await
        .unwrap();

    // All three must have replied within the 500 ms window.
    assert_eq!(
        responses.len(),
        3,
        "expected all 3 responses; got {}",
        responses.len()
    );

    responses.sort_by_key(|m| m.body().to_vec());
    assert_eq!(responses[0].body(), &0u32.to_be_bytes());
    assert_eq!(responses[1].body(), &1u32.to_be_bytes());
    assert_eq!(responses[2].body(), &2u32.to_be_bytes());

    for h in handles {
        h.await.unwrap();
    }
}

/// Verify that `set_survey_time` + `survey()` (no explicit timeout arg) works.
#[tokio::test]
async fn survey_stored_time() {
    let addr = free_addr().await;

    let mut surveyor = survey0::Surveyor0::listen(&addr).await.unwrap();
    surveyor.set_survey_time(Duration::from_millis(500));

    let resp_addr = addr.clone();
    let respondent = tokio::spawn(async move {
        let mut resp = survey0::Respondent0::dial(&resp_addr).await.unwrap();
        let (_, handle) = resp.receive().await.unwrap();
        let mut reply = Message::new();
        reply.push_back(b"ack");
        handle.respond(reply).await.unwrap();
    });

    surveyor.wait_for_respondents(1).await.unwrap();

    let mut question = Message::new();
    question.push_back(b"hello");
    let responses = surveyor.survey(question).await.unwrap();

    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].body(), b"ack");

    respondent.await.unwrap();
}

/// A respondent that replies after the deadline must not appear in results.
#[tokio::test]
async fn survey_late_respondent_excluded() {
    let addr = free_addr().await;

    let mut surveyor = survey0::Surveyor0::listen(&addr).await.unwrap();

    let resp_addr = addr.clone();
    let respondent = tokio::spawn(async move {
        let mut resp = survey0::Respondent0::dial(&resp_addr).await.unwrap();
        let (_, handle) = resp.receive().await.unwrap();
        // Reply after the surveyor's deadline.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let mut reply = Message::new();
        reply.push_back(b"too-late");
        let _ = handle.respond(reply).await;
    });

    surveyor.wait_for_respondents(1).await.unwrap();

    let mut question = Message::new();
    question.push_back(b"fast?");
    let responses = surveyor
        .survey_with_timeout(question, Duration::from_millis(50))
        .await
        .unwrap();

    assert!(
        responses.is_empty(),
        "late respondent must not appear in results"
    );

    respondent.await.unwrap();
}

/// `Surveyor0::bind` returns an empty surveyor plus an `AcceptStream`.
#[tokio::test]
async fn surveyor0_bind_admits_respondents_via_stream() {
    const N_RESPONDENTS: usize = 3;

    let addr = free_addr().await;
    let (mut surveyor, mut accepts) = survey0::Surveyor0::bind(&addr).await.unwrap();

    let mut resp_tasks = Vec::new();
    for i in 0..N_RESPONDENTS {
        let resp_addr = addr.clone();
        resp_tasks.push(tokio::spawn(async move {
            let mut resp = survey0::Respondent0::dial(&resp_addr).await.unwrap();
            let (survey, handle) = resp.receive().await.unwrap();
            assert_eq!(survey.body(), b"ping");
            let mut reply = Message::new();
            reply.push_back(format!("pong-{i}").as_bytes());
            handle.respond(reply).await.unwrap();
        }));
    }

    for _ in 0..N_RESPONDENTS {
        let r = accepts.accept().await.unwrap();
        surveyor.add_respondent(r);
    }
    drop(accepts);

    let mut question = Message::new();
    question.push_back(b"ping");
    let responses = surveyor
        .survey_with_timeout(question, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(responses.len(), N_RESPONDENTS);

    for t in resp_tasks {
        t.await.unwrap();
    }
}
