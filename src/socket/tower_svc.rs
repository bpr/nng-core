use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tokio::sync::Mutex;

use crate::{Message, error::NngError};

use super::reqrep0::Req0;

/// A cloneable [`tower_service::Service`] that sends requests through a
/// shared [`Req0`] socket.
///
/// All clones share the same underlying socket via `Arc<Mutex<Req0>>`,
/// serializing requests (REQ0 allows only one in-flight request at a time).
/// Any resend time configured on the socket is preserved.
#[derive(Clone)]
pub struct Req0Service {
    inner: Arc<Mutex<Req0>>,
}

impl Req0Service {
    pub fn new(req0: Req0) -> Self {
        Self {
            inner: Arc::new(Mutex::new(req0)),
        }
    }
}

impl tower_service::Service<Message> for Req0Service {
    type Response = Message;
    type Error = NngError;
    type Future = Pin<Box<dyn Future<Output = Result<Message, NngError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Message) -> Self::Future {
        let inner = Arc::clone(&self.inner);
        Box::pin(async move { inner.lock().await.request(req).await })
    }
}
