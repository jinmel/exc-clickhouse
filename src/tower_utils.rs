use eyre::Result;
use std::{
    task::{Context, Poll},
    time::Duration,
};

use alloy::transports::BoxFuture;
use tokio::time::sleep;
use tower::{Layer, Service};

/// A [`tower::Service`] that delays the dispatch of requests by a specified duration.
#[derive(Debug, Clone)]
pub struct DelayService<S> {
    service: S,
    delay: Duration,
}

/// A [`tower::Layer`] that returns a new [`DelayService`] with the specified delay.
#[derive(Debug, Clone)]
pub struct DelayLayer {
    delay: Duration,
}

impl DelayLayer {
    /// Creates a new [`DelayLayer`] with the specified delay.
    pub const fn new(delay: Duration) -> Self {
        Self { delay }
    }
}

impl<S> Layer<S> for DelayLayer {
    type Service = DelayService<S>;

    fn layer(&self, service: S) -> Self::Service {
        DelayService {
            service,
            delay: self.delay,
        }
    }
}

/// Implement the [`tower::Service`] trait for the [`DelayService`].
impl<S, Request> Service<Request> for DelayService<S>
where
    S: Service<Request> + Send,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let delay = self.delay;
        let future = self.service.call(req);

        Box::pin(async move {
            tracing::trace!("Delaying for {} seconds...", delay.as_secs());
            sleep(delay).await;
            tracing::trace!("Dispatching request...");
            future.await
        })
    }
}
