use crate::streams::ExchangeStreamError;
use crate::streams::Parser;
use crate::streams::Subscription;
use async_stream::try_stream;
use futures::SinkExt;
use futures::stream::Stream;
use futures::stream::StreamExt;
use std::fmt::Debug;
use std::pin::Pin;
use tokio::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

pub struct ExchangeStreamBuilder<T, P, S>
where
    T: Send + 'static,
    P: Parser<Vec<T>> + Send + 'static + Clone + Unpin + Sync,
    S: Subscription + Send + 'static + Clone + Unpin,
{
    timeout: Option<Duration>,
    parser: P,
    url: String,
    subscription: S,
    cancellation_token: Option<CancellationToken>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, P, S> ExchangeStreamBuilder<T, P, S>
where
    T: Send + 'static + Debug,
    P: Parser<Vec<T>> + Send + 'static + Clone + Unpin + Sync + Debug,
    S: Subscription + Send + 'static + Clone + Unpin,
{
    pub fn new(url: &str, timeout: Option<Duration>, parser: P, subscription: S) -> Self {
        Self {
            timeout,
            parser,
            url: url.to_owned(),
            subscription,
            cancellation_token: None,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn with_cancellation(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    pub fn build(
        self,
    ) -> Pin<Box<dyn Stream<Item = Result<T, ExchangeStreamError>> + Send + 'static>> {
        let url = self.url;
        let subscription = self.subscription;
        let parser = self.parser;
        let timeout = self.timeout;
        let cancellation_token = self.cancellation_token;

        let stream = try_stream! {
            loop {
                tracing::trace!("Connecting to {}", &url);
                let (mut ws, response) = tokio_tungstenite::connect_async(&url)
                    .await
                    .map_err(|e| ExchangeStreamError::Connection(e.to_string()))?;
                tracing::trace!(?response, "Connected to {}", url);
                let connected_at = Instant::now();

                let messages = subscription
                    .to_messages()
                    .map_err(|e| ExchangeStreamError::Subscription(e.to_string()))?;
                for sub_msg in messages {
                    // some exchanges rate limit the subscription message.
                    tracing::trace!(?url, ?sub_msg, "Sending subscription message");
                    ws.send(sub_msg).await.map_err(|e| ExchangeStreamError::Stream(e.to_string()))?;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }

                // Create interval for periodic messages (e.g., every 30 seconds)
                let mut interval = tokio::time::interval(
                    subscription.heartbeat_interval().unwrap_or(Duration::from_secs(30)),
                );
                interval.tick().await; // Skip the first immediate tick

                let mut timeout_sleep: Pin<Box<dyn std::future::Future<Output = ()> + Send>> =
                    if let Some(dur) = timeout {
                        // Sleep until (connected_at + dur)
                        let wake_at = connected_at + dur;
                        let sleep = tokio::time::sleep_until(wake_at);
                        Box::pin(sleep)
                    } else {
                        // never fires
                        Box::pin(futures::future::pending())
                    };

                loop {
                    tokio::select! {
                        // Handle incoming WebSocket messages
                        msg = ws.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    match parser.parse(&text) {
                                        Ok(Some(parsed)) => {
                                            for item in parsed {
                                                yield item;
                                            }
                                        }
                                        Ok(None) => {
                                            // No parsed result, continue
                                        }
                                        Err(e) => {
                                            tracing::warn!("Parse error: {:?} for {:?}", e, &text);
                                            // Continue processing other messages
                                        }
                                    }
                                }
                                Some(Ok(Message::Close(frame))) => {
                                    tracing::trace!("Stream closed: {frame:?}");
                                    // Break from inner loop to reconnect
                                    break;
                                }
                                Some(Ok(_)) => {
                                    // Ping/pong handled automatically
                                }
                                Some(Err(e)) => {
                                    tracing::warn!("Stream error: {e:?}");
                                    // Break from inner loop to reconnect
                                    break;
                                }
                                None => {
                                    tracing::trace!("WebSocket stream ended");
                                    break;
                                }
                            }
                        }

                        // Handle periodic interval
                        _ = interval.tick() => {
                            // Send periodic message (e.g., ping or heartbeat)
                            let heartbeat_msg = subscription.heartbeat();
                            if let Some(msg) = heartbeat_msg {
                                tracing::trace!(?msg, "Sending heartbeat");
                                if let Err(e) = ws.send(msg).await {
                                    tracing::warn!("Failed to send periodic ping: {e}");
                                }
                            }
                        }

                        // Handle timeout if configured
                        _ = &mut timeout_sleep => {
                            tracing::trace!("Timeout reached, reconnecting");
                            break;
                        }

                        // Handle graceful shutdown
                        _ = async {
                            if let Some(ref token) = cancellation_token {
                                token.cancelled().await
                            } else {
                                futures::future::pending().await
                            }
                        } => {
                            tracing::info!("Graceful shutdown requested, closing WebSocket connection");
                            // Attempt graceful close
                            if let Err(e) = ws.close(None).await {
                                tracing::warn!("Failed to close WebSocket gracefully: {e}");
                            }
                            return;
                        }
                    }
                }
            }
        };

        Box::pin(stream)
    }
}
