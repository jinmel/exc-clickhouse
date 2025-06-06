use crate::streams::ExchangeStreamError;
use crate::streams::Parser;
use crate::streams::Subscription;
use futures::stream::Stream;
use futures::SinkExt;
use futures::StreamExt;
use std::pin::Pin;
use tokio::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

pub struct ExchangeStreamBuilder<T, P, S>
where
    T: Send + 'static,
    P: Parser<T> + Send + 'static + Clone + Unpin + Sync,
    S: Subscription + Send + 'static + Clone + Unpin,
{
    url: String,
    timeout: Option<Duration>,
    parser: P,
    subscription: S,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, P, S> ExchangeStreamBuilder<T, P, S>
where
    T: Send + 'static,
    P: Parser<T> + Send + 'static + Clone + Unpin + Sync,
    S: Subscription + Send + 'static + Clone + Unpin,
{
    pub fn new(url: &str, parser: P, subscription: S) -> Self {
        Self {
            url: url.to_owned(),
            timeout: None,
            parser,
            subscription,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn build(self) -> Pin<Box<dyn Stream<Item = Result<T, ExchangeStreamError>> + Send + 'static>> {
        let url = self.url;
        let timeout = self.timeout;
        let parser = self.parser;
        let subscription = self.subscription;

        let stream = async_stream::stream! {
            loop {
                // Connection setup
                tracing::trace!("Connecting to {}", url);
                let (mut ws, _response) = match tokio_tungstenite::connect_async(&url).await {
                    Ok((ws, response)) => {
                        tracing::trace!(?response, "Connected to {}", url);
                        (ws, response)
                    }
                    Err(e) => {
                        yield Err(ExchangeStreamError::ConnectionError(e.to_string()));
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                        continue;
                    }
                };
                
                let connected_at = Instant::now();
                let messages = match subscription.messages() {
                    Ok(messages) => messages,
                    Err(e) => {
                        yield Err(ExchangeStreamError::SubscriptionError(e.to_string()));
                        continue;
                    }
                };
                
                let mut msg_stream = futures::stream::iter(messages.into_iter().map(Ok));
                if let Err(e) = ws.send_all(&mut msg_stream).await {
                    yield Err(ExchangeStreamError::StreamError(e.to_string()));
                    continue;
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
                                        Ok(Some(parsed)) => yield Ok(parsed),
                                        Ok(None) => {}, // skip empty results
                                        Err(e) => yield Err(ExchangeStreamError::MessageError(e.to_string())),
                                    }
                                }
                                Some(Ok(Message::Close(frame))) => {
                                    tracing::error!("Stream closed: {frame:?}");
                                    yield Err(ExchangeStreamError::StreamNotConnected(format!(
                                        "Close frame: {frame:?}"
                                    )));
                                    break;
                                }
                                Some(Ok(_)) => {
                                    // Ping/pong handled automatically
                                }
                                Some(Err(e)) => {
                                    tracing::error!("Stream error: {e:?}");
                                    yield Err(ExchangeStreamError::StreamError(e.to_string()));
                                    break;
                                }
                                None => {
                                    tracing::warn!("WebSocket stream ended");
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
                    }
                }
            }
        };

        Box::pin(stream)
    }
}

// Keep the ExchangeStream type alias for backward compatibility
pub type ExchangeStream<T> = Pin<Box<dyn Stream<Item = Result<T, ExchangeStreamError>> + Send + 'static>>;
