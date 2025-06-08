use crate::streams::ExchangeStreamError;
use crate::streams::Parser;
use crate::streams::Subscription;
use futures::SinkExt;
use futures::stream::Stream;
use futures::stream::StreamExt;
use std::fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::time::{Duration, Instant};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_tungstenite::tungstenite::Message;

pub struct ExchangeStream<T, P, S>
where
    T: Send + 'static,
    P: Parser<Vec<T>> + Send + 'static + Clone + Unpin + Sync,
    S: Subscription + Send + 'static + Clone + Unpin,
{
    inner: Option<UnboundedReceiverStream<Result<T, ExchangeStreamError>>>,
    handle: Option<tokio::task::JoinHandle<Result<(), ExchangeStreamError>>>,
    timeout: Option<Duration>,
    parser: P,
    url: String,
    subscription: S,
}

impl<T, P, S> ExchangeStream<T, P, S>
where
    T: Send + 'static + Debug,
    P: Parser<Vec<T>> + Send + 'static + Clone + Unpin + Sync,
    S: Subscription + Send + 'static + Clone + Unpin,
{
    pub async fn new(
        url: &str,
        timeout: Option<Duration>,
        parser: P,
        subscription: S,
    ) -> Result<Self, ExchangeStreamError> {
        Ok(Self {
            inner: None,
            handle: None,
            timeout,
            parser,
            url: url.to_owned(),
            subscription,
        })
    }

    // Starts the internal task to pump the events from the websocket stream
    pub async fn run(&mut self) -> Result<(), ExchangeStreamError> {
        if let Some(handle) = &self.handle {
            handle.abort();
            self.handle = None;
        }

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<T, ExchangeStreamError>>();

        let url = self.url.clone();
        let sub = self.subscription.clone();
        let parser = self.parser.clone();
        let timeout = self.timeout;

        let handle = tokio::spawn(async move {
            loop {
                tracing::trace!("Connecting to {}", &url);
                let (mut ws, response) = tokio_tungstenite::connect_async(&url)
                    .await
                    .map_err(|e| ExchangeStreamError::ConnectionError(e.to_string()))
                    .unwrap();
                tracing::trace!(?response, "Connected to {}", url);
                let connected_at = Instant::now();
                let messages = sub
                    .messages()
                    .map_err(|e| ExchangeStreamError::SubscriptionError(e.to_string()))?;
                let mut msg_stream = futures::stream::iter(messages.into_iter().map(Ok));
                ws.send_all(&mut msg_stream)
                    .await
                    .map_err(|e| ExchangeStreamError::StreamError(e.to_string()))?;

                // Create interval for periodic messages (e.g., every 30 seconds)
                let mut interval = tokio::time::interval(
                    sub.heartbeat_interval().unwrap_or(Duration::from_secs(30)),
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
                                    let parsed = parser
                                        .parse(&text)
                                        .map_err(|e| ExchangeStreamError::MessageError(e.to_string()))?;
                                    if let Some(parsed) = parsed {
                                        for item in parsed {
                                            tx.send(Ok(item))
                                                .map_err(|e| ExchangeStreamError::StreamError(e.to_string()))?;
                                        }
                                    }
                                }
                                Some(Ok(Message::Close(frame))) => {
                                    tracing::error!("Stream closed: {frame:?}");
                                    tx.send(Err(ExchangeStreamError::StreamNotConnected(format!(
                                        "Close frame: {frame:?}"
                                    ))))
                                    .map_err(|e| ExchangeStreamError::StreamError(e.to_string()))?;
                                    break;
                                }
                                Some(Ok(_)) => {
                                    // Ping/pong handled automatically
                                }
                                Some(Err(e)) => {
                                    tracing::error!("Stream error: {e:?}");
                                    tx.send(Err(ExchangeStreamError::StreamError(e.to_string())))
                                        .map_err(|e| ExchangeStreamError::StreamError(e.to_string()))?;
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
                            let heartbeat_msg = sub.heartbeat();
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
        });
        self.handle = Some(handle);
        self.inner = Some(UnboundedReceiverStream::new(rx));
        Ok(())
    }
}

impl<T, P, S> Stream for ExchangeStream<T, P, S>
where
    T: Send + 'static,
    P: Parser<Vec<T>> + Send + 'static + Clone + Unpin + Sync,
    S: Subscription + Send + 'static + Clone + Unpin,
{
    type Item = Result<T, ExchangeStreamError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(inner) = &mut this.inner {
            Pin::new(inner).poll_next(cx)
        } else {
            Poll::Pending
        }
    }
}

impl<T, P, S> Drop for ExchangeStream<T, P, S>
where
    T: Send + 'static,
    P: Parser<Vec<T>> + Send + 'static + Clone + Unpin + Sync,
    S: Subscription + Send + 'static + Clone + Unpin,
{
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}
