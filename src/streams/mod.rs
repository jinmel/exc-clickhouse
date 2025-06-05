pub mod binance;
pub mod bybit;
pub mod kucoin;
pub mod okx;

use crate::models::{NormalizedEvent, NormalizedQuote, NormalizedTrade};
use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

#[derive(Debug, thiserror::Error, Clone)]
#[non_exhaustive]
pub enum ExchangeStreamError {
    #[error("Stream error: {0}")]
    StreamError(String),
    #[error("Stream not connected: {0}")]
    StreamNotConnected(String),
    #[error("Parse error: {0}")]
    ParseError(String),
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),
    #[error("Connection error: {0}")]
    ConnectionError(String),
}

type WsPostConnectFn = Box<
    dyn FnOnce(
            WebSocketStream<MaybeTlsStream<TcpStream>>,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Result<
                            WebSocketStream<MaybeTlsStream<TcpStream>>,
                            ExchangeStreamError,
                        >,
                    > + Send,
            >,
        > + Send,
>;

pub struct ExchangeStream<T: Send + 'static> {
    inner: ReceiverStream<Result<T, ExchangeStreamError>>,
    handle: tokio::task::JoinHandle<Result<(), ExchangeStreamError>>,
}

impl<T: Send + 'static> ExchangeStream<T> {
    pub async fn new<F>(
        url: &str,
        parser: F,
        timeout: Option<Duration>,
        post_connect: Option<WsPostConnectFn>,
    ) -> Result<Self, ExchangeStreamError>
    where
        F: Fn(&str) -> Result<T, ExchangeStreamError> + Send + Sync + 'static,
    {
        let url = url.to_string();
        let mut connected_at = Instant::now();
        let (mut ws, _) = connect_async(&url)
            .await
            .map_err(|e| ExchangeStreamError::ConnectionError(e.to_string()))?;

        // Execute post-connect closure if provided
        if let Some(connect_fn) = post_connect {
            ws = connect_fn(ws).await?;
        }

        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        let handle = tokio::spawn(async move {
            loop {
                while let Some(msg) = ws.next().await {
                    if let Some(timeout) = timeout {
                        if connected_at.elapsed() > timeout {
                            tracing::info!("Timeout reached, reconnecting");
                            break;
                        }
                    }
                    match msg {
                        Ok(Message::Text(text)) => {
                            let parsed = parser(&text);
                            tx.send(parsed)
                                .await
                                .map_err(|e| ExchangeStreamError::StreamError(e.to_string()))?;
                        }
                        Ok(Message::Close(frame)) => {
                            tracing::error!("Stream closed: {frame:?}");
                            tx.send(Err(ExchangeStreamError::StreamNotConnected(format!(
                                "Close frame: {frame:?}"
                            ))))
                            .await
                            .map_err(|e| ExchangeStreamError::StreamError(e.to_string()))?;
                            break;
                        }
                        Ok(_) => {
                            // Ping/pong handled automatically
                        }
                        Err(e) => {
                            tracing::error!("Stream error: {e:?}");
                            tx.send(Err(ExchangeStreamError::StreamError(e.to_string())))
                                .await
                                .map_err(|e| ExchangeStreamError::StreamError(e.to_string()))?;
                            break;
                        }
                    }
                }
                tracing::info!("Reconnecting to {url}");
                (ws, _) = connect_async(&url)
                    .await
                    .map_err(|e| ExchangeStreamError::ConnectionError(e.to_string()))?;
                connected_at = Instant::now();
            }
        });

        Ok(Self {
            inner: ReceiverStream::new(rx),
            handle,
        })
    }
}

impl<T: Send + 'static> Stream for ExchangeStream<T> {
    type Item = Result<T, ExchangeStreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl<T: Send + 'static> Drop for ExchangeStream<T> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[async_trait]
#[allow(unused)]
pub trait ExchangeClient {
    type TradeStream: Stream<Item = Result<NormalizedTrade, ExchangeStreamError>>
        + Send
        + Unpin
        + 'static;
    type QuoteStream: Stream<Item = Result<NormalizedQuote, ExchangeStreamError>>
        + Send
        + Unpin
        + 'static;

    async fn normalized_trades(&self) -> Result<Self::TradeStream, ExchangeStreamError>;
    async fn normalized_quotes(&self) -> Result<Self::QuoteStream, ExchangeStreamError>;
}

#[async_trait]
pub trait CombinedStream {
    type CombinedStream: Stream<Item = Result<NormalizedEvent, ExchangeStreamError>>
        + Send
        + Unpin
        + 'static;

    async fn combined_stream(&self) -> Result<Self::CombinedStream, ExchangeStreamError>;
}
