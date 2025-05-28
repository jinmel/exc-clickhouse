pub mod binance;
pub mod upbit;

use crate::models::{NormalizedEvent, NormalizedQuote, NormalizedTrade};
use async_trait::async_trait;
use futures::SinkExt;
use futures::stream::{Stream, StreamExt};
use std::future::Future;
use std::pin::Pin;
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
}

pub struct ExchangeStream<T: Send + 'static> {
    inner: ReceiverStream<Result<T, ExchangeStreamError>>,
    handle: tokio::task::JoinHandle<()>,
}

impl<T: Send + 'static> ExchangeStream<T> {
    pub async fn new<F>(
        url: &str,
        parser: F,
        req_msg: Option<String>,
    ) -> Result<Self, ExchangeStreamError>
    where
        F: Fn(&str) -> Result<T, ExchangeStreamError> + Send + Sync + 'static,
    {
        let (mut ws, _) = connect_async(url)
            .await
            .map_err(|e| ExchangeStreamError::StreamError(e.to_string()))?;

        if let Some(req_msg) = req_msg.clone() {
            tracing::debug!("Sending init message: {}", req_msg);
            ws.send(Message::Text(req_msg.into()))
                .await
                .map_err(|e| ExchangeStreamError::StreamError(e.to_string()))?;
        }

        let (tx, rx) = tokio::sync::mpsc::channel(100);

        let ws_url = url.to_owned().clone();

        let handle = tokio::spawn(async move {
            loop {
                let msg = ws.next().await;
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        // Parse and send the message
                        let parsed = parser(&text);
                        if let Err(_) = tx.send(parsed).await {
                            break; // Channel closed
                        }
                    }
                    Some(Ok(Message::Binary(bin))) => {
                        let text = String::from_utf8(bin.to_vec()).unwrap();
                        let parsed = parser(&text);
                        if let Err(_) = tx.send(parsed).await {
                            break; // Channel closed
                        }
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let error = Err(ExchangeStreamError::StreamNotConnected(format!(
                            "Close frame: {frame:?}"
                        )));
                        let _ = tx.send(error).await;
                        break;
                    }
                    Some(Ok(_)) => {
                        // Ping/pong handled automatically, continue
                        continue;
                    }
                    Some(Err(e)) => {
                        tracing::error!("Error receiving message: {:?} {}", e, ws_url);
                        let error = Err(ExchangeStreamError::StreamError(e.to_string()));
                        let _ = tx.send(error).await;
                        break;
                    }
                    None => {
                        // Stream ended
                        break;
                    }
                }

                // Send periodic message if needed
                if let Some(req_msg) = req_msg.clone() {
                    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                    if let Err(e) = ws.send(Message::Text(req_msg.into())).await {
                        tracing::error!("Error sending message: {}", e);
                    }
                }
            }
        });

        Ok(Self {
            inner: ReceiverStream::new(rx),
            handle,
        })
    }

    pub fn shutdown(self) {
        self.handle.abort();
    }
}

impl<T: Send + 'static> Stream for ExchangeStream<T> {
    type Item = Result<T, ExchangeStreamError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl<T: Send + 'static> Drop for ExchangeStream<T> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[async_trait]
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
