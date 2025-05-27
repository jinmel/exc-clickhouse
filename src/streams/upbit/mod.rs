use async_trait::async_trait;
use futures::SinkExt;
use futures::stream::Stream;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use url::Url;
use uuid::Uuid;

use crate::{
    models::{NormalizedQuote, NormalizedTrade},
    streams::{ExchangeClient, ExchangeStream, ExchangeStreamError},
};

pub const DEFAULT_UPBIT_WS_URL: &str = "wss://api.upbit.com/websocket/v1";

pub mod model;
pub mod parser;

pub struct UpbitClient {
    symbols: Vec<String>,
    base_url: String,
    enable_quote: bool,
    enable_trade: bool,
    uid: Uuid,
}

impl UpbitClient {
    /// Creates a new UpbitClientBuilder with default values
    pub fn builder() -> UpbitClientBuilder {
        UpbitClientBuilder::default()
    }

    fn build_subscription_message(
        &self,
        stream_type: &str,
    ) -> Result<serde_json::Value, ExchangeStreamError> {
        if self.symbols.is_empty() {
            return Err(ExchangeStreamError::InvalidConfiguration(
                "No symbols provided".to_string(),
            ));
        }

        Ok(serde_json::json!([
            {
              "ticket": self.uid.to_string(),
            },
            {
              "type": stream_type,
              "codes": self.symbols,
            },
            {
              "format": "SIMPLE"
            }
        ]))
    }
}

#[async_trait]
impl ExchangeClient for UpbitClient {
    type TradeStream = ExchangeStream<NormalizedTrade>;
    type QuoteStream = ExchangeStream<NormalizedQuote>;

    async fn normalized_trades(&self) -> Result<Self::TradeStream, ExchangeStreamError> {
        if !self.enable_trade {
            return Err(ExchangeStreamError::ParseError(
                "Trade streams not enabled".to_string(),
            ));
        }

        let subscribe_msg = self.build_subscription_message("ticker")?;

        ExchangeStream::new(
            &self.base_url,
            parser::parse_upbit_trade,
            Some(Box::new(move |mut ws| {
                Box::pin(async move {
                    let res = ws
                        .send(Message::Text(
                            serde_json::to_string(&subscribe_msg).unwrap().into(),
                        ))
                        .await;
                    if let Err(e) = res {
                        return Err(ExchangeStreamError::StreamError(e.to_string()));
                    }
                    Ok(ws)
                })
            })),
        )
        .await
    }

    async fn normalized_quotes(&self) -> Result<Self::QuoteStream, ExchangeStreamError> {
        if !self.enable_quote {
            return Err(ExchangeStreamError::ParseError(
                "Quote streams not enabled".to_string(),
            ));
        }

        let subscribe_msg = self.build_subscription_message("trade")?;

        ExchangeStream::new(
            &self.base_url,
            parser::parse_upbit_quote,
            Some(Box::new(move |mut ws| {
                Box::pin(async move {
                    let res = ws
                        .send(Message::Text(
                            serde_json::to_string(&subscribe_msg).unwrap().into(),
                        ))
                        .await;
                    if let Err(e) = res {
                        return Err(ExchangeStreamError::StreamError(e.to_string()));
                    }
                    Ok(ws)
                })
            })),
        )
        .await
    }
}

/// Builder for the UpbitClient struct
pub struct UpbitClientBuilder {
    symbols: Vec<String>,
    base_url: String,
    enable_quote: bool,
    enable_trade: bool,
    uid: Uuid,
}

impl Default for UpbitClientBuilder {
    fn default() -> Self {
        Self {
            symbols: vec![],
            base_url: DEFAULT_UPBIT_WS_URL.to_string(),
            enable_quote: false,
            enable_trade: false,
            uid: Uuid::new_v4(),
        }
    }
}

impl UpbitClientBuilder {
    pub fn add_symbols(mut self, symbols: Vec<impl Into<String>>) -> Self {
        // Upbit uses uppercase symbols
        self.symbols
            .extend(symbols.into_iter().map(|s| s.into().to_uppercase()));
        self
    }

    pub fn with_quotes(mut self, enable: bool) -> Self {
        self.enable_quote = enable;
        self
    }

    pub fn with_trades(mut self, enable: bool) -> Self {
        self.enable_trade = enable;
        self
    }

    pub fn with_uid(mut self, uid: Uuid) -> Self {
        self.uid = uid;
        self
    }

    /// Build the UpbitClient instance
    pub fn build(self) -> eyre::Result<UpbitClient> {
        Ok(UpbitClient {
            symbols: self.symbols,
            base_url: self.base_url,
            enable_quote: self.enable_quote,
            enable_trade: self.enable_trade,
            uid: self.uid,
        })
    }
}
