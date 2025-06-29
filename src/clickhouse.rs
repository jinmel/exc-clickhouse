use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::allium::DexVolume;
use crate::models::NormalizedQuote;
use crate::models::NormalizedTrade;
use crate::timeboost::bids::BidData;
use crate::trading_pairs::TradingPair;
use crate::{
    ethereum::BlockMetadata,
    models::{ClickhouseMessage, EthereumMetadataMessage, ExpresslaneMessage, NormalizedEvent},
};
use clickhouse::inserter::Inserter;
use clickhouse::Compression;
use clickhouse::Row;
use clickhouse::{inserter::Quantities, Client};
use eyre::WrapErr;
use futures::pin_mut;
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tower::Service;


#[derive(Debug, Clone)]
pub struct ClickHouseConfig {
    pub url: String,
    pub port: String,
    pub user: String,
    pub password: String,
}

impl ClickHouseConfig {
    fn get_env_var(key: &str) -> eyre::Result<String> {
        std::env::var(key).wrap_err(format!(
            "Clickhouse config: {key} environment variable is not set"
        ))
    }

    pub fn from_env() -> eyre::Result<Self> {
        Ok(Self {
            url: Self::get_env_var("CLICKHOUSE_URL")?,
            port: Self::get_env_var("CLICKHOUSE_PORT")?,
            user: Self::get_env_var("CLICKHOUSE_USER")?,
            password: Self::get_env_var("CLICKHOUSE_PASS")?,
        })
    }

    pub fn url(&self) -> String {
        format!("{}:{}", self.url, self.port)
    }
}
#[derive(Clone)]
pub struct ClickHouseService {
    client: Client,
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
struct ClickhouseQuote {
    pub exchange: String,
    pub symbol: String,
    pub timestamp: u64,
    pub ask_amount: f64,
    pub ask_price: f64,
    pub bid_price: f64,
    pub bid_amount: f64,
}

impl From<&NormalizedQuote> for ClickhouseQuote {
    fn from(quote: &NormalizedQuote) -> Self {
        Self {
            exchange: quote.exchange.to_string(),
            symbol: quote.symbol.to_string(),
            timestamp: quote.timestamp,
            ask_amount: quote.ask_amount,
            ask_price: quote.ask_price,
            bid_price: quote.bid_price,
            bid_amount: quote.bid_amount,
        }
    }
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
struct ClickhouseTrade {
    pub exchange: String,
    pub symbol: String,
    pub timestamp: u64,
    pub side: String,
    pub price: f64,
    pub amount: f64,
}

impl From<&NormalizedTrade> for ClickhouseTrade {
    fn from(trade: &NormalizedTrade) -> Self {
        Self {
            exchange: trade.exchange.to_string(),
            symbol: trade.symbol.to_string(),
            timestamp: trade.timestamp,
            side: trade.side.to_string(),
            price: trade.price,
            amount: trade.amount,
        }
    }
}

impl ClickHouseService {
    pub fn new(config: ClickHouseConfig) -> Self {
        let client = Client::default()
            .with_compression(Compression::Lz4)
            .with_url(config.url())
            .with_user(config.user)
            .with_password(config.password);
        Self { client }
    }

    #[allow(dead_code)]
    pub async fn write_block_metadata_stream(
        &self,
        stream: impl Stream<Item = BlockMetadata>,
    ) -> eyre::Result<Quantities> {
        let mut inserter = self
            .client
            .inserter("ethereum.blocks")?
            .with_max_rows(100)
            .with_period(Some(Duration::from_secs(1)))
            .with_period_bias(0.1);
        pin_mut!(stream);
        while let Some(metadata) = stream.next().await {
            inserter.write(&metadata)?;
            inserter.commit().await?;
        }
        inserter
            .end()
            .await
            .wrap_err("failed to write block metadata")
    }

    pub async fn get_latest_bid(&self) -> eyre::Result<BidData> {
        let query = self
            .client
            .query("SELECT * FROM timeboost.bids ORDER BY round DESC LIMIT 1");
        query
            .fetch_one::<BidData>()
            .await
            .wrap_err("failed to get latest bid")
    }

    pub async fn write_express_lane_bids(&self, bids: Vec<BidData>) -> eyre::Result<Quantities> {
        if bids.is_empty() {
            return Ok(Quantities::ZERO);
        }

        let mut inserter = self
            .client
            .inserter("timeboost.bids")?
            .with_max_rows(100)
            .with_period(Some(Duration::from_secs(1)))
            .with_period_bias(0.1);

        for bid in bids {
            tracing::debug!(?bid.round, ?bid.timestamp, "writing bid");
            inserter.write(&bid)?;
            inserter.commit().await?;
        }
        inserter.end().await.wrap_err("failed to write bids")
    }

    pub async fn write_dex_volumes(&self, volumes: Vec<DexVolume>) -> eyre::Result<Quantities> {
        if volumes.is_empty() {
            return Ok(Quantities::ZERO);
        }

        let mut inserter = self
            .client
            .inserter("dex.dex_volumes")?
            .with_max_rows(10000)
            .with_period(Some(Duration::from_secs(1)))
            .with_period_bias(0.1);

        for volume in volumes {
            inserter.write(&volume)?;
            inserter.commit().await?;
        }

        inserter.end().await.wrap_err("failed to write dex volumes")
    }

    fn get_inserter<T: Row>(
        &self,
        table: &str,
        max_rows: u64,
        period_sec: u64,
        period_bias: f64,
    ) -> eyre::Result<Inserter<T>> {
        let inserter: Inserter<T> = self
            .client
            .inserter(table)?
            .with_max_rows(max_rows)
            .with_period(Some(Duration::from_secs(period_sec)))
            .with_period_bias(period_bias);
        Ok(inserter)
    }

    pub async fn handle_msg(&self, batch: &[ClickhouseMessage]) -> eyre::Result<()> {
        tracing::trace!("Writing {} messages to ClickHouse", batch.len());
        let mut trade_inserter: Inserter<ClickhouseTrade> =
            self.get_inserter("cex.normalized_trades", 5000, 1, 0.1)?;
        let mut quote_inserter: Inserter<ClickhouseQuote> =
            self.get_inserter("cex.normalized_quotes", 5000, 1, 0.1)?;
        let mut block_inserter: Inserter<BlockMetadata> =
            self.get_inserter("ethereum.blocks", 100, 1, 0.1)?;
        let mut bid_inserter: Inserter<BidData> =
            self.get_inserter("timeboost.bids", 100, 1, 0.1)?;

        for msg in batch {
            match msg {
                ClickhouseMessage::Cex(NormalizedEvent::Trade(trade)) => {
                    trade_inserter.write(&trade.into())?;
                    trade_inserter.commit().await?;
                }
                ClickhouseMessage::Cex(NormalizedEvent::Quote(quote)) => {
                    quote_inserter.write(&quote.into())?;
                    quote_inserter.commit().await?;
                }
                ClickhouseMessage::Expresslane(ExpresslaneMessage::Bid(bid)) => {
                    bid_inserter.write(bid)?;
                    bid_inserter.commit().await?;
                }
                ClickhouseMessage::Ethereum(EthereumMetadataMessage::Block(block)) => {
                    block_inserter.write(block)?;
                    block_inserter.commit().await?;
                }
            }
        }

        // End all inserters
        trade_inserter.end().await?;
        quote_inserter.end().await?;
        block_inserter.end().await?;
        bid_inserter.end().await?;
        Ok(())
    }

    pub async fn write_trading_pairs(&self, pairs: Vec<TradingPair>) -> eyre::Result<()> {
        if pairs.is_empty() {
            return Ok(());
        }

        let mut inserter = self.get_inserter("cex.trading_pairs", 1000, 1, 0.1)?;
        for pair in pairs {
            tracing::trace!(pair = %pair.pair, base = %pair.base_asset, quote = %pair.quote_asset, "writing trading pair");
            inserter.write(&pair)?;
        }
        inserter.commit().await?;
        inserter.end().await?;
        Ok(())
    }
}

// Tower Service implementation for ClickHouseService
//
// This implementation allows ClickHouseService to be used as a Tower service,
// enabling composition with other Tower middleware like rate limiting, timeouts, etc.
//
// Example usage:
// ```rust
// use tower::{Service, ServiceBuilder};
// use tower::limit::RateLimitLayer;
// use tower::timeout::TimeoutLayer;
// use std::time::Duration;
//
// let config = ClickHouseConfig::from_env()?;
// let clickhouse_service = ClickHouseService::new(config);
//
// // Compose with Tower middleware
// let mut service = ServiceBuilder::new()
//     .layer(TimeoutLayer::new(Duration::from_secs(30)))
//     .layer(RateLimitLayer::new(100, Duration::from_secs(1)))
//     .service(clickhouse_service);
//
// // Use the service
// let message = ClickhouseMessage::Cex(NormalizedEvent::Trade(trade));
// service.ready().await?;
// service.call(message).await?;
// ```
impl Service<Vec<ClickhouseMessage>> for ClickHouseService {
    type Response = ();
    type Error = eyre::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // ClickHouse service is always ready to accept requests
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Vec<ClickhouseMessage>) -> Self::Future {
        let service = self.clone();
        Box::pin(async move {
            // Convert single message to batch and use existing handle_msg function
            service.handle_msg(&req).await
        })
    }
}
