use std::time::Duration;

use crate::models::NormalizedQuote;
use crate::models::NormalizedTrade;
use crate::timeboost::bids::BidData;
use crate::{ethereum::BlockMetadata, models::NormalizedEvent};
use clickhouse::Row;
use clickhouse::{Client, inserter::Quantities};
use eyre::WrapErr;
use futures::pin_mut;
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};

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

impl From<NormalizedQuote> for ClickhouseQuote {
    fn from(quote: NormalizedQuote) -> Self {
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

impl From<NormalizedTrade> for ClickhouseTrade {
    fn from(trade: NormalizedTrade) -> Self {
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
            .with_url(config.url())
            .with_user(config.user)
            .with_password(config.password);
        Self { client }
    }

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
        let mut count = 0;
        while let Some(metadata) = stream.next().await {
            inserter.write(&metadata)?;
            count += 1;
            // Write 4 blocks per db transaction.
            if count % 4 == 0 {
                inserter.commit().await?;
            }
        }
        tracing::info!("Wrote {} block metadata to clickhouse", count);
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

    pub async fn get_bids_by_round(&self, round: u64) -> eyre::Result<Vec<BidData>> {
        let query = self
            .client
            .query("SELECT * FROM timeboost.bids WHERE round = ?")
            .bind(round);
        query
            .fetch_all::<BidData>()
            .await
            .wrap_err("failed to get bids by round")
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
        }
        inserter.commit().await?;
        inserter.end().await.wrap_err("failed to write bids")
    }

    pub async fn write_events(
        &self,
        event_stream: impl Stream<Item = Vec<NormalizedEvent>>,
    ) -> eyre::Result<()> {
        let mut trade_inserter = self
            .client
            .inserter("cex.normalized_trades")?
            .with_max_rows(500)
            .with_period(Some(Duration::from_secs(1)))
            .with_period_bias(0.1);

        let mut quote_inserter = self
            .client
            .inserter("cex.normalized_quotes")?
            .with_max_rows(500)
            .with_period(Some(Duration::from_secs(1)))
            .with_period_bias(0.1);

        pin_mut!(event_stream);
        while let Some(events) = event_stream.next().await {
            for event in events {
                match event {
                    NormalizedEvent::Trade(trade) => {
                        let trade: ClickhouseTrade = trade.into();
                        trade_inserter.write(&trade)?;
                    }
                    NormalizedEvent::Quote(quote) => {
                        let quote: ClickhouseQuote = quote.into();
                        quote_inserter.write(&quote)?;
                    }
                }
                trade_inserter.commit().await?;
                quote_inserter.commit().await?;
            }
        }
        trade_inserter.end().await?;
        quote_inserter.end().await?;
        Ok(())
    }
}
