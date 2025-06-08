use serde::{Deserialize, Serialize};

use crate::{
    models::{ExchangeName, NormalizedQuote, NormalizedTrade, TradeSide},
    streams::ExchangeStreamError,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TradeData {
    pub symbol: String,
    pub side: String,
    pub price: f64,
    pub qty: f64,
    pub ord_type: String,
    pub trade_id: u64,
    pub timestamp: String, // RFC3339 format
}

impl TryFrom<TradeData> for NormalizedTrade {
    type Error = ExchangeStreamError;

    fn try_from(trade: TradeData) -> Result<NormalizedTrade, Self::Error> {
        // Parse timestamp from RFC3339 format
        let timestamp = chrono::DateTime::parse_from_rfc3339(&trade.timestamp)
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid timestamp: {e}")))?
            .timestamp_micros() as u64;

        let side = match trade.side.as_str() {
            "buy" => TradeSide::Buy,
            "sell" => TradeSide::Sell,
            _ => return Err(ExchangeStreamError::MessageError(format!("Invalid trade side: {}", trade.side))),
        };

        Ok(NormalizedTrade::new(
            ExchangeName::Kraken,
            &trade.symbol,
            timestamp,
            side,
            trade.price,
            trade.qty,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TickerData {
    pub symbol: String,
    pub bid: f64,
    pub bid_qty: f64,
    pub ask: f64,
    pub ask_qty: f64,
    pub last: f64,
    pub volume: f64,
    pub vwap: f64,
    pub low: f64,
    pub high: f64,
    pub change: f64,
    pub change_pct: f64,
}

impl TryFrom<TickerData> for NormalizedQuote {
    type Error = ExchangeStreamError;

    fn try_from(ticker: TickerData) -> Result<NormalizedQuote, Self::Error> {
        // Use current timestamp since Kraken ticker doesn't include timestamp
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| {
                ExchangeStreamError::MessageError(format!("Failed to get timestamp: {e}"))
            })?
            .as_micros() as u64;

        Ok(NormalizedQuote::new(
            ExchangeName::Kraken,
            &ticker.symbol,
            timestamp,
            ticker.ask_qty,
            ticker.ask,
            ticker.bid,
            ticker.bid_qty,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TradeMessage {
    pub channel: String,
    #[serde(rename = "type")]
    pub message_type: String,
    pub data: Vec<TradeData>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TickerMessage {
    pub channel: String,
    #[serde(rename = "type")]
    pub message_type: String,
    pub data: Vec<TickerData>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SubscriptionResult {
    pub method: String,
    pub result: Option<SubscriptionData>,
    pub success: bool,
    pub error: Option<String>,
    pub time_in: Option<String>,
    pub time_out: Option<String>,
    pub req_id: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SubscriptionData {
    pub channel: String,
    pub symbol: Option<String>,
    pub snapshot: Option<bool>,
    pub event_trigger: Option<String>,
    pub warnings: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StatusMessage {
    pub channel: String,
    #[serde(rename = "type")]
    pub message_type: String,
    pub data: Vec<StatusData>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StatusData {
    pub api_version: String,
    pub connection_id: u64,
    pub system: String,
    pub version: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HeartbeatMessage {
    pub channel: String,
    #[serde(rename = "type")]
    pub message_type: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "channel")]
pub enum Response {
    #[serde(rename = "trade")]
    Trade(TradeMessage),
    #[serde(rename = "ticker")]
    Ticker(TickerMessage),
    #[serde(rename = "status")]
    Status(StatusMessage),
    #[serde(rename = "heartbeat")]
    Heartbeat(HeartbeatMessage),
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum KrakenMessage {
    Response(Response),
    Subscription(SubscriptionResult),
}