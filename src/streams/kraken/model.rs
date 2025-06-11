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
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid timestamp: {e}")))?
            .timestamp_micros() as u64;

        let side = match trade.side.as_str() {
            "buy" => TradeSide::Buy,
            "sell" => TradeSide::Sell,
            _ => {
                return Err(ExchangeStreamError::Message(format!(
                    "Invalid trade side: {}",
                    trade.side
                )));
            }
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
            .map_err(|e| ExchangeStreamError::Message(format!("Failed to get timestamp: {e}")))?
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
    #[serde(rename = "type")]
    pub message_type: String,
    pub data: Vec<TradeData>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TickerMessage {
    #[serde(rename = "type")]
    pub message_type: String,
    pub data: Vec<TickerData>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SubscriptionMessage {
    pub method: String,
    pub result: Option<SubscriptionData>,
    pub success: bool,
    pub error: Option<String>,
    pub time_in: String,
    pub time_out: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SubscriptionData {
    pub channel: String,
    pub symbol: String,
    pub snapshot: bool,
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
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PongMessage {
    pub method: String,
    pub result: Option<PongData>,
    pub error: Option<String>,
    pub success: Option<bool>,
    pub time_in: String,
    pub time_out: String,
    pub req_id: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PongData {
    pub warnings: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "channel")]
pub enum Response {
    #[serde(rename = "trade")]
    Trade(TradeMessage),
    #[serde(rename = "ticker")]
    Ticker(TickerMessage),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum KrakenMessage {
    Response(Response),
    Status(StatusMessage),
    Subscription(SubscriptionMessage),
    Heartbeat(HeartbeatMessage),
    Pong(PongMessage),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trade_message_parsing() {
        let json = r#"{
            "channel": "trade",
            "type": "snapshot",
            "data": [
                {
                    "symbol": "MATIC/USD",
                    "side": "buy",
                    "price": 0.5147,
                    "qty": 6423.46326,
                    "ord_type": "limit",
                    "trade_id": 4665846,
                    "timestamp": "2023-09-25T07:48:36.925533Z"
                },
                {
                    "symbol": "MATIC/USD",
                    "side": "buy",
                    "price": 0.5147,
                    "qty": 1136.19677815,
                    "ord_type": "limit",
                    "trade_id": 4665847,
                    "timestamp": "2023-09-25T07:49:36.925603Z"
                }
            ]
        }"#;
        let parsed: KrakenMessage = serde_json::from_str(json).expect("Failed to parse JSON");

        match parsed {
            KrakenMessage::Response(Response::Trade(trade_msg)) => {
                assert_eq!(trade_msg.message_type, "snapshot");
                assert_eq!(trade_msg.data.len(), 2);

                let trade_data = &trade_msg.data[0];
                assert_eq!(trade_data.symbol, "MATIC/USD");
                assert_eq!(trade_data.side, "buy");
                assert_eq!(trade_data.price, 0.5147);
                assert_eq!(trade_data.qty, 6423.46326);
                assert_eq!(trade_data.ord_type, "limit");
                assert_eq!(trade_data.trade_id, 4665846);
                assert_eq!(trade_data.timestamp, "2023-09-25T07:48:36.925533Z");

                let trade_data = &trade_msg.data[1];
                assert_eq!(trade_data.symbol, "MATIC/USD");
                assert_eq!(trade_data.side, "buy");
                assert_eq!(trade_data.price, 0.5147);
                assert_eq!(trade_data.qty, 1136.19677815);
                assert_eq!(trade_data.ord_type, "limit");
                assert_eq!(trade_data.trade_id, 4665847);
                assert_eq!(trade_data.timestamp, "2023-09-25T07:49:36.925603Z");
            }
            _ => panic!("Expected KrakenMessage::Response(Response::Trade), got {:?}", parsed),
        }
    }

    #[test]
    fn test_ticker_message_parsing() {
        let json = r#"{
            "channel": "ticker",
            "type": "snapshot",
            "data": [
                {
                    "symbol": "ALGO/USD",
                    "bid": 0.10025,
                    "bid_qty": 740.0,
                    "ask": 0.10036,
                    "ask_qty": 1361.44813783,
                    "last": 0.10035,
                    "volume": 997038.98383185,
                    "vwap": 0.10148,
                    "low": 0.09979,
                    "high": 0.10285,
                    "change": -0.00017,
                    "change_pct": -0.17
                }
            ]
        }"#;
        let parsed: KrakenMessage = serde_json::from_str(json).expect("Failed to parse JSON");

        match parsed {
            KrakenMessage::Response(Response::Ticker(ticker_msg)) => {
                assert_eq!(ticker_msg.message_type, "snapshot");
                assert_eq!(ticker_msg.data.len(), 1);

                let ticker_data = &ticker_msg.data[0];
                assert_eq!(ticker_data.symbol, "ALGO/USD");
                assert_eq!(ticker_data.bid, 0.10025);
                assert_eq!(ticker_data.bid_qty, 740.0);
                assert_eq!(ticker_data.ask, 0.10036);
                assert_eq!(ticker_data.ask_qty, 1361.44813783);
                assert_eq!(ticker_data.last, 0.10035);
                assert_eq!(ticker_data.volume, 997038.98383185);
                assert_eq!(ticker_data.vwap, 0.10148);
                assert_eq!(ticker_data.low, 0.09979);
                assert_eq!(ticker_data.high, 0.10285);
                assert_eq!(ticker_data.change, -0.00017);
                assert_eq!(ticker_data.change_pct, -0.17);
            }
            _ => panic!("Expected KrakenMessage::Ticker, got {:?}", parsed),
        }
    }

    #[test]
    fn test_status_message_parsing() {
        let json = r#"{
            "channel": "status",
            "type": "update",
            "data": [{
                "version": "2.0.10",
                "system": "online",
                "api_version": "v2",
                "connection_id": 3171616182403061789
            }]
        }"#;
        let parsed: KrakenMessage = serde_json::from_str(json).expect("Failed to parse JSON");

        match parsed {
            KrakenMessage::Status(status_msg) => {
                assert_eq!(status_msg.message_type, "update");
                assert_eq!(status_msg.data.len(), 1);

                let status_data = &status_msg.data[0];
                assert_eq!(status_data.version, "2.0.10");
                assert_eq!(status_data.system, "online");
                assert_eq!(status_data.api_version, "v2");
                assert_eq!(status_data.connection_id, 3171616182403061789);
            }
            _ => panic!(
                "Expected KrakenMessage::Response(Response::Status), got {:?}",
                parsed
            ),
        }
    }

    #[test]
    fn test_subscription_ack_message_parsing() {
        let json = r#"{
            "method": "subscribe",
            "result": {
                "channel": "ticker",
                "snapshot": true,
                "symbol": "ALGO/USD"
            },
            "success": true,
            "time_in": "2023-09-25T09:04:31.742599Z",
            "time_out": "2023-09-25T09:04:31.742648Z"
        }"#;
        let parsed: KrakenMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            KrakenMessage::Subscription(subscription_msg) => {
                assert_eq!(subscription_msg.method, "subscribe");
                assert_eq!(subscription_msg.time_in, "2023-09-25T09:04:31.742599Z");
                assert_eq!(subscription_msg.time_out, "2023-09-25T09:04:31.742648Z");
                assert_eq!(subscription_msg.result.as_ref().unwrap().channel, "ticker");
                assert_eq!(subscription_msg.result.as_ref().unwrap().symbol, "ALGO/USD");
                assert!(subscription_msg.result.as_ref().unwrap().snapshot);
                assert!(subscription_msg.error.is_none());
                assert!(subscription_msg.success);
            }
            _ => panic!("Expected KrakenMessage::Pong, got {:?}", parsed),
        }
    }

    #[test]
    fn test_pong_message_parsing() {
        let json = r#"{
            "method": "pong",
            "req_id": 101,
            "time_in": "2023-09-24T14:10:23.799685Z",
            "time_out": "2023-09-24T14:10:23.799703Z"
        }"#;
        let parsed: KrakenMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            KrakenMessage::Pong(pong_msg) => {
                assert_eq!(pong_msg.method, "pong");
                assert_eq!(pong_msg.req_id, Some(101));
            }
            _ => panic!("Expected KrakenMessage::Pong, got {:?}", parsed),
        }
    }

    #[test]
    fn test_subscription_error_message_parsing() {
        let json = r#"{
            "method": "subscribe",
            "error": "Currency pair not in ISO 4217-A3 format CRV-USD",
            "success": false,
            "time_in": "2025-06-11T07:36:58.524650Z",
            "time_out": "2025-06-11T07:36:58.524663Z",
            "req_id": 7017059621769764116
        }"#;
        let parsed: KrakenMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            KrakenMessage::Subscription(subscription_msg) => {
                assert_eq!(subscription_msg.method, "subscribe");
                assert_eq!(subscription_msg.error, Some(String::from("Currency pair not in ISO 4217-A3 format CRV-USD")));
                assert_eq!(subscription_msg.time_in, "2025-06-11T07:36:58.524650Z");
                assert_eq!(subscription_msg.time_out, "2025-06-11T07:36:58.524663Z");
                assert!(subscription_msg.result.is_none());
                assert!(!subscription_msg.success);
            }
            _ => panic!("Expected KrakenMessage::Subscription, got {:?}", parsed),
        }
    }
}
