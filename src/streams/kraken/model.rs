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
pub struct PongMessage {
    pub method: String,
    pub result: Option<PongData>,
    pub error: Option<String>,
    pub success: Option<bool>,
    pub req_id: Option<u64>,
    pub time_in: String,
    pub time_out: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PongData {
    pub warnings: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StatusMessage {
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
    Pong(PongMessage),
    Subscription(SubscriptionResult),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_response_status_parsing() {
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
        
        // Test parsing directly as Response first
        let response: Response = serde_json::from_str(json).expect("Failed to parse as Response");
        
        match response {
            Response::Status(status_msg) => {
                assert_eq!(status_msg.message_type, "update");
                assert_eq!(status_msg.data.len(), 1);
                
                let status_data = &status_msg.data[0];
                assert_eq!(status_data.version, "2.0.10");
                assert_eq!(status_data.system, "online");
                assert_eq!(status_data.api_version, "v2");
                assert_eq!(status_data.connection_id, 3171616182403061789);
            }
            _ => panic!("Expected Response::Status, got {:?}", response),
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
            KrakenMessage::Response(Response::Status(status_msg)) => {
                assert_eq!(status_msg.message_type, "update");
                assert_eq!(status_msg.data.len(), 1);
                
                let status_data = &status_msg.data[0];
                assert_eq!(status_data.version, "2.0.10");
                assert_eq!(status_data.system, "online");
                assert_eq!(status_data.api_version, "v2");
                assert_eq!(status_data.connection_id, 3171616182403061789);
            }
            _ => panic!("Expected KrakenMessage::Response(Response::Status), got {:?}", parsed),
        }
    }

    #[test]
    fn test_pong_message_parsing() {
        let json = r#"{
            "method": "pong",
            "req_id": 6341134959799692503,
            "time_in": "2025-06-08T01:29:06.857589Z",
            "time_out": "2025-06-08T01:29:06.857606Z"
        }"#;
        let parsed: KrakenMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        
        match parsed {
            KrakenMessage::Pong(pong_msg) => {
                assert_eq!(pong_msg.method, "pong");
                assert_eq!(pong_msg.req_id, Some(6341134959799692503));
                assert_eq!(pong_msg.time_in, "2025-06-08T01:29:06.857589Z");
                assert_eq!(pong_msg.time_out, "2025-06-08T01:29:06.857606Z");
                assert!(pong_msg.result.is_none());
                assert!(pong_msg.error.is_none());
                assert!(pong_msg.success.is_none()); // success field is not present in the JSON
            }
            _ => panic!("Expected KrakenMessage::Pong, got {:?}", parsed),
        }
    }
}