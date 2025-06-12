use chrono::DateTime;
use serde::{Deserialize, Serialize};

use crate::{
    models::{ExchangeName, NormalizedQuote, NormalizedTrade, TradeSide},
    streams::ExchangeStreamError,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MatchEvent {
    #[serde(rename = "type")]
    pub typ: String,
    pub trade_id: u64,
    pub maker_order_id: Option<String>,
    pub taker_order_id: Option<String>,
    pub side: String,
    pub size: String,
    pub price: String,
    pub product_id: String,
    pub sequence: Option<u64>,
    pub time: String,
}

impl TryFrom<MatchEvent> for NormalizedTrade {
    type Error = ExchangeStreamError;

    fn try_from(trade: MatchEvent) -> Result<NormalizedTrade, Self::Error> {
        let price = trade
            .price
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid price value: {e}")))?;
        let amount = trade
            .size
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid size value: {e}")))?;
        let side = match trade.side.to_lowercase().as_str() {
            "buy" => TradeSide::Buy,
            "sell" => TradeSide::Sell,
            _ => {
                return Err(ExchangeStreamError::Message(format!(
                    "Unknown trade side: {}",
                    trade.side
                )));
            }
        };
        let ts = DateTime::parse_from_rfc3339(&trade.time)
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid time: {e}")))?
            .timestamp_micros() as u64;
        Ok(NormalizedTrade::new(
            ExchangeName::Coinbase,
            &trade.product_id,
            ts,
            side,
            price,
            amount,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TickerEvent {
    #[serde(rename = "type")]
    pub typ: String,
    pub sequence: Option<u64>,
    pub product_id: String,
    pub price: String,
    pub best_bid: String,
    pub best_ask: String,
    pub best_bid_size: String,
    pub best_ask_size: String,
    pub time: Option<String>,
    pub open_24h: Option<String>,
    pub volume_24h: Option<String>,
    pub low_24h: Option<String>,
    pub high_24h: Option<String>,
    pub volume_30d: Option<String>,
}

impl TryFrom<TickerEvent> for NormalizedQuote {
    type Error = ExchangeStreamError;

    fn try_from(ticker: TickerEvent) -> Result<NormalizedQuote, Self::Error> {
        let timestamp = match ticker.time {
            Some(time_str) => DateTime::parse_from_rfc3339(&time_str)
                .map_err(|e| ExchangeStreamError::Message(format!("Invalid time: {e}")))?
                .timestamp_micros() as u64,
            None => chrono::Utc::now().timestamp_micros() as u64,
        };
        let bid_price = ticker
            .best_bid
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid bid price: {e}")))?;
        let bid_amount = ticker
            .best_bid_size
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid bid size: {e}")))?;
        let ask_price = ticker
            .best_ask
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid ask price: {e}")))?;
        let ask_amount = ticker
            .best_ask_size
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid ask size: {e}")))?;
        Ok(NormalizedQuote::new(
            ExchangeName::Coinbase,
            &ticker.product_id,
            timestamp,
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SubscriptionResult {
    #[serde(rename = "type")]
    pub typ: String,
    pub channels: Vec<ChannelSubscription>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChannelSubscription {
    pub name: String,
    pub product_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ErrorMessage {
    #[serde(rename = "type")]
    pub typ: String,
    pub message: String,
    pub reason: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum CoinbaseMessage {
    Match(MatchEvent),
    Ticker(TickerEvent),
    Subscriptions(SubscriptionResult),
    Error(ErrorMessage),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_match_event_parsing() {
        let json = r#"{
            "type": "match",
            "trade_id": 12345,
            "maker_order_id": "maker123",
            "taker_order_id": "taker456",
            "side": "buy",
            "size": "0.001",
            "price": "50000.00",
            "product_id": "BTC-USD",
            "sequence": 987654321,
            "time": "2023-01-01T12:00:00.000000Z"
        }"#;

        let parsed: CoinbaseMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            CoinbaseMessage::Match(trade) => {
                assert_eq!(trade.typ, "match");
                assert_eq!(trade.trade_id, 12345);
                assert_eq!(trade.side, "buy");
                assert_eq!(trade.price, "50000.00");
                assert_eq!(trade.size, "0.001");
                assert_eq!(trade.product_id, "BTC-USD");
                assert_eq!(trade.maker_order_id, Some("maker123".to_string()));
                assert_eq!(trade.taker_order_id, Some("taker456".to_string()));
            }
            _ => panic!("Expected Match event, got {:?}", parsed),
        }
    }

    #[test]
    fn test_ticker_event_parsing() {
        let json = r#"{
            "type": "ticker",
            "sequence": 123456789,
            "product_id": "BTC-USD",
            "price": "50000.00",
            "best_bid": "49999.99",
            "best_ask": "50000.01",
            "best_bid_size": "1.5",
            "best_ask_size": "0.8",
            "time": "2023-01-01T12:00:00.000000Z"
        }"#;

        let parsed: CoinbaseMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            CoinbaseMessage::Ticker(ticker) => {
                assert_eq!(ticker.typ, "ticker");
                assert_eq!(ticker.sequence, Some(123456789));
                assert_eq!(ticker.product_id, "BTC-USD");
                assert_eq!(ticker.price, "50000.00");
                assert_eq!(ticker.best_bid, "49999.99");
                assert_eq!(ticker.best_ask, "50000.01");
                assert_eq!(ticker.best_bid_size, "1.5");
                assert_eq!(ticker.best_ask_size, "0.8");
            }
            _ => panic!("Expected Ticker event, got {:?}", parsed),
        }
    }

    #[test]
    fn test_subscription_result_parsing() {
        let json = r#"{
            "type": "subscriptions",
            "channels": [{
                "name": "matches",
                "product_ids": ["BTC-USD", "ETH-USD"]
            }]
        }"#;

        let parsed: CoinbaseMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            CoinbaseMessage::Subscriptions(sub) => {
                assert_eq!(sub.typ, "subscriptions");
                assert_eq!(sub.channels.len(), 1);
                assert_eq!(sub.channels[0].name, "matches");
                assert_eq!(sub.channels[0].product_ids, vec!["BTC-USD", "ETH-USD"]);
            }
            _ => panic!("Expected Subscription result, got {:?}", parsed),
        }
    }

    #[test]
    fn test_error_message_parsing() {
        let json = r#"{
            "type": "error",
            "message": "Invalid request",
            "reason": "Bad format"
        }"#;

        let parsed: CoinbaseMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            CoinbaseMessage::Error(error) => {
                assert_eq!(error.typ, "error");
                assert_eq!(error.message, "Invalid request");
                assert_eq!(error.reason, Some("Bad format".to_string()));
            }
            _ => panic!("Expected Error message, got {:?}", parsed),
        }
    }

    #[test]
    fn test_ticker_event_parsing_24h_stats() {
        let json = r#"{
            "type": "ticker",
            "product_id": "LRC-BTC",
            "price": "7.8e-7",
            "open_24h": "7.9e-7",
            "volume_24h": "832",
            "low_24h": "7.7e-7",
            "high_24h": "7.9e-7",
            "volume_30d": "3718342",
            "best_bid": "0.00000077",
            "best_bid_size": "2340",
            "best_ask": "0.00000078",
            "best_ask_size": "13774"
        }"#;

        let parsed: CoinbaseMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            CoinbaseMessage::Ticker(ticker) => {
                assert_eq!(ticker.typ, "ticker");
                assert_eq!(ticker.product_id, "LRC-BTC");
                assert_eq!(ticker.price, "7.8e-7");
                assert_eq!(ticker.best_bid, "0.00000077");
                assert_eq!(ticker.best_ask, "0.00000078");
                assert_eq!(ticker.best_bid_size, "2340");
                assert_eq!(ticker.best_ask_size, "13774");
                assert_eq!(ticker.open_24h, Some("7.9e-7".to_string()));
                assert_eq!(ticker.volume_24h, Some("832".to_string()));
                assert_eq!(ticker.low_24h, Some("7.7e-7".to_string()));
                assert_eq!(ticker.high_24h, Some("7.9e-7".to_string()));
                assert_eq!(ticker.volume_30d, Some("3718342".to_string()));
                assert_eq!(ticker.sequence, None);
                assert_eq!(ticker.time, None);
            }
            _ => panic!("Expected Ticker event, got {:?}", parsed),
        }
    }

    #[test]
    fn test_ticker_to_normalized_quote_conversion() {
        let ticker_json = r#"{
            "type": "ticker",
            "product_id": "LRC-BTC",
            "price": "7.8e-7",
            "best_bid": "0.00000077",
            "best_bid_size": "2340",
            "best_ask": "0.00000078",
            "best_ask_size": "13774"
        }"#;

        let ticker: TickerEvent = serde_json::from_str(ticker_json).expect("Failed to parse ticker");
        let normalized_quote = NormalizedQuote::try_from(ticker).expect("Failed to convert to NormalizedQuote");

        assert_eq!(normalized_quote.exchange, ExchangeName::Coinbase);
        assert_eq!(normalized_quote.symbol.as_str(), "LRC-BTC");
        assert_eq!(normalized_quote.bid_price, 0.00000077);
        assert_eq!(normalized_quote.bid_amount, 2340.0);
        assert_eq!(normalized_quote.ask_price, 0.00000078);
        assert_eq!(normalized_quote.ask_amount, 13774.0);
        assert!(normalized_quote.timestamp > 0); // Should have a timestamp (current time since time field was None)
    }
}
