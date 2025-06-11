use serde::{Deserialize, Serialize};

use crate::{
    models::{ExchangeName, NormalizedQuote, NormalizedTrade, TradeSide},
    streams::ExchangeStreamError,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TradeEvent {
    #[serde(rename = "p")]
    pub price: String,

    #[serde(rename = "v")]
    pub volume: String,

    #[serde(rename = "t")]
    pub trade_time: u64,

    #[serde(rename = "S")]
    pub trade_type: i32, // 1 = buy, 2 = sell
}

impl TryFrom<TradeEvent> for NormalizedTrade {
    type Error = ExchangeStreamError;

    fn try_from(trade: TradeEvent) -> Result<NormalizedTrade, Self::Error> {
        // Parse price and volume strings to f64
        let price = trade
            .price
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid price value: {e}")))?;

        let amount = trade
            .volume
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid volume value: {e}")))?;

        Ok(NormalizedTrade::new(
            ExchangeName::Mexc,
            "UNKNOWN", // Symbol will be set by parser from channel info
            trade.trade_time * 1000, // Convert milliseconds to microseconds
            match trade.trade_type {
                1 => TradeSide::Buy,
                2 => TradeSide::Sell,
                _ => return Err(ExchangeStreamError::Message(format!("Invalid trade type: {}", trade.trade_type))),
            },
            price,
            amount,
        ))
    }
}

impl TradeEvent {
    pub fn to_normalized_trade(&self, symbol: &str) -> Result<NormalizedTrade, ExchangeStreamError> {
        // Parse price and volume strings to f64
        let price = self
            .price
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid price value: {e}")))?;

        let amount = self
            .volume
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid volume value: {e}")))?;

        Ok(NormalizedTrade::new(
            ExchangeName::Mexc,
            symbol,
            self.trade_time * 1000, // Convert milliseconds to microseconds
            match self.trade_type {
                1 => TradeSide::Buy,
                2 => TradeSide::Sell,
                _ => return Err(ExchangeStreamError::Message(format!("Invalid trade type: {}", self.trade_type))),
            },
            price,
            amount,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BookTickerEvent {
    #[serde(rename = "bidPrice")]
    pub bid_price: String,

    #[serde(rename = "bidQty")]
    pub bid_qty: String,

    #[serde(rename = "askPrice")]
    pub ask_price: String,

    #[serde(rename = "askQty")]
    pub ask_qty: String,
}

impl TryFrom<BookTickerEvent> for NormalizedQuote {
    type Error = ExchangeStreamError;

    fn try_from(ticker: BookTickerEvent) -> Result<NormalizedQuote, Self::Error> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| ExchangeStreamError::Message(format!("Failed to get timestamp: {e}")))?
            .as_micros() as u64;

        // Parse ask and bid values
        let ask_amount = ticker.ask_qty.parse::<f64>().map_err(|e| {
            ExchangeStreamError::Message(format!("Invalid ask quantity value: {e}"))
        })?;

        let ask_price = ticker
            .ask_price
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid ask price value: {e}")))?;

        let bid_price = ticker
            .bid_price
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid bid price value: {e}")))?;

        let bid_amount = ticker.bid_qty.parse::<f64>().map_err(|e| {
            ExchangeStreamError::Message(format!("Invalid bid quantity value: {e}"))
        })?;

        Ok(NormalizedQuote::new(
            ExchangeName::Mexc,
            "UNKNOWN", // Symbol will be set by parser from channel info
            timestamp,
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
        ))
    }
}

impl BookTickerEvent {
    pub fn to_normalized_quote(&self, symbol: &str) -> Result<NormalizedQuote, ExchangeStreamError> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| ExchangeStreamError::Message(format!("Failed to get timestamp: {e}")))?
            .as_micros() as u64;

        // Parse ask and bid values
        let ask_amount = self.ask_qty.parse::<f64>().map_err(|e| {
            ExchangeStreamError::Message(format!("Invalid ask quantity value: {e}"))
        })?;

        let ask_price = self
            .ask_price
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid ask price value: {e}")))?;

        let bid_price = self
            .bid_price
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid bid price value: {e}")))?;

        let bid_amount = self.bid_qty.parse::<f64>().map_err(|e| {
            ExchangeStreamError::Message(format!("Invalid bid quantity value: {e}"))
        })?;

        Ok(NormalizedQuote::new(
            ExchangeName::Mexc,
            symbol,
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
    pub id: Option<i32>,
    pub code: i32,
    pub msg: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MexcChannelMessage {
    #[serde(rename = "c")]
    pub channel: String,

    #[serde(rename = "d")]
    pub data: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum MexcMessage {
    ChannelData(MexcChannelMessage),
    Subscription(SubscriptionResult),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trade_event_parsing() {
        let json = r#"{
            "p": "50000.00",
            "v": "0.001",
            "t": 1672531200000,
            "S": 1
        }"#;

        let parsed: TradeEvent = serde_json::from_str(json).expect("Failed to parse JSON");
        assert_eq!(parsed.price, "50000.00");
        assert_eq!(parsed.volume, "0.001");
        assert_eq!(parsed.trade_type, 1);
        assert_eq!(parsed.trade_time, 1672531200000);
    }

    #[test]
    fn test_book_ticker_event_parsing() {
        let json = r#"{
            "bidPrice": "49999.99",
            "bidQty": "1.00000000",
            "askPrice": "50000.01",
            "askQty": "0.50000000"
        }"#;

        let parsed: BookTickerEvent = serde_json::from_str(json).expect("Failed to parse JSON");
        assert_eq!(parsed.bid_price, "49999.99");
        assert_eq!(parsed.ask_price, "50000.01");
        assert_eq!(parsed.bid_qty, "1.00000000");
        assert_eq!(parsed.ask_qty, "0.50000000");
    }

    #[test]
    fn test_subscription_result_parsing() {
        let json = r#"{
            "id": 0,
            "code": 0,
            "msg": "spot@public.deals.v3.api@BTCUSDT"
        }"#;

        let parsed: MexcMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            MexcMessage::Subscription(sub) => {
                assert_eq!(sub.id, Some(0));
                assert_eq!(sub.code, 0);
                assert_eq!(sub.msg, "spot@public.deals.v3.api@BTCUSDT");
            }
            _ => panic!("Expected Subscription result, got {:?}", parsed),
        }
    }

    #[test]
    fn test_channel_message_parsing() {
        let json = r#"{
            "c": "spot@public.deals.v3.api@BTCUSDT",
            "d": {
                "deals": [{
                    "p": "50000.00",
                    "v": "0.001",
                    "t": 1672531200000,
                    "S": 1
                }]
            }
        }"#;

        let parsed: MexcMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            MexcMessage::ChannelData(channel_msg) => {
                assert_eq!(channel_msg.channel, "spot@public.deals.v3.api@BTCUSDT");
                assert!(channel_msg.data.get("deals").is_some());
            }
            _ => panic!("Expected ChannelData, got {:?}", parsed),
        }
    }
}