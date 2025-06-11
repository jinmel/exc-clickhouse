use serde::{Deserialize, Serialize};

use crate::{
    models::{ExchangeName, NormalizedQuote, NormalizedTrade, TradeSide},
    streams::ExchangeStreamError,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TradeEvent {
    #[serde(rename = "p")]
    pub price: String,

    #[serde(rename = "q")]
    pub quantity: String,

    #[serde(rename = "T")]
    pub trade_type: i32, // 1: buy, 2: sell

    #[serde(rename = "t")]
    pub time: u64,

    #[serde(rename = "s")]
    pub symbol: String,
}

impl TryFrom<TradeEvent> for NormalizedTrade {
    type Error = ExchangeStreamError;

    fn try_from(trade: TradeEvent) -> Result<NormalizedTrade, Self::Error> {
        // Parse price and quantity strings to f64
        let price = trade
            .price
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid price value: {e}")))?;

        let amount = trade
            .quantity
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid quantity value: {e}")))?;

        // MEXC trade_type: 1 = buy, 2 = sell
        let side = match trade.trade_type {
            1 => TradeSide::Buy,
            2 => TradeSide::Sell,
            _ => return Err(ExchangeStreamError::Message(format!("Invalid trade type: {}", trade.trade_type))),
        };

        Ok(NormalizedTrade::new(
            ExchangeName::Mexc,
            &trade.symbol,
            trade.time * 1000, // Convert milliseconds to microseconds
            side,
            price,
            amount,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BookTickerEvent {
    #[serde(rename = "s")]
    pub symbol: String,

    #[serde(rename = "b")]
    pub bid_price: String,

    #[serde(rename = "B")]
    pub bid_qty: String,

    #[serde(rename = "a")]
    pub ask_price: String,

    #[serde(rename = "A")]
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
            &ticker.symbol,
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
    pub result: Option<String>,
    pub id: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ErrorMessage {
    pub id: i64,
    pub code: i32,
    pub msg: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum MexcMessage {
    Trade(TradeEvent),
    Quote(BookTickerEvent),
    Subscription(SubscriptionResult),
    Error(ErrorMessage),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trade_event_parsing() {
        let json = r#"{
            "s": "BTCUSDT",
            "p": "50000.00",
            "q": "0.001",
            "T": 1,
            "t": 1672531200000
        }"#;

        let parsed: MexcMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            MexcMessage::Trade(trade) => {
                assert_eq!(trade.symbol, "BTCUSDT");
                assert_eq!(trade.price, "50000.00");
                assert_eq!(trade.quantity, "0.001");
                assert_eq!(trade.trade_type, 1);
            }
            _ => panic!("Expected Trade event, got {:?}", parsed),
        }
    }

    #[test]
    fn test_book_ticker_event_parsing() {
        let json = r#"{
            "s": "BTCUSDT",
            "b": "49999.99",
            "B": "1.00000000",
            "a": "50000.01",
            "A": "0.50000000"
        }"#;

        let parsed: MexcMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            MexcMessage::Quote(ticker) => {
                assert_eq!(ticker.symbol, "BTCUSDT");
                assert_eq!(ticker.bid_price, "49999.99");
                assert_eq!(ticker.ask_price, "50000.01");
                assert_eq!(ticker.bid_qty, "1.00000000");
                assert_eq!(ticker.ask_qty, "0.50000000");
            }
            _ => panic!("Expected BookTicker event, got {:?}", parsed),
        }
    }

    #[test]
    fn test_subscription_result_parsing() {
        let json = r#"{
            "result": null,
            "id": 1
        }"#;

        let parsed: MexcMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            MexcMessage::Subscription(sub) => {
                assert_eq!(sub.id, 1);
                assert!(sub.result.is_none());
            }
            _ => panic!("Expected Subscription result, got {:?}", parsed),
        }
    }
}