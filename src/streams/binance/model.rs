use serde::{Deserialize, Serialize};

use crate::{
    models::{ExchangeName, NormalizedQuote, NormalizedTrade, TradeSide},
    streams::ExchangeStreamError,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TradeEvent {
    #[serde(rename = "e")]
    pub event_type: String,

    #[serde(rename = "E")]
    pub event_time: u64,

    #[serde(rename = "s")]
    pub symbol: String,

    #[serde(rename = "t")]
    pub trade_id: u64,

    #[serde(rename = "p")]
    pub price: String,

    #[serde(rename = "q")]
    pub quantity: String,

    #[serde(rename = "T")]
    pub trade_order_time: u64,

    #[serde(rename = "m")]
    pub is_buyer_maker: bool,

    #[serde(skip, rename = "M")]
    pub _ignore: bool,
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

        Ok(NormalizedTrade::new(
            ExchangeName::Binance,
            &trade.symbol,
            trade.event_time * 1000, // Convert milliseconds to microseconds
            if trade.is_buyer_maker {
                TradeSide::Sell
            } else {
                TradeSide::Buy
            },
            price,
            amount,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BookTickerEvent {
    #[serde(rename = "u")]
    pub update_id: u64,

    #[serde(rename = "s")]
    pub symbol: String,

    #[serde(rename = "b")]
    pub best_bid: String,

    #[serde(rename = "B")]
    pub best_bid_quantity: String,

    #[serde(rename = "a")]
    pub best_ask: String,

    #[serde(rename = "A")]
    pub best_ask_quantity: String,
}

impl TryFrom<BookTickerEvent> for NormalizedQuote {
    type Error = ExchangeStreamError;

    fn try_from(ticker: BookTickerEvent) -> Result<NormalizedQuote, Self::Error> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| ExchangeStreamError::Message(format!("Failed to get timestamp: {e}")))?
            .as_micros() as u64;

        // Parse ask and bid values
        let ask_amount = ticker.best_ask_quantity.parse::<f64>().map_err(|e| {
            ExchangeStreamError::Message(format!("Invalid ask quantity value: {e}"))
        })?;

        let ask_price = ticker
            .best_ask
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid ask price value: {e}")))?;

        let bid_price = ticker
            .best_bid
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid bid price value: {e}")))?;

        let bid_amount = ticker.best_bid_quantity.parse::<f64>().map_err(|e| {
            ExchangeStreamError::Message(format!("Invalid bid quantity value: {e}"))
        })?;

        Ok(NormalizedQuote::new(
            ExchangeName::Binance,
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
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ErrorMessage {
    pub id: String,
    pub error: ErrorDetail,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ErrorDetail {
    pub code: i32,
    pub msg: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum BinanceMessage {
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
            "e": "trade",
            "E": 1672531200000,
            "s": "BTCUSDT",
            "t": 12345,
            "p": "50000.00",
            "q": "0.001",
            "T": 1672531200000,
            "m": false,
            "M": true
        }"#;

        let parsed: BinanceMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            BinanceMessage::Trade(trade) => {
                assert_eq!(trade.event_type, "trade");
                assert_eq!(trade.symbol, "BTCUSDT");
                assert_eq!(trade.price, "50000.00");
                assert_eq!(trade.quantity, "0.001");
                assert_eq!(trade.is_buyer_maker, false);
            }
            _ => panic!("Expected Trade event, got {:?}", parsed),
        }
    }

    #[test]
    fn test_book_ticker_event_parsing() {
        let json = r#"{
            "u": 400900217,
            "s": "BTCUSDT",
            "b": "49999.99",
            "B": "1.00000000",
            "a": "50000.01",
            "A": "0.50000000"
        }"#;

        let parsed: BinanceMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            BinanceMessage::Quote(ticker) => {
                assert_eq!(ticker.symbol, "BTCUSDT");
                assert_eq!(ticker.best_bid, "49999.99");
                assert_eq!(ticker.best_ask, "50000.01");
                assert_eq!(ticker.best_bid_quantity, "1.00000000");
                assert_eq!(ticker.best_ask_quantity, "0.50000000");
            }
            _ => panic!("Expected BookTicker event, got {:?}", parsed),
        }
    }

    #[test]
    fn test_subscription_result_parsing() {
        let json = r#"{
            "result": null,
            "id": "1"
        }"#;

        let parsed: BinanceMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            BinanceMessage::Subscription(sub) => {
                assert_eq!(sub.id, "1");
                assert!(sub.result.is_none());
            }
            _ => panic!("Expected Subscription result, got {:?}", parsed),
        }
    }
}
