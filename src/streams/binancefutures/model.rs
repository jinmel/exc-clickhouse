use serde::{Deserialize, Serialize};
use crate::models::{NormalizedTrade, NormalizedQuote, ExchangeName, TradeSide};
use eyre::{Result, WrapErr};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TradeEvent {
  #[serde(rename = "e")]
  pub event_type: String,

  #[serde(rename = "E")]
  pub event_time: u64,

  #[serde(rename = "s")]
  pub symbol: String,

  #[serde(rename = "a")]
  pub aggregate_trade_id: u64,

  #[serde(rename = "p")]
  pub price: String,

  #[serde(rename = "q")]
  pub quantity: String,

  #[serde(rename = "f")]
  pub first_trade_id: u64,

  #[serde(rename = "l")]
  pub last_trade_id: u64,

  #[serde(rename = "T")]
  pub trade_time: u64,

  #[serde(rename = "m")]
  pub is_buyer_market_maker: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BookTickerEvent {
  #[serde(rename = "e")]
  pub event_type: String,

  #[serde(rename = "u")]
  pub update_id: u64,

  #[serde(rename = "E")]
  pub event_time: u64,

  #[serde(rename = "T")]
  pub transaction_time: u64,

  #[serde(rename = "s")]
  pub symbol: String,

  #[serde(rename = "b")]
  pub best_bid_price: String,

  #[serde(rename = "B")]
  pub best_bid_qty: String,

  #[serde(rename = "a")]
  pub best_ask_price: String,

  #[serde(rename = "A")]
  pub best_ask_qty: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum BinanceFuturesMessage {
    Trade(TradeEvent),
    Quote(BookTickerEvent),
    Error(ErrorMessage),
    Subscription(SubscriptionResult),
}

impl TryFrom<TradeEvent> for NormalizedTrade {
    type Error = eyre::Error;

    fn try_from(trade: TradeEvent) -> Result<Self> {
        // Parse price from string to f64
        let price = trade.price.parse::<f64>()
            .wrap_err("Failed to parse price")?;
        
        // Parse quantity from string to f64
        let amount = trade.quantity.parse::<f64>()
            .wrap_err("Failed to parse quantity")?;
        
        Ok(NormalizedTrade::new(
            ExchangeName::BinanceFutures,
            &trade.symbol,
            trade.trade_time * 1000, // Convert milliseconds to microseconds
            if trade.is_buyer_market_maker {
                TradeSide::Sell
            } else {
                TradeSide::Buy
            },
            price,
            amount,
        ))
    }
}

impl TryFrom<BookTickerEvent> for NormalizedQuote {
    type Error = eyre::Error;

    fn try_from(ticker: BookTickerEvent) -> Result<Self> {
        // Parse bid price from string to f64
        let bid_price = ticker.best_bid_price.parse::<f64>()
            .wrap_err("Failed to parse bid price")?;
        
        // Parse bid quantity from string to f64
        let bid_amount = ticker.best_bid_qty.parse::<f64>()
            .wrap_err("Failed to parse bid quantity")?;
        
        // Parse ask price from string to f64
        let ask_price = ticker.best_ask_price.parse::<f64>()
            .wrap_err("Failed to parse ask price")?;
        
        // Parse ask quantity from string to f64
        let ask_amount = ticker.best_ask_qty.parse::<f64>()
            .wrap_err("Failed to parse ask quantity")?;
        
        Ok(NormalizedQuote::new(
            ExchangeName::BinanceFutures,
            &ticker.symbol,
            ticker.event_time * 1000, // Convert milliseconds to microseconds
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trade_event_deserialization() {
        let json = r#"{
            "e": "aggTrade",
            "E": 123456789,
            "s": "BTCUSDT",
            "a": 5933014,
            "p": "0.001",
            "q": "100",
            "f": 100,
            "l": 105,
            "T": 123456785,
            "m": true
        }"#;

        let parsed: BinanceFuturesMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            BinanceFuturesMessage::Trade(trade) => {
                assert_eq!(trade.event_type, "aggTrade");
                assert_eq!(trade.event_time, 123456789);
                assert_eq!(trade.symbol, "BTCUSDT");
                assert_eq!(trade.aggregate_trade_id, 5933014);
                assert_eq!(trade.price, "0.001");
                assert_eq!(trade.quantity, "100");
                assert_eq!(trade.first_trade_id, 100);
                assert_eq!(trade.last_trade_id, 105);
                assert_eq!(trade.trade_time, 123456785);
                assert!(trade.is_buyer_market_maker);
            }
            _ => panic!("Expected Trade event, got {:?}", parsed),
        }
    }

    #[test]
    fn test_book_ticker_event_deserialization() {
        let json = r#"{
            "e": "bookTicker",
            "u": 400900217,
            "E": 1568014460893,
            "T": 1568014460891,
            "s": "BNBUSDT",
            "b": "25.35190000",
            "B": "31.21000000",
            "a": "25.36520000",
            "A": "40.66000000"
        }"#;

        let parsed: BinanceFuturesMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            BinanceFuturesMessage::Quote(ticker) => {
                assert_eq!(ticker.event_type, "bookTicker");
                assert_eq!(ticker.update_id, 400900217);
                assert_eq!(ticker.event_time, 1568014460893);
                assert_eq!(ticker.transaction_time, 1568014460891);
                assert_eq!(ticker.symbol, "BNBUSDT");
                assert_eq!(ticker.best_bid_price, "25.35190000");
                assert_eq!(ticker.best_bid_qty, "31.21000000");
                assert_eq!(ticker.best_ask_price, "25.36520000");
                assert_eq!(ticker.best_ask_qty, "40.66000000");
            }
            _ => panic!("Expected BookTicker event, got {:?}", parsed),
        }
    }

    #[test]
    fn test_subscription_result_message_parsing() {
        let json = r#"{
            "result": null,
            "id": "1"
        }"#;

        let parsed: BinanceFuturesMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            BinanceFuturesMessage::Subscription(sub) => {
                assert_eq!(sub.id, "1");
                assert!(sub.result.is_none());
            }
            _ => panic!("Expected Subscription result, got {:?}", parsed),
        }
    }

    #[test]
    fn test_error_message_parsing() {
        let json = r#"{
            "id": "1",
            "error": {
                "code": -1121,
                "msg": "Invalid symbol."
            }
        }"#;

        let parsed: BinanceFuturesMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            BinanceFuturesMessage::Error(error) => {
                assert_eq!(error.id, "1");
                assert_eq!(error.error.code, -1121);
                assert_eq!(error.error.msg, "Invalid symbol.");
            }
            _ => panic!("Expected Error message, got {:?}", parsed),
        }
    }

    #[test]
    fn test_trade_event_direct() {
        let json = r#"{
            "e": "aggTrade",
            "E": 123456789,
            "s": "BTCUSDT",
            "a": 5933014,
            "p": "0.001",
            "q": "100",
            "f": 100,
            "l": 105,
            "T": 123456785,
            "m": true
        }"#;

        let parsed: TradeEvent = serde_json::from_str(json).expect("Failed to parse TradeEvent");
        assert_eq!(parsed.event_type, "aggTrade");
        assert_eq!(parsed.symbol, "BTCUSDT");
        assert_eq!(parsed.price, "0.001");
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