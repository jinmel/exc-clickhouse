use crate::{
    models::{NormalizedEvent, NormalizedQuote, NormalizedTrade},
    streams::{
        ExchangeStreamError,
        coinbase::model::{TickerEvent, TradeEvent},
    },
};

pub fn parse_coinbase_trade(event: &str) -> Result<NormalizedTrade, ExchangeStreamError> {
    let trade: TradeEvent =
        serde_json::from_str(event).map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
    trade.try_into()
}

pub fn parse_coinbase_quote(event: &str) -> Result<NormalizedQuote, ExchangeStreamError> {
    let quote: TickerEvent =
        serde_json::from_str(event).map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
    quote.try_into()
}

pub fn parse_coinbase_combined(event: &str) -> Result<NormalizedEvent, ExchangeStreamError> {
    let value: serde_json::Value =
        serde_json::from_str(event).map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
    let event_type = value
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ExchangeStreamError::ParseError("Missing type".to_string()))?;
    match event_type {
        "match" | "last_match" => Ok(NormalizedEvent::Trade(parse_coinbase_trade(event)?)),
        "ticker" => Ok(NormalizedEvent::Quote(parse_coinbase_quote(event)?)),
        other => Err(ExchangeStreamError::ParseError(format!(
            "Unknown event type: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ExchangeName, TradeSide};

    #[test]
    fn test_parse_coinbase_trade() {
        let msg = r#"{
            "type": "match",
            "trade_id": 1,
            "side": "buy",
            "price": "45000.1",
            "size": "0.2",
            "product_id": "BTC-USD",
            "time": "2024-05-18T12:34:56.789Z"
        }"#;
        let trade = parse_coinbase_trade(msg).unwrap();
        assert!(matches!(trade.exchange, ExchangeName::Coinbase));
        assert_eq!(&trade.symbol[..], "BTC-USD");
        assert_eq!(trade.timestamp, 1716035696789000);
        assert!(matches!(trade.side, TradeSide::Buy));
        assert_eq!(trade.price, 45000.1);
        assert_eq!(trade.amount, 0.2);
    }

    #[test]
    fn test_parse_coinbase_quote() {
        let msg = r#"{
            "type": "ticker",
            "product_id": "BTC-USD",
            "best_bid": "44999.8",
            "best_bid_size": "0.4",
            "best_ask": "45010.2",
            "best_ask_size": "0.3",
            "time": "2024-05-18T12:35:00.000Z"
        }"#;
        let quote = parse_coinbase_quote(msg).unwrap();
        assert!(matches!(quote.exchange, ExchangeName::Coinbase));
        assert_eq!(&quote.symbol[..], "BTC-USD");
        assert_eq!(quote.timestamp, 1716035700000000);
        assert_eq!(quote.ask_price, 45010.2);
        assert_eq!(quote.ask_amount, 0.3);
        assert_eq!(quote.bid_price, 44999.8);
        assert_eq!(quote.bid_amount, 0.4);
    }

    #[test]
    fn test_parse_coinbase_combined_trade() {
        let msg = r#"{
            "type": "match",
            "trade_id": 1,
            "side": "buy",
            "price": "45000.1",
            "size": "0.2",
            "product_id": "BTC-USD",
            "time": "2024-05-18T12:34:56.789Z"
        }"#;
        match parse_coinbase_combined(msg).unwrap() {
            NormalizedEvent::Trade(t) => assert!(matches!(t.exchange, ExchangeName::Coinbase)),
            _ => panic!("Expected trade event"),
        }
    }

    #[test]
    fn test_parse_coinbase_combined_quote() {
        let msg = r#"{
            "type": "ticker",
            "product_id": "BTC-USD",
            "best_bid": "44999.8",
            "best_bid_size": "0.4",
            "best_ask": "45010.2",
            "best_ask_size": "0.3",
            "time": "2024-05-18T12:35:00.000Z"
        }"#;
        match parse_coinbase_combined(msg).unwrap() {
            NormalizedEvent::Quote(q) => assert!(matches!(q.exchange, ExchangeName::Coinbase)),
            _ => panic!("Expected quote event"),
        }
    }
}
