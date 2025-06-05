use crate::models::{NormalizedEvent, NormalizedQuote, NormalizedTrade};
use crate::streams::{
    kucoin::model::{TickerData, TradeData, WsMessage},
    ExchangeStreamError,
};

pub fn parse_kucoin_trade(event: &str) -> Result<NormalizedTrade, ExchangeStreamError> {
    let msg: WsMessage<TradeData> =
        serde_json::from_str(event).map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
    msg.data.try_into()
}

pub fn parse_kucoin_quote(event: &str) -> Result<NormalizedQuote, ExchangeStreamError> {
    let msg: WsMessage<TickerData> =
        serde_json::from_str(event).map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
    msg.data.try_into()
}

pub fn parse_kucoin_combined(event: &str) -> Result<NormalizedEvent, ExchangeStreamError> {
    let value: serde_json::Value = serde_json::from_str(event)
        .map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
    let topic = value
        .get("topic")
        .and_then(|t| t.as_str())
        .ok_or_else(|| ExchangeStreamError::ParseError("Missing topic".to_string()))?;
    if topic.starts_with("/market/match") {
        let msg: WsMessage<TradeData> = serde_json::from_value(value)
            .map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
        Ok(NormalizedEvent::Trade(msg.data.try_into()?))
    } else if topic.starts_with("/market/ticker") {
        let msg: WsMessage<TickerData> = serde_json::from_value(value)
            .map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
        Ok(NormalizedEvent::Quote(msg.data.try_into()?))
    } else {
        Err(ExchangeStreamError::ParseError(format!("Unknown topic: {topic}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ExchangeName, TradeSide};

    #[test]
    fn test_parse_kucoin_trade() {
        let msg = r#"{
            "type": "message",
            "topic": "/market/match:BTC-USDT",
            "subject": "trade.l3match",
            "data": {
                "sequence": 1,
                "price": "45000",
                "size": "0.1",
                "side": "buy",
                "symbol": "BTC-USDT",
                "tradeId": "1",
                "ts": 1700000000000
            }
        }"#;
        let trade = parse_kucoin_trade(msg).unwrap();
        assert!(matches!(trade.exchange, ExchangeName::Kucoin));
        assert_eq!(trade.timestamp, 1700000000000);
        assert!(matches!(trade.side, TradeSide::Buy));
        assert_eq!(trade.price, 45000.0);
        assert_eq!(trade.amount, 0.1);
    }

    #[test]
    fn test_parse_kucoin_quote() {
        let msg = r#"{
            "type": "message",
            "topic": "/market/ticker:BTC-USDT",
            "subject": "trade.ticker",
            "data": {
                "sequence": 1,
                "symbol": "BTC-USDT",
                "bestBidPrice": "44999",
                "bestBidSize": "0.2",
                "bestAskPrice": "45001",
                "bestAskSize": "0.3",
                "ts": 1700000000000
            }
        }"#;
        let quote = parse_kucoin_quote(msg).unwrap();
        assert!(matches!(quote.exchange, ExchangeName::Kucoin));
        assert_eq!(quote.timestamp, 1700000000000);
        assert_eq!(quote.ask_price, 45001.0);
        assert_eq!(quote.bid_price, 44999.0);
    }

    #[test]
    fn test_parse_kucoin_combined_trade() {
        let msg = r#"{
            "type": "message",
            "topic": "/market/match:BTC-USDT",
            "subject": "trade.l3match",
            "data": {
                "sequence": 1,
                "price": "45000",
                "size": "0.1",
                "side": "buy",
                "symbol": "BTC-USDT",
                "tradeId": "1",
                "ts": 1700000000000
            }
        }"#;
        match parse_kucoin_combined(msg).unwrap() {
            NormalizedEvent::Trade(t) => assert!(matches!(t.exchange, ExchangeName::Kucoin)),
            _ => panic!("Expected trade event"),
        }
    }

    #[test]
    fn test_parse_kucoin_combined_quote() {
        let msg = r#"{
            "type": "message",
            "topic": "/market/ticker:BTC-USDT",
            "subject": "trade.ticker",
            "data": {
                "sequence": 1,
                "symbol": "BTC-USDT",
                "bestBidPrice": "44999",
                "bestBidSize": "0.2",
                "bestAskPrice": "45001",
                "bestAskSize": "0.3",
                "ts": 1700000000000
            }
        }"#;
        match parse_kucoin_combined(msg).unwrap() {
            NormalizedEvent::Quote(q) => assert!(matches!(q.exchange, ExchangeName::Kucoin)),
            _ => panic!("Expected quote event"),
        }
    }
}
