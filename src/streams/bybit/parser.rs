use crate::models::{NormalizedEvent, NormalizedQuote, NormalizedTrade};
use crate::streams::{
    bybit::model::{TickerEvent, TradeEvent, WsResponse},
    ExchangeStreamError,
};

pub fn parse_bybit_trade(event: &str) -> Result<NormalizedTrade, ExchangeStreamError> {
    let res: WsResponse<TradeEvent> =
        serde_json::from_str(event).map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
    let trade = res
        .data
        .into_iter()
        .next()
        .ok_or_else(|| ExchangeStreamError::ParseError("No trade data".to_string()))?;
    trade.try_into()
}

pub fn parse_bybit_quote(event: &str) -> Result<NormalizedQuote, ExchangeStreamError> {
    let res: WsResponse<TickerEvent> =
        serde_json::from_str(event).map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
    let quote = res
        .data
        .into_iter()
        .next()
        .ok_or_else(|| ExchangeStreamError::ParseError("No ticker data".to_string()))?;
    quote.try_into()
}

pub fn parse_bybit_combined(event: &str) -> Result<NormalizedEvent, ExchangeStreamError> {
    let value: serde_json::Value =
        serde_json::from_str(event).map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
    let topic = value
        .get("topic")
        .and_then(|t| t.as_str())
        .ok_or_else(|| ExchangeStreamError::ParseError("Missing topic".to_string()))?;
    if topic.starts_with("publicTrade.") {
        let parsed: WsResponse<TradeEvent> = serde_json::from_value(value)
            .map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
        let trade = parsed
            .data
            .into_iter()
            .next()
            .ok_or_else(|| ExchangeStreamError::ParseError("No trade data".to_string()))?;
        Ok(NormalizedEvent::Trade(trade.try_into()?))
    } else if topic.starts_with("tickers.") {
        let parsed: WsResponse<TickerEvent> = serde_json::from_value(value)
            .map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
        let quote = parsed
            .data
            .into_iter()
            .next()
            .ok_or_else(|| ExchangeStreamError::ParseError("No ticker data".to_string()))?;
        Ok(NormalizedEvent::Quote(quote.try_into()?))
    } else {
        Err(ExchangeStreamError::ParseError(format!("Unknown topic: {topic}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ExchangeName, TradeSide};

    #[test]
    fn test_parse_bybit_trade() {
        let msg = r#"{
            \"topic\": \"publicTrade.BTCUSDT\",
            \"type\": \"snapshot\",
            \"data\": [{
                \"T\": 1700000000000,
                \"s\": \"BTCUSDT\",
                \"S\": \"Buy\",
                \"p\": \"45000\",
                \"v\": \"0.1\"
            }]
        }"#;
        let trade = parse_bybit_trade(msg).unwrap();
        assert!(matches!(trade.exchange, ExchangeName::Bybit));
        assert_eq!(&trade.symbol[..], "BTCUSDT");
        assert_eq!(trade.timestamp, 1700000000000u64 * 1000);
        assert!(matches!(trade.side, TradeSide::Buy));
        assert_eq!(trade.price, 45000.0);
        assert_eq!(trade.amount, 0.1);
    }

    #[test]
    fn test_parse_bybit_quote() {
        let msg = r#"{
            \"topic\": \"tickers.BTCUSDT\",
            \"type\": \"snapshot\",
            \"data\": [{
                \"s\": \"BTCUSDT\",
                \"bid1Price\": \"44999\",
                \"bid1Size\": \"0.2\",
                \"ask1Price\": \"45001\",
                \"ask1Size\": \"0.3\",
                \"ts\": 1700000000000
            }]
        }"#;
        let quote = parse_bybit_quote(msg).unwrap();
        assert!(matches!(quote.exchange, ExchangeName::Bybit));
        assert_eq!(&quote.symbol[..], "BTCUSDT");
        assert_eq!(quote.timestamp, 1700000000000u64 * 1000);
        assert_eq!(quote.ask_price, 45001.0);
        assert_eq!(quote.ask_amount, 0.3);
        assert_eq!(quote.bid_price, 44999.0);
        assert_eq!(quote.bid_amount, 0.2);
    }

    #[test]
    fn test_parse_bybit_combined_trade() {
        let msg = r#"{
            \"topic\": \"publicTrade.BTCUSDT\",
            \"type\": \"snapshot\",
            \"data\": [{
                \"T\": 1700000000000,
                \"s\": \"BTCUSDT\",
                \"S\": \"Buy\",
                \"p\": \"45000\",
                \"v\": \"0.1\"
            }]
        }"#;
        match parse_bybit_combined(msg).unwrap() {
            NormalizedEvent::Trade(t) => assert!(matches!(t.exchange, ExchangeName::Bybit)),
            _ => panic!("Expected trade event"),
        }
    }

    #[test]
    fn test_parse_bybit_combined_quote() {
        let msg = r#"{
            \"topic\": \"tickers.BTCUSDT\",
            \"type\": \"snapshot\",
            \"data\": [{
                \"s\": \"BTCUSDT\",
                \"bid1Price\": \"44999\",
                \"bid1Size\": \"0.2\",
                \"ask1Price\": \"45001\",
                \"ask1Size\": \"0.3\",
                \"ts\": 1700000000000
            }]
        }"#;
        match parse_bybit_combined(msg).unwrap() {
            NormalizedEvent::Quote(q) => assert!(matches!(q.exchange, ExchangeName::Bybit)),
            _ => panic!("Expected quote event"),
        }
    }
}

