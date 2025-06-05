use crate::{
    models::{NormalizedEvent, NormalizedQuote, NormalizedTrade},
    streams::{
        ExchangeStreamError,
        okx::model::{TickerEvent, TradeEvent, WsResponse},
    },
};

pub fn parse_okx_trade(event: &str) -> Result<NormalizedTrade, ExchangeStreamError> {
    let res: WsResponse<TradeEvent> =
        serde_json::from_str(event).map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
    let trade = res
        .data
        .into_iter()
        .next()
        .ok_or_else(|| ExchangeStreamError::ParseError("No trade data".to_string()))?;
    trade.try_into()
}

pub fn parse_okx_quote(event: &str) -> Result<NormalizedQuote, ExchangeStreamError> {
    let res: WsResponse<TickerEvent> =
        serde_json::from_str(event).map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
    let quote = res
        .data
        .into_iter()
        .next()
        .ok_or_else(|| ExchangeStreamError::ParseError("No ticker data".to_string()))?;
    quote.try_into()
}

pub fn parse_okx_combined(event: &str) -> Result<NormalizedEvent, ExchangeStreamError> {
    let value: serde_json::Value =
        serde_json::from_str(event).map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
    let channel = value
        .get("arg")
        .and_then(|a| a.get("channel"))
        .and_then(|c| c.as_str())
        .ok_or_else(|| ExchangeStreamError::ParseError("Missing channel".to_string()))?;

    match channel {
        "trades" => {
            let parsed: WsResponse<TradeEvent> = serde_json::from_value(value)
                .map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
            let trade = parsed
                .data
                .into_iter()
                .next()
                .ok_or_else(|| ExchangeStreamError::ParseError("No trade data".to_string()))?;
            Ok(NormalizedEvent::Trade(trade.try_into()?))
        }
        "tickers" => {
            let parsed: WsResponse<TickerEvent> = serde_json::from_value(value)
                .map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
            let quote = parsed
                .data
                .into_iter()
                .next()
                .ok_or_else(|| ExchangeStreamError::ParseError("No ticker data".to_string()))?;
            Ok(NormalizedEvent::Quote(quote.try_into()?))
        }
        other => Err(ExchangeStreamError::ParseError(format!(
            "Unknown channel: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ExchangeName, TradeSide};

    #[test]
    fn test_parse_okx_trade() {
        let msg = r#"{
            "arg": {"channel": "trades", "instId": "BTC-USDT"},
            "data": [{
                "instId": "BTC-USDT",
                "tradeId": "1",
                "px": "45000.1",
                "sz": "0.2",
                "side": "buy",
                "ts": "1700000000000"
            }]
        }"#;

        let trade = parse_okx_trade(msg).unwrap();
        assert!(matches!(trade.exchange, ExchangeName::Okx));
        assert_eq!(&trade.symbol[..], "BTC-USDT");
        assert_eq!(trade.timestamp, 1700000000000u64 * 1000);
        assert!(matches!(trade.side, TradeSide::Buy));
        assert_eq!(trade.price, 45000.1);
        assert_eq!(trade.amount, 0.2);
    }

    #[test]
    fn test_parse_okx_quote() {
        let msg = r#"{
            "arg": {"channel": "tickers", "instId": "BTC-USDT"},
            "data": [{
                "instId": "BTC-USDT",
                "askPx": "45010.2",
                "askSz": "0.3",
                "bidPx": "44999.8",
                "bidSz": "0.4",
                "ts": "1700000000000"
            }]
        }"#;

        let quote = parse_okx_quote(msg).unwrap();
        assert!(matches!(quote.exchange, ExchangeName::Okx));
        assert_eq!(&quote.symbol[..], "BTC-USDT");
        assert_eq!(quote.timestamp, 1700000000000u64 * 1000);
        assert_eq!(quote.ask_price, 45010.2);
        assert_eq!(quote.ask_amount, 0.3);
        assert_eq!(quote.bid_price, 44999.8);
        assert_eq!(quote.bid_amount, 0.4);
    }

    #[test]
    fn test_parse_okx_combined_trade() {
        let msg = r#"{
            "arg": {"channel": "trades", "instId": "BTC-USDT"},
            "data": [{
                "instId": "BTC-USDT",
                "tradeId": "1",
                "px": "45000.1",
                "sz": "0.2",
                "side": "buy",
                "ts": "1700000000000"
            }]
        }"#;

        match parse_okx_combined(msg).unwrap() {
            NormalizedEvent::Trade(t) => {
                assert!(matches!(t.exchange, ExchangeName::Okx));
            }
            _ => panic!("Expected trade event"),
        }
    }

    #[test]
    fn test_parse_okx_combined_quote() {
        let msg = r#"{
            "arg": {"channel": "tickers", "instId": "BTC-USDT"},
            "data": [{
                "instId": "BTC-USDT",
                "askPx": "45010.2",
                "askSz": "0.3",
                "bidPx": "44999.8",
                "bidSz": "0.4",
                "ts": "1700000000000"
            }]
        }"#;

        match parse_okx_combined(msg).unwrap() {
            NormalizedEvent::Quote(q) => {
                assert!(matches!(q.exchange, ExchangeName::Okx));
            }
            _ => panic!("Expected quote event"),
        }
    }
}
