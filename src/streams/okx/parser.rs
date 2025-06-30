use crate::{
    models::NormalizedEvent,
    streams::{
        ExchangeStreamError, Parser,
        okx::model::{OkxDataMessage, OkxEventMessage, OkxMessage},
    },
};

#[derive(Debug, Clone)]
pub struct OkxParser {}

impl Default for OkxParser {
    fn default() -> Self {
        Self::new()
    }
}

impl OkxParser {
    pub fn new() -> Self {
        Self {}
    }
}

impl Parser<Vec<NormalizedEvent>> for OkxParser {
    type Error = ExchangeStreamError;

    fn parse(&self, text: &str) -> Result<Option<Vec<NormalizedEvent>>, Self::Error> {
        if text.trim() == "pong" {
            // handle pong before passing to json deserializer
            return Ok(None);
        }

        let message: OkxMessage = serde_json::from_str(text)
            .map_err(|e| ExchangeStreamError::Message(format!("Failed to parse JSON: {e}")))?;

        match message {
            OkxMessage::Data(data_msg) => match data_msg {
                OkxDataMessage::Trade(trade_msg) => {
                    let normalized_trades = trade_msg
                        .data
                        .into_iter()
                        .map(|trade| {
                            let normalized_trade = trade.try_into()?;
                            Ok(NormalizedEvent::Trade(normalized_trade))
                        })
                        .collect::<Result<Vec<NormalizedEvent>, Self::Error>>()?;
                    Ok(Some(normalized_trades))
                }
                OkxDataMessage::Ticker(ticker_msg) => {
                    let normalized_quotes = ticker_msg
                        .data
                        .into_iter()
                        .map(|ticker| {
                            let normalized_quote = ticker.try_into()?;
                            Ok(NormalizedEvent::Quote(normalized_quote))
                        })
                        .collect::<Result<Vec<NormalizedEvent>, Self::Error>>()?;
                    Ok(Some(normalized_quotes))
                }
            },
            OkxMessage::Event(event) => match event {
                OkxEventMessage::Login { code, msg, conn_id } => {
                    tracing::debug!(?code, ?msg, ?conn_id, "received login");
                    Ok(None)
                }
                OkxEventMessage::Subscribe { arg, conn_id } => {
                    tracing::debug!(?arg, ?conn_id, "received subscribe");
                    Ok(None)
                }
                OkxEventMessage::Error { code, msg, conn_id } => {
                    tracing::debug!(?code, ?msg, ?conn_id, "received error");
                    Ok(None)
                }
                OkxEventMessage::Notice { msg, code, conn_id } => {
                    tracing::debug!(?msg, ?code, ?conn_id, "received notice");
                    Ok(None)
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_trade_message() {
        let parser = OkxParser::new();
        let json = r#"{
            "arg": {
                "channel": "trades",
                "instId": "BTC-USDT"
            },
            "data": [{
                "instId": "BTC-USDT",
                "tradeId": "123456789",
                "px": "50000.0",
                "sz": "0.001",
                "side": "buy",
                "ts": "1640995200000"
            }]
        }"#;

        let result = parser.parse(json).expect("Failed to parse");
        assert!(result.is_some());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);

        match &events[0] {
            NormalizedEvent::Trade(trade) => {
                assert_eq!(trade.symbol.as_str(), "BTC-USDT");
                assert_eq!(trade.price, 50000.0);
                assert_eq!(trade.amount, 0.001);
            }
            _ => panic!("Expected Trade event"),
        }
    }

    #[test]
    fn test_parse_ticker_message() {
        let parser = OkxParser::new();
        let json = r#"{
            "arg": {
                "channel": "tickers",
                "instId": "BTC-USDT"
            },
            "data": [{
                "instId": "BTC-USDT",
                "last": "50000.0",
                "lastSz": "0.001",
                "askPx": "50001.0",
                "askSz": "1.0",
                "bidPx": "49999.0",
                "bidSz": "1.0",
                "ts": "1640995200000"
            }]
        }"#;

        let result = parser.parse(json).expect("Failed to parse");
        assert!(result.is_some());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);

        match &events[0] {
            NormalizedEvent::Quote(quote) => {
                assert_eq!(quote.symbol.as_str(), "BTC-USDT");
                assert_eq!(quote.ask_price, 50001.0);
                assert_eq!(quote.bid_price, 49999.0);
            }
            _ => panic!("Expected Quote event"),
        }
    }

    #[test]
    fn test_parse_login_message() {
        let parser = OkxParser::new();
        let json = r#"{
            "event": "login",
            "code": "0",
            "msg": "",
            "connId": "a4d3ae55"
        }"#;

        let result = parser.parse(json).expect("Failed to parse");
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_subscribe_message() {
        let parser = OkxParser::new();
        let json = r#"{
            "event": "subscribe",
            "arg": {
                "channel": "tickers",
                "instId": "PENDLE-USDT"
            },
            "connId": "78da8d48"
        }"#;

        let result = parser.parse(json).expect("Failed to parse");
        assert!(result.is_none());
    }

    #[test]
    fn test_pong_message() {
        let parser = OkxParser::new();
        let json = "pong";

        let result = parser.parse(json).expect("Failed to parse");
        assert!(result.is_none());
    }
}
