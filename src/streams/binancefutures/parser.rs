use crate::{
    models::NormalizedEvent,
    streams::{ExchangeStreamError, Parser, binancefutures::model::BinanceFuturesMessage},
};

#[derive(Debug, Clone)]
pub struct BinanceFuturesParser {}

impl Default for BinanceFuturesParser {
    fn default() -> Self {
        Self::new()
    }
}

impl BinanceFuturesParser {
    pub fn new() -> Self {
        Self {}
    }
}

impl Parser<Vec<NormalizedEvent>> for BinanceFuturesParser {
    type Error = ExchangeStreamError;

    fn parse(&self, text: &str) -> Result<Option<Vec<NormalizedEvent>>, Self::Error> {
        let value: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| ExchangeStreamError::Message(format!("Failed to parse JSON: {e}")))?;

        let value = value.get("data").unwrap_or(&value);

        let event = serde_json::from_value::<BinanceFuturesMessage>(value.to_owned()).map_err(|e| {
            ExchangeStreamError::Message(format!("Failed to parse BinanceFuturesMessage: {e}"))
        })?;

        let normalized = match event {
            BinanceFuturesMessage::Trade(trade) => {
                let normalized_trade = trade.try_into()
                    .map_err(|e| ExchangeStreamError::Message(format!("Failed to convert trade: {e}")))?;
                Some(vec![NormalizedEvent::Trade(normalized_trade)])
            },
            BinanceFuturesMessage::Quote(quote) => {
                let normalized_quote = quote.try_into()
                    .map_err(|e| ExchangeStreamError::Message(format!("Failed to convert quote: {e}")))?;
                Some(vec![NormalizedEvent::Quote(normalized_quote)])
            },
            BinanceFuturesMessage::Subscription(result) => {
                if result.result.is_some() {
                    return Err(ExchangeStreamError::Subscription(format!(
                        "Subscription result: {result:?}"
                    )));
                }
                None
            }
            BinanceFuturesMessage::Error(error) => {
                return Err(ExchangeStreamError::Message(format!(
                    "Binance Futures error: code={}, msg={}",
                    error.error.code, error.error.msg
                )));
            }
        };
        Ok(normalized)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{NormalizedEvent, ExchangeName};

    #[test]
    fn test_parser_trade_event() {
        let parser = BinanceFuturesParser::new();
        let json = r#"{
            "e": "aggTrade",
            "E": 123456789,
            "s": "BTCUSDT",
            "a": 5933014,
            "p": "50000.00",
            "q": "0.001",
            "f": 100,
            "l": 105,
            "T": 123456785,
            "m": false
        }"#;

        let result = parser.parse(json).expect("Failed to parse trade event");
        assert!(result.is_some());
        
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        
        match &events[0] {
            NormalizedEvent::Trade(trade) => {
                assert_eq!(trade.exchange, ExchangeName::Binance);
                assert_eq!(trade.symbol.as_str(), "BTCUSDT");
                assert_eq!(trade.price, 50000.00);
                assert_eq!(trade.amount, 0.001);
            }
            _ => panic!("Expected Trade event"),
        }
    }

    #[test]
    fn test_parser_book_ticker_event() {
        let parser = BinanceFuturesParser::new();
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

        let result = parser.parse(json).expect("Failed to parse book ticker event");
        assert!(result.is_some());
        
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        
        match &events[0] {
            NormalizedEvent::Quote(quote) => {
                assert_eq!(quote.exchange, ExchangeName::Binance);
                assert_eq!(quote.symbol.as_str(), "BNBUSDT");
                assert_eq!(quote.ask_price, 25.36520000);
                assert_eq!(quote.bid_price, 25.35190000);
            }
            _ => panic!("Expected Quote event"),
        }
    }

    #[test]
    fn test_parser_subscription_result() {
        let parser = BinanceFuturesParser::new();
        let json = r#"{
            "result": null,
            "id": "1"
        }"#;

        let result = parser.parse(json).expect("Failed to parse subscription result");
        assert!(result.is_none());
    }

    #[test]
    fn test_parser_error_message() {
        let parser = BinanceFuturesParser::new();
        let json = r#"{
            "id": "1",
            "error": {
                "code": -1121,
                "msg": "Invalid symbol."
            }
        }"#;

        let result = parser.parse(json);
        assert!(result.is_err());
        
        let error = result.unwrap_err();
        assert!(matches!(error, ExchangeStreamError::Message(_)));
    }
}
