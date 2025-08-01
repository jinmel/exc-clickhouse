use crate::{
    models::NormalizedEvent,
    streams::{hyperliquid::model::HyperliquidMessage, ExchangeStreamError, Parser},
};

#[derive(Debug, Clone)]
pub struct HyperliquidParser {}

impl Default for HyperliquidParser {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperliquidParser {
    pub fn new() -> Self {
        Self {}
    }
}

impl Parser<Vec<NormalizedEvent>> for HyperliquidParser {
    type Error = ExchangeStreamError;

    fn parse(&self, text: &str) -> Result<Option<Vec<NormalizedEvent>>, Self::Error> {
        tracing::debug!("Hyperliquid raw message: {}", text);
        
        let message: HyperliquidMessage = match serde_json::from_str(text) {
            Ok(msg) => msg,
            Err(e) => {
                tracing::warn!("Failed to parse Hyperliquid message: {e}");
                tracing::warn!("Raw message was: {}", text);
                return Err(ExchangeStreamError::Message(format!("Failed to parse Hyperliquid message: {e}")));
            }
        };

        match message {
            HyperliquidMessage::SubscriptionResponse { data: _response } => {
                // Subscription confirmations don't contain trade data
                tracing::debug!("Received subscription response from Hyperliquid");
                Ok(None)
            }
            HyperliquidMessage::Pong => {
                // Heartbeat pong response - connection is alive
                tracing::debug!("Received pong response from Hyperliquid");
                Ok(None)
            }
            HyperliquidMessage::Trades { data: trades } => {
                tracing::debug!("Received {} trades from Hyperliquid", trades.len());
                let mut events = Vec::new();
                for trade in trades {
                    match trade.try_into() {
                        Ok(normalized_trade) => {
                            events.push(NormalizedEvent::Trade(normalized_trade));
                        }
                        Err(e) => {
                            tracing::warn!("Failed to convert Hyperliquid trade: {e}");
                        }
                    }
                }
                Ok(Some(events))
            }
            HyperliquidMessage::L2Book { data: l2_book } => {
                tracing::debug!("Received L2 book from Hyperliquid for {}", l2_book.coin);
                match l2_book.try_into() {
                    Ok(normalized_quote) => Ok(Some(vec![NormalizedEvent::Quote(normalized_quote)])),
                    Err(e) => {
                        tracing::warn!("Failed to convert Hyperliquid L2 book: {e}");
                        Ok(None)
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_response_parsing() {
        let parser = HyperliquidParser::new();
        let json = r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}}"#;
        let result = parser.parse(json).unwrap();
        assert!(result.is_none()); // Subscription responses don't generate events
    }

    #[test]
    fn test_pong_parsing() {
        let parser = HyperliquidParser::new();
        let json = r#"{"channel":"pong"}"#;
        let result = parser.parse(json).unwrap();
        assert!(result.is_none()); // Pong responses don't generate events
    }

    #[test]
    fn test_trade_message_parsing() {
        let parser = HyperliquidParser::new();
        let json = r#"{"channel":"trades","data":[{"coin":"BTC","side":"A","px":"50000.0","sz":"0.001","time":1234567890,"tid":123,"users":["user1","user2"]}]}"#;
        let result = parser.parse(json).unwrap();
        assert!(result.is_some());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        
        match &events[0] {
            NormalizedEvent::Trade(trade) => {
                assert_eq!(trade.symbol.as_str(), "BTC");
                assert_eq!(trade.price, 50000.0);
            }
            _ => panic!("Expected trade event"),
        }
    }

    #[test]
    fn test_l2_book_parsing() {
        let parser = HyperliquidParser::new();
        let json = r#"{"channel":"l2Book","data":{"coin":"BTC","time":1234567890,"levels":[[["49999.0","0.001"]],[["50001.0","0.002"]]]}}"#;
        let result = parser.parse(json).unwrap();
        assert!(result.is_some());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        
        match &events[0] {
            NormalizedEvent::Quote(quote) => {
                assert_eq!(quote.symbol.as_str(), "BTC");
                assert_eq!(quote.bid_price, 49999.0);
                assert_eq!(quote.ask_price, 50001.0);
            }
            _ => panic!("Expected quote event"),
        }
    }
} 