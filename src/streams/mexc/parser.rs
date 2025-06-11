use crate::{
    models::NormalizedEvent,
    streams::{ExchangeStreamError, Parser},
};

use super::model::{BookTickerEvent, MexcMessage, TradeEvent};

#[derive(Debug, Clone)]
pub struct MexcParser;

impl MexcParser {
    pub fn new() -> Self {
        Self
    }

    /// Extract symbol from MEXC channel name
    /// Examples:
    /// - "spot@public.deals.v3.api@BTCUSDT" -> "BTCUSDT"
    /// - "spot@public.bookTicker.v3.api@ETHUSDT" -> "ETHUSDT"
    fn extract_symbol_from_channel(channel: &str) -> Option<&str> {
        channel.split('@').last()
    }

    fn parse_trade_event(
        &self,
        trade_data: &serde_json::Value,
        symbol: &str,
    ) -> Result<Option<Vec<NormalizedEvent>>, ExchangeStreamError> {
        // MEXC trade data comes in a "deals" array
        if let Some(deals) = trade_data.get("deals") {
            if let Some(deals_array) = deals.as_array() {
                if let Some(deal) = deals_array.first() {
                    let trade: TradeEvent = serde_json::from_value(deal.clone())
                        .map_err(|e| ExchangeStreamError::Message(format!("Failed to parse trade: {e}")))?;
                    
                    let normalized_trade = trade.to_normalized_trade(symbol)?;
                    
                    return Ok(Some(vec![NormalizedEvent::Trade(normalized_trade)]));
                }
            }
        }
        Ok(None)
    }

    fn parse_book_ticker_event(
        &self,
        ticker_data: &serde_json::Value,
        symbol: &str,
    ) -> Result<Option<Vec<NormalizedEvent>>, ExchangeStreamError> {
        let ticker: BookTickerEvent = serde_json::from_value(ticker_data.clone())
            .map_err(|e| ExchangeStreamError::Message(format!("Failed to parse book ticker: {e}")))?;
        
        let normalized_quote = ticker.to_normalized_quote(symbol)?;
        
        Ok(Some(vec![NormalizedEvent::Quote(normalized_quote)]))
    }
}

impl Parser<Vec<NormalizedEvent>> for MexcParser {
    type Error = ExchangeStreamError;

    fn parse(&self, text: &str) -> Result<Option<Vec<NormalizedEvent>>, Self::Error> {
        let message: MexcMessage = serde_json::from_str(text)
            .map_err(|e| ExchangeStreamError::Message(format!("Failed to parse JSON: {e}")))?;

        match message {
            MexcMessage::ChannelData(channel_msg) => {
                // Extract symbol from channel name
                let symbol = Self::extract_symbol_from_channel(&channel_msg.channel)
                    .ok_or_else(|| {
                        ExchangeStreamError::Message(format!(
                            "Could not extract symbol from channel: {}",
                            channel_msg.channel
                        ))
                    })?;

                // Determine message type based on channel
                if channel_msg.channel.contains("deals") {
                    self.parse_trade_event(&channel_msg.data, symbol)
                } else if channel_msg.channel.contains("bookTicker") {
                    self.parse_book_ticker_event(&channel_msg.data, symbol)
                } else {
                    tracing::debug!("Ignoring unknown channel: {}", channel_msg.channel);
                    Ok(None)
                }
            }
            MexcMessage::Subscription(sub) => {
                if sub.msg == "PONG" {
                    tracing::debug!("Received pong from MEXC");
                } else {
                    tracing::debug!("Subscription result: {:?}", sub);
                }
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_symbol_from_channel() {
        assert_eq!(
            MexcParser::extract_symbol_from_channel("spot@public.deals.v3.api@BTCUSDT"),
            Some("BTCUSDT")
        );
        assert_eq!(
            MexcParser::extract_symbol_from_channel("spot@public.bookTicker.v3.api@ETHUSDT"),
            Some("ETHUSDT")
        );
    }

    #[test]
    fn test_parse_trade_message() {
        let parser = MexcParser::new();
        let trade_json = r#"{
            "c": "spot@public.deals.v3.api@BTCUSDT",
            "d": {
                "deals": [{
                    "p": "50000.00",
                    "v": "0.001",
                    "t": 1672531200000,
                    "S": 1
                }]
            }
        }"#;

        let result = parser.parse(trade_json).unwrap();
        assert!(result.is_some());
        
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        
        match &events[0] {
            NormalizedEvent::Trade(trade) => {
                assert_eq!(trade.symbol.as_str(), "BTCUSDT");
                assert_eq!(trade.price, 50000.0);
                assert_eq!(trade.amount, 0.001);
            }
            _ => panic!("Expected trade event"),
        }
    }

    #[test]
    fn test_parse_book_ticker_message() {
        let parser = MexcParser::new();
        let ticker_json = r#"{
            "c": "spot@public.bookTicker.v3.api@BTCUSDT",
            "d": {
                "bidPrice": "49999.99",
                "bidQty": "1.00000000",
                "askPrice": "50000.01",
                "askQty": "0.50000000"
            }
        }"#;

        let result = parser.parse(ticker_json).unwrap();
        assert!(result.is_some());
        
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        
        match &events[0] {
            NormalizedEvent::Quote(quote) => {
                assert_eq!(quote.symbol.as_str(), "BTCUSDT");
                assert_eq!(quote.bid_price, 49999.99);
                assert_eq!(quote.ask_price, 50000.01);
            }
            _ => panic!("Expected quote event"),
        }
    }

    #[test]
    fn test_parse_subscription_result() {
        let parser = MexcParser::new();
        let sub_json = r#"{
            "id": 0,
            "code": 0,
            "msg": "spot@public.deals.v3.api@BTCUSDT"
        }"#;

        let result = parser.parse(sub_json).unwrap();
        assert!(result.is_none()); // Subscription results should be ignored
    }

    #[test]
    fn test_parse_pong() {
        let parser = MexcParser::new();
        let pong_json = r#"{
            "id": 0,
            "code": 0,
            "msg": "PONG"
        }"#;

        let result = parser.parse(pong_json).unwrap();
        assert!(result.is_none()); // Pong messages should be ignored
    }
}