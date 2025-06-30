use serde::{Deserialize, Serialize};

use crate::{
    models::{ExchangeName, NormalizedQuote, NormalizedTrade, TradeSide},
    streams::ExchangeStreamError,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HyperliquidTrade {
    pub coin: String,
    pub side: String,
    pub px: String,    // price
    pub sz: String,    // size
    pub time: u64,     // timestamp
    pub tid: u64,      // trade id
}

impl TryFrom<HyperliquidTrade> for NormalizedTrade {
    type Error = ExchangeStreamError;

    fn try_from(trade: HyperliquidTrade) -> Result<NormalizedTrade, Self::Error> {
        let price = trade
            .px
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid price value: {e}")))?;

        let amount = trade
            .sz
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid size value: {e}")))?;

        let side = match trade.side.as_str() {
            "A" => TradeSide::Sell, // Maker was on Ask side
            "B" => TradeSide::Buy,  // Maker was on Bid side
            _ => return Err(ExchangeStreamError::Message(format!("Invalid side: {}", trade.side))),
        };

        Ok(NormalizedTrade::new(
            ExchangeName::Hyperliquid,
            &trade.coin,
            trade.time * 1000, // Convert milliseconds to microseconds
            side,
            price,
            amount,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct HyperliquidL2Book {
    pub coin: String,
    pub levels: Vec<Vec<Vec<String>>>, // [bids, asks] where each level is [price, size]
    pub time: u64,
}

impl TryFrom<HyperliquidL2Book> for NormalizedQuote {
    type Error = ExchangeStreamError;

    fn try_from(book: HyperliquidL2Book) -> Result<NormalizedQuote, Self::Error> {
        if book.levels.len() != 2 {
            return Err(ExchangeStreamError::Message(
                "L2 book must have exactly 2 levels (bids and asks)".to_string(),
            ));
        }

        let bids = &book.levels[0];
        let asks = &book.levels[1];

        if bids.is_empty() || asks.is_empty() {
            return Err(ExchangeStreamError::Message(
                "L2 book must have at least one bid and one ask".to_string(),
            ));
        }

        let best_bid = &bids[0];
        let best_ask = &asks[0];

        let bid_price = best_bid[0].parse::<f64>().map_err(|e| ExchangeStreamError::Message(format!("Invalid bid price: {e}")))?;
        let bid_amount = best_bid[1].parse::<f64>().map_err(|e| ExchangeStreamError::Message(format!("Invalid bid size: {e}")))?;
        let ask_price = best_ask[0].parse::<f64>().map_err(|e| ExchangeStreamError::Message(format!("Invalid ask price: {e}")))?;
        let ask_amount = best_ask[1].parse::<f64>().map_err(|e| ExchangeStreamError::Message(format!("Invalid ask size: {e}")))?;

        Ok(NormalizedQuote::new(
            ExchangeName::Hyperliquid,
            &book.coin,
            book.time * 1000, // Convert milliseconds to microseconds
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct HyperliquidSubscriptionResponse {
    pub method: String,
    pub subscription: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "channel")]
pub enum HyperliquidMessage {
    #[serde(rename = "subscriptionResponse")]
    SubscriptionResponse { data: HyperliquidSubscriptionResponse },
    #[serde(rename = "trades")]
    Trades { data: Vec<HyperliquidTrade> },
    #[serde(rename = "l2Book")]
    L2Book { data: HyperliquidL2Book },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trade_parsing() {
        let json = r#"{
            "coin": "BTC",
            "side": "B",
            "px": "50000.0",
            "sz": "0.1",
            "time": 1672531200000,
            "tid": 12345
        }"#;

        let trade: HyperliquidTrade = serde_json::from_str(json).expect("Failed to parse trade");
        assert_eq!(trade.coin, "BTC");
        assert_eq!(trade.side, "B");
        assert_eq!(trade.px, "50000.0");
        assert_eq!(trade.sz, "0.1");

        let normalized: NormalizedTrade = trade.try_into().expect("Failed to convert to normalized trade");
        assert_eq!(normalized.side, TradeSide::Buy);
    }

    #[test]
    fn test_l2_book_parsing() {
        let json = r#"{
            "coin": "BTC",
            "levels": [
                [["49999.0", "1.0"]],
                [["50001.0", "0.5"]]
            ],
            "time": 1672531200000
        }"#;

        let book: HyperliquidL2Book = serde_json::from_str(json).expect("Failed to parse L2 book");
        assert_eq!(book.coin, "BTC");
        assert_eq!(book.levels.len(), 2);

        let normalized: NormalizedQuote = book.try_into().expect("Failed to convert to normalized quote");
        assert_eq!(normalized.bid_price, 49999.0);
        assert_eq!(normalized.ask_price, 50001.0);
    }
} 