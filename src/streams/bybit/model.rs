use serde::{Deserialize, Serialize};

use crate::{
    models::{ExchangeName, NormalizedQuote, NormalizedTrade, TradeSide},
    streams::ExchangeStreamError,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TradeData {
    #[serde(rename = "T")]
    pub trade_timestamp: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "S")]
    pub side: String,
    #[serde(rename = "v")]
    pub size: String,
    #[serde(rename = "p")]
    pub price: String,
}

impl TryFrom<TradeData> for NormalizedTrade {
    type Error = ExchangeStreamError;

    fn try_from(trade: TradeData) -> Result<NormalizedTrade, Self::Error> {
        let price = trade
            .price
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid price value: {e}")))?;
        let amount = trade
            .size
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid quantity value: {e}")))?;
        let side = match trade.side.as_str() {
            "Buy" => TradeSide::Buy,
            "Sell" => TradeSide::Sell,
            _ => {
                return Err(ExchangeStreamError::Message(format!(
                    "Unknown trade side: {}",
                    trade.side
                )));
            }
        };
        Ok(NormalizedTrade::new(
            ExchangeName::Bybit,
            &trade.symbol,
            trade.trade_timestamp * 1000,
            side,
            price,
            amount,
        ))
    }
}

impl TryFrom<TradeMessage> for Vec<NormalizedTrade> {
    type Error = ExchangeStreamError;

    fn try_from(trade_msg: TradeMessage) -> Result<Vec<NormalizedTrade>, Self::Error> {
        trade_msg
            .data
            .into_iter()
            .map(|trade| trade.try_into())
            .collect()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TradeMessage {
    pub topic: String,
    #[serde(rename = "type")]
    pub typ: String,
    pub ts: u64,
    pub data: Vec<TradeData>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OrderbookData {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(default, rename = "b")]
    pub bids: Vec<[String; 2]>,
    #[serde(default, rename = "a")]
    pub asks: Vec<[String; 2]>,
    pub u: u64,
    pub seq: u64,
}

impl TryFrom<OrderbookData> for NormalizedQuote {
    type Error = ExchangeStreamError;

    fn try_from(orderbook: OrderbookData) -> Result<NormalizedQuote, Self::Error> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| ExchangeStreamError::Message(format!("Failed to get timestamp: {e}")))?
            .as_micros() as u64;

        // Get best bid and ask from the orderbook
        let best_bid = orderbook
            .bids
            .first()
            .ok_or_else(|| ExchangeStreamError::Message("No bids available".to_string()))?;
        let best_ask = orderbook
            .asks
            .first()
            .ok_or_else(|| ExchangeStreamError::Message("No asks available".to_string()))?;

        let bid_price = best_bid[0]
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid bid price: {e}")))?;
        let bid_amount = best_bid[1]
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid bid amount: {e}")))?;
        let ask_price = best_ask[0]
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid ask price: {e}")))?;
        let ask_amount = best_ask[1]
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid ask amount: {e}")))?;

        Ok(NormalizedQuote::new(
            ExchangeName::Bybit,
            &orderbook.symbol,
            timestamp,
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OrderbookMessage {
    pub topic: String,
    #[serde(rename = "type")]
    pub typ: String,
    pub ts: u64,
    #[serde(default)]
    // Timestamp from the match engine when the orderbook was generated
    // Used to ensure orderbook updates are processed in order
    pub cts: Option<u64>,
    pub data: OrderbookData,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SubscriptionResult {
    pub success: bool,
    pub ret_msg: String,
    pub conn_id: String,
    pub req_id: Option<String>,
    pub op: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum BybitMessage {
    Trade(TradeMessage),
    Orderbook(OrderbookMessage),
    Subscription(SubscriptionResult),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trade_message_parsing() {
        let json = r#"{
            "topic": "publicTrade.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304486868,
            "data": [{
                "T": 1672304486865,
                "s": "BTCUSDT",
                "S": "Buy",
                "v": "0.001",
                "p": "16578.50"
            }]
        }"#;

        let parsed: BybitMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            BybitMessage::Trade(trade_msg) => {
                assert_eq!(trade_msg.topic, "publicTrade.BTCUSDT");
                assert_eq!(trade_msg.typ, "snapshot");
                assert_eq!(trade_msg.data.len(), 1);
                let trade_data = &trade_msg.data[0];
                assert_eq!(trade_data.symbol, "BTCUSDT");
                assert_eq!(trade_data.side, "Buy");
                assert_eq!(trade_data.price, "16578.50");
            }
            _ => panic!("Expected Trade message, got {:?}", parsed),
        }
    }

    #[test]
    fn test_orderbook_message_parsing() {
        let json = r#"{
            "topic": "orderbook.1.BTCUSDT",
            "type": "snapshot",
            "ts": 1672304486868,
            "data": {
                "s": "BTCUSDT",
                "b": [["16578.50", "1.431"], ["16578.49", "0.001"]],
                "a": [["16578.51", "1.431"], ["16578.52", "0.001"]],
                "u": 18521288,
                "seq": 7961638724
            }
        }"#;

        let parsed: BybitMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            BybitMessage::Orderbook(orderbook_msg) => {
                assert_eq!(orderbook_msg.topic, "orderbook.1.BTCUSDT");
                assert_eq!(orderbook_msg.typ, "snapshot");
                assert_eq!(orderbook_msg.data.symbol, "BTCUSDT");
                assert_eq!(orderbook_msg.data.bids.len(), 2);
                assert_eq!(orderbook_msg.data.asks.len(), 2);
                assert_eq!(orderbook_msg.data.bids[0][0], "16578.50");
                assert_eq!(orderbook_msg.data.asks[0][0], "16578.51");
            }
            _ => panic!("Expected Orderbook message, got {:?}", parsed),
        }
    }
}
