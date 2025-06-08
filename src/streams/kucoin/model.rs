use serde::{Deserialize, Serialize};

use crate::{
    models::{ExchangeName, NormalizedQuote, NormalizedTrade, TradeSide},
    streams::ExchangeStreamError,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TradeData {
    pub maker_order_id: String,
    pub taker_order_id: String,
    pub sequence: String,
    pub price: String,
    pub size: String,
    pub side: String,
    pub symbol: String,
    pub time: String,
    pub trade_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TradeEvent {
    pub topic: String,
    pub subject: String,
    pub data: TradeData,
    #[serde(rename = "type")]
    pub typ: String,
}

impl TryInto<NormalizedTrade> for TradeEvent {
    type Error = ExchangeStreamError;

    fn try_into(self) -> Result<NormalizedTrade, Self::Error> {
        let price =
            self.data.price.parse::<f64>().map_err(|e| {
                ExchangeStreamError::Message(format!("Invalid price value: {e}"))
            })?;
        let size =
            self.data.size.parse::<f64>().map_err(|e| {
                ExchangeStreamError::Message(format!("Invalid size value: {e}"))
            })?;
        let side = match self.data.side.as_str() {
            "buy" => TradeSide::Buy,
            "sell" => TradeSide::Sell,
            other => {
                return Err(ExchangeStreamError::Message(format!(
                    "Unknown side: {other}"
                )));
            }
        };

        let timestamp =
            self.data.time.parse::<u64>().map_err(|e| {
                ExchangeStreamError::Message(format!("Invalid time value: {e}"))
            })?;

        Ok(NormalizedTrade::new(
            ExchangeName::Kucoin,
            &self.data.symbol,
            timestamp / 1000,
            side,
            price,
            size,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TickerData {
    pub best_ask: String,
    pub best_ask_size: String,
    pub best_bid: String,
    pub best_bid_size: String,
    pub time: u64,
    pub price: String,
    pub sequence: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TickerEvent {
    pub topic: String,
    pub subject: String,
    pub data: TickerData,
    #[serde(rename = "type")]
    pub typ: String,
}

impl TryInto<NormalizedQuote> for TickerEvent {
    type Error = ExchangeStreamError;

    fn try_into(self) -> Result<NormalizedQuote, Self::Error> {
        let ask_price =
            self.data.best_ask.parse::<f64>().map_err(|e| {
                ExchangeStreamError::Message(format!("Invalid ask price: {e}"))
            })?;
        let ask_amount = self
            .data
            .best_ask_size
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid ask size: {e}")))?;
        let bid_price =
            self.data.best_bid.parse::<f64>().map_err(|e| {
                ExchangeStreamError::Message(format!("Invalid bid price: {e}"))
            })?;
        let bid_amount = self
            .data
            .best_bid_size
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid bid size: {e}")))?;

        let symbol =
            &self.topic.split(':').nth(1).ok_or_else(|| {
                ExchangeStreamError::Message("Invalid topic format".to_string())
            })?;

        Ok(NormalizedQuote::new(
            ExchangeName::Kucoin,
            symbol,
            self.data.time * 1000,
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Ack {
    pub id: String,
    #[serde(rename = "type")]
    pub typ: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase", untagged)]
pub enum KucoinMessage {
    Trade(TradeEvent),
    Ticker(TickerEvent),
    Ack(Ack),
    Other {
        #[serde(rename = "type")]
        typ: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kucoin_trade_message_parsing() {
        let json = r#"{
            "topic": "/market/match:UNI-USDT",
            "type": "message",
            "subject": "trade.l3match",
            "data": {
                "makerOrderId": "6843fb19a0c85100075a7978",
                "price": "6.1223",
                "sequence": "15489153933131777",
                "side": "sell",
                "size": "81.6209",
                "symbol": "UNI-USDT",
                "takerOrderId": "6843fb1d02a36d0007d32121",
                "time": "1749285661147000000",
                "tradeId": "15489153933131777",
                "type": "match"
            }
        }"#;

        let parsed: KucoinMessage = serde_json::from_str(json).expect("Failed to parse JSON");

        match parsed {
            KucoinMessage::Trade(event) => {
                assert_eq!(event.topic, "/market/match:UNI-USDT");
                assert_eq!(event.subject, "trade.l3match");
                assert_eq!(event.typ, "message");
                assert_eq!(event.data.symbol, "UNI-USDT");
                assert_eq!(event.data.price, "6.1223");
                assert_eq!(event.data.side, "sell");
                assert_eq!(event.data.size, "81.6209");
                assert_eq!(event.data.maker_order_id, "6843fb19a0c85100075a7978");
                assert_eq!(event.data.taker_order_id, "6843fb1d02a36d0007d32121");
                assert_eq!(event.data.time, "1749285661147000000");
                assert_eq!(event.data.trade_id, "15489153933131777");
                assert_eq!(event.data.sequence, "15489153933131777");
            }
            _ => panic!("Expected KucoinMessage::Trade variant, got {:?}", parsed),
        }
    }
}
