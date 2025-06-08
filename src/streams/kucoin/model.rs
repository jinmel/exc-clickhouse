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

impl TryFrom<TradeEvent> for NormalizedTrade {
    type Error = ExchangeStreamError;

    fn try_from(trade_event: TradeEvent) -> Result<NormalizedTrade, Self::Error> {
        let trade = trade_event.data;
        let price = trade
            .price
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid price value: {e}")))?;
        let size = trade
            .size
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid size value: {e}")))?;
        let side = match trade.side.as_str() {
            "buy" => TradeSide::Buy,
            "sell" => TradeSide::Sell,
            other => {
                return Err(ExchangeStreamError::Message(format!(
                    "Unknown side: {other}"
                )));
            }
        };

        let timestamp = trade
            .time
            .parse::<u64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid time value: {e}")))?;

        Ok(NormalizedTrade::new(
            ExchangeName::Kucoin,
            &trade.symbol,
            timestamp / 1000,
            side,
            price,
            size,
        ))
    }
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

impl TryFrom<TickerEvent> for NormalizedQuote {
    type Error = ExchangeStreamError;

    fn try_from(ticker: TickerEvent) -> Result<NormalizedQuote, Self::Error> {
        let ask_price = ticker
            .data
            .best_ask
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid ask price: {e}")))?;
        let ask_amount = ticker
            .data
            .best_ask_size
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid ask size: {e}")))?;
        let bid_price = ticker
            .data
            .best_bid
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid bid price: {e}")))?;
        let bid_amount = ticker
            .data
            .best_bid_size
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid bid size: {e}")))?;

        let symbol = &ticker
            .topic
            .split(':')
            .nth(1)
            .ok_or_else(|| ExchangeStreamError::Message("Invalid topic format".to_string()))?;

        Ok(NormalizedQuote::new(
            ExchangeName::Kucoin,
            symbol,
            ticker.data.time * 1000,
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
        ))
    }
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

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Ack {
    pub id: String,
    #[serde(rename = "type")]
    pub typ: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WelcomeMessage {
    pub id: String,
    #[serde(rename = "type")]
    pub typ: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PongMessage {
    pub id: String,
    #[serde(rename = "type")]
    pub typ: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum KucoinMessage {
    Trade(TradeEvent),
    Ticker(TickerEvent),
    Ack(Ack),
    Welcome(WelcomeMessage),
    Pong(PongMessage),
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

    #[test]
    fn test_ticker_message_parsing() {
        let json = r#"{
            "topic": "/market/ticker:BTC-USDT",
            "type": "message",
            "subject": "trade.ticker",
            "data": {
                "bestAsk": "50001.0",
                "bestAskSize": "1.5",
                "bestBid": "49999.0",
                "bestBidSize": "2.0",
                "time": 1640995200000,
                "price": "50000.0",
                "sequence": "123456789"
            }
        }"#;

        let parsed: KucoinMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            KucoinMessage::Ticker(ticker) => {
                assert_eq!(ticker.topic, "/market/ticker:BTC-USDT");
                assert_eq!(ticker.subject, "trade.ticker");
                assert_eq!(ticker.typ, "message");
                assert_eq!(ticker.data.best_ask, "50001.0");
                assert_eq!(ticker.data.best_bid, "49999.0");
                assert_eq!(ticker.data.time, 1640995200000);
            }
            _ => panic!("Expected Ticker message, got {:?}", parsed),
        }
    }
}
