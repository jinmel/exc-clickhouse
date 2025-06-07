use std::convert::TryFrom;

use serde::{Deserialize, Serialize};

use crate::{
    models::{ExchangeName, NormalizedQuote, NormalizedTrade, TradeSide},
    streams::ExchangeStreamError,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TickerData {
    pub a: [String; 3],
    pub b: [String; 3],
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TickerMessage {
    pub pair: String,
    pub data: TickerData,
}

impl TryFrom<TickerMessage> for NormalizedQuote {
    type Error = ExchangeStreamError;

    fn try_from(value: TickerMessage) -> Result<Self, Self::Error> {
        let ask_price = value.data.a[0]
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid ask price: {e}")))?;
        let ask_amount = value.data.a[2]
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid ask size: {e}")))?;
        let bid_price = value.data.b[0]
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid bid price: {e}")))?;
        let bid_amount = value.data.b[2]
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::MessageError(format!("Invalid bid size: {e}")))?;

        Ok(NormalizedQuote::new(
            ExchangeName::Kraken,
            &value.pair,
            chrono::Utc::now().timestamp_millis() as u64,
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
        ))
    }
}

pub fn trade_item_to_normalized(
    item: &[serde_json::Value],
    pair: &str,
) -> Result<NormalizedTrade, ExchangeStreamError> {
    if item.len() < 6 {
        return Err(ExchangeStreamError::MessageError(
            "invalid trade item".to_string(),
        ));
    }
    let price = item[0]
        .as_str()
        .ok_or_else(|| ExchangeStreamError::MessageError("missing price".into()))?
        .parse::<f64>()
        .map_err(|e| ExchangeStreamError::MessageError(format!("invalid price: {e}")))?;
    let amount = item[1]
        .as_str()
        .ok_or_else(|| ExchangeStreamError::MessageError("missing size".into()))?
        .parse::<f64>()
        .map_err(|e| ExchangeStreamError::MessageError(format!("invalid size: {e}")))?;
    let time = item[2]
        .as_str()
        .ok_or_else(|| ExchangeStreamError::MessageError("missing time".into()))?
        .parse::<f64>()
        .map_err(|e| ExchangeStreamError::MessageError(format!("invalid time: {e}")))?;
    let side_str = item[3]
        .as_str()
        .ok_or_else(|| ExchangeStreamError::MessageError("missing side".into()))?;
    let side = match side_str {
        "b" => TradeSide::Buy,
        "s" => TradeSide::Sell,
        other => {
            return Err(ExchangeStreamError::MessageError(format!(
                "unknown side: {}",
                other
            )));
        }
    };
    Ok(NormalizedTrade::new(
        ExchangeName::Kraken,
        pair,
        (time * 1000.0) as u64,
        side,
        price,
        amount,
    ))
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TradeMessage {
    pub pair: String,
    pub data: Vec<Vec<serde_json::Value>>,
}

impl TryFrom<TradeMessage> for NormalizedTrade {
    type Error = ExchangeStreamError;

    fn try_from(value: TradeMessage) -> Result<Self, Self::Error> {
        let first = value
            .data
            .first()
            .ok_or_else(|| ExchangeStreamError::MessageError("missing trade data".into()))?;
        trade_item_to_normalized(first, &value.pair)
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "channel")]
pub enum KrakenMessage {
    #[serde(rename = "ticker")]
    Ticker(TickerMessage),
    #[serde(rename = "trade")]
    Trade(TradeMessage),
    #[serde(other)]
    Other,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_trade_message() {
        let json = r#"{"channel":"trade","pair":"XBT/USD","data":[["5541.20000","0.15850568","1534614248.705687","b","l",""]] }"#;
        let msg: KrakenMessage = serde_json::from_str(json).unwrap();
        match msg {
            KrakenMessage::Trade(trade_msg) => {
                let trade: NormalizedTrade = trade_msg.try_into().unwrap();
                assert!(matches!(trade.exchange, ExchangeName::Kraken));
                assert_eq!(trade.symbol.as_str(), "XBT/USD");
                assert_eq!(trade.price, 5541.20000);
                assert_eq!(trade.amount, 0.15850568);
                assert!(matches!(trade.side, TradeSide::Buy));
                assert_eq!(trade.timestamp, 1534614248705);
            }
            _ => panic!("Expected trade message"),
        }
    }

    #[test]
    fn parse_ticker_message() {
        let json = r#"{"channel":"ticker","pair":"XBT/USD","data":{"a":["5000.0","1","0.5"],"b":["4999.0","1","0.6"]}}"#;
        let msg: KrakenMessage = serde_json::from_str(json).unwrap();
        match msg {
            KrakenMessage::Ticker(ticker_msg) => {
                let quote: NormalizedQuote = ticker_msg.try_into().unwrap();
                assert!(matches!(quote.exchange, ExchangeName::Kraken));
                assert_eq!(quote.symbol.as_str(), "XBT/USD");
                assert_eq!(quote.ask_price, 5000.0);
                assert_eq!(quote.ask_amount, 0.5);
                assert_eq!(quote.bid_price, 4999.0);
                assert_eq!(quote.bid_amount, 0.6);
                let now = chrono::Utc::now().timestamp_millis() as u64;
                assert!(quote.timestamp <= now && now - quote.timestamp < 10_000);
            }
            _ => panic!("Expected ticker message"),
        }
    }
}
