use serde::{Deserialize, Serialize};

use crate::{
    models::{ExchangeName, NormalizedQuote, NormalizedTrade, TradeSide},
    streams::ExchangeStreamError,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Arg {
    pub channel: String,
    #[serde(rename = "instId")]
    pub inst_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TradeData {
    #[serde(rename = "instId")]
    pub inst_id: String,
    #[serde(rename = "tradeId")]
    pub trade_id: String,
    pub px: String,
    pub sz: String,
    pub side: String,
    pub ts: String,
    #[serde(default)]
    pub count: Option<String>,
}

impl TryFrom<TradeData> for NormalizedTrade {
    type Error = ExchangeStreamError;

    fn try_from(trade: TradeData) -> Result<NormalizedTrade, Self::Error> {
        let price = trade
            .px
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid price value: {e}")))?;
        let amount = trade
            .sz
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid quantity value: {e}")))?;
        let ts = trade
            .ts
            .parse::<u64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid timestamp: {e}")))?;
        let side = match trade.side.as_str() {
            "buy" => TradeSide::Buy,
            "sell" => TradeSide::Sell,
            _ => {
                return Err(ExchangeStreamError::Message(format!(
                    "Unknown trade side: {}",
                    trade.side
                )));
            }
        };
        Ok(NormalizedTrade::new(
            ExchangeName::Okx,
            &trade.inst_id,
            ts * 1000,
            side,
            price,
            amount,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TradeMessage {
    pub arg: Arg,
    pub data: Vec<TradeData>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TickerData {
    #[serde(rename = "instType")]
    pub inst_type: Option<String>,
    #[serde(rename = "instId")]
    pub inst_id: String,
    pub last: String,
    #[serde(rename = "lastSz")]
    pub last_sz: String,
    #[serde(rename = "askPx")]
    pub ask_px: String,
    #[serde(rename = "askSz")]
    pub ask_sz: String,
    #[serde(rename = "bidPx")]
    pub bid_px: String,
    #[serde(rename = "bidSz")]
    pub bid_sz: String,
    pub ts: String,
}

impl TryFrom<TickerData> for NormalizedQuote {
    type Error = ExchangeStreamError;

    fn try_from(ticker: TickerData) -> Result<NormalizedQuote, Self::Error> {
        let ask_amount = ticker
            .ask_sz
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid ask size: {e}")))?;
        let ask_price = ticker
            .ask_px
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid ask price: {e}")))?;
        let bid_price = ticker
            .bid_px
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid bid price: {e}")))?;
        let bid_amount = ticker
            .bid_sz
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid bid size: {e}")))?;
        let ts = ticker
            .ts
            .parse::<u64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid timestamp: {e}")))?;
        Ok(NormalizedQuote::new(
            ExchangeName::Okx,
            &ticker.inst_id,
            ts * 1000,
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TickerMessage {
    pub arg: Arg,
    pub data: Vec<TickerData>,
}



use serde::de::{self, Deserializer, Visitor};
use std::fmt;

#[derive(Debug, Clone)]
pub enum OkxDataMessage {
    Trade(TradeMessage),
    Ticker(TickerMessage),
}

impl<'de> serde::Deserialize<'de> for OkxDataMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct OkxDataMessageVisitor;

        impl<'de> Visitor<'de> for OkxDataMessageVisitor {
            type Value = OkxDataMessage;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("an OKX data message")
            }

            fn visit_map<V>(self, mut map: V) -> Result<OkxDataMessage, V::Error>
            where
                V: de::MapAccess<'de>,
            {
                let mut arg: Option<Arg> = None;
                let mut data: Option<serde_json::Value> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "arg" => {
                            if arg.is_some() {
                                return Err(de::Error::duplicate_field("arg"));
                            }
                            arg = Some(map.next_value()?);
                        }
                        "data" => {
                            if data.is_some() {
                                return Err(de::Error::duplicate_field("data"));
                            }
                            data = Some(map.next_value()?);
                        }
                        _ => {
                            let _: serde_json::Value = map.next_value()?;
                        }
                    }
                }

                let arg = arg.ok_or_else(|| de::Error::missing_field("arg"))?;
                let data = data.ok_or_else(|| de::Error::missing_field("data"))?;

                match arg.channel.as_str() {
                    "trades" | "trades-all" => {
                        let trade_data: Vec<TradeData> = serde_json::from_value(data)
                            .map_err(de::Error::custom)?;
                        Ok(OkxDataMessage::Trade(TradeMessage { arg, data: trade_data }))
                    }
                    "tickers" => {
                        let ticker_data: Vec<TickerData> = serde_json::from_value(data)
                            .map_err(de::Error::custom)?;
                        Ok(OkxDataMessage::Ticker(TickerMessage { arg, data: ticker_data }))
                    }
                    _ => Err(de::Error::custom(format!("Unknown channel: {}", arg.channel))),
                }
            }
        }

        deserializer.deserialize_map(OkxDataMessageVisitor)
    }
}

impl serde::Serialize for OkxDataMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            OkxDataMessage::Trade(trade_msg) => trade_msg.serialize(serializer),
            OkxDataMessage::Ticker(ticker_msg) => ticker_msg.serialize(serializer),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "event")]
pub enum OkxEventMessage {
    #[serde(rename = "login")]
    Login {
        code: Option<String>,
        msg: Option<String>,
        #[serde(rename = "connId")]
        conn_id: Option<String>,
    },
    #[serde(rename = "subscribe")]
    Subscribe {
        arg: Arg,
        #[serde(rename = "connId")]
        conn_id: Option<String>,
    },
    #[serde(rename = "error")]
    Error {
        code: String,
        msg: String,
        #[serde(rename = "connId")]
        conn_id: Option<String>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum OkxMessage {
    Data(OkxDataMessage),
    Event(OkxEventMessage),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trade_message_parsing() {
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

        let parsed: OkxMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            OkxMessage::Data(OkxDataMessage::Trade(trade_msg)) => {
                assert_eq!(trade_msg.arg.channel, "trades");
                assert_eq!(trade_msg.arg.inst_id, "BTC-USDT");
                assert_eq!(trade_msg.data.len(), 1);
                let trade_data = &trade_msg.data[0];
                assert_eq!(trade_data.inst_id, "BTC-USDT");
                assert_eq!(trade_data.px, "50000.0");
                assert_eq!(trade_data.side, "buy");
            }
            _ => panic!("Expected Trade message, got {:?}", parsed),
        }
    }

    #[test]
    fn test_ticker_message_parsing() {
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

        let parsed: OkxMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            OkxMessage::Data(OkxDataMessage::Ticker(ticker_msg)) => {
                assert_eq!(ticker_msg.arg.channel, "tickers");
                assert_eq!(ticker_msg.arg.inst_id, "BTC-USDT");
                assert_eq!(ticker_msg.data.len(), 1);
                let ticker_data = &ticker_msg.data[0];
                assert_eq!(ticker_data.inst_id, "BTC-USDT");
                assert_eq!(ticker_data.ask_px, "50001.0");
                assert_eq!(ticker_data.bid_px, "49999.0");
            }
            _ => panic!("Expected Ticker message, got {:?}", parsed),
        }
    }

    #[test]
    fn test_login_message_parsing() {
        let json = r#"{
            "event": "login",
            "code": "0",
            "msg": "",
            "connId": "a4d3ae55"
        }"#;

        let parsed: OkxMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            OkxMessage::Event(OkxEventMessage::Login { code, msg, conn_id }) => {
                assert_eq!(code, Some("0".to_string()));
                assert_eq!(msg, Some("".to_string()));
                assert_eq!(conn_id, Some("a4d3ae55".to_string()));
            }
            _ => panic!("Expected Login message, got {:?}", parsed),
        }
    }

    #[test]
    fn test_login_message_parsing_minimal() {
        let json = r#"{
            "event": "login"
        }"#;

        let parsed: OkxMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            OkxMessage::Event(OkxEventMessage::Login { code, msg, conn_id }) => {
                assert_eq!(code, None);
                assert_eq!(msg, None);
                assert_eq!(conn_id, None);
            }
            _ => panic!("Expected Login message, got {:?}", parsed),
        }
    }

    #[test]
    fn test_error_message_parsing() {
        let json = r#"{
            "event": "error",
            "code": "60012",
            "msg": "Invalid request: {\"op\": \"subscribe\", \"args\": []}",
            "connId": "a4d3ae55"
        }"#;

        let parsed: OkxMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            OkxMessage::Event(OkxEventMessage::Error { code, msg, conn_id }) => {
                assert_eq!(code, "60012");
                assert_eq!(msg, "Invalid request: {\"op\": \"subscribe\", \"args\": []}");
                assert_eq!(conn_id, Some("a4d3ae55".to_string()));
            }
            _ => panic!("Expected Error message, got {:?}", parsed),
        }
    }

    #[test]
    fn test_error_message_parsing_without_conn_id() {
        let json = r#"{
            "event": "error",
            "code": "60009",
            "msg": "Login failed"
        }"#;

        let parsed: OkxMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            OkxMessage::Event(OkxEventMessage::Error { code, msg, conn_id }) => {
                assert_eq!(code, "60009");
                assert_eq!(msg, "Login failed");
                assert_eq!(conn_id, None);
            }
            _ => panic!("Expected Error message, got {:?}", parsed),
        }
    }

    #[test]
    fn test_trades_all_channel_parsing() {
        let json = r#"{
            "arg": {
                "channel": "trades-all",
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

        let parsed: OkxMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            OkxMessage::Data(OkxDataMessage::Trade(trade_msg)) => {
                assert_eq!(trade_msg.arg.channel, "trades-all");
                assert_eq!(trade_msg.arg.inst_id, "BTC-USDT");
                assert_eq!(trade_msg.data.len(), 1);
                let trade_data = &trade_msg.data[0];
                assert_eq!(trade_data.inst_id, "BTC-USDT");
                assert_eq!(trade_data.px, "50000.0");
                assert_eq!(trade_data.side, "buy");
            }
            _ => panic!("Expected Trade message, got {:?}", parsed),
        }
    }

    #[test]
    fn test_subscribe_message_parsing() {
        let json = r#"{
            "event": "subscribe",
            "arg": {
                "channel": "tickers",
                "instId": "PENDLE-USDT"
            },
            "connId": "78da8d48"
        }"#;

        let parsed: OkxMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            OkxMessage::Event(OkxEventMessage::Subscribe { arg, conn_id }) => {
                assert_eq!(arg.channel, "tickers");
                assert_eq!(arg.inst_id, "PENDLE-USDT");
                assert_eq!(conn_id, Some("78da8d48".to_string()));
            }
            _ => panic!("Expected Subscribe message, got {:?}", parsed),
        }
    }

    #[test]
    fn test_subscribe_message_parsing_without_conn_id() {
        let json = r#"{
            "event": "subscribe",
            "arg": {
                "channel": "tickers",
                "instId": "PENDLE-USDT"
            }
        }"#;

        let parsed: OkxMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            OkxMessage::Event(OkxEventMessage::Subscribe { arg, conn_id }) => {
                assert_eq!(arg.channel, "tickers");
                assert_eq!(arg.inst_id, "PENDLE-USDT");
                assert_eq!(conn_id, None);
            }
            _ => panic!("Expected Subscribe message, got {:?}", parsed),
        }
    }
}
