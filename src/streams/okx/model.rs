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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LoginMessage {
    pub event: String,
    pub code: Option<String>,
    pub msg: Option<String>,
    pub connId: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ErrorMessage {
    pub event: String,
    pub code: String,
    pub msg: String,
    pub connId: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum OkxMessage {
    Trade(TradeMessage),
    Ticker(TickerMessage),
    Login(LoginMessage),
    Error(ErrorMessage),
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
            OkxMessage::Trade(trade_msg) => {
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
            OkxMessage::Ticker(ticker_msg) => {
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
}
