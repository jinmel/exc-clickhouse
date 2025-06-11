use arrayvec::ArrayString;
use serde::{Deserialize, Serialize};

use crate::{
    models::{ExchangeName, NormalizedQuote, NormalizedTrade, TradeSide},
    streams::ExchangeStreamError,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Deal {
    pub price: String,
    pub quantity: String,
    pub tradetype: u8,
    pub time: u64,
}

impl TryFrom<Deal> for NormalizedTrade {
    type Error = ExchangeStreamError;

    fn try_from(d: Deal) -> Result<NormalizedTrade, Self::Error> {
        let price = d
            .price
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid price: {e}")))?;
        let amount = d
            .quantity
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid quantity: {e}")))?;
        let side = match d.tradetype {
            1 => TradeSide::Buy,
            2 => TradeSide::Sell,
            _ => {
                return Err(ExchangeStreamError::Message(format!(
                    "Unknown trade type: {}",
                    d.tradetype
                )));
            }
        };
        Ok(NormalizedTrade::new(
            ExchangeName::Mexc,
            "", // symbol will be set by parent message
            d.time * 1000,
            side,
            price,
            amount,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DealsData {
    pub deals_list: Vec<Deal>,
    pub eventtype: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TradeMessage {
    pub channel: String,
    pub publicdeals: DealsData,
    pub symbol: String,
    pub sendtime: u64,
}

impl TryFrom<TradeMessage> for Vec<NormalizedTrade> {
    type Error = ExchangeStreamError;

    fn try_from(msg: TradeMessage) -> Result<Vec<NormalizedTrade>, Self::Error> {
        msg.publicdeals
            .deals_list
            .into_iter()
            .map(|d| {
                let mut trade: NormalizedTrade = d.try_into()?;
                trade.symbol = ArrayString::<20>::from(&msg.symbol).unwrap();
                Ok(trade)
            })
            .collect()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BookTickerData {
    pub bidprice: String,
    pub bidquantity: String,
    pub askprice: String,
    pub askquantity: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BookTickerMessage {
    pub channel: String,
    pub publicbookticker: BookTickerData,
    pub symbol: String,
    pub sendtime: u64,
}

impl TryFrom<BookTickerMessage> for NormalizedQuote {
    type Error = ExchangeStreamError;

    fn try_from(msg: BookTickerMessage) -> Result<NormalizedQuote, Self::Error> {
        let bid_price = msg
            .publicbookticker
            .bidprice
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid bid price: {e}")))?;
        let bid_amount = msg
            .publicbookticker
            .bidquantity
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid bid quantity: {e}")))?;
        let ask_price = msg
            .publicbookticker
            .askprice
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid ask price: {e}")))?;
        let ask_amount = msg
            .publicbookticker
            .askquantity
            .parse::<f64>()
            .map_err(|e| ExchangeStreamError::Message(format!("Invalid ask quantity: {e}")))?;
        Ok(NormalizedQuote::new(
            ExchangeName::Mexc,
            &msg.symbol,
            msg.sendtime * 1000,
            ask_amount,
            ask_price,
            bid_price,
            bid_amount,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SubscriptionResponse {
    pub id: Option<u64>,
    pub code: i32,
    pub msg: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum MexcMessage {
    Trade(TradeMessage),
    BookTicker(BookTickerMessage),
    Ack(SubscriptionResponse),
}
