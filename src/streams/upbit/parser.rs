use crate::{
  models::{NormalizedTrade, NormalizedQuote},
  streams::{
    ExchangeStreamError, Parser,
    upbit::model::TradeEvent,
  }
};

pub fn parse_upbit_trade(event: &str) -> Result<NormalizedTrade, ExchangeStreamError> {
    serde_json::from_str::<TradeEvent>(event)
        .map_err(|e| ExchangeStreamError::MessageError(e.to_string()))?
        .try_into()
}

pub fn parse_upbit_quote(event: &str) -> Result<NormalizedQuote, ExchangeStreamError> {
    serde_json::from_str::<TradeEvent>(event)
        .map_err(|e| ExchangeStreamError::MessageError(e.to_string()))?
        .try_into()
}

#[derive(Clone)]
pub struct UpbitTradeParser;

impl Parser<NormalizedTrade> for UpbitTradeParser {
    type Error = ExchangeStreamError;

    fn parse(&self, text: &str) -> Result<Option<NormalizedTrade>, Self::Error> {
        Ok(Some(parse_upbit_trade(text)?))
    }
}

#[derive(Clone)]
pub struct UpbitQuoteParser;

impl Parser<NormalizedQuote> for UpbitQuoteParser {
    type Error = ExchangeStreamError;

    fn parse(&self, text: &str) -> Result<Option<NormalizedQuote>, Self::Error> {
        Ok(Some(parse_upbit_quote(text)?))
    }
}