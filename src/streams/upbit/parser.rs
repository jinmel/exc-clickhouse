use crate::{
  models::{NormalizedTrade, NormalizedQuote},
  streams::{
    ExchangeStreamError,
    upbit::model::TradeEvent,
  }
};

pub fn parse_upbit_trade(event: &str) -> Result<NormalizedTrade, ExchangeStreamError> {
    serde_json::from_str::<TradeEvent>(event)
        .map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?
        .try_into()
}

pub fn parse_upbit_quote(event: &str) -> Result<NormalizedQuote, ExchangeStreamError> {
    serde_json::from_str::<TradeEvent>(event)
        .map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?
        .try_into()
}