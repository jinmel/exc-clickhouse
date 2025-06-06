use crate::{
    models::{NormalizedEvent, NormalizedQuote, NormalizedTrade},
    streams::{
        ExchangeStreamError,
        binance::model::{BookTickerEvent, Event, TradeEvent},
    },
};

#[allow(unused)]
pub fn parse_binance_trade(event: &str) -> Result<NormalizedTrade, ExchangeStreamError> {
    serde_json::from_str::<TradeEvent>(event)
        .map_err(|e| ExchangeStreamError::MessageError(e.to_string()))?
        .try_into()
}

#[allow(unused)]
pub fn parse_binance_quote(event: &str) -> Result<NormalizedQuote, ExchangeStreamError> {
    serde_json::from_str::<BookTickerEvent>(event)
        .map_err(|e| ExchangeStreamError::MessageError(e.to_string()))?
        .try_into()
}

pub fn parse_binance_combined(res: &str) -> Result<NormalizedEvent, ExchangeStreamError> {
    let value: serde_json::Value = serde_json::from_str(res)
        .map_err(|e| ExchangeStreamError::MessageError(format!("Failed to parse JSON: {e}")))?;

    // Extract data field from the JSON
    let data = value.get("data").ok_or_else(|| {
        ExchangeStreamError::MessageError("Missing 'data' field in JSON".to_string())
    })?;

    let event = serde_json::from_value::<Event>(data.clone()).map_err(|e| {
        ExchangeStreamError::MessageError(format!("Failed to parse NormalizedEvent: {e}"))
    })?;

    let normalized = match event {
        Event::Trade(trade) => NormalizedEvent::Trade(trade.try_into()?),
        Event::Quote(quote) => NormalizedEvent::Quote(quote.try_into()?),
    };

    Ok(normalized)
}
