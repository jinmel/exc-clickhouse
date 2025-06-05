use crate::models::{ExchangeName, NormalizedEvent, NormalizedQuote, NormalizedTrade, TradeSide};
use crate::streams::ExchangeStreamError;

pub fn parse_kraken_trade(event: &str) -> Result<NormalizedTrade, ExchangeStreamError> {
    let value: serde_json::Value =
        serde_json::from_str(event).map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
    let arr = value
        .as_array()
        .ok_or_else(|| ExchangeStreamError::ParseError("Expected array message".to_string()))?;
    if arr.len() < 4 {
        return Err(ExchangeStreamError::ParseError(
            "Invalid trade message".into(),
        ));
    }
    let pair = arr[3]
        .as_str()
        .ok_or_else(|| ExchangeStreamError::ParseError("Missing pair".into()))?;
    let trades = arr[1]
        .as_array()
        .and_then(|v| v.get(0))
        .and_then(|v| v.as_array())
        .ok_or_else(|| ExchangeStreamError::ParseError("Missing trade array".into()))?;
    if trades.len() < 4 {
        return Err(ExchangeStreamError::ParseError("Invalid trade data".into()));
    }
    let price: f64 = trades[0]
        .as_str()
        .ok_or_else(|| ExchangeStreamError::ParseError("Missing price".into()))?
        .parse()
        .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid price: {e}")))?;
    let volume: f64 = trades[1]
        .as_str()
        .ok_or_else(|| ExchangeStreamError::ParseError("Missing volume".into()))?
        .parse()
        .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid volume: {e}")))?;
    let ts: f64 = trades[2]
        .as_str()
        .ok_or_else(|| ExchangeStreamError::ParseError("Missing time".into()))?
        .parse()
        .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid time: {e}")))?;
    let side_str = trades[3]
        .as_str()
        .ok_or_else(|| ExchangeStreamError::ParseError("Missing side".into()))?;
    let side = match side_str {
        "b" => TradeSide::Buy,
        "s" => TradeSide::Sell,
        other => {
            return Err(ExchangeStreamError::ParseError(format!(
                "Unknown side: {other}"
            )));
        }
    };
    Ok(NormalizedTrade::new(
        ExchangeName::Kraken,
        pair,
        (ts * 1000.0) as u64,
        side,
        price,
        volume,
    ))
}

pub fn parse_kraken_quote(event: &str) -> Result<NormalizedQuote, ExchangeStreamError> {
    let value: serde_json::Value =
        serde_json::from_str(event).map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
    let arr = value
        .as_array()
        .ok_or_else(|| ExchangeStreamError::ParseError("Expected array message".to_string()))?;
    if arr.len() < 4 {
        return Err(ExchangeStreamError::ParseError(
            "Invalid ticker message".into(),
        ));
    }
    let pair = arr[3]
        .as_str()
        .ok_or_else(|| ExchangeStreamError::ParseError("Missing pair".into()))?;
    let obj = arr[1]
        .as_object()
        .ok_or_else(|| ExchangeStreamError::ParseError("Missing ticker object".into()))?;
    let ask_arr = obj
        .get("a")
        .and_then(|v| v.as_array())
        .ok_or_else(|| ExchangeStreamError::ParseError("Missing ask array".into()))?;
    let bid_arr = obj
        .get("b")
        .and_then(|v| v.as_array())
        .ok_or_else(|| ExchangeStreamError::ParseError("Missing bid array".into()))?;
    let ask_price: f64 = ask_arr
        .get(0)
        .and_then(|v| v.as_str())
        .ok_or_else(|| ExchangeStreamError::ParseError("Missing ask price".into()))?
        .parse()
        .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid ask price: {e}")))?;
    let ask_amount: f64 = ask_arr
        .get(2)
        .and_then(|v| v.as_str())
        .ok_or_else(|| ExchangeStreamError::ParseError("Missing ask size".into()))?
        .parse()
        .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid ask size: {e}")))?;
    let bid_price: f64 = bid_arr
        .get(0)
        .and_then(|v| v.as_str())
        .ok_or_else(|| ExchangeStreamError::ParseError("Missing bid price".into()))?
        .parse()
        .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid bid price: {e}")))?;
    let bid_amount: f64 = bid_arr
        .get(2)
        .and_then(|v| v.as_str())
        .ok_or_else(|| ExchangeStreamError::ParseError("Missing bid size".into()))?
        .parse()
        .map_err(|e| ExchangeStreamError::ParseError(format!("Invalid bid size: {e}")))?;
    Ok(NormalizedQuote::new(
        ExchangeName::Kraken,
        pair,
        0,
        ask_amount,
        ask_price,
        bid_price,
        bid_amount,
    ))
}

pub fn parse_kraken_combined(event: &str) -> Result<NormalizedEvent, ExchangeStreamError> {
    let value: serde_json::Value =
        serde_json::from_str(event).map_err(|e| ExchangeStreamError::ParseError(e.to_string()))?;
    let arr = value
        .as_array()
        .ok_or_else(|| ExchangeStreamError::ParseError("Expected array message".to_string()))?;
    let event_type = arr
        .get(2)
        .and_then(|v| v.as_str())
        .ok_or_else(|| ExchangeStreamError::ParseError("Missing event type".into()))?;
    match event_type {
        "trade" => Ok(NormalizedEvent::Trade(parse_kraken_trade(event)?)),
        "ticker" => Ok(NormalizedEvent::Quote(parse_kraken_quote(event)?)),
        other => Err(ExchangeStreamError::ParseError(format!(
            "Unknown event type: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_kraken_trade() {
        let msg =
            r#"[0, [["45000.0", "0.1", "1700000000.1234", "b", "m", ""]], "trade", "XBT/USD"]"#;
        let trade = parse_kraken_trade(msg).unwrap();
        assert!(matches!(trade.exchange, ExchangeName::Kraken));
        assert_eq!(&trade.symbol[..], "XBT/USD");
        assert_eq!(trade.timestamp, 1700000000123);
        assert!(matches!(trade.side, TradeSide::Buy));
        assert_eq!(trade.price, 45000.0);
        assert_eq!(trade.amount, 0.1);
    }

    #[test]
    fn test_parse_kraken_quote() {
        let msg = r#"[0,{"a":["45001.2","1","0.5"],"b":["44999.8","1","0.4"]},"ticker","XBT/USD"]"#;
        let quote = parse_kraken_quote(msg).unwrap();
        assert!(matches!(quote.exchange, ExchangeName::Kraken));
        assert_eq!(&quote.symbol[..], "XBT/USD");
        assert_eq!(quote.ask_price, 45001.2);
        assert_eq!(quote.ask_amount, 0.5);
        assert_eq!(quote.bid_price, 44999.8);
        assert_eq!(quote.bid_amount, 0.4);
    }

    #[test]
    fn test_parse_kraken_combined_trade() {
        let msg =
            r#"[0, [["45000.0", "0.1", "1700000000.1234", "b", "m", ""]], "trade", "XBT/USD"]"#;
        match parse_kraken_combined(msg).unwrap() {
            NormalizedEvent::Trade(t) => assert!(matches!(t.exchange, ExchangeName::Kraken)),
            _ => panic!("Expected trade"),
        }
    }

    #[test]
    fn test_parse_kraken_combined_quote() {
        let msg = r#"[0,{"a":["45001.2","1","0.5"],"b":["44999.8","1","0.4"]},"ticker","XBT/USD"]"#;
        match parse_kraken_combined(msg).unwrap() {
            NormalizedEvent::Quote(q) => assert!(matches!(q.exchange, ExchangeName::Kraken)),
            _ => panic!("Expected quote"),
        }
    }
}
