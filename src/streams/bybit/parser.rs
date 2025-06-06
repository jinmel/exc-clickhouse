use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use crate::{
    models::{ExchangeName, NormalizedEvent, NormalizedQuote, NormalizedTrade},
    streams::{ExchangeStreamError, Parser},
};

use super::model::{OrderbookMessage, TradeMessage};

#[derive(Debug, Clone, Default)]
struct OrderBook {
    bids: BTreeMap<OrderedFloat<f64>, f64>,
    asks: BTreeMap<OrderedFloat<f64>, f64>,
    last_cts: u64,
}

impl OrderBook {
    fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.last_cts = 0;
    }

    fn update_bid(&mut self, price: f64, qty: f64) {
        let key = OrderedFloat(price);
        if qty == 0.0 {
            self.bids.remove(&key);
        } else {
            self.bids.insert(key, qty);
        }
    }

    fn update_ask(&mut self, price: f64, qty: f64) {
        let key = OrderedFloat(price);
        if qty == 0.0 {
            self.asks.remove(&key);
        } else {
            self.asks.insert(key, qty);
        }
    }

    fn best_bid(&self) -> Option<(f64, f64)> {
        self.bids
            .iter()
            .rev()
            .next()
            .map(|(p, q)| (p.into_inner(), *q))
    }

    fn best_ask(&self) -> Option<(f64, f64)> {
        self.asks.iter().next().map(|(p, q)| (p.into_inner(), *q))
    }
}

#[derive(Clone, Default)]
pub struct BybitParser {
    books: Arc<Mutex<HashMap<String, OrderBook>>>,
}

impl BybitParser {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Parser<NormalizedEvent> for BybitParser {
    type Error = ExchangeStreamError;

    fn parse(&self, text: &str) -> Result<Option<NormalizedEvent>, Self::Error> {
        let value: serde_json::Value = serde_json::from_str(text)
            .map_err(|e| ExchangeStreamError::MessageError(format!("Failed to parse JSON: {e}")))?;

        if value.get("topic").is_none() {
            // Subscription ack or other message
            return Ok(None);
        }

        let topic = value["topic"].as_str().unwrap_or("");

        if topic.starts_with("publicTrade") {
            let msg: TradeMessage = serde_json::from_value(value).map_err(|e| {
                ExchangeStreamError::MessageError(format!("Failed to parse trade: {e}"))
            })?;
            if let Some(trade) = msg.data.into_iter().next() {
                let trade: NormalizedTrade = trade.try_into()?;
                return Ok(Some(NormalizedEvent::Trade(trade)));
            }
            return Ok(None);
        } else if topic.starts_with("orderbook") {
            let msg: OrderbookMessage = serde_json::from_value(value).map_err(|e| {
                ExchangeStreamError::MessageError(format!("Failed to parse orderbook: {e}"))
            })?;
            let mut books = self.books.lock().unwrap();
            let book = books.entry(msg.data.symbol.clone()).or_default();

            let update_ts = msg.cts.unwrap_or(msg.ts);
            if update_ts <= book.last_cts {
                // Ignore out-of-order updates
                return Ok(None);
            }
            book.last_cts = update_ts;

            match msg.typ.as_str() {
                "snapshot" => {
                    book.clear();
                    for [price, qty] in msg.data.bids {
                        if let (Ok(p), Ok(q)) = (price.parse::<f64>(), qty.parse::<f64>()) {
                            book.update_bid(p, q);
                        }
                    }
                    for [price, qty] in msg.data.asks {
                        if let (Ok(p), Ok(q)) = (price.parse::<f64>(), qty.parse::<f64>()) {
                            book.update_ask(p, q);
                        }
                    }
                }
                "delta" => {
                    for [price, qty] in msg.data.bids {
                        if let (Ok(p), Ok(q)) = (price.parse::<f64>(), qty.parse::<f64>()) {
                            book.update_bid(p, q);
                        }
                    }
                    for [price, qty] in msg.data.asks {
                        if let (Ok(p), Ok(q)) = (price.parse::<f64>(), qty.parse::<f64>()) {
                            book.update_ask(p, q);
                        }
                    }
                }
                _ => return Ok(None),
            }

            if let (Some((bid_price, bid_qty)), Some((ask_price, ask_qty))) =
                (book.best_bid(), book.best_ask())
            {
                // Bybit timestamps are in milliseconds. NormalizedQuote expects microseconds.
                let timestamp = update_ts * 1000;
                let quote = NormalizedQuote::new(
                    ExchangeName::Bybit,
                    &msg.data.symbol,
                    timestamp,
                    ask_qty,
                    ask_price,
                    bid_price,
                    bid_qty,
                );
                return Ok(Some(NormalizedEvent::Quote(quote)));
            }
            return Ok(None);
        }

        Ok(None)
    }
}
