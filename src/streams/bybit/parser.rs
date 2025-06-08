use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use crate::{
    models::{ExchangeName, NormalizedEvent, NormalizedQuote, NormalizedTrade},
    streams::bybit::model::BybitMessage,
    streams::{ExchangeStreamError, Parser},
};

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
            .next_back()
            .map(|(p, q)| (p.into_inner(), *q))
    }

    fn best_ask(&self) -> Option<(f64, f64)> {
        self.asks.iter().next().map(|(p, q)| (p.into_inner(), *q))
    }
}

#[derive(Clone, Default, Debug)]
pub struct BybitParser {
    books: Arc<Mutex<HashMap<String, OrderBook>>>,
}

impl BybitParser {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Parser<Vec<NormalizedEvent>> for BybitParser {
    type Error = ExchangeStreamError;

    fn parse(&self, text: &str) -> Result<Option<Vec<NormalizedEvent>>, Self::Error> {
        let msg: BybitMessage = serde_json::from_str(text)
            .map_err(|e| ExchangeStreamError::Message(format!("Failed to parse JSON: {e}")))?;

        match msg {
            BybitMessage::Trade(trade_msg) => {
                let normalized_trades: Vec<NormalizedTrade> = trade_msg.try_into()?;
                Ok(Some(
                    normalized_trades
                        .into_iter()
                        .map(|trade| NormalizedEvent::Trade(trade))
                        .collect(),
                ))
            }
            BybitMessage::Orderbook(orderbook_msg) => {
                let mut books = self.books.lock().unwrap();
                let book = books.entry(orderbook_msg.data.symbol.clone()).or_default();
                let update_ts = orderbook_msg.cts.unwrap_or(orderbook_msg.ts);
                if update_ts <= book.last_cts {
                    // Ignore out-of-order updates
                    return Ok(None);
                }

                match orderbook_msg.typ.as_str() {
                    "snapshot" => {
                        book.clear();
                        book.last_cts = update_ts; // Set timestamp after clear
                        for [price, qty] in orderbook_msg.data.bids {
                            if let (Ok(p), Ok(q)) = (price.parse::<f64>(), qty.parse::<f64>()) {
                                book.update_bid(p, q);
                            }
                        }
                        for [price, qty] in orderbook_msg.data.asks {
                            if let (Ok(p), Ok(q)) = (price.parse::<f64>(), qty.parse::<f64>()) {
                                book.update_ask(p, q);
                            }
                        }
                    }
                    "delta" => {
                        book.last_cts = update_ts; // Set timestamp for delta
                        for [price, qty] in orderbook_msg.data.bids {
                            if let (Ok(p), Ok(q)) = (price.parse::<f64>(), qty.parse::<f64>()) {
                                book.update_bid(p, q);
                            }
                        }
                        for [price, qty] in orderbook_msg.data.asks {
                            if let (Ok(p), Ok(q)) = (price.parse::<f64>(), qty.parse::<f64>()) {
                                book.update_ask(p, q);
                            }
                        }
                    }
                    _ => {
                        tracing::warn!(
                            "Received unhandled bybit orderbook message: {:?}",
                            orderbook_msg
                        );
                        return Ok(None);
                    }
                }

                if let (Some((bid_price, bid_qty)), Some((ask_price, ask_qty))) =
                    (book.best_bid(), book.best_ask())
                {
                    // Bybit timestamps are in milliseconds. NormalizedQuote expects microseconds.
                    let timestamp = update_ts * 1000;
                    let quote = NormalizedQuote::new(
                        ExchangeName::Bybit,
                        &orderbook_msg.data.symbol,
                        timestamp,
                        ask_qty,
                        ask_price,
                        bid_price,
                        bid_qty,
                    );
                    Ok(Some(vec![NormalizedEvent::Quote(quote)]))
                } else {
                    Ok(None)
                }
            }
            BybitMessage::Subscription(subscription_msg) => {
                tracing::debug!("Subscription ack or other message: {:?}", subscription_msg);
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{ExchangeName, NormalizedEvent};

    #[test]
    fn test_orderbook_snapshot_and_delta_updates() {
        let parser = BybitParser::new();

        // Test 1: Snapshot message
        let snapshot_json = r#"{
            "topic": "orderbook.1.BTCUSDT",
            "type": "snapshot",
            "ts": 2,
            "cts": 1,
            "data": {
                "s": "BTCUSDT",
                "b": [["16578.50", "1.431"], ["16578.49", "0.001"], ["16578.48", "2.500"]],
                "a": [["16578.51", "1.431"], ["16578.52", "0.001"], ["16578.53", "3.200"]],
                "u": 18521288,
                "seq": 7961638724
            }
        }"#;

        let result = parser.parse(snapshot_json).unwrap();
        assert!(result.is_some());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        
        // Verify the quote from snapshot
        match &events[0] {
            NormalizedEvent::Quote(quote) => {
                assert_eq!(quote.exchange, ExchangeName::Bybit);
                assert_eq!(quote.symbol.as_str(), "BTCUSDT");
                assert_eq!(quote.timestamp, 1000); // cts * 1000
                assert_eq!(quote.ask_price, 16578.51);
                assert_eq!(quote.ask_amount, 1.431);
                assert_eq!(quote.bid_price, 16578.50);
                assert_eq!(quote.bid_amount, 1.431);
            }
            _ => panic!("Expected Quote event"),
        }

        // Verify internal orderbook state after snapshot
        {
            let books = parser.books.lock().unwrap();
            let book = books.get("BTCUSDT").unwrap();
            assert_eq!(book.last_cts, 1);
            
            // Check bids (should be sorted in descending order)
            let bids: Vec<_> = book.bids.iter().collect();
            assert_eq!(bids.len(), 3);
            assert_eq!(bids[0].0.into_inner(), 16578.48); // lowest bid first in BTreeMap
            assert_eq!(bids[1].0.into_inner(), 16578.49);
            assert_eq!(bids[2].0.into_inner(), 16578.50); // highest bid last
            
            // Check asks (should be sorted in ascending order)
            let asks: Vec<_> = book.asks.iter().collect();
            assert_eq!(asks.len(), 3);
            assert_eq!(asks[0].0.into_inner(), 16578.51); // lowest ask first
            assert_eq!(asks[1].0.into_inner(), 16578.52);
            assert_eq!(asks[2].0.into_inner(), 16578.53); // highest ask last
            
            // Verify best bid/ask
            assert_eq!(book.best_bid(), Some((16578.50, 1.431)));
            assert_eq!(book.best_ask(), Some((16578.51, 1.431)));
        }

        // Test 2: First delta update - modify existing levels and add new ones
        let delta1_json = r#"{
            "topic": "orderbook.1.BTCUSDT",
            "type": "delta",
            "ts": 3,
            "cts": 2,
            "data": {
                "s": "BTCUSDT",
                "b": [["16578.50", "2.500"], ["16578.47", "1.000"]],
                "a": [["16578.51", "0.500"], ["16578.54", "1.500"]],
                "u": 18521289,
                "seq": 7961638725
            }
        }"#;

        let result = parser.parse(delta1_json).unwrap();
        assert!(result.is_some());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);

        // Verify the quote from first delta
        match &events[0] {
            NormalizedEvent::Quote(quote) => {
                assert_eq!(quote.timestamp, 2000); // cts * 1000
                assert_eq!(quote.ask_price, 16578.51);
                assert_eq!(quote.ask_amount, 0.500); // updated amount
                assert_eq!(quote.bid_price, 16578.50);
                assert_eq!(quote.bid_amount, 2.500); // updated amount
            }
            _ => panic!("Expected Quote event"),
        }

        // Verify internal orderbook state after first delta
        {
            let books = parser.books.lock().unwrap();
            let book = books.get("BTCUSDT").unwrap();
            assert_eq!(book.last_cts, 2);
            
            // Check bids - should have 4 levels now
            let bids: Vec<_> = book.bids.iter().collect();
            assert_eq!(bids.len(), 4);
            assert_eq!(bids[0].0.into_inner(), 16578.47); // new level
            assert_eq!(*bids[0].1, 1.000);
            assert_eq!(bids[3].0.into_inner(), 16578.50); // updated level
            assert_eq!(*bids[3].1, 2.500);
            
            // Check asks - should have 4 levels now
            let asks: Vec<_> = book.asks.iter().collect();
            assert_eq!(asks.len(), 4);
            assert_eq!(asks[0].0.into_inner(), 16578.51); // updated level
            assert_eq!(*asks[0].1, 0.500);
            assert_eq!(asks[3].0.into_inner(), 16578.54); // new level
            assert_eq!(*asks[3].1, 1.500);
        }

        // Test 3: Second delta update - remove some levels (qty = 0) and update others
        let delta2_json = r#"{
            "topic": "orderbook.1.BTCUSDT",
            "type": "delta",
            "ts": 4,
            "cts": 3,
            "data": {
                "s": "BTCUSDT",
                "b": [["16578.49", "0"], ["16578.51", "1.200"]],
                "a": [["16578.52", "0"], ["16578.50", "0.800"]],
                "u": 18521290,
                "seq": 7961638726
            }
        }"#;

        let result = parser.parse(delta2_json).unwrap();
        assert!(result.is_some());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);

        // Verify the quote from second delta
        match &events[0] {
            NormalizedEvent::Quote(quote) => {
                assert_eq!(quote.timestamp, 3000); // cts * 1000
                assert_eq!(quote.bid_price, 16578.51); // new best bid (crossed spread)
                assert_eq!(quote.bid_amount, 1.200);
                assert_eq!(quote.ask_price, 16578.50); // new best ask (crossed spread)
                assert_eq!(quote.ask_amount, 0.800);
            }
            _ => panic!("Expected Quote event"),
        }

        // Verify final internal orderbook state
        {
            let books = parser.books.lock().unwrap();
            let book = books.get("BTCUSDT").unwrap();
            assert_eq!(book.last_cts, 3);
            
            // Check bids - 16578.49 should be removed, 16578.51 should be added
            let bids: Vec<_> = book.bids.iter().collect();
            assert_eq!(bids.len(), 4); // 3 original + 1 new - 1 removed + 1 added = 4
            assert!(!bids.iter().any(|(p, _)| p.into_inner() == 16578.49)); // removed
            assert!(bids.iter().any(|(p, q)| p.into_inner() == 16578.51 && **q == 1.200)); // added
            
            // Check asks - 16578.52 should be removed, 16578.50 should be added
            let asks: Vec<_> = book.asks.iter().collect();
            assert_eq!(asks.len(), 4); // 3 original + 1 new - 1 removed + 1 added = 4
            assert!(!asks.iter().any(|(p, _)| p.into_inner() == 16578.52)); // removed
            assert!(asks.iter().any(|(p, q)| p.into_inner() == 16578.50 && **q == 0.800)); // added
            
            // Verify best bid/ask (note: this creates a crossed market which is unusual but tests the logic)
            assert_eq!(book.best_bid(), Some((16578.51, 1.200)));
            assert_eq!(book.best_ask(), Some((16578.50, 0.800)));
        }
    }

    #[test]
    fn test_out_of_order_orderbook_updates() {
        let parser = BybitParser::new();

        // First message with higher timestamp
        let msg1_json = r#"{
            "topic": "orderbook.1.BTCUSDT",
            "type": "snapshot",
            "ts": 10,
            "cts": 5,
            "data": {
                "s": "BTCUSDT",
                "b": [["16578.50", "1.431"]],
                "a": [["16578.51", "1.431"]],
                "u": 18521288,
                "seq": 7961638724
            }
        }"#;

        let result = parser.parse(msg1_json).unwrap();
        assert!(result.is_some());

        // Second message with lower timestamp (should be ignored)
        let msg2_json = r#"{
            "topic": "orderbook.1.BTCUSDT",
            "type": "delta",
            "ts": 8,
            "cts": 3,
            "data": {
                "s": "BTCUSDT",
                "b": [["16578.50", "2.000"]],
                "a": [["16578.51", "2.000"]],
                "u": 18521289,
                "seq": 7961638725
            }
        }"#;

        let result = parser.parse(msg2_json).unwrap();
        assert!(result.is_none()); // Should be ignored due to out-of-order timestamp

        // Third message with equal timestamp (should also be ignored)
        let msg3_json = r#"{
            "topic": "orderbook.1.BTCUSDT",
            "type": "delta",
            "ts": 12,
            "cts": 5,
            "data": {
                "s": "BTCUSDT",
                "b": [["16578.50", "3.000"]],
                "a": [["16578.51", "3.000"]],
                "u": 18521290,
                "seq": 7961638726
            }
        }"#;

        let result = parser.parse(msg3_json).unwrap();
        assert!(result.is_none()); // Should be ignored due to equal timestamp

        // Verify the orderbook state hasn't changed
        {
            let books = parser.books.lock().unwrap();
            let book = books.get("BTCUSDT").unwrap();
            assert_eq!(book.last_cts, 5); // Still the first message's timestamp
            assert_eq!(book.best_bid(), Some((16578.50, 1.431))); // Original values
            assert_eq!(book.best_ask(), Some((16578.51, 1.431)));
        }
    }
}
