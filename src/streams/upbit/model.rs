use serde::{Deserialize, Serialize};
use crate::models::{NormalizedQuote, NormalizedTrade, TradeSide};
use crate::models::ExchangeName;
use crate::streams::ExchangeStreamError;

// https://docs.upbit.com/kr/reference/websocket-ticker
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TickerEvent {
    /// 타입 (ticker : 현재가)
    #[serde(rename = "ty")]
    pub typ: String,

    /// 마켓 코드 (ex. KRW-BTC)
    #[serde(rename = "cd")]
    pub code: String,

    /// 시가
    #[serde(rename = "op")]
    pub opening_price: f64,

    /// 고가
    #[serde(rename = "hp")]
    pub high_price: f64,

    /// 저가
    #[serde(rename = "lp")]
    pub low_price: f64,

    /// 현재가
    #[serde(rename = "tp")]
    pub trade_price: f64,

    /// 전일 종가
    #[serde(rename = "pcp")]
    pub prev_closing_price: f64,

    /// 전일 대비 (RISE : 상승, EVEN : 보합, FALL : 하락)
    #[serde(rename = "c")]
    pub change: String,

    /// 부호 없는 전일 대비 값
    #[serde(rename = "cp")]
    pub change_price: f64,

    /// 전일 대비 값
    #[serde(rename = "scp")]
    pub signed_change_price: f64,

    /// 부호 없는 전일 대비 등락율
    #[serde(rename = "cr")]
    pub change_rate: f64,

    /// 전일 대비 등락율
    #[serde(rename = "scr")]
    pub signed_change_rate: f64,

    /// 가장 최근 거래량
    #[serde(rename = "tv")]
    pub trade_volume: f64,

    /// 누적 거래량(UTC 0시 기준)
    #[serde(rename = "atv")]
    pub acc_trade_volume: f64,

    /// 24시간 누적 거래량
    #[serde(rename = "atv24h")]
    pub acc_trade_volume_24h: f64,

    /// 누적 거래대금(UTC 0시 기준)
    #[serde(rename = "atp")]
    pub acc_trade_price: f64,

    /// 24시간 누적 거래대금
    #[serde(rename = "atp24h")]
    pub acc_trade_price_24h: f64,

    /// 최근 거래 일자(UTC) (yyyyMMdd)
    #[serde(rename = "tdt")]
    pub trade_date: String,

    /// 최근 거래 시각(UTC) (HHmmss)
    #[serde(rename = "ttm")]
    pub trade_time: String,

    /// 체결 타임스탬프 (milliseconds)
    #[serde(rename = "ttms")]
    pub trade_timestamp: i64,

    /// 매수/매도 구분 (ASK : 매도, BID : 매수)
    #[serde(rename = "ab")]
    pub ask_bid: String,

    /// 누적 매도량
    #[serde(rename = "aav")]
    pub acc_ask_volume: f64,

    /// 누적 매수량
    #[serde(rename = "abv")]
    pub acc_bid_volume: f64,

    /// 52주 최고가
    #[serde(rename = "h52wp")]
    pub highest_52_week_price: f64,

    /// 52주 최고가 달성일 (yyyy-MM-dd)
    #[serde(rename = "h52wdt")]
    pub highest_52_week_date: String,

    /// 52주 최저가
    #[serde(rename = "l52wp")]
    pub lowest_52_week_price: f64,

    /// 52주 최저가 달성일 (yyyy-MM-dd)
    #[serde(rename = "l52wdt")]
    pub lowest_52_week_date: String,

    /// 거래상태 (*Deprecated)
    #[serde(rename = "ts")]
    pub trade_status: Option<String>,

    /// 거래상태 (PREVIEW : 입금지원, ACTIVE : 거래지원가능, DELISTED : 거래지원종료)
    #[serde(rename = "ms")]
    pub market_state: String,

    /// 거래 상태 (*Deprecated)
    #[serde(rename = "msfi")]
    pub market_state_for_ios: Option<String>,

    /// 거래 정지 여부 (*Deprecated)
    #[serde(rename = "its")]
    pub is_trading_suspended: Option<bool>,

    /// 거래지원 종료일
    #[serde(rename = "dd")]
    pub delisting_date: Option<String>,

    /// 유의 종목 여부 (NONE : 해당없음, CAUTION : 투자유의)
    #[serde(rename = "mw")]
    pub market_warning: String,

    /// 타임스탬프 (millisecond)
    #[serde(rename = "tms")]
    pub timestamp: i64,

    /// 스트림 타입 (SNAPSHOT : 스냅샷, REALTIME : 실시간)
    #[serde(rename = "st")]
    pub stream_type: String,
}

// https://docs.upbit.com/kr/reference/websocket-trade
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TradeEvent {
    /// 타입 (trade : 체결)
    #[serde(rename = "ty")]
    pub typ: String,

    /// 마켓 코드 (ex. KRW-BTC)
    #[serde(rename = "cd")]
    pub code: String,

    /// 체결 가격
    #[serde(rename = "tp")]
    pub trade_price: f64,

    /// 체결량
    #[serde(rename = "tv")]
    pub trade_volume: f64,

    /// 매수/매도 구분 (ASK : 매도, BID : 매수)
    #[serde(rename = "ab")]
    pub ask_bid: String,

    /// 전일 종가
    #[serde(rename = "pcp")]
    pub prev_closing_price: f64,

    /// 전일 대비 (RISE : 상승, EVEN : 보합, FALL : 하락)
    #[serde(rename = "c")]
    pub change: String,

    /// 부호 없는 전일 대비 값
    #[serde(rename = "cp")]
    pub change_price: f64,

    /// 체결 일자(UTC 기준) (yyyy-MM-dd)
    #[serde(rename = "td")]
    pub trade_date: String,

    /// 체결 시각(UTC 기준) (HH:mm:ss)
    #[serde(rename = "ttm")]
    pub trade_time: String,

    /// 체결 타임스탬프 (millisecond)
    #[serde(rename = "ttms")]
    pub trade_timestamp: i64,

    /// 타임스탬프 (millisecond)
    #[serde(rename = "tms")]
    pub timestamp: i64,

    /// 체결 번호 (Unique)
    #[serde(rename = "sid")]
    pub sequential_id: i64,

    /// 최우선 매도 호가
    #[serde(rename = "bap")]
    pub best_ask_price: f64,

    /// 최우선 매도 잔량
    #[serde(rename = "bas")]
    pub best_ask_size: f64,

    /// 최우선 매수 호가
    #[serde(rename = "bbp")]
    pub best_bid_price: f64,

    /// 최우선 매수 잔량
    #[serde(rename = "bbs")]
    pub best_bid_size: f64,

    /// 스트림 타입 (SNAPSHOT : 스냅샷, REALTIME : 실시간)
    #[serde(rename = "st")]
    pub stream_type: String,
}

impl TryFrom<TickerEvent> for NormalizedQuote {
    type Error = ExchangeStreamError;

    fn try_from(ticker: TickerEvent) -> Result<NormalizedQuote, Self::Error> {
        let ask_amount = ticker.acc_ask_volume;
        let ask_price = ticker.trade_price;
        let bid_amount = ticker.acc_bid_volume;
        let bid_price = ticker.trade_price;
        let timestamp = ticker.trade_timestamp as u64 * 1000;

        Ok(NormalizedQuote::new(
            ExchangeName::Upbit,
            &ticker.code,
            timestamp,
            ask_amount,
            ask_price,
            bid_amount,
            bid_price,
        ))
    }
}

impl TryFrom<TradeEvent> for NormalizedQuote {
    type Error = ExchangeStreamError;

    fn try_from(trade: TradeEvent) -> Result<NormalizedQuote, Self::Error> {
        let ask_amount = trade.best_ask_size;
        let ask_price = trade.best_ask_price;
        let bid_amount = trade.best_bid_size;
        let bid_price = trade.best_bid_price;
        let timestamp = trade.trade_timestamp as u64 * 1000;

        Ok(NormalizedQuote::new(
            ExchangeName::Upbit,
            &trade.code,
            timestamp,
            ask_amount,
            ask_price,
            bid_amount,
            bid_price,
        ))
    }
}

impl TryFrom<TradeEvent> for NormalizedTrade {
    type Error = ExchangeStreamError;

    fn try_from(trade: TradeEvent) -> Result<NormalizedTrade, Self::Error> {
        let timestamp = trade.trade_timestamp as u64 * 1000;

        let side = if trade.ask_bid == "ASK" {
            TradeSide::Sell
        } else {
            TradeSide::Buy
        };

        Ok(NormalizedTrade::new(
            ExchangeName::Upbit,
            &trade.code,
            timestamp,
            side,
            trade.trade_price,
            trade.trade_volume,
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "ty")]
pub enum UpbitEvent {
    #[serde(rename = "ticker")]
    Ticker(TickerEvent),
    #[serde(rename = "trade")]
    Trade(TradeEvent),
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum UpbitMessage {
    Event(UpbitEvent),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ticker_event_parsing() {
        let json = r#"{
            "ty": "ticker",
            "cd": "KRW-BTC",
            "op": 50000000.0,
            "hp": 51000000.0,
            "lp": 49000000.0,
            "tp": 50500000.0,
            "pcp": 50000000.0,
            "c": "RISE",
            "cp": 500000.0,
            "scp": 500000.0,
            "cr": 0.01,
            "scr": 0.01,
            "tv": 0.5,
            "atv": 100.0,
            "atv24h": 200.0,
            "atp": 5000000000.0,
            "atp24h": 10000000000.0,
            "tdt": "20230101",
            "ttm": "120000",
            "ttms": 1672531200000,
            "ab": "BID",
            "aav": 50.0,
            "abv": 50.0,
            "h52wp": 60000000.0,
            "h52wdt": "2022-12-01",
            "l52wp": 30000000.0,
            "l52wdt": "2022-06-01",
            "ms": "ACTIVE",
            "mw": "NONE",
            "tms": 1672531200000,
            "st": "REALTIME"
        }"#;

        let parsed: UpbitMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            UpbitMessage::Event(UpbitEvent::Ticker(ticker)) => {
                assert_eq!(ticker.typ, "ticker");
                assert_eq!(ticker.code, "KRW-BTC");
                assert_eq!(ticker.trade_price, 50500000.0);
                assert_eq!(ticker.ask_bid, "BID");
            }
            _ => panic!("Expected Ticker event, got {:?}", parsed),
        }
    }

    #[test]
    fn test_trade_event_parsing() {
        let json = r#"{
            "ty": "trade",
            "cd": "KRW-BTC",
            "tp": 50500000.0,
            "tv": 0.001,
            "ab": "ASK",
            "pcp": 50000000.0,
            "c": "RISE",
            "cp": 500000.0,
            "td": "2023-01-01",
            "ttm": "12:00:00",
            "ttms": 1672531200000,
            "tms": 1672531200000,
            "sid": 123456789,
            "bap": 50500000.0,
            "bas": 1.0,
            "bbp": 50400000.0,
            "bbs": 1.0,
            "st": "REALTIME"
        }"#;

        let parsed: UpbitMessage = serde_json::from_str(json).expect("Failed to parse JSON");
        match parsed {
            UpbitMessage::Event(UpbitEvent::Trade(trade)) => {
                assert_eq!(trade.typ, "trade");
                assert_eq!(trade.code, "KRW-BTC");
                assert_eq!(trade.trade_price, 50500000.0);
                assert_eq!(trade.ask_bid, "ASK");
                assert_eq!(trade.best_ask_price, 50500000.0);
                assert_eq!(trade.best_bid_price, 50400000.0);
            }
            _ => panic!("Expected Trade event, got {:?}", parsed),
        }
    }
}
