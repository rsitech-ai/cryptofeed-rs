//! Offline frames: serde oracle vs active (and simd when featured).
//!
//! No live network. Uses inline V5 public JSON (shared across linear/spot/inverse).

use marketfeed_adapter_bybit::{decode_text, decode_text_serde};

const FRAMES: &[&[u8]] = &[
    br#"{"success":true,"ret_msg":"","op":"subscribe"}"#,
    br#"{"op":"pong"}"#,
    br#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1000,"data":[{"T":1001,"s":"BTCUSDT","S":"Sell","v":"0.01","p":"65000.5","L":"MinusTick","i":"abc-1","seq":9}]}"#,
    br#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1,"data":{"s":"BTCUSDT","b":[["100.00","1"]],"a":[["101.00","2"]],"u":10,"seq":100}}"#,
    br#"{"topic":"kline.1.BTCUSDT","type":"snapshot","ts":1672324988887,"data":[{"start":1672324800000,"end":1672324859999,"interval":"1","open":"16649.5","close":"16695","high":"16699","low":"16642","volume":"2.081","turnover":"34666.4005","confirm":true,"timestamp":1672324859999}]}"#,
    br#"{"topic":"tickers.BTCUSDT","type":"snapshot","ts":1672376495650,"data":{"symbol":"BTCUSDT","markPrice":"16595.00","indexPrice":"16596.54","fundingRate":"0.0001","nextFundingTime":"1672387200000","openInterest":"458153.0"}}"#,
    br#"{"topic":"allLiquidation.BTCUSDT","type":"snapshot","ts":1739502303204,"data":[{"T":1739502302929,"s":"BTCUSDT","S":"Sell","v":"0.01","p":"65000.5"}]}"#,
];

#[test]
fn fixture_frames_active_matches_serde_oracle() {
    for (i, bytes) in FRAMES.iter().enumerate() {
        assert_eq!(
            decode_text(bytes).unwrap(),
            decode_text_serde(bytes).unwrap(),
            "active vs serde oracle diverged on frame {i}"
        );
    }
}

#[cfg(feature = "simd-json")]
#[test]
fn fixture_frames_serde_simd_canonical_parity() {
    use marketfeed_adapter_bybit::decode_text_simd;

    for (i, bytes) in FRAMES.iter().enumerate() {
        assert_eq!(
            decode_text_serde(bytes).unwrap(),
            decode_text_simd(bytes).unwrap(),
            "serde vs simd diverged on frame {i}"
        );
    }
}
