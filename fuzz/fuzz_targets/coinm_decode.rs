//! Binance Coin-M decode (depth `pu` / snapshot / kline / aggTrade) must not panic.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = marketfeed_adapter_binance::decode_coinm_text(data);
});
