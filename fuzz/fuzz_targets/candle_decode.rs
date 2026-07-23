//! Candle / kline JSON decode must not panic (shared public `decode_text` paths).
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = marketfeed_adapter_binance::decode_text(data);
    let _ = marketfeed_adapter_binance::decode_coinm_text(data);
    let _ = marketfeed_adapter_okx::decode_text(data);
    let _ = marketfeed_adapter_bybit::decode_text(data);
    let _ = marketfeed_adapter_kraken::decode_text(data);
    let _ = marketfeed_adapter_deribit::decode_text(data);
});
