//! Lightweight fuzz smoke for Binance Spot + Coin-M JSON decode (CI-stable).
//! Full libFuzzer targets: `fuzz/fuzz_targets/{venue,candle,coinm}_decode.rs`.

#[test]
fn binance_decode_fuzz_smoke_no_panic() {
    let seeds: &[&[u8]] = &[
        b"",
        b"null",
        b"[]",
        b"{}",
        br#"{"e":"trade","s":"BTCUSDT","t":1,"p":"1.0","q":"1","T":1,"m":true}"#,
        br#"{"stream":"x","data":{"u":1,"s":"BTCUSDT","b":"1","B":"1","a":"2","A":"1"}}"#,
        // Spot candle / kline
        br#"{"e":"kline","E":1,"s":"BTCUSDT","k":{"t":0,"T":1,"s":"BTCUSDT","i":"1m","f":1,"L":2,"o":"1","c":"2","h":"3","l":"0.5","v":"10","n":1,"x":true,"q":"1","V":"5","Q":"1","B":"0"}}"#,
        &[0xff, 0xfe, 0x00, b'{', b'}'],
        br#"{"e":"trade","s":"X","t":1,"p":"not-a-decimal","q":"1","T":1,"m":false}"#,
    ];
    for s in seeds {
        let _ = marketfeed_adapter_binance::decode_text(s);
    }

    let mut state: u64 = 0xB1_A4_CE_11_u64;
    let mut buf = [0u8; 64];
    for _ in 0..1_024 {
        for b in &mut buf {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            *b = (state >> 33) as u8;
        }
        buf[0] = b'{';
        buf[buf.len() - 1] = b'}';
        let len = (state as usize % (buf.len() - 2)) + 2;
        let _ = marketfeed_adapter_binance::decode_text(&buf[..len]);
    }
}

#[test]
fn binance_coinm_decode_fuzz_smoke_no_panic() {
    let seeds: &[&[u8]] = &[
        b"",
        b"{}",
        // Coin-M depthUpdate (`pu`) + REST snapshot shape
        br#"{"e":"depthUpdate","E":1,"s":"BTCUSD_PERP","U":2,"u":3,"pu":1,"b":[["1","1"]],"a":[["2","1"]]}"#,
        br#"{"lastUpdateId":10,"bids":[["1","1"]],"asks":[["2","0"]]}"#,
        // Coin-M candle
        br#"{"e":"kline","E":1,"s":"BTCUSD_PERP","k":{"t":0,"T":1,"s":"BTCUSD_PERP","i":"1m","f":1,"L":2,"o":"1","c":"2","h":"3","l":"0.5","v":"10","n":1,"x":false,"q":"1","V":"5","Q":"1","B":"0"}}"#,
        br#"{"u":9,"s":"BTCUSD_PERP","b":"65000.0","B":"1","a":"65000.1","A":"2"}"#,
        br#"{"e":"forceOrder","E":5,"o":{"s":"BTCUSD_PERP","S":"SELL","o":"LIMIT","f":"IOC","q":"1","p":"9900","ap":"9910","X":"FILLED","l":"1","z":"1","T":5}}"#,
        br#"{"symbol":"BTCUSD_PERP","openInterest":"10659.509","time":1589437530011}"#,
        br#"{"e":"aggTrade","E":1,"s":"BTCUSD_PERP","a":1,"p":"bad","q":"1","f":1,"l":1,"T":1,"m":true}"#,
        &[0xff, 0xfe, 0x00, b'{', b'}'],
    ];
    for s in seeds {
        let _ = marketfeed_adapter_binance::decode_coinm_text(s);
    }

    let mut state: u64 = 0xC0_1F_AD_E0_u64;
    let mut buf = [0u8; 64];
    for _ in 0..1_024 {
        for b in &mut buf {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            *b = (state >> 33) as u8;
        }
        buf[0] = b'{';
        buf[buf.len() - 1] = b'}';
        let len = (state as usize % (buf.len() - 2)) + 2;
        let _ = marketfeed_adapter_binance::decode_coinm_text(&buf[..len]);
    }
}
