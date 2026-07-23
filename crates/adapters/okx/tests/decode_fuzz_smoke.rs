//! Lightweight fuzz smoke for OKX Spot JSON decode (CI-stable).
//! Full libFuzzer target: `fuzz/fuzz_targets/venue_decode.rs`.

#[test]
fn okx_decode_fuzz_smoke_no_panic() {
    let seeds: &[&[u8]] = &[
        b"",
        b"null",
        b"ping",
        b"pong",
        br#"{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"1","px":"1","sz":"1","side":"buy","ts":"1"}]}"#,
        // Spot candle*
        br#"{"arg":{"channel":"candle1m","instId":"BTC-USDT"},"data":[["1597026383085","3.721","3.743","3.677","3.708","8422410","22698348.04828491","0"]]}"#,
        br#"{"event":"error","code":"1","msg":"x"}"#,
        &[0xff, 0xfe, 0x00, b'{', b'}'],
        br#"{"data":[{"instId":"X","bidPx":"bad","bidSz":"1","askPx":"1","askSz":"1","ts":"1"}]}"#,
    ];
    for s in seeds {
        let _ = marketfeed_adapter_okx::decode_text(s);
    }

    let mut state: u64 = 0x0C_0F_EE_D1_u64;
    let mut buf = [0u8; 64];
    for _ in 0..1_024 {
        for b in &mut buf {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            *b = (state >> 33) as u8;
        }
        buf[0] = b'{';
        buf[buf.len() - 1] = b'}';
        let len = (state as usize % (buf.len() - 2)) + 2;
        let _ = marketfeed_adapter_okx::decode_text(&buf[..len]);
    }
}
