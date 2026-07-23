//! Lightweight no-panic smoke for private account text frames (CI-stable).
//! Full libFuzzer target: `fuzz/fuzz_targets/private_account.rs`.
//! No live keys — fixture session machines only.

use marketfeed_adapter_api::SessionInput;
use marketfeed_model::{FrameStamp, TimestampNs};
use marketfeed_private::{
    BinanceSpotUserDataSession, BybitPrivateSession, OkxPrivateSession, PrivateActionBuffer,
    PrivateSessionMachine,
};

fn stamp() -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(1),
        mono_ns: 1,
    }
}

fn drive_private(bytes: &mut [u8]) {
    let mut out = PrivateActionBuffer::new();
    let input = SessionInput::TextFrame {
        bytes,
        received: stamp(),
    };
    let _ = BinanceSpotUserDataSession::new(Default::default()).on_input(input, &mut out);
    out.clear();
    let input = SessionInput::TextFrame {
        bytes,
        received: stamp(),
    };
    let _ = OkxPrivateSession::new(Default::default()).on_input(input, &mut out);
    out.clear();
    let input = SessionInput::TextFrame {
        bytes,
        received: stamp(),
    };
    let _ = BybitPrivateSession::new(Default::default()).on_input(input, &mut out);
}

#[test]
fn private_account_fuzz_smoke_no_panic() {
    let seeds: &[&[u8]] = &[
        b"",
        b"null",
        b"{}",
        br#"{"e":"outboundAccountPosition","E":1,"u":1,"B":[{"a":"BTC","f":"1","l":"0"}]}"#,
        br#"{"e":"balanceUpdate","E":1,"a":"USDT","d":"1.5","T":1}"#,
        br#"{"e":"executionReport","E":1,"s":"BTCUSDT","c":"x","S":"BUY","o":"LIMIT","q":"1","p":"1","x":"NEW","X":"NEW","i":1,"l":"0","z":"0","L":"0","n":"0","N":null,"T":1,"t":-1,"m":false}"#,
        br#"{"subscriptionId":0,"event":{"e":"eventStreamTerminated","E":1}}"#,
        br#"{"arg":{"channel":"account"},"data":[{"details":[{"ccy":"BTC","availBal":"1","frozenBal":"0"}]}]}"#,
        br#"{"topic":"wallet","data":[{"coin":[{"coin":"BTC","walletBalance":"1","locked":"0"}]}]}"#,
        &[0xff, 0xfe, 0x00, b'{', b'}'],
    ];
    for s in seeds {
        let mut buf = s.to_vec();
        drive_private(&mut buf);
    }

    let mut state: u64 = 0xA1_C0_FF_EE_u64;
    let mut buf = [0u8; 64];
    for _ in 0..512 {
        for b in &mut buf {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            *b = (state >> 33) as u8;
        }
        buf[0] = b'{';
        buf[buf.len() - 1] = b'}';
        let len = (state as usize % (buf.len() - 2)) + 2;
        drive_private(&mut buf[..len]);
    }
}
