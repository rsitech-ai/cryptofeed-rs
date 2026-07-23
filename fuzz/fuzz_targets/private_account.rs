//! Private account text frames must not panic (fixture SMs; no live keys).
#![no_main]

use libfuzzer_sys::fuzz_target;
use marketfeed_adapter_api::SessionInput;
use marketfeed_model::{FrameStamp, TimestampNs};
use marketfeed_private::{
    BinanceSpotUserDataSession, BybitPrivateSession, OkxPrivateSession, PrivateActionBuffer,
    PrivateSessionMachine,
};

fuzz_target!(|data: &[u8]| {
    let mut bytes = data.to_vec();
    let stamp = FrameStamp {
        receive_ts: TimestampNs(1),
        mono_ns: 1,
    };
    let mut out = PrivateActionBuffer::new();
    let _ = BinanceSpotUserDataSession::new(Default::default()).on_input(
        SessionInput::TextFrame {
            bytes: &mut bytes,
            received: stamp,
        },
        &mut out,
    );
    out.clear();
    let _ = OkxPrivateSession::new(Default::default()).on_input(
        SessionInput::TextFrame {
            bytes: &mut bytes,
            received: stamp,
        },
        &mut out,
    );
    out.clear();
    let _ = BybitPrivateSession::new(Default::default()).on_input(
        SessionInput::TextFrame {
            bytes: &mut bytes,
            received: stamp,
        },
        &mut out,
    );
});
