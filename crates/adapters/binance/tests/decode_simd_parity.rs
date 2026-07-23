//! Offline fixture frames: serde oracle vs active (and simd when featured).
//!
//! No live network. Exercises recorded JSON under `tests/fixtures/`.

use marketfeed_adapter_binance::{
    decode_coinm_text, decode_coinm_text_serde, decode_text, decode_text_serde, decode_usdm_text,
    decode_usdm_text_serde,
};
use std::path::PathBuf;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn read_fixture(name: &str) -> Vec<u8> {
    std::fs::read(fixtures_dir().join(name)).unwrap_or_else(|e| {
        panic!("read fixture {name}: {e}");
    })
}

#[test]
fn fixture_frames_active_matches_serde_oracle() {
    let spot = read_fixture("spot_l2_snapshot.json");
    assert_eq!(
        decode_text(&spot).unwrap(),
        decode_text_serde(&spot).unwrap()
    );

    let usdm = read_fixture("usdm_l2_snapshot.json");
    assert_eq!(
        decode_usdm_text(&usdm).unwrap(),
        decode_usdm_text_serde(&usdm).unwrap()
    );

    let coinm = read_fixture("coinm_l2_snapshot.json");
    assert_eq!(
        decode_coinm_text(&coinm).unwrap(),
        decode_coinm_text_serde(&coinm).unwrap()
    );
}

#[cfg(feature = "simd-json")]
#[test]
fn fixture_frames_serde_simd_canonical_parity() {
    use marketfeed_adapter_binance::{
        decode_coinm_text_simd, decode_text_simd, decode_usdm_text_simd,
    };

    let spot = read_fixture("spot_l2_snapshot.json");
    assert_eq!(
        decode_text_serde(&spot).unwrap(),
        decode_text_simd(&spot).unwrap()
    );

    let usdm = read_fixture("usdm_l2_snapshot.json");
    assert_eq!(
        decode_usdm_text_serde(&usdm).unwrap(),
        decode_usdm_text_simd(&usdm).unwrap()
    );

    let coinm = read_fixture("coinm_l2_snapshot.json");
    assert_eq!(
        decode_coinm_text_serde(&coinm).unwrap(),
        decode_coinm_text_simd(&coinm).unwrap()
    );
}
