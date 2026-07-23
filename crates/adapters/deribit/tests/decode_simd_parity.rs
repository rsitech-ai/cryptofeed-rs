//! Offline fixture frames: serde oracle vs active (and simd when featured).
//!
//! No live network. Exercises recorded JSON under `tests/fixtures/`.

use marketfeed_adapter_deribit::{decode_text, decode_text_serde};
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
    for name in ["l2_snapshot.json", "l2_update.json", "l2_update2.json"] {
        let bytes = read_fixture(name);
        assert_eq!(
            decode_text(&bytes).unwrap(),
            decode_text_serde(&bytes).unwrap(),
            "active vs serde oracle diverged on {name}"
        );
    }
}

#[cfg(feature = "simd-json")]
#[test]
fn fixture_frames_serde_simd_canonical_parity() {
    use marketfeed_adapter_deribit::decode_text_simd;

    for name in ["l2_snapshot.json", "l2_update.json", "l2_update2.json"] {
        let bytes = read_fixture(name);
        assert_eq!(
            decode_text_serde(&bytes).unwrap(),
            decode_text_simd(&bytes).unwrap(),
            "serde vs simd diverged on {name}"
        );
    }
}
