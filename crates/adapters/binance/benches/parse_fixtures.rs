//! Fixture parse timings for Binance Spot / USD-M / Coin-M `decode_*_text`.
//!
//! Evidence tool only — not a latency claim and not an enablement gate.
//! Numbers vary by host/CPU; do not paste into maturity docs as SLOs.
//!
//! ```bash
//! cargo bench -p marketfeed-adapter-binance --bench parse_fixtures
//! cargo bench -p marketfeed-adapter-binance --bench parse_fixtures --features simd-json
//! ```
//!
//! # ponytail
//! No criterion dep. Fixed iters + `Instant`. Ceiling = laptop noise / no
//! statistical CI. Local >10% helper: `scripts/parse_fixtures_gate.sh` (evidence
//! only — not maturity/CI). Upgrade = criterion + pinned Linux runner (OPS-A).

use std::path::PathBuf;
use std::time::Instant;

use marketfeed_adapter_binance::{
    decode_coinm_text_serde, decode_text_serde, decode_usdm_text_serde,
};

const ITERS: u32 = 2_000;
const WARMUP: u32 = 50;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn read_fixture(name: &str) -> Vec<u8> {
    std::fs::read(fixtures_dir().join(name)).unwrap_or_else(|e| {
        panic!("read fixture {name}: {e}");
    })
}

fn time_ns(iters: u32, mut f: impl FnMut()) -> u64 {
    for _ in 0..WARMUP {
        f();
    }
    let t0 = Instant::now();
    for _ in 0..iters {
        f();
    }
    t0.elapsed().as_nanos() as u64 / u64::from(iters)
}

fn report(label: &str, ns: u64) {
    println!("{label}: {ns} ns/iter (iters={ITERS})");
}

fn main() {
    let spot = read_fixture("spot_l2_snapshot.json");
    let usdm = read_fixture("usdm_l2_snapshot.json");
    let coinm = read_fixture("coinm_l2_snapshot.json");

    // Sanity: decode succeeds once (fails loud if fixtures regress).
    decode_text_serde(&spot).expect("spot serde");
    decode_usdm_text_serde(&usdm).expect("usdm serde");
    decode_coinm_text_serde(&coinm).expect("coinm serde");

    report(
        "binance_spot_l2_snapshot serde",
        time_ns(ITERS, || {
            let _ = decode_text_serde(&spot).unwrap();
        }),
    );
    report(
        "binance_usdm_l2_snapshot serde",
        time_ns(ITERS, || {
            let _ = decode_usdm_text_serde(&usdm).unwrap();
        }),
    );
    report(
        "binance_coinm_l2_snapshot serde",
        time_ns(ITERS, || {
            let _ = decode_coinm_text_serde(&coinm).unwrap();
        }),
    );

    #[cfg(feature = "simd-json")]
    {
        use marketfeed_adapter_binance::{
            decode_coinm_text_simd, decode_text_simd, decode_usdm_text_simd,
        };

        decode_text_simd(&spot).expect("spot simd");
        decode_usdm_text_simd(&usdm).expect("usdm simd");
        decode_coinm_text_simd(&coinm).expect("coinm simd");

        report(
            "binance_spot_l2_snapshot simd-json",
            time_ns(ITERS, || {
                let _ = decode_text_simd(&spot).unwrap();
            }),
        );
        report(
            "binance_usdm_l2_snapshot simd-json",
            time_ns(ITERS, || {
                let _ = decode_usdm_text_simd(&usdm).unwrap();
            }),
        );
        report(
            "binance_coinm_l2_snapshot simd-json",
            time_ns(ITERS, || {
                let _ = decode_coinm_text_simd(&coinm).unwrap();
            }),
        );
    }

    #[cfg(not(feature = "simd-json"))]
    {
        println!("(simd-json feature off — rebuild with --features simd-json for SIMD timings)");
    }
}
