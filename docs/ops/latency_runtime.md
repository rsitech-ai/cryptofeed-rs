# Latency runtime profile

Spec: §13.2 in
[`production_rust_multi_exchange_market_data_spec.md`](../spec/production_rust_multi_exchange_market_data_spec.md).

## Profiles

| Profile | Config | Behavior today |
|---|---|---|
| **portable** (default) | `engine.runtime_profile = "portable"` | Tokio multi-thread; no affinity |
| **latency** | `engine.runtime_profile = "latency"` | Same decode/normalize path; affinity hooks available (no default core map) |

Normalized market/account results MUST match between profiles.

## Affinity

| Platform | `pin_worker_to_core(None)` | `pin_worker_to_core(Some(n))` |
|---|---|---|
| **Linux** | no-op `Ok(())` | real `sched_setaffinity` for the calling thread; invalid core / OS error → `Err` (safe fallback, stay unpinned) |
| **Other** | no-op `Ok(())` | portable no-op `Ok(())` |

`apply_runtime_profile(Latency)` does **not** auto-pin workers (no connection→core
map yet). Call `pin_worker_to_core(Some(n))` from a worker thread when operators
want a pin.

## Code

- `marketfeed_engine::RuntimeProfile` / `apply_runtime_profile` /
  `pin_worker_to_core`
- Cargo feature `latency-runtime` on `marketfeed-engine` — intent flag only
  (no extra deps; mirrors sinks `kafka`/`nats`)
- Linux-only `libc` dep for `sched_setaffinity` (cfg-gated)

```toml
# config
[engine]
runtime_profile = "latency"
```

```bash
cargo build -p marketfeed-engine --features latency-runtime
```

## C10 — optional `simd-json` (hot-path public adapters)

| Item | Today |
|---|---|
| Default | `serde_json` on all adapters |
| Features | `marketfeed-adapter-{binance,okx,bybit,kraken,deribit}` / `simd-json` |
| Scope | Binance Spot + USD-M + Coin-M; OKX Spot/SWAP/Futures; Bybit linear/spot/inverse (shared V5 `decode_text`); Kraken Spot; Deribit JSON-RPC — via per-crate `json` helpers |
| Parity | `decode_text_serde` vs `decode_text_simd` (and USD-M/Coin-M aliases) must match; unit fixtures + `tests/decode_simd_parity.rs` on recorded/inline frames (no live network) |

```bash
cargo test -p marketfeed-adapter-binance
cargo test -p marketfeed-adapter-binance --features simd-json
cargo test -p marketfeed-adapter-okx
cargo test -p marketfeed-adapter-okx --features simd-json
cargo test -p marketfeed-adapter-bybit
cargo test -p marketfeed-adapter-bybit --features simd-json
cargo test -p marketfeed-adapter-kraken
cargo test -p marketfeed-adapter-kraken --features simd-json
cargo test -p marketfeed-adapter-deribit
cargo test -p marketfeed-adapter-deribit --features simd-json
```

**Enable criteria:** turn the feature on in a latency binary only after
`parse_*` histograms / a focused profile show JSON parse as the bottleneck.
Do **not** enable in portable public defaults. No insecure TLS involved
(transport remains rustls/webpki). **No enablement claim from timings alone.**

### §24 — parse fixture evidence tool (not enablement)

Harness: `crates/adapters/binance/benches/parse_fixtures.rs` — fixed iters +
`std::time::Instant` (no `criterion` dep). Prints ns/iter for Spot / USD-M /
Coin-M L2 snapshot fixtures under serde default and, with `--features simd-json`,
the SIMD decode path.

```bash
cargo bench -p marketfeed-adapter-binance --bench parse_fixtures
cargo bench -p marketfeed-adapter-binance --bench parse_fixtures --features simd-json
```

| Kind | Rule |
|---|---|
| **Smoke target (minimal)** | Both commands exit 0; each path prints three `ns/iter` lines (spot/usdm/coinm). Decode sanity fails loud if fixtures regress. |
| **Comparison criterion (evidence only)** | On one host, record §24.1 fields (CPU, OS, rustc, build flags, profile, parser backend). Run each command ≥3 times; take median ns/iter per fixture. SIMD is a *candidate* for a latency binary only when median SIMD is ≥20% faster than median serde on **all three** fixtures **and** live/operator `parse_*` histograms still show JSON parse as the bottleneck. |
| **Local regression helper (W4-P1c)** | `scripts/parse_fixtures_gate.sh` + `docs/ops/parse_fixtures_baseline.txt`. Runs the Instant harness `RUNS` times (default 3), takes median ns/iter per label, fails if any label is **>10%** slower than the baseline (`THRESHOLD_PCT`, default 10). Optional `--simd` / `--simd-vs-serde` also gates simd medians vs baseline and vs same-run serde medians. |
| **Not a maturity / CI gate** | Local evidence only. No published SLO, no maturity unlock, no auto-enable from laptop noise. **Do not** wire into Actions while billing is blocked (OPS-A). Remote/CI timing assertion waits for pinned runners. |
| **Refresh baseline** | Absolute ns/iter are host-local. On a new machine: `./scripts/parse_fixtures_gate.sh --write-baseline` (add `--simd` if you gate SIMD labels). |
| **Upgrade path** | `criterion` + pinned Linux runner when OPS needs a statistical / CI regression gate. |

```bash
./scripts/parse_fixtures_gate.sh --self-check
./scripts/parse_fixtures_gate.sh
./scripts/parse_fixtures_gate.sh --simd --simd-vs-serde
./scripts/parse_fixtures_gate.sh --write-baseline
```

CI job `simd-json` (YAML ready; Actions billing may block) runs
`cargo test -p <adapter> --features simd-json` for all public JSON adapters and
one Binance `parse_fixtures` bench run (compile + print — timings **not** gated).

**ponytail ceiling:** optional parse path + parity + CI feature matrix +
fixture Instant harness + documented comparison criterion + **local** >10%
baseline script. True C10 finish for *enablement* still needs operator profiles
under realistic load (OPS / laptop). Synthetic adapter has no JSON `decode_text`
— skipped.

## Ceiling / upgrade

- **Now:** Linux affinity implemented for explicit pins; non-Linux no-op; no
  automatic shard / connection→core mapping. Optional `simd-json` on public
  JSON adapters (Binance family, OKX, Bybit, Kraken Spot, Deribit) behind
  feature + parity tests (default still serde).
- **Later:** current-thread runtime shards, deterministic connection→core map,
  operator profile gate before enabling in latency binaries. Do not enable
  `target-cpu=native` in public portable binaries. Fixture `parse_fixtures`
  harness + §24 comparison criterion (≥20% median SIMD win on all three
  fixtures, plus operator bottleneck proof) + local `parse_fixtures_gate.sh`
  are evidence only — not a maturity/CI gate until pinned runners + OPS-A.
