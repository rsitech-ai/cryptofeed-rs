# Chaos / supply-chain leftovers (WP-I)

Ponytail ceilings for the full chaos suite; this branch lands the minimum that
unblocks release hygiene without pulling Loom or nightly into default CI.

## Unit chaos harness (offline)

`crates/engine/tests/chaos_harness.rs` covers injected:

| Case | Proof |
|---|---|
| Malformed WS text | `ParseError` + parse_failures metric (no panic) |
| Snapshot HTTP fail / bad body | `ParseError` + reconnect on HTTP non-200 |
| Slow sink overflow | `EventsDropped` on dispatch Drop* + recording queue |
| Wall clock jump | `wall_clock_jump_delta` (+ live loop uses `Instant` mono) |
| Timer jump | `poll_timers` fires after large `now` skew |

Multi-day soak / live disconnect injection remain **OPS**.

## Fuzzing

| Target | Path | CI |
|---|---|---|
| `Fixed::parse_decimal` | `fuzz/fuzz_targets/fixed_parse.rs` + model unit smoke | smoke in `cargo test` |
| Venue `decode_text` (+ Coin-M) | `fuzz/fuzz_targets/venue_decode.rs` + adapter smokes | smoke in `cargo test` |
| Candle / kline paths | `fuzz/fuzz_targets/candle_decode.rs` + Spot/OKX candle seeds | smoke in `cargo test` |
| Binance Coin-M depth/kline | `fuzz/fuzz_targets/coinm_decode.rs` + Coin-M smoke | smoke in `cargo test` |
| Private account frames | `fuzz/fuzz_targets/private_account.rs` + private smoke | smoke in `cargo test` |
| Raw recording reader | `fuzz/fuzz_targets/recording_reader.rs` + recording smoke | smoke in `cargo test` |
| L2 book transitions | `fuzz/fuzz_targets/book_transition.rs` + book smoke | smoke in `cargo test` |

Run libFuzzer (nightly + `cargo-fuzz`; not required for PR green):

```bash
cargo install cargo-fuzz
cargo +nightly fuzz run fixed_parse --fuzz-dir fuzz
cargo +nightly fuzz run venue_decode --fuzz-dir fuzz
cargo +nightly fuzz run candle_decode --fuzz-dir fuzz
cargo +nightly fuzz run coinm_decode --fuzz-dir fuzz
cargo +nightly fuzz run private_account --fuzz-dir fuzz
cargo +nightly fuzz run recording_reader --fuzz-dir fuzz
cargo +nightly fuzz run book_transition --fuzz-dir fuzz
```

Ceiling: live disconnect / disk-full inject remain **OPS**. Offline disk-full
unit: recording `FailEngine` + probe → `DiskFull`; spill WAL limit → fail-closed. C9 CI matrix
YAML includes Windows + Linux aarch64 + `cargo doc` (remote runners may still
be billing-blocked). Upgrade = one fuzz target per decoder listed in spec §27.5.

## Concurrency / overflow permutations

`marketfeed-dispatch::BoundedQueue` is single-owner (`&mut self` push/pop). There
is no shared mutable state to model under Loom today.

- **Now:** deterministic push/pop permutation unit tests in `crates/dispatch`
  (fixed op sequences, not full `n!`) plus policy-survivor oracle.
- **Also:** `chaos_harness.rs` (malformed / snapshot-fail / slow-sink / clock+timer)
  and ActionBuffer DropNewest → `EventsDropped` in `foundation_hardening`.
- **Disk-full (unit):** recording pipeline free-space probe + `FailEngine` →
  `RecordingError::DiskFull`; `SpillWalSink` WAL limit → fail-closed +
  `EventsDropped` / `DiskPressure` (tempdir tests). Live inject remains **OPS**.
- **Upgrade:** Loom (or shuttle) when a concurrent / sharded queue lands;
  live disconnect remains **OPS**.

Ceiling documented for auditors: unit chaos ≠ §3.8 soak matrix.

## Licenses

Workspace claims `Apache-2.0 OR MIT`. Text files:

- `LICENSE-APACHE`
- `LICENSE-MIT`

## SBOM

```bash
cargo install cargo-cyclonedx --locked
./scripts/generate-sbom.sh
```

| Trigger | Workflow | Notes |
|---|---|---|
| PR / push `main` | `ci.yml` job `sbom` | Advisory (`continue-on-error`); artifact `marketfeed-sbom` |
| Tag `v*` | `release.yml` job `sbom` | Artifact `marketfeed-sbom-<tag>`; may not run while Actions billing blocks jobs |
| Tag `v*` | `release.yml` job `attest` (enabled, hard-fail) | YAML ready; publish blocked while Actions billing prevents job start; see [`release_provenance.md`](../runbooks/release_provenance.md) |

Fallback if the Rust plugin is undesirable on the runner:

```bash
syft dir:. -o cyclonedx-json > sbom/marketfeed.cdx.json
```

Ceiling: SBOM artifact + attest job **enabled** in YAML — no published signed
provenance yet (spec §29); upgrade = unblock Actions billing, tag `v0.0.x-rc`,
and archive `gh attestation verify` output per the provenance runbook.
