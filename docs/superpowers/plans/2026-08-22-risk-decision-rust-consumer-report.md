# Standalone Q1 RiskDecision Rust Consumer Report

## Outcome

`marketfeed-event-pulse` now consumes the exact standalone Q1
`risk_decision_v1` family in Rust:

- one published success vector round-trips to the exact canonical JSON and
  content hash;
- all nine published rejection vectors fail at their intended Rust semantic
  rule;
- the standalone schema, golden suite, and rejection suite are verified against
  independent compiled pins and the exact `risk_decision_v1` row in the already
  pinned Q1 contract lock;
- `ContractBundle::load_embedded()` fails closed unless both the historical
  contract provenance and the standalone RiskDecision artifact set verify.

This is schema-consumer proof only. It adds no Risk decision authorship,
adapter, network, filesystem, runtime, persistence, paper, canary, live, order,
execution, or capital authority.

## Historical provenance boundary

The existing `marketfeed-event-pulse-provenance/1.0` manifest,
`PINNED_ARTIFACTS`, and `verify_embedded_contracts()` remain exactly eight
artifacts. The three standalone RiskDecision files use a separate verifier and
are not silently appended to the historical E2 source-lock inventory.

The separate verifier first validates the historical manifest, including the
embedded Q1 lock. It then requires exactly one lock row named
`risk_decision_v1`, compares every path and SHA field to independent compiled
pins, and verifies the three embedded byte lengths and hashes before returning
them.

## TDD evidence

Initial RED command:

```bash
cargo test --locked -p marketfeed-event-pulse --test contract_vectors risk_decision
```

It failed to compile because all three standalone artifact files and both
standalone verifier APIs were absent. After the minimal verifier was added, the
first focused run failed at the exact lock comparison because the golden hash
constant had a transcription error. A diagnostic lock-row equality test showed
the expected value was 63 characters while the lock and file SHA were the same
64-character value. Correcting the compiled pin to the exact lock/file hash
made the focused suite green; no Q1 contract semantic implementation change was
needed.

Final canonical artifacts:

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| `risk_decision_v1.schema.json` | 6,037 | `06a483c06d4186bc05979bcd9f232f0ccc67aee5fbe453ac0e2e9bf74462cf48` |
| `risk_decision_v1_golden.json` | 2,859 | `97eb8772358470a9885797d19c24e9449245823ec42ec28cd8d62e8004bfe984` |
| `risk_decision_v1_rejections.json` | 13,288 | `c706eec6777c9c4f7b6e99db555abf922d5907dc9a2767ade254a36d3c2365a0` |

Repository `.gitattributes` already applies `text eol=lf` to every EventPulse
contract artifact; the contract-vector regression now includes the standalone
three paths in the checkout-attribute proof.

## Validation

- Baseline before edits: full EventPulse GREEN (6 lib, 12 contract, 19 cursor,
  6 feature, 6 window, 11 offline preflight, 9 prospective, 18 replay, 54
  snapshot, 23 wire tests).
- Focused final contract vectors: GREEN (15 tests, including exact standalone
  1 success + 9 rejections).
- Focused final wire regressions: GREEN (23 tests).
- Full current EventPulse: GREEN (7 lib, 15 contract, 19 cursor, 6 feature, 6
  window, 11 offline preflight, 9 prospective, 18 replay, 54 snapshot, 23 wire,
  doc tests).
- Current `cargo clippy --locked -p marketfeed-event-pulse --all-targets
  --all-features -- -D warnings`: GREEN.
- Rust 1.85 full EventPulse: GREEN with `CARGO_INCREMENTAL=0
  RUSTFLAGS='-C debuginfo=0'`.
- Rust 1.85 all-target/all-feature Clippy with `-D warnings`: GREEN under the
  same disk-bounded build settings.
- `cargo fmt --all -- --check`: GREEN.
- `cargo deny --offline --locked check`: GREEN (`advisories ok, bans ok,
  licenses ok, sources ok`).
- No `Cargo.toml` or `Cargo.lock` change.

The first ordinary Rust 1.85 run exhausted the local disk while linking
(`errno=28`), with only 117 MiB free and this worktree's generated `target/`
using 1.8 GiB. Scoped `cargo clean` removed only this worktree's generated
artifacts. The exact Rust 1.85 tests were rerun successfully with incremental
and debug-info output disabled, leaving source and dependency semantics
unchanged.

## Residual boundary

This commit is a local repo-ready Rust consumer candidate. The root
cross-language receipt must not mark `risk_decision_v1` Rust coverage verified
until this proof is independently accepted and default-branch-reachable, then
bound by exact commit/path/hash evidence in a separate root integration change.
Historical E2 source-lock provenance remains unchanged, and E2 remains
`IN_PROGRESS / blocked:fixture-provenance`.
