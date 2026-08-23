# EventPulse E2 Mechanics V2 Consumer Report

## Outcome

Implemented an additive, pure/offline consumer for the accepted EventPulse admission/2.0 and MechanicsInput/2.0 contracts. The result pins the exact root topology and wire blobs independently, retains explicit MARKET cursor and venue provenance in the V2 hash, delegates non-MARKET records byte-exactly to V1, and partitions a complete 15-record/12-source input into nine deterministic in-memory PreflightV4 artifacts with canonical truthful-empty SYSTEM.

The cursor boundary is a separate `SourceStateMachineV2` keyed by exact contributor and family. This was required because PUBLIC QUOTE/BOOK and MARKET TRADE/OI/LIQ intentionally mix derived and native cursor modes under one contributor. V1 maps and APIs remain unchanged. The V2 machine shares checked connection/contributor lifecycle, fans out invalidating SYSTEM effects, enforces cross-family availability monotonicity, and exposes no aggregate cursor or snapshot integration.

Status remains `UNVERIFIED`, `blocked:fixture-provenance`, and `evidence_authoring_allowed() == false`.

## Exact Contract Pins

- Topology: 6,955 LF bytes, SHA-256 `7216d9c5bc4b5bcd463b644c53309594413608586288beb0d32623412a42f0d7`.
- Wire: 10,119 LF bytes, SHA-256 `dc79576062caf952be44e4808359c4328c2976282291838973d4884fadafa50b`.
- Root quote: 1,065 canonical bytes, SHA-256 `d08849ba74b54ef02fa62308be8e16f3af7d300c7bd7c092d92ec3dfdfcfe846`, payload hash `3763341032b451fedc399d27b192ba2583dd0edb4d01e247a98d839db57cfa5e`.
- The historical EventPulse provenance manifest was not modified.

## TDD Evidence

Initial RED signals included:

- missing embedded prospective contract files;
- unresolved `OfflineArtifactPreflightV4` import;
- the original sub-millisecond test fixture flooring MARKET time before its admitted start;
- mixed cursor modes under the V1 contributor-keyed cursor slot;
- stale family cursor visibility after invalidating aggregate lifecycle events;
- Rust 1.85/current clippy `large_enum_variant` failures.

GREEN regressions now cover:

- exact embedded contract and root quote bytes/hashes;
- strict unique/canonical V2 JSON and rehashed semantic drift;
- full-u64 QUOTE provenance independent from derived cursor mode;
- exact V1 non-MARKET serialization and writer strict readback;
- PUBLIC QUOTE/BOOK in both orders and all MARKET TRADE/OI/LIQ permutations;
- family-isolated duplicate mutation/native gap handling and contributor-wide availability regression;
- contributor, connection, and processor SYSTEM fanout; terminal epoch reuse; greater-generation sibling clearing semantics;
- MARKET selected-source/exchange/receive, CLOCK observed/available, and COVERAGE from/through/available capture-start bounds;
- missing topology, duplicate record, and any nonempty SYSTEM rejection;
- complete 15-record/12-source deterministic nine-artifact build and strict readback.

## Validation

Passed:

- `cargo test -p marketfeed-event-pulse` on current Rust (full crate, including unchanged V1/V3 and 54 snapshot tests).
- `CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 CARGO_PROFILE_TEST_DEBUG=0 cargo +1.85.0 test -p marketfeed-event-pulse` (full crate).
- `cargo test -p marketfeed-event-pulse --test wire_v2_regressions --test replay_v2 --test cursor_v2 --test prospective_v2 --test preflight_v4` after final internal representation changes.
- `cargo clippy -p marketfeed-event-pulse --all-targets --all-features -- -D warnings` on current Rust.
- `cargo +1.85.0 clippy -p marketfeed-event-pulse --all-targets --all-features -- -D warnings`.
- `cargo +1.85.0 check -p marketfeed-event-pulse --all-targets --all-features` after final changes.
- `cargo deny --offline --locked check` (`advisories ok, bans ok, licenses ok, sources ok`).
- `cargo fmt --all -- --check`.
- `git diff --check`.

One Rust 1.85 rebuild initially failed with `No space left on device` while compiling `ring`. This was an environment-capacity failure, not a test failure. Only this isolated worktree's generated `target` directory was cleaned; the same full Rust 1.85 suite then passed.

## Authority and Residuals

- No adapter, MFR1, engine, snapshot, capture, filesystem, network, environment, package/manifest writer, producer, source qualification, evidence authorship, paper/canary/live, order, risk, execution, promotion, or capital authority was added.
- PreflightV4 is in-memory only and SYSTEM is truthful-empty only.
- MechanicsInputV2 is intentionally not labeled EPIN2.
- A genuine completion fixture and its provenance remain external work: `blocked:fixture-provenance`.
