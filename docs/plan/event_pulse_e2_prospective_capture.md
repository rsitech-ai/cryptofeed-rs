# EventPulse E2 Prospective Capture ExecPlan

## Task

- Objective: add a deterministic, fail-closed Rust admission contract for a
  future real public-read-only EventPulse E2 prospective fixture capture.
- Owner repository: `cryptofeed-rs`.
- Provenance boundary: root amendment merge
  `24b51a58c670ab722538bec4a3e1def0278b1107`, observed default-reachable at
  `2026-08-22T07:35:52Z`.
- Capital impact: research-only / SENSE-only. No private feed, credential,
  account, order, execution, paper, canary, live-trading, or capital authority.

## Context

The accepted EventPulse mechanics crate already owns strict `MechanicsInputV1`
authoring/parsing, canonical EPIN-JSON1 persistence, and deterministic snapshot
replay. The daemon already owns public venue I/O and can persist raw MFR1 plus
normalized MFPE-JSON1 events. The merged root amendment permits a new capture
only when it begins strictly after the reachability boundary above and exposes
exactly nine evidence roles: TRADE, QUOTE, BOOK, OPEN_INTEREST, LIQUIDATION,
CONFIRMATION, CLOCK, COVERAGE, and SYSTEM.

The missing historical August-19 bytes remain unavailable. This work must not
reconstruct, backfill, synthesize, or relabel them.

## Hypothesis and Assumptions

- A bounded public derivatives session can supply real trade, quote, book, open
  interest, and liquidation observations for one instrument, while a second
  public venue supplies same-instrument confirmation evidence.
- Daemon receive timestamps are the authoritative availability clock for market
  records. Exchange timestamps are source event time only.
- CLOCK is independent positive evidence. It cannot be inferred from exchange,
  receive, normalized, or availability timestamps already carried by a market
  record. An admitted attempt must bind an explicit bounded UTC/monotonic clock
  sidecar and its immutable producer source.
- COVERAGE is independent positive evidence. Absence of a detected gap is not a
  coverage heartbeat. An admitted attempt must bind an explicit bounded
  heartbeat/range tracker sidecar and its immutable producer source.
- SYSTEM must bind an actually observed daemon/session system transition. A
  healthy transition cannot be silently converted into a fault; if no
  role-compatible system record exists, package finalization fails.
- Liquidation is event-driven and may be absent in a bounded attempt. Absence is
  a truthful incomplete capture, never a zero-valued fabricated record.

## Constraints

- Technical:
  - Rust 1.85 MSRV; no floating-point mechanics or timestamp conversion.
  - Reuse the accepted wire/replay types and existing public daemon transports.
  - Pure package construction stays separate from network/file orchestration.
  - Every input passes `MechanicsInputV1::from_epin_json` before publication.
  - Every artifact is canonical newline-terminated EPIN-JSON1 and written via a
    temporary directory followed by an atomic directory rename.
  - Exact source IDs, contributor identities, epochs, generations, and cursor
    order are validated; no implicit fallback identities.
- Operational:
  - Public endpoints only; no environment credentials are read.
  - Capture duration and record count are bounded.
  - Partial attempts remain outside the final package path and are diagnosable.
  - Transformation commit must be immutable and default-reachable before the
    evidence-bearing capture begins.
- Risk/capital:
  - No order API, authenticated transport, private session, wallet, account,
    position, Risk, allocation, execution, paper, canary, or live path.
  - Output authority literals remain false and source qualification remains
    `UNVERIFIED`.

## Options Considered

1. Add a live EventPulse sink directly to every daemon venue loop.
   - Pros: one process and no intermediate normalized file.
   - Cons: couples evidence admission to live I/O, makes partial-package
     rollback harder, and risks emitting a structurally complete package before
     all rare roles exist.
2. Use authoritative MFR1 plus explicit confirmation, clock, coverage, and
   system sources, then run a pure offline prospective package transformer.
   - Pros: preserves immutable source bytes, separates I/O from deterministic
     transformation, makes replay and source locking reproducible, and fails
     closed on missing roles.
   - Cons: requires a small explicit capture metadata/config contract and a
     second command after capture.

## Chosen Approach

Choose option 2 in bounded stages. The initial slice adds only a pure checked
admission descriptor to `marketfeed-event-pulse`. It must reject normalized-only
capture, same-venue or unsupported confirmation, inferred clock/coverage, absent
system mapping, mutable source references, pre-reachability starts, and any
authority escalation. It does not produce a fixture and cannot clear the
fixture-provenance blocker.

The eventual capture architecture is authoritative MFR1 plus a public
Hyperliquid confirmation source, explicit clock and coverage sidecars, and a
stable system-fault mapper, followed by an offline deterministic transformer.
Those sources must be implemented, reviewed, merged, and default-reachable
before an evidence-bearing capture starts. E3 remains blocked and is not
implicitly authorized by this E2 admission slice.

## File Scope

- `crates/event-pulse/src/prospective.rs`: immutable checked admission
  descriptor and exact blocker reasons.
- `crates/event-pulse/src/lib.rs`: minimal public exports.
- `crates/event-pulse/tests/prospective_capture.rs`: RED/GREEN boundary tests.
- `docs/plan/event_pulse_e2_prospective_capture.md`: this plan and final notes.

No daemon, adapter, transport, private-session, sink, or execution module is in
the initial implementation scope. If the existing normalized file format lacks
an identity required by strict EPIN authoring, stop and amend this plan rather
than inventing it.

## Execution Plan

1. RED-test an input-only admission descriptor with exact nine-role inventory,
   immutable SHA-256 source bindings, strict post-reachability start, public
   Binance primary MFR1, distinct public Hyperliquid confirmation, explicit
   clock/coverage sidecars, stable system mapping, and false authority literals.
2. Implement the smallest checked immutable types and stable rejection reasons.
3. RED-test coordinated drift and every false-evidence shortcut: normalized-only
   input, Binance/OKX/Kraken confirmation, market-derived clock, gap-absence
   coverage, absent system mapping, mutable refs, missing hashes, and authority.
4. Run focused and crate quality gates plus independent exact-commit review.
5. Keep the prospective fixture blocker active. Plan the external-source and
   offline-transformer slices separately after this guard is accepted.

## Test Plan

- RED/GREEN unit and integration:
  - `cargo test --locked -p marketfeed-event-pulse --test prospective_capture`
  - `cargo test --locked -p marketfeed-event-pulse`
- Compatibility:
  - `cargo test --locked -p marketfeed-engine --test record_replay`
  - root prospective validator against a test-produced package
- Quality:
  - `cargo fmt --check`
  - `cargo clippy --locked -p marketfeed-event-pulse --all-targets -- -D warnings`
  - `cargo +1.85 test --locked -p marketfeed-event-pulse --lib --tests`
  - `cargo deny --offline --locked check`
  - `git diff --check`
- Future real capture gate, only after all producer slices merge:
  - public-read-only daemon capture with bounded duration/count
  - package transformation and root validator
  - strict EPIN ingestion and deterministic snapshot hash
  - independent source/package lock review

## Invalidating Results

- Any required role can only be produced by fabrication or a private endpoint.
- The normalized recording omits identity/cursor/availability evidence needed
  by strict EPIN and raw MFR1 cannot supply it deterministically.
- Package bytes differ across two transformations of identical captured input.
- Any path reaches credentials, private sessions, orders, accounts, positions,
  execution, paper, canary, or live-trading authority.
- The real public attempt lacks liquidation, confirmation, or a truthful system
  observation within its bounded capture window. That attempt remains
  incomplete; it does not weaken the contract.

## Risks and Rollback

- Public venue terms or event sparsity may prevent a complete bounded capture.
- Clock/coverage inference from market records would overclaim positive
  evidence; admission rejects it even when the market record is hash-bound.
- A normalized-only input is weaker than raw MFR1 provenance; the eventual
  source lock must bind both raw and normalized capture artifacts.
- Rollback is a revert of this additive crate/CLI/docs slice. Partial packages
  remain in a temporary directory and are recoverably removable. No service,
  credential, order, or capital state exists to roll back.

## Memory Impact

Record only the durable two-stage capture boundary, exact commands that prove
strict package admission, and any confirmed public-source limitation. Do not
record transient market bytes or endpoint logs in durable memory.

## Final Notes

- Implementation: added a pure checked admission guard. It requires the exact
  root reachability boundary, exact nine-role order, authoritative Binance MFR1
  primary input, distinct Hyperliquid MFR1 confirmation, independent clock and
  coverage sidecars, stable system mapping, immutable lowercase Git/blob
  hashes, and six false authority literals. Unknown fields fail closed.
- Truth boundary: even a valid topology returns
  `evidence_authoring_allowed() == false` and
  `blocked:fixture-provenance`. No fixture writer, network client, adapter,
  daemon, capture, package, or execution path was added.
- Validation: focused RED failed on the missing API; reviewer counterexamples
  then failed on unbound topology, noncanonical timestamps, and a non-replayable
  clock/coverage/system layout, a parallel confirmation-family vocabulary, and
  missing venue-native symbols/connections; GREEN passed 8 prospective tests
  by constructing the canonical `MechanicsConfigV1` from canonical instrument,
  contributor, clock, coverage, connection, and system keys. Full current
  and Rust 1.85 EventPulse successor suites each passed 152 tests
  (6 library + 12 contract + 19 cursor + 6 feature + 6 window + 8 prospective
  + 18 replay + 54 snapshot + 23 wire). Clippy and formatting passed.
- Tradeoff: this slice deliberately stops before the dedicated capture crate,
  Hyperliquid adapter, clock/coverage producers, and MFR1 transformer. Those
  are separately reviewed E3-adjacent work and cannot be inferred from a valid
  descriptor.
- Rollback: revert the additive module, tests, export, and this plan. There is
  no external or capital state to unwind.
- Evidence capture: remains blocked until all producer slices are accepted,
  merged, default-reachable, independently source-locked, and a real bounded
  post-reachability capture supplies every required role.
