# EventPulse E2 Fixture V4 Assembler Report

## Outcome

Implemented a pure in-memory Fixture V4 assembler and strict partitioned
readback. It consumes only a checked Admission V2, its matching truthful-empty
SYSTEM policy, immutable decision metadata, and complete canonical
MechanicsInputV2 JSONL. It returns exactly eleven ordered in-memory files and
has no filesystem, network, capture, evidence-authoring, runtime, or trading
API.

The result ceiling is `STRUCTURAL_V4_CANDIDATE`. E2 remains
`blocked:fixture-provenance`.

## RED evidence

The first focused command was:

```text
cargo test -p marketfeed-event-pulse-capture --test fixture_v4
```

It failed before implementation because both embedded root assets and the
`FixtureV4Assembler` / `FixtureV4Request` public API were absent. No behavior
was implemented before this RED was captured.

## Implemented boundary

- Exact embedded root contract: 5,527 bytes, SHA-256
  `cb899211245fe039f30d9f0d595133365f36d28fff5b508c20e1bf52363a9f47`.
- Exact embedded amendment: 10,647 bytes, SHA-256
  `2c19540bcc953700318a09738dfdbcf167c591827e8825adcad8003889fff965`.
- Exact 17-record Rust oracle: 17,189 bytes, SHA-256
  `fe9a7de25a34a57ff3565bd039929a47891ef69b5ffa147b19555c71eaac20d1`.
- Crate-local Git attributes preserve LF contract/oracle bytes on Windows.
- Deterministic role partitioning supports the universal 15-record minimum and
  the 17-record accepted structural oracle.
- Strict readback reconstructs authenticated replay order, reruns admission,
  wire, topology, family cursor, Book continuity, Clock, Coverage, timing, and
  capacity checks, and requires byte-identical repartitioning.
- Manifest/admission readback reconstructs every fixed binding, authority,
  role report, hash, count, time bound, and empty `record_identities` field.
- Caller-variable assembly fields remain limited to fixture ID, capture end,
  decision time, source terms, and complete JSONL.

## GREEN evidence

- Focused assembler: 5 passed, 1 ignored external-validator smoke.
- Explicit pinned root validator smoke: 1 passed; Rust output was accepted as
  `STRUCTURAL_V4_CANDIDATE`.
- Focused EventPulse partitioned readback: 2 passed.
- Full current EventPulse + capture all-target suite: green, including 5 new
  assembler tests and all historical V1/V2/V3/V4 regressions.
- Full Rust 1.85 EventPulse + capture all-target suite: green.
- Current clippy all targets with `-D warnings`: green.
- Rust 1.85.0 clippy all targets with `-D warnings`: green. The separate local
  `1.85` alias lacks the clippy component, so the installed exact `1.85.0`
  toolchain was used for this lint gate.
- `cargo fmt --all -- --check`: green.
- `cargo deny --offline --locked check`: advisories, bans, licenses, sources
  all green.
- `git diff --check`: green.

## Residuals and rollback

No real prospective fixture was captured or authored, no source was qualified,
and no completion, snapshot, runtime, paper, canary, live, risk, order, or trade
authority was granted. Rollback is the single additive implementation commit;
historical public APIs and wire bytes remain intact.

## Successor semantic hardening

The initial candidate delegated strict wire/topology/state validation to
EventPulse and used the root Python validator only as an independent smoke. A
review correctly identified that this did not independently enforce every
Fixture V4 contract-specific semantic in Rust.

The successor invokes a pure Rust contract validator from both `assemble` and
strict `readback`. It additionally checks:

- exact PUBLIC, MARKET, and confirmation source-specific catalogs and routes;
- routed Quote/Trade/Book/OpenInterest/Liquidation payload domains, full-u32
  envelope flags, and provenance correlations;
- exact per-artifact and cross-role contributor raw-coordinate and nanosecond
  receive-time progression;
- Binance zero-based action, item-zero, homogeneous-role, and
  snapshot-then-deltas frame grammar;
- strict Trade and Book continuity plus source-local Clock/Coverage cursor
  continuity and domain relations;
- truthful-empty SYSTEM and closed JSON types/numeric bounds.

RED was a canonically rehashed Quote with `bid_quantity: null`; the initial
candidate accepted it, while the successor rejects it at the Rust
`binance quote payload` rule. Additional rehashed regressions cover catalog,
Trade provenance, Book payload, causal time, frame grammar, sidecar domains,
continuity, type aliases, and a coordinated manifest+artifact rehash. The
pinned Python validator remains an independent happy/negative parity smoke,
not an enforcement dependency.

During this repair, the first source patch failed with ENOSPC. Recovery used
only `cargo clean` inside this isolated worktree, removing 1.7 GiB of
disposable build output. No source, fixture, user file, or other worktree was
removed.

Successor GREEN evidence:

- focused Rust assembler contract suite: 9 passed, 2 explicitly external
  parity smokes ignored by the ordinary command;
- explicit pinned Python happy/negative parity: 2 passed;
- focused EventPulse V4 preflight/readback: 11 passed;
- full EventPulse + capture all-target suites: green on current Rust and exact
  Rust 1.85;
- current and Rust 1.85.0 clippy all targets with `-D warnings`: green;
- formatting, offline locked cargo-deny, documentation, and diff checks:
  green.

## Flags parity and differential repair

The first semantic successor overconstrained `EventEnvelope.flags` to
family-specific values. The published root Fixture V4 contract instead accepts
every integer in `0..=u32::MAX`. A direct RED proved that a canonically rehashed
non-oracle Quote with `flags = u32::MAX` failed in Rust. The repair removes only
the invented family equality and retains strict integer/not-Boolean and u32
bounds. The same maximum now passes Rust assembly, strict readback, and the
pinned root validator; Boolean and `u32::MAX + 1` fail in both implementations.

An independent 75-case, canonically rehashed differential matrix now compares
Rust strict readback with the exact pinned root validator. It spans envelope
identity, catalogs, cursor/item coordinates, causal time, all six MARKET
payload families, Clock quality/freshness/reason rules, Coverage
family/interval/generation rules, the flags boundaries, publication floor, and
manifest capture/decision/retention/authority/binding invariants. All 75 cases
agree with the registered expected disposition. The existing Rust suite separately
covers replay/frame grammar, Trade/Book/sidecar continuity, truthful-empty
System, canonical encoding, aggregate bounds, manifest coordination, and
atomic failure.

During this repair, only this worktree's disposable Cargo target was cleaned a
second time after available disk space fell to 101 MiB; it recovered 1.9 GiB.
No source, fixture, user file, or other worktree was removed.

Final successor focused evidence:

- ordinary Fixture V4 Rust suite: 12 passed, 3 pinned-root tests ignored;
- explicit pinned-root parity suite: 3 passed, including the 75-case matrix;
- EventPulse admission/preflight V4 regression suites: 9 + 3 + 11 passed.

## Publication-floor parity repair

Assembly and strict adoption now independently decode every embedded
`bindings.*.merged_at` timestamp, compute their maximum, and require the
admitted `capture_starts_at` to be strictly later. Equality and one microsecond
before fail with the typed `capture publication floor` contract error before
preflight or adoption mutation; one microsecond after succeeds through
assembly, strict readback, and the pinned root validator.

The manifest parity audit also found one deliberate root behavior that strict
readback had previously narrowed: a valid immutable amendment commit is not
required to equal the assembler's emitted historical commit. Adoption now
mirrors the root validator by accepting a lowercase 40-hex commit with the
canonical repository and a canonical default-reachability time strictly before
capture, while assembly continues to emit its fixed historical binding. All
other manifest authority, published binding, admission binding,
transformation, capture, causality, and retention literals remain exact.

The first Rust 1.85 link attempt reached ENOSPC with 118 MiB available. The
authorized recovery again removed only this worktree's disposable Cargo target
(1.9 GiB); the exact Rust 1.85 full suite then passed with incremental output
and debug info disabled. No source or user data was removed.

## Timestamp lexical parity repair

Strict adoption now mirrors the two timestamp parsers actually published by
the root validator instead of requiring every accepted string to equal
`Rfc3339Time`'s six-digit rendering. Amendment `default_reachable_at` uses the
canonical parser: UTC `Z`, zero or 1..6 fractional digits, and no trailing zero
when a fraction is present. V4 capture, decision, max/bounds, and admission
capture strings use the V4 parser: UTC `Z` and zero or 1..6 fractional digits,
including trailing-zero forms such as `.10Z` and the assembler's existing
six-digit output. Both classes reject offsets and fractions longer than six
digits.

Adoption compares parsed instants, then normalizes only a temporary comparison
image; it does not rewrite caller bytes. Assembly output remains deterministic.
Published binding timestamps remain byte-exact because their complete binding
objects are independently pinned. The ordinary Rust regression and expanded
75-case pinned-root matrix cover `.1Z`, `.000001Z`, class-specific `.10Z`,
overprecision, and offsets across capture, decision, artifact-bound, and
amendment fields.

The year-domain successor explicitly rejects lexical year `0000` in both
parser classes while retaining `0001..9999`. The differential matrix includes
canonically rehashed year-zero cases for amendment and V4 package timestamps,
plus malformed year widths and representative invalid month, day, non-leap
February 29, hour, and second values. A direct Rust parser regression proves
year `0001` and a valid leap day remain accepted without weakening the package
publication-floor relation.

The first year-domain Rust 1.85 run reached ENOSPC with 406 MiB free. The
authorized recovery removed only this worktree's disposable Cargo target (1.8
GiB); the complete MSRV suite then passed with incremental output and debug
info disabled. No source or user data was removed.
