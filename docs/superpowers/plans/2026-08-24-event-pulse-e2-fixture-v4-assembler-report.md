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
