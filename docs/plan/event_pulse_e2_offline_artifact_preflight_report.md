# EventPulse E2 Offline Artifact Preflight Report

Date: 2026-08-22

## Scope and Outcome

Implemented the research-only, pure in-memory boundary from a checked
`ProspectiveCaptureAdmissionV1`, immutable decision time, and complete
canonical EPIN-JSON1 bytes to exactly nine nonempty canonical JSONL artifacts:

`TRADE`, `QUOTE`, `BOOK`, `OPEN_INTEREST`, `LIQUIDATION`, `CONFIRMATION`,
`CLOCK`, `COVERAGE`, and `SYSTEM`.

Each artifact reports its record count, exact byte length, SHA-256, and first
and last availability time. Every configured contributor, clock, coverage, and
system source must occur in the accepted input. The result remains
`blocked:fixture-provenance` and cannot author evidence.

The successor review repair binds the preflight to the admitted prospective
interval. `capture_starts_at` is retained as a canonical checked timestamp;
decision time and every input availability must be greater than or equal to
it. All inputs are parsed, classified, and checked against the start before a
`SourceStateMachine` is constructed.

No MFR1, session machine, adapter, daemon, transport, sink, filesystem,
environment, network, producer, manifest/package writer, snapshot, fixture, or
evidence-authoring behavior was added. Dependencies and `Cargo.lock` are
unchanged.

## TDD Evidence

RED command:

```text
cargo test -p marketfeed-event-pulse --test offline_artifact_preflight
```

Observed expected failure before production implementation:

```text
error[E0432]: unresolved imports ArtifactRoleV1, OfflineArtifactError,
OfflineArtifactPreflightV1
error[E0599]: no method named mechanics_config found for
ProspectiveCaptureAdmissionV1
```

GREEN focused result:

```text
cargo test -p marketfeed-event-pulse --test offline_artifact_preflight \
  --test prospective_capture
offline_artifact_preflight: 6 passed
prospective_capture: 8 passed
```

The regressions prove deterministic exact role order, expected per-role counts,
canonical bytes and independent SHA recomputation, strict reread, immutable
blocker/authorship state, missing-role failure, future/noncanonical rejection,
unconfigured topology rejection, and rejection when one configured source is
omitted even though its aggregate role is still nonempty.

Review-repair RED:

```text
error[E0599]: no variant DecisionBeforeCaptureStart
error[E0599]: no variant InputBeforeCaptureStart
error[E0599]: no method capture_starts_at on ProspectiveCaptureAdmissionV1
```

The added GREEN regressions use real 2026 prospective timestamps and cover
start-minus-one-microsecond rejection for market, clock, coverage, and system
records; a decision before start; exact-start acceptance at the temporal gate;
and successful complete input with every remaining record post-start.

## Validation Evidence

```text
cargo test --locked -p marketfeed-event-pulse
158 passed; 0 failed

rustup run 1.85.0 cargo test --locked -p marketfeed-event-pulse
158 passed; 0 failed

cargo clippy --locked -p marketfeed-event-pulse --all-targets -- -D warnings
passed

cargo deny --offline --locked check
advisories ok, bans ok, licenses ok, sources ok

cargo fmt --all -- --check
passed

git diff --check
passed
```

## Residual Boundary

This is a preflight consumer, not a capture producer and not fixture evidence.
A separately authorized, source-locked public read-only producer must still
create real post-reachability EPIN records for all configured sources. No E2
completion, runtime, deployment, paper, canary, live, or execution claim is
made.
