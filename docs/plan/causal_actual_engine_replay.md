# Actual-engine MFCJ3 replay

This implements the next D5 milestone B integration step. The current
OwnerHistory validator proves structure and dispositions but does not execute
an adapter. Completion requires a public disk reader exercising actual native
turns and drains, physical-prefix/resume checks and independent review.

Scope: daemon-owned single-owner Bybit/Hyperliquid public disk/null MFCJ3
recordings. No transport, external sink, production CLI or live path changes.
The recording bootstrap must match the current compiled recipe and actual
executable fingerprint; other artifacts are rejected. This is an artifact
fingerprint check, not loaded-image attestation or reproducible-build proof.

Consume the bootstrap first in physical order, validate supported configuration
and reader bounds, and construct the exact factory once. Keep its actual
CaptureSession through timer polls and reconnects. For each structurally admitted
ExecutionCompleted, execute the recorded ingress through the actual engine with
the bound drain policy and compare the complete typed operation, including
financial values, effects, errors, queues, timers, generations and accounting.
Do not reconstruct callback actions or infer removed values. Any storage, codec
or native comparison error permanently poisons that replay instance.

Return observations only from recorded HandoffObserved dispositions after their
native operation matched. An outstanding permit remains ExecutionUnknown; a
seal means storage closure and cannot turn unknown execution into known state.
The reader has no mutable machine/state escape. A resume cursor is an exact
physical boundary: rebuild the complete engine prefix from the bootstrap and
discard historical handoffs before returning the resumed reader. No fabricated
fresh-machine cursor skip or native checkpoint shortcut is admitted.
Resume work is linear in the initial committed prefix: the target must fit that
initial bound, and a crossed/mismatched physical boundary is rejected before
its record executes. Earlier verified prefix callbacks may already have run in
the private no-I/O engine before a later malformed boundary is discovered.

Validation: actual persisted spot/perps and Bybit factory histories; timer and
reconnect persistence; every physical prefix and exact resume boundary; financial
and nonfinancial mutations that structural validation alone accepts; wrong
profile/build/source/config; future corruption without metadata prescan;
indeterminate and revoked late-completion prefixes. Use focused regressions,
Rust1.85 checks, formatting/Clippy and the required workspace checks before
qualification. Preserve existing capture, frozen-dataset and failed-load bytes.
Rollback is reverting this opt-in reader and its scoped tests/docs.

Status: implementation and verification in progress. Owner scheduling decisions
(deadlines, preflight refusal and revocation) are structurally validated declared
observations; this reader does not independently reproduce wall-clock scheduling.
The opt-in [multi-owner reader](causal_multi_owner_replay.md) extends this with
one physical cursor, independent persistent owners and explicit aggregate
retention/reservation limits. Its current source is undergoing full qualification;
the published single-owner receipts exclude that later candidate. The existing
single-owner API continues to reject additional owner bootstraps.
Committed compatibility projection, complete failure/admission retention, bound external I/O,
daemon/live/control/supervisor migration and native checkpoints remain required.
The registered ingress comparison stays failed and is not rerun in this slice.
Rsibot owns strategy context/features/models/ETF research and decisions;
cryptofeed owns acquisition/storage/replay; hlscreen owns presentation.
No economics, activation, deployment, merge, release or operational promotion.
