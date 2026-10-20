# Actual native-engine replay qualification

This slice adds the opt-in `causal_bootstrap::HistoryReplay` disk reader for
one supported Bybit or Hyperliquid public spot/perpetual MFCJ3 owner. It consumes
the first physical bootstrap, verifies the current artifact/configuration and
reader budgets before constructing one actual factory, and keeps its native
`CaptureSession` through callbacks, timers and reconnects.

Every committed completion is reexecuted and its complete typed operation is
compared, including financial coefficients, errors and actual queue drains.
Recorded handoffs are exposed only after native equality. Invalid data or a
native mismatch poisons replay. Permits without complete execution evidence
remain uncertain, including after a storage seal. Resume rebuilds the native
prefix to one exact initially committed physical cursor and discards historical
handoffs; it provides no native checkpoint or restart authority.

## Evidence

`qualification.json` records both actual terminal results, all 16 local
checks, exact tool versions, raw-log hashes, declared-source digest, retained
integration-test executable and its embedded source recipe. The 530 declared
inputs remained unchanged through the checks. The workspace result is 1,505
passing executions, zero failures and 56 pre-existing ignored tests across 158
suites; the focused public Bybit/Hyperliquid targets pass all 27 tests. Explicit
Rust 1.85 checks include the unfiltered 16-test Hyperliquid target. Cargo.lock
and dependency versions are unchanged; cargo-deny 0.20.2 and cargo-audit 0.22.2
pass, with zero audit vulnerabilities/warnings.

All 16 individual commands exited zero. The original wrapper then exited one:
its final assertion expected `cargo-audit 0.22.2`, while Cargo's subcommand
invocation reports `cargo-audit-audit 0.22.2`. That failure remains retained.
A separate finalizer exited zero after checking both exact direct/subcommand
version strings, selected tool hashes, all original logs and unchanged source/
preserved data. It did not rerun the test commands or relabel the failed wrapper.

Adversarial tests cover every physical spot/perpetual prefix, exact resume and
the next appended step, reconnect persistence, complete typed native failures,
structurally valid price/drain mutations, unknown permits, revoked late
completion, wrong profile/artifact, additional owners, cursor fabrication,
future corruption and changed watermarks. Twenty-one admitted input/cause
examples round-trip exact payloads, stamps, header order and identifiers.
Earlier failed preflights remain explicitly unqualified in the ledger.

The retained file is the exact `hyperliquid_causal` test executable named by the
workspace raw log. Its embedded recipe binds the declared source and truthful
parent metadata; this is not loaded-image attestation, complete transitive build
input closure or reproducible-build proof. Original 22 frozen dataset files,
two pilot files and 33 hlscreen historical evidence files retain their recorded
bytes and metadata.

`parent-producer-hosted.json` records independently inspected hosted checks for
the published parent 56b733d: all 17 jobs pass. `hlscreen-hosted.json` records
hlscreen b1ecfba with that producer pin: main checks and all four native artifact
validations pass; six workflow archives and the complete 311-file source export
were checked. These older source scopes do not qualify this replay candidate
or a later consumer repin. Scheduled API smoke, tag publication and announcement
were skipped. Current-source hosted checks and consumer repin are separate gates.

## Remaining integration

Owner scheduling/refusal/revocation decisions are structurally validated
declared observations, rather than independently reproduced wall-clock
scheduling. This reader supports one disk/null owner. Bounded multi-owner
replay, complete failure/admission retention, committed consumer projection,
bound external I/O, daemon/CLI/live/control/supervisor integration, strict dataset
admission and hardware/calendar qualification remain open. The registered
ingress workload remains failed and has not been rerun.

Cryptofeed owns acquisition, persistence and replay. Rsibot owns cross-venue
features, model/ETF context, research and strategy admission; hlscreen owns
presentation. Both consumers read the shared source directly. No economics,
trading activation, deployment, merge, release or operational promotion follows
from these checks. The full goal remains active.
