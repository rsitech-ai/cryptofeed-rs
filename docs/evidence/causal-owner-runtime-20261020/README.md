# Private MFCJ3 owner runtime

This slice wires actual private Bybit disk/null owner observations to the
original journal actor. It binds a canonical runtime policy, commits an execution
permit before callback entry, preserves complete matching completion after
revocation, keeps terminal state latched and withholds revoked output. Physical
source, native ordinal and journal ordinal remain distinct. This is the
[runtime contract](../../plan/causal_owner_history_runtime.md), with exact
source-scoped checks in the [qualification receipt](qualification.json).

All 18 local checks pass on unchanged frozen source03: 1,486 workspace tests
across 157 suites, zero failures and 56 existing ignored; Rust 1.85 core552
plus50 explicit bootstrap/owner units, four public bootstrap cases and ten public
owner cases; disabled-feature coverage, denied-warning Clippy/docs, formatting,
cargo-deny0.20.2 and cargo-audit0.22.2 with zero vulnerabilities/warnings.
These are local checks; current-source hosted CI remains separate.

Sixteen private cases exercise the actual persistent CaptureSession and writer
actor. They cover real Bybit bootstrap, callback-blocked revocation/deadlines,
native failure/panic, completion encoding overflow, actual writer failure,
healthy-Full retries, source recovery, clock exhaustion, queue limits, dropped
waiters, delayed completion/intent ACKs and finalization expiry. Controlled test
machines are explicitly test-only; they are not venue semantic qualification.
Two public additions preserve a real spot trade through durable handoff/cursor
continuation and reject missing/duplicate/corrupt/noncanonical policy bindings
through externally constructed self-consistent envelopes. Existing public owner
and bootstrap cases remain part of the compatibility checks.

A later review narrowed the initial runtime no-findings disposition: an age
cutoff could discard available complete evidence behind a healthy Full writer.
Two retained old-production regressions fail at the missing committed settlement,
separately for a blocked completion and its blocked predecessor. The repair
retains/retries the identical ordered transaction after observable expiry. Both
branches now preserve original input, one callback, terminal ordering and durable
settlement without output revival. The earlier review and failed evidence remain
unchanged; the final closure applies to the repaired source only.

The complete declared source snapshot and exact retained test binary/embedded
recipe have independent file-fingerprint verification. The recipe truthfully
records parent commit `7f383b2` for the uncommitted candidate content; it is not
an assertion that the parent contained this runtime, loaded-image attestation,
a complete transitive-input closure or reproducible-build proof. Frozen dataset
bytes and recorded metadata remain protected.

A healthy Full writer, stalled accepted I/O or blocked callback can still delay
shutdown indefinitely. Finalization age is an observable threshold, not an
abandonment permission or finite-join guarantee. Status/ingress refresh deadline
visibility during callbacks; there is no autonomous callback watchdog. The outer
worker-panic/ticket-reap path has static review, without an injected outer panic.

No daemon/live/Hyperliquid causal-factory migration, committed compatibility
projection, external-I/O intent/completion binding, actual-engine replay/resume,
strict dataset admission or registered-ingress performance repair follows.
The registered failed workload was not rerun or relabeled. Current hosted CI
and the next hlscreen pin must qualify their own source. Strategy context,
features and model/ETF integration remain rsibot-owned. No economics or
operational promotion is included; the full goal remains active.
