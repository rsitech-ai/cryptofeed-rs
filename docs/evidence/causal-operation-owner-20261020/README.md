# Private operation owner verification

The [owner contract](../../plan/causal_operation_owner.md) describes the actual
persistent worker, bounded ingress, exact admission retry, durable settlement,
safety/source fences and single committed-release slot. Bootstrap V2 binds its
policy; legacy V1 remains inspectable without silent owner defaults.

The final frozen source passes all 14 local gates in [the receipt](validation.json):
529 affected daemon/engine/recording/Bybit tests, zero failed and four existing
ignored; Rust 1.85 core 527 tests plus 34 bootstrap/owner unit cases, three public
bootstrap cases and eight public owner adversaries; the default-disabled case;
denied-warning workspace Clippy/docs, cargo-deny 0.20.2, cargo-audit 0.22.2 with
zero vulnerabilities/warnings, formatting and diff checks. The independent public
owner execution also passes eight cases against unchanged final source hashes.
The preceding full-workspace result (1,439 passed, 56 ignored) belongs to the
snapshot before the idle-writer-closure fix and does not qualify the final full
workspace. Published source `8ba199a` subsequently passes all 17 hosted jobs in
[its source-specific qualification](ci-qualification.json), including complete
workspace runs on Ubuntu x64/ARM, macOS and Windows. This evidence does not
qualify subsequent MFCJ2 owner-history changes.

Review found and closed immediate safety visibility, closed-source fencing,
persistent-Degraded cancellation, preflight settlement classification, stale
runner reconnect-request and idle-writer-closure defects. Regressions use the actual native Bybit
factory/runner and writer actor, not a callback stand-in. Tests distinguish
complete native turn failure, incomplete capture, post-execution encoding
overflow, publication failure, absolute queue/admission/commit deadlines,
revocation/late ACK, dropped waiters, source reuse, timer firing and startup
backlog. The queued-expiry test explicitly injects a scheduler-overdue entry at
a private seam; public delayed-ACK/deadline cases use real ingress and actor.
Idle closure/failure tests inspect the raw worker phase so accessor observation
cannot produce a false pass; in-flight closure still reaps the real accepted
ticket without restoring revoked visibility.

An independent recomputation of the declared 523-file source snapshot finds its
exact generated build recipe in the preserved affected-suite daemon test binary.
This fingerprints declared source and a retained file; it is not loaded-image
attestation, complete transitive-input closure or reproducible-build proof. All
22 frozen dataset files retain their bytes and recorded metadata. Cargo.lock is
unchanged; this owner change introduces no registry versions or dependency edges.

The preceding published bootstrap source failed its ARM post-close watchdog.
Its [failed qualification](bootstrap-ci-failure.json) preserves exact source/merge
tree, raw logs and passing scopes without relabeling the full run. New test phase
logging and a 120-second watchdog receive fresh own-source ARM verification at
`8ba199a`; passing stderr does not expose the individual phase timings.
This is a test-harness bound, not a production deadline or performance guarantee.

Shared recording bytes changed, so the previous all-shared-source-identical
consumer proof ends at its recorded source. Hlscreen `3cabbcb` is repinned and
rebuilt/replayed against producer `8ba199a`. Its own main and all four native
artifact jobs pass, with raw validation logs and archive hashes independently
checked in [consumer qualification](consumer-qualification.json). Those results
do not qualify a subsequent producer or consumer pin.

Owner withholding/handoff decisions and ingress admission settlements are not
durable records yet. A causal operation's actual drain is not proof of observed
availability or external delivery. Deliberate committed projection, bound I/O
intents/completions, actual-engine replay/resume and daemon/live migration remain
required. The registered ingress workload remains failed and was not rerun.
Native semantics/continuity, strict datasets, rsibot features, calendar/hardware
and operational qualification remain open. The goal and both draft PRs remain
active; no economics or promotion follows.
