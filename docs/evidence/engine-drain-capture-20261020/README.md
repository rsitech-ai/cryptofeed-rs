# Causal effect attribution and actual drain capture — 2026-10-20

The actual private runner capture now gives every trace entry an explicit origin:
runner operation, callback ordinal, or callback/action index. Repeated timer IDs
have distinct callback ordinals. Identical actions and generated drop/status
diagnostics retain their exact parent; outer frame acceptance and drain effects
return to runner scope. Native action-prefix failure still leaves the suffix
unattempted.

The actual `drain_dispatch`, `take_pending_writes` and `take_pending_http` methods
capture ordered removed payloads and completion. Captured copies are charged
before each clone. `execute_turn` can declare a successful callback followed by
null dispatch and outbound takes within the same captured operation. Failed
entries leave their private queue prefix untouched and imply no drains. Capture
failure remains independent of native execution: successful native drains can
finish privately while capture is incomplete and the owner becomes poisoned.

Typed sink-completion input validates its result count and terminal-error prefix
before mutation, then uses a scripted sink through the actual runner and
`forward_dispatcher`. It preserves accepted prefixes, the rejected front and the
unattempted suffix, native market-before-system order, and separately queued
post-forward drop diagnostics. This is an offline observation of supplied results;
it does not bind them to a previously committed delivery intent or perform I/O.
That binding remains required in the live owner. Sink forwarding is not replaced
by take-all draining.

Twenty-eight focused tests and separate independent execution pass. Ten added
cases exercise effect origins, repeated timer IDs, all four drain lanes, compound
turn reexecution, failed-prefix retention, rejected sink transcripts/fronts,
generated diagnostics, sensitive/budget faults during private drains, full-width
i128 values through drain and sink boundaries, and a fault after the completed
first drain. The initial compile errors are retained in ignored evidence. The
first mid-drain probe instead faulted just before draining: it omitted the native
zero-drop observation in its entry count. Moving its cap from six to seven reaches
the declared boundary while retaining incomplete-capture, actual-queue and poison
assertions. This does not change the registered ingress fixture or its thresholds.

Broader checks, exact source/log hashes, independent review and preservation
evidence are bound in `validation.json`. The original captures and frozen datasets
are not rewritten. No live or paced-ingress workload is run for this capture-only
slice; the registered durable-ingress failure remains failed.

Full local checks pass 1,363 workspace/all-target/all-feature cases, zero failed
and 56 ignored across 153 suites, plus 498 Rust 1.85 core-library cases across
30 suites. Workspace Clippy/docs with denied warnings, formatting, exact
cargo-deny 0.20.2 and cargo-audit 0.22.2 with denied warnings pass. The full lock
reports zero vulnerabilities/warnings. Manifests and lock are unchanged; staged
public checks pass and final receipt metadata is rechecked before commit.

The complete repair still requires a negotiated causal envelope/profile,
engine-owned exact wire DTOs, a lossless factory/bootstrap committed before
execution, private owner/actor admission and commit state, committed snapshots
and readiness/books/dispatch/wire/HTTP release, delivery-intent/completion binding,
actual-engine durable replay and live caller migration. A 64 MiB logical trace is
not proof that its encoded envelope fits a 32 MiB record; encoding overflow after
mutation must be terminal. Existing debug/sanitized session metadata is evidence,
not a lossless factory binding. See the
[integration contract](../../plan/causal_journal_integration_contract.md).

This slice changes engine source only. The five hlscreen dependency packages and
their static workspace/toolchain inputs are independently checked against the
existing `ba512ad` pin. That source proof does not establish identical build
environment, target or injected build metadata, nor whole-producer equivalence.
Consumer qualification retains its original source and actual binary scope.
Strategy context, features and decisions remain rsibot-owned.
