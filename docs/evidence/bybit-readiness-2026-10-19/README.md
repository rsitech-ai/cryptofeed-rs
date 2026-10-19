# Bybit exact request admission and engine error settlement

This slice repairs startup readiness across spot, linear and inverse public
sessions. It does not qualify continuity, sustained operation, a research dataset
or complete runtime-envelope replay. Strategy context remains rsibot-owned.

## Contract and implementation

Every generation retains exact requested topics per unique request ID. Native
subscription success must correlate to that ledger with a consistent nonempty
native connection ID. Ping/pong is distinct from subscription admission. Exact
duplicates are idempotent; negative/conflicting/missing/foreign responses fail
closed until Connected. All request ACKs, every selected two-sided L1 BBO and
all requested L2 snapshots must be available before MarkLive.

The 20-second startup/recovery deadline uses receipt/timer TimestampNs and also
checks incoming frames before timer delivery. Exact-boundary arrivals are late.
It is a local policy, not an exchange ACK deadline or monotonic elapsed-time
proof. Native request IDs are optional; mandatory correlation is conservative
local admission, not a universal native echo guarantee. Local ID limits are 64
request bytes, 256 connection bytes and 4096 response-message bytes. Ten topics
per request matches the spot limit and is conservatively extended to derivatives.
Aggregate serialized JSON args are bounded to 21,000 UTF-8 bytes locally; native
documentation describes characters. Cold/failed/disconnected/stopped sessions
cannot emit financial frames. Missing L1 sides revoke readiness and start a new
bounded quote-recovery window while retaining the same connection's known ACKs.

Public engine callback errors now revoke live readiness and valid-book metrics
for decoder, recording, action/dispatch and publication failures, including a
failure after MarkLive has already applied. A new connection or close/loss
revokes the prior admission before fallible recording/callback work. Failed
adapter callbacks do not apply partial actions. Side-effect-free dynamic
subscription preparation refusal preserves the existing healthy admission;
subsequent operational failures revoke it.

Rust alpha API users must migrate SubscribeAck patterns to the new request,
connection and response fields and handle UnexpectedControl. No frozen recording
format is silently rewritten.

## Independent review and retained failures

Separate venue, consumer and engine reviewers examined the native correlation,
clock/limit assumptions, corpus preservation and public callback error paths.
Their findings led to cold-frame suppression, warmed error regressions,
complete-operation settlement, pure-rejection preservation and independent replay
identity assertions. Focused reviewer test results and final checks are recorded
in offline-validation.json. Review is bounded to these changes.

The cold financial-frame regression failed with one market event before the
fix. The warmed missing-BBO/deadline-overflow regression failed because its
previously live engine signal remained true. Initial Clippy feedback rejected
two unnecessary closure calls; those were replaced with direct blocks. Original
failed attempts remain in ignored evidence and are identified by hash.

A real Windows CI failure on prior source 81d7ee549a161560088bea05c231708c88888a3a
remains OPEN: run 39337055374, job 118349070805 fails the daemon catalog provenance
test at FeedWriter::create with Access is denied (OS error 5). Directory-open/sync
assumptions in shared-feed and other persistence writers require a reviewed
Windows durable-publication backend and actual platform verification. No fsync
failure is skipped, weakened or relabelled as reader cleanup. Local macOS checks
do not close this platform requirement.

## Native protocol probe

The retained bounded protocol investigation sent two separately identified
requests on each of three sockets (six ACKs total), with 8-second open/receive
bounds, 2-second close and 2MiB output cap per socket. Original direction/base64,
UTC and monotonic receipts were flushed before decoding. All six ACKs echoed
the exact IDs with one consistent conn_id per socket. This is finite observed
compatibility only; it is not universal behavior or daemon qualification.
Probe source and compact results are retained here; raw JSONL hashes are in
native-request-id-probe.json and originals remain in ignored evidence.

## Corpus and replay limits

All three original MFR1 fixtures remain unchanged and SHA-pinned. Active negative
tests require no financial events, no MarkLive, and explicit degradation/reconnect
for their uncorrelated ACKs. Separately named correlated fixtures are created
only with create_new and cannot overwrite old evidence.

Positive fixtures compare complete semantic envelopes while independently
checking runtime venue/session/raw ordinal and configured replay identity/adapter
ordinal. MFR1 omits physical connection identity and ReplayRunner uses adapter
ordinals; those two documented exclusions are not exact runtime-envelope parity.
The authoritative shared feed and general mixed causal replay remain separate
qualification requirements. These fixtures are synthetic, not live receipts.

## Primary sources

- [Bybit connection, limits, heartbeat and ACK examples](https://bybit-exchange.github.io/docs/v5/ws/connect)
- [Pinned official SDK missing-ID fallback](https://github.com/bybit-exchange/pybit/blob/c3309e14e577b1d5610c1f288a413df6990a6d25/pybit/_websocket_stream.py#L473-L479)
- [Prior Windows CI job](https://github.com/rsitech-ai/cryptofeed-rs/actions/runs/39337055374/job/118349070805)

## Bounded forward daemon result on 410c3ca

One 30-second capture ran 2026-10-19T23:11:35.064992Z through 23:12:05.499328Z
with clean exit 0 (exact launch times and binary/config/plan hashes in pilot-launch.json).
It retained 1,123 application frames and four metadata records with exact raw/shared
parity and a final seal. All 972 native book/BBO rows match (three snapshots,
675 deltas,294 quotes). All ten observed trades and eleven candles also match,
but no inverse trade arrived: the unchanged strict three-trade-venue gate exits 1.
This failed coverage disposition is retained alongside the earlier catalog sample;
no capture was repeated to replace it with a passing outcome.

The independent finite inspector finds three exact request/ACK pairs across 17
topics and 1,117 accepted inbound frames. Each selected BBO and requested L2
snapshot precedes the canonical live transition. Receipt-time admission delays
are 302.323ms spot, 1,642.469ms inverse and 1,453.945ms linear. Only one request
chunk per venue was sampled; live multi-chunk/recovery behavior remains unobserved.
The initial inspector's incorrect rejection of the matching post-LocalStop
canonical degraded status was retained and corrected with a narrowly checked
shutdown exception. Financial/live events after close remain rejected.

Raw 296,976 bytes SHA256 84f55814d3c4e40135ee657aecdf781cd79211ab0c62f315089ea45b04b4541a.
Shared feed 170f29174ced997888da73046bcf328edc83a00f /3325 /3187231;
committed-prefix SHA256 d6d8ba9f62c9c430f1029d63cefa0c726eb9dafa84f72725b73623e6b1572966.
Original bytes remain in ignored evidence at the paths bound by the launch plan.
Finite inspector source, witnesses, config/plan and compact result receipts are
retained here. Native value audits and readiness inspection prove distinct scopes.
All prior unmet coverage, dataset, settlement/expiry/units, calendar canary and
24h/7d operational gates remain open. The goal remains active.

Final-source validation passes 1,263 workspace/all-target/all-feature tests with
56 ignored cases (including network and corpus creation), 178 focused Bybit/engine
tests with four ignored, 36 Rust 1.85 library tests, Clippy, rustdoc, formatting,
dependency/source checks and eleven finite-inspector challenges. The exploratory
workspace run compiled while source changed and is explicitly separated from the
final-source run. Filesystem/publication error injection is not claimed by the
portable in-memory error tests; the actual Windows durability failure remains open.

Hlscreen d40d4136a53ad130c4374d2e34ccb9778f1c8748 now pins all four marketfeed
packages to 410c3cae95f17c3b2b8ee88a9809c5fc1d7ad28b. Its 542 all-target tests,
Clippy/MSRV/license/source/pin checks pass. A saved 1,055-event Hyperliquid summary
matches the old-pin result exactly, independently replayed with unchanged archive
hashes. This migration does not relabel earlier live/soak results as new-pin runs.

The public finite-challenge receipt normalizes only displayed host/interpreter
paths to a portable invocation; its original receipt hash and ignored evidence
path are retained explicitly. No fixture, cursor, prefix or financial value changed.
An initial public-source scan rejected that workstation path and is retained as
bybit-readiness-oss-final-02.log. The final source scan is recorded separately.

Cargo-deny passes the configured policy, including the existing documented
RUSTSEC-2026-0009 time exception for the unreachable RFC2822 parser and Rust1.85
constraint. This is not an advisory-free claim. The separate dependency inventory
and any dependency repairs retain their own source and validation scope.
