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

The20-second startup/recovery deadline uses receipt/timer TimestampNs and also
checks incoming frames before timer delivery. Exact-boundary arrivals are late.
It is a local policy, not an exchange ACK deadline or monotonic elapsed-time
proof. Native request IDs are optional; mandatory correlation is conservative
local admission, not a universal native echo guarantee. Local ID limits are64
request bytes,256 connection bytes and4096 response-message bytes. Ten topics
per request matches the spot limit and is conservatively extended to derivatives.
Aggregate serialized JSON args are bounded to21,000 UTF-8 bytes locally; native
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

A real Windows CI failure on prior source81d7ee549a161560088bea05c231708c88888a3a
remains OPEN: run39337055374, job118349070805 fails the daemon catalog provenance
test at FeedWriter::create with Access is denied (OS error5). Directory-open/sync
assumptions in shared-feed and other persistence writers require a reviewed
Windows durable-publication backend and actual platform verification. No fsync
failure is skipped, weakened or relabelled as reader cleanup. Local macOS checks
do not close this platform requirement.

## Native protocol probe

The retained bounded protocol investigation sent two separately identified
requests on each of three sockets (six ACKs total), with8-second open/receive
bounds,2-second close and2MiB output cap per socket. Original direction/base64,
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

Forward daemon qualification is recorded separately after a clean source build.
All prior unmet coverage, dataset, settlement/expiry/units, calendar canary and
24h/7d operational gates remain open. The goal remains active.
