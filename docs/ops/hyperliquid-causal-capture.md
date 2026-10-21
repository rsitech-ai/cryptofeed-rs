# Owned public Hyperliquid capture

This opt-in command records one source history and one MFOP projection for public
spot/perpetual observations. It never starts the legacy collector or a strategy.
Use a new output directory for every lifetime. The current candidate still needs
its combined platform, live and consumer qualification; a successful command is
not operational promotion or proof of uninterrupted market coverage.

New captures explicitly bind driver policy version 2: a local timer wake admits
a native poll only when the latest observed committed native schedule has a due
deadline. The 100 ms wake, owner polling, queue/frame limits and durability gates
are unchanged. The driver still alternates ready frames with due polls and checks
Stop while waiting for accepted work. Version 1 retains its recorded periodic-poll
behavior for existing profiles; an unknown policy version fails preflight. This
removes unnecessary no-due journal turns, but its effect on acquisition delay must
be measured separately from storage correctness.

Build and retain both tools from the same reviewed source and lockfile:

Use the release profile for operational timing checks. The retained debug runs06
and07 failed their unchanged two-second final-file-mtime diagnostic; release
run08 passed it on the same source and request, but release run09 failed it at
3.473495950 seconds with the locally updated consumer. Release optimization alone
does not establish sustained delivery. This short development check is
not a freshness SLA. See the [source-specific evidence](../evidence/owned-hyperliquid-capture-20261020/README.md)
for exact artifact pins, preserved failures and remaining delivery measurements.

```sh
cargo build --locked --release -p marketfeed-daemon --features causal-journal \
  --bin marketfeed-capture --bin marketfeed-observations
target/release/marketfeed-capture identity
target/release/marketfeed-capture run --request capture.json --output /data/new-canary
/data/new-canary/capture-executable verify --output /data/new-canary
```

`capture.json` is a regular UTF-8 JSON file, at most 64KiB. For example:

```json
{
  "schema_version": 1,
  "markets": [
    {
      "market": "spot",
      "coins": ["PURR/USDC"],
      "channels": [{"kind": "trades"}, {"kind": "quote"}]
    },
    {
      "market": "perps",
      "coins": ["BTC"],
      "channels": [{"kind": "trades"}, {"kind": "mark_price"}]
    }
  ],
  "duration_ms": 60000,
  "source_max_bytes": 2147483648,
  "target_max_bytes": 4294967296
}
```

Coins must match the actual discovered native names exactly. The example is a
selection, not a permanent catalog guarantee. No aliases are inferred. Request
one instance of each market, at most 64 coins/market and at most 64 initial native
topics/market. Supported selected channels are adapter-defined; unsupported
selections fail before WebSocket startup. Duration is at most 24h per lifetime;
storage bounds are 64MiB–1TiB each. These are ceilings, not predicted consumption.
Preflight also rejects metadata that cannot fit the bounded replay/projection.

The current negotiated preset bounds a raw frame at 64KiB, a native unit at 128
rows/256 public participants/64KiB of strings, and its encoding at 256KiB. The
native string budget matches the existing frame ceiling: the first public pilot
exposed that 4KiB rejected valid 30-trade snapshots carrying hashes/participants.
That failed corpus stays unsealed and unchanged. Oversize rows or frames still
fail closed; a different capture preset requires its own qualification.

The command acquires exact decoded metadata bodies from the fixed public HTTPS
endpoint without redirects. It binds request/body hashes, selected response
headers and acquisition clocks into the bootstrap. It commits the bootstrap
before creating each persistent native owner/transport. Both markets share one
collector budget. Application writes require durable one-use wire authorization;
connect/read/automatic Pong/close remain explicitly managed, unjournaled controls.

The source owner may defer ACK observation for at most two non-authorizing
`Started`/`Ingress` records, each still a separate one-record transaction. One
following authority record may be admitted before that prefix is drained. Native
execution, completion processing, releases and application writes wait for every
predecessor's own validated receipt; a covering publication cursor alone cannot
substitute for an ACK. Ingress credits remain charged until their validated
receipt. Full retries retain the identical transaction, and normal idle/closing
and panic cleanup drain accepted tickets. Actor capacities and batch age are
unchanged. Source `0f332bdf` passes its [local qualification](../evidence/owned-hyperliquid-capture-20261020/closing-boundary-validation.json)
and [joint11 feed-tail gate](../evidence/owned-hyperliquid-capture-20261020/joint11.json).
Acquisition freshness remains unqualified: this sample's perps timestamp/receipt
differences exceed eight seconds. Preserve that diagnostic separately from the
25.7ms source-to-target final-file tail; neither is a per-event latency SLA.

`Stopped` is the logical closing boundary: accepted tickets are drained, queues
are empty, and snapshot/release views are cleared under the admission lock.
Late revocation or rejected input cannot add unrecordable terminal state after
that boundary. `shutdown` still joins physical capture destruction and worker
cleanup; callers must join when they need physical completion.

The output contains `request.json`, `build.json`, the retained exact
`capture-executable`, `source/`, `feed/` and a terminal `report.json` after driver
settlement. macOS/Linux auxiliary files and directory entries are synced; source
and feed use the recording layer's publication barriers. A setup failure or hard
process termination can leave an unsealed partial root without a terminal report.
Never overwrite it or silently append with a new writer.

When writer shutdown succeeds, the report includes each writer's final published
cursor, transaction/publication-group counts, and cumulative I/O stage timings.
Writer publication is separate from a consumer's last observed ACK. Missing
writer metrics after a failure mean unavailable, and cumulative/overlapping
stage times do not measure source freshness or end-to-end delivery latency.

Both consumers must use the capture's **`feed/` subdirectory**, not `source/`
or the output root. After the coordinated HLS producer pin is qualified, use
`hls live --shared-feed /data/new-canary/feed --symbols spot:@107,perps:BTC`
with the actual selected native symbols. Add `--once` to replay a saved committed
prefix. For Quants, use its Python 3.12+ module
`scripts.data.observe_cryptofeed_projection` with `--feed /data/new-canary/feed`,
`--binary` pointing to the retained observation bridge, its actual
`--binary-sha256`, a new `--output` outside the feed, and explicit diagnostic
time/group budgets. Each consumer has its own progress; neither forwards data
through the other. These commands do not qualify unknown availability or persist
strategy feature state.

The optional request field `projection_batch_groups` accepts 1–8; omitted or
explicit 1 retains the original one-record transaction behavior and the same
serialized request bytes. A value such as 8 bounds both each physical publication
and the total outstanding window to eight complete MFOP records. A candidate
transaction may contain several records; the writer can combine queued
transactions in one publication. This changes no native subscriptions, source
handoffs or consumer logical group. Values 0/9 fail before output creation or HTTP acquisition.
The report declares the configured maximum and last observed publication's N
records and exclusive-before/inclusive-after source/target cursor interval.

`projection.groups` counts logical MFOP records. Writer `committed_operations`
counts admitted transactions, `groups` counts physical publications, and
`timings.accepted_records` counts acknowledged records. A publication can combine
transactions; a transaction can contain multiple records. Keep these counts
separate when inspecting batching or deciding whether a consumer caught up.

Grouping stops at count, available committed source, Seal, or the existing byte
and preparation-age bounds. Another source step is read only when a policy-maximum
record can fit, so there is no held lookahead. The capture keeps the existing
8-operation/16MiB publication, 32MiB pending and 1ms actor grouping bounds. Preparation
checks that age between replay calls; native execution, scheduling and I/O are not
interruptible or covered by a wall-time guarantee. Caller preparation memory and
the writer encoding buffer are additional to the actor's pending-byte budget.

A Full refusal retains the exact immutable transaction without replaying native
callbacks or reading past that candidate. Preparation may overlap accepted writes
within the same N-record/16MiB encoded window. Prepared, accepted and
receipt-cached records consume credit until their complete physical publication
is acknowledged; actor status or refunded writer credits never grant progress.
Every source read first reserves room for a policy-maximum record, including the
first record of a new candidate beside accepted work. The source replay's
private speculative state remains bounded by that same window.

Each ticket covers one transaction, while its published cursor can cover later
transactions in the same physical publication. The pump validates every covered
ticket's admission, record range, source chain and identical complete published
cursor before atomically advancing the paired checkpoint. Unknown or
split-transaction publication ends poison the pump. Partial receipt observation
retains all credits; Seal becomes visible only after its complete group ACK.
Readers still acknowledge and resume individual complete MFOP records, including
boundaries inside a physical transaction. An error while preparing later records
discards the entire unadmitted candidate and poisons the pump; an earlier valid
prefix of that candidate need not have been published. An I/O failure or dropped
waiter can leave a complete later transaction visible without a pump ACK. Retain
that evidence and reconstruct into a new target lifetime; never invent progress
or append takeover. Source data and native eligibility rules are unchanged.

This opt-in physical grouping requires its own source/platform and growing-feed
consumer qualification. Fewer target transactions in an offline correctness
fixture do not establish an acquisition-delay or freshness repair.

The projector tails only committed source records and independently reexecutes
native callbacks. Readers tail `feed/` with independent complete-group ACK pairs.
Retain the separate `marketfeed-observations` executable and SHA256 pin for the
Quants observer. Cursor ACK does not persist consumer feature state. Consumers
must rebuild state after uncertain application/ACK outcomes.

Stop/SIGINT requests are journaled and fenced. A failed/unknown application send,
refused acquired frame, failed source/target publication or unsettled owner
prevents successful completion. A clean semantic Seal may still report managed
transport errors, and even zero market rows; check requested-market connections,
observations, quality transitions and gaps separately. OS calls and accepted-work
joins have no finite shutdown guarantee. The session budget is checked at driver
loop boundaries; separate connect/write/close timeouts may extend elapsed time.

`verify` requires the exact retained executable, OS and architecture. Rebuilding
the same source creates a different executable identity and cannot substitute for
it. Verification compares the target's initially committed prefix to native
source execution; source may be ahead of a live target. An unsealed verified
prefix is not complete history. Keep the original files, hashes and errors.
Artifact hashing is not signed attestation or proof of the OS loaded image.

Unknown usable/availability time remains unavailable. Quants' opt-in RAM-state
and paired-cursor checkpoint is locally implemented and tested separately; it
does not qualify strategy features or external side effects. Strict dataset and
strategy admission, economic runs and 24h/7d operational qualification remain
separate gates. Follow [the Razer verification contract](../plan/razer_shared_feed_verification.md).
