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

Unknown usable/availability time remains unavailable. Strict dataset admission,
consumer feature checkpoints, economic runs and 24h/7d operational qualification
remain separate gates. Follow [the Razer verification contract](../plan/razer_shared_feed_verification.md).
