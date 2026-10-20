# Hyperliquid causal bootstrap and persistent owner

The opt-in disk/null bootstrap can construct actual Hyperliquid spot and
perpetual sessions after its original durable bootstrap receipt. The factory
and emission policy are explicit. The Bybit policy spelling and complete-bundle
behavior remain unchanged; incompatible factory/policy pairs fail closed.

Hyperliquid market identity binds the full catalog to native discovery bodies,
including spot token indices and per-DEX collateral. The successful spotMeta body
uses `hyperliquid-spot-meta-v1`; perpetual discovery additionally uses
`hyperliquid-perp-metas-v1`. Reproduction hashes bind the supplied bodies, not
independent authentication of an exchange response. Assigned instrument IDs and
catalog versions are retained while every native definition is re-derived.
Changing a coin and its topics together cannot silently switch markets.

Ordered wire subscriptions come from the same conversion as the native session.
Shared asset-context subscriptions are deduplicated while funding, open interest,
mark, index and statistics remain separate semantic requests. Snapshot/BBO
options must be enabled and exchange deltas disabled. Direct factory construction
now enforces the planner's 200-native-subscription session bound; larger valid
requests still split into multiple planned sessions.

## Verification scope

`qualification.json` records the exact candidate inputs, commands, raw-log
hashes, source-specific independent review and preserved development failures.
The public tests use actual factories and the persistent MFCJ3 owner. For both
markets they compare complete operation evidence against a fresh native capture,
then inspect saved physical-order journal records and internal handoff drains.
They cover exact large/scale-20 trade decimals, books, BBO, context, candles,
timer polls and reconnects. A malformed later trade row is a complete durable
native failure, without partial trade release or subsequent execution. A
namespaced builder perpetual preserves collateral identity and requests only
funding/OI on one wire subscription; runner lifecycle records remain visible.

Independent review found an initial market-identity admission gap. The exact
initial source and coordinated coin/topic regression were retained; that source
fails the regression. Final source binds native discovery definitions and
passes it. Review also found a quadratic duplicate scan: canonical nonfinancial
channel keys now use an ordered set. A 65,536-item invalid cadence vector is
rejected; this is not a latency benchmark. The first final-check attempt failed
because the new builder test counted runner VenueStatus records as asset context;
its failed receipt remains separate from the corrected final qualification.

`parent-runtime-hosted-ci.json` qualifies parent producer 7a68984 only. Its own
run passed all 17 jobs with actual checkout/whole-tree and all 527 declared-input
proofs. It does not qualify these newer Hyperliquid changes.

`parent-hls-hosted-ci.json` independently qualifies hlscreen 18bc9e4 with
producer pin 7a: main checks, all four native platform validations, six workflow
archive digests/CRCs, inner checksums/global manifests and the exact 309-file
source export. Scheduled API smoke, publication and announcement were skipped.
This success also excludes the newer Hyperliquid producer source.

## Remaining work

This is a private disk/null library integration. Normal daemon/live/control/
supervisor migration, committed consumer projection, bound external I/O,
actual-engine physical-order replay/checkpoints and safe restart remain open.
The tests compare actual captures and validate owner-history ordering; they do
not implement a production replay executor. Internal handoff does not acknowledge
external delivery. Existing writer/callback shutdown and watchdog limitations
remain in the [runtime contract](../../plan/causal_owner_history_runtime.md).

The registered ingress workload remains failed and was not rerun. Preserve its
original schedule, caps and abort rules for the comparison after full integration.
Native continuity, strict datasets, rsibot context/features and calendar/hardware
qualification remain open. cryptofeed owns acquisition/storage/replay, rsibot
owns strategy intelligence, and hlscreen remains a consumer. No deployment,
economics, activation, merge or release follows from this evidence.
