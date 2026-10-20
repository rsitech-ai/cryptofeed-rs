# Causal journal: integration contract

This continues D5 milestone B and the registered failed-ingress repair. The
bounded writer and private offline capture/drain executor are prerequisites;
they are not the requested live integration or complete durable replay.

## Required execution path

1. Recording owns a bounded profile/version/routing envelope and opaque payload;
   engine owns exact bootstrap/input/effect/outcome DTOs and decoding. Avoid a
   recording-to-engine dependency. Negotiate a distinct causal header/profile
   before creating a factory or running an operation; old readers must reject
   unsupported profiles immediately. A new record kind discovered halfway through
   an otherwise legacy feed is insufficient negotiation.
2. Commit a lossless versioned bootstrap before first execution: source/build/
   lock/features and actual executable hash, adapter and typed category/config,
   ordered session channels and
   delivery policy, exact full catalog, runner/capture limits and overflow,
   venue/session identity and immutable reproduction inputs. Existing sanitized
   endpoints and Debug-formatted metadata cannot reconstruct factory policy.
   A first public Bybit/null-sink profile may explicitly restrict supported
   configurations; unsupported adapters/sinks/configs fail before connecting.
3. Own the actual persistent runner privately. Start with one executed,
   unacknowledged turn per session and independent bounded ingress. Keep machine,
   mutable state, metrics, stop/readiness handles, books and dispatcher from
   escaping through legacy accessors. Replay injects recorded connection identity.
4. Preserve `Idle → Executed/AwaitingAdmission → AwaitingCommit → Committed`,
   with terminal failure branches. A full writer queue after mutation must retain
   the same prepared operation and retry admission only, with an absolute
   deadline. Never execute it again. Incomplete/sensitive capture, post-execution
   encoding overflow, writer closure or terminal error poisons the owner; none
   constitutes rollback or safe retry. Size the complete encoded envelope before
   admission; logical capture budgets are not encoded-record bounds.
5. A ticket is admission. Release only after its matching post-barrier durable
   receipt. Serve committed snapshots rather than querying speculative books.
   Immediate safety revocation remains allowed. A late ACK must not revive a
   stopped owner or superseded generation. Settle accepted work even if a caller
   drops its waiter. Preserve attempted/never-attempted/indeterminate distinctions.
6. For a declared null-sink/disk-consumer profile, execute the callback and actual
   deterministic null/write/HTTP drains in one captured turn, journal it once and
   withhold removed values until ACK. For external sinks, commit an intent naming
   eligible fronts/prefixes before I/O, then apply recorded results through actual
   forwarding in a completion operation. A rejected front remains queued. Bind
   results to the exact prior intent/items; a result vector alone is not evidence
   that the external delivery happened. Implement or explicitly reject configured
   sinks rather than silently disabling them. Transport automatic control replies
   also need an explicit capture disposition; they cannot silently bypass a claim
   that all wire release is gated.
7. Keep produced, queue-admitted/evicted and drained/released effects distinct.
   Queue admission does not establish observed availability: a later eviction or
   failed turn can strand the item. Derive one deliberate compatibility projection
   from committed release outcomes. An archival-produced profile, if supported,
   is separate from observed availability. Consumers must not apply both causal
   operations and the compatibility projection.
8. Implement replay in engine or above it. Process bootstrap/metadata in physical
   order, instantiate the bound factory, execute actual turns and compare effects,
   dispositions, failures, drain outcomes and state. No metadata prescan,
   callback-only reconstruction, inferred drains or recreated reconnect machines.
   Migrate actual daemon/live/control/supervisor/consumer paths explicitly. Seal
   after all owners and accepted completions settle. Raw/shared files and external
   I/O retain separate uncertain crash windows; no exactly-once claim follows.

## Exact values and acceptance

Use explicit decimal coefficient plus scale DTOs, or proven exact typed RawValue
leaves, through every financial action/queue/eviction/drain/state/bootstrap path.
Do not route values through serde_json::Value or buffered tagged-enum content.
Preserve ordered protocol lists; canonicalize only genuinely unordered maps.
Declare direct serde dependencies where used rather than relying on transitives.

Run actual owner/actor/replay tests for delayed ACK with zero premature release,
admission-full retry without second execution, idle-session groups, publication
failure/panic/caller cancellation, partial callback failure, timer polls,
reconnect generations, immediate revocation/late ACK, drain-dependent overload,
sink rejected fronts, delivery crash uncertainty and replay-prefix equality.
Only after integration compare the unchanged registered ingress schedule,
capacities, thresholds and abort rules. Retain the failure if it persists.

Native semantics, units/continuity, strict dataset admission, rsibot context,
calendar canaries/24h/7d and native hardware gates remain part of the full goal.
Strategy features/models/ETF/decisions stay in existing rsibot owners; hlscreen
consumes the shared data. No economics, activation, deployment, merge or
operational promotion is granted by this infrastructure contract.
