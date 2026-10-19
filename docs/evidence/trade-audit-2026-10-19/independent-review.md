# Independent review record

Baseline source: `1c668981ee2d5b456dfa6d085fbd558b1998937d`, 2026-10-19.
Three separate delegated reviewers performed source/consumer/documentation work
and reported to the primary editor. This is independent agent review, **not** an
external human approval, operational sign-off or repeated primary self-review.
No reviewer ran an economic evaluation, changed strategy ownership or mutated
frozen inputs. Reviewer source suggestions are not represented as executed tests.

| Reviewer | Scope / evidence actually examined | Result |
|---|---|---|
| `trade_review` | all nine families' native semantics, engine identity/recording/replay boundaries, new tools/spec/plan/report | confirmed batched loss, aggregation/type, exact numeric, session/ordinal, durability and replay limitations; independently recomputed pilot counts and payload replay |
| `consumer_review` | actual current owner readers, pinned preserved snapshots and full five-pair frozen candle grids; BNB raw/derived/bundle; candidate storage prototype | no MFR1 trade bridge; strict finality/quote-volume blockers; USDT-source USDC-proxy identity; hash/grid/parity checks run read-only; Python exact storage probe measured |
| `venue_sources` | factories, catalog product exclusions, current primary venue documents and endpoints/history limits | 19 real IDs / 31 product scopes; current INTX migration blocker, OKX aggregate semantics, routed Binance contract and contradictory Gemini retention captured |

## Objections and dispositions

1. **Raw session vs default normalized session identity** — confirmed by separate
   call-path inspection and measured events. Added finding F04. Original capture
   remains unchanged. Analyzer resolves a unique venue-local symbol only for
   labeled value accounting, records mismatch, and rejects ambiguity.
2. **Manifest durability** — staged manifest contents must flush/sync before
   atomic no-clobber publication and directory sync. Added explicitly to spec,
   P2 acceptance and crash boundaries. This is planned behavior, not a repair.
3. **Historical raw lineage** — MFR1 offsets cannot address arbitrary archives.
   Added tagged lineage kind, archive member/codec/decoded coordinate and a
   historical acquisition context distinct from real live connections.
4. **Corrections/cancellations** — flags alone were insufficient. Added operation,
   scoped target, revision order/ID/known-at policy and conditional values;
   cancellations do not manufacture zero/negative executions.
5. **Deterministic tie-breaking** — local ordinals collide across collectors.
   Added collector/run/context/session/generation/object tie-breaks. Observed mode
   preserves receipt chronology, without event-time resorting away causality.
6. **Signed aggregate volume** — original strict signed-orderflow profile would
   exclude legitimate aggregate-volume studies. Renamed to signed-execution-flow;
   a weaker use needs explicit separate admission. Disagreement resolved without
   weakening individual-execution count requirements.
7. **Pilot boundaries/provenance** — reject destinations outside fresh evidence,
   symlinks and network/private sinks; distinguish soft storage stop threshold;
   hash binary and record dirty status; launch captured config and verify metrics
   bind. Implemented in audit tooling after original pilot. Original config paths
   were contained/reviewed; original binary hash was measured only afterward.
8. **Test coverage** — added tests for two venues using instrument1, public/business
   catalog duplication, ambiguous map rejection, path escape/network sinks and
   telemetry identity. Nine Python tests pass. No hard-quota/cap-overshoot fault
   injection is claimed; soft threshold is documented.
9. **Cargo-deny report** — final compatible parser is 0.19.0, not the initial
   missing/old-parser state. Final advisories fail on locked rustls; bans,
   licenses and sources pass. Recorded exact exit/log and public advisory IDs.

## Independent recomputation

`trade_review` reran the current native analyzer against original bulk artifacts
and confirmed equality with tracked native-audit JSON. Eligible raw and normalized
trades:4853 each; additional administrative records:4. Native OKX counts:spot70
rows→130, swap375→771. Raw/sink reconstruction:445 versus445 exact semantic
payloads as a multiset, zero missing/extra. Both raw segment hashes/sizes and all17
fixture payloads/38309 total bytes checked. Kraken Futures100 snapshot-only;
Bitfinex30 snapshot rows per product; Coinbase1 last_match+346 matches; Advanced
100 snapshot+348 update rows. Baseline workspace1123 passed/56 ignored; nine
Python boundary tests pass.

No unresolved design disagreement remains in the reviewed specification/report.
All ledger defects remain open implementation tasks. Human PR review and CI
remain separate; a draft PR is appropriate while required checks are incomplete.
