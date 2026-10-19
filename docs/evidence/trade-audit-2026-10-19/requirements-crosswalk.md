# Requested audit deliverable crosswalk

This crosswalk checks the original 14-section assignment. Completion of the
specification/plan PR does not mean implementation or operational promotion.
Every unresolved implementation/access requirement has an explicit non-passing
status and file-level acceptance work; it is not silently omitted.

| Requested section | Current authoritative evidence | Limits / disposition |
|---|---|---|
|1 Source/contracts/prior work | baseline SHA/clean status/open-PR inspection in report; workspace28 crates; model/adapter/transport/engine/dispatch/recording/replay/sinks/daemon/analytics/EventPulse call paths; owner read-only source/blob pins | prior linked production spec absent; owner bootstrap path unavailable; recorded as unavailable, not read |
|2 Exchange/trade matrix | inventory JSON:19 real IDs,31 product subfamilies,37 protocol/channel scopes; synthetic/excluded/auth/legacy scopes explicit; exact pilot rows and bounds |17 trade-receipt scopes; one futures failure; INTX blocked; other product/instrument/channel scopes not-run/unsupported |
|3 Trade meaning | primary sources accessed2026-10-19; authentic17 fixtures; independent review; batching/numeric synthetic failures; native OKX constituent counts; schema fields | no universal side/time/quantity/continuity claim; future golden/metadata work S1–S6 |
|4 Stream→durable delivery | trace and source failure boundaries; required recorder/file pilot, raw/sink balances and metrics; current failure harness gate | fsync/transactional boundary absent; new live crash/disk/slow-sink injection not performed; D1–D3 planned |
|5 Recovery | endpoint capability/retention limits and contradictory docs in inventory; explicit gap/watermark/pagination/failure state contract | no new trade backfill/disconnect recovery proof; no ID-consecutiveness assumption; D4 acceptance defined |
|6 Three evidence paths | native samples/challenges; six venues' bounded official reference intersections;445 OKX raw-reconstructed payloads and actual owner-cache reader-contract checks | repeated decoder use not independent truth; no full same-venue coverage proof or new actual trade-reader bridge |
|7 Storage/schema | exact field/null/unit/lineage/revision schema; raw sidecar compatibility; sync/manifest publication; measured Python Parquet exact layout and resource/timing probe | proposed canonical writer/reader not implemented; Rust throughput/CPU/burst/sync/restore metrics unmeasured; P1/P2 gates |
|8 Qualification/manifests | use-specific profiles, transitions, immutable revisions, pinned readers, rejection rules; capture/reference/probe manifests and hashes | existing manifests are raw/probe inventories, not qualified dataset publication; P2/P3 required |
|9 Actual OKX compatibility | concrete current+snapshot owner readers, pins, five frozen candle grids/hashes, BNB raw/derived/bundle parity, field mapping | native-USDC/EU spot/trade parity not demonstrated; finality/quote-volume/bridge blocked; legacy behavior unchanged |
|10 Candles/causality |84 native candle updates observed; finality loss finding; independent contract for derived/native candles, availability modes and deterministic tie/prefix rules | no derived-vs-official candle reconciliation or full engine causal replay; unknown arrivals remain unknown |
|11 Bounded acquisition | inventory first; original plan/config/capture with actual UTC interval, shutdown and hashes;19 capped reference GET objects; future gated JSON plan | original absolute bounds resolved at forward launch, storage threshold soft, catalog request budget not instrumented; no archives/corpus replacements/services; qualification stages blocked explicitly |
|12 Meaningful verification |1123 offline passes/56 ignored; final daemon and Clippy; nine Python boundary tests; native challenges; actual frozen-cache checks; candidate storage tests; gate receipts | cargo-deny final advisories fail; gitleaks scan skipped; calendar canary/soak and unimplemented crash/recovery/reader tests remain planned |
|13 Implementation plan | phases0–6, task IDs/exact modules/reuse/owners/deps/tests/measurable acceptance/estimates/migration and rollback | engineering estimates exclude access/calendar time; source/coverage and operational blockers distinct |
|14 Documentation/review/PR | spec/plan/report, corrected prior matrix claims, evidence + finding ledger, separate reviewer record, focused branch and draft PR | human review and PR-level checks remain distinct; no merge/deploy/orders/economic run authorized |

Frozen economic inputs were only inspected. No bytes were redownloaded,
substituted, shortened or overwritten, and no economic evaluation ran. No new
dataset was admitted. All raw/probe outputs are outside the frozen owner corpus.
