# Active goal extension: time-qualified cross-venue observations

Accepted user scope, 2026-10-19. This extends the active Hyperliquid/shared-feed,
exact persistence and exchange-integrity goal; it does not replace unfinished
repairs. The supplied market-context review is a set of hypotheses and contract
requirements, not verification of current code or evidence of profitability.

## Objective and authority

Build an auditable observation layer that can help existing OKX spot strategies
distinguish persistent selling from patterns consistent with selling exhaustion.
Qualify data and timing first, then implement reusable descriptive features and
an additive owner integration. Preserve the existing 1x cash/inventory model,
sizing, DCA, exits, persistence and native execution ownership.

Authority is repository-process-only and research infrastructure. No orders,
paper/live activation, economic run, frozen-corpus substitution or production
deployment follows from this goal. Existing frozen scoring window remains
[2026-05-01T00:00:00Z,2026-09-01T00:00:00Z). Missing external historical bytes or
an unsupported meridian_v3 owner/baseline make that comparison unavailable.
Forward captures do not reconstruct those missing historical inputs.

## Owners and current evidence

cryptofeed-rs owns exact acquisition, native semantics, validation, persistence,
replay and generic rendering-neutral primitives. hlscreen consumes the pinned
shared feed. rsibot owns the strategy-context program: Quants Lab owns cross-venue
features, time-qualified model/ETF integration, economics and accounting; Research
OS owns hypotheses and assessments; Hummingbot owns admitted entry decisions,
orders, inventory and exits. Inspect and
reuse existing cross-venue-v2 contracts before touching those owner repositories.
No second strategy engine, research platform or exchange collector is authorized.

Hyperliquid spot/perps shared capture, saved replay and bounded dataset recovery
have measured evidence in the companion ledger. Strict research admission is
still denied. Numeric venue and OKX batch/finality/readiness repairs are tested;
Binance provenance/classification repair is committed at8fb517d; the first
rsibot/Quants Lab manifest boundary is locally implemented and independently
reviewed, including real read-only-copy integrity and strict denial checks. None of this establishes
cross-venue alpha, whole-window continuity or sustained operational promotion.

## Implementation sequence and acceptance

| Work | Required result and checks | Status |
|---|---|---|
| Integrity prerequisites | Finish native granularity/side/unit/origin/time repairs, visible exclusions, scoped readiness, exact storage and mixed replay; independent mutation/failure tests | In progress |
| Data feasibility | Inventory immutable historical bytes, eligible instruments, source/quote/product identity, timing and actual owner readers; preserve unavailable/blocked dispositions | Prior audit exists; extend for this experiment |
| Observation contract (rsibot owners) | Versioned source identity, native product and semantic basis, event/receipt/usable cutoffs, coverage/freshness, conversion provenance and feature/model version; deterministic replay and unavailable states | Next rsibot-owned integration slice |
| Small observation set | OKX spot execution reference, Binance spot, Coinbase spot, one best-qualified overlapping perp feed; optional Kraken. Per-symbol admission; external benchmark excludes OKX | Selection pending quality/coverage evidence |
| Descriptive features (Quants Lab) | Predeclared return/agreement/dispersion windows, per-venue signed-flow coverage and normalization, price response to selling, separate derivative stress, as-of breadth/relative strength | Pending contract/data gates |
| Owner bridge | Add context to existing decision trace with timing/quality; stale required inputs become unavailable while inventory/exits remain operational; legacy parity/isolation tests | Pending owner review |
| First comparison | Unchanged supported baseline vs one predefined entry veto; sizing/DCA/exits fixed, reduction-in-activity control, rejected-entry accounting, chronological/purged evaluation, registered latency and1/5/30s delays | Unavailable until historical and owner gates pass; no economics run |
| Model attribution | OKX-only regularized baseline, same baseline plus external features, constrained nonlinear baseline, then frozen Kronos feature. Pin checkpoint/code/cutoff and inference completion | Later, separate measured hypotheses |
| Slower context | ETF creation estimates with publication/vintage/settlement/corporate-action basis; macro vintages and actual availability; options/on-chain only with scoped hypotheses | Later, source feasibility first |
| Operational qualification | Calendar-spaced canaries, declared24h/7d soak, resource/failure/recovery evidence and ownership | Not run; existing gates retained |

## Mandatory semantic and timing safeguards

- A venue is not an independent vote merely because it has another product ID.
  Verify Coinbase USD/USDC source aliases and deduplicate observations/volume.
- Quote conversion requires an explicit time-qualified conversion observation;
  USD, USDT, USDC and EUR are not silently interchangeable. Derivatives retain
  contract units, settlement and conversion metadata and remain observation-only.
- Signed flow reports unknown-side coverage. One aggregate row is not one
  execution; excluded/admin rows remain traceable and counted. Normalize activity
  against each venue's own trailing history before combination.
- Bybit all-liquidation position side and bankruptcy price must remain distinct
  from ordinary trade aggressor side and execution price. Verify against the
  exact native channel before exposing derivative stress features.
- External benchmarks exclude OKX. Feature universes, peer membership, trailing
  normalizers and estimated sensitivities use only then-available data.
- Usable time includes acquisition, completion and processing/inference. Late
  backfills, confirmations, revisions and future volume-bar completion times
  cannot enter earlier decisions. Preserve actual unknown availability.
- Required stale/gapped/unqualified inputs yield an explicit unavailable state;
  never silently substitute a favorable venue or relabel unknown as no activity.
- A model forecast carries horizon, source cutoff, coverage, freshness, model/
  checkpoint version and inference completion. Extra Kronos columns or separate
  batch items are not assumed to supply joint external covariates. Mean sampled
  forecasts are not calibrated profitable-trade probabilities.
- ETF secondary volume, shares, creations, AUM and premium/discount are different
  measurements. Preserve source versions, first-seen publication and later
  revisions; creations do not establish a future spot purchase. Macroeconomic
  revised history cannot impersonate an intraday historical vintage.

## Validation and rollback

Each slice needs exact fixtures and adversarial cases: duplicate source aliases,
missing conversion, unknown sides/units, malformed data, clock jumps, gaps,
staleness, future/late inputs, normalizer leakage and replay prefix equality.
Use current workspace gates and independent reviews, retaining ignored live
tests and unrun operational gates explicitly. Owner integration additionally
requires frozen legacy behavior and dependency-isolation tests. Features are
opt-in/versioned; rollback pins the prior feed/manifest/feature version without
rewriting original evidence or changing native strategy ownership.

This plan is a completion contract. A specification or passing fixture is not
implementation, an admitted dataset, a profitable model or a deployment sign-off.


## Placement correction from the user follow-up

The user clarified that strategy-context implementation belongs in rsibot.
Cross-venue feature combination, model/ETF context, experiment registration and
strategy admission will therefore be implemented through existing rsibot owner
packages and contracts. No strategy-specific context engine will be added to
cryptofeed-rs. Finish feed-integrity prerequisites here and inspect the existing
owner source/pins/dirty state before an additive rsibot change. Generic exact
market-data primitives may remain here when they have no strategy ownership.

The concrete implementation boundary is:

| Repository | Implementation |
|---|---|
| cryptofeed-rs | Exchange acquisition, exact native values and instrument identity, receipt/source timestamps, quality events, authoritative live publication, raw/canonical storage and replay |
| rsibot | Shared-feed and immutable-dataset readers; cross-venue alignment, availability, feature combination, models and strategy decision traces in existing owner packages |
| hlscreen | Shared-feed reader and presentation |

rsibot reads cryptofeed directly rather than using hlscreen as a data source.
Consumers advance independently over versioned schemas and durable cursors;
research inputs also pin immutable dataset manifests. Shared identity/provenance
and explicit freshness/quality checks keep consumers consistent without requiring
them to process every event simultaneously. An unavailable required input remains
unavailable in rsibot. Collection recovery and exact replay remain feed concerns;
entry-context admission and legacy inventory/exit behavior remain owner concerns.

The first Quants Lab change is a local read-only manifest inspector and admission
boundary. It does not yet implement a live context consumer or strategy features.
The next owner implementation must inspect the existing cross-venue contracts,
add one opt-in reader and deterministic availability tests, and demonstrate
unchanged legacy behavior before introducing a predefined research hypothesis.
