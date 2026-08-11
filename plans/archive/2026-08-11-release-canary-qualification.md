# Release canary qualification

## Goal

- Add a repeatable read-only qualification gate for the UI-enabled release binary.
- Produce a supervised one-hour GO/HOLD decision before any separate 24-hour beta gate.

## Authority boundary

- Public market data and read-only UI only.
- No audio, trading, order placement, credentials, CI remediation, push, PR, merge, deployment, or maturity promotion.

## Completed work

- Implemented deterministic canary analysis, isolated runner ownership, exact Git/binary/config metadata, readiness and UI smoke, venue/book/reconnect/resource/API/log gates, and graceful cleanup.
- Added host-local parser comparison without rewriting its noisy baseline.
- Diagnosed and repaired API lock starvation, CPU amplification, full book snapshot work, depth-history allocation, bubble rollover cloning, redundant adaptive calibration, and excess finalized-history retention.
- Ran focused analytics and daemon tests, full workspace tests/lint, UI tests/build, live UI smoke, short canaries, and exact one-hour canaries.
- Published the evidence-backed decision in `docs/ops/release_canary_qualification_2026-08-11.md`.

## Final decision

**HOLD on commit `7c97336f5656df7a6bdc099218b6a46325dc5652`.** The final 15-minute canary kept 13/13 venues live and passed books, reconnect allowance, queue, API, CPU, peak RSS, logs, UI smoke, and shutdown. RSS growth was 109.50 MiB/hour against a 64 MiB/hour limit.

The one-hour rerun and 24-hour beta gate remain deliberately unstarted after the final failed short gate.

## Recovery sequence

1. Attribute live heap and allocator residency by projection/instrument.
2. Compact adaptive calibration history or prove released-memory behavior.
3. Obtain an unwaived 15-minute GO.
4. Obtain an uninterrupted one-hour GO.
5. Run the separate 24-hour beta qualification.

## Rollback

- Revert only the isolated local performance commits if a semantic regression is found.
- Preserve timestamped ignored evidence.
- Never weaken thresholds to convert a failed qualification into a pass.
