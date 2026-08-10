# Reflection: CryptoFeed release audit

## Task

- **ID / title:** 2026-08-10 CryptoFeed release audit and integration
- **Date:** 2026-08-10
- **Scope:** Public read-only adapters, analytics, daemon API, and browser UI
- **Authority boundary:** Local repair, PR, and main integration; no audio, private data, trading, or order placement

## Success and Risk

- **Success criteria:** Exact public-data semantics, clean local gates, real release runtime, reviewed integration
- **Hypothesis 1:** Existing unit coverage would expose scale errors across multi-symbol sessions
- **Hypothesis 2:** Browser-derived DOM analytics would remain consistent with server fixed-point data
- **Hypothesis 3:** The profile basis label reflected a user-selectable server calculation
- **Rollback path:** Revert only the focused candidate commit or keep the PR unmerged

## Candidate Directions

| Candidate | Expected benefit | Main risk | Evidence before choice | Decision |
| --- | --- | --- | --- | --- |
| Preserve a session-wide L2 scale | Minimal code | Plausible but invalid mixed-symbol books | Live XRP/BNB scale rejection | rejected |
| Carry catalog scale per symbol | Exact reconstruction across one session | Slightly larger session config | Catalog already owns authoritative grids | retained |
| Expand depth history to 15 minutes immediately | Matches concept spec | Large string-heavy memory multiplier | A 3,000-sample response was already about 15.5 MB | rejected pending packed storage |
| Duplicate and clone a profile per basis | Simple API wiring | Hot-path cost grows with the session map | Coinbase watchdog failures appeared after profile growth | rejected |
| Select basis when snapshotting one accumulator | Same exact activity, bounded ingest cost | Small analytics API addition | Builder already accumulates both activity maps | retained |

## Evidence

- **First meaningful failure signal:** live OKX Swap and Bybit Linear rejected XRP/BNB book updates because their scales differed from the session's first instrument
- **Commands or runtime checks:** full workspace tests, live release restart, exact DOM queries, browser interaction, frame pacing, clean shutdown
- **What the evidence ruled in or out:** sequencing was healthy; catalog-to-session scale loss was the root cause

## Decision

- **Root cause:** per-session scalar metadata was reused for every instrument; the first basis-switch implementation also cloned growing profile maps on each trade
- **Retained fix:** per-symbol scale maps plus one exact profile accumulator with snapshot-time basis selection
- **Why alternatives were rejected:** coercing venue values would corrupt exact fixed-point semantics
- **Residual risk:** depth history should be packed before retention grows beyond five minutes
- **Rollback trigger:** any live book invalidation, scale mismatch, or frame-pacing regression on the qualified venues

## Reusable Lesson

- **Pattern to retain:** test multi-symbol sessions with deliberately different price and quantity scales
- **Pattern to avoid:** proving a multi-market adapter only with one catalog instrument
- **Where it applies next:** every multiplexed venue session and every exact numeric view projection
