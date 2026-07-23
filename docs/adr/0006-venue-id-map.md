# ADR 0006: Canonical VenueId map

**Status:** Accepted  
**Date:** 2026-07-21  
**Spec:** domain `VenueId` (`u16`); registry [`docs/plan/venue_ids.md`](../plan/venue_ids.md)

## Decision

`VenueId` is a global `u16` in `marketfeed_model`. Parallel workers MUST claim IDs in `docs/plan/venue_ids.md` before coding. Do not invent overlapping IDs. Same exchange / different segments get distinct IDs when they are distinct `VenueSpecification`s.

Assigned range at CODE plateau: **1–18** (0 reserved; next free **19**). New IDs are product expansion only — they do not unlock beta/stable/1.0.

## Why

- Early parallel adapters collided on 3/4; a single registry prevents silent cross-venue bugs.
- Status/catalog/R6 tags and daemon factories key off stable IDs.

## Consequences

- Claim → constant → fixtures/canaries in that order.
- Registry file is authoritative; drive/orchestrator docs must not invent IDs.
- Collision history (OKX/Bybit/Kraken remaps; Coinbase off 14/15 → 16) stays documented in the registry.
