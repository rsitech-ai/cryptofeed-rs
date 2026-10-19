# Exact offline Hyperliquid datasets

This tool publishes a sealed Hyperliquid public capture to a local immutable,
content-addressed store. It verifies original MFR1 frames against the MFSF1
journal, session metadata, runtime connection generations and accepted canonical
trades. It preserves raw bytes, catalog HTTP provenance, collector configuration,
build lockfile, duplicate sightings and exact trade rows. Export needs a fresh
raw directory containing only this capture's segments. An older capture lacking
required provenance is rejected; missing evidence cannot be reconstructed later.

Build with Rust 1.88 or newer:

```sh
cargo build -p marketfeed-datasets
```

Use signed integer nanoseconds for the requested event-time interval `[start,end)`.
Region must be evidenced; use `unknown` when the collector location is unknown.
Do not infer location from a workstation timezone.

```sh
target/debug/marketfeed-dataset export --store data/store --feed data/capture/feed \
  --raw data/capture/raw --dataset hl-pilot --start-ns 1700000000000000000 \
  --end-ns 1700000060000000000 --region unknown --expected-parent none
# Pin the returned manifest SHA256 explicitly for every read/backup.
target/debug/marketfeed-dataset inspect --store data/store --manifest MANIFEST_SHA256
target/debug/marketfeed-dataset backup --store data/store --manifest MANIFEST_SHA256 \
  --destination data/backup-new
target/debug/marketfeed-dataset restore --store data/backup-new --manifest MANIFEST_SHA256 \
  --destination data/restore-new
target/debug/marketfeed-dataset recover --store data/store
```

The store uses an exclusive OS lock, immutable SHA256 objects and manifests,
parent compare-and-swap, synced intent publication and atomic HEAD replacement.
Recovery finishes published intents idempotently; unreferenced staging files do
not authorize publication. Backup/restore verifies the pinned ancestry and all
referenced objects into a new destination. Existing destinations are refused.
Disk/filesystem sync errors fail the operation; actual power-loss durability also
depends on the filesystem and hardware honoring sync semantics.

Parquet coefficients are **signed 16-byte big-endian integers with separate uint8
scales**, preserving the full i128 and scale domains. They are not Decimal128 or
float values. Timestamps are signed int64 nanoseconds; native IDs are UTF8.
The typed financial columns are cross-checked against the complete lossless JSON
row contract in `docs/spec/hyperliquid_trade_row_v1.schema.json`. Untrusted reads
have footer, nested count, page, row, byte and object traversal limits. The reader
accepts bounded plain/dictionary data encodings; delta encodings are refused.

Current limits: 128MiB each for original raw and sealed journal, 4096 raw files,
100000 trade deliveries, 128MiB canonical JSON, 64KiB per row; reference/backup
walks are capped at 4096 unique objects and 512MiB. Partition longer acquisition
windows before these limits. This is an offline tool, outside the collector hot
path; it does not resume an existing live writer or recover missing source data.

**Inspection is not research admission.** Current policy leaves continuity,
execution granularity, native side meaning, quantity units and observed
availability unverified. First deliveries and reconnect overlap remain recorded.
Trades are deduplicated by venue/coin/time/tid; conflicting economics fail export.
Transport failures quarantine a dataset. All strict profiles are denied, including
execution counts, signed execution flow, confirmed candles and observed replay.
The tool currently exports Hyperliquid trades only. General venue policies,
mixed-input causal replay qualification and external backtest consumer integration
remain separate acceptance gates.

Validation includes corruption, coordinated provenance forgery, publication
cutpoints, recovery, backup/restore and exact-domain tests. The independent
`scripts/verify_exact_parquet.py` checks both Rust-to-PyArrow and PyArrow-to-Rust.
