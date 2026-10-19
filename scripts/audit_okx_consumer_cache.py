#!/usr/bin/env python3
"""Read-only checks of the existing OKX consumer corpus; never runs economics.

Requires numpy and pyarrow in the explicitly selected Python environment. This
script does not install packages, contact a venue, repair files, import owner
modules (some create directories), or change manifests. Baseline manifest and
bundle digests identify the frozen files independently of their own receipts.
"""

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import sys

sys.dont_write_bytecode = True


START, END, STEP = 1777593600, 1788220800, 60
ROWS = (END - START) // STEP
CORPUS = Path("output/data_backfills/spot_corpus_okx_usdt_2025_07_2026_08")
BUNDLE = Path("output/backtests/meridian_v3/laptop2/bundles/bnb_mayaug26_okx")
SNAPSHOTS = Path("output/backtests/meridian_v3/laptop2/src")
MANIFEST_SHA = "fe159bf4069198acc92280310f79fbdc578bf01bd7e753adf7caeee348d85133"
BUNDLE_SHA = "2a6d9351f3ddb24e2c0ac570e6710e79d7df363011153f226c7fb980709c5f7b"
Q_COMMIT = "6c4ed9a6feadd2c3a658ae7a8d4bde1f7deecb36"
Q_LEGACY_COMMIT = "79069cc47f2f39ecc1672fa04ba81c3bc8c78870"
H_COMMIT = "356b3da6e35fff5b81dcbc7390e035f83267da22"
COLUMNS = ("timestamp", "open", "high", "low", "close", "volume",
           "quote_asset_volume", "n_trades", "taker_buy_base_volume",
           "taker_buy_quote_volume")
PAIRS = {f"{base}-USDC" for base in ("BNB", "ETH", "BTC", "SOL", "XRP")}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def fingerprint(path):
    stat = path.stat()
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns


def receipt(path, cap=128 * 1024 * 1024):
    require(path.is_file(), f"missing regular input: {path}")
    before = fingerprint(path)
    require(before[2] <= cap, f"input exceeds bounded audit byte cap: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    require(before == fingerprint(path), f"input changed while hashing: {path}")
    return {"path": str(path), "bytes": before[2], "sha256": digest.hexdigest()}


def read_json(path):
    require(path.stat().st_size <= 1024 * 1024, f"JSON exceeds 1 MiB cap: {path}")
    with path.open(encoding="utf-8") as stream:
        return json.load(stream)


def bound_path(root, value):
    path = Path(value)
    if not path.is_absolute():
        path = root / path
    path = path.resolve(strict=True)
    require(path.is_relative_to(root), f"receipt is outside --quants-root: {path}")
    return path


def checked_receipt(root, expected):
    path = bound_path(root, expected["path"])
    actual = receipt(path)
    require(actual["sha256"] == expected["sha256"], f"SHA256 mismatch: {path}")
    if "bytes" in expected:
        require(actual["bytes"] == expected["bytes"], f"byte-count mismatch: {path}")
    actual["receipt_matches"] = True
    return path, actual


def git(root, *args):
    result = subprocess.run(["git", "-C", str(root), *args], capture_output=True,
                            check=True, timeout=20)
    return result.stdout


def source_inventory(root):
    definitions = [
        ("quants-lab", None, "core/backtesting/engine.py",
         ["BacktestingEngine._load_candles_cache", "BacktestingEngine.load_candles_cache_by_connector_pair"]),
        ("quants-lab", None, "core/data_sources/clob.py", ["CLOBDataSource.load_candles_cache"]),
        ("hummingbot", None, "hummingbot/data_feed/candles_feed/okx_spot_candles/okx_spot_candles.py",
         ["OKXSpotCandles._parse_rest_candles", "OKXSpotCandles._parse_websocket_message"]),
        ("quants-lab", ("q-6c4ed9a", Q_COMMIT), "scripts/research/build_corpus_minute_bundle.py", ["build"]),
        ("quants-lab", ("q-79069cc", Q_LEGACY_COMMIT), "scripts/data/ok_rsi_may_aug_spot_cache.py",
         ["audit_trades", "audit_seconds"]),
        ("quants-lab", ("q-79069cc", Q_LEGACY_COMMIT), "scripts/research/ok_rsi_zero_fill_repair.py", ["prepare"]),
        ("hummingbot", ("hb-356b3da", H_COMMIT), "services/okx-candle-service/src/model.rs", ["parse_row", "history_valid"]),
        ("hummingbot", ("hb-356b3da", H_COMMIT), "hummingbot/strategy_v2/components/candle_stream.py",
         ["CandleStreamClient._row", "CandleStreamClient._apply"]),
        ("hummingbot", ("hb-356b3da", H_COMMIT), "hummingbot/strategy_v2/components/candle_data.py",
         ["Candle", "CandleSnapshot.continuity", "CandleSnapshot.frame", "Replica"]),
    ]
    owners = {"quants-lab": root, "hummingbot": root.parent / "hummingbot"}
    result = []
    for owner, snapshot, relative, functions in definitions:
        owner_root = owners[owner]
        path = root / SNAPSHOTS / snapshot[0] / relative if snapshot else owner_root / relative
        row = {"owner": owner, "path": str(path), "functions": functions,
               "kind": "preserved_source_snapshot" if snapshot else "current_checkout",
               "functions_executed_by_audit": False}
        row["commit"] = snapshot[1] if snapshot else git(owner_root, "rev-parse", "HEAD").decode().strip()
        row.update(receipt(path))
        blob = git(owner_root, "show", f"{row['commit']}:{relative}")
        row["matches_commit_blob"] = hashlib.sha256(blob).hexdigest() == row["sha256"]
        require(row["matches_commit_blob"], f"reader bytes differ from recorded commit: {path}")
        result.append(row)
    return result


def audit(root, np, pq):
    manifest_path = root / CORPUS / "manifest.json"
    manifest_before = fingerprint(manifest_path)
    manifest_receipt = receipt(manifest_path)
    require(manifest_receipt["sha256"] == MANIFEST_SHA, "frozen manifest differs from audit baseline")
    manifest = read_json(manifest_path)
    require(manifest["schema"] == "rsibot.spot_modular.corpus_okx_spot_data.v1", "unexpected corpus schema")
    require({d["pair"] for d in manifest["datasets"]} == PAIRS and len(manifest["datasets"]) == 5,
            "expected exactly five frozen candle datasets")
    expected_grid = np.arange(START, END, STEP, dtype=np.int64)
    report = {"schema": "cryptofeed.okx_consumer_cache_audit.v1", "status": "PASS_BOUNDED_CACHE_CHECKS",
              "authority": "READ_ONLY_VERIFICATION_NO_ECONOMIC_EXECUTION",
              "generated_at_utc": datetime.now(timezone.utc).isoformat(),
              "runtime": {"executable": sys.executable, "python": platform.python_version(),
                          "numpy": np.__version__, "pyarrow": __import__("pyarrow").__version__},
              "quants_root": str(root), "manifest": manifest_receipt,
              "window": {"start": START, "end_exclusive": END, "step_seconds": STEP,
                         "start_utc": "2026-05-01T00:00:00Z", "end_exclusive_utc": "2026-09-01T00:00:00Z",
                         "expected_rows_per_pair": ROWS}, "datasets": [],
              "limitations": ["USDT source instruments are logical USDC consumer proxies, not native USDC data",
                              "Candle values are float64; this is not exact-decimal evidence",
                              "Raw-to-derived parity is checked only for BNB",
                              "Cached API pages are not an independent trade/reference reconciliation",
                              "No live transport, economic evaluation, dataset admission or operational promotion is tested"]}
    bnb = None
    for dataset in manifest["datasets"]:
        path, row = checked_receipt(root, dataset)
        before = fingerprint(path)
        table = pq.ParquetFile(path)
        require(set(table.schema_arrow.names) == set(COLUMNS), f"unexpected candle columns: {path}")
        stamps = table.read(columns=["timestamp"])["timestamp"].to_numpy()
        require(stamps.dtype == np.dtype("int64"), f"timestamps must be int64: {path}")
        require(bool(np.all(np.diff(stamps) > 0)), f"unordered or duplicate full timestamp grid: {path}")
        mask = (stamps >= START) & (stamps < END)
        scored = stamps[mask]
        require(np.array_equal(scored, expected_grid), f"incomplete scored minute grid: {path}")
        row.update({"pair": dataset["pair"], "source_instrument": dataset["source_instrument"],
                    "full_rows": len(stamps), "scored_rows": len(scored), "first": int(scored[0]),
                    "last": int(scored[-1]), "scored_grid_exact": True,
                    "schema": {field.name: str(field.type) for field in table.schema_arrow},
                    "codecs": sorted({table.metadata.row_group(0).column(i).compression for i in range(len(COLUMNS))}),
                    "declared_continuity": dataset["continuity"], "evidence": "hash_and_actual_timestamp_grid"})
        if dataset["pair"] == "BNB-USDC":
            frame = table.read(columns=list(COLUMNS))
            bnb = dataset, {name: frame[name].to_numpy()[mask] for name in COLUMNS}
        require(before == fingerprint(path), f"Parquet changed during inspection: {path}")
        report["datasets"].append(row)

    dataset, derived = bnb
    seen, confirmations, field_counts, sources = {}, Counter(), Counter(), []
    overlaps = conflicts = 0
    selected = [s for s in dataset["sources"] if s["month"] in ("2026-05", "2026-06", "2026-07", "2026-08")]
    require(len(selected) == 4 and len({s["month"] for s in selected}) == 4, "missing or duplicate BNB scored raw month")
    for source in selected:
        path, info = checked_receipt(root, source)
        before = fingerprint(path)
        pages = scoped_rows = 0
        with path.open(encoding="utf-8") as stream:
            for line in stream:
                pages += 1
                require(pages <= 2000, f"raw page cap exceeded: {path}")
                page = json.loads(line)
                require(page["request"]["instId"] == "BNB-USDT" and page["request"]["bar"] == "1m",
                        f"unexpected raw source identity: {path}")
                for raw in page["data"]:
                    require(len(raw) == 9 and all(isinstance(v, str) for v in raw), "invalid raw candle shape")
                    milliseconds = int(raw[0])
                    require(milliseconds % 60000 == 0, "raw timestamp not minute aligned")
                    timestamp = milliseconds // 1000
                    if not START <= timestamp < END:
                        continue
                    scoped_rows += 1
                    confirmations[raw[8]] += 1
                    field_counts[len(raw)] += 1
                    if timestamp in seen:
                        overlaps += 1
                        conflicts += int(seen[timestamp] != raw)
                    seen[timestamp] = raw
                    require(len(seen) <= ROWS, "raw unique row cap exceeded")
        require(before == fingerprint(path), f"raw file changed during inspection: {path}")
        info.update({"month": source["month"], "pages": pages, "scoped_returned_rows": scoped_rows})
        sources.append(info)
    require(conflicts == 0, "conflicting overlapping raw candles")
    require(set(confirmations) == {"1"}, "unconfirmed scored raw candle")
    ordered = sorted(seen)
    require(np.array_equal(np.asarray(ordered, dtype=np.int64), expected_grid), "raw BNB scored grid incomplete")
    raw_values = np.asarray([[seen[t][i] for i in (1, 2, 3, 4, 5, 7)] for t in ordered], dtype=np.float64)
    derived_values = np.column_stack([derived[name] for name in COLUMNS[1:7]])
    require(bool(np.isfinite(raw_values).all()), "nonfinite raw BNB value")
    mismatches = int(np.count_nonzero(np.any(raw_values != derived_values, axis=1)))
    require(mismatches == 0, "raw BNB to derived float parity mismatch")
    report["bnb_raw_parity"] = {"sources": sources, "unique_scored_rows": len(seen),
                                "overlap_rows": overlaps, "conflicting_overlap_rows": conflicts,
                                "confirmation_counts": dict(confirmations), "row_field_counts": dict(field_counts),
                                "raw_grid_exact": True, "float_parity_mismatched_rows": mismatches,
                                "compared_raw_fields": [1, 2, 3, 4, 5, 7],
                                "unavailable_columns_all_nan": {name: bool(np.isnan(derived[name]).all())
                                                                for name in COLUMNS[7:]}}

    bundle_path = root / BUNDLE / "bundle.json"
    bundle_before = fingerprint(bundle_path)
    bundle_receipt = receipt(bundle_path)
    require(bundle_receipt["sha256"] == BUNDLE_SHA, "frozen bundle JSON differs from audit baseline")
    bundle = read_json(bundle_path)
    require(bundle["schema"] == "spot_modular.minute_bundle.corpus.v1" and bundle["window"] == [START, END],
            "unexpected bundle schema/window")
    require(bundle["corpus_manifest"]["sha256"] == MANIFEST_SHA and bundle["corpus_parquet"]["sha256"] == dataset["sha256"],
            "bundle parents differ from inspected corpus")
    npz_path, npz_receipt = checked_receipt(root, bundle["minutes"])
    npz_before = fingerprint(npz_path)
    with np.load(npz_path, allow_pickle=False) as arrays:
        require(set(arrays.files) == set(COLUMNS), "unexpected bundle NPZ columns")
        times = arrays["timestamp"]
        require(times.dtype == np.dtype("int64") and times.ndim == 1, "bundle timestamp dtype or dimension mismatch")
        require(np.array_equal(times, np.arange(bundle["first_timestamp"], END, STEP, dtype=np.int64)),
                "bundle warmup/scored grid incomplete")
        mask = (times >= START) & (times < END)
        require(np.array_equal(times[mask], expected_grid), "bundle scored grid incomplete")
        require(len(times) == bundle["rows"], "bundle row count differs from declaration")
        for name in COLUMNS[1:]:
            values = arrays[name]
            require(values.dtype == np.dtype("float64") and values.shape == times.shape,
                    f"bundle value dtype or shape mismatch: {name}")
            require(np.array_equal(values[mask], derived[name], equal_nan=True), f"bundle scored values differ: {name}")
        report["bundle"] = {"manifest": bundle_receipt, "minutes": npz_receipt,
                            "rows": len(times), "first": int(times[0]), "last": int(times[-1]),
                            "scored_rows": int(mask.sum()), "complete_grid": True,
                            "scored_values_match_corpus": True,
                            "dtypes": {name: str(arrays[name].dtype) for name in COLUMNS},
                            "source_instrument": bundle["corpus_dataset"]["source_instrument"],
                            "trading_rules_source": bundle["trading_rules_source"]}
    require(npz_before == fingerprint(npz_path), "bundle NPZ changed during inspection")
    require(bundle_before == fingerprint(bundle_path), "bundle JSON changed during inspection")
    require(manifest_before == fingerprint(manifest_path), "manifest changed during inspection")
    report["readers"] = source_inventory(root)
    report["repositories"] = {
        name: {"path": str(path), "commit": git(path, "rev-parse", "HEAD").decode().strip(),
               "branch": git(path, "branch", "--show-current").decode().strip() or None}
        for name, path in (("rsibot", root.parent), ("quants-lab", root),
                           ("hummingbot", root.parent / "hummingbot"))
    }
    report["script"] = receipt(Path(__file__).resolve())
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quants-root", type=Path, required=True)
    parser.add_argument("--json", action="store_true", help="emit the full verification report as JSON on stdout")
    args = parser.parse_args()
    try:
        import numpy as np
        import pyarrow.parquet as pq
    except ImportError as error:
        print(f"Required runtime dependencies numpy and pyarrow are unavailable: {error}. "
              "Select an existing environment containing them; nothing is installed automatically.", file=sys.stderr)
        return 2
    try:
        report = audit(args.quants_root.resolve(strict=True), np, pq)
    except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
        failure = {"schema": "cryptofeed.okx_consumer_cache_audit.v1", "status": "FAIL", "error": str(error)}
        print(json.dumps(failure, sort_keys=True) if args.json else f"FAIL: {error}")
        return 1
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True, allow_nan=False))
    else:
        print(f"{report['status']}: five hashed candle datasets, {ROWS} scored minutes each; "
              "BNB raw/Parquet/bundle parity passed. USDT proxy and float64 limits remain.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
