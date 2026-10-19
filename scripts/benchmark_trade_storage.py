#!/usr/bin/env python3
"""Bounded Python/Arrow layout probe of existing normalized pilot trades.

Candidate evidence only: no Rust storage implementation, canonical dataset,
throughput qualification, raw-lineage repair, or economic execution. Inputs are
read-only; output directories and metrics are created exclusively, never reused.
Requires an existing pyarrow installation; no installation or network fallback.
"""

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import resource
import sys
import time

sys.dont_write_bytecode = True

MASK64 = (1 << 64) - 1
I64_MIN, I64_MAX = -(1 << 63), (1 << 63) - 1
I128_MIN, I128_MAX = -(1 << 127), (1 << 127) - 1
MAX_BYTES, MAX_LINES, MAX_TRADES = 64 * 1024 * 1024, 250000, 100000


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value, lower, upper, name):
    require(type(value) is int and lower <= value <= upper, f"out-of-range integer {name}")
    return value


def fixed_from_wire(value):
    require(set(value) == {"coefficient_hi", "coefficient_lo", "scale"}, "unexpected fixed fields")
    high = integer(value["coefficient_hi"], I64_MIN, I64_MAX, "coefficient_hi")
    low = integer(value["coefficient_lo"], I64_MIN, I64_MAX, "coefficient_lo")
    scale = integer(value["scale"], 0, 255, "scale")
    coefficient = (high << 64) | (low & MASK64)
    return {"coefficient_be_i128": coefficient.to_bytes(16, "big", signed=True), "scale": scale}


def fixed_semantic(value):
    coefficient = value["coefficient_be_i128"]
    require(isinstance(coefficient, bytes) and len(coefficient) == 16, "invalid binary i128")
    return {"coefficient": str(int.from_bytes(coefficient, "big", signed=True)), "scale": value["scale"]}


def wire_semantic(value):
    # Independent arithmetic reconstruction; no Decimal context/rounding.
    high, low = value["coefficient_hi"], value["coefficient_lo"]
    return {"coefficient": str(high * (1 << 64) + (low % (1 << 64))), "scale": value["scale"]}


def digest_rows(rows):
    digest = hashlib.sha256()
    for row in rows:
        digest.update(json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False).encode())
        digest.update(b"\n")
    return digest.hexdigest()


def receipt(path):
    before = path.stat()
    require(path.is_file() and before.st_size <= MAX_BYTES, f"input exceeds {MAX_BYTES}-byte cap: {path}")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    after = path.stat()
    require((before.st_ino, before.st_size, before.st_mtime_ns) ==
            (after.st_ino, after.st_size, after.st_mtime_ns), f"file changed during inspection: {path}")
    return {"path": str(path), "bytes": before.st_size, "sha256": digest.hexdigest()}


def schema(pa):
    fixed = pa.struct([pa.field("coefficient_be_i128", pa.binary(16), nullable=False),
                       pa.field("scale", pa.uint8(), nullable=False)])
    fields = [("source_row", pa.uint64(), False), ("input_line", pa.uint64(), False),
              ("input_event_index", pa.uint32(), False), ("schema_version", pa.uint16(), False),
              ("venue_id", pa.uint32(), False), ("instrument_id", pa.uint32(), True),
              ("connection_id", pa.uint64(), False), ("session_id", pa.uint64(), False),
              ("frame_seq", pa.uint64(), False), ("event_index", pa.uint16(), False),
              ("exchange_ts_ns", pa.int64(), True), ("receive_ts_ns", pa.int64(), False),
              ("source_sequence_first", pa.uint64(), True), ("source_sequence_last", pa.uint64(), True),
              ("flags", pa.uint32(), False), ("price", fixed, False), ("quantity", fixed, False),
              ("aggressor", pa.string(), False), ("trade_id", pa.string(), True)]
    return pa.schema([pa.field(name, dtype, nullable=nullable) for name, dtype, nullable in fields],
                     metadata={b"layout_status": b"CANDIDATE_NOT_CANONICAL_NOT_QUALIFIED",
                               b"fixed_encoding": b"signed_i128_twos_complement_big_endian_16_bytes_plus_u8_scale",
                               b"lineage": b"normalized_JSONL_positions_only_not_raw_frame_lineage"})


def self_test(pa, pq):
    values = []
    for coefficient in (I128_MIN, I128_MAX, -1, 0, 1, (1 << 64) - 1):
        for scale in (0, 38, 255):
            low = coefficient & MASK64
            values.append({"coefficient_hi": coefficient >> 64,
                           "coefficient_lo": low if low <= I64_MAX else low - (1 << 64), "scale": scale})
    encoded = [fixed_from_wire(value) for value in values]
    fixed_type = schema(pa).field("price").type
    table = pa.Table.from_pylist([{"fixed": value} for value in encoded],
                                schema=pa.schema([pa.field("fixed", fixed_type, nullable=False)]))
    output = pa.BufferOutputStream()
    pq.write_table(table, output, compression="zstd", compression_level=3, version="2.6")
    restored = pq.ParquetFile(pa.BufferReader(output.getvalue())).read()["fixed"].to_pylist()
    expected = [wire_semantic(value) for value in values]
    actual = [fixed_semantic(value) for value in restored]
    require(expected == actual, "signed i128/scale boundary roundtrip failed")
    require(digest_rows(expected) == digest_rows(actual), "boundary semantic hash mismatch")
    rejected = 0
    for invalid in ({"coefficient_hi": I64_MAX + 1, "coefficient_lo": 0, "scale": 0},
                    {"coefficient_hi": 0, "coefficient_lo": I64_MIN - 1, "scale": 0},
                    {"coefficient_hi": 0, "coefficient_lo": 0, "scale": 256},
                    {"coefficient_hi": 0, "coefficient_lo": 0, "scale": -1}):
        try:
            fixed_from_wire(invalid)
        except ValueError:
            rejected += 1
    require(rejected == 4, "invalid limb/scale was accepted")
    return {"status": "PASS", "roundtrip_cases": len(values), "rejected_invalid_cases": rejected,
            "coefficient_extrema": [str(I128_MIN), str(I128_MAX)], "scales": [0, 38, 255],
            "semantic_sha256_before": digest_rows(expected), "semantic_sha256_after": digest_rows(actual)}


def trade_row(event, source_row, line_number, event_index):
    allowed = {"schema_version", "venue_id", "instrument_id", "connection_id", "session_id", "frame_seq",
               "event_index", "exchange_ts", "receive_ts", "source_sequence", "flags", "payload"}
    require(set(event) <= allowed and set(event["payload"]) == {"trade"}, "unknown Trade envelope fields")
    trade = event["payload"]["trade"]
    require(set(trade) <= {"price", "quantity", "aggressor", "trade_id"}, "unknown Trade fields")
    require(isinstance(trade["aggressor"], str), "aggressor must remain a string")
    require(trade.get("trade_id") is None or isinstance(trade["trade_id"], str), "trade_id must remain an opaque string")
    row = {"source_row": source_row, "input_line": line_number, "input_event_index": event_index}
    widths = {"schema_version": 16, "venue_id": 32, "instrument_id": 32, "connection_id": 64,
              "session_id": 64, "frame_seq": 64, "event_index": 16, "flags": 32}
    for name, width in widths.items():
        row[name] = None if name == "instrument_id" and event.get(name) is None else integer(event[name], 0, (1 << width) - 1, name)
    for original, name in (("exchange_ts", "exchange_ts_ns"), ("receive_ts", "receive_ts_ns")):
        value = event.get(original)
        require(value is None or set(value) == {"ns"}, "unexpected timestamp fields")
        row[name] = None if value is None else integer(value["ns"], I64_MIN, I64_MAX, name)
    require(row["receive_ts_ns"] is not None, "missing receive timestamp")
    sequence = event.get("source_sequence")
    require(sequence is None or set(sequence) == {"first", "last"}, "unexpected sequence fields")
    for name in ("first", "last"):
        row[f"source_sequence_{name}"] = None if sequence is None else integer(sequence[name], 0, MASK64, name)
    for name in ("price", "quantity"):
        require(set(trade[name]) == {"value"}, "unexpected fixed wrapper")
        row[name] = fixed_from_wire(trade[name]["value"])
    row.update(aggressor=trade["aggressor"], trade_id=trade.get("trade_id"))
    semantic = dict(row)
    for name in ("price", "quantity"):
        semantic[name] = wire_semantic(trade[name]["value"])
    return row, semantic


def rss():
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    system = platform.system()
    units = "bytes" if system == "Darwin" else "KiB" if system == "Linux" else "platform_native_unknown"
    return {"ru_maxrss": value, "units": units,
            "bytes": value if system == "Darwin" else value * 1024 if system == "Linux" else None,
            "scope": "whole_process_high_water_including_imports_and_self_test_not_phase_isolated"}


def run(args, pa, pq):
    source, target, metrics = args.input.resolve(strict=True), args.output.absolute(), args.metrics.absolute()
    require(not target.exists() and not metrics.exists(), "output root and metrics must both be new")
    source_receipt = receipt(source)
    baseline_rss = rss()
    boundaries = self_test(pa, pq)
    start = time.perf_counter()
    rows, semantics = [], []
    batches = nontrade = 0
    with source.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            require(line_number <= MAX_LINES, "JSONL line cap exceeded")
            item = json.loads(line)
            if item.get("kind") != "batch":
                continue
            batches += 1
            for event_index, event in enumerate(item["events"]):
                if "trade" not in event.get("payload", {}):
                    nontrade += 1
                    continue
                require(len(rows) < MAX_TRADES, "trade row cap exceeded")
                row, semantic = trade_row(event, len(rows), line_number, event_index)
                rows.append(row)
                semantics.append(semantic)
    parse_seconds = time.perf_counter() - start
    require(rows, "input contains no Trade envelopes")
    require(receipt(source) == source_receipt, "source changed during parsing")
    semantic_before = digest_rows(semantics)
    groups = defaultdict(list)
    for row in rows:
        day = datetime.fromtimestamp(row["receive_ts_ns"] // 1_000_000_000, timezone.utc).strftime("%Y-%m-%d")
        groups[(row["venue_id"], day)].append(row)
    arrow_schema = schema(pa)
    tables = {key: pa.Table.from_pylist(group, schema=arrow_schema) for key, group in groups.items()}
    target.mkdir(parents=True, exist_ok=False)
    files = []
    start = time.perf_counter()
    for (venue, day), table in sorted(tables.items()):
        directory = target / f"venue_id={venue}" / f"receive_date_utc={day}"
        directory.mkdir(parents=True, exist_ok=False)
        path = directory / "part-00000.parquet"
        with path.open("xb") as stream:
            pq.write_table(table, stream, compression="zstd", compression_level=3, version="2.6",
                           use_dictionary=True, write_statistics=True, row_group_size=65536)
        files.append(path)
    write_seconds = time.perf_counter() - start
    start = time.perf_counter()
    restored = []
    for path in files:
        restored.extend(pq.ParquetFile(path).read().to_pylist())
    scan_seconds = time.perf_counter() - start
    restored.sort(key=lambda row: row["source_row"])
    require(restored == rows, "exact typed Arrow row equality failed")
    restored_semantics = []
    for row in restored:
        item = dict(row)
        for name in ("price", "quantity"):
            item[name] = fixed_semantic(row[name])
        restored_semantics.append(item)
    semantic_after = digest_rows(restored_semantics)
    require(semantic_before == semantic_after, "roundtrip semantic hash mismatch")
    require(receipt(source) == source_receipt, "source changed during storage probe")
    file_receipts = []
    for path in files:
        info = receipt(path)
        metadata = pq.ParquetFile(path).metadata
        info.update(rows=metadata.num_rows, row_groups=metadata.num_row_groups,
                    codecs=sorted({metadata.row_group(0).column(i).compression for i in range(metadata.num_columns)}))
        file_receipts.append(info)
    total_bytes = sum(item["bytes"] for item in file_receipts)
    sessions = defaultdict(set)
    for row in rows:
        sessions[row["session_id"]].add(row["venue_id"])
    report = {"schema": "cryptofeed.trade_storage_layout_probe.v1", "status": "PASS_EXACT_BOUNDED_ROUNDTRIP",
              "layout_status": "CANDIDATE_NOT_CANONICAL_NOT_QUALIFIED", "input": source_receipt,
              "generated_at_utc": datetime.now(timezone.utc).isoformat(),
              "runtime": {"executable": sys.executable, "python": platform.python_version(),
                          "pyarrow": pa.__version__, "platform": platform.platform()},
              "layout": {"format": "Parquet", "parquet_version": "2.6", "compression": "ZSTD", "level": 3,
                         "partition_columns": ["venue_id", "receive_date_utc"],
                         "row_group_target": 65536, "coefficient_encoding": "signed_i128_big_endian_binary16",
                         "scale_type": "uint8", "schema": str(arrow_schema)},
              "rows": len(rows), "input_batches": batches, "nontrade_events_skipped": nontrade,
              "venue_counts": dict(sorted(Counter(row["venue_id"] for row in rows).items())),
              "session_venue_sets": {str(key): sorted(value) for key, value in sorted(sessions.items())},
              "exact_row_equality": True, "semantic_sha256_before": semantic_before,
              "semantic_sha256_after": semantic_after, "self_test": boundaries,
              "storage": {"parquet_files": len(files), "parquet_bytes": total_bytes,
                          "parquet_bytes_per_trade_row": total_bytes / len(rows),
                          "input_jsonl_bytes_per_selected_trade_row": source_receipt["bytes"] / len(rows),
                          "input_ratio_caveat": "JSONL contains skipped nontrade/system events; not a comparable Trade-only compression ratio"},
              "timing": {"parse_seconds": parse_seconds, "write_seconds": write_seconds,
                         "scan_seconds_including_python_materialization": scan_seconds,
                         "write_rows_per_second_this_sample_only": len(rows) / write_seconds,
                         "scan_rows_per_second_this_sample_only": len(rows) / scan_seconds,
                         "scope": "single small warm local Python/Arrow sample; not Rust throughput or sustained capacity"},
              "max_rss_before": baseline_rss, "max_rss_after": rss(), "files": file_receipts,
              "limitations": ["Pilot session IDs collide across venues; storing them faithfully does not repair identity",
                              "Input positions refer to normalized JSONL, not raw MFR1 frame/record lineage",
                              "Small bounded sample includes source snapshots and does not prove independent executions or losslessness",
                              "No aggregation/unit/side semantics, exchange completeness or dataset admission is established",
                              "No production Rust writer, transactional durability/crash recovery, remote store or throughput qualification is implemented",
                              "Partition footer overhead and warm filesystem cache materially affect this small sample"]}
    with (target / "manifest.json").open("x", encoding="utf-8") as stream:
        json.dump(report, stream, sort_keys=True, indent=2, allow_nan=False)
        stream.write("\n")
    report["manifest"] = receipt(target / "manifest.json")
    report["script"] = receipt(Path(__file__).resolve())
    with metrics.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, sort_keys=True, indent=2, allow_nan=False)
        stream.write("\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=Path(".local/evidence/trade-audit/pilot-001/events.jsonl"))
    parser.add_argument("--output", type=Path, default=Path(".local/evidence/trade-audit/storage-probe"))
    parser.add_argument("--metrics", type=Path, default=Path(".local/evidence/trade-audit/storage-probe.json"))
    parser.add_argument("--self-test", action="store_true", help="run in-memory exact boundary tests only; create no files")
    args = parser.parse_args()
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as error:
        print(f"Existing pyarrow runtime required; no packages installed: {error}", file=sys.stderr)
        return 2
    try:
        report = self_test(pa, pq) if args.self_test else run(args, pa, pq)
    except (OSError, ValueError, TypeError, KeyError) as error:
        print(json.dumps({"status": "FAIL", "error": str(error)}), file=sys.stderr)
        return 1
    print(json.dumps({"status": report["status"], "metrics": None if args.self_test else str(args.metrics.absolute()),
                      "rows": report.get("rows"), "self_test": report if args.self_test else report["self_test"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
