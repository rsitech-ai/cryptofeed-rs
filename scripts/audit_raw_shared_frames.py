#!/usr/bin/env python3
"""Bounded exact MFR1/application-frame parity with a pinned MFSF1 prefix.

Closed captures only. Compares every application payload, timestamp, session,
ordinal, direction/opcode and metadata, preserving duplicate multiplicity.
Not exchange completeness or mixed timer/HTTP causal replay qualification.
"""
import argparse
import base64
from collections import Counter
import hashlib
import json
import itertools
from pathlib import Path
from audit_shared_feed import scan, strict_json, committed_cursor, committed_identity, integer
from audit_trade_capture import records_bytes

if not __debug__:
    raise RuntimeError("audit requires assertions enabled")


def audit(raw_directory, shared_directory):
    cap = 128 * 1024 * 1024
    paths = sorted(itertools.islice(Path(raw_directory).glob("*.mfr1"), 4097))
    assert 0 < len(paths) <= 4096, "bounded nonempty raw segment set required"
    raw_frames, shared_frames, raw_meta, shared_meta = Counter(), Counter(), Counter(), Counter()
    objects = []
    total = 0
    for path in paths:
        assert path.is_file() and not path.is_symlink(), "regular raw segment required"
        with path.open("rb") as source:
            data = source.read(cap - total + 1)
        total += len(data)
        assert total <= cap, "raw total128MiB cap"
        objects.append(
            {"file": path.name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
        )
        for record in records_bytes(data):
            assert record["direction"] in (0, 1), "invalid raw direction"
            if record["opcode"] == 7:
                raw_meta[
                    json.dumps(
                        strict_json(record["payload"]), sort_keys=True, separators=(",", ":")
                    )
                ] += 1
            elif record["opcode"] in range(1, 6):
                assert record["flags"] == 0, "raw-only flags not represented by shared frame"
                raw_frames[
                    (
                        record["session"],
                        record["seq"],
                        record["receipt_ns"],
                        record["monotonic_ns"],
                        record["direction"],
                        record["opcode"],
                        record["payload"],
                    )
                ] += 1
            else:
                raise ValueError(
                    "HTTP/subscription-command reconciliation not implemented by this public WS audit"
                )
    cursor = committed_cursor(shared_directory)
    sealed = False
    for record in scan(shared_directory, cursor):
        assert not sealed, "records after final shared seal"
        kind, data = record["kind"], record["data"]
        if kind == "seal":
            assert integer(data["schema_version"], 16) == 1
            integer(data["now_ns"], signed=True)
            assert type(data["reason"]) is str
            sealed = True
        elif kind == "metadata":
            shared_meta[json.dumps(data, sort_keys=True, separators=(",", ":"))] += 1
        elif kind == "frame":
            for key in ("session", "frame_seq", "monotonic_ns"):
                integer(data[key])
            integer(data["receive_ts_ns"], signed=True)
            assert integer(data["direction"], 8) in (0, 1)
            assert integer(data["opcode"], 8) in range(1, 6)
            assert type(data["payload_base64"]) is str
            shared_frames[
                (
                    data["session"],
                    data["frame_seq"],
                    data["receive_ts_ns"],
                    data["monotonic_ns"],
                    data["direction"],
                    data["opcode"],
                    base64.b64decode(data["payload_base64"], validate=True),
                )
            ] += 1
    missing = sum((raw_frames - shared_frames).values())
    extra = sum((shared_frames - raw_frames).values())
    meta_diff = sum((raw_meta - shared_meta).values()) + sum((shared_meta - raw_meta).values())
    result = {
        "schema": "marketfeed.raw_shared_frame_parity.v1",
        "raw_objects": objects,
        "raw_bytes": total,
        "raw_application_frames": sum(raw_frames.values()),
        "shared_application_frames": sum(shared_frames.values()),
        "raw_metadata_records": sum(raw_meta.values()),
        "shared_metadata_records": sum(shared_meta.values()),
        "missing_shared_frames": missing,
        "extra_shared_frames": extra,
        "metadata_difference_count": meta_diff,
        "checks_passed": bool(raw_frames)
        and sealed
        and not missing
        and not extra
        and not meta_diff,
        "shared_final_seal": sealed,
        "raw_closed_input": "caller assumption; bounded bytes inspected and hashed, source process closure is external evidence",
        "claim_limit": "exact saved public WS application frames and metadata only; source continuity/HTTP/timer causal replay unqualified",
    }
    result.update(committed_identity(shared_directory, cursor))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", required=True)
    parser.add_argument("--feed", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = audit(args.raw, args.feed)
    text = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(text)
    print(text)
    raise SystemExit(0 if result["checks_passed"] else 1)


if __name__ == "__main__":
    main()
