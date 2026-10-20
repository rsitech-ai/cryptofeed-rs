#!/usr/bin/env python3
"""Independent Bybit native book/BBO-to-canonical row reconciliation.

Local closed capture only. Observed ID steps are not a source continuity proof.
Lifecycle/failure semantics are separately verified with action-ordered tests.
"""
import argparse
import base64
from collections import Counter
from decimal import Decimal
import json
from pathlib import Path
import re
from audit_shared_feed import (
    scan,
    strict_json,
    integer,
    committed_cursor,
    committed_identity,
)

if not __debug__:
    raise RuntimeError("audit requires assertions enabled")


def parts(value):
    assert type(value) is str and len(value) <= 512, "bounded decimal token"
    token = re.fullmatch(
        r"([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))(?:[eE]([+-]?[0-9]+))?", value
    )
    assert token is not None, "ASCII decimal syntax"
    decimal = Decimal(token[1])
    tup = decimal.as_tuple()
    assert len(tup.digits) <= 39 and -255 <= tup.exponent <= 0, "bounded mantissa"
    coefficient = int("".join(map(str, tup.digits))) * (-1 if tup.sign else 1)
    integer(coefficient, 128, True)
    scale = -tup.exponent
    if token[2] is None:
        return coefficient, scale
    exponent = int(token[2])
    integer(abs(exponent), 32)
    if coefficient == 0:
        return 0, 0
    scale -= exponent
    assert -38 <= scale <= 255, "bounded coefficient/scale domain"
    if scale < 0:
        coefficient *= 10**-scale
        scale = 0
    return integer(coefficient, 128, True), integer(scale, 8)


def numeric_key(value):
    """A bounded integer comparison key, independent of Decimal context precision."""
    return value[0] * 10 ** (255 - value[1])


def canonical_parts(value):
    assert type(value) is dict and set(value) == {"coefficient", "scale"}
    return integer(value["coefficient"], 128, True), integer(value["scale"], 8)


def milliseconds(value):
    integer(value, 64, True)
    assert 0 <= value <= ((1 << 63) - 1) // 1_000_000
    return value * 1_000_000


def audit_records(records, required_venues=()):
    metadata, originals, inbound, accepted, native, canonical, last = (
        {},
        {},
        set(),
        set(),
        {},
        {},
        {},
    )
    counts, steps, errors = Counter(), Counter(), []
    for record in records:
        kind, data = record["kind"], record["data"]
        if kind == "metadata" and data["kind"] == "session":
            session = integer(data["session_id"])
            assert session not in metadata
            assert integer(data["venue_id"], 16) in (5, 6, 11)
            ids = [integer(item["instrument_id"], 32) for item in data["catalog"]]
            assert len(ids) == len(set(ids))
            metadata[session] = data
        elif kind == "connection":
            session = integer(data["session"])
            last = {key: value for key, value in last.items() if key[0] != session}
        elif kind == "frame" and integer(data["direction"], 8) == 0:
            key = integer(data["session"]), integer(data["frame_seq"])
            assert key not in inbound and key[0] in metadata
            inbound.add(key)
            if integer(data["opcode"], 8) == 1:
                originals[key] = data
        elif kind == "frame_accepted":
            key = integer(data["session"]), integer(data["frame_seq"])
            assert key in inbound and key not in accepted
            accepted.add(key)
            if key not in originals:
                continue
            source = originals[key]
            value = strict_json(
                base64.b64decode(source["payload_base64"], validate=True)
            )
            topic = value.get("topic", "")
            if not topic.startswith("orderbook."):
                continue
            prefix, depth, symbol = topic.split(".")
            depth = int(depth)
            assert prefix == "orderbook" and depth in (1, 50, 200, 1000)
            row = value["data"]
            assert row["s"] == symbol and type(row["s"]) is str
            u = integer(row["u"])
            assert u > 0
            seq = integer(row["seq"]) if "seq" in row else None
            ts = milliseconds(value["ts"])
            cts = milliseconds(value["cts"]) if "cts" in value else None
            assert value["type"] in ("snapshot", "delta")
            levels = []
            for side in ("b", "a"):
                assert type(row[side]) is list and len(row[side]) <= 4096
                parsed = []
                for level in row[side]:
                    assert type(level) is list and len(level) == 2
                    price, size = map(parts, level)
                    assert price[0] > 0 and size[0] >= 0
                    parsed.append((price, size))
                assert len({numeric_key(price) for price, _ in parsed}) == len(
                    parsed
                ), "duplicate native price"
                levels.append(tuple(parsed))
            attrs = {
                "s": symbol,
                "u": str(u),
                "type": value["type"],
                "ts": str(value["ts"]),
            }
            if seq is not None:
                attrs["seq"] = str(seq)
            if cts is not None:
                attrs["cts"] = str(value["cts"])
            source_context = (topic, attrs)
            state_key = key[0], symbol, depth
            fingerprint = (
                value["type"],
                u,
                seq,
                ts,
                cts,
                tuple(levels),
                source_context,
            )
            previous = last.get(state_key)
            snapshot = value["type"] == "snapshot" or u == 1
            if snapshot:
                for side, items in zip(("b", "a"), levels):
                    assert len(items) <= depth, "snapshot exceeds native depth"
                    assert all(
                        size[0] > 0 for _, size in items
                    ), "snapshot positive size"
                    prices = [numeric_key(price) for price, _ in items]
                    assert all(
                        left > right if side == "b" else left < right
                        for left, right in zip(prices, prices[1:])
                    ), "snapshot native sort order"
                if levels[0] and levels[1]:
                    assert numeric_key(levels[0][0][0]) < numeric_key(
                        levels[1][0][0]
                    ), "crossed native snapshot"
            venue = metadata[key[0]]["venue_id"]
            label = f"{venue}:{depth}"
            counts[label + ":native"] += 1
            if depth == 1:
                assert snapshot, "unexpected native L1 delta"
                if not levels[0] or not levels[1]:
                    counts[label + ":unavailable"] += 1
                    continue
                expected = (
                    "Quote",
                    symbol,
                    levels[0][0],
                    levels[1][0],
                    ts,
                    source_context,
                    1,
                )
            else:
                if not snapshot and previous == fingerprint:
                    counts[label + ":exact_duplicate"] += 1
                    continue
                if not snapshot:
                    assert previous is not None, "delta without snapshot"
                    if u <= previous[1] or (
                        previous[2] is not None
                        and seq is not None
                        and seq < previous[2]
                    ):
                        errors.append("native ordering anomaly:" + label)
                        continue
                    steps[f"{label}:{u-previous[1]}"] += 1
                    changes = tuple(
                        (
                            side,
                            "Delete" if size[0] == 0 else "Upsert",
                            price,
                            None if size[0] == 0 else size,
                        )
                        for side, items in zip(("Bid", "Ask"), levels)
                        for price, size in items
                    )
                    expected = ("BookDelta", symbol, changes, ts, source_context, 2)
                else:
                    expected = (
                        "BookSnapshot",
                        symbol,
                        tuple(levels),
                        depth,
                        ts,
                        source_context,
                        1,
                    )
                last[state_key] = fingerprint
            native[key + (0,)] = expected
            counts[f"{venue}:{expected[0]}"] += 1
        elif kind == "batch":
            for event in data["events"]:
                payload = event["payload"]
                names = set(payload) & {"Quote", "BookSnapshot", "BookDelta"}
                if not names:
                    continue
                assert len(payload) == 1 and len(names) == 1, "one enum payload"
                key = integer(event["session"]), integer(event["frame_seq"])
                index = integer(event["event_index"], 16)
                assert event["session"] == integer(data["session"]) and event[
                    "frame_seq"
                ] == integer(data["frame_seq"])
                assert key in originals and event["source_sequence"] is None
                assert integer(event["receive_ts"], 64, True) == integer(
                    originals[key]["receive_ts_ns"], 64, True
                )
                meta = metadata[key[0]]
                assert integer(event["venue"], 16) == meta["venue_id"]
                instrument = integer(event["instrument"], 32)
                symbol = next(
                    item["native_symbol"]
                    for item in meta["catalog"]
                    if item["instrument_id"] == instrument
                )
                identity = key + (index,)
                assert identity not in canonical
                name = next(iter(names))
                row = payload[name]
                provenance = row["provenance"]
                assert (
                    type(provenance["source_channel"]) is str
                    and type(provenance["native_attributes"]) is dict
                )
                assert all(
                    type(k) is str and type(v) is str
                    for k, v in provenance["native_attributes"].items()
                )
                source_context = (
                    provenance["source_channel"],
                    provenance["native_attributes"],
                )
                ts = integer(event["exchange_ts"], 64, True)
                flags = integer(event["flags"], 32)
                if name == "Quote":
                    canonical[identity] = (
                        name,
                        symbol,
                        (
                            canonical_parts(row["bid_price"]),
                            canonical_parts(row["bid_quantity"]),
                        ),
                        (
                            canonical_parts(row["ask_price"]),
                            canonical_parts(row["ask_quantity"]),
                        ),
                        ts,
                        source_context,
                        flags,
                    )
                elif name == "BookSnapshot":
                    assert row["checksum"] is None
                    levels = tuple(
                        tuple(
                            (
                                canonical_parts(level["price"]),
                                canonical_parts(level["quantity"]),
                            )
                            for level in row[side]
                        )
                        for side in ("bids", "asks")
                    )
                    canonical[identity] = (
                        name,
                        symbol,
                        levels,
                        integer(row["depth"], 32),
                        ts,
                        source_context,
                        flags,
                    )
                else:
                    assert row["checksum"] is None
                    changes = tuple(
                        (
                            item["side"],
                            item["operation"],
                            canonical_parts(item["price"]),
                            (
                                canonical_parts(item["quantity"])
                                if item["quantity"] is not None
                                else None
                            ),
                        )
                        for item in row["changes"]
                    )
                    canonical[identity] = (
                        name,
                        symbol,
                        changes,
                        ts,
                        source_context,
                        flags,
                    )
        elif kind == "system" and any(
            name in data["event"]
            for name in (
                "ParseError",
                "UnknownMessage",
                "EventsDropped",
                "SequenceGap",
                "BookInvalidated",
            )
        ):
            errors.append(data["event"])
    differences = [
        str(key)
        for key in native.keys() | canonical.keys()
        if native.get(key) != canonical.get(key)
    ]
    missing = [
        venue
        for venue in required_venues
        if not counts[f"{venue}:BookSnapshot"] or not counts[f"{venue}:BookDelta"]
    ]
    unaccepted = len(set(originals) - accepted)
    return {
        "schema": "marketfeed.bybit_book_native_audit.v1",
        "counts": dict(counts),
        "observed_update_id_steps": dict(steps),
        "native_book_quote_rows": len(native),
        "canonical_book_quote_rows": len(canonical),
        "difference_count": len(differences),
        "difference_examples": differences[:10],
        "required_venues_missing_snapshot_or_delta": missing,
        "unaccepted_text_frames": unaccepted,
        "errors": errors,
        "checks_passed": bool(native)
        and not differences
        and not missing
        and not unaccepted
        and not errors,
        "continuity": "unknown; observed ID steps are not missing-record counts",
        "dataset_admitted": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory")
    parser.add_argument(
        "--require-venue", type=int, choices=(5, 6, 11), action="append", default=[]
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    cursor = committed_cursor(args.directory)
    result = audit_records(scan(args.directory, cursor), args.require_venue)
    result.update(committed_identity(args.directory, cursor))
    text = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(text)
    print(text)
    raise SystemExit(0 if result["checks_passed"] else 1)


if __name__ == "__main__":
    main()
