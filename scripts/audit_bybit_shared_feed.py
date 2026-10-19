#!/usr/bin/env python3
"""Independently reconcile observed Bybit trades/candles against committed MFSF1.

Checks native values/provenance, row ordinals and raw receipt linkage. Does not
prove continuity, subscription readiness, unit qualification or dataset admission.
"""
import argparse
import base64
from collections import Counter
from decimal import Decimal
import json
from pathlib import Path
from audit_shared_feed import (
    scan,
    fixed,
    integer,
    strict_json,
    committed_cursor,
    committed_identity,
)

if not __debug__:
    raise RuntimeError("audit requires assertions enabled")

TRADE_KEYS = ("T", "s", "S", "v", "p", "i", "seq", "L", "BT", "RPI")
CANDLE_KEYS = (
    "start",
    "end",
    "interval",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "turnover",
    "confirm",
    "timestamp",
)
INTERVALS = {
    "1": 60_000_000_000,
    "5": 300_000_000_000,
    "15": 900_000_000_000,
    "60": 3_600_000_000_000,
    "D": 86_400_000_000_000,
    "1D": 86_400_000_000_000,
}


def attributes(row, keys, ts):
    def text(value):
        assert type(value) in (str, int, bool), "unexpected native attribute type"
        return str(value).lower() if type(value) is bool else str(value)

    result = {key: text(row[key]) for key in keys if key in row}
    result["ts"] = str(ts)
    return result


def audit_records(records, required_venues=()):
    metadata, originals, accepted, native, canonical = {}, {}, set(), {}, {}
    inbound = {}
    errors, counts, channels = [], Counter(), Counter()
    for record in records:
        kind, data = record["kind"], record["data"]
        counts[kind] += 1
        if kind == "metadata" and data["kind"] == "session":
            integer(data["session_id"])
            integer(data["venue_id"], 16)
            for item in data["catalog"]:
                integer(item["instrument_id"], 32)
            assert data["session_id"] not in metadata
            assert data["venue_id"] in (5, 6, 11), "not a Bybit capture"
            metadata[data["session_id"]] = data
        elif kind == "frame" and integer(data["direction"], 8) == 0:
            key = integer(data["session"]), integer(data["frame_seq"])
            integer(data["receive_ts_ns"], signed=True)
            opcode = integer(data["opcode"], 8)
            assert key[0] in metadata, "inbound frame session lacks metadata"
            assert key not in inbound, "duplicate inbound frame ordinal"
            inbound[key] = data
            if opcode == 1:
                originals[key] = data
        elif kind == "frame_accepted":
            key = integer(data["session"]), integer(data["frame_seq"])
            assert key in inbound, "orphan acceptance receipt"
            assert key not in accepted, "duplicate acceptance receipt"
            accepted.add(key)
            if inbound[key]["opcode"] != 1:
                continue
            message = strict_json(base64.b64decode(originals[key]["payload_base64"], validate=True))
            topic = message.get("topic", "")
            if not (topic.startswith("publicTrade.") or topic.startswith("kline.")):
                continue
            venue = metadata[data["session"]]["venue_id"]
            frame_ms = integer(message["ts"], signed=True)
            assert 0 <= frame_ms <= ((1 << 63) - 1) // 1_000_000
            assert type(message["data"]) is list and message["data"]
            for index, row in enumerate(message["data"]):
                assert type(row) is dict
                time_keys = (
                    ("T",) if topic.startswith("publicTrade.") else ("start", "end", "timestamp")
                )
                for name in time_keys:
                    if name in row:
                        ms = integer(row[name], signed=True)
                        assert 0 <= ms <= ((1 << 63) - 1) // 1_000_000
                if "seq" in row:
                    integer(row["seq"])
                string_keys = (
                    ("s", "S", "v", "p", "i", "L")
                    if topic.startswith("publicTrade.")
                    else ("interval", "open", "high", "low", "close", "volume", "turnover")
                )
                for name in string_keys:
                    if name in row:
                        assert type(row[name]) is str
                for name in ("BT", "RPI"):
                    if name in row:
                        assert type(row[name]) is bool
                identity = key + (index,)
                channels[f"{venue}:{topic}"] += 1
                if topic.startswith("publicTrade."):
                    assert topic == "publicTrade." + row["s"]
                    native[identity] = (
                        "Trade",
                        row["s"],
                        row["i"],
                        Decimal(row["p"]),
                        Decimal(row["v"]),
                        {"Buy": "Buy", "Sell": "Sell"}.get(row["S"], "Unknown"),
                        int(row["T"]) * 1_000_000,
                        topic,
                        "IndividualExecution",
                        None,
                        None,
                        None,
                        attributes(row, TRADE_KEYS, message["ts"]),
                    )
                else:
                    _, interval, symbol = topic.split(".", 2)
                    assert interval == row["interval"] and type(row["confirm"]) is bool
                    native[identity] = (
                        "Candle",
                        symbol,
                        *(Decimal(row[k]) for k in ("open", "high", "low", "close", "volume")),
                        Decimal(row["turnover"]) if "turnover" in row else None,
                        row["confirm"],
                        int(row["start"]) * 1_000_000,
                        INTERVALS[interval],
                        int(message["ts"]) * 1_000_000,
                        topic,
                        attributes(row, CANDLE_KEYS, message["ts"]),
                    )
        elif kind == "batch":
            for event in data["events"]:
                if not any(name in event["payload"] for name in ("Trade", "Candle")):
                    continue
                assert (
                    type(event["payload"]) is dict and len(event["payload"]) == 1
                ), "one enum payload required"
                key = integer(event["session"]), integer(event["frame_seq"])
                integer(event["event_index"], 16)
                integer(event["receive_ts"], signed=True)
                integer(event["exchange_ts"], signed=True)
                integer(event["venue"], 16)
                integer(event["instrument"], 32)
                integer(event["flags"], 32)
                integer(data["session"])
                integer(data["frame_seq"])
                assert event["flags"] & (1 << 9) == 0, "native snapshot type is not initial history"
                assert (
                    key in originals
                    and event["session"] == data["session"]
                    and event["frame_seq"] == data["frame_seq"]
                )
                meta = metadata[event["session"]]
                assert event["venue"] == meta["venue_id"]
                assert event["receive_ts"] == originals[key]["receive_ts_ns"]
                symbol = next(
                    item["native_symbol"]
                    for item in meta["catalog"]
                    if item["instrument_id"] == event["instrument"]
                )
                identity = key + (event["event_index"],)
                assert identity not in canonical, "duplicate canonical ordinal"
                assert event["source_sequence"] is None, "native cross-sequence is not continuity"
                if "Trade" in event["payload"]:
                    row = event["payload"]["Trade"]
                    source = row["provenance"]
                    assert (
                        event["flags"] & (1 << 9) == 0
                    ), "snapshot message type is not initial history"
                    canonical[identity] = (
                        "Trade",
                        symbol,
                        row["trade_id"],
                        fixed(row["price"]),
                        fixed(row["quantity"]),
                        row["aggressor"],
                        event["exchange_ts"],
                        source["source_channel"],
                        source["granularity"],
                        source["native_count"],
                        source["constituent_first_id"],
                        source["constituent_last_id"],
                        source["native_attributes"],
                    )
                else:
                    row = event["payload"]["Candle"]
                    integer(row["start_ts"], signed=True)
                    integer(row["interval_ns"], signed=True)
                    source = row["provenance"]
                    assert type(row["is_closed"]) is bool and row.get("quote_volume") is None
                    canonical[identity] = (
                        "Candle",
                        symbol,
                        *(fixed(row[k]) for k in ("open", "high", "low", "close", "volume")),
                        (
                            fixed(row["native_volume_ccy"])
                            if row.get("native_volume_ccy") is not None
                            else None
                        ),
                        row["is_closed"],
                        row["start_ts"],
                        row["interval_ns"],
                        event["exchange_ts"],
                        source["source_channel"],
                        source["native_attributes"],
                    )
        elif kind == "system" and any(
            name in data["event"]
            for name in ("ParseError", "EventsDropped", "SequenceGap", "UnknownMessage")
        ):
            errors.append(data["event"])
    missing = set(originals) - accepted
    differences = [
        str(key)
        for key in native.keys() | canonical.keys()
        if native.get(key) != canonical.get(key)
    ]
    trade_venues = {
        metadata[key[0]]["venue_id"] for key, row in native.items() if row[0] == "Trade"
    }
    absent_venues = sorted(set(required_venues) - trade_venues)
    return {
        "schema_version": 1,
        "records": dict(counts),
        "native_rows_by_venue_channel": dict(channels),
        "native_trade_rows": sum(row[0] == "Trade" for row in native.values()),
        "native_candle_rows": sum(row[0] == "Candle" for row in native.values()),
        "difference_count": len(differences),
        "difference_examples": differences[:10],
        "unaccepted_text_frames": len(missing),
        "required_trade_venues_missing": absent_venues,
        "errors": errors,
        "checks_passed": bool(native)
        and not missing
        and not differences
        and not absent_venues
        and not errors,
        "continuity": "unknown; exact observed rows do not prove complete source delivery",
        "unit_qualification": "native volume/turnover retained; WS quote-volume qualification unavailable",
        "subscription_readiness": "not audited",
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
