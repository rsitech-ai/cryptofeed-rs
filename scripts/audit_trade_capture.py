#!/usr/bin/env python3
"""Independent, bounded native-row accounting for the audit pilot protocols.

This is evidence tooling, not a canonical normalizer or qualification publisher.
Counts include source snapshots; matching IDs do not prove exchange completeness.
"""
import argparse
from collections import Counter, defaultdict
from decimal import Decimal, getcontext
import hashlib
import json
from pathlib import Path
import struct

getcontext().prec = 1000


def crc_table():
    result = []
    for value in range(256):
        for _ in range(8):
            value = (value >> 1) ^ (0x82F63B78 if value & 1 else 0)
        result.append(value)
    return result


CRC_TABLE = crc_table()


def crc32c(data):
    crc = 0xFFFFFFFF
    for byte in data:
        crc = (crc >> 8) ^ CRC_TABLE[(crc ^ byte) & 0xFF]
    return crc ^ 0xFFFFFFFF


def records(path):
    if path.stat().st_size > 128 * 1024 * 1024:
        raise ValueError("segment exceeds audit 128 MiB cap")
    data = path.read_bytes()
    if data[:4] != b"MFR1" or struct.unpack_from("<H", data, 4)[0] not in (1, 2, 3):
        raise ValueError("unsupported MFR1")
    offset = 22
    while offset < len(data):
        if offset + 47 > len(data):
            raise ValueError(f"truncated header at {offset}")
        n, session, seq, ts, mono, direction, opcode, flags, length, crc = struct.unpack_from(
            "<IQQqQBBBII", data, offset)
        if n != 47 + length or n > 16 * 1024 * 1024 or offset + n > len(data):
            raise ValueError(f"invalid/truncated record at {offset}")
        payload = data[offset + 47:offset + n]
        if crc32c(payload) != crc:
            raise ValueError(f"CRC mismatch at {offset}")
        yield dict(session=session, seq=seq, receipt_ns=ts, opcode=opcode,
                   direction=direction, offset=offset, payload=payload)
        offset += n


def native_rows(venue, value, bindings):
    """Return native (symbol, id, price, quantity) tuples, preserving row count.

    Side/time independent verification is separate; do not infer it from matches.
    """
    if venue in (2, 3, 12, 15):
        v = value.get("data", value)
        if v.get("e") in ("trade", "aggTrade") or (venue == 15 and "t" in v and "p" in v):
            return [(v["s"], str(v.get("t", v.get("a"))), v["p"], v["q"])]
    elif venue in (4, 9, 10) and "data" in value and value.get("arg", {}).get("channel") == "trades":
        return [(r["instId"], r["tradeId"], r["px"], r["sz"]) for r in value["data"]]
    elif venue in (5, 6, 11) and value.get("topic", "").startswith("publicTrade."):
        return [(r["s"], r["i"], r["p"], r["v"]) for r in value["data"]]
    elif venue == 7 and value.get("channel") == "trade":
        return [(r["symbol"], str(r["trade_id"]), r["price"], r["qty"]) for r in value["data"]]
    elif venue == 8 and value.get("params", {}).get("channel", "").startswith("trades."):
        return [(r["instrument_name"], r["trade_id"], r["price"], r["amount"]) for r in value["params"]["data"]]
    elif venue == 13 and "event" not in value and value.get("feed") in ("trade", "trade_snapshot"):
        return [(r["product_id"], r["uid"], r["price"], r["qty"]) for r in
                (value["trades"] if value["feed"] == "trade_snapshot" else [value])]
    elif venue == 14 and value.get("event") == "trade":
        r = value["data"]
        return [(value["channel"].removeprefix("live_trades_"), str(r["id"]),
                 r.get("price_str", r["price"]), r.get("amount_str", r["amount"]))]
    elif venue == 16 and value.get("type") in ("match", "last_match"):
        return [(value["product_id"], str(value["trade_id"]), value["price"], value["size"])]
    elif venue == 18 and value.get("channel") == "market_trades":
        return [(r["product_id"], r["trade_id"], r["price"], r["size"])
                for event in value["events"] for r in event.get("trades", [])]
    elif venue in (17, 20):
        if isinstance(value, dict):
            if value.get("event") == "subscribed" and value.get("channel") == "trades":
                bindings[value["chanId"]] = value["symbol"]
        elif isinstance(value, list) and value and value[0] in bindings:
            if value[1] == "te":
                rows = [value[2]]
            elif isinstance(value[1], list):
                rows = value[1]
            else:
                return []  # tu is a paired update; hb is control, neither a new execution.
            return [(bindings[value[0]], str(r[0]), r[3], abs(Decimal(str(r[2])))) for r in rows]
    return []


def fixed(value):
    # MFPE-JSON1 splits signed i128 into signed high / unsigned low limbs.
    return Decimal((int(value["coefficient_hi"]) << 64) + (int(value["coefficient_lo"]) & ((1 << 64) - 1))).scaleb(-int(value["scale"]))


def symbol_for_event(event, metadata):
    session = metadata[event["session_id"]]
    mismatch = session["venue_id"] != event["venue_id"]
    symbols = {i["native_symbol"] for s in metadata.values()
        if s["venue_id"] == event["venue_id"] for i in s["catalog"]
        if i["instrument_id"] == event["instrument_id"]}
    if len(symbols) != 1:
        raise ValueError("ambiguous venue-local instrument; cannot compare")
    return symbols.pop(), mismatch


def analyze(root):
    metadata, raw, native, normalized = {}, [], defaultdict(list), defaultdict(list)
    bindings = defaultdict(dict)
    totals = Counter()
    administrative = Counter()
    files = sorted((root / "raw").glob("*.mfr1"))
    if not files:
        raise ValueError("no raw segments")
    for path in files:
        for record in records(path):
            totals["records"] += 1
            if totals["records"] > 1_000_000:
                raise ValueError("audit exceeds 1000000 records")
            record["file"] = path.name
            if record["opcode"] == 7:
                value = json.loads(record["payload"])
                if value.get("kind") == "session":
                    sid = value["session_id"]
                    if sid in metadata and metadata[sid] != value:
                        raise ValueError("conflicting metadata")
                    metadata[sid] = value
            elif record["direction"] == 0:
                raw.append(record)
    samples = {}
    for record in raw:
        if record["opcode"] != 1:
            continue
        session = metadata[record["session"]]
        venue = session["venue_id"]
        totals["inbound_text"] += 1
        try:
            value = json.loads(record["payload"], parse_float=Decimal)
        except (ValueError, UnicodeDecodeError):
            totals["non_json_text"] += 1
            continue
        rows = native_rows(venue, value, bindings[record["session"]])
        if rows:
            totals["trade_bearing_messages"] += 1
            wrapped = value.get("data", value) if isinstance(value, dict) else {}
            if venue == 3 and wrapped.get("st") == 1 and wrapped.get("X") == "NA" and wrapped.get("p") == "0" and wrapped.get("q") == "0":
                administrative[venue] += len(rows)
                continue  # Explicitly report observed non-execution messages.
            for symbol, trade_id, price, qty in rows:
                native[venue].append((symbol.upper(), trade_id, Decimal(str(price)), Decimal(str(qty))))
            if venue not in samples:
                samples[venue] = dict(file=record["file"], offset=record["offset"],
                    session=record["session"], raw_frame_seq=record["seq"],
                    receipt_ns=record["receipt_ns"], rows=len(rows),
                    payload_sha256=hashlib.sha256(record["payload"]).hexdigest())
    with (root / "events.jsonl").open() as stream:
        for line in stream:
            for event in json.loads(line).get("events", []):
                if "trade" not in event["payload"]:
                    continue
                trade = event["payload"]["trade"]
                symbol, mismatch = symbol_for_event(event, metadata)
                if mismatch:
                    totals["normalized_session_venue_mismatch"] += 1
                # Current factories can emit default session=1. Resolve only a
                # unique venue-local symbol for value accounting; report the
                # broken session identity separately, never claim raw lineage.
                normalized[event["venue_id"]].append((symbol.upper(), str(trade["trade_id"]),
                    fixed(trade["price"]["value"]), fixed(trade["quantity"]["value"])))
    venues = []
    for venue in sorted({s["venue_id"] for s in metadata.values()}):
        source, sink = Counter(native[venue]), Counter(normalized[venue])
        venues.append(dict(venue_id=venue, native_trade_rows=sum(source.values()),
            native_administrative_rows=administrative[venue],
            normalized_trade_rows=sum(sink.values()), native_unique_rows=len(source),
            missing_exact_rows=sum((source-sink).values()), extra_or_mismatched_rows=sum((sink-source).values()),
            count_price_quantity_multiset="pass" if source == sink and source else "fail" if source != sink else "not-observed",
            sample=samples.get(venue), continuity="unknown", qualified=False))
    return dict(schema_version=1, scope="native row identity/price/quantity multiset; excludes side/time/ordering qualification",
        totals=dict(totals), venues=venues)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture", type=Path)
    args = parser.parse_args()
    result = analyze(args.capture)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
