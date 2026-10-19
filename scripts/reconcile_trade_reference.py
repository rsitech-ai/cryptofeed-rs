#!/usr/bin/env python3
"""Bounded same-venue price/quantity/ID intersections, never coverage proof."""
import argparse
from collections import defaultdict
from decimal import Decimal
import hashlib
import json
from pathlib import Path

from audit_trade_capture import fixed


def reference(name, data):
    if name == "binance":
        return 2, [(str(r["id"]), r["price"], r["qty"]) for r in data]
    if name == "bybit":
        if data["retCode"] != 0:
            raise ValueError(data["retMsg"])
        return 6, [(r["execId"], r["price"], r["size"]) for r in data["result"]["list"]]
    if name == "kraken":
        if data["error"]:
            raise ValueError(str(data["error"]))
        rows = next(v for k, v in data["result"].items() if k != "last")
        return 7, [(str(r[6]), r[0], r[1]) for r in rows]
    if name == "deribit":
        return 8, [(r["trade_id"], r["price"], r["amount"]) for r in data["result"]["trades"]]
    if name == "bitstamp":
        return 14, [(r["tid"], r["price"], r["amount"]) for r in data]
    if name == "gemini":
        return 15, [(str(r["tid"]), r["price"], r["amount"]) for r in data]
    if name == "coinbase":
        return 16, [(str(r["trade_id"]), r["price"], r["size"]) for r in data]
    if name == "bitfinex":
        return 17, [(str(r[0]), r[3], abs(Decimal(str(r[2])))) for r in data]
    raise ValueError("unsupported reference")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture", type=Path)
    parser.add_argument("references", type=Path, nargs="+")
    args = parser.parse_args()
    live = defaultdict(lambda: defaultdict(set))
    for line in (args.capture / "events.jsonl").open():
        for event in json.loads(line).get("events", []):
            if "trade" in event["payload"]:
                r = event["payload"]["trade"]
                live[event["venue_id"]][r["trade_id"]].add((fixed(r["price"]["value"]), fixed(r["quantity"]["value"])))
    results = []
    for directory in args.references:
        for path in sorted(directory.glob("*-recent.json")):
            name = path.name.split("-")[0]
            row = dict(file=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                       scope="price/quantity/native-ID intersection; side/time/coverage not established")
            if name == "okx":
                row.update(status="blocked", reason="WS trades aggregates cannot be equated to REST individual trades")
                results.append(row)
                continue
            venue, native = reference(name, json.loads(path.read_bytes(), parse_float=Decimal))
            reference_ids = defaultdict(set)
            for trade_id, price, qty in native:
                reference_ids[trade_id].add((Decimal(str(price)), Decimal(str(qty))))
            overlap = reference_ids.keys() & live[venue].keys()
            mismatches = sorted(i for i in overlap if reference_ids[i] != live[venue][i])
            row.update(venue_id=venue, reference_rows=len(native), overlapping_ids=len(overlap),
                       mismatched_ids=mismatches, status="verified-for-stated-scope" if overlap and not mismatches else "failed" if mismatches else "not-run-no-overlap")
            results.append(row)
    print(json.dumps(dict(schema_version=1, results=results, continuity="unknown", qualified=False), indent=2))


if __name__ == "__main__":
    main()
