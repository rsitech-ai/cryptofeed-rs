#!/usr/bin/env python3
"""Finite closed clean-sample Bybit startup inspector; NOT a general reader/replay.

No network, writes, retries, qualification, trade-count gate, or value-oracle claim.
Requires exactly one clean closed connection per venue 5/6/11. Native trade/book
value reconciliation must be run separately. Output goes to stdout only.
"""
import argparse
import base64
from collections import Counter
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import sys

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts"))
from audit_shared_feed import committed_cursor, committed_identity, scan, strict_json, integer, fixed

if not __debug__:
    raise RuntimeError("assertions must remain enabled")

EXPECTED = {5: "linear", 6: "spot", 11: "inverse"}
TIMEOUT_NS = 20_000_000_000  # local receipt/timer domain, not a native ACK deadline
BAD_SYSTEM = {"ParseError", "UnknownMessage", "SequenceGap", "EventsDropped",
              "BookInvalidated", "QuoteInvalidated", "ChecksumMismatch",
              "HeartbeatMissed", "RateLimited", "DiskPressure"}


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def payload(data):
    raw = base64.b64decode(data["payload_base64"], validate=True)
    require(len(raw) <= 8 * 1024 * 1024, "native frame exceeds finite 8MiB cap")
    value = strict_json(raw)
    require(type(value) is dict, "native text must be an object")
    return value, hashlib.sha256(raw).hexdigest()


def witness(sequence, data, scope, digest, detail=None):
    result = {"journal_sequence": sequence, "session": scope[0], "connection": scope[1],
              "frame_seq": data["frame_seq"], "receive_ts_ns": data["receive_ts_ns"],
              "payload_sha256": digest}
    if detail is not None:
        result["detail"] = detail
    return result


def requested_topics(metadata):
    """Deliberately limited to this source version's metadata Debug spellings."""
    catalog = metadata["catalog"]
    require(type(catalog) is list and 0 < len(catalog) <= 1024, "bounded selected catalog required")
    symbols, ids = {}, set()
    for item in catalog:
        instrument = integer(item["instrument_id"], 32)
        symbol = item["native_symbol"]
        require(instrument > 0 and instrument not in ids and type(symbol) is str and symbol
                and symbol not in symbols, "duplicate/empty selected identity")
        ids.add(instrument)
        symbols[symbol] = instrument
    intervals, depths, selected = set(), set(), set()
    intervals_by_name = {"M1": "1", "M5": "5", "M15": "15", "H1": "60", "D1": "D"}
    ordinary = {"Trades", "Quote", "MarkPrice", "IndexPrice", "Funding", "OpenInterest",
                "Liquidations", "Statistics24h"}
    for item in metadata["initial_subscriptions"]:
        instrument = integer(item["instrument_id"], 32)
        require(instrument in ids, "subscription instrument absent from selected catalog")
        selected.add(instrument)
        channel = item["channel"]
        require(type(channel) is str, "invalid metadata channel")
        if channel in ordinary:
            continue
        candle = re.fullmatch(r"Candles \{ interval: (M1|M5|M15|H1|D1) \}", channel)
        book = re.fullmatch(r"L2Book \{ depth: (None|Some\((50|200|1000)\)), cadence: None \}", channel)
        if candle:
            intervals.add(intervals_by_name[candle[1]])
        elif book:
            depths.add(int(book[2]) if book[2] else 50)
        else:
            raise AssertionError("unsupported finite metadata channel: " + channel)
    require(selected == ids and len(depths) <= 1, "selected subscription/catalog or L2 depth mismatch")
    topics = set()
    for symbol in symbols:
        topics.update((f"publicTrade.{symbol}", f"orderbook.1.{symbol}", f"tickers.{symbol}"))
        if metadata["venue_id"] != 6:
            topics.add(f"allLiquidation.{symbol}")
        topics.update(f"orderbook.{depth}.{symbol}" for depth in depths)
        topics.update(f"kline.{interval}.{symbol}" for interval in intervals)
    return symbols, topics, {f"orderbook.{depth}.{symbol}" for depth in depths for symbol in symbols}


def audit(directory, cursor):
    metadata, venue_sessions, originals, accepted = {}, {}, {}, {}
    active, states, counts = {}, {}, Counter()
    sealed = False
    # First pass proves complete framing/CRC and acceptance of every original.
    # Action batches precede their frame_accepted marker within one transaction;
    # acceptance is established here before the chronological second pass.
    for sequence, record in enumerate(scan(directory, cursor), 1):
        require(not sealed, "record follows final seal")
        kind, data = record["kind"], record["data"]
        counts[kind] += 1
        if kind == "metadata" and data["kind"] == "session":
            session, venue = integer(data["session_id"]), integer(data["venue_id"], 16)
            require(venue in EXPECTED and session not in metadata and venue not in venue_sessions,
                    "finite inspector needs exactly one session per Bybit venue")
            require(data["endpoint"] == f"wss://stream.bybit.com/v5/public/{EXPECTED[venue]}",
                    "unexpected public endpoint")
            metadata[session] = data
            venue_sessions[venue] = session
        elif kind == "connection":
            session = integer(data["session"])
            connection = integer(data["connection"])
            require(type(data["connected"]) is bool and session in metadata, "connection scope missing metadata")
            scope = (session, connection)
            if data["connected"]:
                require(session not in active and scope not in states
                        and not any(s[0] == session for s in states),
                        "reconnect/recovery sample is outside finite clean-sample scope")
                symbols, topics, l2 = requested_topics(metadata[session])
                started = integer(data["now_ns"], 64, True)
                require(started <= (1 << 63) - 1 - TIMEOUT_NS, "startup timestamp overflow")
                states[scope] = {"venue": metadata[session]["venue_id"], "start": started,
                                 "deadline": started + TIMEOUT_NS, "closed": False,
                                 "close_now": None, "close_reason": None, "shutdown_status": None,
                                 "symbols": symbols, "expected": topics, "l2": l2,
                                 "requests": {}, "topics": [], "conn_id": None,
                                 "bbo": {}, "snapshots": {}, "live": None}
                active[session] = scope
            else:
                require(active.get(session) == scope, "close without matching active connection")
                states[scope]["closed"] = True
                states[scope]["close_now"] = integer(data["now_ns"], 64, True)
                states[scope]["close_reason"] = data["reason"]
                del active[session]
        elif kind == "frame":
            session = integer(data["session"])
            require(session in active, "frame outside active connection")
            require(integer(data["direction"], 8) in (0, 1), "unexpected frame direction")
            integer(data["opcode"], 8)
            if data["direction"] == 0:
                key = (session, integer(data["frame_seq"]))
                require(key not in originals, "reused original inbound coordinate")
                originals[key] = (sequence, data, active[session])
        elif kind == "frame_accepted":
            key = integer(data["session"]), integer(data["frame_seq"])
            require(key in originals and key not in accepted, "missing/repeated original acceptance")
            require(active.get(key[0]) == originals[key][2], "acceptance crossed connection scope")
            accepted[key] = sequence
        elif kind == "seal":
            require(integer(data["schema_version"], 16) == 1 and data["reason"].startswith("clean_shutdown"),
                    "clean final seal required")
            sealed = True
    require(sealed and not active, "closed feed with final seal required")
    require(set(venue_sessions) == set(EXPECTED), "all three Bybit venues required")
    require(set(originals) == set(accepted), "unaccepted original inbound frames")
    require((directory / "events.mfsf").stat().st_size == cursor["offset"], "closed feed has trailing bytes")
    active, closed = {}, set()
    for sequence, record in enumerate(scan(directory, cursor), 1):
        kind, data = record["kind"], record["data"]
        if kind == "connection":
            if data["connected"]:
                active[data["session"]] = (data["session"], data["connection"])
            else:
                closed.add(active.pop(data["session"]))
        elif kind == "frame":
            scope = active[data["session"]]
            state = states[scope]
            require(data["opcode"] == 1, "non-text native frame outside finite clean-sample scope")
            value, digest = payload(data)
            receipt = integer(data["receive_ts_ns"], 64, True)
            require(receipt >= state["start"], "receipt predates current connection")
            if data["direction"] == 1:
                if value.get("op") == "ping":
                    continue
                require(value.get("op") == "subscribe", "unexpected outbound operation")
                req_id, args = value.get("req_id"), value.get("args")
                require(type(req_id) is str and 0 < len(req_id.encode()) <= 64
                        and req_id not in state["requests"], "invalid/reused outbound request ID")
                require(type(args) is list and 0 < len(args) <= 10
                        and all(type(topic) is str for topic in args), "invalid outbound topic chunk")
                require(receipt < state["deadline"], "late outbound startup request")
                require(not (set(args) & set(state["topics"])) and len(set(args)) == len(args),
                        "duplicate outbound topics")
                state["topics"].extend(args)
                require(len(json.dumps(state["topics"], ensure_ascii=False, separators=(",", ":")).encode()) <= 21000,
                        "cumulative local JSON args-byte budget exceeded")
                require(set(state["topics"]) <= state["expected"], "unrequested outbound topic")
                state["requests"][req_id] = {"topics": args, "outbound": witness(sequence, data, scope, digest, value),
                                              "ack": None, "ret_msg": None}
            else:
                require((data["session"], data["frame_seq"]) in accepted, "native input lacks acceptance")
                if value.get("op") == "subscribe":
                    require("topic" not in value and "data" not in value, "mixed control/market ACK")
                    req_id, conn_id = value.get("req_id"), value.get("conn_id")
                    require(type(req_id) is str and req_id in state["requests"], "uncorrelated native ACK")
                    require(type(conn_id) is str and 0 < len(conn_id.encode()) <= 256,
                            "invalid ACK connection ID")
                    require(value.get("success") is True and value.get("ret_msg") in ("", "subscribe"),
                            "failed or unsupported native ACK")
                    require(state["conn_id"] in (None, conn_id), "native connection ID changed")
                    state["conn_id"] = conn_id
                    request = state["requests"][req_id]
                    if request["ack"] is None:
                        require(state["live"] is None and receipt < state["deadline"], "late first ACK")
                        request["ack"] = witness(sequence, data, scope, digest, value)
                        request["ret_msg"] = value["ret_msg"]
                    else:
                        require(request["ret_msg"] == value["ret_msg"], "conflicting duplicate ACK")
                elif "op" in value:
                    require(value.get("op") in ("ping", "pong"), "unexpected native control")
                    if value["op"] == "ping":
                        require(value.get("success") is True and value.get("ret_msg") == "pong", "invalid ping reply")
        elif kind == "system":
            event = data["event"]
            names = {event} if type(event) is str else set(event)
            require(not (names & BAD_SYSTEM), "unclean system event: " + str(event))
            if type(event) is dict and "SubscriptionStateChanged" in event:
                require(event["SubscriptionStateChanged"]["state"] == "subscribed", "subscription failure")
        elif kind == "batch":
            for event in data["events"]:
                session = integer(event["session"])
                require(session == data["session"] and event["frame_seq"] == data["frame_seq"], "batch context mismatch")
                scope = (session, integer(event["connection"]))
                require(scope in states, "canonical event has unknown connection")
                state = states[scope]
                require(integer(event["schema_version"], 16) == 1, "unexpected canonical schema")
                require(event["venue"] == state["venue"], "canonical venue mismatch")
                item = event["payload"]
                if active.get(session) != scope:
                    require(scope in closed and item == {"VenueStatus": {"message": "degraded"}}
                            and state["close_reason"] == "LocalStop;continuity_unverified"
                            and event["receive_ts"] == state["close_now"]
                            and state["shutdown_status"] is None and state["live"] is not None,
                            "canonical event connection mismatch or unsupported post-close event")
                    state["shutdown_status"] = {"journal_sequence": sequence, "event": event}
                    continue
                if "VenueStatus" in item:
                    status = item["VenueStatus"]["message"]
                    if status == "live":
                        require(state["live"] is None and state["requests"]
                                and set(state["topics"]) == state["expected"], "live before exact outbound plan")
                        require(all(r["ack"] for r in state["requests"].values()), "live before every ACK")
                        require(set(state["bbo"]) == set(state["symbols"]), "live before every two-sided BBO")
                        require(set(state["snapshots"]) == state["l2"], "live before requested L2 snapshots")
                        key = session, event["frame_seq"]
                        require(key in originals, "live lacks original triggering input")
                        _, original, original_scope = originals[key]
                        require(original_scope == scope and state["start"] <= event["receive_ts"] < state["deadline"]
                                and event["receive_ts"] == original["receive_ts_ns"], "live receipt/deadline mismatch")
                        _, digest = payload(original)
                        state["live"] = witness(sequence, original, scope, digest, event)
                    elif status == "degraded":
                        require(state["live"] is None, "post-live degradation outside clean-sample scope")
                    else:
                        require(status == "connected", "unsupported finite lifecycle status")
                    continue
                key = session, event["frame_seq"]
                require(key in originals and key in accepted, "canonical event lacks accepted original")
                _, original, original_scope = originals[key]
                require(original_scope == scope and original["receive_ts_ns"] == event["receive_ts"],
                        "canonical/original receipt mismatch")
                if "Quote" not in item and "BookSnapshot" not in item:
                    continue  # financial values belong to the independent native oracles
                value, digest = payload(original)
                topic, native = value.get("topic"), value.get("data")
                require(type(native) is dict and topic in state["expected"], "book source missing requested topic")
                symbol = native.get("s")
                require(symbol in state["symbols"] and event["instrument"] == state["symbols"][symbol], "book identity mismatch")
                require(integer(native["u"]) > 0, "invalid native book update ID")
                require(value.get("type") == "snapshot" or native["u"] == 1, "canonical snapshot from partial source")
                require(event["source_sequence"] is None, "native book ID promoted to contiguous sequence")
                book = item.get("Quote", item.get("BookSnapshot"))
                require(book.get("provenance", {}).get("source_channel") == topic, "book provenance topic mismatch")
                observed = witness(sequence, original, scope, digest,
                                   {"topic": topic, "event_index": event["event_index"],
                                    "native_type": value.get("type"), "u": native.get("u"), "seq": native.get("seq")})
                if "Quote" in item:
                    require(topic == f"orderbook.1.{symbol}", "quote source is not L1")
                    require(fixed(book["bid_price"]) > 0 and fixed(book["bid_price"]) < fixed(book["ask_price"])
                            and fixed(book["bid_quantity"]) > 0 and fixed(book["ask_quantity"]) > 0,
                            "canonical BBO unavailable or crossed")
                    state["bbo"].setdefault(symbol, observed)
                else:
                    require(topic in state["l2"] and book["depth"] == int(topic.split(".")[1]), "L2 snapshot scope mismatch")
                    state["snapshots"].setdefault(topic, observed)
    summary = {}
    for scope, state in states.items():
        require(state["closed"] and state["live"] is not None, "venue did not complete clean startup")
        summary[str(state["venue"])] = {
            "session": scope[0], "connection": scope[1], "connection_now_ns": state["start"],
            "local_receipt_deadline_ns": state["deadline"], "native_conn_id": state["conn_id"],
            "requests": state["requests"], "bbo_witnesses": state["bbo"],
            "l2_snapshot_witnesses": state["snapshots"], "canonical_live_witness": state["live"],
            "clean_shutdown_status_witness": state["shutdown_status"],
            "live_after_connection_ns": state["live"]["receive_ts_ns"] - state["start"]}
    return {"record_counts": dict(counts), "accepted_inbound_frames": len(accepted), "venues": summary}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    result = {"schema": "marketfeed.bybit_readiness_finite_inspection.v1", "checks_passed": False,
              "dataset_admitted": False, "general_reader_or_replay_qualified": False,
              "scope": "sealed clean sample only; exact outbound/ACK correlation and initial canonical readiness; independent book/trade value oracles required separately",
              "deadline_policy": "local 20s receipt/timer TimestampNs domain, not native ACK deadline or monotonic elapsed guarantee",
              "inspector_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    try:
        cursor = committed_cursor(args.directory)
        result.update(committed_identity(args.directory, cursor))
        result.update(audit(args.directory, cursor))
        require(committed_identity(args.directory, cursor)["committed_prefix_sha256"] == result["committed_prefix_sha256"],
                "audited committed prefix changed")
        result["checks_passed"] = True
    except (AssertionError, ValueError, KeyError, TypeError, OSError) as error:
        result["error"] = f"{type(error).__name__}: {error}"
    print(json.dumps(result, indent=2))
    return 0 if result["checks_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
