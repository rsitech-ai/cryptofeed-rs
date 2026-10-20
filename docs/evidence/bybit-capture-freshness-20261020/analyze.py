#!/usr/bin/env python3
"""Read-only finite analysis of two frozen failed Bybit samples. No network/builds.

Uses committed-prefix CRC scanning and raw MFR1 CRC scanning independently.
No missing trade or source-time interval is turned into successful coverage.
"""
import base64
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import subprocess
import sys

REPO = Path(__file__).resolve().parents[3]
ROOT = REPO / ".local/evidence/hyperliquid-integration-review"
sys.path.insert(0, str(REPO / "scripts"))
from audit_shared_feed import committed_cursor, committed_identity, scan, strict_json, integer
from audit_trade_capture import records as raw_records


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def signature(data, raw):
    return (data["session"], data["frame_seq"], data["direction"], data["opcode"],
            data["receive_ts_ns"], data["monotonic_ns"], hashlib.sha256(raw).hexdigest())


def witness(index, data, value):
    return {"journal_sequence": index, "frame_seq": data["frame_seq"],
            "session": data["session"], "receipt_ns": data["receive_ts_ns"],
            "monotonic_ns": data["monotonic_ns"], "native_ts_ms": value.get("ts"),
            "payload_sha256": hashlib.sha256(base64.b64decode(data["payload_base64"], validate=True)).hexdigest()}


def analyze(stem):
    directory = ROOT / (stem + "-feed")
    cursor = committed_cursor(directory)
    identity = committed_identity(directory, cursor)
    receipt_path = ROOT / (stem + "-receipt.json")
    receipt = strict_json(receipt_path.read_bytes())
    assert receipt["exit"] == 0 and receipt["stop_condition"] == "duration" and receipt["source_clean"] is True
    metadata, connections, active = {}, {}, {}
    streams, candles, trade_tickers = defaultdict(list), [], []
    original, accepted, shared_signatures = set(), set(), Counter()
    counts, channels, native_trades, canonical_trades = Counter(), Counter(), Counter(), Counter()
    requests, acknowledgements, source_errors = defaultdict(list), defaultdict(list), []
    sealed = False
    failure_names = {"ParseError", "UnknownMessage", "EventsDropped", "SequenceGap", "BookInvalidated",
                     "QuoteInvalidated", "HeartbeatMissed", "RateLimited", "DiskPressure"}
    for index, record in enumerate(scan(directory, cursor), 1):
        assert not sealed
        kind, data = record["kind"], record["data"]
        counts[kind] += 1
        if kind == "metadata" and data["kind"] == "session":
            assert data["session_id"] not in metadata
            metadata[data["session_id"]] = data
        elif kind == "connection":
            session = data["session"]
            venue = metadata[session]["venue_id"]
            if data["connected"]:
                assert venue not in connections and session not in active
                connections[venue] = {"session": session, "connection": data["connection"],
                                      "opened_ns": data["now_ns"], "closed_ns": None}
                active[session] = data["connection"]
            else:
                assert active.pop(session) == data["connection"]
                connections[venue]["closed_ns"] = data["now_ns"]
        elif kind == "frame":
            assert data["session"] in active
            raw = base64.b64decode(data["payload_base64"], validate=True)
            shared_signatures[signature(data, raw)] += 1
            if data["direction"] == 0:
                key = data["session"], data["frame_seq"]
                assert key not in original
                original.add(key)
            if data["opcode"] != 1:
                continue
            value = strict_json(raw)
            assert type(value) is dict
            venue = metadata[data["session"]]["venue_id"]
            if data["direction"] == 1:
                if value.get("op") == "subscribe":
                    requests[venue].append({"witness": witness(index, data, value), "control": value})
                continue
            topic = value.get("topic")
            if topic:
                channels[f"{venue}:{topic}"] += 1
                if topic.startswith("publicTrade."):
                    native_trades[str(venue)] += len(value["data"])
                if topic.startswith(("orderbook.50.", "tickers.")):
                    integer(value["ts"], 64, True)
                    streams[venue, topic].append(witness(index, data, value))
                if venue == 11 and topic == "kline.1.BTCUSD":
                    candles.append({"witness": witness(index, data, value), "native": value})
                if venue == 11 and topic == "tickers.BTCUSD":
                    fields = {k: value["data"][k] for k in ("lastPrice", "volume24h", "turnover24h") if k in value["data"]}
                    if fields:
                        trade_tickers.append({"witness": witness(index, data, value), "native_fields": fields})
            elif value.get("op") == "subscribe":
                acknowledgements[venue].append({"witness": witness(index, data, value), "control": value})
        elif kind == "frame_accepted":
            key = data["session"], data["frame_seq"]
            assert key in original and key not in accepted
            accepted.add(key)
        elif kind == "batch":
            for event in data["events"]:
                if "Trade" in event["payload"]:
                    canonical_trades[str(event["venue"])] += 1
        elif kind == "system":
            event = data["event"]
            names = {event} if type(event) is str else set(event)
            if names & failure_names:
                source_errors.append({"journal_sequence": index, "data": data})
        elif kind == "seal":
            assert data["reason"].startswith("clean_shutdown")
            sealed = True
    assert sealed and not active and set(connections) == {5, 6, 11}
    assert all(value["closed_ns"] is not None for value in connections.values())
    assert original == accepted and not source_errors
    assert (directory / "events.mfsf").stat().st_size == cursor["offset"]
    raw_signatures, raw_channels, raw_trade_rows, raw_files = Counter(), Counter(), Counter(), []
    for path in sorted((ROOT / (stem + "-raw")).glob("*.mfr1")):
        before = sha(path)
        raw_files.append({"path": str(path.relative_to(REPO)), "bytes": path.stat().st_size, "sha256": before})
        for record in raw_records(path):
            if not 1 <= record["opcode"] <= 5:
                continue
            data = {"session": record["session"], "frame_seq": record["seq"],
                    "receive_ts_ns": record["receipt_ns"], "monotonic_ns": record["monotonic_ns"],
                    "direction": record["direction"], "opcode": record["opcode"]}
            raw_signatures[signature(data, record["payload"])] += 1
            if record["direction"] == 0 and record["opcode"] == 1:
                value = strict_json(record["payload"])
                topic = value.get("topic", "")
                venue = metadata[record["session"]]["venue_id"]
                if topic:
                    raw_channels[f"{venue}:{topic}"] += 1
                if topic.startswith("publicTrade."):
                    raw_trade_rows[str(venue)] += len(value["data"])
        assert sha(path) == before
    assert raw_files and raw_signatures == shared_signatures and raw_channels == channels
    assert raw_trade_rows == native_trades == canonical_trades
    assert native_trades["11"] == 0 and channels["11:publicTrade.BTCUSD"] == 0
    spans = []
    for (venue, topic), values in sorted(streams.items()):
        first, last = values[0], values[-1]
        receipt_span = last["receipt_ns"] - first["receipt_ns"]
        monotonic_span = last["monotonic_ns"] - first["monotonic_ns"]
        native_span = (last["native_ts_ms"] - first["native_ts_ms"]) * 1_000_000
        spans.append({"venue": venue, "topic": topic, "frames": len(values), "first": first, "last": last,
                      "native_generation_span_ns": native_span, "receipt_utc_span_ns": receipt_span,
                      "receipt_monotonic_span_ns": monotonic_span,
                      "utc_monotonic_span_difference_ns": receipt_span - monotonic_span,
                      "start_native_to_receipt_age_ns": first["receipt_ns"] - first["native_ts_ms"] * 1_000_000,
                      "end_native_to_receipt_age_ns": last["receipt_ns"] - last["native_ts_ms"] * 1_000_000,
                      "age_growth_using_receipt_utc_ns": receipt_span - native_span,
                      "age_growth_using_receipt_monotonic_ns": monotonic_span - native_span})
    controls = {}
    for venue in (5, 6, 11):
        req, ack = requests[venue], acknowledgements[venue]
        assert len(req) == len(ack) == 1
        out, inbound = req[0]["control"], ack[0]["control"]
        symbol = "BTCUSD" if venue == 11 else "BTCUSDT"
        assert f"publicTrade.{symbol}" in out["args"] and inbound["success"] is True
        echoed = type(out.get("req_id")) is str and bool(out["req_id"]) and inbound.get("req_id") == out["req_id"]
        if stem == "bybit-readiness-pilot-01":
            assert echoed and type(inbound.get("conn_id")) is str and inbound["conn_id"]
        controls[str(venue)] = {"outbound": req[0], "inbound": ack[0], "exact_nonempty_request_id_echo": echoed,
                                "endpoint": metadata[connections[venue]["session"]]["endpoint"]}
    assert committed_identity(directory, cursor) == identity
    return {"stem": stem, "source": receipt["source"], "launch_receipt_path": str(receipt_path.relative_to(REPO)),
            "launch_receipt_sha256": sha(receipt_path), **identity, "raw_files": raw_files,
            "saved_application_frame_parity": True, "application_frames": sum(raw_signatures.values()),
            "accepted_inbound_frames": len(accepted), "connections": connections,
            "channel_frame_counts": dict(channels), "native_trade_rows_by_venue": dict(native_trades),
            "canonical_trade_rows_by_venue": dict(canonical_trades), "inverse_trade_frames_raw_and_shared": 0,
            "required_trade_venues_missing": [11], "recorded_source_errors": source_errors,
            "publisher_spans": spans, "request_ack_evidence": controls,
            "inverse_candle_witnesses": candles, "inverse_trade_related_ticker_witnesses": trade_tickers}


def main():
    captures = [analyze(stem) for stem in ("bybit-catalog-pilot-01", "bybit-readiness-pilot-01")]
    source = "410c3cae95f17c3b2b8ee88a9809c5fc1d7ad28b"
    paths = ["crates/engine/src/live.rs", "crates/engine/src/runner.rs", "crates/recording/src/shared_feed.rs",
             "crates/adapters/bybit/src/session.rs", "crates/adapters/bybit/src/messages.rs"]
    blobs = {}
    for path in paths:
        data = subprocess.check_output(["git", "-C", str(REPO), "show", source + ":" + path])
        blobs[path] = hashlib.sha256(data).hexdigest()
    result = {"schema": "marketfeed.bybit_frozen_lag_analysis.v1", "analysis_completed": True,
              "helper_sha256": sha(Path(__file__)), "captures": captures,
              "source_freshness_qualified": False, "all_required_trade_venues_observed": False,
              "dataset_admitted": False, "cause_attributed": False,
              "timestamp_contract": {"official_documentation": "https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook",
                                     "accessed": "2026-10-20", "ts": "milliseconds of system data generation",
                                     "cts": "matching-engine timestamp, not used for the reported publisher spans",
                                     "local_receipt": "live loop stamps after transport.read_frame returns, before synchronous runner callback; not kernel/wire arrival",
                                     "caveat": "absolute age needs source/local clock alignment; age growth cancels constant offset; matching UTC/monotonic spans exclude a local UTC-step explanation, not remote clock/source-delay anomalies"},
              "source_path_hypothesis": {"source": source, "git_blob_sha256": blobs,
                                         "live.rs": "read_frame354; post-read stamp382; synchronous on_text_frame393",
                                         "runner.rs": "record_frame363 before decode; direct feed.append517; frame action transaction append_many541",
                                         "shared_feed.rs": "file.sync_data343; watermark.sync_all369; rename371; directory.sync_all372; shared Mutex append384/390",
                                         "inference": "serialized per-operation durable work can block each reader callback and contend across sessions; no stage timings in retained evidence prove whether it caused the measured lag rather than network/server replay/delay"},
              "disposition": "zero inverse trade frames were saved, so no post-recording inverse trade decoder drop is demonstrated; correct topic/ACK is observed, but neither full-duration quietness nor channel/source completeness is established by the lagging prefixes",
              "smallest_next_diagnostic": {
                  "kind": "registered finite offline three-session paced-ingress attribution before any new capture",
                  "arms": ["same fixed synthetic generation schedule and immutable payload copies, parser/raw path with shared feed disabled as diagnostic baseline only", "same schedule and raw path with production durable shared writer enabled"],
                  "measurement": "separate producer schedule/enqueue, transport read return, callback/parse, shared Mutex wait, data sync, watermark/directory sync and accepted commit monotonic times; record exact input hash/rate/burst/bytes/time bounds before running",
                  "suggested_registered_schedule": "10s at declared aggregate480frames/s plus one declared64-frame burst;15s total stop/drain cap; bounded frame and byte queues; rates are a local stress diagnostic, not guaranteed native message counts",
                  "suggested_acceptance": "all generated inputs accounted for; committed source-generation span>=9.5s; receive/source age growth<=1s and terminal queue empty by the predeclared drain deadline, or fail freshness; injected blocking/overflow must revoke admission, never publish undurable accepted data; raw/shared equality, CRC/seal and fsync/publication guarantees unchanged",
                  "repair_if_attributed": "only after measured attribution, decouple/batch durable service using bounded queues and explicit durable acceptance while preserving original read-return receipt stamps and fail-closed overflow/publication semantics; do not weaken sync or use later history to fill frozen windows"}}
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    if not __debug__:
        raise RuntimeError("assertions must remain enabled")
    main()
