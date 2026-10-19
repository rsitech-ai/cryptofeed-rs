#!/usr/bin/env python3
"""Reproduce finite-inspector logic challenges; stdout receipt, no file writes.

Synthetic cases replace only scan() and the final file-size lookup, so they
exercise readiness logic, not framing/CRC. The older-corpus negative invokes the
unchanged real inspector in a separate process against its closed exact prefix.
No network, capture, runtime mutation, or qualification.
"""
import argparse
import base64
import copy
import hashlib
import json
from pathlib import Path
import runpy
import subprocess
import sys
from types import SimpleNamespace

REPO = Path(__file__).resolve().parents[3]
EVIDENCE = REPO / ".local/evidence/hyperliquid-integration-review"
OLD_PREFIX = "a53a06f18c93e37a41473f88728d7880442d7801fc06c0b022d3f3bd332b74c0"
OLD_CURSOR = {"feed_id": "97fc91bac89f8d1fc31eedcc06f86e0620637cb8", "sequence": 3478, "offset": 3507908}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class SyntheticDirectory:
    def __truediv__(self, filename):
        assert filename == "events.mfsf"
        return self

    def stat(self):
        return SimpleNamespace(st_size=0)


def fixtures(module):
    records = []

    def add(kind, data):
        records.append({"kind": kind, "data": data})

    def frame(session, ordinal, at, value, direction=0):
        add("frame", {"session": session, "frame_seq": ordinal, "receive_ts_ns": at,
                      "monotonic_ns": at, "direction": direction, "opcode": 1,
                      "payload_base64": base64.b64encode(json.dumps(value).encode()).decode()})

    def event(session, venue, ordinal, at, payload, instrument=None):
        return {"schema_version": 1, "venue": venue, "session": session, "connection": session,
                "frame_seq": ordinal, "event_index": 0, "receive_ts": at,
                "exchange_ts": None, "instrument": instrument, "source_sequence": None,
                "flags": 0, "payload": payload}

    def batch(session, ordinal, value):
        add("batch", {"session": session, "frame_seq": ordinal, "events": [value]})

    def fixed(value):
        return {"coefficient": value, "scale": 0}

    for session, (venue, category) in enumerate(module["EXPECTED"].items(), 1):
        metadata = {"kind": "session", "session_id": session, "venue_id": venue,
                    "endpoint": f"wss://stream.bybit.com/v5/public/{category}",
                    "catalog": [{"instrument_id": 1, "native_symbol": "BTC"}],
                    "initial_subscriptions": [
                        {"instrument_id": 1, "channel": "Trades"},
                        {"instrument_id": 1, "channel": "Quote"},
                        {"instrument_id": 1, "channel": "L2Book { depth: None, cadence: None }"}]}
        add("metadata", metadata)
        add("connection", {"session": session, "connection": session, "connected": True, "now_ns": 0})
        _, topics, _ = module["requested_topics"](metadata)
        frame(session, 0, 0, {"op": "subscribe", "req_id": "synthetic-request", "args": sorted(topics)}, 1)
        frame(session, 1, 100, {"op": "subscribe", "req_id": "synthetic-request", "success": True,
                                "conn_id": "native-" + category, "ret_msg": ""})
        add("frame_accepted", {"session": session, "frame_seq": 1})
        for ordinal, depth, at in [(2, 1, 200), (3, 50, 300)]:
            topic = f"orderbook.{depth}.BTC"
            frame(session, ordinal, at, {"topic": topic, "type": "snapshot", "ts": 0,
                                         "data": {"s": "BTC", "u": 10, "seq": 100,
                                                  "b": [["100", "1"]], "a": [["101", "1"]]}})
            book = {"provenance": {"source_channel": topic}}
            if depth == 1:
                name = "Quote"
                book.update({"bid_price": fixed(100), "ask_price": fixed(101),
                             "bid_quantity": fixed(1), "ask_quantity": fixed(1)})
            else:
                name = "BookSnapshot"
                book.update({"depth": 50, "bids": [], "asks": []})
            batch(session, ordinal, event(session, venue, ordinal, at, {name: book}, 1))
            if depth == 50:
                batch(session, ordinal, event(session, venue, ordinal, at, {"VenueStatus": {"message": "live"}}))
            add("frame_accepted", {"session": session, "frame_seq": ordinal})
        add("connection", {"session": session, "connection": session, "connected": False,
                           "now_ns": 400, "reason": "LocalStop;continuity_unverified"})
        batch(session, 3, event(session, venue, 3, 400, {"VenueStatus": {"message": "degraded"}}))
    add("seal", {"schema_version": 1, "reason": "clean_shutdown;exchange_continuity_unverified"})
    return records


def mutate(records, case):
    result = copy.deepcopy(records)
    if case == "wrong_ack":
        record = next(r for r in result if r["kind"] == "frame" and r["data"]["direction"] == 0)
        value = json.loads(base64.b64decode(record["data"]["payload_base64"]))
        value["req_id"] = "foreign"
        record["data"]["payload_base64"] = base64.b64encode(json.dumps(value).encode()).decode()
    elif case == "missing_bbo":
        for record in result:
            if record["kind"] == "batch":
                record["data"]["events"] = [e for e in record["data"]["events"] if "Quote" not in e["payload"]]
    elif case == "missing_acceptance":
        result.remove(next(r for r in result if r["kind"] == "frame_accepted"))
    elif case == "unit_heartbeat_failure":
        result.insert(-1, {"kind": "system", "data": {"session": 1, "event": "HeartbeatMissed"}})
    else:
        index = next(i for i, r in enumerate(result) if r["kind"] == "connection" and not r["data"]["connected"])
        event = next(r["data"]["events"][0] for r in result[index:] if r["kind"] == "batch")
        if case == "post_close_live":
            event["payload"] = {"VenueStatus": {"message": "live"}}
        elif case == "post_close_financial":
            event["payload"] = {"Trade": {}}
        elif case == "post_close_wrong_timestamp":
            event["receive_ts"] += 1
        elif case == "remote_close_is_not_local_stop":
            result[index]["data"]["reason"] = "RemoteClose;continuity_unverified"
        elif case == "repeated_post_close_status":
            status_index = next(i for i in range(index, len(result)) if result[i]["kind"] == "batch")
            result.insert(status_index + 1, copy.deepcopy(result[status_index]))
        else:
            raise AssertionError("unknown fixture")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inspector", type=Path, default=EVIDENCE / "bybit_readiness_finite_audit_01.py")
    parser.add_argument("--older-feed", type=Path, default=EVIDENCE / "bybit-catalog-pilot-01-feed")
    args = parser.parse_args()
    module = runpy.run_path(str(args.inspector), run_name="finite_fixture_module")
    audit = module["audit"]
    original_scan = audit.__globals__["scan"]

    def challenge(rows):
        try:
            audit.__globals__["scan"] = lambda *_: iter(copy.deepcopy(rows))
            return audit(SyntheticDirectory(), {"offset": 0})
        finally:
            audit.__globals__["scan"] = original_scan

    rows = fixtures(module)
    result = challenge(rows)
    assert set(result["venues"]) == {"5", "6", "11"}
    assert result["accepted_inbound_frames"] == 9
    for state in result["venues"].values():
        assert state["clean_shutdown_status_witness"] is not None, "LocalStop exception must actually be traversed"
        assert len(state["requests"]) == len(state["bbo_witnesses"]) == len(state["l2_snapshot_witnesses"]) == 1
    cases = [{"name": "three_venue_positive_with_exact_local_stop_exception", "passed": True,
              "fixture_sha256": digest(rows), "accepted_inbound_frames": 9}]
    expected_errors = {
        "wrong_ack": "uncorrelated native ACK",
        "missing_bbo": "live before every two-sided BBO",
        "missing_acceptance": "unaccepted original inbound frames",
        "unit_heartbeat_failure": "unclean system event: HeartbeatMissed",
        "post_close_live": "canonical event connection mismatch or unsupported post-close event",
        "post_close_financial": "canonical event connection mismatch or unsupported post-close event",
        "post_close_wrong_timestamp": "canonical event connection mismatch or unsupported post-close event",
        "remote_close_is_not_local_stop": "canonical event connection mismatch or unsupported post-close event",
        "repeated_post_close_status": "canonical event connection mismatch or unsupported post-close event",
    }
    for name, expected in expected_errors.items():
        malformed = mutate(rows, name)
        try:
            challenge(malformed)
        except AssertionError as error:
            assert str(error) == expected, (name, str(error), expected)
            cases.append({"name": name, "passed": True, "fixture_sha256": digest(malformed),
                          "observed_rejection": str(error)})
        else:
            raise AssertionError(name + " unexpectedly passed")
    command = [sys.executable, "-B", str(args.inspector), str(args.older_feed)]
    older = subprocess.run(command, capture_output=True, check=False)
    assert older.returncode == 1 and not older.stderr, older.stderr.decode()
    older_result = json.loads(older.stdout)
    assert older_result["checks_passed"] is False
    assert older_result["error"] == "AssertionError: invalid/reused outbound request ID"
    assert older_result["cursor"] == OLD_CURSOR
    assert older_result["committed_prefix_sha256"] == OLD_PREFIX
    inspector_sha = hashlib.sha256(args.inspector.read_bytes()).hexdigest()
    assert older_result["inspector_sha256"] == inspector_sha
    cases.append({"name": "original_older_closed_corpus_rejected", "passed": True,
                  "command": command, "exit_code": older.returncode,
                  "observed_rejection": older_result["error"], "cursor": OLD_CURSOR,
                  "committed_prefix_sha256": OLD_PREFIX,
                  "inspector_stdout_sha256": hashlib.sha256(older.stdout).hexdigest()})
    receipt = {"schema": "marketfeed.bybit_readiness_finite_challenge.v1", "checks_passed": True,
               "challenge_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
               "inspector_sha256": inspector_sha, "cases": cases,
               "case_count": len(cases), "original_final_negative_case_count": 7,
               "extra_local_stop_policy_negative_case_count": 2,
               "scope": "synthetic readiness-logic fixtures bypass framing; original older-corpus control uses real committed cursor/CRC scan; no live capture, continuity or dataset qualification"}
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    if not __debug__:
        raise RuntimeError("assertions must remain enabled")
    main()
