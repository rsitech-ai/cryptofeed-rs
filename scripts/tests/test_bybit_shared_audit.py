"""Adversarial mutations of explicit native expectations, independent of Rust."""

import base64
import copy
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from audit_bybit_shared_feed import audit_records


def fixture():
    native = dict(
        T=1, s="BTCUSDT", S="Buy", v="2", p="10.25", i="9007199254740993", seq=7, BT=True, RPI=False
    )
    source = dict(
        source_channel="publicTrade.BTCUSDT",
        granularity="IndividualExecution",
        native_count=None,
        constituent_first_id=None,
        constituent_last_id=None,
        native_attributes=dict(
            T="1",
            s="BTCUSDT",
            S="Buy",
            v="2",
            p="10.25",
            i="9007199254740993",
            seq="7",
            BT="true",
            RPI="false",
            ts="2",
        ),
    )
    event = dict(
        session=1,
        frame_seq=1,
        venue=5,
        instrument=1,
        event_index=0,
        receive_ts=3,
        exchange_ts=1_000_000,
        source_sequence=None,
        flags=0,
        payload=dict(
            Trade=dict(
                trade_id="9007199254740993",
                price=dict(coefficient=1025, scale=2),
                quantity=dict(coefficient=2, scale=0),
                aggressor="Buy",
                provenance=source,
            )
        ),
    )
    frame = dict(
        kind="frame",
        data=dict(
            session=1,
            frame_seq=1,
            direction=0,
            opcode=1,
            receive_ts_ns=3,
            payload_base64=base64.b64encode(
                json.dumps(
                    dict(topic="publicTrade.BTCUSDT", type="snapshot", ts=2, data=[native])
                ).encode()
            ).decode(),
        ),
    )
    return [
        dict(
            kind="metadata",
            data=dict(
                kind="session",
                session_id=1,
                venue_id=5,
                catalog=[dict(instrument_id=1, native_symbol="BTCUSDT")],
            ),
        ),
        frame,
        dict(kind="batch", data=dict(session=1, frame_seq=1, events=[event])),
        dict(kind="frame_accepted", data=dict(session=1, frame_seq=1)),
    ]


def candles():
    records = fixture()
    native = dict(
        start=1,
        end=60000,
        interval="1",
        open="10",
        high="11",
        low="9",
        close="10",
        volume="2",
        turnover="20.25",
        confirm=False,
        timestamp=5,
    )
    message = dict(topic="kline.1.BTCUSDT", type="snapshot", ts=61000, data=[native])
    records[1]["data"]["payload_base64"] = base64.b64encode(json.dumps(message).encode()).decode()
    event = records[2]["data"]["events"][0]
    event["exchange_ts"] = 61_000_000_000
    event["payload"] = dict(
        Candle=dict(
            open=dict(coefficient=10, scale=0),
            high=dict(coefficient=11, scale=0),
            low=dict(coefficient=9, scale=0),
            close=dict(coefficient=10, scale=0),
            volume=dict(coefficient=2, scale=0),
            native_volume_ccy=dict(coefficient=2025, scale=2),
            quote_volume=None,
            is_closed=False,
            start_ts=1_000_000,
            interval_ns=60_000_000_000,
            provenance=dict(
                source_channel="kline.1.BTCUSDT",
                native_attributes=dict(
                    start="1",
                    end="60000",
                    interval="1",
                    open="10",
                    high="11",
                    low="9",
                    close="10",
                    volume="2",
                    turnover="20.25",
                    confirm="false",
                    timestamp="5",
                    ts="61000",
                ),
            ),
        )
    )
    return records


class BybitAuditTest(unittest.TestCase):
    def fails(self, records):
        try:
            result = audit_records(records)
        except (AssertionError, KeyError, StopIteration):
            return
        self.assertFalse(result["checks_passed"])

    def test_explicit_native_trade_and_candle_goldens(self):
        for records in [fixture(), candles()]:
            self.assertTrue(audit_records(records)["checks_passed"])

    def test_native_trade_mutations_are_not_green(self):
        for field in [
            "side",
            "price",
            "BT",
            "RPI",
            "seq",
            "rowloss",
            "sequence",
            "granularity",
            "source",
            "time",
            "receive",
            "history",
        ]:
            with self.subTest(field=field):
                records = fixture()
                event = records[2]["data"]["events"][0]
                row = event["payload"]["Trade"]
                if field == "side":
                    row["aggressor"] = "Sell"
                elif field == "price":
                    row["price"]["coefficient"] += 1
                elif field in ("BT", "RPI", "seq"):
                    row["provenance"]["native_attributes"][field] = "changed"
                elif field == "rowloss":
                    records[2]["data"]["events"] = []
                elif field == "sequence":
                    event["source_sequence"] = dict(first=7, last=7)
                elif field == "granularity":
                    row["provenance"]["granularity"] = "NativeAggregate"
                elif field == "source":
                    row["provenance"]["source_channel"] = "wrong"
                elif field == "time":
                    event["exchange_ts"] = 2_000_000
                elif field == "receive":
                    event["receive_ts"] += 1
                else:
                    event["flags"] = 512
                self.fails(records)

    def test_candle_finality_native_time_and_unit_mutations_are_not_green(self):
        for field in [
            "finality",
            "lastmatch",
            "end",
            "turnover",
            "publication",
            "interval",
            "quote",
            "type",
        ]:
            with self.subTest(field=field):
                records = candles()
                event = records[2]["data"]["events"][0]
                row = event["payload"]["Candle"]
                if field == "finality":
                    row["is_closed"] = True
                elif field == "type":
                    row["is_closed"] = 0
                elif field in ("lastmatch", "end"):
                    row["provenance"]["native_attributes"][
                        "timestamp" if field == "lastmatch" else "end"
                    ] = "123"
                elif field == "turnover":
                    row["native_volume_ccy"]["coefficient"] += 1
                elif field == "publication":
                    event["exchange_ts"] = 5_000_000
                elif field == "interval":
                    row["interval_ns"] = 1
                else:
                    row["quote_volume"] = row["native_volume_ccy"]
                self.fails(records)

    def test_missing_acceptance_duplicates_source_errors_and_required_venue_fail(self):
        records = fixture()
        records.pop()
        self.fails(records)
        records = fixture()
        records[2]["data"]["events"] *= 2
        self.fails(records)
        records = fixture()
        records.append(
            dict(kind="system", data=dict(event=dict(ParseError=dict(detail="bad native row"))))
        )
        self.fails(records)
        self.assertFalse(audit_records(fixture(), [6])["checks_passed"])

    def test_second_row_cannot_disappear_even_with_repeated_cross_sequence(self):
        records = fixture()
        message = json.loads(base64.b64decode(records[1]["data"]["payload_base64"]))
        message["data"] *= 2
        records[1]["data"]["payload_base64"] = base64.b64encode(
            json.dumps(message).encode()
        ).decode()
        self.fails(records)
        second = copy.deepcopy(records[2]["data"]["events"][0])
        second["event_index"] = 1
        records[2]["data"]["events"].append(second)
        self.assertTrue(audit_records(records)["checks_passed"])

    def test_review_orphan_receipt_primitive_types_duplicate_keys_and_candle_flag(self):
        records = fixture()
        records.append(dict(kind="frame_accepted", data=dict(session=999, frame_seq=999)))
        self.fails(records)
        for field, value in [
            ("event_index", False),
            ("receive_ts", 3.0),
            ("frame_seq", True),
            ("instrument", 1.0),
            ("flags", False),
        ]:
            with self.subTest(field=field):
                records = fixture()
                records[2]["data"]["events"][0][field] = value
                self.fails(records)
        records = fixture()
        raw = (
            base64.b64decode(records[1]["data"]["payload_base64"])
            .decode()
            .replace('"p": "10.25"', '"p": "999", "p": "10.25"')
        )
        records[1]["data"]["payload_base64"] = base64.b64encode(raw.encode()).decode()
        self.fails(records)
        records = candles()
        records[2]["data"]["events"][0]["flags"] = 512
        self.fails(records)

    def test_review_candle_native_integer_types_and_enum_shape_fail(self):
        for field in ("start_ts", "interval_ns"):
            records = candles()
            row = records[2]["data"]["events"][0]["payload"]["Candle"]
            row[field] = float(row[field])
            self.fails(records)
        for records, key in [(fixture(), "T"), (candles(), "start")]:
            message = json.loads(base64.b64decode(records[1]["data"]["payload_base64"]))
            message["data"][0][key] = str(message["data"][0][key])
            records[1]["data"]["payload_base64"] = base64.b64encode(
                json.dumps(message).encode()
            ).decode()
            self.fails(records)
        records = fixture()
        records[2]["data"]["events"][0]["payload"]["Candle"] = {}
        self.fails(records)

    def test_recorded_binary_acceptance_is_distinct_from_an_orphan(self):
        records = fixture()
        records.extend(
            [
                dict(
                    kind="frame",
                    data=dict(
                        session=1,
                        frame_seq=2,
                        direction=0,
                        opcode=2,
                        receive_ts_ns=4,
                        payload_base64="AA==",
                    ),
                ),
                dict(kind="frame_accepted", data=dict(session=1, frame_seq=2)),
            ]
        )
        self.assertTrue(audit_records(records)["checks_passed"])

    def test_real_framing_hashes_only_the_pinned_prefix_and_rejects_changed_watermark(self):
        import tempfile, struct, hashlib
        from audit_shared_feed import scan, committed_cursor, committed_identity
        from audit_trade_capture import crc32c

        feed = "a" * 40
        payload = json.dumps(dict(sequence=1, record=fixture()[0])).encode()
        original = (
            b"MFSF1\0\0\0"
            + feed.encode()
            + struct.pack("<II", len(payload), crc32c(payload))
            + payload
        )
        cursor = dict(feed_id=feed, sequence=1, offset=len(original))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "events.mfsf"
            with path.open("wb") as output:
                output.write(original)
                output.seek(256 * 1024 * 1024)
                output.write(b"x")
            watermark = root / "committed.json"
            watermark.write_text(json.dumps(dict(schema_version=1, cursor=cursor)))
            self.assertEqual(list(scan(root, cursor)), [fixture()[0]])
            self.assertEqual(
                committed_identity(root, cursor)["committed_prefix_sha256"],
                hashlib.sha256(original).hexdigest(),
            )
            changed = dict(cursor, sequence=True)
            watermark.write_text(json.dumps(dict(schema_version=1, cursor=changed)))
            with self.assertRaises(AssertionError):
                committed_cursor(root)
            changed = dict(cursor, offset=cursor["offset"] + 1)
            watermark.write_text(json.dumps(dict(schema_version=1, cursor=changed)))
            with self.assertRaisesRegex(AssertionError, "changed"):
                committed_identity(root, cursor)


if __name__ == "__main__":
    unittest.main()
