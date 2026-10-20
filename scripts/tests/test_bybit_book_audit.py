"""Explicit native book expectations with source/canonical mutation challenges."""

import base64
import copy
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from audit_bybit_books import audit_records, parts


def fixture():
    records = [
        dict(
            kind="metadata",
            data=dict(
                kind="session",
                session_id=1,
                venue_id=5,
                catalog=[dict(instrument_id=1, native_symbol="BTCUSDT")],
            ),
        )
    ]
    for frame, depth, kind, u, seq in [
        (1, 50, "snapshot", 10, 100),
        (2, 50, "delta", 30, 900),
        (3, 50, "delta", 30, 900),
        (4, 1, "snapshot", 8, 90),
    ]:
        topic = f"orderbook.{depth}.BTCUSDT"
        ts = 1000
        cts = 999
        row = dict(
            s="BTCUSDT",
            b=[["100.00", "1.000" if kind == "snapshot" else "2.000"]],
            a=[["101.00", "1.000"]] if kind == "snapshot" else [],
            u=u,
            seq=seq,
        )
        value = dict(topic=topic, type=kind, ts=ts, cts=cts, data=row)
        records.append(
            dict(
                kind="frame",
                data=dict(
                    session=1,
                    frame_seq=frame,
                    direction=0,
                    opcode=1,
                    receive_ts_ns=2_000_000_000,
                    payload_base64=base64.b64encode(
                        json.dumps(value).encode()
                    ).decode(),
                ),
            )
        )
        if frame != 3:
            source = dict(
                source_channel=topic,
                native_attributes=dict(
                    s="BTCUSDT", u=str(u), seq=str(seq), type=kind, ts="1000", cts="999"
                ),
            )
            if depth == 1:
                payload = dict(
                    Quote=dict(
                        provenance=source,
                        bid_price=dict(coefficient=10000, scale=2),
                        bid_quantity=dict(coefficient=1000, scale=3),
                        ask_price=dict(coefficient=10100, scale=2),
                        ask_quantity=dict(coefficient=1000, scale=3),
                    )
                )
            elif kind == "snapshot":
                payload = dict(
                    BookSnapshot=dict(
                        provenance=source,
                        bids=[
                            dict(
                                price=dict(coefficient=10000, scale=2),
                                quantity=dict(coefficient=1000, scale=3),
                            )
                        ],
                        asks=[
                            dict(
                                price=dict(coefficient=10100, scale=2),
                                quantity=dict(coefficient=1000, scale=3),
                            )
                        ],
                        depth=50,
                        checksum=None,
                    )
                )
            else:
                payload = dict(
                    BookDelta=dict(
                        provenance=source,
                        changes=[
                            dict(
                                side="Bid",
                                operation="Upsert",
                                price=dict(coefficient=10000, scale=2),
                                quantity=dict(coefficient=2000, scale=3),
                            )
                        ],
                        checksum=None,
                    )
                )
            event = dict(
                session=1,
                frame_seq=frame,
                event_index=0,
                venue=5,
                instrument=1,
                exchange_ts=1_000_000_000,
                receive_ts=2_000_000_000,
                source_sequence=None,
                flags=1 if kind == "snapshot" else 2,
                payload=payload,
            )
            records.append(
                dict(
                    kind="batch", data=dict(session=1, frame_seq=frame, events=[event])
                )
            )
        records.append(
            dict(kind="frame_accepted", data=dict(session=1, frame_seq=frame))
        )
    return records


def first_event(records, name):
    return next(
        record["data"]["events"][0]
        for record in records
        if record["kind"] == "batch" and name in record["data"]["events"][0]["payload"]
    )


def mutate_native(records, frame, change):
    record = next(
        record
        for record in records
        if record["kind"] == "frame" and record["data"]["frame_seq"] == frame
    )
    value = json.loads(base64.b64decode(record["data"]["payload_base64"]))
    change(value)
    record["data"]["payload_base64"] = base64.b64encode(
        json.dumps(value).encode()
    ).decode()


class BybitBookAuditTest(unittest.TestCase):
    def fails(self, records):
        try:
            result = audit_records(records, [5])
        except (AssertionError, ValueError, KeyError, StopIteration):
            return
        self.assertFalse(result["checks_passed"])

    def test_native_nonunit_ids_duplicate_and_bbo_golden_pass(self):
        result = audit_records(fixture(), [5])
        self.assertTrue(result["checks_passed"])
        self.assertEqual(result["counts"]["5:50:exact_duplicate"], 1)
        self.assertEqual(result["observed_update_id_steps"]["5:50:20"], 1)
        self.assertEqual(result["native_book_quote_rows"], 3)

    def test_changed_exact_value_provenance_order_scope_and_time_cannot_pass(self):
        for field in (
            "price",
            "scale",
            "u",
            "cts",
            "sequence",
            "flags",
            "depth",
            "time",
            "instrument",
            "enum",
            "delete",
        ):
            with self.subTest(field=field):
                records = fixture()
                event = first_event(records, "BookSnapshot")
                row = event["payload"]["BookSnapshot"]
                if field == "price":
                    row["bids"][0]["price"]["coefficient"] += 1
                elif field == "scale":
                    row["bids"][0]["price"]["scale"] = 3
                elif field == "u":
                    row["provenance"]["native_attributes"]["u"] = "11"
                elif field == "cts":
                    row["provenance"]["native_attributes"].pop("cts")
                elif field == "sequence":
                    event["source_sequence"] = dict(first=10, last=10)
                elif field == "flags":
                    event["flags"] = False
                elif field == "depth":
                    row["depth"] = 50.0
                elif field == "time":
                    event["exchange_ts"] = float(event["exchange_ts"])
                elif field == "instrument":
                    event["instrument"] = True
                elif field == "enum":
                    event["payload"]["Quote"] = {}
                else:
                    first_event(records, "BookDelta")["payload"]["BookDelta"][
                        "changes"
                    ][0]["operation"] = "Delete"
                self.fails(records)

    def test_missing_extra_and_duplicate_canonical_rows_fail(self):
        records = fixture()
        records = [
            record
            for record in records
            if not (record["kind"] == "batch" and record["data"]["frame_seq"] == 2)
        ]
        self.fails(records)
        records = fixture()
        batch = next(record for record in records if record["kind"] == "batch")
        batch["data"]["events"] *= 2
        self.fails(records)
        records = fixture()
        extra = copy.deepcopy(
            next(
                record
                for record in records
                if record["kind"] == "batch" and record["data"]["frame_seq"] == 2
            )
        )
        extra["data"]["frame_seq"] = 3
        extra["data"]["events"][0]["frame_seq"] = 3
        records.append(extra)
        self.fails(records)

    def test_orphan_acceptance_source_fault_and_required_venue_fail(self):
        records = fixture()
        records.append(dict(kind="frame_accepted", data=dict(session=99, frame_seq=99)))
        self.fails(records)
        records = fixture()
        records.append(
            dict(
                kind="system",
                data=dict(
                    event=dict(BookInvalidated=dict(instrument=1, reason="failure"))
                ),
            )
        )
        self.fails(records)
        self.assertFalse(audit_records(fixture(), [6])["checks_passed"])

    def test_decimal_exponent_is_bounded_before_integer_expansion(self):
        for value in ["1e10000000", "1e-10000000", "1" * 1000]:
            with self.assertRaises(AssertionError):
                parts(value)

    def test_malformed_native_decimal_tokens_cannot_match_unchanged_canonical_rows(
        self,
    ):
        for token in [
            "100_.00",
            " 100.00 ",
            "１００.００",
            "NaN",
            "Infinity",
            ".",
            "1e",
        ]:
            with self.subTest(token=token):
                records = fixture()
                mutate_native(
                    records,
                    1,
                    lambda value: value["data"]["b"][0].__setitem__(0, token),
                )
                self.fails(records)

    def test_fixed_scientific_domain_preserves_exact_parts_and_checks_mantissa_first(
        self,
    ):
        for token, expected in [
            ("+.5", (5, 1)),
            ("1.", (1, 0)),
            ("1.00e-2", (100, 4)),
            ("0.00", (0, 2)),
            ("0.00e-2", (0, 0)),
            ("1.25E+2", (125, 0)),
            (str(-(1 << 127)), (-(1 << 127), 0)),
        ]:
            with self.subTest(token=token):
                self.assertEqual(parts(token), expected)
        for token in [str(1 << 127) + "e-1", "0e4294967296", "0." + "0" * 256]:
            with self.subTest(token=token), self.assertRaises(AssertionError):
                parts(token)

    def test_snapshot_depth_sort_duplicate_and_zero_size_fail_including_u1_reset(self):
        for reset in (False, True):
            records = fixture()

            def extra_level(value):
                value["data"]["b"].append(["99.00", "1.000"])
                if reset:
                    value["type"] = "delta"
                    value["data"]["u"] = 1

            mutate_native(records, 4, extra_level)
            self.fails(records)
        for rows in [
            [["99.00", "1.000"], ["100.00", "1.000"]],
            [["100.00", "1.000"], ["100.0", "1.000"]],
            [["100.00", "0"]],
        ]:
            records = fixture()
            mutate_native(
                records, 1, lambda value: value["data"].__setitem__("b", rows)
            )
            self.fails(records)

    def test_same_id_different_source_channel_is_not_an_exact_duplicate(self):
        records = fixture()
        mutate_native(
            records,
            3,
            lambda value: value.__setitem__("topic", "orderbook.050.BTCUSDT"),
        )
        self.fails(records)


if __name__ == "__main__":
    unittest.main()
