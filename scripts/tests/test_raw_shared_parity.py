"""Real bounded binary framing with independent metadata/frame mutations."""

import base64
import copy
import json
from pathlib import Path
import struct
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from audit_raw_shared_frames import audit
from audit_trade_capture import crc32c


def fixture(root, changed=None):
    raw = root / "raw"
    feed = root / "feed"
    raw.mkdir()
    feed.mkdir()
    metadata = dict(kind="session", session_id=2, venue_id=5)
    payload = b'{"topic":"publicTrade.BTCUSDT","data":[]}'
    raw_bytes = b"MFR1" + struct.pack("<HqQ", 3, 1, 0)
    for session, seq, ts, mono, direction, opcode, data in [
        (0, 0, 1, 0, 0, 7, json.dumps(metadata).encode()),
        (2, 3, 4, 5, 0, 1, payload),
        (2, 3, 4, 5, 0, 1, payload),
    ]:
        raw_bytes += (
            struct.pack(
                "<IQQqQBBBII",
                47 + len(data),
                session,
                seq,
                ts,
                mono,
                direction,
                opcode,
                0,
                len(data),
                crc32c(data),
            )
            + data
        )
    (raw / "test.mfr1").write_bytes(raw_bytes)
    frame = dict(
        kind="frame",
        data=dict(
            session=2,
            frame_seq=3,
            receive_ts_ns=4,
            monotonic_ns=5,
            direction=0,
            opcode=1,
            payload_base64=base64.b64encode(payload).decode(),
        ),
    )
    rows = [dict(kind="metadata", data=metadata), frame, copy.deepcopy(frame)]
    if changed:
        changed(rows)
    rows.append(
        dict(kind="seal", data=dict(schema_version=1, now_ns=7, reason="test_closed_capture"))
    )
    feed_id = "a" * 40
    data = b"MFSF1\0\0\0" + feed_id.encode()
    for seq, row in enumerate(rows, 1):
        encoded = json.dumps(dict(sequence=seq, record=row)).encode()
        data += struct.pack("<II", len(encoded), crc32c(encoded)) + encoded
    (feed / "events.mfsf").write_bytes(data)
    (feed / "committed.json").write_text(
        json.dumps(
            dict(
                schema_version=1, cursor=dict(feed_id=feed_id, sequence=len(rows), offset=len(data))
            )
        )
    )
    return raw, feed


class RawSharedParityTest(unittest.TestCase):
    def test_exact_duplicate_multiplicity_and_metadata_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            result = audit(*fixture(Path(directory)))
            self.assertTrue(result["checks_passed"])
            self.assertEqual(result["raw_application_frames"], 2)

    def test_changed_payload_scope_time_metadata_and_missing_duplicate_fail(self):
        for key in (
            "payload_base64",
            "session",
            "frame_seq",
            "receive_ts_ns",
            "monotonic_ns",
            "direction",
            "opcode",
            "metadata",
            "missing",
        ):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as directory:

                def mutate(rows):
                    if key == "missing":
                        rows.pop()
                    elif key == "metadata":
                        rows[0]["data"]["venue_id"] = 6
                    elif key == "payload_base64":
                        rows[1]["data"][key] = "AA=="
                    else:
                        rows[1]["data"][key] += 1 if key not in ("direction",) else 1

                result = audit(*fixture(Path(directory), mutate))
                self.assertFalse(result["checks_passed"])

    def test_boolean_and_float_coordinates_are_not_equal_integer_evidence(self):
        for key, value in [
            ("session", 2.0),
            ("frame_seq", 3.0),
            ("receive_ts_ns", 4.0),
            ("monotonic_ns", 5.0),
            ("direction", False),
            ("opcode", True),
        ]:
            with self.subTest(key=key), tempfile.TemporaryDirectory() as directory:
                raw, feed = fixture(
                    Path(directory), lambda rows: rows[1]["data"].__setitem__(key, value)
                )
                with self.assertRaises(AssertionError):
                    audit(raw, feed)

    def test_review_truncated_headers_and_version_opcode_mismatch_are_rejected(self):
        from audit_trade_capture import records_bytes

        with self.assertRaisesRegex(ValueError, "header"):
            list(records_bytes(b"MFR1" + struct.pack("<H", 3)))
        with tempfile.TemporaryDirectory() as directory:
            raw, feed = fixture(Path(directory))
            path = raw / "test.mfr1"
            data = bytearray(path.read_bytes())
            data[4:6] = struct.pack("<H", 1)
            path.write_bytes(data)
            with self.assertRaisesRegex(ValueError, "version-specific"):
                audit(raw, feed)

    def test_empty_frame_evidence_cannot_pass_even_with_valid_metadata_and_seal(self):
        with tempfile.TemporaryDirectory() as directory:
            raw, feed = fixture(Path(directory), lambda rows: rows.__setitem__(slice(1, None), []))
            path = raw / "test.mfr1"
            data = path.read_bytes()
            size = struct.unpack_from("<I", data, 22)[0]
            path.write_bytes(data[: 22 + size])
            result = audit(raw, feed)
            self.assertFalse(result["checks_passed"])
            self.assertTrue(result["shared_final_seal"])


if __name__ == "__main__":
    unittest.main()
