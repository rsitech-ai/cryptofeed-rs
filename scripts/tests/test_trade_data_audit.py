"""Evidence-tool boundary tests, independent of exchange decoder expectations."""
import importlib.util
from pathlib import Path
import struct
import tempfile
import unittest
from urllib.parse import urlsplit

spec = importlib.util.spec_from_file_location("audit", Path(__file__).parents[1] / "audit_trade_capture.py")
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)
pilot_spec = importlib.util.spec_from_file_location("pilot", Path(__file__).parents[1] / "trade_data_pilot.py")
pilot = importlib.util.module_from_spec(pilot_spec)
pilot_spec.loader.exec_module(pilot)


class TradeAuditTests(unittest.TestCase):
    def test_crc32c_standard_check_vector(self):
        self.assertEqual(audit.crc32c(b"123456789"), 0xE3069283)

    def test_raw_record_rejects_partial_and_corrupt_payloads(self):
        payload = b'{"data":[]}'
        data = b"MFR1" + struct.pack("<HqQ", 3, 1, 0) + struct.pack(
            "<IQQqQBBBII", 47 + len(payload), 2, 3, 4, 5, 0, 1, 0,
            len(payload), audit.crc32c(payload)) + payload
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.mfr1"
            path.write_bytes(data)
            self.assertEqual(list(audit.records(path))[0]["payload"], payload)
            path.write_bytes(data[:-1])
            with self.assertRaises(ValueError):
                list(audit.records(path))
            path.write_bytes(data[:-1] + b"!")
            with self.assertRaises(ValueError):
                list(audit.records(path))

    def test_signed_i128_limbs_are_exact_beyond_float_and_decimal_defaults(self):
        for coefficient in [-1, -(1 << 127), (1 << 127) - 1, 9007199254740993]:
            low = coefficient & ((1 << 64) - 1)
            if low >= 1 << 63:
                low -= 1 << 64
            self.assertEqual(audit.fixed(dict(coefficient_hi=coefficient >> 64,
                coefficient_lo=low, scale=0)), coefficient)

    def test_native_batch_count_and_repeated_values_preserve_ids(self):
        row = dict(instId="BTC-USDT", tradeId="1", px="1.00", sz="0.1")
        rows = audit.native_rows(4, dict(arg=dict(channel="trades"),
            data=[row, dict(row, tradeId="9007199254740993")]), {})
        self.assertEqual(len(rows), 2)
        self.assertNotEqual(rows[0][1], rows[1][1])

    def test_kraken_subscription_ack_is_control(self):
        self.assertEqual(audit.native_rows(13, dict(event="subscribed", feed="trade"), {}), [])

    def test_two_venues_instrument_one_do_not_use_wrong_session_catalog(self):
        metadata = {1: dict(venue_id=17, catalog=[dict(instrument_id=1, native_symbol="tBTCUSD")]),
                    2: dict(venue_id=4, catalog=[dict(instrument_id=1, native_symbol="BTC-USDT")])}
        event = dict(venue_id=4, session_id=1, instrument_id=1)
        self.assertEqual(audit.symbol_for_event(event, metadata), ("BTC-USDT", True))
        metadata[3] = dict(venue_id=4, catalog=[dict(instrument_id=1, native_symbol="ETH-USDT")])
        with self.assertRaises(ValueError):
            audit.symbol_for_event(event, metadata)

    def test_public_business_duplicate_catalog_is_unambiguous(self):
        session = dict(venue_id=4, catalog=[dict(instrument_id=1, native_symbol="BTC-USDT")])
        self.assertEqual(audit.symbol_for_event(dict(venue_id=4, session_id=1, instrument_id=1),
            {1: session, 2: session}), ("BTC-USDT", False))

    def test_public_capture_rejects_escape_and_network_sinks(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory).resolve() / "fresh"
            config = dict(recording=dict(raw=dict(enabled=True, directory=str(output / "raw"))),
                          sinks=[dict(type="file", path=str(output / "events.jsonl"))])
            pilot.validate_public_paths(config, output)
            config["sinks"][0]["path"] = str(Path(directory) / "old.jsonl")
            with self.assertRaises(ValueError):
                pilot.validate_public_paths(config, output)
            config["sinks"][0]["type"] = "udp"
            with self.assertRaises(ValueError):
                pilot.validate_public_paths(config, output)

    def test_metrics_identity_must_match_pilot(self):
        config = dict(telemetry=dict(bind="127.0.0.1:19348"))
        pilot.validate_metrics(config, urlsplit("http://127.0.0.1:19348/metrics"))
        with self.assertRaises(ValueError):
            pilot.validate_metrics(config, urlsplit("http://127.0.0.1:9108/metrics"))


if __name__ == "__main__":
    unittest.main()
