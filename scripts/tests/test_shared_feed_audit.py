"""Independent oracle must reject a correct price mapped to the wrong coin."""
import base64
import copy
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
spec = importlib.util.spec_from_file_location('shared_audit', Path(__file__).parents[1] / 'audit_shared_feed.py')
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)

class SharedAuditTest(unittest.TestCase):
    def test_instrument_transposition_cannot_pass_exact_reconciliation(self):
        native = dict(channel='trades', data=[dict(coin='BTC', time=1, tid=7, px='10.25', sz='2', side='B')])
        event = dict(session=3, frame_seq=4, venue=22, instrument=1, event_index=0, receive_ts=2,
            exchange_ts=1000000, flags=0, payload=dict(Trade=dict(trade_id='BTC:1:7',
            price=dict(coefficient=1025, scale=2), quantity=dict(coefficient=2, scale=0), aggressor='Buy')))
        records = [dict(kind='metadata', data=dict(kind='session', session_id=3, venue_id=22,
                catalog=[dict(instrument_id=1, native_symbol='BTC'), dict(instrument_id=2, native_symbol='ETH')])),
            dict(kind='frame', data=dict(session=3, frame_seq=4, direction=0, opcode=1, receive_ts_ns=2,
                payload_base64=base64.b64encode(json.dumps(native).encode()).decode())),
            dict(kind='batch', data=dict(session=3, frame_seq=4, events=[event])),
            dict(kind='frame_accepted', data=dict(session=3, frame_seq=4))]
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / 'events.mfsf').write_bytes(b'framing tested separately')
            with patch.object(audit, 'scan', return_value=copy.deepcopy(records)):
                result = audit.audit(directory)
                self.assertTrue(result['checks_passed'])
                self.assertEqual(result['initial_trade_delivery_rows'], 1)
            records[2]['data']['events'][0]['instrument'] = 2
            with patch.object(audit, 'scan', return_value=records):
                result = audit.audit(directory)
                self.assertFalse(result['checks_passed'])
                self.assertEqual(result['trade_difference_count'], 1)

if __name__ == '__main__':
    unittest.main()
