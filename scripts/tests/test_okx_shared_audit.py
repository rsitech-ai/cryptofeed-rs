"""Challenge the independent OKX oracle with coordinated output mutations."""
import base64
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import audit_okx_shared_feed as oracle


def frame(seq, payload, direction=0):
    return dict(kind='frame', data=dict(session=3, frame_seq=seq, direction=direction,
        opcode=1, receive_ts_ns=2, payload_base64=base64.b64encode(json.dumps(payload).encode()).decode()))


def fixture():
    arg = dict(channel='trades', instId='BTC-USDT')
    event = dict(session=3, frame_seq=2, venue=4, instrument=1, event_index=0,
        receive_ts=2, exchange_ts=1000000, source_sequence=dict(first=7, last=7),
        payload=dict(Trade=dict(trade_id='9007199254740993',
            price=dict(coefficient=1025, scale=2), quantity=dict(coefficient=2, scale=0),
            aggressor='Buy', provenance=dict(source_channel='trades', granularity='NativeAggregate',
                native_count=2, constituent_first_id=None, constituent_last_id=None,
                native_attributes=dict(count='2', seqId='7', source='0')))))
    return [dict(kind='metadata', data=dict(kind='session', session_id=3, venue_id=4,
                catalog=[dict(instrument_id=1, native_symbol='BTC-USDT')])),
        dict(kind='connection', data=dict(session=3, connected=True)),
        frame(0, dict(op='subscribe', args=[arg]), 1),
        frame(1, dict(event='subscribe', arg=arg)),
        dict(kind='frame_accepted', data=dict(session=3, frame_seq=1)),
        frame(2, dict(arg=arg, data=[dict(instId='BTC-USDT', tradeId='9007199254740993',
            px='10.25', sz='2', side='buy', ts='1', count='2', seqId=7, source='0')])),
        dict(kind='batch', data=dict(session=3, frame_seq=2, events=[event])),
        dict(kind='frame_accepted', data=dict(session=3, frame_seq=2))]


def candle_fixture():
    records = fixture()
    arg = dict(channel='candle1m', instId='BTC-USDT')
    records[2] = frame(0, dict(op='subscribe', args=[arg]), 1)
    records[3] = frame(1, dict(event='subscribe', arg=arg))
    records[5] = frame(2, dict(arg=arg, data=[['1', '10', '11', '9', '10', '2', '3', '20', '0']]))
    records[6]['data']['events'][0].update(exchange_ts=1000000, source_sequence=None,
        payload=dict(Candle=dict(open=dict(coefficient=10, scale=0), high=dict(coefficient=11, scale=0),
            low=dict(coefficient=9, scale=0), close=dict(coefficient=10, scale=0), volume=dict(coefficient=2, scale=0),
            native_volume_ccy=dict(coefficient=3, scale=0), quote_volume=dict(coefficient=20, scale=0),
            is_closed=False, start_ts=1000000, interval_ns=60000000000)))
    return records


class OkxAuditTest(unittest.TestCase):
    def inspect(self, records):
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / 'events.mfsf').write_bytes(b'framing tested separately')
            with patch.object(oracle, 'scan', return_value=records):
                return oracle.audit(directory)

    def test_exact_expected_source_row_and_all_acknowledgments(self):
        self.assertTrue(self.inspect(fixture())['checks_passed'])

    def test_changed_count_granularity_side_time_and_price_do_not_pass(self):
        for change in ('count', 'granularity', 'side', 'time', 'price'):
            with self.subTest(change=change):
                records = fixture()
                event = records[6]['data']['events'][0]
                trade = event['payload']['Trade']
                if change == 'count':
                    trade['provenance']['native_count'] = 1
                elif change == 'granularity':
                    trade['provenance']['granularity'] = 'IndividualExecution'
                elif change == 'side':
                    trade['aggressor'] = 'Sell'
                elif change == 'time':
                    event['exchange_ts'] += 1
                else:
                    trade['price']['coefficient'] += 1
                result = self.inspect(records)
                self.assertFalse(result['checks_passed'])
                self.assertEqual(result['difference_count'], 1)

    def test_duplicate_canonical_ordinal_cannot_overwrite_the_oracle(self):
        records = fixture()
        records[6]['data']['events'] *= 2
        with self.assertRaisesRegex(AssertionError, 'duplicate canonical'):
            self.inspect(records)

    def test_lost_row_and_missing_native_ack_fail(self):
        records = fixture()
        records[6]['data']['events'] = []
        self.assertFalse(self.inspect(records)['checks_passed'])
        records = fixture()
        records[2]['data']['payload_base64'] = base64.b64encode(json.dumps(dict(op='subscribe',
            args=[dict(channel='trades', instId='BTC-USDT'), dict(channel='tickers', instId='BTC-USDT')])).encode()).decode()
        self.assertFalse(self.inspect(records)['checks_passed'])

    def test_candle_interval_envelope_time_and_sequence_are_native(self):
        self.assertTrue(self.inspect(candle_fixture())['checks_passed'])
        for change in ('interval', 'time', 'sequence', 'finality', 'volume'):
            with self.subTest(change=change):
                records = candle_fixture()
                event = records[6]['data']['events'][0]
                candle = event['payload']['Candle']
                if change == 'interval':
                    candle['interval_ns'] = 123
                elif change == 'time':
                    event['exchange_ts'] = 999
                elif change == 'sequence':
                    event['source_sequence'] = dict(first=42, last=42)
                elif change == 'finality':
                    candle['is_closed'] = True
                else:
                    candle['native_volume_ccy']['coefficient'] = 2
                self.assertFalse(self.inspect(records)['checks_passed'])

    def test_reconnect_cannot_erase_an_incomplete_ack_generation(self):
        records = fixture()
        # Preserve an emitted native row before the lost subscription ACK.
        records = records[:3] + records[5:]
        second = fixture()[1:]
        for record in second:
            if record['kind'] in ('frame', 'frame_accepted', 'batch'):
                record['data']['frame_seq'] += 10
            if record['kind'] == 'batch':
                record['data']['events'][0]['frame_seq'] += 10
        records += second
        self.assertFalse(self.inspect(records)['checks_passed'])

    def test_native_id_does_not_become_a_sequence(self):
        records = fixture()
        records[6]['data']['events'][0]['source_sequence'] = dict(first=9007199254740993, last=9007199254740993)
        with self.assertRaises(AssertionError):
            self.inspect(copy.deepcopy(records))


if __name__ == '__main__':
    unittest.main()
