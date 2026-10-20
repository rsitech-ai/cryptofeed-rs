#!/usr/bin/env python3
"""Read-only native-byte/order/output audit. No adapter/engine normalizer used.

This reconciles a synthetic diagnostic prefix, never exchange completeness or
dataset admission. Every unexpected failure remains a failed workload.
"""
import base64
from collections import Counter, defaultdict
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / 'scripts'))
from audit_shared_feed import committed_cursor, committed_identity, integer, scan
from audit_trade_capture import records


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fixed(value):
    return Decimal(integer(value['coefficient'], 128, signed=True)).scaleb(-integer(value['scale'], 8))


def expected_events(value):
    topic = value['topic']
    if topic.startswith('orderbook.1.'):
        return ['Quote']
    if topic.startswith('orderbook.50.'):
        return ['BookSnapshot']
    assert topic.startswith('tickers.')
    data = value['data']
    result = [kind for key, kind in (
        ('markPrice', 'MarkPrice'), ('indexPrice', 'IndexPrice'),
        ('fundingRate', 'Funding'), ('openInterest', 'OpenInterest'),
    ) if data.get(key) not in (None, '')]
    if any(data.get(key) not in (None, '') for key in (
        'prevPrice24h', 'highPrice24h', 'lowPrice24h', 'lastPrice',
        'volume24h', 'turnover24h',
    )):
        result.append('Statistics24h')
    assert result, 'empty fixture ticker is not a meaningful stress input'
    return result


def check_values(value, events):
    assert [next(iter(e['payload'])) for e in events] == expected_events(value)
    for event in events:
        assert event['exchange_ts'] == value['ts'] * 1_000_000
        kind, body = next(iter(event['payload'].items()))
        native = value['data']
        if kind == 'Quote':
            bid = max(native['b'], key=lambda row: Decimal(row[0]))
            ask = min(native['a'], key=lambda row: Decimal(row[0]))
            for key, expected in (('bid_price', bid[0]), ('bid_quantity', bid[1]),
                                  ('ask_price', ask[0]), ('ask_quantity', ask[1])):
                assert fixed(body[key]) == Decimal(expected)
        elif kind == 'BookSnapshot':
            assert body['depth'] == 50
            for side, field, reverse in (('b', 'bids', True), ('a', 'asks', False)):
                levels = sorted(((Decimal(p), Decimal(q)) for p, q in native[side]
                                 if Decimal(q) > 0), reverse=reverse)[:50]
                actual = [(fixed(row['price']), fixed(row['quantity'])) for row in body[field]]
                assert actual == levels
        elif kind in ('MarkPrice', 'IndexPrice'):
            key = 'markPrice' if kind == 'MarkPrice' else 'indexPrice'
            assert fixed(body['price']) == Decimal(native[key])
        elif kind == 'Funding':
            assert fixed(body['rate']) == Decimal(native['fundingRate'])
            expected = int(native['nextFundingTime']) * 1_000_000 if native.get('nextFundingTime') else None
            assert body['next_funding_ts'] == expected
        elif kind == 'OpenInterest':
            assert fixed(body['quantity']) == Decimal(native['openInterest'])
        elif kind == 'Statistics24h':
            for target, key in (('open', 'prevPrice24h'), ('high', 'highPrice24h'),
                                ('low', 'lowPrice24h'), ('close', 'lastPrice'),
                                ('volume', 'volume24h'), ('quote_volume', 'turnover24h')):
                if native.get(key) in (None, ''):
                    assert body[target] is None
                else:
                    assert fixed(body[target]) == Decimal(native[key])


def envelope_numbers(event):
    # bool is an int subclass in Python. Exact scalar types matter even when
    # equality against 0/1 would otherwise pass a malformed canonical envelope.
    for key, bits in (('schema_version', 16), ('venue', 16), ('connection', 64),
                      ('session', 64), ('frame_seq', 64), ('event_index', 16), ('flags', 32)):
        integer(event[key], bits)
    if event['instrument'] is not None:
        integer(event['instrument'], 32)
    integer(event['receive_ts'], signed=True)
    if event['exchange_ts'] is not None:
        integer(event['exchange_ts'], signed=True)


def signature(row, shared=False):
    return (row['session'], row['frame_seq'] if shared else row['seq'],
            row['receive_ts_ns'] if shared else row['receipt_ns'], row['monotonic_ns'],
            row['direction'], row['opcode'],
            sha_bytes(base64.b64decode(row['payload_base64'], validate=True) if shared else row['payload']))


def sha_bytes(data):
    return hashlib.sha256(data).hexdigest()


def audit_arm(root, arm, fixture, plan):
    report_path = root / (arm + '.stdout.json')
    receipt = json.loads((root / (arm + '.receipt.json')).read_text())
    assert sha(report_path) == receipt['stdout_sha256']
    assert not receipt['watchdog_fired']
    report = json.loads(report_path.read_text())
    assert report['qualified_dataset'] is False and report['independent_storage_audit_required'] is True
    assert report['verdict_scope'] == 'local_harness_only'
    for field in ('producers', 'consumers', 'queue_reservations', 'input_disposition'):
        assert len(report[field]) == 3
    for queue in report['queue_reservations']:
        for value in queue.values():
            integer(value)
        assert queue['frames'] == queue['bytes'] == 0
        assert 0 <= queue['peak_frames'] <= plan['queue_frames_per_session']
        assert 0 <= queue['peak_bytes'] <= plan['queue_bytes_per_session']
    data_root = root / arm
    original_hashes = {str(p.relative_to(data_root)): sha(p) for p in data_root.rglob('*') if p.is_file()}
    raw = []
    metadata = []
    for path in sorted((data_root / 'raw').glob('*.mfr1')):
        for row in records(path):
            if row['opcode'] == 7:
                metadata.append(json.loads(row['payload']))
            else:
                raw.append(row)
    assert Counter(m['kind'] for m in metadata) == {'build': 1, 'session': 3}
    assert len(raw) == report['raw']['records_written']
    assert report['raw']['queue_len'] == 0 and report['raw']['dropped_total'] == 0
    inbound = defaultdict(list)
    for row in raw:
        assert row['opcode'] == 1
        if row['direction'] == 0:
            inbound[row['session']].append(row)
    expected_load_events, expected_resets, load_frames = {}, {}, {}
    for slot, session in enumerate(fixture['sessions']):
        sid = session['metadata']['session_id']
        c, p = report['consumers'][slot], report['producers'][slot]
        for field in ('session', 'warmup_frames', 'dequeued', 'callbacks_returned_ok',
                      'callbacks_completed', 'market_rows', 'system_events', 'source_span_ns'):
            integer(c[field])
        for field in ('planned', 'attempted', 'enqueued', 'enqueue_rejected'):
            integer(p[field])
        assert c['session'] == sid and c['warmup_frames'] == 3
        rows = inbound[sid]
        assert len(rows) == c['dequeued'] + c['warmup_frames']
        request_rows = [r for r in raw if r['session'] == sid and r['direction'] == 1]
        assert len(request_rows) == 1
        request = json.loads(request_rows[0]['payload'])
        ack = json.loads(rows[0]['payload'])
        assert request['op'] == ack['op'] == 'subscribe' and ack['success'] is True
        assert request['req_id'] == ack['req_id'] and ack['conn_id'].startswith('synthetic-offline-')
        pool = [base64.b64decode(p, validate=True) for p in session['payloads']]
        assert rows[1]['payload'] == pool[0] and rows[2]['payload'] == pool[1]
        schedule = []
        for index in range(1600):
            which = 0 if index % 16 < 10 else 1 if index % 16 < 15 else 2
            schedule.extend([pool[which]] * (1 + (22 if slot == 0 else 21) if index == 800 else 1))
        load = rows[3:]
        assert [r['payload'] for r in load] == schedule[:c['dequeued']]
        assert c['dequeued'] == c['callbacks_returned_ok'] == c['callbacks_completed']
        assert p['enqueued'] >= c['dequeued'] and p['attempted'] == p['enqueued'] + p['enqueue_rejected']
        assert p['planned'] == len(schedule)
        d = report['input_disposition'][slot]
        assert d == {'session': sid, 'not_attempted': p['planned'] - p['attempted'],
                     'abandoned_enqueued': p['enqueued'] - c['dequeued'],
                     'dequeued_without_validated_callback': 0}
        expected_load_events[sid] = sum(len(expected_events(json.loads(r['payload']))) for r in load)
        expected_resets[sid] = sum(json.loads(r['payload'])['topic'].startswith('orderbook.50.') for r in load)
        assert c['market_rows'] == expected_load_events[sid]
        assert c['system_events'] == expected_resets[sid]
        load_frames[sid] = {r['seq']: r for r in load}
        assert c['read_age']['count'] == c['dequeued']
        if report['passed']:
            assert p['planned'] == p['attempted'] == p['enqueued'] == c['dequeued']
            assert c['source_span_ns'] >= plan['minimum_source_span_ns']
            assert c['read_age']['max_ns'] <= plan['max_schedule_to_read_return_ns']
    shared_result = None
    if arm != 'baseline':
        feed = data_root / 'feed'
        cursor = committed_cursor(feed)
        identity = committed_identity(feed, cursor)
        wire, shared_frames, accepted, batches = Counter(), [], set(), defaultdict(list)
        load_resets = Counter()
        connections = {}
        sealed = False
        session_metadata = {s['metadata']['session_id']: s for s in fixture['sessions']}
        for record in scan(feed, cursor):
            assert not sealed, 'record after seal'
            kind, data = record['kind'], record['data']
            wire[kind] += 1
            if kind == 'frame':
                for field in ('session', 'frame_seq', 'monotonic_ns', 'direction', 'opcode'):
                    integer(data[field])
                integer(data['receive_ts_ns'], signed=True)
                shared_frames.append(data)
            elif kind == 'connection':
                integer(data['session'])
                integer(data['connection'])
                assert data['connected'] is True and data['session'] not in connections
                connections[data['session']] = data['connection']
            elif kind == 'frame_accepted':
                integer(data['session'])
                integer(data['frame_seq'])
                key = (data['session'], data['frame_seq'])
                assert key not in accepted
                accepted.add(key)
            elif kind == 'batch':
                integer(data['session'])
                integer(data['frame_seq'])
                session = session_metadata[data['session']]
                instrument = next(m['instrument_id'] for m in session['metadata']['catalog']
                                  if m['native_symbol'] == session['symbol'])
                for event in data['events']:
                    envelope_numbers(event)
                    assert event['session'] == data['session'] and event['frame_seq'] == data['frame_seq']
                    assert event['venue'] == session['metadata']['venue_id']
                    assert event['instrument'] == (None if 'VenueStatus' in event['payload'] else instrument)
                    assert event['schema_version'] == 1 and event['connection'] == connections[data['session']]
                    assert event['source_sequence'] is None
                    kind_name = next(iter(event['payload']))
                    assert event['flags'] == (1 if kind_name in ('Quote', 'BookSnapshot') else 0)
                assert [e['event_index'] for e in data['events']] == list(range(len(data['events'])))
                batches[data['session'], data['frame_seq']].extend(data['events'])
            elif kind == 'system':
                name = next(iter(data['event'])) if isinstance(data['event'], dict) else data['event']
                assert name in ('ConnectionStateChanged', 'SubscriptionStateChanged', 'BookResynchronized')
                if name == 'BookResynchronized':
                    load_resets[data['session']] += 1
            elif kind == 'seal':
                sealed = True
                assert ('complete' if report['passed'] else 'failed') in data['reason']
        assert sealed
        assert set(connections) == set(inbound)
        for sid in inbound:
            assert [signature(r) for r in raw if r['session'] == sid] == [signature(r, True) for r in shared_frames if r['session'] == sid]
            assert load_resets[sid] == expected_resets[sid] + 1  # native L2 warmup
        assert accepted == {(r['session'], r['seq']) for rows in inbound.values() for r in rows}
        assert set(batches) <= accepted | {(sid, 0) for sid in inbound}
        for sid, rows in inbound.items():
            assert [e['payload'] for e in batches[sid, 0]] == [
                {'VenueStatus': {'message': 'degraded'}}, {'VenueStatus': {'message': 'connected'}}]
            assert not batches[sid, rows[0]['seq']], 'ACK unexpectedly emitted market rows'
            for warm_index, frame in enumerate(rows[1:3], 1):
                all_events = batches[sid, frame['seq']]
                events = [e for e in all_events if 'VenueStatus' not in e['payload']]
                check_values(json.loads(frame['payload']), events)
                statuses = [e['payload'] for e in all_events if 'VenueStatus' in e['payload']]
                assert statuses == ([{'VenueStatus': {'message': 'live'}}] if warm_index == 2 else [])
        for sid, frames in load_frames.items():
            for seq, frame in frames.items():
                events = batches[sid, seq]
                assert [e['event_index'] for e in events] == list(range(len(events)))
                assert all(e['receive_ts'] == frame['receipt_ns'] for e in events)
                check_values(json.loads(frame['payload']), events)
            assert sum(len(batches[sid, seq]) for seq in frames) == expected_load_events[sid]
        assert report['writer_timings_after']['accepted_records'] == cursor['sequence']
        before, after = report['writer_timings_before'], report['writer_timings_after']
        load_count = sum(len(rows) for rows in load_frames.values())
        for snapshot in (before, after):
            for key in ('failed_attempts', 'append_attempts', 'accepted_transactions', 'accepted_records'):
                integer(snapshot[key])
            assert snapshot['failed_attempts'] == 0
            assert snapshot['append_attempts'] == snapshot['accepted_transactions']
            for key in ('lock_wait', 'append', 'encode', 'write', 'data_sync',
                        'watermark_write_sync', 'replace', 'directory_sync'):
                stage = snapshot[key]
                for field in ('completed_calls', 'errors', 'max_ns', 'total_ns'):
                    integer(stage[field])
                for value in stage['latency_buckets']:
                    integer(value)
                assert stage['completed_calls'] == snapshot['append_attempts']
                assert stage['errors'] == 0
                assert sum(stage['latency_buckets']) == stage['completed_calls']
                assert 0 <= stage['max_ns'] <= stage['total_ns']
        assert after['append_attempts'] - before['append_attempts'] == 2 * load_count + 1
        assert after['accepted_records'] - before['accepted_records'] == 3 * load_count + sum(expected_resets.values()) + 1
        shared_result = {'identity': identity, 'record_counts': dict(wire),
                         'raw_shared_exact_per_session_parity': True,
                         'native_value_and_output_count_checks': True}
    assert original_hashes == {str(p.relative_to(data_root)): sha(p) for p in data_root.rglob('*') if p.is_file()}, 'audit modified input bytes'
    return {'arm': arm, 'workload_passed': report['passed'], 'first_failure': report['first_failure'],
            'raw_frame_count': len(raw), 'load_frames': sum(len(rows) for rows in load_frames.values()),
            'canonical_load_rows': sum(expected_load_events.values()),
            'input_crc_order_payload_and_accounting_audit': 'pass', 'shared': shared_result,
            'files_sha256': original_hashes, 'qualified_dataset': False}


def main():
    root, fixture_path = map(Path, sys.argv[1:])
    plan_path = Path(__file__).with_name('plan.json')
    plan = json.loads(plan_path.read_text())
    execution = json.loads((root / 'execution.json').read_text())
    assert sha(plan_path) == execution['plan_sha256']
    assert sha(fixture_path) == execution['fixture_sha256'] == plan['fixture_sha256']
    fixture = json.loads(fixture_path.read_text())
    receipts = json.loads((root / 'receipts.json').read_text())
    result = {'schema': 'marketfeed.paced_ingress_audit.v1', 'qualified_dataset': False,
              'arms': [audit_arm(root, r['arm'], fixture, plan) for r in receipts],
              'not_run': [a for a in plan['order'] if a not in [r['arm'] for r in receipts]],
              'audit_source_sha256': sha(Path(__file__))}
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
