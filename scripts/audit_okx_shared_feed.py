#!/usr/bin/env python3
"""Independent exact native OKX trades/candles and ACK audit of a saved MFSF1.

The framing reader enforces its 128 MiB bound. This is not a source-continuity
proof, an execution-count reconstruction, or dataset admission.
"""
import argparse
import base64
from collections import Counter
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from audit_shared_feed import scan, fixed

if not __debug__:
    raise RuntimeError("audit requires Python assertions enabled")


def scope(arg):
    return arg.get('channel'), arg.get('instId'), arg.get('instType')


def audit(directory):
    originals, metadata, native, canonical = {}, {}, {}, {}
    accepted, expected, acknowledged = set(), {}, {}
    channels, counts, errors = Counter(), Counter(), []
    for record in scan(directory):
        kind, data = record['kind'], record['data']
        counts[kind] += 1
        if kind == 'metadata' and data['kind'] == 'session':
            assert data['session_id'] not in metadata
            metadata[data['session_id']] = data
        elif kind == 'connection' and data['connected']:
            session = data['session']
            pending = expected.get(session, set()) - acknowledged.get(session, set())
            if pending:
                errors.append({'incomplete_ack_generation': {'session': session, 'pending': sorted(map(str, pending))}})
            expected[session] = set()
            acknowledged[data['session']] = set()
        elif kind == 'frame' and data['opcode'] == 1:
            key = data['session'], data['frame_seq']
            if data['direction'] != 0:
                payload = base64.b64decode(data['payload_base64'])
                if payload == b'ping':
                    continue
                outgoing = json.loads(payload)
                if outgoing.get('op') == 'subscribe':
                    expected.setdefault(data['session'], set()).update(map(scope, outgoing['args']))
                continue
            assert key not in originals
            originals[key] = data
        elif kind == 'frame_accepted':
            key = data['session'], data['frame_seq']
            assert key in originals and key not in accepted
            accepted.add(key)
            payload = base64.b64decode(originals[key]['payload_base64'])
            if payload in (b'ping', b'pong'):
                continue
            message = json.loads(payload, parse_float=Decimal)
            if message.get('event') == 'subscribe':
                arg = scope(message['arg'])
                assert arg in expected[data['session']], 'unexpected ACK scope'
                acknowledged[data['session']].add(arg)
            channel = message.get('arg', {}).get('channel')
            for index, row in enumerate(message.get('data', [])):
                venue = metadata[data['session']]['venue_id']
                channels[f'{venue}:{channel}'] += 1
                identity = key + (index,)
                if channel in ('trades', 'trades-all'):
                    native[identity] = ('Trade', row['instId'], str(row['tradeId']),
                        Decimal(row['px']), Decimal(row['sz']),
                        {'buy': 'Buy', 'sell': 'Sell'}[row['side']], int(row['ts']) * 1_000_000,
                        channel, 'NativeAggregate' if channel == 'trades' else 'IndividualExecution',
                        int(row['count']) if 'count' in row else None,
                        {k: str(row[k]) for k in ('count', 'seqId', 'source') if k in row})
                elif channel and channel.startswith('candle'):
                    assert len(row) in (8, 9)
                    native[identity] = ('Candle', message['arg']['instId'],
                        *(Decimal(row[i]) for i in range(1, 7)),
                        Decimal(row[7]) if len(row) == 9 else None,
                        row[-1] == '1', int(row[0]) * 1_000_000,
                        {'candle1m': 60_000_000_000, 'candle5m': 300_000_000_000,
                         'candle15m': 900_000_000_000, 'candle1H': 3_600_000_000_000,
                         'candle1D': 86_400_000_000_000}[channel], int(row[0]) * 1_000_000, None)
        elif kind == 'batch':
            for event in data['events']:
                if not any(kind in event['payload'] for kind in ('Trade', 'Candle')):
                    continue
                key = event['session'], event['frame_seq']
                meta = metadata[event['session']]
                assert meta['venue_id'] == event['venue']
                assert event['session'] == data['session'] and event['frame_seq'] == data['frame_seq']
                original = originals[key]
                assert event['receive_ts'] == original['receive_ts_ns']
                symbol = next(i['native_symbol'] for i in meta['catalog'] if i['instrument_id'] == event['instrument'])
                identity = key + (event['event_index'],)
                assert identity not in canonical, 'duplicate canonical event ordinal'
                if 'Trade' in event['payload']:
                    row = event['payload']['Trade']
                    source = row['provenance']
                    canonical[identity] = ('Trade', symbol, row['trade_id'], fixed(row['price']),
                        fixed(row['quantity']), row['aggressor'], event['exchange_ts'],
                        source['source_channel'], source['granularity'], source['native_count'], source['native_attributes'])
                    seq = event['source_sequence']
                    expected_seq = source['native_attributes'].get('seqId')
                    assert seq == ({'first': int(expected_seq), 'last': int(expected_seq)} if expected_seq is not None else None)
                    assert source['constituent_first_id'] is None and source['constituent_last_id'] is None
                elif 'Candle' in event['payload']:
                    row = event['payload']['Candle']
                    canonical[identity] = ('Candle', symbol,
                        *(fixed(row[k]) for k in ('open', 'high', 'low', 'close', 'volume', 'native_volume_ccy')),
                        fixed(row['quote_volume']) if row.get('quote_volume') is not None else None,
                        row['is_closed'], row['start_ts'], row['interval_ns'], event['exchange_ts'], event['source_sequence'])
        elif kind == 'system' and any(k in data['event'] for k in ('ParseError', 'EventsDropped', 'SequenceGap', 'UnknownMessage')):
            errors.append(data['event'])
    missing_acceptance = set(originals) - accepted
    differences = [str(key) for key in native.keys() | canonical.keys() if native.get(key) != canonical.get(key)]
    pending = {str(session): sorted(map(str, args - acknowledged.get(session, set()))) for session, args in expected.items()}
    assert expected and all(expected.values()), 'missing native subscription evidence'
    return {'schema_version': 1, 'records': dict(counts), 'session_venues': {str(k): v['venue_id'] for k, v in metadata.items()},
        'native_rows_by_venue_channel': dict(channels), 'native_trade_rows': sum(row[0] == 'Trade' for row in native.values()),
        'native_candle_rows': sum(row[0] == 'Candle' for row in native.values()),
        'unaccepted_text_frames': len(missing_acceptance), 'difference_count': len(differences), 'difference_examples': differences[:10],
        'pending_acks_by_session': pending, 'errors': errors,
        'checks_passed': bool(native) and not missing_acceptance and not differences and not errors and not any(pending.values()),
        'continuity': 'unverified; exact observed rows and ACKs do not prove complete source delivery',
        'sha256': hashlib.sha256((Path(directory) / 'events.mfsf').read_bytes()).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    result = audit(args.directory)
    text = json.dumps(result, indent=2) + '\n'
    if args.output:
        args.output.write_text(text)
    print(text)
    raise SystemExit(0 if result['checks_passed'] else 1)


if __name__ == '__main__':
    main()
