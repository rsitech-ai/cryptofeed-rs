#!/usr/bin/env python3
"""Independent MFSF1 framing, identity and Hyperliquid native trade audit.

Uses original JSON decimal tokens as the oracle. Not a continuity proof or a
strict dataset admission decision. Reads only the published committed prefix.
"""
import argparse
import base64
from collections import Counter
from decimal import Decimal, getcontext
import hashlib
import json
from pathlib import Path
import struct
from audit_trade_capture import crc32c
getcontext().prec = 1000


def scan(directory):
    root = Path(directory)
    watermark = json.loads((root / 'committed.json').read_bytes())
    assert watermark['schema_version'] == 1
    commit = watermark['cursor']
    assert 48 <= commit['offset'] <= 128 * 1024 * 1024, 'audit 128 MiB cap'
    with (root / 'events.mfsf').open('rb') as stream:
        header = stream.read(48)
        assert header[:8] == b'MFSF1\0\0\0' and header[8:].decode() == commit['feed_id']
        sequence = 0
        while stream.tell() < commit['offset']:
            frame = stream.read(8)
            assert len(frame) == 8
            size, crc = struct.unpack('<II', frame)
            assert size <= 32 * 1024 * 1024 and stream.tell() + size <= commit['offset']
            payload = stream.read(size)
            assert crc32c(payload) == crc
            row = json.loads(payload)
            sequence += 1
            assert row['sequence'] == sequence
            yield row['record']
        assert stream.tell() == commit['offset'] and sequence == commit['sequence']


def fixed(value):
    return Decimal(value['coefficient']).scaleb(-value['scale'])


def audit(directory):
    counts, channels, trades, errors = Counter(), Counter(), Counter(), []
    metadata, originals, accepted, native, normalized = {}, {}, set(), {}, {}
    for record in scan(directory):
        kind, data = record['kind'], record['data']
        counts[kind] += 1
        if kind == 'metadata' and data['kind'] == 'session':
            assert data['session_id'] not in metadata, 'redefined session'
            metadata[data['session_id']] = data
        elif kind == 'frame' and data['direction'] == 0 and data['opcode'] == 1:
            key = data['session'], data['frame_seq']
            assert key not in originals, 'duplicate original frame ordinal'
            originals[key] = data
        elif kind == 'frame_accepted':
            key = data['session'], data['frame_seq']
            assert key in originals and key not in accepted
            accepted.add(key)
            frame = json.loads(base64.b64decode(originals[key]['payload_base64']), parse_float=Decimal)
            channels[frame['channel']] += 1
            if frame['channel'] == 'trades':
                for index, trade in enumerate(frame['data']):
                    identity = f"{trade['coin']}:{trade['time']}:{trade['tid']}"
                    native[key + (index,)] = (identity, Decimal(trade['px']), Decimal(trade['sz']),
                                              {'B': 'Buy', 'A': 'Sell'}[trade['side']], trade['time'] * 1000000)
        elif kind == 'batch':
            for event in data['events']:
                key = event['session'], event['frame_seq']
                if event['session'] != data['session'] or event['frame_seq'] != data['frame_seq']:
                    errors.append('batch/envelope context mismatch')
                session = metadata.get(event['session'])
                if not session or session['venue_id'] != event['venue']:
                    errors.append('runtime session/venue mismatch')
                if 'Trade' in event['payload']:
                    trade = event['payload']['Trade']
                    identity = key + (event['event_index'],)
                    assert identity not in normalized
                    normalized[identity] = (trade['trade_id'], fixed(trade['price']), fixed(trade['quantity']),
                                            trade['aggressor'], event['exchange_ts'])
                    trades[str(event['venue'])] += 1
                    original = originals.get(key)
                    if not original or event['receive_ts'] != original['receive_ts_ns']:
                        errors.append('normalized/original frame or receipt mismatch')
        elif kind == 'system' and any(k in data['event'] for k in ('ParseError','EventsDropped','SequenceGap','UnknownMessage')):
            errors.append(f"source system failure: {data['event']}")
    missing_acceptance = set(originals) - accepted
    if missing_acceptance:
        errors.append(f"{len(missing_acceptance)} original text frames lack acceptance")
    differences = [str(key) for key in native.keys() | normalized.keys() if native.get(key) != normalized.get(key)]
    result = {'schema_version': 1, 'records': dict(counts), 'accepted_channels': dict(channels),
              'session_venues': {str(k): v['venue_id'] for k, v in metadata.items()},
              'normalized_trades_by_venue': dict(trades), 'native_trade_rows': len(native), 'unaccepted_text_frames': len(missing_acceptance),
              'normalized_trade_rows': len(normalized), 'trade_difference_count': len(differences),
              'trade_difference_examples': differences[:10], 'errors': errors,
              'checks_passed': not differences and not errors,
              'continuity': 'unverified; finite capture and recent overlap cannot prove lossless exchange coverage',
              'sha256': hashlib.sha256((Path(directory) / 'events.mfsf').read_bytes()).hexdigest()}
    return result


def main():
    parser = argparse.ArgumentParser()
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
