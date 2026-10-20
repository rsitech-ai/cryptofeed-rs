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
import re
from pathlib import Path
import struct
from audit_trade_capture import crc32c
getcontext().prec = 1000
if not __debug__:
    raise RuntimeError("audit requires Python assertions enabled")


def integer(value, bits=64, signed=False):
    minimum = -(1 << (bits-1)) if signed else 0
    maximum = (1 << (bits-1 if signed else bits)) - 1
    assert type(value) is int and minimum <= value <= maximum, 'invalid exact integer'
    return value


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            assert key not in result, 'duplicate JSON key'
            result[key] = value
        return result
    def constant(value):
        raise ValueError('nonfinite JSON: ' + value)
    return json.loads(raw, parse_float=Decimal, parse_constant=constant, object_pairs_hook=pairs)


def committed_cursor(directory):
    with (Path(directory) / 'committed.json').open('rb') as source:
        raw = source.read(4097)
    assert len(raw) <= 4096, 'watermark byte cap'
    watermark = strict_json(raw)
    assert integer(watermark['schema_version'], 16) == 1
    cursor = watermark['cursor']
    assert type(cursor['feed_id']) is str and re.fullmatch('[0-9a-f]{40}', cursor['feed_id'])
    integer(cursor['sequence'])
    assert 48 <= integer(cursor['offset']) <= 128 * 1024 * 1024, 'audit 128 MiB cap'
    return cursor


def committed_identity(directory, cursor):
    assert committed_cursor(directory) == cursor, 'committed cursor changed during audit'
    digest = hashlib.sha256()
    with (Path(directory) / 'events.mfsf').open('rb') as source:
        remaining = cursor['offset']
        while remaining:
            chunk = source.read(min(65536, remaining))
            assert chunk, 'truncated audited prefix'
            digest.update(chunk)
            remaining -= len(chunk)
    assert committed_cursor(directory) == cursor, 'committed cursor changed during hash'
    return {'cursor': cursor, 'committed_prefix_sha256': digest.hexdigest()}


def scan(directory, expected_cursor=None):
    root = Path(directory)
    commit = committed_cursor(root)
    assert expected_cursor is None or commit == expected_cursor, 'committed cursor changed before scan'
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
            row = strict_json(payload)
            sequence += 1
            assert integer(row['sequence']) == sequence
            yield row['record']
        assert stream.tell() == commit['offset'] and sequence == commit['sequence']


def fixed(value):
    return Decimal(integer(value['coefficient'], 128, signed=True)).scaleb(-integer(value['scale'], 8))


def audit(directory):
    counts, channels, trades, errors = Counter(), Counter(), Counter(), []
    metadata, originals, accepted, native, normalized = {}, {}, set(), {}, {}
    initial_seen, initial_rows, flagged_initial_rows = set(), 0, 0
    for record in scan(directory):
        kind, data = record['kind'], record['data']
        counts[kind] += 1
        if kind == 'metadata' and data['kind'] == 'session':
            assert data['session_id'] not in metadata, 'redefined session'
            metadata[data['session_id']] = data
        elif kind == 'connection' and data['connected']:
            initial_seen = {key for key in initial_seen if key[0] != data['session']}
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
                initial_coins = {t['coin'] for t in frame['data'] if (key[0], t['coin']) not in initial_seen}
                initial_rows += sum(t['coin'] in initial_coins for t in frame['data'])
                initial_seen.update((key[0], t['coin']) for t in frame['data'])
                for index, trade in enumerate(frame['data']):
                    identity = f"{trade['coin']}:{trade['time']}:{trade['tid']}"
                    native[key + (index,)] = (trade['coin'], identity, Decimal(trade['px']), Decimal(trade['sz']),
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
                    catalog = {i['instrument_id']: i['native_symbol'] for i in session['catalog']} if session else {}
                    coin = catalog.get(event['instrument'])
                    if coin is None:
                        errors.append('trade instrument absent from session catalog')
                    flagged_initial_rows += bool(event['flags'] & (1 << 9))
                    normalized[identity] = (coin, trade['trade_id'], fixed(trade['price']), fixed(trade['quantity']),
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
              'normalized_trade_rows': len(normalized), 'initial_trade_delivery_rows': initial_rows,
              'canonical_flagged_initial_rows': flagged_initial_rows,
              'other_trade_delivery_rows': len(native) - initial_rows, 'trade_difference_count': len(differences),
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
