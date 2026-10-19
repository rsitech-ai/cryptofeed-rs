#!/usr/bin/env python3
"""Independent PyArrow read/write against the compiled Rust fixture program.

No decimals or floats: reconstruct all signed 128-bit coefficients from their
big-endian two's-complement bytes. A fresh evidence directory is mandatory.
"""
import argparse
import json
from pathlib import Path
import subprocess
import pyarrow as pa
import pyarrow.parquet as pq

def run(binary, mode, path):
    process = subprocess.run([binary, mode, str(path)], check=True, text=True, capture_output=True, timeout=60)
    return json.loads(process.stdout)

def verify(binary, directory):
    root = Path(directory)
    root.mkdir(parents=True, exist_ok=False)
    rust_path, python_path = root/'rust.parquet', root/'pyarrow.parquet'
    expected = run(binary, 'write', rust_path)
    table = pq.read_table(rust_path)
    assert table.schema.field('price').type.field('coefficient').type == pa.binary(16)
    assert table.schema.field('price').type.field('scale').type == pa.uint8()
    assert table.schema.field('event_time_ns').type == pa.int64()
    assert table.schema.field('receipt_time_ns').type == pa.int64()
    actual = table.to_pylist()
    assert len(actual) == len(expected) == 20
    for row, oracle in zip(actual, expected):
        record = json.loads(row['record'])
        assert record == oracle
        for name in ('price', 'native_quantity'):
            assert int.from_bytes(row[name]['coefficient'], byteorder='big', signed=True) == int(oracle[name]['coefficient'])
            assert row[name]['scale'] == oracle[name]['scale']
        for name in ('event_time_ns', 'receipt_time_ns'):
            assert row[name] == int(oracle['fields'][name])
        for name in ('native_symbol', 'native_trade_id'):
            assert row[name] == oracle['fields'][name]
    pq.write_table(table, python_path, compression='zstd')
    assert run(binary, 'read', python_path) == expected
    result = dict(checks_passed=True, rows=len(actual), directions=['Rust-to-PyArrow','PyArrow-to-Rust'],
        coefficients='full signed i128 domain, including extrema', scales=[0,18,38,255],
        timestamps='signed i64 extrema', pyarrow_version=pa.__version__)
    (root/'receipt.json').write_text(json.dumps(result, indent=2)+'\n')
    return result

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--fixture-binary',required=True)
    parser.add_argument('--directory',required=True)
    args=parser.parse_args()
    print(json.dumps(verify(args.fixture_binary,args.directory),indent=2))

if __name__=='__main__':
    if not __debug__:
        raise RuntimeError('verification requires assertions enabled')
    main()
