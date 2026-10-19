#!/usr/bin/env python3
"""Read synthetic Rust-encoded records with generated Google protobuf classes.
Run protoc --proto_path=proto --python_out=GENERATED
    proto/marketfeed/v1/market_event.proto first; put GENERATED on PYTHONPATH.
"""
import json
import sys
from pathlib import Path
from google.protobuf import json_format
from marketfeed.v1 import market_event_pb2 as pb
if not __debug__:
    raise RuntimeError("verification requires Python assertions enabled")
root = Path(sys.argv[1])
records = []
for index in range(7):
    binary = pb.EventEnvelope.FromString((root / f'{index}.pb').read_bytes())
    projected = json_format.ParseDict(json.loads((root / f'{index}.json').read_text()), pb.EventEnvelope())
    if binary != projected:
        raise RuntimeError(f'normalized JSON and protobuf differ for {index}')
    records.append(binary)
trade = records[0].payload.trade
assert trade.provenance.native_count == (1 << 64) - 1
assert trade.provenance.granularity == pb.TRADE_GRANULARITY_NATIVE_AGGREGATE
assert trade.provenance.constituent_first_id == '9007199254740993'
assert trade.provenance.native_attributes['count_meaning'] == 'unverified-native-only'
assert records[1].payload.candle.HasField('is_closed')
assert records[1].payload.candle.is_closed is False
assert records[1].payload.candle.provenance.source_channel == 'kline.1.BTCUSD'
assert records[1].payload.candle.provenance.native_attributes['turnover'] == '0.000000000000000001'
assert not records[2].payload.candle.HasField('provenance')
assert not records[2].payload.candle.HasField('is_closed')
assert not records[2].payload.candle.HasField('quote_volume')
assert records[1].payload.candle.native_volume_ccy.value.coefficient_lo == 17
assert not records[2].payload.candle.HasField('native_volume_ccy')
empty = records[3].payload.trade
assert empty.HasField('trade_id') and empty.trade_id == ''
assert empty.provenance.HasField('constituent_first_id') and empty.provenance.constituent_first_id == ''
assert empty.provenance.HasField('constituent_last_id') and empty.provenance.constituent_last_id == ''
assert empty.provenance.HasField('native_count') and empty.provenance.native_count == 0
value = records[1].payload.candle.quote_volume.value
assert ((value.coefficient_hi << 64) | (value.coefficient_lo & ((1 << 64)-1))) == (1 << 127)-1
assert value.scale == 255
for record, name in zip(records[4:], ('book_snapshot','book_delta','quote')):
    source = getattr(record.payload,name).provenance
    assert source.source_channel == 'orderbook.50.BTCUSDT'
    assert source.native_attributes['u'] == str((1 << 64)-1)
    assert source.native_attributes['seq'] == '9007199254740993'
    assert 'cts' not in source.native_attributes
assert records[5].payload.book_delta.changes[0].operation == pb.BOOK_OPERATION_DELETE
assert not records[5].payload.book_delta.changes[0].HasField('quantity')
assert not records[6].payload.quote.HasField('ask_quantity')
print(json.dumps({'checks_passed': True, 'records': 7, 'independent_reader': 'generated-google-protobuf', 'optional_false_and_unknown_distinct': True}))
