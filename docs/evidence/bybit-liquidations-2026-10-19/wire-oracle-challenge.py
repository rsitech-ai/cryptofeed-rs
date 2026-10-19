import json, os, shutil, subprocess, sys
from pathlib import Path
from google.protobuf import json_format
from marketfeed.v1 import market_event_pb2 as pb
root=Path('.local/evidence/hyperliquid-integration-review')
original=root/'native-wire-liquidation-01'
output=root/'native-wire-liquidation-mutations-01'
output.mkdir(exist_ok=False)
cases=[
('bankruptcy_as_execution',7,lambda l:setattr(l.provenance,'price_basis',pb.LIQUIDATION_PRICE_BASIS_EXECUTION)),
('unknown_units_as_base',7,lambda l:setattr(l.provenance,'quantity_unit',pb.LIQUIDATION_QUANTITY_UNIT_BASE)),
('long_as_short',7,lambda l:setattr(l.provenance,'position_side',pb.LIQUIDATED_POSITION_SIDE_SHORT)),
('inferred_execution_sell',7,lambda l:setattr(l,'side',pb.AGGRESSOR_SIDE_SELL)),
('zero_updated_replaced_by_publication',7,lambda l:l.provenance.native_attributes.__setitem__('T','1700000000000')),
('lost_i128_precision',7,lambda l:setattr(l.price.value,'coefficient_lo',-2)),
('lost_scale',7,lambda l:setattr(l.price.value,'scale',18)),
('lost_unknown_position_presence',8,lambda l:l.provenance.ClearField('position_side')),
('legacy_absence_replaced_by_empty',9,lambda l:l.provenance.SetInParent()),
('absent_position_replaced_by_unknown',10,lambda l:setattr(l.provenance,'position_side',pb.LIQUIDATED_POSITION_SIDE_UNKNOWN)),
]
results=[]
for name,index,mutate in cases:
    target=output/name
    shutil.copytree(original,target)
    path=target/f'{index}.pb'
    envelope=pb.EventEnvelope.FromString(path.read_bytes())
    mutate(envelope.payload.liquidation)
    path.write_bytes(envelope.SerializeToString(deterministic=True))
    # Corrupt both projections consistently: agreement alone must not pass.
    (target/f'{index}.json').write_text(json.dumps(json_format.MessageToDict(envelope,preserving_proto_field_name=True)))
    result=subprocess.run([sys.executable,'scripts/verify_native_provenance_wire.py',str(target)],capture_output=True,text=True,timeout=10)
    (target/'verifier.stderr').write_text(result.stderr)
    results.append({'case':name,'returncode':result.returncode,'rejected':result.returncode!=0})
assert all(r['rejected'] for r in results),results
record={'schema':'marketfeed.liquidation_wire_oracle_challenge.v1','source_fixture':'synthetic Rust fixture; original files unchanged','mutations':results,'all_rejected':True,'synchronized_binary_json_corruption':True}
(output/'results.json').write_text(json.dumps(record,indent=2)+'\n')
print(json.dumps(record,indent=2))
