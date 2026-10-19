import asyncio, base64, datetime, hashlib, json, time
from pathlib import Path
from websockets.asyncio.client import connect
ROOT=Path('.local/evidence/hyperliquid-integration-review/bybit-reqid-probe-01')
ROOT.mkdir(exist_ok=False)
async def probe(category,symbol):
    path=ROOT/f'{category}.jsonl'
    seen=[]; failure=None; size=0
    with path.open('x') as out:
        def record(direction,message):
            nonlocal size
            encoded=json.dumps({'received_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'monotonic_ns':time.monotonic_ns(),'direction':direction,'raw_base64':base64.b64encode(message.encode() if isinstance(message,str) else message).decode()})+'\n'
            size+=len(encoded)
            if size>2_097_152: raise RuntimeError('local2MiB protocol probe bound')
            out.write(encoded); out.flush()
        try:
            async with connect(f'wss://stream.bybit.com/v5/public/{category}',open_timeout=8,close_timeout=2,max_size=1_048_576,ping_interval=None) as ws:
                ids=[f'probe-{category}-trade',f'probe-{category}-quote']
                for req_id,topic in zip(ids,[f'publicTrade.{symbol}',f'orderbook.1.{symbol}']):
                    message=json.dumps({'op':'subscribe','req_id':req_id,'args':[topic]})
                    record('out',message); await ws.send(message)
                deadline=time.monotonic()+8
                while len(seen)<2:
                    message=await asyncio.wait_for(ws.recv(),deadline-time.monotonic())
                    record('in',message)
                    value=json.loads(message)
                    if value.get('op')=='subscribe': seen.append(value)
        except Exception as e: failure=f'{type(e).__name__}: {e}'
    return {'category':category,'symbol':symbol,'acks':seen,'failure':failure,'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'bytes':path.stat().st_size}
async def main():
    results=await asyncio.gather(*(probe(c,s) for c,s in [('spot','BTCUSDT'),('linear','BTCUSDT'),('inverse','BTCUSD')]))
    record={'schema':'bybit.finite_reqid_protocol_probe.v1','scope':'two requests per public socket; protocol investigation only, no data qualification','results':results}
    (ROOT/'results.json').write_text(json.dumps(record,indent=2)+'\n')
    print(json.dumps(record,indent=2))
asyncio.run(main())
