# Bybit capture freshness remains unqualified — 2026-10-20

Independent reanalysis of the two retained failed pilots finds material
publisher-to-receipt lag growth across spot, linear and inverse streams. These
are historical captures, not a new live test or a current-source qualification.

| Sample | Venue ID | Native book span (s) | Receipt span (s) | Final age (s) |
|---|---:|---:|---:|---:|
| catalog | 5 | 8.300 | 27.448 | 19.493 |
| catalog | 6 | 11.541 | 27.426 | 16.544 |
| catalog | 11 | 9.960 | 28.011 | 18.625 |
| readiness | 5 | 13.060 | 27.226 | 14.603 |
| readiness | 6 | 11.680 | 27.583 | 16.333 |
| readiness | 11 | 5.520 | 27.455 | 22.305 |

The native field is orderbook `ts`, documented as the millisecond system-data
creation timestamp; matching-engine `cts` is distinct and is not used here.
[Primary contract, checked 2026-10-20](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook).
Absolute age assumes source/local clock alignment. Constant clock offset cancels
from age growth. UTC and monotonic receipt spans differ by less than 0.17 ms, so
a local UTC step does not explain these span differences. Remote clock anomalies,
server replay/delay, network/TCP buffering and local processing remain unresolved.
The local stamp follows `read_frame` return; it is not kernel or wire arrival.

Exact raw/shared frame comparisons still pass for 1,179 and 1,123 frames, and
both original streams contain zero inverse public-trade frames. Correct native
inverse topics and subscription acknowledgements are present. No inverse trade
reaches the decoder in these prefixes, so post-recording decoder loss is not
demonstrated. The shorter retained source spans do not establish 30 seconds of
quiet inverse trading. Freshness and inverse trade coverage remain non-passing;
CRC equality, ACKs and increasing output counts cannot qualify those properties.

The synchronous reader callback performs durable shared publication, including
file and watermark barriers under a shared mutex. This is a plausible backlog
mechanism, not an attributed cause. Original bytes, failed gates and historical
source identities remain unchanged. All witness ordinals, clocks, payload/file
hashes, ACKs and source-blob hashes are retained in `analysis.json`.

Reproduce locally where the original ignored captures are available:

```sh
python3 -B docs/evidence/bybit-capture-freshness-20261020/analyze.py
```

The helper reads only existing artifacts and source blobs, using the repository's
CRC audit readers independently of production adapter normalization. Parent rerun
matches the independent result exactly. `validation.json` binds the helper and
its imported audit scripts. The result does not admit a dataset.

Next is a finite offline paced-ingress attribution benchmark, before another
capture. Register immutable payload copies and one independent synthetic
monotonic generation schedule, comparing the parser/raw path with and without
the production durable shared writer. Measure read return, callback/parse, mutex
wait, data sync, watermark/directory sync and durable acceptance separately.
Synthetic schedule lag must not be described as observed exchange latency.
Suggested bounds in the analysis (10 s at aggregate 480 frames/s plus a 64-frame
burst, 15 s total/drain cap) are proposed local diagnostic thresholds, not exchange
promises or an executed benchmark. Require exact accounting, bounded lag and
empty terminal queues; injected blocking/overflow must fail closed. Preserve
fsync, durable acknowledgement and original receipt stamps in any later repair.
