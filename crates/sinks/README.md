# marketfeed-sinks

Bounded external [`EventSink`](crate::EventSink) implementations for normalized
market batches and system events (spec §17.4–17.5).

## Built-in sinks

| Type | Daemon `[[sinks]] type` | Format |
|---|---|---|
| `MemorySink` | `memory` | In-process queues |
| `LoggingSink` | `logging` | tracing Debug lines |
| `FileSink` | `file` | Append Debug text lines |
| `ProtobufFileSink` | `protobuf-file` | Length-prefixed JSON (MFPE-JSON1) |
| `ProtobufBinaryFileSink` | `protobuf-file-bin` | Length-prefixed protobuf (MFPE-PB1) |
| `UdpSink` | `udp` | Best-effort UDP datagrams (Debug text) |
| `SpillWalSink` | `spill-wal` | Memory + bounded spill WAL (`SpillToDisk`) |
| `KafkaSink` | `kafka` (feature `kafka`) | TCP Produce v0 MessageSet (`acks=0`) |
| `NatsSink` | `nats` (feature `nats`) | TCP `INFO`/`CONNECT`/`PUB` |

Without features `kafka` / `nats`, the types compile as stubs that always return
`SinkError::Unsupported`. With features enabled, they open a real TCP session
and speak a minimal broker wire format (no `rdkafka` / `async-nats`).

## SpillWalSink (`SpillToDisk`)

Implements `OverflowPolicy::SpillToDisk`: live items stay in bounded memory
queues; overflow appends length-prefixed JSON records to a WAL file until
`wal_limit`. Exhaustion **fails closed** (`FailEngine`) and surfaces
`EventsDropped` / `DiskPressure` via `take_system_events()`.

```toml
[[sinks]]
type = "spill-wal"
path = "./spill.wal"
wal_limit = "64MiB"
capacity = 1024
overflow = "spill_to_disk"
```

WAL framing (MFSPILL2):
`["MFSPILL2\n"][u8 tag][u32 le body_len][UTF-8 JSON SpillItem]`. Records carry
complete typed `EventBatch` or `SystemEvent` values. Opening validates records
as a bounded stream; a torn final append is truncated to the valid prefix,
while a malformed complete record fails closed.

Recovery is explicit and at-least-once: consume `pop_recovered()` in append
order, then call `checkpoint_recovery()`. The checkpoint uses a synced
same-directory replacement and atomic rename on Unix. MFSPILL1 metadata-only
files are rejected with quarantine/migration guidance. Daemon startup also
fails closed while a configured spill WAL has unacknowledged recovery records.

## KafkaSink (feature `kafka`)

Bounded ingress + explicit overflow policy (same as other sinks). Sync Produce
API key `0` / version `0` MessageSet to `address`, topic `topic`. Payload lines
match `FileSink` Debug text.

```toml
[[sinks]]
type = "kafka"
address = "127.0.0.1:9092"
topic = "marketfeed"
capacity = 1024
overflow = "drop_newest"
```

Loopback unit tests use a mock TCP peer. **External broker integration**
(Kafka/Redpanda) is operator-only — not started by default CI. Produce v0 may be
rejected by RecordBatch-only clusters; upgrade = ApiVersions + Produce v3+ or
`rdkafka` when operators need that surface.

## NatsSink (feature `nats`)

Same bounded ingress model. Handshake `INFO` → `CONNECT`, then `PUB subject n`.

```toml
[[sinks]]
type = "nats"
address = "127.0.0.1:4222"
subject = "marketfeed.events"
capacity = 1024
overflow = "drop_newest"
```

Loopback mocks cover framing. Full `nats-server` is optional for operator checks.

## ProtobufFileSink framing (MFPE-JSON1)

No prost. Records use the same **field names** as
[`proto/marketfeed/v1/market_event.proto`](../../proto/marketfeed/v1/market_event.proto).

```text
[u32 little-endian length][UTF-8 JSON body] …
```

### Market record

JSON object shaped like proto `EventEnvelope`:

- Top-level: `schema_version`, `venue_id`, optional `instrument_id`,
  `connection_id`, `session_id`, `frame_seq`, `event_index`, optional
  `exchange_ts` (`{ "ns": … }`), `receive_ts`, optional `source_sequence`
  (`{ "first", "last" }`), `flags`, `payload`.
- `payload` is a oneof object with one key among: `trade`, `quote`,
  `book_snapshot`, `book_delta`, `candle`, `mark_price`, `index_price`,
  `funding`, `open_interest`, `liquidation`, `statistics_24h`,
  `instrument_update`, `venue_status`.
- `Fixed` values: `{ "coefficient_lo", "coefficient_hi", "scale" }` (i128 split).
- Enums: proto names (`AGGRESSOR_SIDE_BUY`, `BOOK_SIDE_BID`, …).

Empty batches write **zero** market records (ingress still accepted).

### System record

Companion (not in the MarketEvent proto):

```json
{"kind":"system","event":"<SystemEvent Debug>"}
```

```toml
[[sinks]]
type = "protobuf-file"
path = "./events.mfpe"
capacity = 1024
overflow = "fail_engine"
```

## ProtobufBinaryFileSink framing (MFPE-PB1)

Hand-written protobuf3 wire encoding (tags match the same `.proto`). **No prost /
prost-build** — documented ceiling; upgrade = feature-gated prost codegen when a
consumer needs generated stubs.

```text
[u32 little-endian length][record body] …
```

### Market record

Binary protobuf3 `EventEnvelope` body. Proto3 scalar defaults (`0`, empty
string) are omitted; `optional` / message fields are encoded when present.
Full `MarketEvent` oneof coverage (same surface as MFPE-JSON1).

### System record

Same JSON companion as MFPE-JSON1. Readers: if body starts with `{`, treat as
system JSON; otherwise decode as protobuf `EventEnvelope`.

```toml
[[sinks]]
type = "protobuf-file-bin"
path = "./events.mfpeb"
capacity = 1024
overflow = "fail_engine"
```

## UdpSink

Best-effort UTF-8 datagrams (same Debug-ish text shape as `FileSink` batch lines).
Ingress is bounded; wire `send` failures are counted and do not fail the push.

```toml
[[sinks]]
type = "udp"
address = "127.0.0.1:19090"
capacity = 1024
overflow = "drop_newest"
```

### Ceiling / upgrade

- **Now:** sync append; JSON (`protobuf-file`) and hand binary (`protobuf-file-bin`);
  UDP best-effort Debug text; optional Kafka Produce v0 + NATS PUB over TCP.
- **Upgrade:** prost-generated encode/decode behind a feature; keep Rust
  `crates/model` authoritative; same length prefix. UDP: non-blocking +
  length-prefixed schema. Brokers: ApiVersions/RecordBatch or JetStream/auth
  via heavier clients when required.
