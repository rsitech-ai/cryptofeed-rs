# Protobuf schemas (optional)

Hand-maintained `.proto` stubs for cross-language / `protobuf-file` sink design
(spec layout `proto/marketfeed/v1/`, §18.5 normalized export).

## Status

| Path | Contents |
|---|---|
| `marketfeed/v1/market_event.proto` | Core `MarketEvent` + `EventEnvelope` types mirroring `crates/model` |

**Rust SoT remains `crates/model`.** These files are documentation + a wire-shape
contract, not a build input.

## Generation is optional (ponytail)

- No `prost` / `prost-build` / `tonic` in the workspace today.
- No generated Rust sources are checked in.
- Default `cargo test --workspace` / daemon builds **do not** compile protos.
- Adding codegen is an explicit upgrade: feature-gated crate + pinned `protoc`
  (or `prost-build` alone) when a real protobuf sink or external consumer needs
  it. Until then, prefer this schema file over pulling build-time deps.

```bash
# optional local check only (not CI)
protoc --descriptor_set_out=/dev/null \
  -I proto proto/marketfeed/v1/market_event.proto
```

## Ceiling

- **Now:** schema stub + two length-prefixed file sinks (no prost):
  - `ProtobufFileSink` (`type = "protobuf-file"`) — **MFPE-JSON1** (field-name JSON)
  - `ProtobufBinaryFileSink` (`type = "protobuf-file-bin"`) — **MFPE-PB1** hand
    protobuf3 wire matching these tags (full `MarketEvent` oneof)
  See [`crates/sinks/README.md`](../crates/sinks/README.md).
- **Upgrade:** prost / prost-build feature-gated codegen when an external
  consumer needs generated stubs; keep Rust model authoritative and generate
  *from* or *to* this package with a version gate.
