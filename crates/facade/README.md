# marketfeed (facade)

Thin public API for embedded consumers (spec **§19** / **§7.1** / **R28**).

```toml
marketfeed = { path = "crates/facade" }  # or crates.io once published
```

## Surface

| Area | Re-exported |
|---|---|
| Model | `Fixed`, `Price`/`Quantity`, `MarketEvent` / `SystemEvent`, ids |
| Adapter contracts | `SessionMachine`, `VenueFactory`, `Subscription` / `Channel` |
| Engine control | `EngineControl`, `EngineSupervisor`, health / book snapshot / rotate |
| Sinks | `sinks::{EventSink, MemorySink, LoggingSink, FileSink, UdpSink}` |

Internal crates (`transport`, `recording`, `dispatch`, `book`, adapters) are **not**
re-exported. Depend on them directly when you need those seams.

## Smoke

```bash
cargo test -p marketfeed
```

## Stability

Pre-1.0: APIs may change per release notes (spec §19.3). This crate is the
intended publish boundary; it does not claim beta/stable/1.0 maturity.
