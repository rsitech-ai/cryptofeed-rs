# Production specification conformance

> This matrix records evidence against the normative production specification.
> `done:code` does not imply beta, stable, release-ready, or production-ready.
> Operational and external gates cannot be closed by repository changes alone.

**Specification:** [`production_rust_multi_exchange_market_data_spec.md`](../spec/production_rust_multi_exchange_market_data_spec.md)
**Maintainer:** RSI Tech
**Last verified:** 2026-07-29

Statuses are `done:code`, `partial`, `blocked:ops`, `blocked:external`, and
`decision-required`.

| ID | Requirement | Status | Repository evidence | Remaining proof | Owner / gate |
|---|---|---|---|---|---|
| §3.1 | At least three exchange families, including spot and derivatives | `done:code` | [`venue_channel_audit.md`](venue_channel_audit.md), adapter crates | Preserve the capability audit in every release | Maintainer |
| §3.2, §36 | At least two stable adapters | `blocked:ops` | [`maturity_matrix.md`](maturity_matrix.md) | Scheduled canaries, multi-day soak, ownership, dashboards | Operations |
| §3.3 | Deterministic L2 sequence, gap, checksum, snapshot, and replay tests | `done:code` | `crates/book`, adapter corpus tests, `crates/engine/tests/record_replay.rs` | Hosted CI receipt and live recovery evidence | CI / operations |
| §3.4 | Every queue, buffer, cache, and recording segment is explicitly bounded | `partial` | `crates/dispatch/src/queue.rs`, isolated daemon sink FIFOs, bounded private pump, recording pipeline | Complete the whole-workspace bound inventory | Engineering |
| §3.5 | No silent data drops | `partial` | Per-sink enqueue/drop/error metrics; dispatch and sink-pressure tests | Multi-day injected-pressure proof and alert receipts | Engineering / operations |
| §3.6 | Every failure transition emits a metric and structured event | `partial` | Engine chaos tests and metrics | Complete failure-to-event/metric traceability matrix | Engineering |
| §3.7 | Continuous live soak with bounded memory | `blocked:ops` | [`soak_results.md`](../ops/soak_results.md) | At least 24-hour release soak and seven-day stable soak on Linux | Operations |
| §3.8 | Disconnect, malformed data, snapshot, slow sink, disk-full, and clock chaos | `partial` | [`chaos_supply_chain.md`](chaos_supply_chain.md), offline tests | Release-profile live fault campaign and recovery receipts | Operations |
| §3.9 | Dependency, license, vulnerability, API, compatibility, and provenance gates | `partial` | `deny.toml`, CI, release scripts | Hosted green CI, API/protobuf compatibility, multi-target artifacts, verified attestations | CI / release |
| §3.10 | Public API, configuration, recording, and maturity documentation | `partial` | Specification, README, config examples, ADRs, maturity matrix | Task-oriented architecture, semantics, adapter, and recording guides | Documentation |
| §23.3 | Readiness reflects the required data path | `partial` | Shutdown, disk-pressure, required-sink, required-venue, and distinct per-symbol required-L2-book gates with HTTP/state tests | Emit bounded readiness-reason metrics | Engineering / operations |
| §36 | External public-API review | `blocked:external` | No review receipt | Named external review and disposition record | Independent reviewer |
| §36 | Linux x86_64 and aarch64 release pipelines | `partial` | CI matrix configuration | Exact-tag artifacts, install smoke, checksums, provenance | CI / release |
| §36 | Recording crash recovery | `partial` | Recording and spill-WAL tests | Release-profile crash campaign and archived receipt | Engineering / operations |
| §36 | Supported historical recordings remain readable | `partial` | MFR1 compatibility tests | Versioned compatibility fixtures in the release gate | Engineering |
| §18.5, §36 | Protobuf wire contract and compatibility | `partial` | Hand encoder tests and documented `.proto` field map | Pinned schema compile/currentness check, descriptor compatibility, and independent decoder round-trip | Engineering / CI |
| §19, §36 | Adapter-driven replay is available through the daemon | `partial` | Replay library and engine deterministic tests; daemon raw-recording scanner | Wire the CLI to the adapter replay runner with end-to-end fixtures | Engineering |
| §20, §36 | C header and ABI remain compatible | `done:code` | Rust layout test, linked-and-executed C11 smoke, exported-symbol gate in CI | Cross-version ABI baseline before stable | Engineering / CI |
| §36 | Required sinks drain on shutdown | `done:code` | Atomic queue/in-flight worker state, explicit worker join, isolated sink-worker deadline tests, daemon coordinator drain gate, and `crates/engine/tests/shutdown_drain.rs` | Release-profile live external-sink receipt remains an operations gate | Engineering / operations |
| §36 | Disk exhaustion behavior passes | `partial` | Offline disk-pressure tests and runbook | Real filesystem pressure/full integration run | Operations |
| §36 | Security policy and private reporting channel | `done:code` | [`SECURITY.md`](../../SECURITY.md) | Periodic live verification | Maintainer |
| §36 | SBOM and release attestations | `partial` | Release scripts and workflow | Corrected-tag hosted run and `gh attestation verify` receipt | Release |
| §36 | Metrics dashboards and runbooks | `partial` | `/metrics`, `docs/runbooks` | Checked-in dashboard definitions and alert thresholds | Operations |
| §36 | No open high-severity correctness defect | `blocked:external` | No repository receipt | Issue and private-security tracker review at release time | Maintainer |
| §36 | Reproducible benchmark methodology and results | `partial` | [`latency_runtime.md`](../ops/latency_runtime.md), soak results | Versioned baseline, environment, datasets, regression threshold | Performance |
| §36 | All examples compile | `partial` | README commands | Embedded example plus CI compile/run smoke | Engineering |
| §36 | Maintainer and adapter ownership | `partial` | `CODEOWNERS`, maturity matrix | Per-adapter named owner and succession policy | Governance |
| §31.4 | Distinctive independent project name | `decision-required` | Public repository currently uses `cryptofeed-rs` | Owner naming/provenance decision and any required registry review | Owner |

## Promotion boundary

- `done:code` means the repository contains reviewed implementation and focused
  tests for that criterion.
- `blocked:ops` requires calendar time, live venues, retained evidence, and an
  operator decision.
- `blocked:external` requires a separate account, reviewer, tracker, identity,
  or hosted-system action.
- This matrix must be updated before any beta, stable, `1.0`, or
  production-ready claim.
