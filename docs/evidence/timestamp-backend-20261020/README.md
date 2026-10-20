# Timestamp backend and advisory repair — 2026-10-20

The remaining project-lock advisory is removed without raising core Rust 1.85.
EventPulse now uses already-locked Chrono 0.4.45 with default features disabled
and `std` enabled. Capture reuses its UTC microsecond formatter; dataset export
uses a separate exact nanosecond formatter. The offline dataset tool retains
Rust 1.88. No shared feed schema or strategy policy changes.

Exactly six time-only packages are removed: time 0.3.41, time-core 0.1.4,
time-macros 0.2.22, deranged 0.4.0, num-conv 0.1.0 and powerfmt 0.2.0. No registry
package is added, and all 263 remaining registry records retain their complete
contents. The exact RUSTSEC-2026-0009 exception is removed from cargo-deny.
Full-lock cargo-audit 0.22.2 with denied warnings changes from exit 1 with one
advisory to exit 0 with zero advisories/warnings. Both scans use the same database
commit and retain their original results. This describes the branch scan;
default-branch alerts await reviewed integration.

Before changing the backend, the retained legacy probe at `82a871b` froze
302 parser cases (274 accepted, 28 rejected) and 17 nanosecond cases. The exact
fixture bytes remain unchanged. Current tests preserve accepted/rejected status,
canonical bytes, epoch microseconds, UTC conversion/refusal and shortest exported
nanosecond spelling. Strict ASCII/calendar/uppercase-T/zone guards prevent
Chrono's broader input aliases from expanding the wire contract. Leap seconds
remain rejected. Offset spelling, zero-or-six fractional digits, year 0000 core
acceptance and capture's separate minimum year 0001 remain unchanged.

UTC conversion uses microseconds directly, including year 9999, and refuses UTC
years outside 0000..9999. Negative nanoseconds use Euclidean floor, including
i64::MIN. Dataset export retains `.1Z`, `.1234Z` and `.0000001Z` spelling rather
than padded AutoSi output. Additional tests challenge Gregorian century rules.

Local verification passes:

- Full workspace/all-target/all-feature tests: 1,316 passed, zero failed,
  56 ignored across 153 suites. Ignored checks retain their original scope.
- Rust 1.85 core CI library command: 451 passed across 30 suites; final timestamp
  integration: four passed, including all frozen cases and century boundaries.
- Rust 1.88 dataset command: 20 passed. The final affected suite was rerun after
  Clippy caught a test module before production items; moving that module to the
  file end changed no runtime function. The failed lint log remains retained.
- Workspace Clippy with denied warnings, docs with denied warnings, formatting,
  exact cargo-deny 0.20.2 and full-lock RustSec checks pass. Final public-surface
  checks and source/log hashes are recorded in `validation.json`.

Separate review independently checks all 274 accepted epochs using integer
proleptic Gregorian arithmetic, 272 using Python datetime, and all 17 nanosecond
strings/floor cases. It also verifies exact lock changes and all 17 original
contract/schema/golden files against their retained hashes. The vectors are
compatibility evidence, not an exhaustive parser-space proof.

The rebuilt final Rust 1.88 dataset reader verifies all 2,666 rows in original
manifest `a146175ce1fa387e6cfe301626d28798d8136542ad723e9ccbc61dc732f68d39`.
All five admission profiles remain refused. All 22 original files retain hashes,
sizes, modes, modification and change times; the store directory remains read-only.
The complete inspect output hash remains
`03a8fc06c21fa6877f6a756e8b31e9eb783257bc73c32043b61fdd0ddb94a90b`.
The parent executes these CLI checks; the independent audit inspects the retained
receipts and does not claim its own CLI/store execution.

Exporter source participates in decoder provenance, so future source/revision
hashes legitimately change even when timestamp bytes match. No original frozen
manifest, object, contract or golden is regenerated. The five packages used by
hlscreen have unchanged source trees, so its coordinated `82a871b` pin remains
appropriate. The full goal remains active: the registered durable-ingress failure,
complete causal engine replay, native semantics/continuity, rsibot live context,
strict datasets and calendar/runtime qualification remain open.

Reproduce the current gates from the repository root:

```sh
cargo test --locked --workspace --all-targets --all-features
cargo +1.85.0 test --locked --workspace --exclude marketfeed-datasets --lib
cargo +1.85.0 test --locked -p marketfeed-event-pulse --test timestamp_backend_compatibility
cargo +1.88.0 test --locked -p marketfeed-datasets
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --all-features --no-deps
cargo audit --deny warnings
cargo deny check
```

Use the tool versions recorded above. `legacy-probe.rs` is reproduction source
for the historical `82a871b` checkout, not a new dependency or example compiled
by this workspace. Bulk datasets and full logs remain in ignored local evidence.

Hosted CI for source `7bef521` subsequently completed all 17 jobs successfully.
Actual merge `7538d1b` has the identical complete tree `cfa48db`. Linux x64/ARM
each pass 1,317, macOS 1,316 and Windows 1,314, with zero failed, 56 ignored and
153 suites each. The hosted Windows durability subset passes 85. Exact checkout
and log hashes are recorded in `ci-qualification.json`. This qualifies that
source only; the later actor and engine/ingress work require separate checks.
