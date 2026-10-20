# Bounded shared writer infrastructure — 2026-10-20

`marketfeed_recording::shared_batch` adds a unique writer owner and bounded
admission for complete prepared transactions. This is an independently testable
part of the ingress/causal-replay repair. The existing live runner still publishes
input and output separately and exposes mutable state before its second ACK.
The registered durable-ingress workload remains failed. No new workload attempt,
engine release qualification or D5 completion follows from this change.

Prepared values are private and immutable. Capped serialization measures the
entire WireRecord envelope with a worst-case 20-digit sequence number and every
8-byte length/CRC frame. It validates nonempty transactions, at most 65,536
records, 32 MiB per record, 64 MiB per transaction and final supported seals.
Invalid transactions are rejected before admission. Configured count and encoded
byte reservations include queued and in-flight work. Preparation memory belongs
to callers; the writer's bounded encoding buffer is additional. These are not
whole-process RSS limits.

One dedicated thread owns FeedWriter. Admission, close and seal acceptance share
one linearization lock; encoding and filesystem I/O run outside that lock.
Grouping flushes on operation count, exact byte/record capacity, an incompatible
queued suffix, oldest admission age, close or seal. New arrivals do not reset the
oldest deadline. Grouping age excludes scheduling and filesystem duration.
Filesystem calls can block; shutdown drains and joins but cannot promise a
bounded I/O completion time. Close alone does not synthesize a seal.

A ticket means admitted. A successful receipt follows all existing data sync,
watermark sync/replace and directory barriers; it binds the operation's record
sequence range to the covering group cursor. A dropped ticket never cancels its
accepted work. Failure poisons further admission and settles retained tickets:
active publication has an attempted range, while the untouched suffix is
NotAttempted. RecordingError::PublicationIndeterminate retains its exact type;
worker panic is publication-unknown for active work. The last acknowledged
watermark does not claim absence of a later indeterminate visible group.
Unexpected receipt disconnection is Unknown, never proof of cancellation.

Tests challenge exact record/transaction/framing limits, in-flight count/byte
reservations, grouped ranges and nonzero/u64::MAX sequences, exact-full flush,
idle and aged partial groups, rejected seals/oversized transactions, concurrent
close/seal/admission, dropped tickets, owner drop/join, and separate attempted/
not-attempted failure dispositions. Private test hooks exercise failed write,
partial write, data sync, watermark write/sync, replace and post-replace directory
sync on the actual writer. Readers see no failed pre-replace group, or the whole
post-replace group without an ACK. A real successful publication followed by a
panic remains uncertain to its waiter. Another regression panics before a second
publication and preserves the first acknowledgment.

No production failpoint, extra dependency or shared-feed wire variant is added.
Existing FeedHandle and MFSF1 readers retain their behavior. The actor is not yet
wired into SessionRunner. Next implement the private engine owner, complete typed
operation/outcome capture, committed state/dispatch/wire/readiness release and
actual-engine replay; then compare the unchanged registered ingress workload.
Coordinate all five hlscreen source pins when delivering this recording change.
Retain native-unit, continuity, strict-dataset, rsibot context and calendar/runtime
gates. Strategy features and decisions remain in existing rsibot owners.

Local verification passes: 1,335 workspace/all-target/all-feature tests, zero
failed, 56 ignored across 153 suites; 82 recording cases (71 library and 11
integration); Rust 1.85 core libraries 470 passed across 30 suites. Separate review
ran all 19 focused actor tests and closed both findings. Workspace Clippy/docs
with denied warnings, formatting, exact cargo-deny 0.20.2 and cargo-audit 0.22.2
with denied warnings pass. Full-lock RustSec reports zero vulnerabilities/warnings.
No dependency changes were made. All 22 original frozen dataset files retain
bytes, sizes, modes and modification/change times. Source/log hashes and exact
commands are recorded in `validation.json`; final public checks precede commit.
Bulk logs and original captures remain ignored.

```sh
cargo test --locked --workspace --all-targets --all-features
cargo +1.85.0 test --locked --workspace --exclude marketfeed-datasets --lib
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --all-features --no-deps
cargo deny check
cargo audit --deny warnings
```

The new actor source needs its own hosted CI. Earlier timestamp source `7bef521`
completed all 17 jobs; its complete tested merge tree equals that source. Those
results are retained separately and do not qualify the actor change.
