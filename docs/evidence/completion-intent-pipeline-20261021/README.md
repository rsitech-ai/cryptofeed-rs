# Bounded Completion and Intent admission

The owner can admit a Completion record and its prepared Intent before waiting
for either record's receipt. Each record retains its own acknowledgement and
ordinal. FIFO validation settles the Completion once; a later Intent failure,
retry or panic cannot erase that known result. Ready state and output release
still require their own acknowledgements and fresh safety checks.

The limit is three distinct owned journal ordinals, including prepared, admitted
and cached work. This change does not enlarge queues, encoded byte limits,
grouping age, deadlines or watchdogs, and does not batch native callbacks.

## Local qualification

The exact 570 compiled inputs have source digest
`e5cd4371d81a253bccc4cc3d645519726fcfe168e0333a40c944b3fe525ec75d`.
All 17 local checks passed: stable workspace 1,699 passed / 0 failed / 56 ignored;
MSRV suites 111 / 314 / 143 / 80 passed; release suites 314 / 143 / 80 passed;
format, Clippy, documentation, CLI and security checks passed.

The history suite has 29 existing and 12 new cases. New cases cover simultaneous
ownership, exact Full retry, failed suffixes, panic cleanup, shared publication
failure, invalid predecessor receipts, expiry, writer closure, reconnection,
generation changes and replay at every record boundary. One existing test was
adapted to deterministic acknowledgement pauses because it assumed Completion
and Intent could not share a physical publication. Its original test and failed
run are preserved; all other original test bytes remain exact.

The independent final audit verifies the raw logs, actual terminal receipts,
all source hashes, retained binaries and build bindings. The capture binary has
an embedded recipe; the observations bridge is bound by the fresh Cargo build
output and exact retained bytes, and has no embedded recipe.

- [Source inventory](source.json)
- [Local qualification](qualification.json)
- [Source review](source-independent.json)
- [Independent final qualification](qualification-independent.json)

## Joint capture 14

The unchanged 120-second request ran with hlscreen and Quants as independent
growing-feed consumers. Producer, both consumers and the native verifier exited
zero. The audit reconciles 9,163 source records and projected groups, 1,800 items,
and 2,007 released native observations: 232 context, 46 book, 1,351 BBO and 378
trade observations. Both consumers reached matching sealed cursor pairs. Seven
gap groups remain explicit. One executed perps BBO was withheld at shutdown
after an accepted Stop; every eligible handoff matches exactly.

The final file modification time difference was 22,909,100 ns, within the
unchanged two-second development bound. A source-final / target-not-final
committed interval was not sampled and remains **UNKNOWN**.

Perps native timestamp to local acquisition receipt: 913 BBO samples, median
1.094018 seconds, p95 2.290035 seconds, maximum 2.623870 seconds and final
0.764974 seconds. Spot: 439 samples, maximum 1.539541 seconds and final
0.344882 seconds. These compare unsynchronized clocks and uncontrolled public
samples. They are encouraging diagnostics, not a causal benchmark, availability
time, network latency or freshness SLA. The preceding joint 13 result remains
unchanged at 21.916159 seconds for its final perps BBO diagnostic.

All 163 protected historical files and the eight new capture files are preserved
with exact hashes and filesystem metadata. The retained hlscreen reader is the
previously qualified `1338b012` build pinned to producer `84aa6353`; this proves
wire compatibility and does not qualify the final consumer dependency pin.

- [Strict framing, values and timing audit](joint14.json)
- [Actual parent terminal](joint14-terminal.json)
- [Independent capture and preservation audit](joint14-independent.json)

The independent audit verifies all 171 files through its final read, reconstructs
the exact source from the published parent and four retained changed files, and
checks the public JSON transformations without changing result values or types.

## Remaining gates

The candidate needs its own hosted Mac/Linux checks and coordinated final
hlscreen dependency pins, local checks, hosted packages and integrated proof.
Quants hosted checks remain externally blocked before any test execution by the
GitHub account restriction. The original Bybit 480-frame comparison remains
failed and unrepeated; it uses legacy wiring and needs an explicit migration.
Strict datasets, native context admission and the longer Razer run remain open.
No root submodule pointer, strategy activation, order, deployment or release is
part of this qualification.

Public JSON copies replace private absolute paths with distinct opaque roots.
Each copy binds its original receipt hash. Results, scalar types, counts, artifact
hashes and scope limits are preserved; original logs and failures remain private.
