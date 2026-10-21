# Application acquisition timing diagnostics

These receipts qualify the intermediate 568-input diagnostic recipe
`347481b92b568c3bd57a1f971121ab216573c8f8092740be9dc62f2a4df7b4e4`.
Its compiled Git parent is 84aa635; later pipeline edits need separate proof.
The implementation adds bounded terminal-report timing counters without changing
read ordering, FrameStamp, native permission, journal semantics or capacities.
See [the diagnostic contract](../../plan/acquisition_read_diagnostics.md).

Local stable checks pass 1687 tests, zero failures and 56 ignored; MSRV checks
pass 111/314/131/80 and release checks pass 314/131/80. Formatting, Clippy,
documentation, default CLI, fresh audit/deny/secret scans and independent source
and final qualification audits pass. The retained capture executable SHA is
`2ff387334c53a9b1793d70c90f52a9c21c5f7cc3f6b529810f9ba3d07c061886`;
the Rust observation bridge SHA is
`05c2b36ca9c107543192705e59dcbcf0c9b698b9c3c57824426901abd034c3f3`.
Capture identity includes an embedded recipe; bridge provenance is the fresh
Cargo build/output and retained bytes. Neither is loaded-image attestation.

Published diagnostic66ed95e also passes its own required hosted Mac checks
(1687/0/56), Linux x86_64 and aarch64 checks (1688/0/56 each), and MSRV checks
(829/0/0). [hosted66.json](hosted66.json) binds all568 Git inputs and the full
tested-merge tree to that author commit. The workflow is terminal with16 successes
and one secondary Windows failure, zero cancellations. Windows fails the
wire-owner exactly-once/replay watchdog, distinct from the earlier paused32
failure. Both are retained without an inferred cause. This proof does not
qualify later pipeline code.

The unchanged 120-second joint13 request ran producer, HLS1338b01, Quants67a9e1d
and native verification; all four exited zero. Strict CRC/sequence/cursor and
eligible-handoff reconciliation finds 7684 source and target records and 1613
released native observations. Both consumers reach matching sealed cursor pairs.
One native book row withheld at Stop is accounted for separately. All 155 earlier
protected files and the new eight capture files keep their recorded hashes and
stat fields. Final projection file tail is 23,904,762ns, within the unchanged
two-second development gate. The unsampled final source/target interval remains
unknown.

Acquisition freshness remains **unqualified**. Perps observed owner-gate runs
total 119.445 seconds; waits attributed to AwaitingCommit at entry total 116.592
seconds. These overlap and are non-additive. Completed transport reads total
3.909 milliseconds. Perps BBO native-to-local-receipt differences reach 21.953
seconds and end at 21.916 seconds. Native and local clocks are unsynchronized;
these values do not establish network latency or event availability. The local
counters support investigating owner waiting before reads. They do not identify
a sole filesystem cause or qualify a later optimization.

JSON files are portable copies with private absolute paths replaced by distinct
opaque roots and original receipt hashes retained. Actual logs, captures,
executables, preflight helper failure and corrected launch helper remain private
and unchanged. Independent joint13 evidence review passes; see
[joint13-independent.json](joint13-independent.json). Original failed timing samples and the 480 frames/s failure remain valid;
this diagnostic does not qualify a longer Razer run or research dataset.
