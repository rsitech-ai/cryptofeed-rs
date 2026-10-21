# Final shared-feed integration checkpoint

Producer `20f5b1fa16665ce93d0d2da6764e9ad0fd2bfca9`, hlscreen
`0c07b927f999b943ce9304af50314bed9fb148c3`, and Quants
`67a9e1d055fe3dd137756225fd8d97c8ee1f7b2a` are the tested repository pins.
All seven direct / 23 resolved HLS producer packages pin that producer. The
570-input producer recipe remains `e5cd4371d81a253bccc4cc3d645519726fcfe168e0333a40c944b3fe525ec75d`.
The HLS local qualification freezes 352 source files; its published tree adds ten
derivative proofs, for 362 Git files. These counts describe different sets.

## Platform and package verification

Producer [own CI39593757331](https://github.com/rsitech-ai/cryptofeed-rs/actions/runs/39593757331)
passes all 17 jobs. Actual Mac results are 1699 passed / 0 failed / 56 ignored;
both Linux hosts have 1700 / 0 / 56; MSRV has 841 / 0 / 0; Windows has
1696 / 0 / 56. The original paused32 watchdog, closing tests, all six diagnostics
and all twelve new pipeline cases pass. Tested merge and author trees match.
Earlier failures remain preserved; this does not establish their historical cause.

HLS [main39595932396](https://github.com/rsitech-ai/hlscreen/actions/runs/39595932396)
and [native39595932607](https://github.com/rsitech-ai/hlscreen/actions/runs/39595932607)
pass on its own `0c07` head. Main has 589 Rust executions / 565 unique names and
Python suites of 8, 5 and 10; those Rust counts are not additive. Linux, Windows,
Mac ARM and Intel Mac packaged synthetic smokes all pass. All seven ZIP digests,
CRCs, checksums, source export and six packaged documents were independently
verified. The downloaded Mac ARM package also passed a local smoke. This is
package verification, not live-market or extended-operation qualification.

- [Producer hosted proof](producer-hosted.json)
- [HLS hosted proof](hls-hosted.json) and [second review](hls-hosted-peer.json)

## Actual growing feed and recovery

Joint15 ran the byte-exact [120-second request](request-120s.json): spot `@107`
and perps `BTC`, source/target ceilings of 64MiB each, projection batch eight,
unchanged one-millisecond actor grouping age, and 600-second consumer budgets.
Producer, both independent growing-feed consumers and native verifier exited
zero. Audits reconcile **9379 source records / projected groups, 1844 items and
2021 released native observations** (234 context, 46 book, 1373 BBO, 368 trade).
Both consumers reached matching sealed cursor pairs. Six gap groups remain
explicit. All executed native observations were released; two Stop observations
were withheld. All 171 previous and eight new capture files remain unchanged.

Final file modification time difference was **31,034,556ns**, passing the unchanged
two-second development bound. The final source-committed / target-not-final
interval was not sampled and remains **UNKNOWN**. Unsynchronized perps BBO
native timestamp to receipt: median 1.412922s, p95 3.183834s, maximum 3.706897s,
final 0.231199s. Spot maximum was 2.841206s, final 0.358354s. These are uncontrolled
public samples, not availability time, a causal performance benchmark or a
freshness SLA. All preceding failed timing checks remain retained.

Five real Quants processes restarted at groups 5, 11, 9378, 9379 and sealed idle.
Each exited zero, preserving exact state/configuration/checksum/cursor pairs;
sealed idle preserved checkpoint bytes. All 179 originals remained unchanged.
This proves orderly application recovery with a RAM-counter state callback. It
does not prove hard-crash/lost-ACK recovery, physical-publication interior resume,
external exactly-once effects or persistence of actual strategy feature state.
Structural bridge success alone does not authenticate native financial values.

- [Strict native/timing audit](joint15.json), [actual terminal](joint15-terminal.json)
  and [independent capture/preservation proof](joint15-independent.json)
- [Actual restart closure](checkpoint15.json) and [independent recovery proof](checkpoint15-independent.json)

## Saved data

A private local export contains all 15 complete capture layouts, 179 protected
original files, supporting receipts and retained Mac executables: 241 payload
members, 912237349 uncompressed input bytes, 176229337 archive bytes. Archive SHA-256:
`c0c575d331dccfd52e803a93864cfe3be881bb2bde932d3b5ecbf1a1829f343c`.
Each archived member was read back and checked against its source. File and
directory fsync calls completed. The archive and raw market data are not in Git.
This is a local integrity export; off-device backup, power-loss behavior and a
digital signature are not claimed. Mac executable-bound native replay requires
its original platform. Linux must create and retain its own native capture.

- [Export receipt](export.json), [flush receipt](export-flush.json) and
  [independent archive/preservation audit](export-independent.json)

## Remaining gates

Quants own hosted run39579537152 executed zero steps/tests: all four jobs were
rejected by GitHub's account payment/spending restriction. The account owner
must resolve it and rerun the unchanged workflow. Local proof is not hosted proof.
The original Bybit480 comparison remains **FAILED / UNRERUN** under its registered
legacy contract; migrating its actual offline owner/raw-sidecar/native oracle
remains separate work. Strict historical datasets, native availability, full
RESEARCH_V1/perps context, economics and the longer user-run Razer verification
remain open. The rsibot root gitlinks are unchanged. No orders, activation,
deployment, merge or release occurred.

See the [Mac/Linux handoff](../../plan/razer_shared_feed_verification.md).
Portable proofs retain exact bytes where already public, except one explicitly
recorded HLS limitation sentence reworded after a secret scanner matched English
prose. Its original hash and failed scan are preserved; every other HLS value is
exact. Other copies replace only private absolute roots and add an original-hash
binding; result values, scalar types, scopes and failure evidence are preserved.
