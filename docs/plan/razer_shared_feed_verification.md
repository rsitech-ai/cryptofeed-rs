# macOS/Linux shared-feed handoff and extended verification

The user will pull the reviewed revisions from Git and has deferred remote
access and Razer execution. This document describes the next host run; it does
not record one. macOS and Linux are the required platforms.

## Qualified pins and preserved limits

| Component | Tested Git revision | Evidence |
| --- | --- | --- |
| Producer | `20f5b1fa16665ce93d0d2da6764e9ad0fd2bfca9` | Own17 hosted jobs pass, including Mac/Linux x86_64/Linux ARM/MSRV, original paused32 watchdog and closing tests |
| Hlscreen | `0c07b927f999b943ce9304af50314bed9fb148c3` | All7direct/23resolved producer pins20f5; own main and all4 native package smokes pass |
| Quants | `67a9e1d055fe3dd137756225fd8d97c8ee1f7b2a` | Runtime unchanged from locally tested a1d45ac5; actual growing-feed and orderly checkpoint recovery pass |
| Native context owner | `22f364e6a60a48601fcac0cc124394033e7ff411` | Separate bounded synthetic115-cell spot proof; full context admission remains unsupported |

The producer's 570 compiled inputs have recipe
`e5cd4371d81a253bccc4cc3d645519726fcfe168e0333a40c944b3fe525ec75d`.
Later documentation commits need an exact unchanged compiled-input comparison;
checkout names alone are insufficient. Retain actual compiler identities,
executable hashes and embedded recipes. Dependency workspace build profiles do
not propagate to consumer roots. Do not change rsibot root gitlinks as setup.

[Final integration evidence](../evidence/shared-feed-integration-20261021/README.md)
records the unchanged120-second joint15:9379 groups/1844 items/2021 native
observations, both consumers sealed, six explicit gaps,31.034556ms final file
catch-up and179 original files unchanged. Five real Quants processes resumed at
5/11/9378/9379/sealed idle. This is orderly RAM-counter recovery, not strategy
feature-state, hard-crash, lost-ACK or external exactly-once qualification.
Unsynchronized perps timestamp diagnostics peaked3.706897s and ended0.231199s;
these are not a freshness SLA, historical usable time or causal performance proof.

Quants own hosted run39579537152 executed zero steps/tests under GitHub's account
payment/spending restriction. The account owner must resolve it and rerun the
unchanged workflow. Local results do not close that external platform gate.
The original Bybit480 workload remains **FAILED / UNRERUN**. Its registered legacy
SessionRunner/raw-sidecar/MFSF1 contract needs actual offline causal-owner and
native-oracle migration; a Hyperliquid capture cannot substitute for it. Preserve
all prior failed runs and unknown intervals.

## Host preflight and unchanged reference capture

Use clean separate checkouts at the pins above. Save OS/architecture, Git trees,
compiler verbose identities, filesystem/mount information, free space, resource
measurements, commands and actual terminal exit codes. Reserve at least10GiB of
free disk beyond declared capture/export storage budgets. Refuse a reused output
root. Keep targets separate from other projects/toolchains. On the producer:

```sh
export CARGO_INCREMENTAL=0
export CARGO_TARGET_DIR="$PWD/.local/target-razer-validation"
cargo fmt --all -- --check
cargo test --locked --workspace --all-targets --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo +1.85.0 test --locked -p marketfeed-daemon --features causal-journal --test hyperliquid_causal
cargo build --locked --release -p marketfeed-daemon --features causal-journal \
  --bin marketfeed-capture --bin marketfeed-observations
"$CARGO_TARGET_DIR/release/marketfeed-capture" identity
```

Use each consumer's own documented validation and root runtime settings. Build
and retain fresh Linux executables and actual SHA-256 pins; the saved Mac archive
cannot provide executable-bound native replay on Linux. Never claim a new build
can replay an old executable-bound source because its Git source matches.

The byte-exact [reference request](../evidence/shared-feed-integration-20261021/request-120s.json)
was added in documentation commit `b3da14730be3dc88287fd81429829779efb1a488`;
it is absent from the runtime20f5 checkout. Keep that runtime checkout and fetch
the documentation branch, then extract the request from the immutable commit.
This checks its exact hash and creates a new file outside the capture directory;
an existing file is refused. Use an existing writable data parent in these
example paths.

```sh
git fetch origin codex/native-observations
python3.12 - <<'PYREQUEST'
import hashlib
from pathlib import Path
import subprocess

request = subprocess.check_output([
    "git", "show",
    "b3da14730be3dc88287fd81429829779efb1a488:"
    "docs/evidence/shared-feed-integration-20261021/request-120s.json",
])
if hashlib.sha256(request).hexdigest() != (
    "3c099a18482fc18deb3b83fbe781f9860ee8fd19e5bda4a245b05acacc6f6294"
):
    raise SystemExit("Reference request checksum mismatch")
with Path("/data/reference-request-120s-01.json").open("xb") as output:
    output.write(request)
PYREQUEST
```

First run that unchanged120-second request with a new output root. It uses native
spot `@107` and perps `BTC`,120000ms,64MiB source/target ceilings and projection
batch8. Verify selected native names still exist; a changed selection needs a
separately registered request.

```sh
"$CARGO_TARGET_DIR/release/marketfeed-capture" run \
  --request /data/reference-request-120s-01.json \
  --output /data/new-reference-01
```

While that process is running, wait for `/data/new-reference-01/feed` to be
created, then launch both consumers independently in separate terminals. Record
startup/witness times and retain their stdout/stderr and actual exit codes.
Use the retained HLS executable and explicit600-second reference budgets:

```sh
hls live --shared-feed /data/new-reference-01/feed \
  --symbols spot:@107,perps:BTC --duration-secs 600 --refresh-secs 5
```

From the Quants checkout, substitute the actual retained Linux bridge path and
hash; do not copy the Mac bridge hash from the historical receipt:

```sh
python3.12 -m scripts.data.observe_cryptofeed_projection \
  --binary /data/tools/marketfeed-observations \
  --binary-sha256 ACTUAL_RETAINED_LINUX_BRIDGE_SHA256 \
  --feed /data/new-reference-01/feed \
  --output /data/new-reference-01-quants.json \
  --seconds 600 --max-groups 100000 --idle-poll-ms 10
```

Wait for actual producer and consumer termination. Retain all partial output on
failure. After capture settlement, run its own retained executable:

```sh
/data/new-reference-01/capture-executable verify --output /data/new-reference-01
```

Verify native financial values against source observations, exact framing/CRCs,
bootstrap/request/metadata provenance, per-market/channel coverage, quality gaps,
all child outcomes and matching sealed consumer cursor pairs. Structural MFOP
validation alone does not authenticate native values. An orderly Stop/Seal can
coexist with managed transport errors or missing market rows. Keep final file
mtime differences distinct from actual sampled committed-watermark intervals;
an unsampled interval remains unknown. A final cursor alone does not prove a
consumer observed the feed while growing or met freshness requirements.

Repeat actual checkpoint tests with state/config/version-bound cursor pairs and
immutable-source preservation checks. Do not label an orderly restart as a
hard-crash or feature-state recovery test. A new writer always uses a fresh
lifetime; never silently take over or append to an old feed root.

## Longer verification

Before a longer capture, freeze a new plan with duration, file ceilings,
consumer budgets/group limits, instruments/channels, source and consumer pins,
disk/resource budgets, sampling interval, explicit freshness requirements and
abort/recovery rules. The120-second reference's600-second consumer timeout is
not suitable for arbitrary longer runs. Duration is at most24h per native
lifetime; a7-day study requires separately preserved lifetimes with explicit
continuity gaps. No longer configuration has been qualified by this checkpoint.

Do not relax actor/native caps, grouping age, watchdogs, authority barriers or
original failure thresholds to obtain a pass. Never rerun the legacy480 contract
with changed load or substitute the Hyperliquid reference. Retain raw/native
observations, metadata, quality transitions, committed watermarks, consumer ACK
pairs, counters and resource measurements. Record acquisition, receipt and
application clocks separately; keep unknown usable time unavailable.

Qualify disconnect/reconnect, consumer restarts, producer Stop, slow consumers
and storage failures before calendar-spaced canaries and declared24h/7d soaks.
Preserve partial/failed intervals and I/O uncertainty; do not fill historical gaps
with forward captures. Copy the checksummed private export to independently
managed storage and verify it there before calling it an off-device backup.
The local archive/readback/fsync receipt alone does not establish that property.

This runbook grants no orders, economic/model evaluation, strategy activation,
deployment, root gitlink update, merge or release. Operational promotion remains
a separate decision after the required evidence and external gates close.
