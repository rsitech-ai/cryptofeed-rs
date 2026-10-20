# macOS/Linux shared-feed handoff and extended verification

This continues the active Hyperliquid/shared-feed/rsibot goal. Required hosts are
macOS and Linux; preserve Windows failures separately. Razer host access and
workspace location must be supplied before host-specific execution. A successful
test loop is not an exchange-data soak or operational promotion.

## Before the handoff

1. Producer 696525da now passes all 17 hosted jobs, including required Mac/Linux
   hosts. Preserve the earlier watchdog failures and cancelled coverage; their
   full causes remain unproven. Requalify every later source revision on its own
   hosts rather than inheriting this predecessor result.
2. Finish the opt-in causal live transport and its durable outbound intent/result
   protocol. Captured null-drain writes are not authorization to send over a
   socket. One privately owned native session must drive acquisition, control,
   timers, reconnects and shutdown; no second legacy runner may shadow it.
3. Tail committed source observations into MFOP groups. Publish only acknowledged
   groups, preserve independent consumer cursors, and settle owners and target
   publication before sealing. Recovery creates a fresh writer lifetime; never
   silently take over an existing feed directory.
4. The direct Quants reader and diagnostic observer are implemented and under
   final source qualification. Bind cursor pairs to the consumer's restored
   state/config/version; acknowledging a feed does not persist feature state.
   Structural MFOP validation is separate from native source comparison and
   research dataset admission. Unknown usable time stays unavailable.

## Reproducible host preflight

Use separate checkouts with exact reviewed producer, HLS and Quants commits. Save
the clean Git trees, OS/architecture, compiler verbose identity, available disk,
filesystem/mount details, commands and actual exit codes. Preserve failed logs.
Keep Cargo targets separate from other projects and concurrent toolchains:

```sh
export CARGO_INCREMENTAL=0
export CARGO_TARGET_DIR="$PWD/.local/target-razer-validation"
cargo fmt --all -- --check
cargo test --locked --workspace --all-targets --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo +1.85.0 test --locked -p marketfeed-daemon --features causal-journal --test hyperliquid_causal
```

Record actual executable hashes and embedded recipes, not just checkout names.
Keep dev/test debug information when checking hosted-test parity. The SHA-256
package optimization retains the real executable rechecks; do not substitute a
cached identity, reduced corpus, relaxed watchdog or disabled durability barrier.
HLS and Quants have their own root build/runtime settings and validation commands;
Cargo dependency-workspace profiles do not propagate into consumer workspaces.

## Extended verification gates

After the live path is implemented and tested, freeze the capture configuration,
source/consumer pins, instruments/channels, capacities, disk budget, freshness
limits, recovery procedure and abort rules before starting a bounded pilot.
Record the real acquisition, receipt and application clocks independently; do
not turn owner-time approximations into historical availability claims.

Keep raw/native observations, original metadata, quality transitions, committed
watermarks, consumer ACK pairs, error counters and resource measurements. Check
that hlscreen and Quants independently reach matching logical group boundaries.
Inject disconnect/reconnect, consumer restart, producer stop, slow-consumer and
storage-failure cases before the calendar-spaced canaries and declared 24h/7d
soaks. Retain unknown I/O outcomes and incomplete intervals; never fill a gap by
relabeling forward captures or changing frozen historical data.

Only rerun the existing registered ingress comparison after real owner/transport
integration, with its original workload, capacities, thresholds and abort rules.
No economic/model run, orders, strategy activation, deployment, merge or release
is granted by this runbook. A handoff is ready only when the implemented commands,
host receipts, failures, limitations and rollback pins are recorded together.
