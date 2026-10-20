# Opt-in causal bootstrap and factory boundary

This implements the bootstrap/factory prerequisite in the
[integration contract](causal_journal_integration_contract.md). It does not wire
the live daemon, commit operation outcomes, release outputs or replay a journal.
The next owner must retain the private initialized machine and implement those
remaining transitions.

## Exact bootstrap bytes and identity

Engine-owned schema v1 retains the full typed factory/category, environment,
ordered SessionSpec/channel/delivery lists, complete exact catalog, capture
bounds/overflow, effective private-runner constants, declared output/adapter
policies, ordered effective native topics and public reproduction bytes. Catalog
decimals remain canonical i128 coefficient strings plus u8 scales. Protocol
durations preserve u64 seconds/u32 nanoseconds. Embedded reproduction inputs are
base64 bytes with checked length and SHA-256; they are not unverified hash labels.
All input material must be public: this is not credential redaction.

The bootstrap ID hashes the exact canonical payload bytes. Decode checks that
digest before typed decoding, then schema, structural bounds, effective runner
policy, inner/outer venue/session and canonical bytes. Unknown fields/variants,
noncanonical values and altered routes fail. Bootstrap ordinal/generation are
zero. The whole encoded payload has the same explicit capped-writer limit as
operations; this is not a peak-memory/RSS guarantee.

## Observed build and artifact binding

Daemon feature `causal-journal` opts into a workspace-source recipe. Its build
script records compiler verbose identity, target/profile/optimization/debug,
encoded Rust flags, package version and enabled daemon features; runtime adds
the actual compiled engine, Bybit decoder and sink feature lists. It hashes the
exact Cargo.lock and a source snapshot of root Cargo.toml/Cargo.lock/toolchain,
all files under crates and ui/dist, and .cargo if present. Source paths are
normalized to forward slashes and sorted. The SHA-256 input begins with
`marketfeed-source-snapshot-v1\0`, then each path length/path and content
length/content, with little-endian u64 lengths. Symlinks and unsupported source
types are refused. The optional Git commit is informational; the snapshot hash
binds actual bytes, including uncommitted bytes, rather than claiming a clean
commit tree.

Runtime streams the current executable file through SHA-256 with checked size
and before/after modification metadata. Startup compares the complete build and
artifact binding, and checks it again after durable ACK before construction.
This fingerprints the observed executable under a preserved-artifact deployment
assumption. Portable std APIs cannot prove that a replaced pathname still names
the OS's loaded image. It is not signed attestation or a claim of bit-reproducible
compilation, and it does not qualify other binaries/platforms as equivalent.

Without the feature, the build script emits a disabled marker and current_build
returns explicit Unsupported before parsing it. Ordinary legacy/package builds
do not recursively assume a workspace layout. The opt-in feature requires the
complete workspace-source recipe; standalone registry-package builds are not a
supported recipe for this profile.

## First supported factory profile

The first profile supports production public Bybit spot, linear and inverse,
with DiskNullDispatchV1 and CompleteNativeBundleV1. It validates the exact public
WebSocket endpoint, category/venue/product identity, unambiguous catalog IDs and
symbols, supported channels and real adapter protocol limits before admission.
Unknown configurations/sinks are rejected rather than silently disabled.

The legacy adapter produces a broad native bundle and does not honor arbitrary
requested channel subsets or negative delivery flags. This profile does not
claim that defect is globally repaired. It requires every selected instrument
to request Trades, Quote and Statistics24h, plus MarkPrice, IndexPrice, Funding,
OpenInterest and Liquidations for derivatives. All delivery flags are true.
Optional L2/candle shapes must be identical across selected instruments, without
duplicate channels, and match factory enable_l2. The exact effective ordered
topics must equal the same pure topic builder used by the actual adapter.
Partial bundles, differing per-symbol policies and contradictory flags fail
before ACK. Other adapter/configuration profiles remain unsupported here.

Bybit book/quote maps now have deterministic native-symbol order. A real fresh-
factory capture regression first reproduced unordered invalidation effects,
then verifies both ordered invalidation families and complete repeated captures.
This repairs ordering, not source continuity or native-unit qualification.

## Durable factory gate

PreparedBootstrap owns immutable verified DTO/identity and one fully sized
PreparedFeedTransaction. Rejected admission returns that exact prepared object;
Full retries admission rather than preparing another bootstrap or executing a
machine. PendingBootstrap owns the actor's actual ticket. Its blocking wait must
receive the matching post-barrier single-record receipt. Dropped waiters leave
accepted storage work intact and cannot construct a machine.

Only the unforgeable CommittedBootstrap token can construct the actual factory
and persistent CaptureSession. It returns an opaque BootstrappedSession with no
mutable machine/state, metrics/readiness/stop, book or dispatcher escape. Ordinary
publication failure and actor unknown/indeterminate dispositions remain typed
errors; no token or factory follows. Filesystem stalls retain the actor's
blocking-wait limitation. This phase implements no execution retry or rollback.

Required next work is operation admission on this private machine, absolute
deadlines and retained executed operations, post-barrier snapshots/release with
generation-safe revocation, sink-intent binding, one committed consumer
projection, actual-engine physical-order replay and daemon/live migration.
The unchanged registered ingress workload remains failed and has not been
rerun. Native semantics/continuity, strict datasets, rsibot context/features,
calendar and hardware gates remain open. No economics or promotion follows.
