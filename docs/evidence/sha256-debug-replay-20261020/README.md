# Debug-build executable fingerprint cost

The causal bootstrap deliberately hashes the actual executable on every build
check. A local read/hash probe over the same 25,947,376-byte artifact produced
the same SHA-256 in both configurations: median 693.740125 ms unoptimized versus
45.881375 ms with the existing `sha2` package optimized. This is one local probe,
not a hosted failure diagnosis or whole-application speed guarantee.

The change adds only dev/test package overrides for `sha2`, at opt-level 3.
Debug information, assertions, overflow checks, caller optimization settings,
workload, watchdogs, capacity limits, durability barriers and repeated executable
verification remain unchanged. There is no identity cache or release-profile
change. Consumer workspaces choose their own root profiles.

The same 48 named Hyperliquid cases passed with debug information level 2 in
both builds: 228.35 s before and 45.57 s after. Artifact sizes differ by about
0.16%. Cargo's actual compiler-artifact messages report `sha2` opt-level 0 versus
3 with the other profile fields unchanged; the test target remains opt-level 0.
These are sequential local observations, not a controlled latency qualification.

The frozen 548-input candidate source additionally passes all seven declared
gates: full all-target/all-feature workspace 1,573 passed / 0 failed / 56 existing
ignored; Rust 1.85 bootstrap 57 and public integration 63 passed; formatting,
diff checks, denied-warning Clippy and denied-warning documentation. The lockfile
and dependency versions remain unchanged. [Validation](validation.json) includes
all 48 names, actual artifact-file/embedded-recipe bindings and raw-log hashes;
[source inventory](source.json) binds the frozen build inputs. Retained local
executables are read-only. This is not loaded-image proof, full transitive build
attestation, reproducibility or operational qualification.

Original producer `e812e44` Windows/Linux failures and diagnostic `87b2e4c`
Windows failure remain valid. The latter observed 100 acknowledged projection
groups and its last ACK 307 ms before the whole-projection 15 s deadline; it was
not a demonstrated 15 s stall. Diagnostic `87b2e4c` hosted tests passed 1,573/0/56 on macOS and 1,574/0/56
on Linux x86_64 (one additional Linux affinity case); both witnessed all 48
Hyperliquid cases and all 21 projection regressions. ARM/MSRV remained running
at the latest retained readback.
The profile successor needs its own hosted evidence. A later green run alone
does not prove the cause of an earlier failure. The registered ingress comparison
has not been rerun, and live causal transport/rsibot/operational gates remain open.
