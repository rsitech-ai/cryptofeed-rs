# Checked directory backend: implementation and measured scope

The earlier Windows OS error 5 failure in shared-feed creation is repaired by a single
checked directory backend used by feed publication, raw rotation and datasets.
The new helper retains all file-before-namespace ordering, permanent writer
poison and recovered-intent/reference barriers from the portable repair.
Windows opens with GENERIC_WRITE plus FILE_READ_ATTRIBUTES, BACKUP_SEMANTICS
and default reader/delete sharing. It checks the same live handle's directory
type, NTFS identity, volume-GUID path and fixed-volume classification before
normal sync_all. Unsupported/query/permission/sync failures remain failures.
Financial crates retain forbid(unsafe_code); only three bounded native queries
in the private Windows platform module permit unsafe Rust.

Dedicated job 118377561165/run 39345628986 passes 81 real Windows tests:
4 filesystem, 50 recording unit, 9 shared-feed integration, 12 dataset unit and 6 dataset
integration. Recording/backend runs on Rust 1.85; dataset publication/recovery
runs on Rust 1.88. Zero failures and ignored cases; two Unix-only tests are absent
(the permission and symlink tests), not silently counted as Windows coverage.
The full Windows workspace job 118377561075 also passes 1,271 tests, 0 failed and
56 ignored, including the originally failing daemon pagination/feed-creation
regression. Ignored network/corpus cases are not live evidence.

The workflow's head is 4c2bd1520d16573fd3cdc2fd7a818d126f28a12d, but checkout logs
prove execution on PR merge f02bb3bc941ff0b1a691f5e26d9a350e42330da6. The merge's
Git tree 58a14c234a522af09f0475a66de25aa9189cdfde is independently verified equal
to the producer-head tree. Native logs and API metadata retain both identities.

The actual runtime tests cover std metadata/native flush, full resolved ancestor
barriers, replacement while a reader remains open, complete transaction visibility
after indeterminate publication, permanent poison, raw rotation/metadata,
reconciliation/reused references, post-HEAD failure and reopened intent recovery,
shared-instance writes/CAS, backup and exact/read-only admission. Three filesystem
fixtures separately test unsupported FS/media/path/UTF16 classification; they do
not constitute execution on actual ReFS/FAT/SMB/removable-media mounts.

Final local source passes 1,273 workspace/all-target/all-feature tests (0 failed,
56 ignored),83 focused storage tests,64 Rust 1.85 filesystem/recording tests and a
Rust 1.85 Windows all-target cross-check. Workspace Clippy/rustdoc, formatting,
exact cargo-deny 0.20.2 policy and staged OSS/gitleaks checks pass. Earlier 0.19
policy outputs are exploratory, not exact-tool CI parity. Adding Windows to the
policy graph exposed fs4's old windows-sys 0.59 binding: a version-specific
incompatible-binding disposition records that edge, with no advisory ignore.
Full-lock RustSec still exits 1 only for the pre-existing time/MSRV exception.
All registry package records remain unchanged; only the local helper/recording
edge is new. Exact source/log hashes and review findings are in validation.json.

The hosted NTFS token is administrative. Owning a leaf directory does not prove
write access to every bootstrap ancestor; ordinary-user operation remains
unqualified. OS/FS barrier contracts and injected process failures do not prove
hardware power-loss behavior. No schema change, new capture, strict research
admission or operational promotion is implied. All 17 CI jobs on this producer source pass, including all four platform suites,
MSRV, native storage, Clippy/docs, SIMD, ABI and dependency/SBOM gates. Exact
job identities and the terminal successful run are recorded in validation.json.

Hlscreen 6f13f02 consumes exactly this producer via all five marketfeed packages,
with unchanged 419 registry records. Its full PR gate passes 543 unique Rust tests
and 8 Python bindings; saved 1,055-event projection/output SHA, cursor, clock, seal
and archive hashes remain unchanged. Consumer receipts remain source-scoped in
hlscreen/docs/evidence/shared-feed-native-pin-20261020.
