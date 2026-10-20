# Exact engine operation codec

The engine now serializes complete actual captures into versioned owned DTOs
inside the negotiated causal envelope. Inputs, full pre/post queues and state,
callback/action origins, queue admission and eviction, drains, failures and
outcomes remain distinct. Exact financial record leaves and full catalog
coefficient/scale DTOs avoid floating-point conversion. Decode checks canonical
bytes, schema, structure and inner/outer routing against the expected identity.
See the [codec contract](../../plan/engine_causal_operation_codec.md).

The final unchanged sources pass **1,394 workspace tests**, with zero failures
and 56 explicitly ignored tests. Rust 1.85 core libraries pass **523 tests**.
Independent engine verification passes **141 tests**, including 11 codec unit
cases and four separately authored public regressions. Review findings about
unfinished effect sentinels and owning runner identity were corrected. Clippy
and docs with denied warnings, formatting, cargo-deny 0.20.2 and cargo-audit
0.22.2 with denied warnings pass; no vulnerabilities or dependency warnings.
The [receipt](validation.json) binds source and raw-log hashes and retains the
earlier fixture/harness failures without relabeling them as final results.

The lock changes only add direct engine edges to existing serde/base64 packages;
all 263 registry records remain identical. Independent proof confirms all 52
shared tracked files, six static root bindings and the conservative 71-record
shared dependency closure are unchanged. Hlscreen can retain its `6565c94` feed
pin; this is not whole-producer or artifact equivalence. A separately discovered
Windows artifact-validator decoding failure is being repaired in hlscreen.

All 22 frozen dataset files preserve original bytes and metadata. The preceding
storage source's completed 17-job hosted run is qualified in its own
[receipt](../causal-storage-profile-20261020/ci-qualification.json); this later
codec source needs its own hosted qualification.

This codec does not verify a bootstrap, create a factory, perform replay, release
committed values or serve a live caller. Encoded-output limits are not RSS or
preallocation guarantees. Required next work remains lossless verified bootstrap,
private durable ownership/admission/release, bound sink intents, one committed
consumer projection, actual-engine replay and live integration. The registered
ingress workload remains failed and was not rerun. Native/continuity, strict
datasets, rsibot context/features, calendar and hardware gates remain open.
