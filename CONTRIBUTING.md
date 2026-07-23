# Contributing

## Local CI parity

Match PR checks before opening or updating a PR:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo deny check
```

Install `cargo-deny` once:

```bash
cargo install cargo-deny --locked
```

Toolchain pin: `rust-toolchain.toml` (includes `rustfmt` and `clippy`).
Workspace MSRV: `rust-version = "1.85"` in root `Cargo.toml` (CI `msrv` job).
PR CI also runs `cargo test --workspace` on `ubuntu-latest` + `macos-latest`.
Clippy thresholds: `clippy.toml`. Workspace lint allows: `Cargo.toml` `[workspace.lints]`.
Supply-chain policy: `deny.toml` (licenses, advisories, sources; OpenSSL banned in favor of Rustls).

Release SBOM (local, advisory CI job `sbom`, tag workflow, or release checklist):

```bash
cargo install cargo-cyclonedx --locked
./scripts/generate-sbom.sh
# or: syft dir:. -o cyclonedx-json > sbom/marketfeed.cdx.json
```

| Path | Behavior |
|---|---|
| PR / `main` job `sbom` in `.github/workflows/ci.yml` | Advisory (`continue-on-error`); uploads `sbom/` artifact |
| Tag push `v*` → `.github/workflows/release.yml` | Builds SBOM and uploads `marketfeed-sbom-<tag>` artifact |
| Tag `attest` job (commented stub) | Disabled until Actions billing + OIDC/attestation perms; see runbook |

Actions may fail to start when the org spending limit blocks runners — keep the
workflows merged so provenance is ready when billing works. Live attestation /
cosign signing remain an **OPS** enable step (spec §29). See
[`docs/runbooks/release_provenance.md`](docs/runbooks/release_provenance.md) and
[`CHANGELOG.md`](CHANGELOG.md).

See [`docs/plan/chaos_supply_chain.md`](docs/plan/chaos_supply_chain.md) for fuzz targets and Loom ceiling.

Live network tests are ignored by default; see `README.md`.
