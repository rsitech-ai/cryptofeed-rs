# Contributing

Thanks for helping improve cryptofeed-rs. RSI Tech maintains this project at
[rsitech.ai](https://rsitech.ai); public and confidential project questions can
be sent to [info@rsitech.ai](mailto:info@rsitech.ai).

By contributing, you agree that your contribution is licensed under
[Apache-2.0](LICENSE). Sign commits with the Developer Certificate of Origin:

```text
Signed-off-by: Your Name <your-email@example.com>
```

Use `git commit -s` to add the sign-off. Do not include exchange credentials,
private recordings, customer data, or generated build output.

## Local CI parity

Match PR checks before opening or updating a PR:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps
cargo deny check
./scripts/check-oss-readiness.sh
```

Install `cargo-deny` once:

```bash
cargo install cargo-deny --locked
```

Toolchain pin: `rust-toolchain.toml` (includes `rustfmt` and `clippy`).
Workspace MSRV: `rust-version = "1.85"` in root `Cargo.toml` (CI `msrv` job).
PR CI runs the full workspace test matrix on Linux, macOS, Windows, and Linux
ARM64.
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
| Tag `attest` job | Enabled; requires GitHub OIDC and attestation permissions |

Live attestation / cosign signing remain an **OPS** enable step (spec §29). See
[`docs/runbooks/release_provenance.md`](docs/runbooks/release_provenance.md) and
[`CHANGELOG.md`](CHANGELOG.md).

See [`docs/plan/chaos_supply_chain.md`](docs/plan/chaos_supply_chain.md) for fuzz targets and Loom ceiling.

Live network tests are ignored by default; see `README.md`.

## Pull requests

- Keep changes focused and explain operational or compatibility impact.
- Add a regression test for behavior changes.
- Update public docs when configuration, output, or support changes.
- Confirm the full local CI parity block above before requesting review.
- Report security-sensitive findings through [`SECURITY.md`](SECURITY.md), not a public issue.
