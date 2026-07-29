# Runbook: Release provenance (SBOM + attestation)

**Owner:** release captain  
**Spec:** §29 (SBOM + binary provenance / artifact attestations)  
**Related:** [`docs/plan/chaos_supply_chain.md`](../plan/chaos_supply_chain.md),
[`.github/workflows/release.yml`](../../.github/workflows/release.yml)

## What exists today

| Artifact | How | Status |
|---|---|---|
| CycloneDX SBOM | `./scripts/generate-sbom.sh` → `sbom/` | Local generator repaired after the first alpha selected the wrong workspace target |
| `v0.1.0-alpha.1` release | Public GitHub prerelease | Published; archive and checksums exist |
| Tag workflow | `release.yml` on `v*` | The first alpha run failed before any job started |
| Signed attestation | GitHub Artifact Attestations | Not published or verified for `v0.1.0-alpha.1` |

Do **not** claim production-ready provenance until attestations actually publish
for a tag build. YAML-ready ≠ published.

## Target shape (when enabled)

1. Push tag `v*` → `release.yml` runs the release gate and builds the package
   from the tagged commit.
2. The package-and-attest job attaches provenance to the binary archive, SBOM,
   checksums, evidence manifest, and unpacked binary
   via one of:
   - **GitHub Artifact Attestations** (`actions/attest`) —
     OIDC; needs `id-token: write` + `attestations: write`.
   - **cosign** (`sigstore/cosign-installer` + `cosign attest` / keyless) —
     same OIDC path, or a dedicated signing key in repo secrets.
3. Consumers verify with `gh attestation verify` or `cosign verify-attestation`.

## Publication checklist

- [x] `v0.1.0-alpha.1` tag and prerelease published
- [x] Path selected: GitHub Artifact Attestations
- [ ] Run the corrected tag workflow successfully
- [ ] Confirm archive, SBOM, checksums, evidence manifest, and binary each have
      an attestation
- [ ] Document verify command output for consumers (`gh attestation verify`)

## Local / manual dry-run (no CI)

```bash
cargo install cargo-cyclonedx --locked --version 0.5.9
./scripts/generate-sbom.sh
# optional, once tooling + identity are available:
# cosign attest-blob --new-bundle-format --predicate sbom/marketfeed.cdx.json \
#   --type cyclonedx sbom/marketfeed.cdx.json
```

## Honesty

Enabled attest job in git ≠ signed release. Spec §3.9 / §29 stay **IN_PROGRESS**
until a real tag run publishes verifiable provenance.
