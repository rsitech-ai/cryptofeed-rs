# Runbook: Release provenance (SBOM + attestation)

**Owner:** release captain  
**Spec:** §29 (SBOM + binary provenance / artifact attestations)  
**Related:** [`docs/plan/chaos_supply_chain.md`](../plan/chaos_supply_chain.md),
[`.github/workflows/release.yml`](../../.github/workflows/release.yml)

## What exists today

| Artifact | How | Status |
|---|---|---|
| CycloneDX SBOM | `./scripts/generate-sbom.sh` → `sbom/` | Local + advisory PR `ci.yml` job `sbom` |
| Tag SBOM upload | `release.yml` job `sbom` on `v*` | Enabled |
| Signed attestation | GitHub Artifact Attestations (`actions/attest-build-provenance@v2`) | **Job enabled** in `release.yml` (hard-fail); no published tag yet |

Do **not** claim production-ready provenance until attestations actually publish
for a tag build. YAML-ready ≠ published.

## Target shape (when enabled)

1. Push tag `v*` → `release.yml` builds SBOM (existing `sbom` job).
2. Attest job attaches provenance to the SBOM artifact (and later release
   binaries) via one of:
   - **GitHub Artifact Attestations** (`actions/attest-build-provenance`) —
     OIDC; needs `id-token: write` + `attestations: write`.
   - **cosign** (`sigstore/cosign-installer` + `cosign attest` / keyless) —
     same OIDC path, or a dedicated signing key in repo secrets.
3. Consumers verify with `gh attestation verify` or `cosign verify-attestation`.

## Publication checklist

- [x] Path: GitHub Artifact Attestations (`actions/attest-build-provenance@v2`)
- [x] `attest` job enabled in `release.yml` (needs `id-token: write` + `attestations: write`; hard-fail, no `continue-on-error`)
- [ ] Tag an alpha release and confirm SBOM artifact + attestation appear
- [ ] Document verify command output for consumers (`gh attestation verify`)

## Local / manual dry-run (no CI)

```bash
cargo install cargo-cyclonedx --locked
./scripts/generate-sbom.sh
# optional, once tooling + identity are available:
# cosign attest-blob --new-bundle-format --predicate sbom/marketfeed.cdx.json \
#   --type cyclonedx sbom/marketfeed.cdx.json
```

## Honesty

Enabled attest job in git ≠ signed release. Spec §3.9 / §29 stay **IN_PROGRESS**
until a real tag run publishes verifiable provenance.
