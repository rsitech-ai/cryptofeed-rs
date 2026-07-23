#!/usr/bin/env bash
# Generate a CycloneDX SBOM for the workspace (release supply-chain artifact).
#
# Prefer cargo-cyclonedx (lightweight Rust toolchain plugin). If missing, print
# a syft fallback one-liner — do not pull heavy CI images from this script.
#
# Used by:
#   - local / release checklist
#   - advisory CI job `sbom` (.github/workflows/ci.yml)
#   - tag release job (.github/workflows/release.yml) → artifact marketfeed-sbom-<tag>
#
# Usage:
#   ./scripts/generate-sbom.sh
#   ./scripts/generate-sbom.sh --out-dir sbom
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${ROOT}/sbom"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    -h|--help)
      sed -n '2,14p' "$0"
      exit 0
      ;;
    *)
      echo "unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

mkdir -p "$OUT_DIR"
cd "$ROOT"

if cargo cyclonedx -h >/dev/null 2>&1; then
  # Emits <crate>.cdx.json beside each Cargo.toml (ponytail: gather into sbom/).
  cargo cyclonedx -f json --all --describe binaries \
    --manifest-path "${ROOT}/Cargo.toml"
  count=0
  while IFS= read -r f; do
    cp "$f" "${OUT_DIR}/$(basename "$f")"
    rm -f "$f"
    count=$((count + 1))
  done < <(find "${ROOT}" -name '*.cdx.json' -not -path '*/target/*' -not -path "${OUT_DIR}/*" | sort)

  if [[ "$count" -eq 0 ]]; then
    echo "cargo-cyclonedx produced no *.cdx.json" >&2
    exit 1
  fi

  tip=""
  for candidate in marketfeed-daemon.cdx.json marketfeed.cdx.json; do
    if [[ -f "${OUT_DIR}/${candidate}" ]]; then
      tip="${OUT_DIR}/${candidate}"
      break
    fi
  done
  if [[ -z "$tip" ]]; then
    tip="$(find "${OUT_DIR}" -name '*.cdx.json' | head -1)"
  fi
  cp "$tip" "${OUT_DIR}/marketfeed.cdx.json"
  echo "wrote ${OUT_DIR}/marketfeed.cdx.json (plus ${count} package BOM(s))"
  exit 0
fi

cat <<EOF >&2
cargo-cyclonedx not installed.

Install (lightweight):
  cargo install cargo-cyclonedx --locked

Or generate with syft in CI (documented fallback):
  syft dir:. -o cyclonedx-json > sbom/marketfeed.cdx.json

EOF
exit 1
