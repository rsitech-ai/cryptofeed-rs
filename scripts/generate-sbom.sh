#!/usr/bin/env bash
# Generate a CycloneDX SBOM for the released marketfeed daemon.
#
# Prefer cargo-cyclonedx (lightweight Rust toolchain plugin). If missing, print
# a syft fallback one-liner — do not pull heavy CI images from this script.
#
# Used by:
#   - local / release checklist
#   - required CI job `sbom` (.github/workflows/ci.yml)
#   - tag `package-and-attest` job (.github/workflows/release.yml)
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
  GEN_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/marketfeed-sbom-source.XXXXXX")"
  SOURCE_ARCHIVE="${GEN_ROOT}/source.tar"
  SOURCE_BOM="${GEN_ROOT}/crates/daemon/marketfeed_bin.cdx.json"
  CANONICAL="${OUT_DIR}/marketfeed.cdx.json"
  SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-$(git show -s --format=%ct HEAD)}"

  # shellcheck disable=SC2329 # Invoked indirectly by the EXIT trap below.
  cleanup_generation_tree() {
    rm -rf "$GEN_ROOT"
  }
  trap cleanup_generation_tree EXIT

  # cargo-cyclonedx writes beside every participating manifest. Generate from a
  # temporary source snapshot so ignored crate-local BOMs never leak into or
  # overwrite the working tree. Include tracked and non-ignored untracked files
  # so ALLOW_DIRTY development packages still describe the current source.
  git ls-files -z --cached --others --exclude-standard |
    tar --null -T - -cf "$SOURCE_ARCHIVE"
  tar -xf "$SOURCE_ARCHIVE" -C "$GEN_ROOT"
  rm -f "$SOURCE_ARCHIVE"

  # Generate only the binary that is packaged by package-release.sh. Running
  # against the workspace root also emits unrelated library targets and made
  # the first alpha SBOM select marketfeed-ffi by filename accident.
  SOURCE_DATE_EPOCH="$SOURCE_DATE_EPOCH" \
  cargo cyclonedx -f json --all --describe binaries \
    --manifest-path "${GEN_ROOT}/crates/daemon/Cargo.toml"

  if [[ ! -s "$SOURCE_BOM" ]]; then
    echo "cargo-cyclonedx produced no daemon SBOM" >&2
    exit 1
  fi

  # cargo-cyclonedx represents workspace packages with local path references.
  # Preserve graph identity while replacing those workstation-specific refs
  # with canonical Cargo purls. Every dependency ref is transformed by the
  # same walk, so the CycloneDX dependency graph remains connected.
  jq -S '
    def canonical_local_ref:
      if startswith("path+file://") and contains("#") then
        split("#")[-1]
        | capture("^(?<name>.+)@(?<version>[^@]+)$")
        | "pkg:cargo/\(.name)@\(.version)"
      elif startswith("pkg:cargo/") and contains("?download_url=file://") then
        split("?")[0]
      else
        .
      end;
    walk(if type == "string" then canonical_local_ref else . end)
  ' "$SOURCE_BOM" >"$CANONICAL"

  jq -e '
    ([.metadata.component["bom-ref"]] + [.components[]?["bom-ref"]]) as $known |
    ([.dependencies[]?.ref] + [.dependencies[]?.dependsOn[]?]) as $used |
    .metadata.component.type == "application" and
    .metadata.component.name == "marketfeed" and
    (.components | length) > 100 and
    (($used - $known) | unique | length) == 0 and
    (($known | length) == ($known | unique | length))
  ' "$CANONICAL" >/dev/null || {
    echo "generated SBOM has the wrong root or broken reference graph" >&2
    exit 1
  }

  if jq -e '
    [
      .. | strings |
      select(test("(^|[^A-Za-z])file://|/Users/|[A-Za-z]:\\\\\\\\Users\\\\\\\\"))
    ] | length > 0
  ' "$CANONICAL" >/dev/null; then
    echo "generated SBOM contains local file or workstation paths" >&2
    exit 1
  fi

  echo "wrote ${CANONICAL}"
  exit 0
fi

cat <<EOF >&2
cargo-cyclonedx not installed.

Install (lightweight):
  cargo install cargo-cyclonedx --locked --version 0.5.9

Or generate with syft in CI (documented fallback):
  syft dir:. -o cyclonedx-json > sbom/marketfeed.cdx.json

EOF
exit 1
