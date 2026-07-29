#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT_DIR="$(mktemp -d "${TMPDIR:-/tmp}/marketfeed-sbom-test.XXXXXX")"
mkdir -p "${ROOT}/dist"
SENTINEL="$(mktemp "${ROOT}/dist/sbom-generator-must-preserve.XXXXXX.cdx.json")"

cleanup() {
  rm -f "$SENTINEL"
  rm -rf "$OUT_DIR"
}
trap cleanup EXIT

printf '{"sentinel":true}\n' >"$SENTINEL"
find "${ROOT}/crates" -name '*.cdx.json' -print | LC_ALL=C sort \
  >"${OUT_DIR}/crate-boms-before.txt"

"${ROOT}/scripts/generate-sbom.sh" --out-dir "$OUT_DIR"

find "${ROOT}/crates" -name '*.cdx.json' -print | LC_ALL=C sort \
  >"${OUT_DIR}/crate-boms-after.txt"
cmp "${OUT_DIR}/crate-boms-before.txt" "${OUT_DIR}/crate-boms-after.txt" || {
  echo "generate-sbom left crate-local SBOM side effects" >&2
  exit 1
}

[[ -s "$SENTINEL" ]] || {
  echo "generate-sbom removed an existing release SBOM" >&2
  exit 1
}

CANONICAL="${OUT_DIR}/marketfeed.cdx.json"
[[ -s "$CANONICAL" ]] || {
  echo "canonical daemon SBOM is missing" >&2
  exit 1
}

jq -e '
  .metadata.component.type == "application" and
  .metadata.component.name == "marketfeed" and
  (.components | length) > 100
' "$CANONICAL" >/dev/null || {
  echo "canonical SBOM does not describe the marketfeed daemon" >&2
  exit 1
}

if jq -e '
  [
    .. | strings |
    select(test("(^|[^A-Za-z])file://|/Users/|[A-Za-z]:\\\\\\\\Users\\\\\\\\"))
  ] | length > 0
' "$CANONICAL" >/dev/null; then
  echo "canonical SBOM contains local file or workstation paths" >&2
  exit 1
fi

echo "generate-sbom-test: PASS"
