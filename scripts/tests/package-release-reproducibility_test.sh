#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
LOG_DIR="$(mktemp -d "${TMPDIR:-/tmp}/marketfeed-package-test.XXXXXX")"

cleanup() {
  rm -rf "$LOG_DIR"
}
trap cleanup EXIT

cd "$ROOT"

run_package() {
  local run_root="${LOG_DIR}/$1"
  local dist_dir="${run_root}/dist"
  local target_dir="${run_root}/target"

  mkdir -p "$dist_dir" "$target_dir"
  if ! ALLOW_DIRTY=1 \
    DIST_DIR="$dist_dir" \
    CARGO_TARGET_DIR="$target_dir" \
    ./scripts/package-release.sh >"${LOG_DIR}/package-$1.log" 2>&1; then
    cat "${LOG_DIR}/package-$1.log" >&2
    return 1
  fi

  local archive
  local sbom
  local checksums
  local evidence
  archive="$(find "$dist_dir" -maxdepth 1 -name 'marketfeed-v*.tar.gz' -print -quit)"
  sbom="$(find "$dist_dir" -maxdepth 1 -name 'marketfeed-v*.cdx.json' -print -quit)"
  checksums="$(find "$dist_dir" -maxdepth 1 -name 'SHA256SUMS-*' -print -quit)"
  evidence="$(find "$dist_dir" -maxdepth 1 -name 'EVIDENCE-MANIFEST-*.txt' -print -quit)"
  [[ -s "$archive" && -s "$sbom" && -s "$checksums" && -s "$evidence" ]] || {
    echo "release package did not produce archive, SBOM, checksums, and evidence" >&2
    return 1
  }

  shasum -a 256 \
    "$target_dir/release/marketfeed" \
    "$archive" \
    "$sbom" \
    "$checksums" \
    "$evidence" |
    awk '{print $1}' >"${LOG_DIR}/hashes-$1.txt"
}

run_package first
sleep 2
run_package second

if ! cmp -s "${LOG_DIR}/hashes-first.txt" "${LOG_DIR}/hashes-second.txt"; then
  echo "release package is not reproducible across two builds" >&2
  diff -u "${LOG_DIR}/hashes-first.txt" "${LOG_DIR}/hashes-second.txt" || true
  exit 1
fi

echo "package-release-reproducibility-test: PASS"
