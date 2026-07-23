#!/usr/bin/env bash
# Local §24.2 regression helper for Binance parse_fixtures Instant harness.
#
# HONESTY — local evidence tool only:
#   - NOT a maturity gate (alpha / alpha+ unchanged).
#   - NOT a CI / Actions gate while billing is blocked (OPS-A).
#   - Laptop noise; do NOT publish ns/iter as SLOs.
#   - Pinned-runner statistical suite remains the upgrade path.
#
# Compares median ns/iter (N runs) to a checked-in baseline file and fails when
# any label is >THRESHOLD_PCT slower (default 10). With --simd, also runs the
# simd-json Instant path and can gate serde-vs-simd medians (--simd-vs-serde).
#
# Usage:
#   ./scripts/parse_fixtures_gate.sh
#   ./scripts/parse_fixtures_gate.sh --simd
#   ./scripts/parse_fixtures_gate.sh --simd --simd-vs-serde
#   ./scripts/parse_fixtures_gate.sh --write-baseline
#   ./scripts/parse_fixtures_gate.sh --write-baseline --simd
#   ./scripts/parse_fixtures_gate.sh --self-check
#   RUNS=5 THRESHOLD_PCT=10 ./scripts/parse_fixtures_gate.sh
#
# Baseline: .local/evidence/parse_fixtures_baseline.txt
# Refresh on a new host (absolute ns/iter are host-local):
#   ./scripts/parse_fixtures_gate.sh --write-baseline
#
# # ponytail: bash 3.2 (macOS /bin/bash); no assoc arrays. Ceiling = laptop noise.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BASELINE_PATH="${BASELINE_PATH:-.local/evidence/parse_fixtures_baseline.txt}"
RUNS="${RUNS:-3}"
THRESHOLD_PCT="${THRESHOLD_PCT:-10}"
WRITE_BASELINE=0
WITH_SIMD=0
SIMD_VS_SERDE=0
SELF_CHECK=0

usage() {
  sed -n '2,28p' "$0" | sed 's/^# \?//'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --write-baseline) WRITE_BASELINE=1 ;;
    --simd) WITH_SIMD=1 ;;
    --simd-vs-serde) SIMD_VS_SERDE=1; WITH_SIMD=1 ;;
    --self-check) SELF_CHECK=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

# median of space-separated integers (odd or even: lower middle for even).
median_of() {
  local sorted n idx
  sorted=$(printf '%s\n' "$@" | sort -n)
  n=$(printf '%s\n' "$sorted" | grep -c . || true)
  if [[ -z "$n" || "$n" -eq 0 ]]; then
    echo "median_of: empty" >&2
    return 1
  fi
  idx=$(( (n - 1) / 2 ))
  printf '%s\n' "$sorted" | sed -n "$((idx + 1))p"
}

# fail if measured > baseline * (100+THRESHOLD_PCT)/100
regress_over() {
  local measured=$1 baseline=$2
  local allow=$(( (baseline * (100 + THRESHOLD_PCT) + 99) / 100 ))
  if [[ "$measured" -gt "$allow" ]]; then
    return 1
  fi
  return 0
}

# lookup label in TSV file (label<TAB>ns); prints ns or empty
tsv_get() {
  local file=$1 label=$2
  awk -F '\t' -v k="$label" '$1 == k { print $2; exit }' "$file"
}

self_check() {
  local m
  m=$(median_of 1 2 100)
  [[ "$m" == "2" ]] || { echo "self-check median odd fail: $m" >&2; exit 1; }
  m=$(median_of 10 20 30 40)
  [[ "$m" == "20" ]] || { echo "self-check median even fail: $m" >&2; exit 1; }
  THRESHOLD_PCT=10
  if ! regress_over 1100 1000; then
    echo "self-check 10% boundary fail (1100 vs 1000)" >&2
    exit 1
  fi
  if regress_over 1101 1000; then
    echo "self-check should fail at 1101 vs 1000 (+10%)" >&2
    exit 1
  fi
  if ! regress_over 900 1000; then
    echo "self-check faster path should pass" >&2
    exit 1
  fi
  echo "parse_fixtures_gate self-check ok"
}

if [[ "$SELF_CHECK" -eq 1 ]]; then
  self_check
  exit 0
fi

if ! [[ "$RUNS" =~ ^[1-9][0-9]*$ ]]; then
  echo "RUNS must be positive int, got: $RUNS" >&2
  exit 2
fi
if ! [[ "$THRESHOLD_PCT" =~ ^[0-9]+$ ]]; then
  echo "THRESHOLD_PCT must be non-neg int, got: $THRESHOLD_PCT" >&2
  exit 2
fi

run_harness() {
  local backend=$1
  if [[ "$backend" == "simd" ]]; then
    cargo bench -p marketfeed-adapter-binance --bench parse_fixtures --features simd-json
  else
    cargo bench -p marketfeed-adapter-binance --bench parse_fixtures
  fi
}

SAMPLES_DIR="$(mktemp -d "${TMPDIR:-/tmp}/parse_fixtures_gate.XXXXXX")"
cleanup() { rm -rf "$SAMPLES_DIR"; }
trap cleanup EXIT

# Sanitize label for filename (spaces -> __)
label_file() {
  local label=$1
  echo "$SAMPLES_DIR/$(printf '%s' "$label" | tr ' /' '__').samples"
}

record_samples() {
  local backend=$1
  local i out line label ns f
  i=1
  while [[ "$i" -le "$RUNS" ]]; do
    echo "run $i/$RUNS ($backend)…" >&2
    out=$(run_harness "$backend")
    while IFS= read -r line; do
      case "$line" in
        *': '*' ns/iter'*)
          label=${line%%:*}
          ns=${line#*: }
          ns=${ns%% ns/iter*}
          ns=${ns// /}
          if [[ "$ns" =~ ^[0-9]+$ ]]; then
            f=$(label_file "$label")
            printf '%s\n' "$ns" >>"$f"
            if [[ ! -f "$SAMPLES_DIR/labels.txt" ]] || ! grep -Fxq "$label" "$SAMPLES_DIR/labels.txt"; then
              printf '%s\n' "$label" >>"$SAMPLES_DIR/labels.txt"
            fi
          fi
          ;;
      esac
    done <<<"$out"
    i=$((i + 1))
  done
}

write_medians() {
  local label f med
  [[ -f "$SAMPLES_DIR/labels.txt" ]] || return 0
  while IFS= read -r label; do
    f=$(label_file "$label")
    [[ -f "$f" ]] || continue
    # shellcheck disable=SC2046
    med=$(median_of $(cat "$f"))
    printf '%s\t%s\n' "$label" "$med"
  done <"$SAMPLES_DIR/labels.txt"
}

echo "parse_fixtures_gate: RUNS=$RUNS THRESHOLD_PCT=$THRESHOLD_PCT simd=$WITH_SIMD" >&2

record_samples serde
if [[ "$WITH_SIMD" -eq 1 ]]; then
  record_samples simd
fi

MEDIAN_FILE="$SAMPLES_DIR/medians.tsv"
write_medians >"$MEDIAN_FILE"

if [[ ! -s "$MEDIAN_FILE" ]]; then
  echo "no timing lines parsed from harness output" >&2
  exit 1
fi

echo "medians (ns/iter):" >&2
sed 's/^/  /' "$MEDIAN_FILE" >&2

if [[ "$WRITE_BASELINE" -eq 1 ]]; then
  mkdir -p "$(dirname "$BASELINE_PATH")"
  {
    echo "# Local §24.2 parse_fixtures baseline (ns/iter medians)."
    echo "# Evidence only — not an SLO, not a maturity/CI gate (billing/OPS-A)."
    echo "# Refresh: ./scripts/parse_fixtures_gate.sh --write-baseline [--simd]"
    echo "# threshold_pct=$THRESHOLD_PCT runs=$RUNS"
    echo "# recorded_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ) tip=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
    echo "# host=$(uname -srm) rustc=$(rustc --version 2>/dev/null || echo unknown)"
    cat "$MEDIAN_FILE"
  } >"$BASELINE_PATH"
  echo "wrote $BASELINE_PATH" >&2
  exit 0
fi

if [[ ! -f "$BASELINE_PATH" ]]; then
  echo "missing baseline: $BASELINE_PATH" >&2
  echo "record one with: ./scripts/parse_fixtures_gate.sh --write-baseline" >&2
  exit 1
fi

BASE_TSV="$SAMPLES_DIR/baseline.tsv"
grep -v '^#' "$BASELINE_PATH" | grep -v '^[[:space:]]*$' >"$BASE_TSV" || true

fail=0

while IFS=$'\t' read -r label measured; do
  [[ -z "$label" ]] && continue
  baseline=$(tsv_get "$BASE_TSV" "$label")
  if [[ -z "$baseline" ]]; then
    echo "SKIP (no baseline): $label = $measured ns/iter" >&2
    continue
  fi
  allow=$(( (baseline * (100 + THRESHOLD_PCT) + 99) / 100 ))
  if regress_over "$measured" "$baseline"; then
    echo "OK  $label: $measured ns/iter (baseline $baseline, allow≤$allow)"
  else
    echo "FAIL $label: $measured ns/iter > allow $allow (baseline $baseline, +${THRESHOLD_PCT}%)" >&2
    fail=1
  fi
done <"$MEDIAN_FILE"

# Optional: within-run serde vs simd medians (same fixture family).
# Fails if simd median is >THRESHOLD_PCT slower than serde median for that fixture.
if [[ "$SIMD_VS_SERDE" -eq 1 ]]; then
  for family in binance_spot_l2_snapshot binance_usdm_l2_snapshot binance_coinm_l2_snapshot; do
    skey="$family serde"
    mkey="$family simd-json"
    serde_ns=$(tsv_get "$MEDIAN_FILE" "$skey")
    simd_ns=$(tsv_get "$MEDIAN_FILE" "$mkey")
    if [[ -z "$serde_ns" || -z "$simd_ns" ]]; then
      echo "FAIL simd-vs-serde: missing medians for $family" >&2
      fail=1
      continue
    fi
    allow=$(( (serde_ns * (100 + THRESHOLD_PCT) + 99) / 100 ))
    if [[ "$simd_ns" -gt "$allow" ]]; then
      echo "FAIL simd-vs-serde $family: simd $simd_ns > allow $allow (serde median $serde_ns, +${THRESHOLD_PCT}%)" >&2
      fail=1
    else
      echo "OK  simd-vs-serde $family: simd $simd_ns vs serde $serde_ns (allow≤$allow)"
    fi
  done
fi

if [[ "$fail" -eq 1 ]]; then
  echo "parse_fixtures_gate: REGRESSION (local evidence only; not CI/maturity)" >&2
  exit 1
fi

echo "parse_fixtures_gate: PASS (local evidence only; not CI/maturity)"
