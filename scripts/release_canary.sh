#!/usr/bin/env bash
# Build and qualify the exact UI-enabled release binary on public, read-only feeds.
# Evidence is written under ignored .local/evidence/release-canary/runs/.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

duration_seconds() {
  local raw="$1" number unit
  if ! [[ "$raw" =~ ^([0-9]+)([smh]?)$ ]]; then
    return 2
  fi
  number="${BASH_REMATCH[1]}"
  unit="${BASH_REMATCH[2]}"
  case "$unit" in
    s|'') echo "$number" ;;
    m) echo $((number * 60)) ;;
    h) echo $((number * 3600)) ;;
  esac
}

if [[ "${1:-}" == "--self-check" ]]; then
  python3 -m unittest scripts/tests/test_release_canary.py
  [[ "$(duration_seconds 1h)" == "3600" ]]
  [[ "$(duration_seconds 90m)" == "5400" ]]
  [[ "$(duration_seconds 3600)" == "3600" ]]
  if duration_seconds '1+2h' >/dev/null 2>&1; then
    echo "unsafe duration accepted" >&2
    exit 1
  fi
  bash -n scripts/release_canary.sh
  echo "release_canary self-check ok"
  exit 0
fi

DURATION="${DURATION:-1h}"
if ! DURATION_SECONDS="$(duration_seconds "$DURATION")"; then
  echo "DURATION must be an integer with optional s/m/h suffix" >&2
  exit 2
fi

if ! [[ "$DURATION_SECONDS" =~ ^[0-9]+$ ]] || (( DURATION_SECONDS < 60 )); then
  echo "DURATION must resolve to at least 60 seconds" >&2
  exit 2
fi

CONFIG="${MARKETFEED_LIVE_UI_CONFIG:-.local/live-ui/config.live.ui.toml}"
BIN="${MARKETFEED_BIN:-target/release/marketfeed}"
SAMPLE_INTERVAL_SECONDS="${SAMPLE_INTERVAL_SECONDS:-15}"

echo "building UI-enabled release binary"
cargo build --locked --release -p marketfeed-daemon --features ui

exec python3 scripts/release_canary.py \
  --binary "$BIN" \
  --config "$CONFIG" \
  --duration-seconds "$DURATION_SECONDS" \
  --sample-interval-seconds "$SAMPLE_INTERVAL_SECONDS" \
  "$@"
