#!/usr/bin/env bash
# Bounded laptop RSS soak (synthetic and/or live).
#
# Writes RSS CSV + metrics snapshots under .local/evidence/soak/runs/.
# Checked-in result summaries are updated deliberately after review.
#
# HONESTY:
#   - Bounded duration only (default 30m; override via DURATION or SOAK_SECS).
#   - This is NOT a multi-day live soak.
#   - Does NOT unlock stable / Spec §3.7.
#   - Does not write operator logs into the public source tree.
#
# Duration (pick one):
#   DURATION=30m|1h|2h|4h|8h|90m|3600|7200  # presets + secs; 7200/2h = optional operator run
#   SOAK_SECS=1800                      # raw seconds when DURATION unset
#
# Usage:
#   ./scripts/laptop_soak.sh                              # synthetic, 30m
#   DURATION=1h ./scripts/laptop_soak.sh                  # synthetic 60m
#   DURATION=2h ./scripts/laptop_soak.sh                  # optional 2h (alias DURATION=7200)
#   MODE=live DURATION=30m ./scripts/laptop_soak.sh       # live binance+okx
#   MODE=live SOAK_SECS=600 ./scripts/laptop_soak.sh      # live, raw secs
#   DRY_RUN=1 DURATION=2h ./scripts/laptop_soak.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Parse DURATION → seconds. Accepts Ns / Nm / Nh / Nd or bare integer seconds.
duration_to_secs() {
  local raw="$1"
  local n unit
  if [[ "$raw" =~ ^([0-9]+)([smhd])$ ]]; then
    n="${BASH_REMATCH[1]}"
    unit="${BASH_REMATCH[2]}"
    case "$unit" in
      s) echo "$n" ;;
      m) echo $((n * 60)) ;;
      h) echo $((n * 3600)) ;;
      d) echo $((n * 86400)) ;;
      *) return 1 ;;
    esac
  elif [[ "$raw" =~ ^[0-9]+$ ]]; then
    echo "$raw"
  else
    echo "DURATION must be Ns/Nm/Nh/Nd or integer seconds (got: $raw)" >&2
    return 2
  fi
}

# Duration self-check (ponytail: fails if parse/presets regress).
if [[ "${SELF_CHECK:-0}" == "1" ]]; then
  fail=0
  expect() {
    local got want
    got="$(duration_to_secs "$1")" || { echo "FAIL parse $1"; fail=1; return; }
    want="$2"
    if [[ "$got" != "$want" ]]; then
      echo "FAIL $1 → $got want $want"
      fail=1
    fi
  }
  expect 30m 1800
  expect 1h 3600
  expect 2h 7200
  expect 90m 5400
  expect 3600 3600
  if (( fail )); then exit 1; fi
  echo "SELF_CHECK ok (30m/1h/2h/90m/secs)"
  exit 0
fi

MODE="${MODE:-synthetic}" # synthetic | live
# DURATION wins over ambient SOAK_SECS (laptop shells often leave SOAK_SECS exported).
if [[ -n "${DURATION:-}" ]]; then
  SOAK_SECS="$(duration_to_secs "$DURATION")"
  HOLD_LABEL="$DURATION"
elif [[ -n "${SOAK_SECS:-}" ]]; then
  HOLD_LABEL="${SOAK_SECS}s"
else
  SOAK_SECS=1800 # 30m default
  HOLD_LABEL="30m"
fi
# Reject nonsense holds (min 60s; max soft-cap 8h — still not multi-day).
if (( SOAK_SECS < 60 )); then
  echo "SOAK_SECS/DURATION too short (${SOAK_SECS}s); min 60s" >&2
  exit 2
fi
if (( SOAK_SECS > 28800 )); then
  echo "SOAK_SECS/DURATION > 8h (${SOAK_SECS}s) — use multi-day OPS soak, not this script" >&2
  exit 2
fi

RSS_INTERVAL_SECS="${RSS_INTERVAL_SECS:-30}"
READY_TIMEOUT_SECS="${READY_TIMEOUT_SECS:-120}"
DRY_RUN="${DRY_RUN:-0}"
BIND_HOST="${MARKETFEED_BIND_HOST:-127.0.0.1}"
BIND_PORT="${MARKETFEED_BIND_PORT:-19290}"
ADDR="${BIND_HOST}:${BIND_PORT}"

EVIDENCE_DIR="${EVIDENCE_DIR:-.local/evidence/soak}"
TIP="$(git rev-parse --short HEAD)"
START_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RUN_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_DIR="${EVIDENCE_DIR}/runs/${MODE}_${RUN_STAMP}"

case "$MODE" in
  synthetic)
    CONFIG="${MARKETFEED_OFFLINE_CONFIG:-crates/daemon/config.offline.toml}"
    ;;
  live)
    CONFIG="${MARKETFEED_LIVE_CONFIG:-crates/daemon/config.example.toml}"
    ;;
  *)
    echo "MODE must be synthetic or live (got: $MODE)" >&2
    exit 2
    ;;
esac

echo "=== laptop_soak (NOT multi-day / NOT stable) ==="
echo "mode=${MODE} tip=${TIP} start=${START_UTC} soak=${SOAK_SECS}s (${HOLD_LABEL}) interval=${RSS_INTERVAL_SECS}s"
echo "config=${CONFIG} bind=${ADDR} out=${RUN_DIR}"

if [[ "$DRY_RUN" == "1" ]]; then
  echo "DRY_RUN=1 — would run bounded ${MODE} soak for ${SOAK_SECS}s (${HOLD_LABEL})"
  exit 0
fi

if [[ ! -f "$CONFIG" ]]; then
  echo "missing config: $CONFIG" >&2
  exit 1
fi

mkdir -p "$RUN_DIR"
echo "$START_UTC" >"${RUN_DIR}/start_utc.txt"
echo "$TIP" >"${RUN_DIR}/tip.txt"
cp "$CONFIG" "${RUN_DIR}/config.source.toml"

TMP_CFG="$(mktemp)"
CHILD_PID=
cleanup() {
  rm -f "$TMP_CFG"
  if [[ -n "${CHILD_PID}" ]] && kill -0 "$CHILD_PID" 2>/dev/null; then
    kill -TERM "$CHILD_PID" 2>/dev/null || true
    wait "$CHILD_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

awk -v host="$BIND_HOST" -v port="$BIND_PORT" '
  /^bind[[:space:]]*=/ { print "bind = \"" host ":" port "\""; next }
  { print }
' "$CONFIG" >"$TMP_CFG"
cp "$TMP_CFG" "${RUN_DIR}/config.runtime.toml"

echo "starting marketfeed-daemon mode=${MODE}"
cargo run --locked -q -p marketfeed-daemon -- run --config "$TMP_CFG" \
  >"${RUN_DIR}/daemon.log" 2>&1 &
CHILD_PID=$!

http_code() {
  local path="$1"
  curl -sS -o /dev/null -w '%{http_code}' --max-time 1 "http://${ADDR}${path}" 2>/dev/null || echo "000"
}

metric_val() {
  local name="$1"
  local blob="$2"
  printf '%s\n' "$blob" | awk -v n="$name" '$1 == n { print $2; found=1 } END { if (!found) print "0" }'
}

deadline=$((SECONDS + READY_TIMEOUT_SECS))
while true; do
  live="$(http_code /live)"
  ready="$(http_code /ready)"
  if [[ "$live" == "200" && "$ready" == "200" ]]; then
    echo "health ok: /live=$live /ready=$ready"
    date -u +%Y-%m-%dT%H:%M:%SZ >"${RUN_DIR}/ready_utc.txt"
    break
  fi
  if ! kill -0 "$CHILD_PID" 2>/dev/null; then
    echo "daemon exited before ready (live=$live ready=$ready)" >&2
    wait "$CHILD_PID" || true
    exit 1
  fi
  if (( SECONDS >= deadline )); then
    echo "timed out waiting for /live+/ready (live=$live ready=$ready)" >&2
    exit 1
  fi
  sleep 0.2
done

curl -sS --max-time 2 "http://${ADDR}/metrics" >"${RUN_DIR}/metrics_start.txt" || true

RSS_CSV="${RUN_DIR}/rss_samples.csv"
echo "utc,rss_kib,live,ready,frames,dispatched,dropped,overflows,live_sessions" >"$RSS_CSV"

rss_kb() {
  ps -o rss= -p "$CHILD_PID" 2>/dev/null | tr -d ' ' || echo "?"
}

sample_once() {
  local utc kb live ready metrics frames dispatched dropped overflows sessions
  utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  kb="$(rss_kb)"
  live="$(http_code /live)"
  ready="$(http_code /ready)"
  metrics="$(curl -sS --max-time 2 "http://${ADDR}/metrics" || true)"
  frames="$(metric_val marketfeed_frames_received_total "$metrics")"
  dispatched="$(metric_val marketfeed_events_dispatched_total "$metrics")"
  dropped="$(metric_val marketfeed_events_dropped_total "$metrics")"
  overflows="$(metric_val marketfeed_queue_overflows_total "$metrics")"
  sessions="$(metric_val marketfeed_live_sessions "$metrics")"
  echo "${utc},${kb},${live},${ready},${frames},${dispatched},${dropped},${overflows},${sessions}" >>"$RSS_CSV"
  echo "rss_sample t=${SECONDS}s rss_kib=${kb} live=${live} ready=${ready} frames=${frames} dropped=${dropped}"
  if [[ "$live" != "200" || "$ready" != "200" ]]; then
    echo "health dropped during soak: /live=$live /ready=$ready" >&2
    return 1
  fi
}

echo "soaking ${SOAK_SECS}s (${MODE}; NOT multi-day)"
end=$((SECONDS + SOAK_SECS))
sample_once
next_rss=$SECONDS
while (( SECONDS < end )); do
  if ! kill -0 "$CHILD_PID" 2>/dev/null; then
    echo "daemon exited during soak" >&2
    wait "$CHILD_PID" || true
    exit 1
  fi
  if (( SECONDS >= next_rss + RSS_INTERVAL_SECS )); then
    sample_once
    next_rss=$SECONDS
  fi
  sleep 0.5
done
sample_once

curl -sS --max-time 2 "http://${ADDR}/metrics" >"${RUN_DIR}/metrics_pre_stop.txt" || true

echo "sending SIGTERM for graceful drain"
kill -TERM "$CHILD_PID"
wait "$CHILD_PID"
CHILD_PID=
STOP_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "$STOP_UTC" >"${RUN_DIR}/stop_utc.txt"

echo "=== laptop_soak done mode=${MODE} secs=${SOAK_SECS} ==="
echo "archived ${RUN_DIR}/"
echo "HONESTY: not multi-day; not stable."
