#!/usr/bin/env bash
# Offline daemon E2E / mini-soak: synthetic venue only (no exchange I/O).
# Usage:
#   ./scripts/offline_daemon_e2e.sh              # ready check + graceful stop
#   SOAK_SECS=30 ./scripts/offline_daemon_e2e.sh # hold ready for N seconds
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CONFIG="${MARKETFEED_OFFLINE_CONFIG:-crates/daemon/config.offline.toml}"
BIND_HOST="${MARKETFEED_BIND_HOST:-127.0.0.1}"
BIND_PORT="${MARKETFEED_BIND_PORT:-19108}"
ADDR="${BIND_HOST}:${BIND_PORT}"
SOAK_SECS="${SOAK_SECS:-2}"
READY_TIMEOUT_SECS="${READY_TIMEOUT_SECS:-60}"
# Optional RSS sample interval (seconds). When set, prints rss_kb= every interval.
RSS_INTERVAL_SECS="${RSS_INTERVAL_SECS:-0}"
RSS_LOG="${RSS_LOG:-}"

if [[ ! -f "$CONFIG" ]]; then
  echo "missing config: $CONFIG" >&2
  exit 1
fi

TMP_CFG="$(mktemp)"
trap 'rm -f "$TMP_CFG"; if [[ -n "${CHILD_PID:-}" ]] && kill -0 "$CHILD_PID" 2>/dev/null; then kill -TERM "$CHILD_PID" 2>/dev/null || true; wait "$CHILD_PID" 2>/dev/null || true; fi' EXIT

# Rewrite bind so parallel CI/local runs can override the port.
awk -v host="$BIND_HOST" -v port="$BIND_PORT" '
  /^bind[[:space:]]*=/ { print "bind = \"" host ":" port "\""; next }
  { print }
' "$CONFIG" >"$TMP_CFG"

echo "starting marketfeed run (offline) config=$TMP_CFG bind=$ADDR soak=${SOAK_SECS}s"
cargo run --locked -q -p marketfeed-daemon -- run --config "$TMP_CFG" &
CHILD_PID=$!

http_code() {
  local path="$1"
  curl -sS -o /dev/null -w '%{http_code}' --max-time 1 "http://${ADDR}${path}" 2>/dev/null || echo "000"
}

deadline=$((SECONDS + READY_TIMEOUT_SECS))
while true; do
  live="$(http_code /live)"
  ready="$(http_code /ready)"
  if [[ "$live" == "200" && "$ready" == "200" ]]; then
    echo "health ok: /live=$live /ready=$ready"
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

metrics="$(curl -sS --max-time 2 "http://${ADDR}/metrics" || true)"
grep -q 'marketfeed_ready 1' <<<"$metrics"
grep -q 'marketfeed_live_sessions 1' <<<"$metrics"
echo "metrics gate ok"

rss_kb() {
  # macOS `ps -o rss=` is KiB; Linux same for RSS column.
  ps -o rss= -p "$CHILD_PID" 2>/dev/null | tr -d ' ' || echo "?"
}

if (( SOAK_SECS > 0 )); then
  echo "soaking ${SOAK_SECS}s (offline synthetic)"
  end=$((SECONDS + SOAK_SECS))
  next_rss=$SECONDS
  sample_rss() {
    local kb elapsed
    kb="$(rss_kb)"
    elapsed=$((SECONDS))
    line="t=${elapsed}s rss_kb=${kb} /live=$(http_code /live) /ready=$(http_code /ready)"
    echo "rss_sample $line"
    if [[ -n "$RSS_LOG" ]]; then
      echo "$line" >>"$RSS_LOG"
    fi
  }
  if (( RSS_INTERVAL_SECS > 0 )); then
    sample_rss
  fi
  while (( SECONDS < end )); do
    if ! kill -0 "$CHILD_PID" 2>/dev/null; then
      echo "daemon exited during soak" >&2
      wait "$CHILD_PID" || true
      exit 1
    fi
    live="$(http_code /live)"
    ready="$(http_code /ready)"
    if [[ "$live" != "200" || "$ready" != "200" ]]; then
      echo "health dropped during soak: /live=$live /ready=$ready" >&2
      exit 1
    fi
    if (( RSS_INTERVAL_SECS > 0 )) && (( SECONDS >= next_rss + RSS_INTERVAL_SECS )); then
      sample_rss
      next_rss=$SECONDS
    fi
    sleep 0.5
  done
  if (( RSS_INTERVAL_SECS > 0 )); then
    sample_rss
  fi
fi

echo "sending SIGTERM for graceful drain"
kill -TERM "$CHILD_PID"
wait "$CHILD_PID"
echo "offline daemon e2e passed (clean exit)"
CHILD_PID=
