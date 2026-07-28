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
UI_PORT="${MARKETFEED_UI_PORT:-19109}"
ADDR="${BIND_HOST}:${BIND_PORT}"
UI_ADDR="${BIND_HOST}:${UI_PORT}"
SOAK_SECS="${SOAK_SECS:-2}"
READY_TIMEOUT_SECS="${READY_TIMEOUT_SECS:-60}"
# Optional RSS sample interval (seconds). When set, prints rss_kb= every interval.
RSS_INTERVAL_SECS="${RSS_INTERVAL_SECS:-0}"
RSS_LOG="${RSS_LOG:-}"
# Optional cargo features, e.g. FEATURES=ui for SPA + /v1 checks.
FEATURES="${FEATURES:-}"
FEATURE_ARGS=()
if [[ -n "$FEATURES" ]]; then
  FEATURE_ARGS=(--features "$FEATURES")
fi

if [[ ! -f "$CONFIG" ]]; then
  echo "missing config: $CONFIG" >&2
  exit 1
fi

TMP_CFG="$(mktemp)"
trap 'rm -f "$TMP_CFG"; if [[ -n "${CHILD_PID:-}" ]] && kill -0 "$CHILD_PID" 2>/dev/null; then kill -TERM "$CHILD_PID" 2>/dev/null || true; wait "$CHILD_PID" 2>/dev/null || true; fi' EXIT

# Rewrite bind / ui_bind so parallel CI/local runs can override the port.
awk -v host="$BIND_HOST" -v port="$BIND_PORT" -v uiport="$UI_PORT" '
  /^bind[[:space:]]*=/ { print "bind = \"" host ":" port "\""; next }
  /^ui_bind[[:space:]]*=/ { print "ui_bind = \"" host ":" uiport "\""; next }
  { print }
' "$CONFIG" >"$TMP_CFG"

echo "starting marketfeed run (offline) config=$TMP_CFG bind=$ADDR ui=$UI_ADDR soak=${SOAK_SECS}s features=${FEATURES:-none}"
cargo run -q -p marketfeed-daemon "${FEATURE_ARGS[@]}" -- run --config "$TMP_CFG" &
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
echo "$metrics" | grep -q 'marketfeed_ready 1'
echo "$metrics" | grep -q 'marketfeed_live_sessions 1'
echo "metrics gate ok"

if [[ "$FEATURES" == *"ui"* ]]; then
  ui_http() {
    local path="$1"
    curl -sS -o /tmp/mf_ui_body.$$ -w '%{http_code}' --max-time 2 "http://${UI_ADDR}${path}" 2>/dev/null || echo "000"
  }
  deadline=$((SECONDS + 10))
  while true; do
    st="$(ui_http /v1/status)"
    if [[ "$st" == "200" ]]; then
      break
    fi
    if (( SECONDS >= deadline )); then
      echo "timed out waiting for /v1/status on ${UI_ADDR} (got $st)" >&2
      exit 1
    fi
    sleep 0.2
  done
  grep -q '"live":true' /tmp/mf_ui_body.$$
  echo "view /v1/status ok"

  deadline=$((SECONDS + 10))
  while true; do
    st="$(ui_http '/v1/tape?venue=synthetic-demo&symbol=BTC-USD&limit=10')"
    if [[ "$st" == "200" ]] && grep -q '"kind":"trade"' /tmp/mf_ui_body.$$; then
      break
    fi
    if (( SECONDS >= deadline )); then
      echo "timed out waiting for non-empty /v1/tape (status=$st body=$(head -c 200 /tmp/mf_ui_body.$$))" >&2
      exit 1
    fi
    sleep 0.2
  done
  echo "view /v1/tape ok"

  deadline=$((SECONDS + 10))
  while true; do
    st="$(ui_http '/v1/books?venue=synthetic-demo&symbol=BTC-USD&depth=5')"
    if [[ "$st" == "200" ]] && grep -q '"bids"' /tmp/mf_ui_body.$$; then
      break
    fi
    if (( SECONDS >= deadline )); then
      echo "timed out waiting for /v1/books (status=$st)" >&2
      exit 1
    fi
    sleep 0.2
  done
  echo "view /v1/books ok"

  # SPA is only embedded with feature `ui` (not bare `ui-api`).
  HAS_SPA=0
  IFS=',' read -ra _feats <<< "$FEATURES"
  for _f in "${_feats[@]}"; do
    if [[ "${_f}" == "ui" ]]; then HAS_SPA=1; fi
  done
  if (( HAS_SPA )); then
    st="$(ui_http /)"
    [[ "$st" == "200" ]] || { echo "SPA / expected 200 got $st" >&2; exit 1; }
    st="$(ui_http /assets/app.js)"
    [[ "$st" == "200" ]] || { echo "SPA /assets/app.js expected 200 got $st" >&2; exit 1; }
    echo "SPA assets ok"
  fi
  rm -f /tmp/mf_ui_body.$$
fi

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
