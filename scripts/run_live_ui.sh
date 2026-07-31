#!/usr/bin/env bash
# Run the live market panel (marketfeed-daemon --features ui).
#
# Usage:
#   ./scripts/run_live_ui.sh                 # foreground (Ctrl+C to stop)
#   ./scripts/run_live_ui.sh --background    # detach; pid/log under .local/live-ui/
#   ./scripts/run_live_ui.sh --rebuild       # always cargo build --features ui
#   ./scripts/run_live_ui.sh --force         # kill stale listeners on bind ports
#   ./scripts/run_live_ui.sh --help
#
# Config: prefers .local/live-ui/config.live.ui.toml; creates it from
# crates/daemon/config.live.ui.example.toml when missing.
#
# Requires network for live public venues. No secrets / credentials.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CONFIG_DIR=".local/live-ui"
CONFIG="${MARKETFEED_LIVE_UI_CONFIG:-${CONFIG_DIR}/config.live.ui.toml}"
EXAMPLE="crates/daemon/config.live.ui.example.toml"
BIN="${MARKETFEED_BIN:-target/debug/marketfeed}"
PID_FILE="${CONFIG_DIR}/daemon.pid"
LOG_FILE="${CONFIG_DIR}/daemon.log"
BIND_HOST="${MARKETFEED_BIND_HOST:-127.0.0.1}"
BIND_PORT="${MARKETFEED_BIND_PORT:-19108}"
UI_PORT="${MARKETFEED_UI_PORT:-19109}"
PANEL_URL="http://${BIND_HOST}:${UI_PORT}/?asset=BTC&mode=lines&dock=1"

REBUILD=0
BACKGROUND=0
FORCE=0

usage() {
  cat <<EOF
Run the cryptofeed-rs live market panel (daemon with --features ui).

Usage:
  $(basename "$0") [options]

Options:
  --background   Start in background (logs: ${LOG_FILE}, pid: ${PID_FILE})
  --rebuild      Always rebuild marketfeed-daemon --features ui
  --force        Kill any process listening on ${BIND_PORT}/${UI_PORT} first
  -h, --help     Show this help

Environment:
  MARKETFEED_LIVE_UI_CONFIG   Config path (default: ${CONFIG_DIR}/config.live.ui.toml)
  MARKETFEED_BIN              Binary path (default: target/debug/marketfeed)
  MARKETFEED_BIND_HOST/PORT   Telemetry bind (default: ${BIND_HOST}:${BIND_PORT})
  MARKETFEED_UI_PORT          UI / SPA bind port (default: ${UI_PORT})

Open after start:
  ${PANEL_URL}

Stop background:
  kill \$(cat ${PID_FILE})
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --background) BACKGROUND=1 ;;
    --rebuild) REBUILD=1 ;;
    --force) FORCE=1 ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1 (try --help)" >&2
      exit 2
      ;;
  esac
  shift
done

port_pids() {
  local port="$1"
  # macOS / BSD lsof; empty when free.
  lsof -nP -iTCP:"${port}" -sTCP:LISTEN -t 2>/dev/null || true
}

ensure_ports_free() {
  local port pids
  for port in "$BIND_PORT" "$UI_PORT"; do
    pids="$(port_pids "$port" | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
    if [[ -z "$pids" ]]; then
      continue
    fi
    if [[ "$FORCE" -eq 1 ]]; then
      echo "port ${port} in use by pid(s) ${pids} — killing (--force)"
      # shellcheck disable=SC2086
      kill $pids 2>/dev/null || true
      sleep 0.5
      pids="$(port_pids "$port" | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
      if [[ -n "$pids" ]]; then
        echo "port ${port} still held by ${pids}; try: kill -9 ${pids}" >&2
        exit 1
      fi
    else
      echo "port ${port} already in use by pid(s): ${pids}" >&2
      echo "Stop that process, or re-run with --force to replace it." >&2
      echo "  lsof -nP -iTCP:${port} -sTCP:LISTEN" >&2
      exit 1
    fi
  done
}

ensure_config() {
  mkdir -p "$CONFIG_DIR" "${CONFIG_DIR}/raw"
  if [[ -f "$CONFIG" ]]; then
    echo "config: $CONFIG"
    return
  fi
  if [[ ! -f "$EXAMPLE" ]]; then
    echo "missing example config: $EXAMPLE" >&2
    exit 1
  fi
  cp "$EXAMPLE" "$CONFIG"
  echo "created config from example: $CONFIG"
  echo "  (edit venues/symbols locally; .local/ is gitignored)"
}

ensure_binary() {
  if [[ "$REBUILD" -eq 1 || ! -x "$BIN" ]]; then
    if [[ "$REBUILD" -eq 1 ]]; then
      echo "rebuilding marketfeed-daemon --features ui …"
    else
      echo "binary missing ($BIN) — building marketfeed-daemon --features ui …"
    fi
    cargo build --locked -p marketfeed-daemon --features ui
  else
    echo "binary: $BIN (pass --rebuild to force cargo build)"
  fi
  if [[ ! -x "$BIN" ]]; then
    echo "build succeeded but binary not found at $BIN" >&2
    exit 1
  fi
}

ensure_config
ensure_binary
ensure_ports_free

echo ""
echo "=== live market panel ==="
echo "  telemetry: http://${BIND_HOST}:${BIND_PORT}/live"
echo "  panel:     ${PANEL_URL}"
echo "  config:    ${CONFIG}"
echo ""

if [[ "$BACKGROUND" -eq 1 ]]; then
  # Fresh log for this session.
  : >"$LOG_FILE"
  nohup "$BIN" run --config "$CONFIG" >>"$LOG_FILE" 2>&1 &
  echo $! >"$PID_FILE"
  echo "started in background pid=$(cat "$PID_FILE")"
  echo "  log:  $LOG_FILE"
  echo "  stop: kill \$(cat $PID_FILE)"
  echo ""
  echo "Open: ${PANEL_URL}"
  exit 0
fi

echo "Running in foreground — Ctrl+C to stop."
echo "Open: ${PANEL_URL}"
echo ""
exec "$BIN" run --config "$CONFIG"
