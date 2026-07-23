#!/usr/bin/env bash
# Laptop private user-data canary (OKX / Bybit Spot).
#
# Archives local evidence under .local/evidence/private-canary/runs/cycle_N/.
# Checked-in result summaries are updated deliberately after review.
#
# HONESTY:
#   - Library live_ignored only — no order placement.
#   - Binance is blocked pending authenticated WebSocket API migration.
#   - Secrets from env / optional .env (never commit .env).
#   - Missing keys → SKIP that venue (clean exit); not a failure.
#   - Not scheduled canary. Does not unlock beta / maturity.
#
# Usage:
#   ./scripts/laptop_private_canary.sh
#   INCLUDE_EXTENDED=1 ./scripts/laptop_private_canary.sh
#   INCLUDE_REAUTH=1 ./scripts/laptop_private_canary.sh
#   DRY_RUN=1 ./scripts/laptop_private_canary.sh
#   PRIVATE_LIVE_SECS=10 ./scripts/laptop_private_canary.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

INCLUDE_EXTENDED="${INCLUDE_EXTENDED:-0}"
INCLUDE_REAUTH="${INCLUDE_REAUTH:-0}"
DRY_RUN="${DRY_RUN:-0}"
AUTO_SOURCE_ENV="${AUTO_SOURCE_ENV:-1}"
EVIDENCE_ROOT="${EVIDENCE_ROOT:-.local/evidence/private-canary/runs}"
TIP="$(git rev-parse --short HEAD)"
START_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

if [[ "$AUTO_SOURCE_ENV" == "1" && -f .env ]]; then
  # ponytail: source local .env when present; never echo values.
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

next_cycle() {
  local max=0 n
  shopt -s nullglob
  for d in "${EVIDENCE_ROOT}"/cycle_*; do
    n="${d##*/cycle_}"
    if [[ "$n" =~ ^[0-9]+$ ]] && (( n > max )); then
      max=$n
    fi
  done
  echo $((max + 1))
}

has_okx() {
  [[ -n "${OKX_API_KEY:-}" && -n "${OKX_API_SECRET:-}" && -n "${OKX_API_PASSPHRASE:-}" ]]
}
has_bybit() { [[ -n "${BYBIT_API_KEY:-}" && -n "${BYBIT_API_SECRET:-}" ]]; }

echo "=== laptop_private_canary (NOT scheduled / NOT beta) ==="
echo "tip=${TIP} start=${START_UTC}"
echo "binance=BLOCKED_PROTOCOL_MIGRATION okx_keys=$(has_okx && echo set || echo missing) bybit_keys=$(has_bybit && echo set || echo missing)"
echo "extended=${INCLUDE_EXTENDED} reauth=${INCLUDE_REAUTH} private_live_secs=${PRIVATE_LIVE_SECS:-5}"
echo "no_order_placement=1 maturity=alpha (no promotion from this script)"

if [[ "$DRY_RUN" == "1" ]]; then
  echo "DRY_RUN=1 — would run private live_ignored for venues with keys set"
  exit 0
fi

if ! has_okx && ! has_bybit; then
  echo "SKIP: no private API keys in env (see .env.example). Clean exit."
  exit 0
fi

CYCLE="$(next_cycle)"
OUT="${EVIDENCE_ROOT}/cycle_${CYCLE}"
mkdir -p "$OUT"
echo "$START_UTC" >"${OUT}/start_utc.txt"
echo "$TIP" >"${OUT}/tip.txt"

run_filters() {
  local log="$1"
  shift
  echo "--- cargo test -p marketfeed-private --features live --test live_ignored $* ---"
  set +e
  cargo test -p marketfeed-private --features live --test live_ignored -- --ignored --nocapture "$@" \
    > >(tee "${OUT}/${log}") 2>&1
  local ec=$?
  set -e
  echo "${ec}" >"${OUT}/${log%.log}_ec.txt"
  return "$ec"
}

BINANCE_STATUS="BLOCKED_PROTOCOL_MIGRATION"
OKX_STATUS="SKIP"
BYBIT_STATUS="SKIP"
BINANCE_EC=0
OKX_EC=0
BYBIT_EC=0
RAN=0
FAIL=0

echo "BLOCKED binance: migrate to authenticated WebSocket API userDataStream.subscribe.signature" \
  | tee "${OUT}/binance_blocked.txt"

if has_okx; then
  RAN=1
  FILTERS=(live_okx_private_login_and_ws)
  if [[ "$INCLUDE_EXTENDED" == "1" ]]; then
    FILTERS+=(live_okx_private_extended)
  fi
  if [[ "$INCLUDE_REAUTH" == "1" ]]; then
    FILTERS+=(live_okx_private_reauth_probe)
  fi
  OKX_EC=0
  run_filters okx_private.log "${FILTERS[@]}" || OKX_EC=$?
  if (( OKX_EC == 0 )); then
    OKX_STATUS="PASS"
  else
    OKX_STATUS="FAIL"
    FAIL=1
  fi
else
  echo "SKIP okx: OKX_API_KEY/SECRET/PASSPHRASE unset" | tee "${OUT}/okx_skip.txt"
fi

if has_bybit; then
  RAN=1
  FILTERS=(live_bybit_private_auth_and_ws)
  if [[ "$INCLUDE_EXTENDED" == "1" ]]; then
    FILTERS+=(live_bybit_private_extended)
  fi
  if [[ "$INCLUDE_REAUTH" == "1" ]]; then
    FILTERS+=(live_bybit_private_reauth_probe)
  fi
  BYBIT_EC=0
  run_filters bybit_private.log "${FILTERS[@]}" || BYBIT_EC=$?
  if (( BYBIT_EC == 0 )); then
    BYBIT_STATUS="PASS"
  else
    BYBIT_STATUS="FAIL"
    FAIL=1
  fi
else
  echo "SKIP bybit: BYBIT_API_KEY/SECRET unset" | tee "${OUT}/bybit_skip.txt"
fi

END_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "$END_UTC" >"${OUT}/end_utc.txt"

OVERALL="PASS"
if (( FAIL != 0 )); then
  OVERALL="FAIL"
elif (( RAN == 0 )); then
  OVERALL="SKIP"
fi

{
  echo "tip=${TIP}"
  echo "start=${START_UTC}"
  echo "end=${END_UTC}"
  echo "binance=${BINANCE_STATUS} ec=${BINANCE_EC}"
  echo "okx=${OKX_STATUS} ec=${OKX_EC}"
  echo "bybit=${BYBIT_STATUS} ec=${BYBIT_EC}"
  echo "include_extended=${INCLUDE_EXTENDED}"
  echo "include_reauth=${INCLUDE_REAUTH}"
  echo "no_order_placement=1"
  echo "scheduled_canary=0"
  echo "maturity=alpha (no promotion; laptop private ≠ beta)"
  echo "overall=${OVERALL}"
  echo "notes=scripts/laptop_private_canary.sh; secrets env-only"
} >"${OUT}/result.txt"

echo "=== laptop_private_canary done overall=${OVERALL} cycle=${CYCLE} ==="
echo "archived ${OUT}/"
echo "HONESTY: not scheduled; not beta; no order placement."
if [[ "$OVERALL" == "FAIL" ]]; then
  exit 1
fi
exit 0
