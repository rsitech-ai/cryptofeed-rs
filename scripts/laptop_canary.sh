#!/usr/bin/env bash
# Laptop live canary runner (Binance Spot + OKX Spot).
#
# Archives local evidence under .local/evidence/canary/runs/cycle_N/.
# Checked-in result summaries are updated deliberately after review.
#
# HONESTY:
#   - This is a laptop / operator burst tool.
#   - It is NOT scheduled canary.yml.
#   - Laptop N/N ≠ beta. Scheduled canary remains 0 until OPS-A/B.
#
# Usage:
#   ./scripts/laptop_canary.sh
#   INCLUDE_RECONNECT=0 ./scripts/laptop_canary.sh   # skip reconnect probes
#   INCLUDE_ALPHA=1 ./scripts/laptop_canary.sh       # also venues 13–18 + 20 (not beta)
#   DRY_RUN=1 ./scripts/laptop_canary.sh             # print plan only
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

INCLUDE_RECONNECT="${INCLUDE_RECONNECT:-1}"
INCLUDE_ALPHA="${INCLUDE_ALPHA:-0}"  # VenueIds 13–18 + 20; still alpha, not beta
DRY_RUN="${DRY_RUN:-0}"
EVIDENCE_ROOT="${EVIDENCE_ROOT:-.local/evidence/canary/runs}"
TIP="$(git rev-parse --short HEAD)"
START_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

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

CYCLE="$(next_cycle)"
OUT="${EVIDENCE_ROOT}/cycle_${CYCLE}"

echo "=== laptop_canary (NOT scheduled beta) ==="
echo "tip=${TIP} start=${START_UTC} cycle=${CYCLE} out=${OUT}"
echo "scheduled_canary=0 maturity=alpha+ (no promotion from this script)"

if [[ "$DRY_RUN" == "1" ]]; then
  echo "DRY_RUN=1 — would run binance+okx live_ignored (+ reconnect=${INCLUDE_RECONNECT} alpha=${INCLUDE_ALPHA})"
  exit 0
fi

mkdir -p "$OUT"
echo "$START_UTC" >"${OUT}/start_utc.txt"
echo "$TIP" >"${OUT}/tip.txt"

run_pkg() {
  local pkg="$1"
  local log="$2"
  shift 2
  # ponytail: re-mkdir OUT — ceiling = concurrent checkout wipe; upgrade = lock OUT early.
  mkdir -p "$OUT"
  echo "--- cargo test -p ${pkg} --test live_ignored $* ---"
  set +e
  cargo test -p "$pkg" --test live_ignored -- --ignored --nocapture "$@" \
    > >(tee "${OUT}/${log}") 2>&1
  local ec=$?
  set -e
  mkdir -p "$OUT"
  echo "${ec}" >"${OUT}/${log%.log}_ec.txt"
  return "$ec"
}

BINANCE_EC=0
OKX_EC=0

if [[ "$INCLUDE_RECONNECT" == "1" ]]; then
  run_pkg marketfeed-adapter-binance binance_live_ignored.log \
    live_binance_spot_trade_or_quote live_binance_spot_reconnect_probe || BINANCE_EC=$?
  run_pkg marketfeed-adapter-okx okx_live_ignored.log \
    live_okx_spot_trade_or_quote live_okx_spot_reconnect_probe || OKX_EC=$?
else
  run_pkg marketfeed-adapter-binance binance_live_ignored.log \
    live_binance_spot_trade_or_quote || BINANCE_EC=$?
  run_pkg marketfeed-adapter-okx okx_live_ignored.log \
    live_okx_spot_trade_or_quote || OKX_EC=$?
fi

ALPHA_EC=0
if [[ "$INCLUDE_ALPHA" == "1" ]]; then
  run_pkg marketfeed-adapter-kraken kraken_futures_live_ignored.log \
    live_kraken_futures_trade_or_ticker || ALPHA_EC=$?
  run_pkg marketfeed-adapter-bitstamp bitstamp_live_ignored.log \
    live_bitstamp_spot_trade_or_quote || ALPHA_EC=$?
  run_pkg marketfeed-adapter-gemini gemini_live_ignored.log \
    live_gemini_spot_trade_or_quote || ALPHA_EC=$?
  run_pkg marketfeed-adapter-coinbase coinbase_live_ignored.log \
    live_coinbase_spot_trade_or_quote || ALPHA_EC=$?
  # VenueId 17 Bitfinex + 18 Coinbase-adv + 20 bitfinex-deriv (still alpha)
  run_pkg marketfeed-adapter-bitfinex bitfinex_live_ignored.log \
    live_bitfinex_spot_trade_or_quote live_bitfinex_deriv_trade_or_mark || ALPHA_EC=$?
  run_pkg marketfeed-adapter-coinbase coinbase_adv_live_ignored.log \
    live_coinbase_adv_trade_or_quote live_coinbase_adv_l2 live_coinbase_adv_candle || ALPHA_EC=$?
fi

END_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "$END_UTC" >"${OUT}/end_utc.txt"

OVERALL="PASS"
if (( BINANCE_EC != 0 || OKX_EC != 0 || ALPHA_EC != 0 )); then
  OVERALL="FAIL"
fi

{
  echo "tip=${TIP}"
  echo "start=${START_UTC}"
  echo "end=${END_UTC}"
  echo "binance_ec=${BINANCE_EC}"
  echo "okx_ec=${OKX_EC}"
  echo "alpha_ec=${ALPHA_EC}"
  echo "include_alpha=${INCLUDE_ALPHA}"
  echo "reconnect_included=${INCLUDE_RECONNECT}"
  echo "private=SKIP (not run by this script)"
  echo "scheduled_canary=0"
  echo "maturity=alpha+ (no promotion; laptop ≠ scheduled)"
  echo "overall=${OVERALL}"
  echo "notes=scripts/laptop_canary.sh; NOT scheduled beta"
} >"${OUT}/result.txt"

echo "=== laptop_canary done overall=${OVERALL} cycle=${CYCLE} ==="
echo "archived ${OUT}/"
echo "HONESTY: scheduled canary still 0; not beta."
if [[ "$OVERALL" == "PASS" ]]; then
  exit 0
fi
exit 1
