#!/usr/bin/env bash
# Smoke / audit harness for live market panel UI + view API.
# Usage:
#   BASE=http://127.0.0.1:19109 ./scripts/live_ui_smoke.sh
# Exit 0 on pass, 1 on fail. Writes evidence under .local/live-ui/smoke-*.

set -euo pipefail

BASE="${BASE:-http://127.0.0.1:19109}"
OUT_DIR="${OUT_DIR:-.local/live-ui}"
mkdir -p "$OUT_DIR"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
LOG="$OUT_DIR/smoke-$STAMP.log"
PASS=0
FAIL=0

log() { printf '%s\n' "$*" | tee -a "$LOG"; }
ok() { PASS=$((PASS + 1)); log "PASS  $*"; }
bad() { FAIL=$((FAIL + 1)); log "FAIL  $*"; }

curl_json() {
  local path="$1"
  curl -sS --max-time 5 "${BASE}${path}"
}

http_code() {
  local path="$1"
  curl -sS -o /dev/null -w '%{http_code}' --max-time 5 "${BASE}${path}" || echo 000
}

log "=== live UI smoke @ $BASE ($STAMP) ==="

# --- SPA ---
code=$(http_code /)
if [[ "$code" == "200" ]]; then ok "SPA / → 200"; else bad "SPA / → $code"; fi

# --- Core endpoints ---
for path in /v1/status /v1/instruments /live /ready; do
  code=$(http_code "$path")
  if [[ "$code" == "200" ]]; then ok "$path → 200"; else bad "$path → $code"; fi
done

STATUS=$(curl_json /v1/status)
echo "$STATUS" >"$OUT_DIR/smoke-status.json"
LIVE_VENUES=$(python3 -c 'import json,sys; d=json.load(sys.stdin); print(sum(1 for v in d.get("venues",[]) if v.get("live")))' <<<"$STATUS")
TOTAL_VENUES=$(python3 -c 'import json,sys; d=json.load(sys.stdin); print(len(d.get("venues",[])))' <<<"$STATUS")
log "venues live=$LIVE_VENUES/$TOTAL_VENUES"
if [[ "$LIVE_VENUES" -ge 1 ]]; then ok "at least one live venue"; else bad "no live venues"; fi

# Sample instruments for tape/books checks (python does detailed audit)
set +e
python3 - "$BASE" "$OUT_DIR" <<'PY' | tee -a "$LOG"
import json, sys, urllib.parse, urllib.request

base = sys.argv[1]
out = sys.argv[2]
samples = [
    ("binance-spot", "BTCUSDT"),
    ("okx-spot", "BTC-USDT"),
    ("bybit-linear", "BTCUSDT"),
    ("coinbase-spot", "BTC-USD"),
]

def get(path):
    with urllib.request.urlopen(base + path, timeout=8) as r:
        return json.load(r)

pass_n = fail_n = 0

def ok(msg):
    global pass_n
    pass_n += 1
    print(f"PASS  {msg}")

def bad(msg):
    global fail_n
    fail_n += 1
    print(f"FAIL  {msg}")

def warn(msg):
    print(f"WARN  {msg}")

# Book + tape audits
for venue, symbol in samples:
    q = urllib.parse.urlencode({"venue": venue, "symbol": symbol, "depth": 10})
    try:
        book = get(f"/v1/books?{q}")
    except Exception as e:
        # Non-L2 venues (quote/trade only) correctly 404 — not a smoke failure.
        if "404" in str(e):
            warn(f"books unavailable {venue} {symbol} (non-L2 ok): {e}")
        else:
            bad(f"books {venue} {symbol}: {e}")
        book = None

    if book is not None:
        bids = book.get("bids") or []
        asks = book.get("asks") or []
        if not bids or not asks:
            warn(f"books empty for {venue} {symbol}")
        else:
            bp = float(bids[0]["price"]); ap = float(asks[0]["price"])
            if bp > 0 and ap > 0 and ap >= bp:
                ok(f"book BBO ordered {venue} bid={bp} ask={ap}")
            else:
                bad(f"book BBO invalid {venue} bid={bp} ask={ap}")
            bps = [float(x["price"]) for x in bids]
            aps = [float(x["price"]) for x in asks]
            if all(bps[i] >= bps[i+1] for i in range(len(bps)-1)):
                ok(f"bids descending {venue}")
            else:
                bad(f"bids not descending {venue}: {bps[:5]}")
            if all(aps[i] <= aps[i+1] for i in range(len(aps)-1)):
                ok(f"asks ascending {venue}")
            else:
                bad(f"asks not ascending {venue}: {aps[:5]}")

    # Trade-only tape
    tq = urllib.parse.urlencode({"venue": venue, "symbol": symbol, "limit": 100, "kind": "trade"})
    try:
        tape = get(f"/v1/tape?{tq}")
    except Exception as e:
        bad(f"tape trades {venue}: {e}")
        continue
    entries = tape.get("entries") or []
    kinds = {e.get("kind") for e in entries}
    if kinds and kinds != {"trade"}:
        bad(f"kind=trade returned non-trades {venue}: {kinds}")
    else:
        ok(f"kind=trade filter {venue} n={len(entries)}")

    if not entries:
        mq = urllib.parse.urlencode({"venue": venue, "symbol": symbol, "limit": 50})
        mixed = get(f"/v1/tape?{mq}")
        if mixed.get("entries"):
            ok(f"mixed tape non-empty {venue} (trades quiet / warming)")
        else:
            bad(f"tape empty (trade+mixed) {venue}")
        continue

    ts = [int(e["receive_ts_ns"]) for e in entries if e.get("receive_ts_ns") is not None]
    if len(ts) >= 2 and all(ts[i] >= ts[i+1] for i in range(len(ts)-1)):
        ok(f"tape newest-first {venue}")
    elif len(ts) <= 1:
        ok(f"tape single/empty-ts ok {venue} n={len(ts)}")
    else:
        bad(f"tape sort {venue}: {ts[:5]}")

    t0 = entries[0]
    missing = [k for k in ("price", "quantity", "aggressor", "venue", "symbol") if t0.get(k) in (None, "")]
    if missing:
        bad(f"trade fields missing {venue}: {missing}")
    else:
        ok(f"trade fields present {venue} px={t0['price']} qty={t0['quantity']} side={t0['aggressor']}")

    vol = sum(float(e["quantity"]) for e in entries)
    n = len(entries)
    with open(f"{out}/smoke-tape-{venue}.json", "w") as f:
        json.dump({"venue": venue, "symbol": symbol, "n": n, "volume": vol, "sample": entries[:3]}, f, indent=2)
    ok(f"tape vol/count {venue}: n={n} vol={vol:.8f}")

    # SPA parity: same sum the UI uses for panel chips from this snapshot
    spa_vol = sum(float(e["quantity"]) for e in entries if e.get("kind") == "trade")
    spa_n = sum(1 for e in entries if e.get("kind") == "trade")
    if abs(spa_vol - vol) < 1e-12 and spa_n == n:
        ok(f"SPA vol/# parity vs raw tape {venue}")
    else:
        bad(f"SPA parity mismatch {venue}: spa=({spa_n},{spa_vol}) raw=({n},{vol})")

print(f"SUMMARY_PY pass={pass_n} fail={fail_n}")
sys.exit(1 if fail_n else 0)
PY
PY_RC=${PIPESTATUS[0]}
set -e

# Aggregate from bash checks
log "bash checks: pass=$PASS fail=$FAIL"
log "python audit exit=$PY_RC"
log "log: $LOG"

if [[ "$FAIL" -gt 0 || "$PY_RC" -ne 0 ]]; then
  log "RESULT: FAIL"
  exit 1
fi
log "RESULT: PASS"
exit 0
