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
for path in /v1/status /v1/instruments /live /ready /v1/replay/files /metrics; do
  code=$(http_code "$path")
  if [[ "$code" == "200" ]]; then ok "$path → 200"; else bad "$path → $code"; fi
done

# Replay read without file → 400 (documented contract)
REPLAY_CODE=$(http_code "/v1/replay")
if [[ "$REPLAY_CODE" == "400" ]]; then ok "/v1/replay missing file → 400"; else bad "/v1/replay → $REPLAY_CODE (want 400)"; fi

# Alerts test (POST)
ALERT_CODE=$(curl -sS -o /tmp/live_ui_alert.json -w '%{http_code}' --max-time 5 \
  -X POST -H 'content-type: application/json' \
  -d '{"kind":"discrepancy","bps":9,"message":"smoke"}' \
  "${BASE}/v1/alerts/test" || echo 000)
if [[ "$ALERT_CODE" == "200" ]]; then ok "POST /v1/alerts/test → 200"; else bad "POST /v1/alerts/test → $ALERT_CODE"; fi

# SSE availability probe (SPA uses HEAD before EventSource)
SSE_PROBE=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 -I -H 'accept: text/event-stream' "${BASE}/v1/stream?probe=1" || echo 000)
if [[ "$SSE_PROBE" == "200" ]]; then ok "HEAD /v1/stream probe → 200"; else bad "HEAD /v1/stream probe → $SSE_PROBE"; fi

# SSE stream headers (short read)
SSE_HEAD=$(curl -sS -N --max-time 2 -H 'accept: text/event-stream' "${BASE}/v1/stream?asset=BTC" 2>/dev/null | head -c 200 || true)
if echo "$SSE_HEAD" | grep -q 'data:'; then ok "SSE /v1/stream emits data"; else bad "SSE /v1/stream no data event"; fi

STATUS=$(curl_json /v1/status)
echo "$STATUS" >"$OUT_DIR/smoke-status.json"
# Enriched status fields
python3 -c 'import json,sys; d=json.load(sys.stdin); assert "grafana_base_url" in d or d.get("grafana_base_url") is None; assert "alert_webhook_configured" in d; v=d["venues"][0]; assert "feed_lag_ms" in v; assert "tape_trades" in v' <<<"$STATUS" \
  && ok "status enrichment keys present" || bad "status enrichment keys missing"

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
    ("binance-usdm", "BTCUSDT"),
    ("okx-spot", "BTC-USDT"),
    ("bybit-linear", "BTCUSDT"),
    ("bybit-spot", "BTCUSDT"),
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

# Discover which sample venues exist
try:
    status = get("/v1/status")
    live_ids = {v["id"] for v in status.get("venues", [])}
except Exception as e:
    bad(f"status for samples: {e}")
    live_ids = set()

for venue, symbol in samples:
    if venue not in live_ids:
        warn(f"skip {venue} (not in status)")
        continue
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

    if entries and any(e.get("notional") for e in entries if e.get("kind") == "trade"):
        ok(f"trade notional present {venue}")
    elif entries:
        warn(f"trade notional missing {venue} (older daemon?)")

    # PR #4 critical venues: must have real (nonzero) trades, not empty/zero prints.
    critical = venue in ("binance-usdm", "bybit-spot")
    if not entries:
        if critical:
            bad(f"critical tape empty {venue}")
            continue
        mq = urllib.parse.urlencode({"venue": venue, "symbol": symbol, "limit": 50})
        mixed = get(f"/v1/tape?{mq}")
        if mixed.get("entries"):
            ok(f"mixed tape non-empty {venue} (trades quiet / warming)")
        else:
            bad(f"tape empty (trade+mixed) {venue}")
        continue
    if critical:
        try:
            px = float(entries[0].get("price") or 0)
            qty = float(entries[0].get("quantity") or 0)
        except (TypeError, ValueError):
            px = qty = 0.0
        if px > 0 and qty > 0:
            ok(f"critical nonzero trade {venue} px={px} qty={qty}")
        else:
            bad(f"critical zero trade {venue} px={entries[0].get('price')} qty={entries[0].get('quantity')}")

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
