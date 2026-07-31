/**
 * Binance Spot "Top Movers" status formulas.
 *
 * Source: https://www.binance.com/en/support/faq/detail/18c97e8ab67a4e1b824edd590cae9f16
 * ("Understanding Top Movers Statuses on Binance Spot Trading")
 *
 * Statuses fire from focus-instrument tape / OHLCV we already retain. Horizons
 * Binance documents that exceed SPA retention (7d / 30d highs-lows, 6h of
 * 15-minute volume history, 120h average order size) are evaluated when data
 * coverage allows and otherwise marked `limited` / skipped with an explicit
 * coverage note — we do not invent missing history.
 */

import { nsToSec } from './format.js';
import { tradeNotional } from './orderflow.js';

/** Rise / Fall magnitude bands (same for 5min and 2hr). */
export const RISE_FALL_BANDS = Object.freeze([
  { id: 'small', label: 'Small', minPct: 3, maxPct: 7 },
  { id: 'mid', label: 'Mid', minPct: 7, maxPct: 11 },
  { id: 'high', label: 'High', minPct: 11, maxPct: Infinity },
]);

/** Price±HighVol 15-minute bands. */
export const HIGH_VOL_BANDS = Object.freeze([
  { id: 'small', label: 'Small', minPct: 7, maxPct: 11 },
  { id: 'mid', label: 'Mid', minPct: 11, maxPct: 15 },
  { id: 'high', label: 'High', minPct: 15, maxPct: Infinity },
]);

export const HIGH_VOL_MULT = 50;
export const HIGH_VOL_PRIOR_BUCKETS = 24; // previous 24 × 15 minutes
export const HIGH_VOL_BUCKET_SEC = 15 * 60;
export const LARGE_ORDER_MULT = 50;
export const LARGE_ORDER_LOOKBACK_SEC = 120 * 3600;
export const PULLBACK_DAY_UP_PCT = 8;
export const PULLBACK_NEAR_HIGH_PCT = 5;
export const RALLY_DAY_DOWN_PCT = 8;
export const RALLY_NEAR_LOW_PCT = 5;

/**
 * Classify absolute % move into Small / Mid / High band, or null if < Small.
 * @param {number} absPct
 * @param {readonly { id: string, label: string, minPct: number, maxPct: number }[]} bands
 */
export function classifyPctBand(absPct, bands = RISE_FALL_BANDS) {
  const p = Math.abs(Number(absPct));
  if (!Number.isFinite(p)) return null;
  for (const b of bands) {
    if (p >= b.minPct && p < b.maxPct) return b;
  }
  return null;
}

/**
 * Percent change from `from` → `to`.
 * @param {number} from
 * @param {number} to
 */
export function pctChange(from, to) {
  const a = Number(from);
  const b = Number(to);
  if (!Number.isFinite(a) || !Number.isFinite(b) || a === 0) return null;
  return ((b - a) / a) * 100;
}

/**
 * Price at or just before `sec` from ascending candles (close), or null.
 * @param {Array<{ time: number, open?: number, high?: number, low?: number, close: number, volume?: number }>} candles
 * @param {number} sec
 */
export function priceAtOrBefore(candles, sec) {
  if (!candles?.length || !Number.isFinite(sec)) return null;
  let best = null;
  for (const c of candles) {
    if (c.time <= sec) best = c.close;
    else break;
  }
  return best != null && Number.isFinite(best) ? best : null;
}

/**
 * Extreme (high or low) over [fromSec, toSec] inclusive on candle times.
 * @param {Array<{ time: number, high?: number, low?: number, close: number }>} candles
 * @param {number} fromSec
 * @param {number} toSec
 * @param {'high'|'low'} which
 */
export function extremeInRange(candles, fromSec, toSec, which) {
  let best = null;
  for (const c of candles || []) {
    if (c.time < fromSec || c.time > toSec) continue;
    const v = which === 'high' ? Number(c.high ?? c.close) : Number(c.low ?? c.close);
    if (!Number.isFinite(v)) continue;
    if (best == null || (which === 'high' ? v > best : v < best)) best = v;
  }
  return best;
}

/**
 * Aggregate 1s (or finer) candles into fixed-size buckets.
 * @param {Array<{ time: number, open: number, high: number, low: number, close: number, volume?: number, notional?: number, trades?: number }>} candles
 * @param {number} bucketSec
 */
export function aggregateCandles(candles, bucketSec) {
  const step = Math.max(1, Math.floor(bucketSec));
  /** @type {Map<number, { time: number, open: number, high: number, low: number, close: number, volume: number, trades: number }>} */
  const map = new Map();
  for (const c of candles || []) {
    const t = Math.floor(Number(c.time) / step) * step;
    if (!Number.isFinite(t)) continue;
    const open = Number(c.open ?? c.close);
    const high = Number(c.high ?? c.close);
    const low = Number(c.low ?? c.close);
    const close = Number(c.close);
    const vol = Number(c.notional ?? c.volume ?? 0) || 0;
    const trades = Number(c.trades ?? 0) || 0;
    if (!Number.isFinite(close)) continue;
    const prev = map.get(t);
    if (!prev) {
      map.set(t, {
        time: t,
        open: Number.isFinite(open) ? open : close,
        high: Number.isFinite(high) ? high : close,
        low: Number.isFinite(low) ? low : close,
        close,
        volume: vol,
        trades,
      });
    } else {
      if (Number.isFinite(high)) prev.high = Math.max(prev.high, high);
      if (Number.isFinite(low)) prev.low = Math.min(prev.low, low);
      prev.close = close;
      prev.volume += vol;
      prev.trades += trades;
    }
  }
  return [...map.values()].sort((a, b) => a.time - b.time);
}

/**
 * UTC day bounds containing `nowSec`.
 * @param {number} nowSec
 */
export function utcDayBounds(nowSec) {
  const d = new Date(nowSec * 1000);
  const start = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) / 1000;
  return { startSec: start, endSec: start + 86400 - 1 };
}

/**
 * Pullback (Binance): strong day up from open, close still near day high.
 *
 * FAQ writes `(close - high) / high ≤ 5%`. With close ≤ high that is always
 * true; economically "Pullback" means within 5% *below* the high after ≥8%
 * day range-up — so we use `(high - close) / high ≤ 5%`.
 *
 * @param {{ dayOpen: number, dayHigh: number, close: number }} p
 */
export function isPullback(p) {
  const open = Number(p.dayOpen);
  const high = Number(p.dayHigh);
  const close = Number(p.close);
  if (![open, high, close].every(Number.isFinite) || open === 0 || high === 0) return false;
  const dayUp = ((high - open) / open) * 100;
  const fromHigh = ((high - close) / high) * 100;
  return dayUp >= PULLBACK_DAY_UP_PCT && fromHigh >= 0 && fromHigh <= PULLBACK_NEAR_HIGH_PCT;
}

/**
 * Rally (Binance): strong day selloff from open, close bounced from day low.
 *
 * FAQ writes `(low - open) / open ≤ 8%` (always true when low ≤ open). Symmetric
 * to Pullback we require a ≥8% drawdown: `(low - open) / open ≤ -8%`, plus
 * `(close - low) / low ≥ 5%`.
 *
 * @param {{ dayOpen: number, dayLow: number, close: number }} p
 */
export function isRally(p) {
  const open = Number(p.dayOpen);
  const low = Number(p.dayLow);
  const close = Number(p.close);
  if (![open, low, close].every(Number.isFinite) || open === 0 || low === 0) return false;
  const dayDown = ((low - open) / open) * 100;
  const fromLow = ((close - low) / low) * 100;
  return dayDown <= -RALLY_DAY_DOWN_PCT && fromLow >= RALLY_NEAR_LOW_PCT;
}

/**
 * High-vol volume gate: current 15m volume ≥ avg(prior 24 × 15m) × 50.
 * @param {number} currentVol
 * @param {number[]} priorVols length ideally 24
 */
export function passesHighVolGate(currentVol, priorVols) {
  const cur = Number(currentVol);
  const priors = (priorVols || []).map(Number).filter((v) => Number.isFinite(v) && v >= 0);
  if (!Number.isFinite(cur) || cur < 0 || priors.length === 0) return false;
  const avg = priors.reduce((a, b) => a + b, 0) / priors.length;
  if (!(avg > 0)) return false;
  return cur >= avg * HIGH_VOL_MULT;
}

/**
 * Large buy/sell: trade qty ≥ avg(trade qty over lookback) × 50.
 * @param {number} qty
 * @param {number} avgQty
 */
export function isLargeOrder(qty, avgQty) {
  const q = Number(qty);
  const avg = Number(avgQty);
  if (!Number.isFinite(q) || !Number.isFinite(avg) || avg <= 0) return false;
  return q >= avg * LARGE_ORDER_MULT;
}

/**
 * @typedef {{
 *   id: string,
 *   label: string,
 *   kind: 'high'|'low'|'rise'|'fall'|'pullback'|'rally'|'vol_up'|'vol_down'|'large_buy'|'large_sell',
 *   period?: string,
 *   pct?: number|null,
 *   limited?: boolean,
 *   detail?: string,
 * }} TopMoverStatus
 */

/**
 * Detect Binance Top Movers statuses for the focus instrument.
 *
 * @param {{
 *   candles?: Array<{ time: number, open: number, high: number, low: number, close: number, volume?: number, notional?: number, trades?: number }>,
 *   tape?: object[],
 *   nowSec?: number,
 *   historySecs?: number,
 * }} opts
 * @returns {{
 *   statuses: TopMoverStatus[],
 *   coverage: {
 *     historySecs: number,
 *     candleSpanSec: number,
 *     hasUtcDay: boolean,
 *     has7d: boolean,
 *     has30d: boolean,
 *     highVolPriors: number,
 *     highVolReady: boolean,
 *     largeAvgLookbackSec: number,
 *     largeAvgLimited: boolean,
 *     notes: string[],
 *   },
 *   metrics: {
 *     pct5m: number|null,
 *     pct2h: number|null,
 *     pct15m: number|null,
 *     dayOpen: number|null,
 *     dayHigh: number|null,
 *     dayLow: number|null,
 *     close: number|null,
 *     last1mHigh: number|null,
 *     last1mLow: number|null,
 *   },
 * }}
 */
export function detectTopMovers(opts = {}) {
  const candles = [...(opts.candles || [])].sort((a, b) => a.time - b.time);
  const nowSec =
    opts.nowSec != null && Number.isFinite(opts.nowSec)
      ? opts.nowSec
      : candles.length
        ? candles[candles.length - 1].time
        : Math.floor(Date.now() / 1000);
  const historySecs = Number(opts.historySecs) || 0;

  /** @type {TopMoverStatus[]} */
  const statuses = [];
  /** @type {string[]} */
  const notes = [];

  const spanSec =
    candles.length >= 2 ? candles[candles.length - 1].time - candles[0].time : candles.length ? 1 : 0;
  const close = candles.length ? Number(candles[candles.length - 1].close) : null;

  const day = utcDayBounds(nowSec);
  const dayCandles = candles.filter((c) => c.time >= day.startSec && c.time <= nowSec);
  const hasUtcDay = dayCandles.length > 0 && dayCandles[0].time <= day.startSec + 60;
  if (!hasUtcDay && dayCandles.length) {
    notes.push('UTC day open/high/low use session-visible candles (retention < full day)');
  }

  const dayOpen = dayCandles.length ? Number(dayCandles[0].open ?? dayCandles[0].close) : null;
  const dayHigh = extremeInRange(dayCandles, day.startSec, nowSec, 'high');
  const dayLow = extremeInRange(dayCandles, day.startSec, nowSec, 'low');

  const last1mHigh = extremeInRange(candles, nowSec - 60, nowSec, 'high');
  const last1mLow = extremeInRange(candles, nowSec - 60, nowSec, 'low');

  const has7d = spanSec >= 7 * 86400 - 60;
  const has30d = spanSec >= 30 * 86400 - 60;
  if (!has7d) notes.push('New 7day High/Low need ≥7d candle history — unavailable');
  if (!has30d) notes.push('New 30day High/Low need ≥30d candle history — unavailable');

  // --- New High / Low -------------------------------------------------------
  if (last1mHigh != null && dayHigh != null && last1mHigh >= dayHigh) {
    statuses.push({
      id: 'new_24hr_high',
      label: 'New 24hr High',
      kind: 'high',
      period: '1d',
      limited: !hasUtcDay,
      detail: hasUtcDay
        ? 'Last 1m high = UTC day high'
        : 'Last 1m high = session day high (partial UTC day)',
    });
  }
  if (has7d) {
    const weekHigh = extremeInRange(candles, nowSec - 7 * 86400, nowSec, 'high');
    if (last1mHigh != null && weekHigh != null && last1mHigh >= weekHigh) {
      statuses.push({
        id: 'new_7day_high',
        label: 'New 7day High',
        kind: 'high',
        period: '7d',
        detail: 'Last 1m high = 7d high',
      });
    }
  }
  if (has30d) {
    const monthHigh = extremeInRange(candles, nowSec - 30 * 86400, nowSec, 'high');
    if (last1mHigh != null && monthHigh != null && last1mHigh >= monthHigh) {
      statuses.push({
        id: 'new_30day_high',
        label: 'New 30day High',
        kind: 'high',
        period: '30d',
        detail: 'Last 1m high = 30d high',
      });
    }
  }
  if (last1mLow != null && dayLow != null && last1mLow <= dayLow) {
    statuses.push({
      id: 'new_24hr_low',
      label: 'New 24hr Low',
      kind: 'low',
      period: '1d',
      limited: !hasUtcDay,
      detail: hasUtcDay
        ? 'Last 1m low = UTC day low'
        : 'Last 1m low = session day low (partial UTC day)',
    });
  }
  if (has7d) {
    const weekLow = extremeInRange(candles, nowSec - 7 * 86400, nowSec, 'low');
    if (last1mLow != null && weekLow != null && last1mLow <= weekLow) {
      statuses.push({
        id: 'new_7day_low',
        label: 'New 7day Low',
        kind: 'low',
        period: '7d',
        detail: 'Last 1m low = 7d low',
      });
    }
  }
  if (has30d) {
    const monthLow = extremeInRange(candles, nowSec - 30 * 86400, nowSec, 'low');
    if (last1mLow != null && monthLow != null && last1mLow <= monthLow) {
      statuses.push({
        id: 'new_30day_low',
        label: 'New 30day Low',
        kind: 'low',
        period: '30d',
        detail: 'Last 1m low = 30d low',
      });
    }
  }

  // --- Rise / Fall (5min, 2hr) -----------------------------------------------
  const px5m = priceAtOrBefore(candles, nowSec - 5 * 60);
  const px2h = priceAtOrBefore(candles, nowSec - 2 * 3600);
  const pct5m = close != null && px5m != null ? pctChange(px5m, close) : null;
  const pct2h = close != null && px2h != null ? pctChange(px2h, close) : null;

  /**
   * @param {number|null} pct
   * @param {string} windowLabel
   * @param {string} period
   */
  function pushRiseFall(pct, windowLabel, period) {
    if (pct == null) return;
    const band = classifyPctBand(pct, RISE_FALL_BANDS);
    if (!band) return;
    const rising = pct >= 0;
    statuses.push({
      id: `${band.id}_${period}_${rising ? 'rise' : 'fall'}`,
      label: `[${band.label}] ${windowLabel} ${rising ? 'Rise' : 'Fall'}`,
      kind: rising ? 'rise' : 'fall',
      period,
      pct,
      detail: `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}% over ${windowLabel}`,
    });
  }
  pushRiseFall(pct5m, '5min', '5m');
  if (px2h != null) pushRiseFall(pct2h, '2hr', '2h');
  else if (spanSec < 2 * 3600 - 30) notes.push('2hr Rise/Fall need ≥2h candle history');

  // --- Pullback / Rally -----------------------------------------------------
  if (
    dayOpen != null &&
    dayHigh != null &&
    close != null &&
    isPullback({ dayOpen, dayHigh, close })
  ) {
    statuses.push({
      id: 'pullback',
      label: 'Pullback',
      kind: 'pullback',
      limited: !hasUtcDay,
      detail: `Day high ≥${PULLBACK_DAY_UP_PCT}% above open; close within ${PULLBACK_NEAR_HIGH_PCT}% of high`,
    });
  }
  if (dayOpen != null && dayLow != null && close != null && isRally({ dayOpen, dayLow, close })) {
    statuses.push({
      id: 'rally',
      label: 'Rally',
      kind: 'rally',
      limited: !hasUtcDay,
      detail: `Day low ≤−${RALLY_DAY_DOWN_PCT}% from open; close ≥${RALLY_NEAR_LOW_PCT}% above low`,
    });
  }

  // --- Price ± High Vol (15m) -----------------------------------------------
  const m15 = aggregateCandles(candles, HIGH_VOL_BUCKET_SEC);
  const curBucketStart = Math.floor(nowSec / HIGH_VOL_BUCKET_SEC) * HIGH_VOL_BUCKET_SEC;
  const cur15 = m15.find((c) => c.time === curBucketStart) || m15[m15.length - 1];
  const prior15 = m15.filter((c) => c.time < curBucketStart).slice(-HIGH_VOL_PRIOR_BUCKETS);
  const highVolReady = prior15.length >= HIGH_VOL_PRIOR_BUCKETS;
  if (!highVolReady) {
    notes.push(
      `High-Vol statuses need ${HIGH_VOL_PRIOR_BUCKETS} prior 15m buckets (have ${prior15.length})`,
    );
  }
  const px15m = priceAtOrBefore(candles, nowSec - HIGH_VOL_BUCKET_SEC);
  const pct15m = close != null && px15m != null ? pctChange(px15m, close) : null;
  if (pct15m != null && cur15 && passesHighVolGate(cur15.volume, prior15.map((c) => c.volume))) {
    const abs = Math.abs(pct15m);
    const band = classifyPctBand(abs, HIGH_VOL_BANDS);
    if (band) {
      const up = pct15m >= 0;
      // FAQ Mid "Price down" row mistakenly says "increase"; we use drop.
      statuses.push({
        id: `${band.id}_vol_${up ? 'up' : 'down'}`,
        label: `[${band.label}] Price ${up ? 'up' : 'down'} with High Vol`,
        kind: up ? 'vol_up' : 'vol_down',
        pct: pct15m,
        limited: !highVolReady,
        detail: highVolReady
          ? `15m ${pct15m >= 0 ? '+' : ''}${pct15m.toFixed(2)}%; vol ≥ ${HIGH_VOL_MULT}× prior-24 avg`
          : `15m ${pct15m >= 0 ? '+' : ''}${pct15m.toFixed(2)}%; vol gate on ${prior15.length}/${HIGH_VOL_PRIOR_BUCKETS} priors`,
      });
    }
  }

  // --- Large Buy / Sell -----------------------------------------------------
  const tape = (opts.tape || []).filter((e) => e && e.kind === 'trade');
  let largeLookbackSec = 0;
  let oldestTradeSec = null;
  let newestTradeSec = null;
  const qtys = [];
  for (const e of tape) {
    const qty = Number(e.quantity);
    const sec = nsToSec(e.exchange_ts_ns ?? e.receive_ts_ns);
    if (!Number.isFinite(qty) || qty <= 0 || sec == null) continue;
    qtys.push({ qty, sec, side: e.aggressor, notional: tradeNotional(e) });
    if (oldestTradeSec == null || sec < oldestTradeSec) oldestTradeSec = sec;
    if (newestTradeSec == null || sec > newestTradeSec) newestTradeSec = sec;
  }
  if (qtys.length) {
    largeLookbackSec = Math.max(0, (newestTradeSec ?? nowSec) - (oldestTradeSec ?? nowSec));
  }
  const largeAvgLimited = !qtys.length || largeLookbackSec < LARGE_ORDER_LOOKBACK_SEC * 0.9;
  if (largeAvgLimited) {
    notes.push(
      `Large Buy/Sell uses ${Math.round(largeLookbackSec / 3600)}h avg order size (Binance: 120h)`,
    );
  }
  if (qtys.length >= 2) {
    // Compare each recent print to the average of *other* prints (Binance uses
    // prior-window mean; including the whale in the mean would mute the flag).
    const recent = [...qtys].sort((a, b) => b.sec - a.sec).slice(0, 48);
    let sawBuy = false;
    let sawSell = false;
    for (const t of recent) {
      const others = qtys.filter((x) => x !== t);
      if (!others.length) continue;
      const mean = others.reduce((a, x) => a + x.qty, 0) / others.length;
      if (!isLargeOrder(t.qty, mean)) continue;
      if (t.side === 'buy' && !sawBuy) {
        sawBuy = true;
        statuses.push({
          id: 'large_buy',
          label: 'Large Buy',
          kind: 'large_buy',
          limited: largeAvgLimited,
          detail: `qty ${t.qty} ≥ ${LARGE_ORDER_MULT}× avg ${mean.toPrecision(4)}`,
        });
      } else if (t.side === 'sell' && !sawSell) {
        sawSell = true;
        statuses.push({
          id: 'large_sell',
          label: 'Large Sell',
          kind: 'large_sell',
          limited: largeAvgLimited,
          detail: `qty ${t.qty} ≥ ${LARGE_ORDER_MULT}× avg ${mean.toPrecision(4)}`,
        });
      }
      if (sawBuy && sawSell) break;
    }
  }

  // Stable display order: highs → lows → rise/fall → pullback/rally → vol → large
  const order = {
    high: 0,
    low: 1,
    rise: 2,
    fall: 3,
    pullback: 4,
    rally: 5,
    vol_up: 6,
    vol_down: 7,
    large_buy: 8,
    large_sell: 9,
  };
  statuses.sort((a, b) => (order[a.kind] ?? 50) - (order[b.kind] ?? 50));

  return {
    statuses,
    coverage: {
      historySecs,
      candleSpanSec: spanSec,
      hasUtcDay,
      has7d,
      has30d,
      highVolPriors: prior15.length,
      highVolReady,
      largeAvgLookbackSec: largeLookbackSec,
      largeAvgLimited,
      notes,
    },
    metrics: {
      pct5m,
      pct2h,
      pct15m,
      dayOpen,
      dayHigh,
      dayLow,
      close,
      last1mHigh,
      last1mLow,
    },
  };
}
