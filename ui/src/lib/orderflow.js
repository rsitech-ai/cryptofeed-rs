/**
 * Order-flow analytics from L2 books + aggressor-tagged trades.
 * Footprint / absorption / sweep are trade-aggregated heuristics — not MBO.
 */

import { nsToSec } from './format.js';

/**
 * USD notional for a tape trade (prefer server `notional` when present).
 * @param {object} e
 * @returns {number|null}
 */
export function tradeNotional(e) {
  if (e?.notional != null && e.notional !== '') {
    const n = Number(e.notional);
    if (Number.isFinite(n)) return n;
  }
  const px = Number(e?.price);
  const qty = Number(e?.quantity);
  if (Number.isFinite(px) && Number.isFinite(qty)) return px * qty;
  return null;
}

/**
 * @param {string|null|undefined} aggressor
 * @returns {1|-1|0}
 */
export function aggressorSign(aggressor) {
  if (aggressor === 'buy') return 1;
  if (aggressor === 'sell') return -1;
  return 0;
}

/**
 * Chronological trade list (oldest → newest), optional window filter.
 * @param {object[]} tape
 * @param {{ windowSec?: number|null, nowSec?: number|null }} [opts]
 */
export function filterTrades(tape, opts = {}) {
  const { windowSec = null, nowSec = null } = opts;
  let rows = (tape || []).filter((e) => e && e.kind === 'trade');
  if (windowSec != null && windowSec > 0) {
    const end = nowSec ?? Math.floor(Date.now() / 1000);
    const start = end - windowSec;
    rows = rows.filter((e) => {
      const sec = nsToSec(e.exchange_ts_ns ?? e.receive_ts_ns);
      return sec != null && sec >= start && sec <= end;
    });
  }
  return rows.slice().sort((a, b) => {
    const sa = Number(a.exchange_ts_ns ?? a.receive_ts_ns) || 0;
    const sb = Number(b.exchange_ts_ns ?? b.receive_ts_ns) || 0;
    return sa - sb;
  });
}

/**
 * Cumulative Volume Delta (buy+ / sell−), USD-normalized when possible.
 * @param {object[]} tape
 * @param {{ windowSec?: number|null, nowSec?: number|null }} [opts]
 * @returns {{
 *   cvd: number,
 *   buyUsd: number,
 *   sellUsd: number,
 *   buyQty: number,
 *   sellQty: number,
 *   trades: number,
 *   points: Array<{ sec: number, cvd: number, buyUsd: number, sellUsd: number }>,
 *   histogram: Array<{ sec: number, buyUsd: number, sellUsd: number }>,
 * }}
 */
export function computeCvd(tape, opts = {}) {
  const trades = filterTrades(tape, opts);
  let cvd = 0;
  let buyUsd = 0;
  let sellUsd = 0;
  let buyQty = 0;
  let sellQty = 0;
  /** @type {Array<{ sec: number, cvd: number, buyUsd: number, sellUsd: number }>} */
  const points = [];
  /** @type {Map<number, { buyUsd: number, sellUsd: number }>} */
  const hist = new Map();

  for (const e of trades) {
    const sign = aggressorSign(e.aggressor);
    const usd = tradeNotional(e) ?? 0;
    const qty = Number(e.quantity) || 0;
    const sec = nsToSec(e.exchange_ts_ns ?? e.receive_ts_ns) ?? 0;
    if (sign > 0) {
      cvd += usd;
      buyUsd += usd;
      buyQty += qty;
    } else if (sign < 0) {
      cvd -= usd;
      sellUsd += usd;
      sellQty += qty;
    }
    points.push({ sec, cvd, buyUsd, sellUsd });
    const bucket = sec;
    const h = hist.get(bucket) || { buyUsd: 0, sellUsd: 0 };
    if (sign > 0) h.buyUsd += usd;
    else if (sign < 0) h.sellUsd += usd;
    hist.set(bucket, h);
  }

  const histogram = [...hist.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([sec, v]) => ({ sec, buyUsd: v.buyUsd, sellUsd: v.sellUsd }));

  return {
    cvd,
    buyUsd,
    sellUsd,
    buyQty,
    sellQty,
    trades: trades.length,
    points,
    histogram,
  };
}

/**
 * Trade-aggregated volume-at-price (footprint-lite). Not true MBO footprint.
 * @param {object[]} tape
 * @param {{ windowSec?: number|null, nowSec?: number|null, tickSize?: number|null, maxBuckets?: number }} [opts]
 * @returns {Array<{
 *   price: number,
 *   buyQty: number,
 *   sellQty: number,
 *   buyUsd: number,
 *   sellUsd: number,
 *   delta: number,
 * }>}
 */
export function volumeAtPrice(tape, opts = {}) {
  const { tickSize = null, maxBuckets = 48 } = opts;
  const trades = filterTrades(tape, opts);
  /** @type {Map<number, { buyQty: number, sellQty: number, buyUsd: number, sellUsd: number }>} */
  const buckets = new Map();

  for (const e of trades) {
    const px = Number(e.price);
    const qty = Number(e.quantity) || 0;
    const usd = tradeNotional(e) ?? 0;
    if (!Number.isFinite(px) || px <= 0) continue;
    const key =
      tickSize && tickSize > 0
        ? Math.round(px / tickSize) * tickSize
        : Math.round(px * 100) / 100;
    const b = buckets.get(key) || { buyQty: 0, sellQty: 0, buyUsd: 0, sellUsd: 0 };
    const sign = aggressorSign(e.aggressor);
    if (sign > 0) {
      b.buyQty += qty;
      b.buyUsd += usd;
    } else if (sign < 0) {
      b.sellQty += qty;
      b.sellUsd += usd;
    }
    buckets.set(key, b);
  }

  let rows = [...buckets.entries()]
    .map(([price, v]) => ({
      price,
      buyQty: v.buyQty,
      sellQty: v.sellQty,
      buyUsd: v.buyUsd,
      sellUsd: v.sellUsd,
      delta: v.buyUsd - v.sellUsd,
    }))
    .sort((a, b) => b.price - a.price);

  if (rows.length > maxBuckets) {
    // Keep buckets nearest mid (mean of extremes).
    const mid = (rows[0].price + rows[rows.length - 1].price) / 2;
    rows = rows
      .slice()
      .sort((a, b) => Math.abs(a.price - mid) - Math.abs(b.price - mid))
      .slice(0, maxBuckets)
      .sort((a, b) => b.price - a.price);
  }
  return rows;
}

/**
 * Per-level ladder with cumulative size/USD and imbalance vs opposite top-N.
 * @param {object|null} book
 * @param {number} depth
 */
export function ladderLevels(book, depth = 16) {
  const n = Math.max(1, Math.min(50, Number(depth) || 16));
  const asksRaw = [...(book?.asks || [])].slice(0, n);
  const bidsRaw = [...(book?.bids || [])].slice(0, n);

  let askCumQty = 0;
  let askCumUsd = 0;
  const asks = asksRaw.map((l) => {
    const price = Number(l.price);
    const qty = Number(l.quantity) || 0;
    const usd = Number.isFinite(price) ? price * qty : 0;
    askCumQty += qty;
    askCumUsd += usd;
    return { price, qty, usd, cumQty: askCumQty, cumUsd: askCumUsd };
  });

  let bidCumQty = 0;
  let bidCumUsd = 0;
  const bids = bidsRaw.map((l) => {
    const price = Number(l.price);
    const qty = Number(l.quantity) || 0;
    const usd = Number.isFinite(price) ? price * qty : 0;
    bidCumQty += qty;
    bidCumUsd += usd;
    return { price, qty, usd, cumQty: bidCumQty, cumUsd: bidCumUsd };
  });

  const maxCumQty = Math.max(
    asks.length ? asks[asks.length - 1].cumQty : 0,
    bids.length ? bids[bids.length - 1].cumQty : 0,
    1e-12,
  );
  const maxCumUsd = Math.max(
    asks.length ? asks[asks.length - 1].cumUsd : 0,
    bids.length ? bids[bids.length - 1].cumUsd : 0,
    1e-12,
  );

  return {
    asks,
    bids,
    askCumQty,
    bidCumQty,
    askCumUsd,
    bidCumUsd,
    maxCumQty,
    maxCumUsd,
  };
}

/**
 * Book pressure: bid depth USD vs ask depth USD (top N).
 * imbalancePct = (bid - ask) / (bid + ask) * 100; positive = bid-heavy.
 * @param {object|null} book
 * @param {number} depth
 */
export function bookPressure(book, depth = 16) {
  const L = ladderLevels(book, depth);
  const bidUsd = L.bidCumUsd;
  const askUsd = L.askCumUsd;
  const sum = bidUsd + askUsd;
  const imbalancePct = sum > 0 ? ((bidUsd - askUsd) / sum) * 100 : 0;
  const bidPct = sum > 0 ? (bidUsd / sum) * 100 : 50;
  const ratio = askUsd > 0 ? bidUsd / askUsd : bidUsd > 0 ? Infinity : 1;
  return {
    bidUsd,
    askUsd,
    bidQty: L.bidCumQty,
    askQty: L.askCumQty,
    imbalancePct,
    bidPct,
    askPct: 100 - bidPct,
    ratio,
  };
}

/**
 * Level imbalance % at index i: bid_i vs ask_i size.
 * @param {number} bidQty
 * @param {number} askQty
 */
export function levelImbalancePct(bidQty, askQty) {
  const b = Number(bidQty) || 0;
  const a = Number(askQty) || 0;
  const sum = b + a;
  if (sum <= 0) return 0;
  return ((b - a) / sum) * 100;
}

/**
 * Push depth imbalance sample into a bounded ring.
 * @param {Array<{ t: number, imbalancePct: number }>} history
 * @param {number} imbalancePct
 * @param {number} [max]
 */
export function pushImbalanceHistory(history, imbalancePct, max = 90) {
  const next = [...(history || []), { t: Date.now(), imbalancePct }];
  return next.length > max ? next.slice(next.length - max) : next;
}

/**
 * Large trade / sweep / absorption heuristics (honest labels).
 * @param {object[]} tape
 * @param {object|null} book
 * @param {{ largeUsd?: number, windowSec?: number|null, nowSec?: number|null }} [opts]
 * @returns {Array<{
 *   kind: 'large'|'sweep'|'absorption',
 *   label: string,
 *   heuristic: true,
 *   side: 'buy'|'sell'|null,
 *   usd: number,
 *   price: number|null,
 *   ts: number|null,
 * }>}
 */
export function detectFlowHeuristics(tape, book, opts = {}) {
  const largeUsd = opts.largeUsd ?? 25000;
  const trades = filterTrades(tape, opts);
  if (!trades.length) return [];

  const pressure = bookPressure(book, 8);
  /** @type {Array<object>} */
  const out = [];

  for (let i = 0; i < trades.length; i++) {
    const e = trades[i];
    const usd = tradeNotional(e) ?? 0;
    const side = e.aggressor === 'buy' || e.aggressor === 'sell' ? e.aggressor : null;
    const price = Number(e.price);
    const ts = nsToSec(e.exchange_ts_ns ?? e.receive_ts_ns);

    if (usd < largeUsd) continue;

    out.push({
      kind: 'large',
      label: `Large ${side || '?'} ${Math.round(usd).toLocaleString()} USD`,
      heuristic: true,
      side,
      usd,
      price: Number.isFinite(price) ? price : null,
      ts,
    });

    // Sweep heuristic: large aggressor into thin opposite book (top-of-book USD << print).
    const thinSideUsd = side === 'buy' ? pressure.askUsd : side === 'sell' ? pressure.bidUsd : 0;
    if (side && thinSideUsd > 0 && usd >= thinSideUsd * 0.35) {
      out.push({
        kind: 'sweep',
        label: `Sweep? ${side} into thin ${side === 'buy' ? 'asks' : 'bids'} (heuristic)`,
        heuristic: true,
        side,
        usd,
        price: Number.isFinite(price) ? price : null,
        ts,
      });
    }

    // Absorption heuristic: large print but next same-side print within ~2s at similar price
    // while opposite book still has depth — suggests refill / absorption, not continuation.
    if (side && i + 1 < trades.length) {
      const nxt = trades[i + 1];
      const nxtSec = nsToSec(nxt.exchange_ts_ns ?? nxt.receive_ts_ns);
      const nxtPx = Number(nxt.price);
      const sameSide = nxt.aggressor === side;
      const closeInTime = ts != null && nxtSec != null && Math.abs(nxtSec - ts) <= 2;
      const closeInPrice =
        Number.isFinite(price) && Number.isFinite(nxtPx) && Math.abs(nxtPx - price) / price < 0.00015;
      const refillDepth = side === 'buy' ? pressure.askUsd : pressure.bidUsd;
      if (sameSide && closeInTime && closeInPrice && refillDepth > usd * 0.5) {
        out.push({
          kind: 'absorption',
          label: `Absorption? large ${side} with opposite depth refill (heuristic)`,
          heuristic: true,
          side,
          usd,
          price: Number.isFinite(price) ? price : null,
          ts,
        });
      }
    }
  }

  // Deduplicate by kind+side+ts, keep newest few.
  const seen = new Set();
  const deduped = [];
  for (let i = out.length - 1; i >= 0; i--) {
    const h = out[i];
    const key = `${h.kind}|${h.side}|${h.ts}|${Math.round(h.usd)}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(h);
    if (deduped.length >= 12) break;
  }
  return deduped.reverse();
}

/**
 * Sparkline path for numeric series (0..w × 0..h).
 * @param {number[]} values
 * @param {{ w?: number, h?: number }} [opts]
 */
export function sparkPath(values, opts = {}) {
  const w = opts.w ?? 100;
  const h = opts.h ?? 28;
  const pts = (values || []).filter((v) => Number.isFinite(v));
  if (pts.length < 2) return '';
  const min = Math.min(...pts);
  const max = Math.max(...pts);
  const span = max - min || 1;
  const step = w / (pts.length - 1);
  return pts
    .map((v, i) => {
      const x = i * step;
      const y = h - ((v - min) / span) * (h - 2) - 1;
      return `${i === 0 ? 'M' : 'L'}${x.toFixed(1)} ${y.toFixed(1)}`;
    })
    .join(' ');
}

/* ── Liquidity heatmap (L2 snapshot reconstruction — not MBO) ─────────── */

/**
 * Quantize a price onto a tick grid.
 * @param {number} price
 * @param {number} tick
 */
export function quantizePrice(price, tick) {
  if (!Number.isFinite(price) || !(tick > 0)) return price;
  const n = Math.round(price / tick);
  const q = n * tick;
  if (tick >= 1) return q;
  // Stabilize Map keys: 64287.99 / 0.01 * 0.01 can become 64287.990000000005.
  const decimals = Math.min(8, Math.max(0, Math.ceil(-Math.log10(tick) - 1e-12)));
  return Number(q.toFixed(decimals));
}

/**
 * Infer a reasonable tick from BBO / book levels.
 * @param {object|null} book
 * @param {number} [fallback]
 */
export function inferTickSize(book, fallback = 0.1) {
  const bids = book?.bids || [];
  const asks = book?.asks || [];
  const px = [];
  for (const l of [...bids.slice(0, 8), ...asks.slice(0, 8)]) {
    const p = Number(l.price);
    if (Number.isFinite(p)) px.push(p);
  }
  if (px.length < 2) return fallback;
  px.sort((a, b) => a - b);
  let minDiff = Infinity;
  for (let i = 1; i < px.length; i++) {
    const d = px[i] - px[i - 1];
    if (d > 0 && d < minDiff) minDiff = d;
  }
  if (!Number.isFinite(minDiff) || minDiff <= 0) return fallback;
  // Snap to nice increments.
  if (minDiff >= 1) return Math.max(1, Math.round(minDiff));
  if (minDiff >= 0.1) return Math.round(minDiff * 10) / 10;
  if (minDiff >= 0.01) return Math.round(minDiff * 100) / 100;
  return minDiff;
}

/**
 * Compact one book snapshot into price→USD size maps (bids/asks separate).
 * Caps levels for render performance.
 * @param {object|null} book
 * @param {{ tick?: number, maxLevels?: number, t?: number }} [opts]
 * @returns {{
 *   t: number,
 *   mid: number|null,
 *   tick: number,
 *   bids: Map<number, number>,
 *   asks: Map<number, number>,
 *   bidUsd: number,
 *   askUsd: number,
 * }|null}
 */
export function sampleBookDepth(book, opts = {}) {
  const maxLevels = opts.maxLevels ?? 40;
  const tick = opts.tick ?? inferTickSize(book);
  const t = opts.t ?? Date.now();
  const bidsRaw = (book?.bids || []).slice(0, maxLevels);
  const asksRaw = (book?.asks || []).slice(0, maxLevels);
  if (!bidsRaw.length && !asksRaw.length) return null;

  /** @type {Map<number, number>} */
  const bids = new Map();
  /** @type {Map<number, number>} */
  const asks = new Map();
  let bidUsd = 0;
  let askUsd = 0;

  for (const l of bidsRaw) {
    const px = Number(l.price);
    const qty = Number(l.quantity) || 0;
    if (!Number.isFinite(px) || qty <= 0) continue;
    const key = quantizePrice(px, tick);
    const usd = px * qty;
    bids.set(key, (bids.get(key) || 0) + usd);
    bidUsd += usd;
  }
  for (const l of asksRaw) {
    const px = Number(l.price);
    const qty = Number(l.quantity) || 0;
    if (!Number.isFinite(px) || qty <= 0) continue;
    const key = quantizePrice(px, tick);
    const usd = px * qty;
    asks.set(key, (asks.get(key) || 0) + usd);
    askUsd += usd;
  }

  const b0 = Number(bidsRaw[0]?.price);
  const a0 = Number(asksRaw[0]?.price);
  const mid =
    Number.isFinite(b0) && Number.isFinite(a0) ? (b0 + a0) / 2 : Number.isFinite(b0) ? b0 : Number.isFinite(a0) ? a0 : null;

  return { t, mid, tick, bids, asks, bidUsd, askUsd };
}

/**
 * Push a depth sample into a bounded ring (oldest dropped).
 * @param {Array<object>} history
 * @param {object|null} sample
 * @param {number} [max]
 */
export function pushDepthHistory(history, sample, max = 240) {
  if (!sample) return history || [];
  const next = [...(history || []), sample];
  return next.length > max ? next.slice(next.length - max) : next;
}

/**
 * Aggregate trades into time×price bubbles (buy/sell split).
 * @param {object[]} tape
 * @param {{
 *   windowSec?: number|null,
 *   nowSec?: number|null,
 *   tick?: number,
 *   bucketMs?: number,
 *   maxBubbles?: number,
 * }} [opts]
 * @returns {Array<{
 *   t: number,
 *   price: number,
 *   buyUsd: number,
 *   sellUsd: number,
 *   totalUsd: number,
 *   delta: number,
 * }>}
 */
export function tradeBubbles(tape, opts = {}) {
  const tick = opts.tick ?? 0.1;
  const bucketMs = opts.bucketMs ?? 500;
  const maxBubbles = opts.maxBubbles ?? 180;
  const trades = filterTrades(tape, opts);
  /** @type {Map<string, { t: number, price: number, buyUsd: number, sellUsd: number }>} */
  const buckets = new Map();

  for (const e of trades) {
    const px = Number(e.price);
    const usd = tradeNotional(e) ?? 0;
    const sec = nsToSec(e.exchange_ts_ns ?? e.receive_ts_ns);
    if (!Number.isFinite(px) || sec == null || usd <= 0) continue;
    const tMs = Math.floor((sec * 1000) / bucketMs) * bucketMs;
    const qpx = quantizePrice(px, tick);
    const key = `${tMs}|${qpx}`;
    const b = buckets.get(key) || { t: tMs, price: qpx, buyUsd: 0, sellUsd: 0 };
    const sign = aggressorSign(e.aggressor);
    if (sign > 0) b.buyUsd += usd;
    else if (sign < 0) b.sellUsd += usd;
    else {
      // Untagged — split evenly so bubble still shows size.
      b.buyUsd += usd / 2;
      b.sellUsd += usd / 2;
    }
    buckets.set(key, b);
  }

  let rows = [...buckets.values()]
    .map((b) => ({
      ...b,
      totalUsd: b.buyUsd + b.sellUsd,
      delta: b.buyUsd - b.sellUsd,
    }))
    .sort((a, b) => a.t - b.t);

  if (rows.length > maxBubbles) rows = rows.slice(rows.length - maxBubbles);
  return rows;
}

/**
 * Volume bars for subplot (buy/sell per time bucket).
 * @param {object[]} tape
 * @param {{ windowSec?: number|null, nowSec?: number|null, bucketSec?: number }} [opts]
 */
export function volumeBarsFromTape(tape, opts = {}) {
  const bucketSec = opts.bucketSec ?? 1;
  const trades = filterTrades(tape, opts);
  /** @type {Map<number, { buyUsd: number, sellUsd: number }>} */
  const hist = new Map();
  for (const e of trades) {
    const sec = nsToSec(e.exchange_ts_ns ?? e.receive_ts_ns);
    const usd = tradeNotional(e) ?? 0;
    if (sec == null || usd <= 0) continue;
    const bucket = Math.floor(sec / bucketSec) * bucketSec;
    const h = hist.get(bucket) || { buyUsd: 0, sellUsd: 0 };
    const sign = aggressorSign(e.aggressor);
    if (sign > 0) h.buyUsd += usd;
    else if (sign < 0) h.sellUsd += usd;
    else {
      h.buyUsd += usd / 2;
      h.sellUsd += usd / 2;
    }
    hist.set(bucket, h);
  }
  return [...hist.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([sec, v]) => ({
      sec,
      buyUsd: v.buyUsd,
      sellUsd: v.sellUsd,
      totalUsd: v.buyUsd + v.sellUsd,
    }));
}

/**
 * Bookmap-style color: dark blue → cyan → yellow → red by intensity 0..1.
 * @param {number} intensity
 * @returns {[number, number, number, number]} RGBA 0–255
 */
export function heatmapColor(intensity) {
  const t = Math.max(0, Math.min(1, intensity));
  // Keep faint liquidity visible (was fully transparent below 0.01 → empty canvas).
  if (t < 0.02) return [12, 28, 48, Math.round(t * 900)];
  // piecewise: blue → cyan → yellow → orange → red
  if (t < 0.25) {
    const u = t / 0.25;
    return [10 + u * 20, 40 + u * 140, 120 + u * 80, Math.round(70 + u * 110)];
  }
  if (t < 0.5) {
    const u = (t - 0.25) / 0.25;
    return [30 + u * 180, 180 + u * 40, 200 - u * 160, Math.round(180 + u * 40)];
  }
  if (t < 0.75) {
    const u = (t - 0.5) / 0.25;
    return [210 + u * 30, 220 - u * 80, 40 - u * 20, 235];
  }
  const u = (t - 0.75) / 0.25;
  return [240, 140 - u * 90, 20 + u * 30, 255];
}

/**
 * Left-pad (CSS/canvas px) so full price labels like "64,287.99" are not clipped.
 * Root cause of "4,287" axis: leading digit drawn past x=0.
 * @param {number|null|undefined} price
 * @param {number} [dpr]
 */
export function priceAxisPadPx(price, dpr = 1) {
  const n = Math.abs(Number(price)) || 0;
  let chars = 7;
  if (n >= 100000) chars = 10;
  else if (n >= 10000) chars = 9;
  else if (n >= 1000) chars = 8;
  else if (n >= 100) chars = 7;
  // Monospace ~7.2 CSS-px/char at 11px + gutter for tick marks.
  return Math.ceil((chars * 7.2 + 18) * Math.max(1, dpr));
}

/**
 * Bookmap-style Y domain around focus/BBO using recent L2 walls.
 * Prefer book span (padded) over a fixed ±bps band so BTC pennies aren't
 * crushed into a 1px ribbon inside an empty ±160$ void.
 *
 * @param {Array<object>} history samples from sampleBookDepth
 * @param {{
 *   focusPrice?: number|null,
 *   tick?: number|null,
 *   lookback?: number,
 *   minTicks?: number,
 *   padFrac?: number,
 *   minBps?: number,
 *   maxBps?: number,
 * }} [opts]
 * @returns {{ priceMin: number, priceMax: number, mid: number, tick: number, wallLo: number|null, wallHi: number|null }|null}
 */
export function computePriceWindow(history, opts = {}) {
  const samples = (history || []).filter((s) => s && (s.bids?.size || s.asks?.size || s.mid != null));
  const latest = samples.at(-1) || null;
  const focus =
    opts.focusPrice != null && Number.isFinite(Number(opts.focusPrice))
      ? Number(opts.focusPrice)
      : latest?.mid != null && Number.isFinite(latest.mid)
        ? latest.mid
        : null;
  if (focus == null || !(focus > 0)) return null;

  const tick =
    opts.tick != null && opts.tick > 0
      ? opts.tick
      : latest?.tick > 0
        ? latest.tick
        : 0.1;
  const lookback = Math.max(1, opts.lookback ?? 64);
  const minTicks = Math.max(8, opts.minTicks ?? 48);
  const padFrac = opts.padFrac ?? 0.2;
  const minBps = opts.minBps ?? 0.8;
  const maxBps = opts.maxBps ?? 12;

  const slice = samples.slice(-lookback);
  let wallLo = Infinity;
  let wallHi = -Infinity;
  const maxDist = focus * (maxBps / 10000) * 1.5;
  for (const s of slice) {
    for (const px of s.bids?.keys?.() || []) {
      if (!Number.isFinite(px)) continue;
      // Ignore outlier stubs far from focus (bad/stale ladder rows).
      if (Math.abs(px - focus) > maxDist) continue;
      if (px < wallLo) wallLo = px;
      if (px > wallHi) wallHi = px;
    }
    for (const px of s.asks?.keys?.() || []) {
      if (!Number.isFinite(px)) continue;
      if (Math.abs(px - focus) > maxDist) continue;
      if (px < wallLo) wallLo = px;
      if (px > wallHi) wallHi = px;
    }
    if (s.mid != null && Number.isFinite(s.mid) && Math.abs(s.mid - focus) <= maxDist) {
      if (s.mid < wallLo) wallLo = s.mid;
      if (s.mid > wallHi) wallHi = s.mid;
    }
  }
  if (!Number.isFinite(wallLo) || !Number.isFinite(wallHi)) {
    wallLo = focus;
    wallHi = focus;
  }

  const bookHalf = Math.max(0, (wallHi - wallLo) / 2) * (1 + padFrac);
  const tickHalf = tick * minTicks;
  const minHalf = focus * (minBps / 10000);
  const maxHalf = focus * (maxBps / 10000);
  let half = Math.max(bookHalf, tickHalf, minHalf, tick * 4);
  half = Math.min(half, Math.max(maxHalf, bookHalf, tickHalf));

  // Keep focus centered; never collapse to zero span.
  if (!(half > 0)) half = Math.max(tick * minTicks, focus * 1e-4);

  return {
    priceMin: focus - half,
    priceMax: focus + half,
    mid: focus,
    tick,
    wallLo: Number.isFinite(wallLo) ? wallLo : null,
    wallHi: Number.isFinite(wallHi) ? wallHi : null,
  };
}

/**
 * Resting USD at/near a price from a depth sample (exact tick, else nearest).
 * @param {object|null|undefined} sample
 * @param {number} price
 * @param {number} [tick]
 * @returns {{ bidUsd: number, askUsd: number, price: number|null, dist: number }}
 */
export function restingAtPrice(sample, price, tick = 0.1) {
  const empty = { bidUsd: 0, askUsd: 0, price: null, dist: Infinity };
  if (!sample || !Number.isFinite(price)) return empty;
  const t = tick > 0 ? tick : 0.1;
  const q = quantizePrice(price, t);
  const bidExact = sample.bids?.get?.(q) || 0;
  const askExact = sample.asks?.get?.(q) || 0;
  if (bidExact > 0 || askExact > 0) {
    return { bidUsd: bidExact, askUsd: askExact, price: q, dist: Math.abs(q - price) };
  }

  let bestPx = null;
  let bestD = Infinity;
  for (const px of sample.bids?.keys?.() || []) {
    const d = Math.abs(px - price);
    if (d < bestD) {
      bestD = d;
      bestPx = px;
    }
  }
  for (const px of sample.asks?.keys?.() || []) {
    const d = Math.abs(px - price);
    if (d < bestD) {
      bestD = d;
      bestPx = px;
    }
  }
  // Snap within a small band so hover near BBO doesn't report $0 on float gaps.
  const snap = Math.max(t * 8, t);
  if (bestPx == null || bestD > snap) {
    return { bidUsd: 0, askUsd: 0, price: q, dist: bestD };
  }
  return {
    bidUsd: sample.bids?.get?.(bestPx) || 0,
    askUsd: sample.asks?.get?.(bestPx) || 0,
    price: bestPx,
    dist: bestD,
  };
}

/**
 * Nearest bid wall (≤ price) and ask wall (≥ price) for Bookmap-style tooltips.
 * @param {object|null|undefined} sample
 * @param {number} price
 */
export function nearestWalls(sample, price) {
  const empty = {
    bidUsd: 0,
    askUsd: 0,
    bidPrice: null,
    askPrice: null,
  };
  if (!sample || !Number.isFinite(price)) return empty;
  let bidPrice = null;
  let bidUsd = 0;
  let askPrice = null;
  let askUsd = 0;
  for (const [px, usd] of sample.bids?.entries?.() || []) {
    if (px <= price + 1e-12 && (bidPrice == null || px > bidPrice)) {
      bidPrice = px;
      bidUsd = usd;
    }
  }
  for (const [px, usd] of sample.asks?.entries?.() || []) {
    if (px >= price - 1e-12 && (askPrice == null || px < askPrice)) {
      askPrice = px;
      askUsd = usd;
    }
  }
  // Fallback: absolute nearest on each side if cursor is outside the book.
  if (bidPrice == null) {
    for (const [px, usd] of sample.bids?.entries?.() || []) {
      if (bidPrice == null || px > bidPrice) {
        bidPrice = px;
        bidUsd = usd;
      }
    }
  }
  if (askPrice == null) {
    for (const [px, usd] of sample.asks?.entries?.() || []) {
      if (askPrice == null || px < askPrice) {
        askPrice = px;
        askUsd = usd;
      }
    }
  }
  return { bidUsd, askUsd, bidPrice, askPrice };
}

/**
 * OHLC candles from aggressor tape (hybrid candle+heat layer).
 * @param {object[]} tape
 * @param {{ windowSec?: number|null, nowSec?: number|null, bucketSec?: number, maxBars?: number }} [opts]
 */
export function ohlcBucketsFromTape(tape, opts = {}) {
  const bucketSec = Math.max(1, opts.bucketSec ?? 5);
  const maxBars = opts.maxBars ?? 48;
  const trades = filterTrades(tape, opts);
  /** @type {Map<number, { sec: number, o: number, h: number, l: number, c: number, buyUsd: number, sellUsd: number, n: number }>} */
  const buckets = new Map();
  for (const e of trades) {
    const px = Number(e.price);
    const sec = nsToSec(e.exchange_ts_ns ?? e.receive_ts_ns);
    const usd = tradeNotional(e) ?? 0;
    if (!Number.isFinite(px) || sec == null) continue;
    const bsec = Math.floor(sec / bucketSec) * bucketSec;
    let b = buckets.get(bsec);
    if (!b) {
      b = { sec: bsec, o: px, h: px, l: px, c: px, buyUsd: 0, sellUsd: 0, n: 0 };
      buckets.set(bsec, b);
    }
    b.h = Math.max(b.h, px);
    b.l = Math.min(b.l, px);
    b.c = px;
    b.n += 1;
    const sign = aggressorSign(e.aggressor);
    if (sign > 0) b.buyUsd += usd;
    else if (sign < 0) b.sellUsd += usd;
  }
  return [...buckets.values()].sort((a, b) => a.sec - b.sec).slice(-maxBars);
}

/**
 * Footprint / cluster cells: per time bucket × price, bid(sell) vs ask(buy) USD.
 * Honest trade-aggregated footprint — not MBO queue.
 * @param {object[]} tape
 * @param {{ windowSec?: number|null, nowSec?: number|null, tick?: number, bucketSec?: number, maxCells?: number }} [opts]
 */
export function footprintClusters(tape, opts = {}) {
  const tick = opts.tick ?? 0.1;
  const bucketSec = Math.max(1, opts.bucketSec ?? 15);
  const maxCells = opts.maxCells ?? 400;
  const trades = filterTrades(tape, opts);
  /** @type {Map<string, { t: number, price: number, bidUsd: number, askUsd: number }>} */
  const cells = new Map();
  for (const e of trades) {
    const px = Number(e.price);
    const sec = nsToSec(e.exchange_ts_ns ?? e.receive_ts_ns);
    const usd = tradeNotional(e) ?? 0;
    if (!Number.isFinite(px) || sec == null || usd <= 0) continue;
    const t = Math.floor(sec / bucketSec) * bucketSec * 1000;
    const qpx = quantizePrice(px, tick);
    const key = `${t}|${qpx}`;
    const c = cells.get(key) || { t, price: qpx, bidUsd: 0, askUsd: 0 };
    const sign = aggressorSign(e.aggressor);
    // Convention: sell aggressor hits bid; buy aggressor lifts ask.
    if (sign < 0) c.bidUsd += usd;
    else if (sign > 0) c.askUsd += usd;
    else {
      c.bidUsd += usd / 2;
      c.askUsd += usd / 2;
    }
    cells.set(key, c);
  }
  let rows = [...cells.values()].map((c) => ({
    ...c,
    totalUsd: c.bidUsd + c.askUsd,
    delta: c.askUsd - c.bidUsd,
  }));
  if (rows.length > maxCells) {
    rows = rows.sort((a, b) => b.totalUsd - a.totalUsd).slice(0, maxCells);
  }
  return rows.sort((a, b) => a.t - b.t || a.price - b.price);
}

/**
 * Large-trade / sweep-style markers (honest: tape heuristics, not exchange liquidations).
 * @param {object[]} tape
 * @param {object|null} book
 * @param {{ largeUsd?: number, windowSec?: number|null, nowSec?: number|null }} [opts]
 */
export function flowMarkers(tape, book, opts = {}) {
  const largeUsd = opts.largeUsd ?? 15000;
  const heuristics = detectFlowHeuristics(tape, book, { ...opts, largeUsd });
  return heuristics.map((h) => ({
    ...h,
    // Visual kind for canvas: diamond = large, triangle = sweep, square = absorption.
    marker: h.kind === 'sweep' ? 'triangle' : h.kind === 'absorption' ? 'square' : 'diamond',
    honest: true,
    note:
      h.kind === 'sweep'
        ? 'sweep heuristic (not exchange liquidation)'
        : h.kind === 'absorption'
          ? 'absorption heuristic (not MBO)'
          : 'large print (not exchange liquidation)',
  }));
}

/**
 * COB column rows from latest depth sample (Bookmap-style right rail).
 * @param {object|null|undefined} sample
 * @param {{ priceMin?: number, priceMax?: number, maxRows?: number }} [opts]
 */
export function cobColumn(sample, opts = {}) {
  if (!sample) return { rows: [], maxUsd: 1, bestBid: null, bestAsk: null, mid: sample?.mid ?? null };
  const priceMin = opts.priceMin ?? -Infinity;
  const priceMax = opts.priceMax ?? Infinity;
  const maxRows = opts.maxRows ?? 80;
  /** @type {Map<number, { price: number, bidUsd: number, askUsd: number }>} */
  const byPx = new Map();
  for (const [px, usd] of sample.bids?.entries?.() || []) {
    if (px < priceMin || px > priceMax) continue;
    const r = byPx.get(px) || { price: px, bidUsd: 0, askUsd: 0 };
    r.bidUsd += usd;
    byPx.set(px, r);
  }
  for (const [px, usd] of sample.asks?.entries?.() || []) {
    if (px < priceMin || px > priceMax) continue;
    const r = byPx.get(px) || { price: px, bidUsd: 0, askUsd: 0 };
    r.askUsd += usd;
    byPx.set(px, r);
  }
  let rows = [...byPx.values()].sort((a, b) => b.price - a.price);
  if (rows.length > maxRows) {
    const mid = sample.mid ?? (rows[0].price + rows[rows.length - 1].price) / 2;
    rows = rows
      .slice()
      .sort((a, b) => Math.abs(a.price - mid) - Math.abs(b.price - mid))
      .slice(0, maxRows)
      .sort((a, b) => b.price - a.price);
  }
  let maxUsd = 0;
  let askCum = 0;
  let bidCum = 0;
  const askSide = rows.filter((r) => r.askUsd > 0).sort((a, b) => a.price - b.price);
  const bidSide = rows.filter((r) => r.bidUsd > 0).sort((a, b) => b.price - a.price);
  /** @type {Map<number, { bidCumUsd: number, askCumUsd: number }>} */
  const cum = new Map();
  for (const r of bidSide) {
    bidCum += r.bidUsd;
    cum.set(r.price, { ...(cum.get(r.price) || { bidCumUsd: 0, askCumUsd: 0 }), bidCumUsd: bidCum });
  }
  for (const r of askSide) {
    askCum += r.askUsd;
    cum.set(r.price, { ...(cum.get(r.price) || { bidCumUsd: 0, askCumUsd: 0 }), askCumUsd: askCum });
  }
  rows = rows.map((r) => {
    maxUsd = Math.max(maxUsd, r.bidUsd, r.askUsd);
    const c = cum.get(r.price) || { bidCumUsd: 0, askCumUsd: 0 };
    return { ...r, bidCumUsd: c.bidCumUsd || 0, askCumUsd: c.askCumUsd || 0 };
  });
  return {
    rows,
    maxUsd: maxUsd || 1,
    bestBid: bestPrice(sample.bids, 'max'),
    bestAsk: bestPrice(sample.asks, 'min'),
    mid: sample.mid ?? null,
  };
}

/**
 * Build a dense heatmap grid from depth history for canvas blit.
 * @param {Array<object>} history samples from sampleBookDepth
 * @param {{
 *   priceMin?: number|null,
 *   priceMax?: number|null,
 *   rows?: number,
 *   cols?: number,
 * }} [opts]
 * @returns {{
 *   grid: Float32Array,
 *   rows: number,
 *   cols: number,
 *   priceMin: number,
 *   priceMax: number,
 *   tMin: number,
 *   tMax: number,
 *   maxVal: number,
 *   midPath: Array<{ t: number, mid: number }>,
 * }|null}
 */
export function buildHeatmapGrid(history, opts = {}) {
  const samples = (history || []).filter((s) => s && (s.bids?.size || s.asks?.size));
  if (samples.length < 2) return null;

  const rows = Math.max(16, Math.min(160, opts.rows ?? 80));
  const cols = Math.min(samples.length, opts.cols ?? samples.length);
  const slice = samples.slice(samples.length - cols);

  let priceMin = opts.priceMin;
  let priceMax = opts.priceMax;
  if (priceMin == null || priceMax == null) {
    let lo = Infinity;
    let hi = -Infinity;
    for (const s of slice) {
      for (const px of s.bids.keys()) {
        if (px < lo) lo = px;
        if (px > hi) hi = px;
      }
      for (const px of s.asks.keys()) {
        if (px < lo) lo = px;
        if (px > hi) hi = px;
      }
      if (s.mid != null) {
        if (s.mid < lo) lo = s.mid;
        if (s.mid > hi) hi = s.mid;
      }
    }
    if (!Number.isFinite(lo) || !Number.isFinite(hi) || hi <= lo) return null;
    const pad = (hi - lo) * 0.08 || 1;
    priceMin = lo - pad;
    priceMax = hi + pad;
  }

  const span = priceMax - priceMin || 1;
  const grid = new Float32Array(rows * cols);
  let maxVal = 0;

  for (let c = 0; c < cols; c++) {
    const s = slice[c];
    const fill = (map) => {
      for (const [px, usd] of map) {
        if (px < priceMin || px > priceMax) continue;
        const rowF = ((priceMax - px) / span) * rows;
        const row = Math.min(rows - 1, Math.max(0, Math.floor(rowF)));
        // Spread one tick across neighboring rows so thin books still fill a band.
        const neighbors = row > 0 && row < rows - 1 ? [row - 1, row, row + 1] : [row];
        const share = usd / neighbors.length;
        for (const r of neighbors) {
          const idx = r * cols + c;
          grid[idx] += share;
          if (grid[idx] > maxVal) maxVal = grid[idx];
        }
      }
    };
    fill(s.bids);
    fill(s.asks);
  }

  const midPath = slice
    .filter((s) => s.mid != null)
    .map((s) => ({ t: s.t, mid: s.mid }));

  return {
    grid,
    rows,
    cols,
    priceMin,
    priceMax,
    tMin: slice[0].t,
    tMax: slice[slice.length - 1].t,
    maxVal: maxVal || 1,
    midPath,
    bidAskPath: slice.map((s) => ({
      t: s.t,
      bestBid: bestPrice(s.bids, 'max'),
      bestAsk: bestPrice(s.asks, 'min'),
    })),
  };
}

/**
 * Best price from a price→usd map.
 * @param {Map<number, number>|undefined} map
 * @param {'min'|'max'} which
 */
function bestPrice(map, which) {
  if (!map?.size) return null;
  let best = which === 'max' ? -Infinity : Infinity;
  for (const px of map.keys()) {
    if (which === 'max') {
      if (px > best) best = px;
    } else if (px < best) best = px;
  }
  return Number.isFinite(best) ? best : null;
}

/**
 * Normalize intensity with user heat gain (0.5–2.5).
 * @param {number} value
 * @param {number} maxVal
 * @param {number} [gain]
 */
export function heatIntensity(value, maxVal, gain = 1) {
  if (!(value > 0) || !(maxVal > 0)) return 0;
  const base = Math.log1p(value) / Math.log1p(maxVal);
  // Slight floor so thin books still paint a field, not a 1px filament.
  const shaped = Math.pow(base, 1 / Math.max(0.35, gain));
  return Math.max(0, Math.min(1, 0.08 + shaped * 0.92));
}

/**
 * Classic DOM ladder aligned on a tick grid: bid | price | ask.
 * @param {object|null} book
 * @param {{ depth?: number, tick?: number|null }} [opts]
 */
export function domLadder(book, opts = {}) {
  const depth = Math.max(1, Math.min(50, Number(opts.depth) || 16));
  const tick = opts.tick && opts.tick > 0 ? opts.tick : inferTickSize(book);
  const bidsAll = book?.bids || [];
  const asksAll = book?.asks || [];
  const b0 = Number(bidsAll[0]?.price);
  const a0 = Number(asksAll[0]?.price);
  const focusMid =
    Number.isFinite(b0) && Number.isFinite(a0)
      ? (b0 + a0) / 2
      : Number.isFinite(b0)
        ? b0
        : Number.isFinite(a0)
          ? a0
          : null;
  // Drop stale/outlier stubs that blow the ladder (e.g. 64,285 next to 64,203).
  const maxDist = focusMid != null ? Math.max(focusMid * 0.0025, tick * 80) : Infinity;
  const near = (l) => {
    const px = Number(l.price);
    return Number.isFinite(px) && (focusMid == null || Math.abs(px - focusMid) <= maxDist);
  };
  const bidsRaw = bidsAll.filter(near).slice(0, depth);
  const asksRaw = asksAll.filter(near).slice(0, depth);

  /** @type {Map<number, { bidQty: number, askQty: number, bidUsd: number, askUsd: number }>} */
  const byPx = new Map();

  for (const l of bidsRaw) {
    const px = Number(l.price);
    const qty = Number(l.quantity) || 0;
    if (!Number.isFinite(px) || qty <= 0) continue;
    const key = quantizePrice(px, tick);
    const row = byPx.get(key) || { bidQty: 0, askQty: 0, bidUsd: 0, askUsd: 0 };
    row.bidQty += qty;
    row.bidUsd += px * qty;
    byPx.set(key, row);
  }
  for (const l of asksRaw) {
    const px = Number(l.price);
    const qty = Number(l.quantity) || 0;
    if (!Number.isFinite(px) || qty <= 0) continue;
    const key = quantizePrice(px, tick);
    const row = byPx.get(key) || { bidQty: 0, askQty: 0, bidUsd: 0, askUsd: 0 };
    row.askQty += qty;
    row.askUsd += px * qty;
    byPx.set(key, row);
  }

  const prices = [...byPx.keys()].sort((a, b) => b - a);
  const bestBid = bidsRaw.length ? Number(bidsRaw[0].price) : null;
  const bestAsk = asksRaw.length ? Number(asksRaw[0].price) : null;
  const mid =
    Number.isFinite(bestBid) && Number.isFinite(bestAsk)
      ? (bestBid + bestAsk) / 2
      : Number.isFinite(bestBid)
        ? bestBid
        : Number.isFinite(bestAsk)
          ? bestAsk
          : null;

  const bidPrices = prices.filter((p) => (byPx.get(p)?.bidQty || 0) > 0).sort((a, b) => b - a);
  const askPrices = prices.filter((p) => (byPx.get(p)?.askQty || 0) > 0).sort((a, b) => a - b);
  /** @type {Map<number, { bidCumQty: number, askCumQty: number, bidCumUsd: number, askCumUsd: number }>} */
  const cum = new Map();
  let bq = 0;
  let bu = 0;
  for (const p of bidPrices) {
    const r = byPx.get(p);
    bq += r.bidQty;
    bu += r.bidUsd;
    cum.set(p, { bidCumQty: bq, bidCumUsd: bu, askCumQty: 0, askCumUsd: 0 });
  }
  let aq = 0;
  let au = 0;
  for (const p of askPrices) {
    const r = byPx.get(p);
    aq += r.askQty;
    au += r.askUsd;
    const prev = cum.get(p) || { bidCumQty: 0, bidCumUsd: 0, askCumQty: 0, askCumUsd: 0 };
    cum.set(p, { ...prev, askCumQty: aq, askCumUsd: au });
  }

  let maxUsd = 0;
  let maxCumUsd = 0;
  const rows = prices.map((price) => {
    const r = byPx.get(price);
    const c = cum.get(price) || {
      bidCumQty: 0,
      askCumQty: 0,
      bidCumUsd: 0,
      askCumUsd: 0,
    };
    maxUsd = Math.max(maxUsd, r.bidUsd, r.askUsd);
    maxCumUsd = Math.max(maxCumUsd, c.bidCumUsd || 0, c.askCumUsd || 0);
    const side =
      r.bidQty > 0 && r.askQty > 0 ? 'both' : r.askQty > 0 ? 'ask' : 'bid';
    return {
      price,
      key: String(price),
      side,
      bidQty: r.bidQty,
      askQty: r.askQty,
      bidUsd: r.bidUsd,
      askUsd: r.askUsd,
      bidCumQty: c.bidCumQty || 0,
      askCumQty: c.askCumQty || 0,
      bidCumUsd: c.bidCumUsd || 0,
      askCumUsd: c.askCumUsd || 0,
      imbPct: levelImbalancePct(r.bidQty, r.askQty),
    };
  });

  return {
    rows,
    maxUsd: maxUsd || 1,
    maxCumUsd: maxCumUsd || 1,
    bestBid: Number.isFinite(bestBid) ? bestBid : null,
    bestAsk: Number.isFinite(bestAsk) ? bestAsk : null,
    mid,
    tick,
  };
}

/**
 * Resolve tick override: null/'auto' → infer from book.
 * @param {number|string|null|undefined} tickOpt
 * @param {object|null} book
 */
export function resolveTick(tickOpt, book) {
  if (tickOpt == null || tickOpt === '' || tickOpt === 'auto') return inferTickSize(book);
  const n = Number(tickOpt);
  return Number.isFinite(n) && n > 0 ? n : inferTickSize(book);
}

/**
 * Parse layer visibility flags from csv/string/object.
 * @param {string|object|null|undefined} raw
 */
export function parseOfLayers(raw) {
  const defaults = {
    heat: true,
    bubbles: true,
    mid: true,
    vap: true,
    cvd: true,
    vol: true,
    cob: true,
    candles: true,
    footprint: false,
    markers: true,
  };
  if (raw == null || raw === '') return { ...defaults };
  if (typeof raw === 'object') return { ...defaults, ...raw };
  const parts = String(raw)
    .split(',')
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
  if (!parts.length) return { ...defaults };
  const out = {
    heat: false,
    bubbles: false,
    mid: false,
    vap: false,
    cvd: false,
    vol: false,
    cob: false,
    candles: false,
    footprint: false,
    markers: false,
  };
  for (const p of parts) {
    if (p in out) out[p] = true;
  }
  if (!out.heat && !out.bubbles && !out.candles) out.heat = true;
  return out;
}

/**
 * Serialize layer flags to csv.
 * @param {ReturnType<typeof parseOfLayers>} layers
 */
export function serializeOfLayers(layers) {
  const L = parseOfLayers(layers);
  return Object.entries(L)
    .filter(([, v]) => v)
    .map(([k]) => k)
    .join(',');
}
