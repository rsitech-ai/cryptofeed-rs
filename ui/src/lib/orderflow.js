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
