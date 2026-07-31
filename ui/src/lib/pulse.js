/**
 * Multi-venue market pulse: activity + spread + imbalance → heartbeat score.
 */

import { bookPressure } from './orderflow.js';

/**
 * Median of numeric array (or null).
 * @param {number[]} values
 * @returns {number|null}
 */
export function median(values) {
  const xs = (values || []).filter((v) => Number.isFinite(v)).slice().sort((a, b) => a - b);
  if (!xs.length) return null;
  const mid = Math.floor(xs.length / 2);
  return xs.length % 2 ? xs[mid] : (xs[mid - 1] + xs[mid]) / 2;
}

/**
 * Per-venue pulse chip inputs → activity heat 0..100.
 * @param {{
 *   tradesPerMin?: number|null,
 *   usdPerMin?: number|null,
 *   spreadBps?: number|null,
 *   imbalancePct?: number|null,
 *   live?: boolean,
 * }} v
 */
export function venueHeat(v) {
  if (v?.live === false) return 0;
  const tpm = Math.max(0, Number(v?.tradesPerMin) || 0);
  const usd = Math.max(0, Number(v?.usdPerMin) || 0);
  // Soft saturating curves so one hot venue doesn't clip everything to 100.
  const tScore = 100 * (1 - Math.exp(-tpm / 40));
  const uScore = 100 * (1 - Math.exp(-usd / 250000));
  const imb = Math.abs(Number(v?.imbalancePct) || 0);
  const iScore = Math.min(100, imb * 1.2);
  const spread = Number(v?.spreadBps);
  const sScore = Number.isFinite(spread) ? Math.max(0, 100 - spread * 8) : 50;
  return clamp(0.4 * tScore + 0.35 * uScore + 0.15 * iScore + 0.1 * sScore, 0, 100);
}

/**
 * Aggregate pulse for selected asset across venues.
 * @param {Array<{
 *   venue: string,
 *   symbol?: string,
 *   live?: boolean,
 *   tradesPerMin?: number|null,
 *   usdPerMin?: number|null,
 *   spreadBps?: number|null,
 *   imbalancePct?: number|null,
 *   last?: number|null,
 *   color?: string,
 * }>} venues
 * @param {{ crossBps?: number|null, windowSec?: number }} [opts]
 */
export function computePulse(venues, opts = {}) {
  const list = venues || [];
  const live = list.filter((v) => v.live !== false);
  const windowSec = opts.windowSec ?? 60;

  const tradesPerMin = live.reduce((s, v) => s + (Number(v.tradesPerMin) || 0), 0);
  const usdPerMin = live.reduce((s, v) => s + (Number(v.usdPerMin) || 0), 0);
  const spreads = live.map((v) => Number(v.spreadBps)).filter((n) => Number.isFinite(n));
  const imbs = live.map((v) => Number(v.imbalancePct)).filter((n) => Number.isFinite(n));
  const medianSpread = median(spreads);
  const bookImbalance = median(imbs);
  const crossBps = opts.crossBps != null && Number.isFinite(Number(opts.crossBps))
    ? Number(opts.crossBps)
    : null;

  const chips = list.map((v) => ({
    venue: v.venue,
    symbol: v.symbol,
    live: v.live !== false,
    color: v.color,
    tradesPerMin: v.tradesPerMin ?? null,
    usdPerMin: v.usdPerMin ?? null,
    spreadBps: v.spreadBps ?? null,
    imbalancePct: v.imbalancePct ?? null,
    heat: venueHeat(v),
  }));

  const avgHeat = chips.length
    ? chips.reduce((s, c) => s + c.heat, 0) / chips.length
    : 0;

  // Pulse score blends activity heat with cross-venue tension + imbalance.
  const crossBoost =
    crossBps != null ? Math.min(25, Math.abs(crossBps) * 1.5) : 0;
  const imbBoost =
    bookImbalance != null ? Math.min(15, Math.abs(bookImbalance) * 0.25) : 0;
  const score = clamp(avgHeat * 0.75 + crossBoost + imbBoost, 0, 100);

  return {
    tradesPerMin,
    usdPerMin,
    crossBps,
    medianSpread,
    bookImbalance,
    score,
    chips: chips.sort((a, b) => b.heat - a.heat),
    windowSec,
    venueCount: live.length,
  };
}

/**
 * Snapshot book imbalance % for a venue book.
 * @param {object|null} book
 * @param {number} [depth]
 */
export function bookImbalanceFromSnap(book, depth = 10) {
  if (!book) return null;
  const p = bookPressure(book, depth);
  if (p.bidUsd + p.askUsd <= 0) return null;
  return p.imbalancePct;
}

/**
 * Spread in bps from book BBO.
 * @param {object|null} book
 */
export function spreadBpsFromBook(book) {
  const bid = Number(book?.bids?.[0]?.price);
  const ask = Number(book?.asks?.[0]?.price);
  if (!Number.isFinite(bid) || !Number.isFinite(ask) || bid <= 0 || ask < bid) return null;
  const mid = (bid + ask) / 2;
  return ((ask - bid) / mid) * 10000;
}

/**
 * Push pulse score into history ring.
 * Prefer exchange-tip `tMs` so Pulse shares the Lines chart clock.
 *
 * @param {Array<{ t: number, score: number, tradesPerMin: number, usdPerMin: number }>} history
 * @param {{ score: number, tradesPerMin: number, usdPerMin: number }} point
 * @param {number} [max]
 * @param {number} [tMs] unix epoch ms (exchange tip); defaults to wall clock
 */
export function pushPulseHistory(history, point, max = 120, tMs = Date.now()) {
  const t = Number.isFinite(tMs) ? Number(tMs) : Date.now();
  const next = [
    ...(history || []),
    {
      t,
      score: point.score,
      tradesPerMin: point.tradesPerMin,
      usdPerMin: point.usdPerMin,
    },
  ];
  return next.length > max ? next.slice(next.length - max) : next;
}

/**
 * Detect pulse spike vs recent baseline (mean + k*stdev or absolute jump).
 * @param {Array<{ score: number }>} history
 * @param {number} [threshold]
 * @returns {boolean}
 */
export function pulseSpike(history, threshold = 72) {
  const pts = history || [];
  if (pts.length < 5) return false;
  const last = pts[pts.length - 1].score;
  if (last < threshold) return false;
  const prior = pts.slice(0, -1).map((p) => p.score);
  const mean = prior.reduce((s, v) => s + v, 0) / prior.length;
  const variance =
    prior.reduce((s, v) => s + (v - mean) ** 2, 0) / Math.max(prior.length, 1);
  const stdev = Math.sqrt(variance);
  return last >= mean + Math.max(12, 1.5 * stdev);
}

function clamp(n, lo, hi) {
  return Math.min(hi, Math.max(lo, n));
}
