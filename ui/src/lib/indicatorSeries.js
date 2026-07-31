/**
 * Indicator series helpers — keep Pulse/Imb/CVD on the same unix-second
 * clock as the main Lines chart so LWC setVisibleRange lockstep works.
 */

/**
 * Step-hold sparse samples onto a 1s grid covering [fromSec, toSec].
 * Backfills the first sample backward so panes always have bars across the
 * main visible window — required for LWC setVisibleRange lockstep.
 *
 * @param {Array<{ t: number, v: number }>} pts  `t` in unix seconds
 * @param {number} fromSec
 * @param {number} toSec
 * @returns {Array<{ time: number, value: number }>}
 */
export function stepHoldSeries(pts, fromSec, toSec) {
  const from = Math.floor(Number(fromSec));
  const to = Math.floor(Number(toSec));
  if (!Number.isFinite(from) || !Number.isFinite(to) || to < from) return [];

  /** @type {Array<{ t: number, v: number }>} */
  const sorted = [];
  for (const p of pts || []) {
    if (!p || !Number.isFinite(p.t) || !Number.isFinite(p.v)) continue;
    sorted.push({ t: Math.floor(p.t), v: Number(p.v) });
  }
  sorted.sort((a, b) => a.t - b.t);
  if (!sorted.length) return [];

  /** @type {Array<{ time: number, value: number }>} */
  const out = [];
  let i = 0;
  // Backfill: start with the earliest sample so the grid covers [from, to]
  // even when the first real sample arrives later in the window.
  let last = sorted[0].v;
  while (i < sorted.length && sorted[i].t <= from) {
    last = sorted[i].v;
    i += 1;
  }
  for (let t = from; t <= to; t += 1) {
    while (i < sorted.length && sorted[i].t <= t) {
      last = sorted[i].v;
      i += 1;
    }
    out.push({ time: t, value: last });
  }
  return out;
}

/**
 * Best exchange/receive tip in unix seconds from a tape-like array.
 *
 * @param {Array<{ exchange_ts_ns?: number, receive_ts_ns?: number }>|null|undefined} tape
 * @returns {number|null}
 */
export function tapeTipSec(tape) {
  let best = null;
  for (const e of tape || []) {
    const ns = Number(e?.exchange_ts_ns ?? e?.receive_ts_ns);
    if (!Number.isFinite(ns) || ns <= 0) continue;
    const sec = Math.floor(ns / 1e9);
    if (best == null || sec > best) best = sec;
  }
  return best;
}
