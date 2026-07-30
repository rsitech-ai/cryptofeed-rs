/**
 * Shared ~1h history retention for SPA series buffers.
 *
 * Session presets (1m/5m/1h) remain *view* windows; this module owns the
 * underlying retention SLA so Lines / Candles / Order Flow can switch modes
 * without wiping buffered history.
 */

export const DEFAULT_HISTORY_SECS = 3600;
export const HISTORY_SECS_MIN = 300;
export const HISTORY_SECS_MAX = 7200;

/**
 * @param {unknown} v
 * @param {number} [fallback]
 */
export function clampHistorySecs(v, fallback = DEFAULT_HISTORY_SECS) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(HISTORY_SECS_MAX, Math.max(HISTORY_SECS_MIN, Math.round(n)));
}

/**
 * Earliest second to keep given a data tip and retention window.
 * @param {number} latestSec
 * @param {number} historySecs
 */
export function retentionCutoff(latestSec, historySecs) {
  const tip = Number(latestSec);
  const keep = Math.max(1, Number(historySecs) || DEFAULT_HISTORY_SECS);
  if (!Number.isFinite(tip) || tip <= 0) return 0;
  return tip - keep;
}

/**
 * Drop Map entries whose numeric keys are strictly before cutoffSec.
 * @param {Map<number, unknown>} map
 * @param {number} cutoffSec
 * @returns {number} deleted count
 */
export function trimTimeMap(map, cutoffSec) {
  if (!map || !(map instanceof Map) || cutoffSec <= 0) return 0;
  let n = 0;
  for (const k of map.keys()) {
    if (k < cutoffSec) {
      map.delete(k);
      n += 1;
    }
  }
  return n;
}

/**
 * Tiered downsample for ascending `{ time, ... }` points.
 * Recent window kept dense; mid/old kept at coarser steps to bound memory.
 *
 * @template {{ time: number }} T
 * @param {T[]} points
 * @param {{
 *   tipSec?: number,
 *   recentSec?: number,
 *   midSec?: number,
 *   recentStep?: number,
 *   midStep?: number,
 *   oldStep?: number,
 * }} [opts]
 * @returns {T[]}
 */
export function downsampleByAge(points, opts = {}) {
  const list = Array.isArray(points) ? points : [];
  if (list.length < 3) return list.slice();

  const tipSec =
    opts.tipSec != null && Number.isFinite(opts.tipSec)
      ? opts.tipSec
      : list[list.length - 1].time;
  const recentSec = Math.max(1, opts.recentSec ?? 300);
  const midSec = Math.max(recentSec, opts.midSec ?? 1200);
  const recentStep = Math.max(1, opts.recentStep ?? 1);
  const midStep = Math.max(recentStep, opts.midStep ?? 5);
  const oldStep = Math.max(midStep, opts.oldStep ?? 15);

  const recentStart = tipSec - recentSec;
  const midStart = tipSec - midSec;

  /** @type {Map<number, T>} */
  const recentBuckets = new Map();
  /** @type {Map<number, T>} */
  const midBuckets = new Map();
  /** @type {Map<number, T>} */
  const oldBuckets = new Map();

  for (const p of list) {
    const t = p?.time;
    if (!Number.isFinite(t)) continue;
    if (t >= recentStart) {
      recentBuckets.set(Math.floor(t / recentStep) * recentStep, p);
    } else if (t >= midStart) {
      midBuckets.set(Math.floor(t / midStep) * midStep, p);
    } else {
      oldBuckets.set(Math.floor(t / oldStep) * oldStep, p);
    }
  }

  return [
    ...[...oldBuckets.entries()].sort((a, b) => a[0] - b[0]).map(([, v]) => v),
    ...[...midBuckets.entries()].sort((a, b) => a[0] - b[0]).map(([, v]) => v),
    ...[...recentBuckets.entries()].sort((a, b) => a[0] - b[0]).map(([, v]) => v),
  ];
}

/**
 * Column budget for L2 depth heat rings over `historySecs`.
 * Full-rate recent (~5 Hz), 1 Hz mid, 0.2 Hz older — honest density limit.
 *
 * @param {number} [historySecs]
 */
export function depthHistoryBudget(historySecs = DEFAULT_HISTORY_SECS) {
  const hs = clampHistorySecs(historySecs);
  const recentSec = Math.min(300, hs);
  const midSec = Math.min(900, Math.max(0, hs - recentSec));
  const oldSec = Math.max(0, hs - recentSec - midSec);
  const cols =
    Math.ceil(recentSec * 5) + Math.ceil(midSec * 1) + Math.ceil(oldSec / 5);
  return {
    historySecs: hs,
    maxCols: Math.min(4200, Math.max(480, cols + 64)),
    recentMs: recentSec * 1000,
    midMs: (recentSec + midSec) * 1000,
    recentStepMs: 200,
    midStepMs: 1000,
    oldStepMs: 5000,
  };
}

/**
 * Compact depth samples with tiered column keep + hard max.
 * Prefer denser recent columns; older columns are step-decimated.
 *
 * @param {Array<{ t: number }>} history
 * @param {number} [historySecs]
 * @param {number} [tipMs]
 * @returns {Array<object>}
 */
export function compactDepthHistory(history, historySecs = DEFAULT_HISTORY_SECS, tipMs) {
  const list = Array.isArray(history) ? history.filter((s) => s && Number.isFinite(s.t)) : [];
  if (!list.length) return [];
  const budget = depthHistoryBudget(historySecs);
  const tip =
    tipMs != null && Number.isFinite(tipMs) ? tipMs : list[list.length - 1].t;
  const cutoff = tip - budget.historySecs * 1000;
  const inWindow = list.filter((s) => s.t >= cutoff);
  if (inWindow.length <= budget.maxCols) return inWindow;

  const recentStart = tip - budget.recentMs;
  const midStart = tip - budget.midMs;
  /** @type {Map<number, object>} */
  const kept = new Map();

  for (const s of inWindow) {
    let step = budget.oldStepMs;
    if (s.t >= recentStart) step = budget.recentStepMs;
    else if (s.t >= midStart) step = budget.midStepMs;
    const slot = Math.floor(s.t / step) * step;
    kept.set(slot, s);
  }

  let out = [...kept.entries()].sort((a, b) => a[0] - b[0]).map(([, v]) => v);
  if (out.length > budget.maxCols) {
    out = out.slice(out.length - budget.maxCols);
  }
  return out;
}

/**
 * Max raw focus-tape trades to retain for OF/CVD/VAP over historySecs.
 * @param {number} historySecs
 */
export function tapeMaxEntries(historySecs = DEFAULT_HISTORY_SECS) {
  const hs = clampHistorySecs(historySecs);
  // ~8 trades/sec average budget; hard cap for bursty venues.
  return Math.min(40000, Math.max(8000, Math.floor(hs * 8)));
}

/**
 * Max BPS sparkline points for historySecs (~1 Hz).
 * @param {number} historySecs
 */
export function bpsMaxPoints(historySecs = DEFAULT_HISTORY_SECS) {
  const hs = clampHistorySecs(historySecs);
  return Math.min(HISTORY_SECS_MAX + 120, Math.max(600, hs + 120));
}

/**
 * Max MultiVenueTracker samples per venue before downsample.
 * @param {number} historySecs
 */
export function venueSampleBudget(historySecs = DEFAULT_HISTORY_SECS) {
  const hs = clampHistorySecs(historySecs);
  return Math.min(24000, Math.max(4000, hs * 4));
}

/**
 * Policy object App + trackers share for retention knobs.
 */
export class SeriesHistoryPolicy {
  /** @param {number} [historySecs] */
  constructor(historySecs = DEFAULT_HISTORY_SECS) {
    this.historySecs = clampHistorySecs(historySecs);
  }

  /** @param {unknown} secs */
  setHistorySecs(secs) {
    this.historySecs = clampHistorySecs(secs, this.historySecs);
    return this.historySecs;
  }

  /** Soft keep window for focus tape (history + slack). */
  tapeKeepSec() {
    return this.historySecs + 60;
  }

  tapeMaxEntries() {
    return tapeMaxEntries(this.historySecs);
  }

  depthBudget() {
    return depthHistoryBudget(this.historySecs);
  }

  bpsMaxPoints() {
    return bpsMaxPoints(this.historySecs);
  }

  venueSampleBudget() {
    return venueSampleBudget(this.historySecs);
  }
}
