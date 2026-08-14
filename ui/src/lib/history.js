/**
 * Shared history retention for SPA series buffers (24/7-safe budgets).
 *
 * Session presets (1m/5m/1h/2h) remain *view* windows; this module owns the
 * underlying retention SLA (default 2h) so Lines / Candles / Order Flow can
 * switch modes and pan/zoom soak history — while hard-capping memory/CPU.
 */

export const DEFAULT_HISTORY_SECS = 7200;
export const HISTORY_SECS_MIN = 300;
export const HISTORY_SECS_MAX = 7200;

/** Max DOM rows for Market Trades tape (virtualization substitute). */
export const TAPE_DOM_MAX = 160;

/** Max focus-tape trades retained for OF/CVD (separate from DOM). */
export const TAPE_OF_MAX = 4000;

/** Max in-app alert objects retained. */
export const ALERTS_MAX = 24;

/** Max points lightweight-charts should paint per venue (display downsample). */
export const CHART_DISPLAY_MAX_POINTS = 900;

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
 *   maxPoints?: number,
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

  let out = [
    ...[...oldBuckets.entries()].sort((a, b) => a[0] - b[0]).map(([, v]) => v),
    ...[...midBuckets.entries()].sort((a, b) => a[0] - b[0]).map(([, v]) => v),
    ...[...recentBuckets.entries()].sort((a, b) => a[0] - b[0]).map(([, v]) => v),
  ];

  const maxPoints = opts.maxPoints;
  if (maxPoints != null && Number.isFinite(maxPoints) && maxPoints > 0 && out.length > maxPoints) {
    out = strideDownsample(out, maxPoints);
  }
  return out;
}

/**
 * Uniform stride downsample preserving first + last.
 * @template {unknown} T
 * @param {T[]} points
 * @param {number} maxPoints
 * @returns {T[]}
 */
export function strideDownsample(points, maxPoints) {
  const list = Array.isArray(points) ? points : [];
  const cap = Math.max(2, Math.floor(maxPoints));
  if (list.length <= cap) return list.slice();
  const out = [];
  const last = list.length - 1;
  const step = last / (cap - 1);
  let prev = -1;
  for (let i = 0; i < cap; i++) {
    const idx = i === cap - 1 ? last : Math.round(i * step);
    if (idx === prev) continue;
    out.push(list[idx]);
    prev = idx;
  }
  if (out[out.length - 1] !== list[last]) out.push(list[last]);
  return out;
}

/**
 * Chart display downsample: dense recent + sparse older, hard-capped.
 * @template {{ time: number }} T
 * @param {T[]} points
 * @param {number} [windowSec]
 * @param {number} [maxPoints]
 * @returns {T[]}
 */
export function downsampleForChart(points, windowSec = 300, maxPoints = CHART_DISPLAY_MAX_POINTS) {
  const list = Array.isArray(points) ? points : [];
  if (list.length <= maxPoints) return list;
  const tip = list[list.length - 1]?.time;
  const win = Math.max(1, Number(windowSec) || 300);
  const recentSec = Math.min(180, Math.max(60, Math.floor(win * 0.2)));
  const midSec = Math.min(win, Math.max(recentSec * 2, Math.floor(win * 0.55)));
  return downsampleByAge(list, {
    tipSec: tip,
    recentSec,
    midSec,
    recentStep: 1,
    midStep: Math.max(2, Math.ceil(win / maxPoints)),
    oldStep: Math.max(5, Math.ceil((win * 2) / maxPoints)),
    maxPoints,
  });
}

/**
 * Column budget for L2 depth heat rings over `historySecs`.
 * Aggressive 24/7 caps: dense recent, sparse older.
 *
 * @param {number} [historySecs]
 */
export function depthHistoryBudget(historySecs = DEFAULT_HISTORY_SECS) {
  const hs = clampHistorySecs(historySecs);
  const recentSec = Math.min(120, hs);
  const midSec = Math.min(480, Math.max(0, hs - recentSec));
  const oldSec = Math.max(0, hs - recentSec - midSec);
  const cols =
    Math.ceil(recentSec * 4) + Math.ceil(midSec * 1) + Math.ceil(oldSec / 5);
  return {
    historySecs: hs,
    maxCols: Math.min(1800, Math.max(360, cols + 48)),
    recentMs: recentSec * 1000,
    midMs: (recentSec + midSec) * 1000,
    recentStepMs: 250,
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
 * Hard-capped for 24/7 — DOM uses TAPE_DOM_MAX; OF uses TAPE_OF_MAX.
 * @param {number} historySecs
 */
export function tapeMaxEntries(historySecs = DEFAULT_HISTORY_SECS) {
  const hs = clampHistorySecs(historySecs);
  // ~2 trades/sec average budget; hard cap well below previous 40k.
  return Math.min(TAPE_OF_MAX, Math.max(1500, Math.floor(hs * 2)));
}

/**
 * Max BPS sparkline points for historySecs (~1 Hz, display-capped).
 * @param {number} historySecs
 */
export function bpsMaxPoints(historySecs = DEFAULT_HISTORY_SECS) {
  const hs = clampHistorySecs(historySecs);
  return Math.min(CHART_DISPLAY_MAX_POINTS + 120, Math.max(300, Math.min(hs + 60, 1200)));
}

/**
 * Max MultiVenueTracker samples per venue before downsample.
 * @param {number} historySecs
 */
export function venueSampleBudget(historySecs = DEFAULT_HISTORY_SECS) {
  const hs = clampHistorySecs(historySecs);
  return Math.min(8000, Math.max(2000, Math.floor(hs * 1.5)));
}

/**
 * Max 1s (or TF) line buckets to keep for `historySecs`. Must cover the full
 * retention window — a 4200 cap silently dropped 2h of 1s history.
 *
 * @param {number} [historySecs]
 * @param {number} [intervalSec]
 */
export function venueBucketBudget(historySecs = DEFAULT_HISTORY_SECS, intervalSec = 1) {
  const hs = clampHistorySecs(historySecs);
  const step = Math.max(1, Number(intervalSec) || 1);
  return Math.max(900, Math.ceil(hs / step) + 120);
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
