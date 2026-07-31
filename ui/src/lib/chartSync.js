/**
 * Keep two Lightweight Charts time scales aligned and return an explicit
 * disposer. Call the disposer before removing either chart.
 *
 * Logical-range sync is ideal when panes share the same bar index density
 * (e.g. main price + BPS). Prefer {@link wireVisibleTimeRangeSync} when
 * series densities differ (Pulse/Imb/CVD vs price).
 *
 * @param {any} source
 * @param {any} target
 * @param {{ active: boolean }} guard
 * @returns {() => void}
 */
export function wireVisibleLogicalRangeSync(source, target, guard) {
  const sourceTimeScale = source.timeScale();
  const targetTimeScale = target.timeScale();

  const onVisibleLogicalRangeChange = (range) => {
    if (!range || guard.active) return;
    guard.active = true;
    try {
      targetTimeScale.setVisibleLogicalRange(range);
    } finally {
      guard.active = false;
    }
  };

  sourceTimeScale.subscribeVisibleLogicalRangeChange(onVisibleLogicalRangeChange);

  let disposed = false;
  return () => {
    if (disposed) return;
    disposed = true;
    sourceTimeScale.unsubscribeVisibleLogicalRangeChange(onVisibleLogicalRangeChange);
  };
}

/**
 * Apply a wall-clock visible window. If the exact range is not yet covered by
 * series data, clamp to the overlap with the target's current data extent
 * (via getVisibleRange after a no-op) — otherwise try setVisibleLogicalRange
 * fallback is unavailable, so we clamp using time scale options when present.
 *
 * @param {any} timeScale
 * @param {number} from
 * @param {number} to
 */
export function setVisibleTimeRangeSafe(timeScale, from, to) {
  if (!timeScale || !Number.isFinite(from) || !Number.isFinite(to) || to <= from) return false;
  try {
    timeScale.setVisibleRange({ from, to });
    return true;
  } catch {
    /* target may lack data covering the full range — clamp to overlap */
  }
  try {
    // Prefer keeping the requested window when the library accepts a slightly
    // coerced range (same from/to as numbers). Some LWC builds reject only
    // when entirely outside; retry with a 1s inset.
    const inset = Math.min(1, (to - from) / 4);
    timeScale.setVisibleRange({ from: from + inset, to: to - inset });
    return true;
  } catch {
    return false;
  }
}

/**
 * Time-range sync — aligns wall-clock windows across charts with different
 * bar densities. Uses subscribeVisibleTimeRangeChange / setVisibleRange.
 *
 * @param {any} source
 * @param {any} target
 * @param {{ active: boolean }} guard
 * @returns {() => void}
 */
export function wireVisibleTimeRangeSync(source, target, guard) {
  const sourceTimeScale = source.timeScale();
  const targetTimeScale = target.timeScale();

  const onVisibleTimeRangeChange = (range) => {
    if (!range || guard.active) return;
    const from = Number(range.from);
    const to = Number(range.to);
    if (!Number.isFinite(from) || !Number.isFinite(to) || to <= from) return;
    guard.active = true;
    try {
      setVisibleTimeRangeSafe(targetTimeScale, from, to);
    } finally {
      guard.active = false;
    }
  };

  sourceTimeScale.subscribeVisibleTimeRangeChange(onVisibleTimeRangeChange);

  let disposed = false;
  return () => {
    if (disposed) return;
    disposed = true;
    sourceTimeScale.unsubscribeVisibleTimeRangeChange(onVisibleTimeRangeChange);
  };
}

/**
 * Fan-out: wire source → each target (one direction). Optionally also
 * target → source for bidirectional lockstep.
 *
 * @param {any} source
 * @param {any[]} targets
 * @param {{
 *   active: boolean,
 * }} guard
 * @param {{
 *   mode?: 'time' | 'logical',
 *   bidirectional?: boolean,
 * }} [opts]
 * @returns {() => void}
 */
export function wireChartTimeScales(source, targets, guard, opts = {}) {
  const mode = opts.mode === 'logical' ? 'logical' : 'time';
  const bidirectional = opts.bidirectional === true;
  const wire = mode === 'logical' ? wireVisibleLogicalRangeSync : wireVisibleTimeRangeSync;
  /** @type {Array<() => void>} */
  const disposers = [];
  const list = (targets || []).filter(Boolean);
  for (const t of list) {
    if (t === source) continue;
    disposers.push(wire(source, t, guard));
    if (bidirectional) disposers.push(wire(t, source, guard));
  }
  let disposed = false;
  return () => {
    if (disposed) return;
    disposed = true;
    for (const d of disposers.splice(0)) d();
  };
}

/**
 * Apply a wall-clock visible window to one or more charts (OF / follow-live).
 *
 * @param {any[]} charts
 * @param {{ fromSec: number, toSec: number }} range
 * @param {{ active: boolean }} [guard]
 */
export function applyVisibleTimeRange(charts, range, guard) {
  if (!range) return;
  const from = Number(range.fromSec);
  const to = Number(range.toSec);
  if (!Number.isFinite(from) || !Number.isFinite(to) || to <= from) return;
  const g = guard || { active: false };
  if (g.active) return;
  g.active = true;
  try {
    for (const c of charts || []) {
      if (!c) continue;
      setVisibleTimeRangeSafe(c.timeScale(), from, to);
    }
  } finally {
    g.active = false;
  }
}

export function createRangeActivity() {
  const syncGuard = { active: false };
  let programmaticDepth = 0;

  return {
    syncGuard,
    isUserDriven() {
      return !syncGuard.active && programmaticDepth === 0;
    },
    runProgrammatic(operation) {
      programmaticDepth += 1;
      try {
        return operation();
      } finally {
        programmaticDepth -= 1;
      }
    },
  };
}
