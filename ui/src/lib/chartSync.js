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
      targetTimeScale.setVisibleRange({ from, to });
    } catch {
      /* target may lack data covering the range yet */
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
      try {
        c.timeScale().setVisibleRange({ from, to });
      } catch {
        /* ignore until series has data */
      }
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
