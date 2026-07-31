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
 * True when two wall-clock ranges match within `eps` seconds.
 * Used to suppress redundant setVisibleRange that otherwise fights
 * scrollToRealTime / auto-scale and jitters stacked panes.
 *
 * @param {{ from?: number, to?: number }|null|undefined} a
 * @param {{ from?: number, to?: number }|null|undefined} b
 * @param {number} [eps]
 */
export function visibleTimeRangesNearlyEqual(a, b, eps = 0.05) {
  if (!a || !b) return false;
  const af = Number(a.from);
  const at = Number(a.to);
  const bf = Number(b.from);
  const bt = Number(b.to);
  if (![af, at, bf, bt].every(Number.isFinite)) return false;
  return Math.abs(af - bf) <= eps && Math.abs(at - bt) <= eps;
}

/**
 * Apply a wall-clock visible window. Skips the write when the target already
 * shows nearly the same range (avoids left/right plot-area jitter). If the
 * exact range is not yet covered by series data, retry with a small inset.
 *
 * @param {any} timeScale
 * @param {number} from
 * @param {number} to
 * @param {{ eps?: number }} [opts]
 */
export function setVisibleTimeRangeSafe(timeScale, from, to, opts = {}) {
  if (!timeScale || !Number.isFinite(from) || !Number.isFinite(to) || to <= from) return false;
  const eps = Number.isFinite(opts.eps) ? opts.eps : 0.05;
  try {
    const cur = timeScale.getVisibleRange?.();
    if (visibleTimeRangesNearlyEqual(cur, { from, to }, eps)) return true;
  } catch {
    /* proceed to set */
  }
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
  /** @type {{ from: number, to: number }|null} */
  let lastPushed = null;

  const onVisibleTimeRangeChange = (range) => {
    if (!range || guard.active) return;
    const from = Number(range.from);
    const to = Number(range.to);
    if (!Number.isFinite(from) || !Number.isFinite(to) || to <= from) return;
    const next = { from, to };
    // Skip no-op / float-noise updates so slave panes don't thrash.
    if (visibleTimeRangesNearlyEqual(lastPushed, next)) return;
    guard.active = true;
    try {
      if (setVisibleTimeRangeSafe(targetTimeScale, from, to)) {
        lastPushed = next;
      }
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

/**
 * Normalize LWC crosshair times to integer unix seconds so every pane
 * samples the same bar and the stack overlay X stays stable.
 *
 * @param {number|string|null|undefined} time
 * @returns {number|null}
 */
export function normalizeCrosshairTime(time) {
  if (time == null || time === '') return null;
  if (typeof time === 'object') return null;
  const n = Number(time);
  if (!Number.isFinite(n)) return null;
  return Math.floor(n);
}

/**
 * Resolve a Y price for setCrosshairPosition on a target series.
 * Prefers the bar at the shared logical index; falls back to 0 so the
 * vertical line still paints at `time`.
 *
 * @param {any} chart
 * @param {any} series
 * @param {number|string} time
 * @param {number} [fallbackPrice]
 */
export function crosshairPriceForSeries(chart, series, time, fallbackPrice = 0) {
  if (!chart || !series) return fallbackPrice;
  const t = normalizeCrosshairTime(time) ?? time;
  try {
    const logical = chart.timeScale().timeToIndex?.(t, true);
    if (logical != null && Number.isFinite(logical)) {
      const bar = series.dataByIndex?.(logical, -1);
      if (bar) {
        if (Number.isFinite(bar.value)) return bar.value;
        if (Number.isFinite(bar.close)) return bar.close;
      }
    }
  } catch {
    /* ignore */
  }
  return fallbackPrice;
}

/**
 * Programmatically place (or clear) the crosshair on every entry.
 *
 * @param {Array<{ chart: any, series: any }|null|undefined>} entries
 * @param {number|string|null|undefined} time
 * @param {{ active: boolean }} [guard]
 * @param {number} [priceHint]
 */
export function setCrosshairOnCharts(entries, time, guard, priceHint = 0) {
  const list = (entries || []).filter((e) => e?.chart && e?.series);
  if (!list.length) return;
  const g = guard || { active: false };
  if (g.active) return;
  g.active = true;
  try {
    if (time == null) {
      for (const e of list) {
        try {
          e.chart.clearCrosshairPosition();
        } catch {
          /* ignore */
        }
      }
      return;
    }
    const t = normalizeCrosshairTime(time) ?? time;
    for (const e of list) {
      try {
        const price = crosshairPriceForSeries(e.chart, e.series, t, priceHint);
        e.chart.setCrosshairPosition(price, t, e.series);
      } catch {
        /* series may be empty */
      }
    }
  } finally {
    g.active = false;
  }
}

/**
 * Sync crosshairs across independent Lightweight Charts instances.
 * Coalesces to one fan-out per animation frame, snaps to unix seconds, and
 * briefly ignores echo callbacks from setCrosshairPosition so the line
 * does not wiggle between panes.
 *
 * @param {Array<{ chart: any, series: any }|null|undefined>} entries
 * @param {{ active: boolean }} guard
 * @param {{
 *   onMove?: (payload: {
 *     time: number|string|null,
 *     point: { x: number, y: number }|null,
 *     source: any,
 *     param: any,
 *   }) => void,
 *   echoQuietMs?: number,
 * }} [opts]
 * @returns {() => void}
 */
export function wireCrosshairSync(entries, guard, opts = {}) {
  const list = (entries || []).filter((e) => e?.chart && e?.series);
  const onMove = typeof opts.onMove === 'function' ? opts.onMove : null;
  const echoQuietMs = Number.isFinite(opts.echoQuietMs) ? opts.echoQuietMs : 48;
  if (!list.length) return () => {};

  /** @type {Array<{ chart: any, handler: (param: any) => void }>} */
  const subs = [];
  let quietUntil = 0;
  /** @type {number|null} */
  let lastTime = null;
  let raf = 0;
  /** @type {{ entry: any, param: any, time: number|null, left: boolean }|null} */
  let pending = null;

  const schedule =
    typeof requestAnimationFrame === 'function'
      ? (fn) => requestAnimationFrame(fn)
      : (fn) => setTimeout(fn, 0);
  const cancelSchedule =
    typeof cancelAnimationFrame === 'function'
      ? (id) => cancelAnimationFrame(id)
      : (id) => clearTimeout(id);

  const flush = () => {
    raf = 0;
    const job = pending;
    pending = null;
    if (!job || guard.active) return;

    if (job.left) {
      lastTime = null;
      quietUntil = performance.now() + echoQuietMs;
      setCrosshairOnCharts(list, null, guard);
      onMove?.({ time: null, point: null, source: job.entry.chart, param: job.param });
      return;
    }

    const time = job.time;
    if (time == null) return;
    const own = job.param?.seriesData?.get?.(job.entry.series);
    const priceHint = Number.isFinite(own?.value)
      ? Number(own.value)
      : Number.isFinite(own?.close)
        ? Number(own.close)
        : 0;

    // Same second: still notify for overlay X, but skip peer setCrosshair churn.
    const timeChanged = lastTime !== time;
    lastTime = time;
    if (timeChanged) {
      quietUntil = performance.now() + echoQuietMs;
      guard.active = true;
      try {
        for (const e of list) {
          if (e.chart === job.entry.chart) continue;
          try {
            const price = crosshairPriceForSeries(e.chart, e.series, time, priceHint);
            e.chart.setCrosshairPosition(price, time, e.series);
          } catch {
            /* ignore */
          }
        }
      } finally {
        guard.active = false;
      }
    }

    onMove?.({
      time,
      point: job.param?.point
        ? { x: job.param.point.x, y: job.param.point.y }
        : null,
      source: job.entry.chart,
      param: job.param,
    });
  };

  for (const entry of list) {
    const handler = (param) => {
      if (guard.active) return;
      if (performance.now() < quietUntil) return;
      const left =
        !param ||
        param.time == null ||
        !param.point ||
        param.point.x < 0 ||
        param.point.y < 0;
      const time = left ? null : normalizeCrosshairTime(param.time);
      if (!left && time == null) return;
      pending = { entry, param, time, left };
      if (!raf) raf = schedule(flush);
    };

    entry.chart.subscribeCrosshairMove(handler);
    subs.push({ chart: entry.chart, handler });
  }

  let disposed = false;
  return () => {
    if (disposed) return;
    disposed = true;
    if (raf) cancelSchedule(raf);
    raf = 0;
    pending = null;
    for (const s of subs) {
      try {
        s.chart.unsubscribeCrosshairMove(s.handler);
      } catch {
        /* ignore */
      }
    }
    subs.length = 0;
  };
}

/**
 * Map a unix-second (or business-day string) to an X pixel inside the chart
 * container, or null when off-scale.
 *
 * @param {any} chart
 * @param {number|string} time
 * @returns {number|null}
 */
export function timeToCoordinateSafe(chart, time) {
  if (!chart || time == null) return null;
  try {
    const x = chart.timeScale().timeToCoordinate(time);
    return x == null || !Number.isFinite(x) ? null : x;
  } catch {
    return null;
  }
}
