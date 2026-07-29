/**
 * Coalesce high-frequency UI updates onto a capped paint cadence.
 * Data can still accrue between paints; only the latest flush runs.
 */

function scheduleFrame(cb) {
  if (typeof requestAnimationFrame === 'function') return requestAnimationFrame(cb);
  return setTimeout(cb, 16);
}

function cancelFrame(id) {
  if (typeof cancelAnimationFrame === 'function') cancelAnimationFrame(id);
  else clearTimeout(id);
}

/**
 * @param {() => void} flush
 * @param {{ minIntervalMs?: number }} [opts]
 */
export function createPaintGate(flush, opts = {}) {
  const minIntervalMs = opts.minIntervalMs ?? 80;
  let raf = 0;
  let timer = 0;
  let dirty = false;
  let lastPaint = 0;

  function run() {
    raf = 0;
    timer = 0;
    if (!dirty) return;
    dirty = false;
    lastPaint = performance.now();
    flush();
  }

  function schedule() {
    dirty = true;
    const now = performance.now();
    const wait = Math.max(0, minIntervalMs - (now - lastPaint));
    if (wait <= 0) {
      if (!raf) raf = scheduleFrame(run);
      return;
    }
    if (!timer && !raf) {
      timer = setTimeout(() => {
        timer = 0;
        if (!raf) raf = scheduleFrame(run);
      }, wait);
    }
  }

  function dispose() {
    if (raf) cancelFrame(raf);
    if (timer) clearTimeout(timer);
    raf = 0;
    timer = 0;
    dirty = false;
  }

  /** Force an immediate flush (e.g. venue switch). */
  function flushNow() {
    if (raf) cancelFrame(raf);
    if (timer) clearTimeout(timer);
    raf = 0;
    timer = 0;
    dirty = false;
    lastPaint = performance.now();
    flush();
  }

  return { schedule, flushNow, dispose };
}

/**
 * Exponential moving average for stable chart scales.
 * @param {number|null} current
 * @param {number} target
 * @param {number} [alpha]
 */
export function ema(current, target, alpha = 0.12) {
  if (current == null || !Number.isFinite(current)) return target;
  if (!Number.isFinite(target)) return current;
  return current + (target - current) * alpha;
}
