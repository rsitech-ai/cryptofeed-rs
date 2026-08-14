/**
 * Persistable market-panel layout (column / dock / plot splits).
 * Session presets still own the time window; this module only stores sizes.
 */

export const LAYOUT_DEFAULTS = {
  bookPx: 250,
  rightPx: 310,
  dockPx: 220,
  mainFrac: 0.58,
  bpsPx: 64,
  casPulse: 1,
  casImb: 1,
  casCvd: 1,
  casVol: 1.15,
};

/**
 * @param {unknown} v
 * @param {number} min
 * @param {number} max
 * @param {number} fallback
 */
export function clampLayoutNum(v, min, max, fallback) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, n));
}

/**
 * @param {unknown} parsed
 */
export function normalizeLayout(parsed) {
  const src = parsed && typeof parsed === 'object' ? parsed : {};
  return {
    bookPx: Math.round(clampLayoutNum(src.layoutBookPx ?? src.bookPx, 180, 420, LAYOUT_DEFAULTS.bookPx)),
    rightPx: Math.round(clampLayoutNum(src.layoutRightPx ?? src.rightPx, 220, 480, LAYOUT_DEFAULTS.rightPx)),
    dockPx: Math.round(clampLayoutNum(src.layoutDockPx ?? src.dockPx, 140, 480, LAYOUT_DEFAULTS.dockPx)),
    mainFrac: clampLayoutNum(src.layoutMainFrac ?? src.mainFrac, 0.28, 0.82, LAYOUT_DEFAULTS.mainFrac),
    bpsPx: Math.round(clampLayoutNum(src.layoutBpsPx ?? src.bpsPx, 40, 160, LAYOUT_DEFAULTS.bpsPx)),
    casPulse: clampLayoutNum(src.layoutCasPulse ?? src.casPulse, 0.4, 3, LAYOUT_DEFAULTS.casPulse),
    casImb: clampLayoutNum(src.layoutCasImb ?? src.casImb, 0.4, 3, LAYOUT_DEFAULTS.casImb),
    casCvd: clampLayoutNum(src.layoutCasCvd ?? src.casCvd, 0.4, 3, LAYOUT_DEFAULTS.casCvd),
    casVol: clampLayoutNum(src.layoutCasVol ?? src.casVol, 0.4, 3, LAYOUT_DEFAULTS.casVol),
  };
}

/**
 * Flatten a layout object into the persistable `layout*` settings keys.
 *
 * @param {unknown} [layout]
 */
export function layoutToSettings(layout = LAYOUT_DEFAULTS) {
  const n = normalizeLayout(layout);
  return {
    layoutBookPx: n.bookPx,
    layoutRightPx: n.rightPx,
    layoutDockPx: n.dockPx,
    layoutMainFrac: n.mainFrac,
    layoutBpsPx: n.bpsPx,
    layoutCasPulse: n.casPulse,
    layoutCasImb: n.casImb,
    layoutCasCvd: n.casCvd,
    layoutCasVol: n.casVol,
  };
}

/**
 * Drag along one axis and report the clamped next size.
 *
 * @param {PointerEvent} event
 * @param {{
 *   axis: 'x' | 'y',
 *   startValue: number,
 *   min: number,
 *   max: number,
 *   invert?: boolean,
 *   scale?: number,
 *   round?: boolean,
 *   onChange: (next: number) => void,
 *   onEnd?: () => void,
 * }} opts
 */
export function beginAxisDrag(event, opts) {
  event.preventDefault();
  const startPtr = opts.axis === 'x' ? event.clientX : event.clientY;
  const startValue = Number(opts.startValue);
  const scaleRaw = Number(opts.scale);
  const scale = Number.isFinite(scaleRaw) && scaleRaw !== 0 ? scaleRaw : 1;
  const move = (ev) => {
    const now = opts.axis === 'x' ? ev.clientX : ev.clientY;
    const delta = (opts.invert ? startPtr - now : now - startPtr) * scale;
    const next = clampLayoutNum(startValue + delta, opts.min, opts.max, startValue);
    opts.onChange(opts.round === false ? next : Math.round(next));
  };
  let ended = false;
  const stop = () => {
    if (ended) return;
    ended = true;
    window.removeEventListener('pointermove', move);
    window.removeEventListener('pointerup', stop);
    window.removeEventListener('pointercancel', stop);
    opts.onEnd?.();
  };
  window.addEventListener('pointermove', move);
  window.addEventListener('pointerup', stop);
  window.addEventListener('pointercancel', stop);
}
