/**
 * Hover-legend helpers for the multi-pane crosshair table.
 */

/**
 * Last point at or before `timeSec` (step-hold / OHLC-friendly).
 *
 * @param {Array<{ time: number, value?: number, close?: number }>|null|undefined} points
 * @param {number} timeSec
 * @returns {{ time: number, value: number }|null}
 */
export function samplePointAtTime(points, timeSec) {
  const t = Number(timeSec);
  if (!Array.isArray(points) || !points.length || !Number.isFinite(t)) return null;
  let lo = 0;
  let hi = points.length - 1;
  let best = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const mt = Number(points[mid].time);
    if (!Number.isFinite(mt)) {
      lo = mid + 1;
      continue;
    }
    if (mt <= t) {
      best = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  if (best < 0) return null;
  const p = points[best];
  const value =
    Number.isFinite(p.value) ? Number(p.value)
      : Number.isFinite(p.close) ? Number(p.close)
        : NaN;
  if (!Number.isFinite(value)) return null;
  return { time: Number(p.time), value };
}

/**
 * Format a unix-second for the dense hover table header.
 *
 * @param {number|string|null|undefined} timeSec
 * @returns {string}
 */
export function formatHoverTime(timeSec) {
  if (timeSec == null || timeSec === '') return '—';
  const t = Number(timeSec);
  if (!Number.isFinite(t) || t <= 0) return '—';
  const d = new Date(t * 1000);
  if (Number.isNaN(d.getTime())) return '—';
  const day = d.getUTCDate();
  const mon = d.toLocaleString('en-GB', { month: 'short', timeZone: 'UTC' });
  const yy = String(d.getUTCFullYear()).slice(-2);
  const hh = String(d.getUTCHours()).padStart(2, '0');
  const mm = String(d.getUTCMinutes()).padStart(2, '0');
  const ss = String(d.getUTCSeconds()).padStart(2, '0');
  return `${day} ${mon} '${yy} ${hh}:${mm}:${ss}`;
}

/**
 * @param {object} opts
 * @param {number} opts.timeSec
 * @param {Array<{ venue: string, color?: string, hidden?: boolean, data?: Array<{time:number,value:number}>, last?: number|null, pct?: number|null }>} opts.venues
 * @param {'percent'|'absolute'} [opts.priceMode]
 * @param {Array<{t:number,score:number}>} [opts.pulseHistory]  // ms
 * @param {Array<{t:number,imbalancePct:number}>} [opts.imbalanceHistory] // ms
 * @param {Array<{time:number,value:number}>} [opts.cvdPoints]
 * @param {Array<{sec:number,buyUsd:number,sellUsd:number}>} [opts.histogram]
 * @returns {{
 *   timeLabel: string,
 *   timeSec: number,
 *   venues: Array<{ venue: string, color: string, text: string }>,
 *   indicators: Array<{ id: string, label: string, color: string, text: string }>,
 * }}
 */
export function buildHoverLegend(opts) {
  const timeSec = Number(opts.timeSec);
  const priceMode = opts.priceMode === 'absolute' ? 'absolute' : 'percent';
  const venues = [];
  for (const row of opts.venues || []) {
    if (!row || row.hidden) continue;
    const hit = samplePointAtTime(row.data, timeSec);
    let text = '—';
    if (hit) {
      if (priceMode === 'percent') {
        const sign = hit.value > 0 ? '+' : '';
        text = `${sign}${hit.value.toFixed(3)}%`;
      } else {
        text = Number(hit.value).toLocaleString('en-US', {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        });
      }
    }
    venues.push({
      venue: String(row.venue || ''),
      color: row.color || '#848e9c',
      text,
    });
  }

  const pulsePts = (opts.pulseHistory || [])
    .filter((p) => p && Number.isFinite(p.t) && Number.isFinite(p.score))
    .map((p) => ({ time: p.t / 1000, value: Number(p.score) }));
  const imbPts = (opts.imbalanceHistory || [])
    .filter((p) => p && Number.isFinite(p.t) && Number.isFinite(p.imbalancePct))
    .map((p) => ({ time: p.t / 1000, value: Number(p.imbalancePct) }));

  const pulse = samplePointAtTime(pulsePts, timeSec);
  const imb = samplePointAtTime(imbPts, timeSec);
  const cvd = samplePointAtTime(opts.cvdPoints || [], timeSec);
  const hist = samplePointAtTime(
    (opts.histogram || [])
      .filter((h) => h && Number.isFinite(h.sec))
      .map((h) => ({
        time: h.sec,
        value: Number(h.buyUsd) || 0,
        sell: Number(h.sellUsd) || 0,
      })),
    timeSec,
  );
  // Re-find sell for the matched hist second.
  let buyUsd = null;
  let sellUsd = null;
  if (hist) {
    const raw = (opts.histogram || []).find((h) => h.sec === hist.time);
    buyUsd = raw ? Number(raw.buyUsd) || 0 : hist.value;
    sellUsd = raw ? Number(raw.sellUsd) || 0 : null;
  }

  /** @param {number|null|undefined} n */
  const usd = (n) => {
    if (n == null || !Number.isFinite(n)) return '—';
    const a = Math.abs(n);
    if (a >= 1e6) return `${n < 0 ? '-' : ''}$${(a / 1e6).toFixed(2)}M`;
    if (a >= 1e3) return `${n < 0 ? '-' : ''}$${(a / 1e3).toFixed(1)}K`;
    if (a >= 1) return `${n < 0 ? '-' : ''}$${a.toFixed(0)}`;
    return `${n < 0 ? '-' : ''}$${a.toFixed(2)}`;
  };

  const indicators = [
    {
      id: 'pulse',
      label: 'Pulse',
      color: '#f0b90b',
      text: pulse ? pulse.value.toFixed(0) : '—',
    },
    {
      id: 'imb',
      label: 'Imb',
      color: '#3861fb',
      text: imb ? `${imb.value.toFixed(1)}%` : '—',
    },
    {
      id: 'cvd',
      label: 'CVD',
      color: cvd && cvd.value < 0 ? '#f6465d' : '#02c076',
      text: cvd ? usd(cvd.value) : '—',
    },
    {
      id: 'buy',
      label: 'Buy',
      color: '#02c076',
      text: buyUsd != null ? usd(buyUsd) : '—',
    },
    {
      id: 'sell',
      label: 'Sell',
      color: '#f6465d',
      text: sellUsd != null ? usd(sellUsd) : '—',
    },
  ];

  return {
    timeSec,
    timeLabel: formatHoverTime(timeSec),
    venues,
    indicators,
  };
}
