/** Format helpers for dense terminal UI. */

export function fmtPrice(v, digits = 2) {
  const n = Number(v);
  if (!Number.isFinite(n)) return '—';
  if (Math.abs(n) >= 1000) return n.toLocaleString('en-US', { minimumFractionDigits: digits, maximumFractionDigits: digits });
  if (Math.abs(n) >= 1) return n.toFixed(Math.max(2, digits));
  return n.toPrecision(6);
}

export function fmtQty(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return '—';
  if (n >= 1000) return n.toLocaleString('en-US', { maximumFractionDigits: 3 });
  if (n >= 1) return n.toFixed(4);
  if (n >= 0.0001) return n.toFixed(6);
  return n.toPrecision(4);
}

export function fmtTotal(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return '—';
  if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(2) + 'K';
  return n.toFixed(4);
}

/** USD notional formatting. */
export function fmtUsd(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return '—';
  if (n >= 1e9) return '$' + (n / 1e9).toFixed(2) + 'B';
  if (n >= 1e6) return '$' + (n / 1e6).toFixed(2) + 'M';
  if (n >= 1e3) return '$' + (n / 1e3).toFixed(1) + 'K';
  if (n >= 1) return '$' + n.toFixed(0);
  return '$' + n.toFixed(2);
}

/** Trades per minute intensity. */
export function fmtTradesPerMin(count, windowSec) {
  const n = Number(count);
  const w = Number(windowSec);
  if (!Number.isFinite(n) || !Number.isFinite(w) || w <= 0) return '—';
  const tpm = (n / w) * 60;
  if (tpm >= 1000) return tpm.toFixed(0) + '/m';
  if (tpm >= 10) return tpm.toFixed(1) + '/m';
  return tpm.toFixed(2) + '/m';
}

export function fmtPct(v, digits = 2) {
  const n = Number(v);
  if (!Number.isFinite(n)) return '—';
  const sign = n > 0 ? '+' : '';
  return `${sign}${n.toFixed(digits)}%`;
}

export function fmtTs(ns) {
  if (ns == null) return '—';
  const ms = Number(ns) / 1e6;
  if (!Number.isFinite(ms)) return '—';
  const d = new Date(ms);
  return d.toISOString().slice(11, 19);
}

export function fmtTsUtcLabel(ns) {
  const t = fmtTs(ns);
  return t === '—' ? t : `${t}Z`;
}

export function fmtCount(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return '—';
  if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (n >= 1e3) return n.toLocaleString('en-US');
  return String(Math.round(n));
}

export function fmtWindowLabel(sec) {
  if (sec < 60) return `${sec}s`;
  if (sec < 3600) return `${Math.round(sec / 60)}m`;
  return `${(sec / 3600).toFixed(sec % 3600 === 0 ? 0 : 1)}h`;
}

export function fmtUtcClock(date = new Date()) {
  return date.toISOString().replace('T', ' ').slice(0, 19) + ' UTC';
}

export function nsToSec(ns) {
  const n = Number(ns);
  if (!Number.isFinite(n)) return null;
  return Math.floor(n / 1e9);
}

export function trimSymbol(symbol) {
  if (!symbol) return '';
  return String(symbol);
}

/** Best-effort pair display: BTCUSDT → BTC/USDT */
export function displayPair(symbol) {
  const s = String(symbol || '');
  const m = s.match(/^([A-Z0-9]+)[-_/]?(USDT|USDC|USD|BTC|ETH|BNB|EUR)$/i);
  if (m) return `${m[1].toUpperCase()}/${m[2].toUpperCase()}`;
  if (s.includes('-') || s.includes('/')) return s.replace('-', '/');
  return s;
}
