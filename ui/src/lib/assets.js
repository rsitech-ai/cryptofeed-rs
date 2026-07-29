/**
 * Map venue symbols onto logical assets (BTC, ETH, …).
 */

const QUOTE_SUFFIXES = [
  'USDT',
  'USDC',
  'USD',
  'EUR',
  'BTC',
  'ETH',
  'BNB',
];

/** Explicit overrides for awkward venue symbols. */
const SYMBOL_BASE_OVERRIDES = {
  BTCUSD_PERP: 'BTC',
  ETHUSD_PERP: 'ETH',
  SOLUSD_PERP: 'SOL',
  XRPUSD_PERP: 'XRP',
  BNBUSD_PERP: 'BNB',
  'BTC-PERPETUAL': 'BTC',
  'ETH-PERPETUAL': 'ETH',
  'BTC-USDT-SWAP': 'BTC',
  'ETH-USDT-SWAP': 'ETH',
  'SOL-USDT-SWAP': 'SOL',
  'XRP-USDT-SWAP': 'XRP',
  'BNB-USDT-SWAP': 'BNB',
  TBTCUSD: 'BTC',
  TETHUSD: 'ETH',
  TSOLUSD: 'SOL',
  TXRPUSD: 'XRP',
  XBTUSD: 'BTC',
  'XBT/USD': 'BTC',
  'XBT/USDT': 'BTC',
  'BTC/USD': 'BTC',
};

/** Venue ids that are clearly perpetual / futures. */
const PERP_VENUE_RE = /(usdm|coinm|swap|linear|perp|futures|inverse)/i;
/** Venue ids that are clearly spot. */
const SPOT_VENUE_RE = /spot/i;
const KNOWN_SPOT = new Set([
  'gemini',
  'bitstamp',
  'bitfinex',
  'coinbase-spot',
  'kraken-spot',
  'binance-spot',
  'bybit-spot',
  'okx-spot',
]);
const KNOWN_PERP = new Set(['deribit', 'binance-usdm', 'binance-coinm', 'okx-swap', 'bybit-linear']);

/**
 * Normalize a venue symbol to a base asset ticker, or null if unknown.
 * @param {string} symbol
 * @returns {string|null}
 */
export function baseAssetOf(symbol) {
  if (!symbol) return null;
  const raw = String(symbol).trim();
  const upper = raw.toUpperCase();
  if (SYMBOL_BASE_OVERRIDES[upper]) return SYMBOL_BASE_OVERRIDES[upper];
  if (SYMBOL_BASE_OVERRIDES[raw]) return SYMBOL_BASE_OVERRIDES[raw];

  // Strip leading exchange prefixes (Bitfinex tBTCUSD).
  let s = upper.replace(/^T(?=[A-Z])/, '');
  s = s.replace(/[-_/]/g, '');

  // Drop contract suffixes first.
  s = s.replace(/(PERPETUAL|PERP|SWAP|SPOT)$/g, '');

  for (const q of QUOTE_SUFFIXES) {
    if (s.endsWith(q) && s.length > q.length) {
      const base = s.slice(0, -q.length);
      if (base === 'XBT') return 'BTC';
      if (base.length >= 2 && base.length <= 6) return base;
    }
  }

  if (s.startsWith('XBT')) return 'BTC';
  if (s === 'BTC' || s.startsWith('BTC')) return 'BTC';
  if (s === 'ETH' || s.startsWith('ETH')) return 'ETH';
  return null;
}

/**
 * @param {{ venues?: Array<{ id: string, symbols?: Array<{ symbol: string }> }> }} instruments
 * @returns {string[]} sorted unique base assets
 */
export function listAssets(instruments) {
  const set = new Set();
  for (const v of instruments?.venues || []) {
    for (const s of v.symbols || []) {
      const base = baseAssetOf(s.symbol);
      if (base) set.add(base);
    }
  }
  return [...set].sort((a, b) => {
    const rank = { BTC: 0, ETH: 1, SOL: 2, XRP: 3, BNB: 4 };
    return (rank[a] ?? 50) - (rank[b] ?? 50) || a.localeCompare(b);
  });
}

/**
 * Classify a market as spot / perp / other from venue id + symbol.
 * @param {string} venueId
 * @param {string} [symbol]
 * @returns {'spot'|'perp'|'other'}
 */
export function marketKind(venueId, symbol = '') {
  const id = String(venueId || '').toLowerCase();
  const sym = String(symbol || '').toUpperCase();
  if (/(PERPETUAL|PERP|SWAP)/.test(sym)) return 'perp';
  if (KNOWN_PERP.has(id) || PERP_VENUE_RE.test(id)) return 'perp';
  if (KNOWN_SPOT.has(id) || SPOT_VENUE_RE.test(id)) return 'spot';
  return 'other';
}

/**
 * Short label for venue kind chips.
 * @param {'spot'|'perp'|'other'} kind
 */
export function kindLabel(kind) {
  if (kind === 'spot') return 'spot';
  if (kind === 'perp') return 'perp';
  return '—';
}

/**
 * @param {{ venues?: Array<{ id: string, adapter?: string, symbols?: Array<{ symbol: string, instrument?: number }> }> }} instruments
 * @param {string} asset
 * @param {{ venues?: Array<{ id: string, live?: boolean }> }|null} status
 * @returns {Array<{ venue: string, adapter: string, symbol: string, instrument: number|null, live: boolean, kind: 'spot'|'perp'|'other', asset: string }>}
 */
export function mapAssetToVenues(instruments, asset, status = null) {
  if (!asset) return [];
  const liveMap = new Map((status?.venues || []).map((v) => [v.id, !!v.live]));
  const out = [];
  for (const v of instruments?.venues || []) {
    for (const s of v.symbols || []) {
      if (baseAssetOf(s.symbol) !== asset) continue;
      out.push({
        venue: v.id,
        adapter: v.adapter || '',
        symbol: s.symbol,
        instrument: s.instrument ?? null,
        live: liveMap.has(v.id) ? liveMap.get(v.id) : true,
        kind: marketKind(v.id, s.symbol),
        asset,
      });
      break; // one symbol per venue for this asset
    }
  }
  return out;
}

/**
 * Flat market rows across all instruments, with asset + kind + live.
 * @param {{ venues?: Array<{ id: string, adapter?: string, symbols?: Array<{ symbol: string, instrument?: number }> }> }} instruments
 * @param {{ venues?: Array<{ id: string, live?: boolean, events_dispatched?: number }> }|null} status
 * @returns {Array<{ venue: string, adapter: string, symbol: string, instrument: number|null, live: boolean, kind: 'spot'|'perp'|'other', asset: string|null, events: number }>}
 */
export function listMarkets(instruments, status = null) {
  const liveMap = new Map((status?.venues || []).map((v) => [v.id, v]));
  const out = [];
  for (const v of instruments?.venues || []) {
    const live = liveMap.get(v.id);
    for (const s of v.symbols || []) {
      out.push({
        venue: v.id,
        adapter: v.adapter || '',
        symbol: s.symbol,
        instrument: s.instrument ?? null,
        live: live ? !!live.live : true,
        kind: marketKind(v.id, s.symbol),
        asset: baseAssetOf(s.symbol),
        events: live?.events_dispatched ?? 0,
      });
    }
  }
  return out;
}

/**
 * Per-asset venue coverage for the asset picker.
 * @param {{ venues?: Array }} instruments
 * @param {{ venues?: Array }|null} status
 * @returns {Array<{ asset: string, total: number, live: number, venues: string[] }>}
 */
export function assetCoverage(instruments, status = null) {
  return listAssets(instruments).map((asset) => {
    const mapped = mapAssetToVenues(instruments, asset, status);
    return {
      asset,
      total: mapped.length,
      live: mapped.filter((m) => m.live).length,
      venues: mapped.map((m) => m.venue),
    };
  });
}

/** Distinct neon palette for overlaid venue lines. */
export const VENUE_COLORS = [
  '#f0b90b',
  '#02c076',
  '#3b82f6',
  '#f6465d',
  '#a78bfa',
  '#22d3ee',
  '#fb923c',
  '#e879f9',
  '#84cc16',
  '#f472b6',
  '#38bdf8',
  '#c084fc',
  '#facc15',
];

export function colorForVenue(venue, index = 0) {
  let h = 0;
  const s = String(venue || '');
  for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
  return VENUE_COLORS[(h || index) % VENUE_COLORS.length];
}
