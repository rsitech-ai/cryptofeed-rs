/** Persist/load market-panel UI preferences in localStorage. */

import { parseUrlState } from './urlState.js';

const KEY = 'marketfeed.live.ui.v1';

export const DEFAULTS = {
  asset: 'BTC',
  timeframe: '1s',
  chartMode: 'lines', // lines | candles
  priceMode: 'percent', // percent | absolute
  showVolume: true,
  bookDepth: 16,
  tapeLimit: 120,
  pollFocusMs: 120,
  pollMultiMs: 220,
  hiddenVenues: [],
  pinnedVenues: [],
  marketsGroup: 'asset', // asset | venue | all
  marketsLiveFilter: 'all', // all | live | offline
  marketsKindFilter: 'all', // all | spot | perp
  alertBpsThreshold: 15,
  density: 'comfortable', // compact | comfortable
  sessionPreset: '5m', // 1m | 5m | 1h
  grafanaUrl: '',
  webhookUrl: '',
  activeWatchlist: '',
  /** @type {Array<{ id: string, name: string, assets: string[] }>} */
  watchlists: [],
  tapeMinUsd: 0,
  tapeSideFilter: 'all', // all | buy | sell
  tapeAggregatePrints: false,
  /** Analytics dock: orderflow | pulse | hidden */
  analyticsTab: 'orderflow',
  analyticsOpen: true,
  /** Large trade / sweep highlight threshold (USD notional). */
  largeTradeUsd: 25000,
  /** Pulse spike alert threshold (0–100 score). */
  pulseSpikeThreshold: 72,
};

/**
 * @returns {typeof DEFAULTS}
 */
export function loadSettings() {
  let base = { ...DEFAULTS };
  try {
    const raw = localStorage.getItem(KEY);
    if (raw) {
      const parsed = JSON.parse(raw);
      base = mergeParsed(parsed);
    }
  } catch {
    /* use defaults */
  }
  // URL overrides localStorage on load
  const urlPatch = parseUrlState();
  return { ...base, ...urlPatch, ...normalizeArrays(urlPatch, base) };
}

/**
 * @param {Partial<typeof DEFAULTS>} patch
 */
export function saveSettings(patch) {
  const next = { ...loadSettings(), ...patch };
  try {
    localStorage.setItem(KEY, JSON.stringify(next));
  } catch {
    /* ignore quota */
  }
  return next;
}

/**
 * @param {string} name
 * @param {string[]} assets
 */
export function saveWatchlist(name, assets) {
  const s = loadSettings();
  const id = name.toLowerCase().replace(/\s+/g, '-');
  const existing = s.watchlists.filter((w) => w.id !== id);
  const watchlists = [...existing, { id, name, assets: [...new Set(assets)] }];
  return saveSettings({ watchlists, activeWatchlist: id });
}

/**
 * @param {string} id
 */
export function deleteWatchlist(id) {
  const s = loadSettings();
  const watchlists = s.watchlists.filter((w) => w.id !== id);
  const activeWatchlist = s.activeWatchlist === id ? '' : s.activeWatchlist;
  return saveSettings({ watchlists, activeWatchlist });
}

function mergeParsed(parsed) {
  return {
    ...DEFAULTS,
    ...parsed,
    hiddenVenues: Array.isArray(parsed?.hiddenVenues) ? parsed.hiddenVenues : [],
    pinnedVenues: Array.isArray(parsed?.pinnedVenues) ? parsed.pinnedVenues : [],
    watchlists: Array.isArray(parsed?.watchlists) ? parsed.watchlists : [],
    bookDepth: clampInt(parsed?.bookDepth, 5, 50, DEFAULTS.bookDepth),
    tapeLimit: clampInt(parsed?.tapeLimit, 20, 500, DEFAULTS.tapeLimit),
    pollFocusMs: clampInt(parsed?.pollFocusMs, 80, 2000, DEFAULTS.pollFocusMs),
    pollMultiMs: clampInt(parsed?.pollMultiMs, 100, 5000, DEFAULTS.pollMultiMs),
    alertBpsThreshold: clampNum(parsed?.alertBpsThreshold, 1, 500, DEFAULTS.alertBpsThreshold),
    tapeMinUsd: clampNum(parsed?.tapeMinUsd, 0, 1e9, 0),
    showVolume: parsed?.showVolume !== false,
    marketsGroup: ['asset', 'venue', 'all'].includes(parsed?.marketsGroup)
      ? parsed.marketsGroup
      : DEFAULTS.marketsGroup,
    marketsLiveFilter: ['all', 'live', 'offline'].includes(parsed?.marketsLiveFilter)
      ? parsed.marketsLiveFilter
      : DEFAULTS.marketsLiveFilter,
    marketsKindFilter: ['all', 'spot', 'perp'].includes(parsed?.marketsKindFilter)
      ? parsed.marketsKindFilter
      : DEFAULTS.marketsKindFilter,
    density: ['compact', 'comfortable'].includes(parsed?.density)
      ? parsed.density
      : DEFAULTS.density,
    sessionPreset: ['1m', '5m', '1h'].includes(parsed?.sessionPreset)
      ? parsed.sessionPreset
      : DEFAULTS.sessionPreset,
    tapeSideFilter: ['all', 'buy', 'sell'].includes(parsed?.tapeSideFilter)
      ? parsed.tapeSideFilter
      : DEFAULTS.tapeSideFilter,
    analyticsTab: ['orderflow', 'pulse', 'hidden'].includes(parsed?.analyticsTab)
      ? parsed.analyticsTab
      : DEFAULTS.analyticsTab,
    analyticsOpen: parsed?.analyticsOpen !== false,
    largeTradeUsd: clampNum(parsed?.largeTradeUsd, 0, 1e9, DEFAULTS.largeTradeUsd),
    pulseSpikeThreshold: clampNum(
      parsed?.pulseSpikeThreshold,
      10,
      100,
      DEFAULTS.pulseSpikeThreshold,
    ),
  };
}

function normalizeArrays(urlPatch, base) {
  const out = {};
  if (Array.isArray(urlPatch.hiddenVenues)) out.hiddenVenues = urlPatch.hiddenVenues;
  if (Array.isArray(urlPatch.pinnedVenues)) out.pinnedVenues = urlPatch.pinnedVenues;
  return out;
}

function clampInt(v, min, max, fallback) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, Math.round(n)));
}

function clampNum(v, min, max, fallback) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, n));
}
