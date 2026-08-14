/** Persist/load market-panel UI preferences in localStorage. */

import { parseUrlState } from './urlState.js';
import { normalizeLayout } from './layout.js';

const KEY = 'marketfeed.live.ui.v1';

/**
 * Allow only http(s) absolute URLs. Rejects javascript:/data:/relative
 * values that would otherwise reach window.open or fetch.
 *
 * @param {unknown} raw
 * @param {string} [fallback]
 */
export function safeHttpUrl(raw, fallback = '') {
  const s = String(raw ?? '').trim();
  if (!s) return fallback;
  try {
    const u = new URL(s);
    if (u.protocol !== 'http:' && u.protocol !== 'https:') return fallback;
    return u.toString();
  } catch {
    return fallback;
  }
}

export const DEFAULTS = {
  asset: 'BTC',
  timeframe: '1s',
  chartMode: 'lines', // lines | candles | orderflow
  priceMode: 'percent', // percent | absolute
  showVolume: true,
  bookDepth: 16,
  tapeLimit: 120,
  // Focus poll is a fallback when SSE is stale; keep modest to avoid DOM thrash.
  pollFocusMs: 180,
  pollMultiMs: 280,
  hiddenVenues: [],
  pinnedVenues: [],
  marketsGroup: 'asset', // asset | venue | all
  marketsLiveFilter: 'all', // all | live | offline
  marketsKindFilter: 'all', // all | spot | perp
  alertBpsThreshold: 15,
  density: 'comfortable', // compact | comfortable
  sessionPreset: '5m', // 1m | 5m | 1h | 2h
  grafanaUrl: '',
  webhookUrl: '',
  activeWatchlist: '',
  /** @type {Array<{ id: string, name: string, assets: string[] }>} */
  watchlists: [],
  tapeMinUsd: 0,
  tapeSideFilter: 'all', // all | buy | sell
  tapeAggregatePrints: false,
  /** Analytics dock: open (`both`) or `hidden`. Legacy flow|pulse|orderflow map to open. */
  analyticsTab: 'both',
  analyticsOpen: true,
  /** Large trade / sweep highlight threshold (USD notional). */
  largeTradeUsd: 25000,
  /** Pulse spike alert threshold (0–100 score). */
  pulseSpikeThreshold: 72,
  /** Order-flow tick bucket: 'auto' or numeric string. */
  ofTick: 'auto',
  /** Heat intensity gain 0.5–2.5. */
  ofHeat: 1,
  /** Min bubble notional (USD) to draw. */
  ofBubbleMinUsd: 50,
  /** Server-backed Rust bubble signal mode. */
  ofBubbleMode: 'volume',
  /** Market Profile value-area weighting basis. */
  profileBasis: 'volume',
  /** Visible layers csv: heat,bubbles,levels,mid,vap,cvd,vol,cob,candles,footprint,markers */
  ofLayers: 'heat,bubbles,levels,mid,vap,cvd,vol,cob,candles,markers',
  ofLayersVersion: 2,
  /** Order Flow price zoom (1=auto book window; <1 zoom in; >1 zoom out). */
  ofPriceZoom: 1,
  /** Order Flow visible time window seconds (null = use session preset). */
  ofViewSec: null,
  /** When true, Order Flow time axis tracks live edge. */
  ofFollowLive: true,
  ofDomWidth: 260,
  domShowCum: true,
  domShowMbp: true,
  domShowExec: true,
  /**
   * Underlying SPA series retention (seconds). Session presets clip the live
   * *view*; buffers keep ~historySecs so Lines↔Candles↔OF mode switches and
   * pan/zoom still have soak history. Default 2h; URL `historySecs=`.
   */
  historySecs: 7200,
  layoutBookPx: 250,
  layoutRightPx: 310,
  layoutDockPx: 220,
  layoutMainFrac: 0.58,
  layoutBpsPx: 64,
  layoutCasPulse: 1,
  layoutCasImb: 1,
  layoutCasCvd: 1,
  layoutCasVol: 1.15,
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
  const layout = normalizeLayout(parsed);
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
    chartMode: ['lines', 'candles', 'orderflow'].includes(parsed?.chartMode)
      ? parsed.chartMode
      : DEFAULTS.chartMode,
    sessionPreset: ['1m', '5m', '1h', '2h'].includes(parsed?.sessionPreset)
      ? parsed.sessionPreset
      : DEFAULTS.sessionPreset,
    tapeSideFilter: ['all', 'buy', 'sell'].includes(parsed?.tapeSideFilter)
      ? parsed.tapeSideFilter
      : DEFAULTS.tapeSideFilter,
    analyticsTab: normalizeAnalyticsTab(parsed?.analyticsTab),
    analyticsOpen: parsed?.analyticsOpen !== false,
    largeTradeUsd: clampNum(parsed?.largeTradeUsd, 0, 1e9, DEFAULTS.largeTradeUsd),
    pulseSpikeThreshold: clampNum(
      parsed?.pulseSpikeThreshold,
      10,
      100,
      DEFAULTS.pulseSpikeThreshold,
    ),
    ofTick:
      parsed?.ofTick === 'auto' || (parsed?.ofTick != null && Number(parsed.ofTick) > 0)
        ? String(parsed.ofTick)
        : DEFAULTS.ofTick,
    ofHeat: clampNum(parsed?.ofHeat, 0.5, 2.5, DEFAULTS.ofHeat),
    ofBubbleMinUsd: clampNum(parsed?.ofBubbleMinUsd, 0, 1e9, DEFAULTS.ofBubbleMinUsd),
    ofBubbleMode: parsed?.ofBubbleMode === 'delta' ? 'delta' : 'volume',
    profileBasis: parsed?.profileBasis === 'tpo' ? 'tpo' : 'volume',
    ofLayers: normalizeOfLayers(parsed?.ofLayers, parsed?.ofLayersVersion),
    ofLayersVersion: DEFAULTS.ofLayersVersion,
    ofPriceZoom: clampNum(parsed?.ofPriceZoom, 0.25, 6, DEFAULTS.ofPriceZoom),
    ofViewSec:
      parsed?.ofViewSec == null || parsed?.ofViewSec === ''
        ? null
        : clampInt(parsed.ofViewSec, 15, 7200, null),
    ofFollowLive: parsed?.ofFollowLive !== false,
    ofDomWidth: clampInt(parsed?.ofDomWidth, 200, 520, DEFAULTS.ofDomWidth),
    domShowCum: parsed?.domShowCum !== false,
    domShowMbp: parsed?.domShowMbp !== false,
    domShowExec: parsed?.domShowExec !== false,
    grafanaUrl: safeHttpUrl(parsed?.grafanaUrl, DEFAULTS.grafanaUrl),
    webhookUrl: safeHttpUrl(parsed?.webhookUrl, DEFAULTS.webhookUrl),
    historySecs: clampInt(parsed?.historySecs, 300, 7200, DEFAULTS.historySecs),
    layoutBookPx: layout.bookPx,
    layoutRightPx: layout.rightPx,
    layoutDockPx: layout.dockPx,
    layoutMainFrac: layout.mainFrac,
    layoutBpsPx: layout.bpsPx,
    layoutCasPulse: layout.casPulse,
    layoutCasImb: layout.casImb,
    layoutCasCvd: layout.casCvd,
    layoutCasVol: layout.casVol,
  };
}

export function normalizeOfLayers(value, version) {
  const layers = typeof value === 'string' && value ? value : DEFAULTS.ofLayers;
  if (Number(version) >= DEFAULTS.ofLayersVersion || layers.split(',').includes('levels')) {
    return layers;
  }
  return `${layers},levels`;
}

function normalizeArrays(urlPatch, base) {
  const out = {};
  if (Array.isArray(urlPatch.hiddenVenues)) out.hiddenVenues = urlPatch.hiddenVenues;
  if (Array.isArray(urlPatch.pinnedVenues)) out.pinnedVenues = urlPatch.pinnedVenues;
  return out;
}

/** @param {unknown} tab */
function normalizeAnalyticsTab(tab) {
  if (tab === 'hidden') return 'hidden';
  // Legacy flow|pulse|orderflow → single open pane
  if (tab === 'orderflow' || tab === 'flow' || tab === 'pulse' || tab === 'both') {
    return 'both';
  }
  return DEFAULTS.analyticsTab;
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
