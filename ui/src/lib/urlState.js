/** Sync UI state with URL query params (URL overrides localStorage on load). */

import { DEFAULTS } from './settings.js';
import { TIMEFRAMES } from './series.js';
import { SESSION_PRESETS } from './session.js';

const TF_IDS = new Set(TIMEFRAMES.map((t) => t.id));
const SESSION_IDS = new Set(SESSION_PRESETS.map((s) => s.id));

/**
 * Parse URL search params into a partial settings patch.
 * @returns {Record<string, unknown>}
 */
export function parseUrlState() {
  const p = new URLSearchParams(window.location.search);
  /** @type {Record<string, unknown>} */
  const out = {};

  const asset = p.get('asset');
  if (asset) out.asset = asset.toUpperCase();

  const mode = p.get('mode');
  if (mode === 'lines' || mode === 'candles' || mode === 'orderflow') out.chartMode = mode;

  const tf = p.get('tf');
  if (tf && TF_IDS.has(tf)) out.timeframe = tf;

  const price = p.get('price');
  if (price === 'percent' || price === 'absolute') out.priceMode = price;

  const depth = p.get('depth');
  if (depth != null) out.bookDepth = Number(depth);

  const alertBps = p.get('alertBps');
  if (alertBps != null) out.alertBpsThreshold = Number(alertBps);

  const density = p.get('density');
  if (density === 'compact' || density === 'comfortable') out.density = density;

  const session = p.get('session');
  if (session && SESSION_IDS.has(session)) out.sessionPreset = session;

  const vol = p.get('vol');
  if (vol === '0' || vol === 'false') out.showVolume = false;
  if (vol === '1' || vol === 'true') out.showVolume = true;

  const watchlist = p.get('watchlist');
  if (watchlist) out.activeWatchlist = watchlist;

  const pinned = p.get('pinned');
  if (pinned) out.pinnedVenues = pinned.split(',').filter(Boolean);

  const hidden = p.get('hidden');
  if (hidden) out.hiddenVenues = hidden.split(',').filter(Boolean);

  const grafana = p.get('grafana');
  if (grafana) out.grafanaUrl = grafana;

  const tab = p.get('tab');
  // Legacy tab=flow|pulse|orderflow|both → open single pane; only hidden stays hidden.
  if (tab === 'hidden') out.analyticsTab = 'hidden';
  else if (
    tab === 'orderflow' ||
    tab === 'flow' ||
    tab === 'pulse' ||
    tab === 'both'
  ) {
    out.analyticsTab = 'both';
  }

  const dock = p.get('dock');
  if (dock === '0' || dock === 'false') out.analyticsOpen = false;
  if (dock === '1' || dock === 'true') out.analyticsOpen = true;

  const largeUsd = p.get('largeUsd');
  if (largeUsd != null) out.largeTradeUsd = Number(largeUsd);

  const pulseAlert = p.get('pulseAlert');
  if (pulseAlert != null) out.pulseSpikeThreshold = Number(pulseAlert);

  const ofTick = p.get('ofTick');
  if (ofTick === 'auto' || (ofTick != null && Number(ofTick) > 0)) out.ofTick = ofTick;

  const ofHeat = p.get('ofHeat');
  if (ofHeat != null) out.ofHeat = Number(ofHeat);

  const ofBubble = p.get('ofBubble');
  if (ofBubble != null) out.ofBubbleMinUsd = Number(ofBubble);

  const ofLayers = p.get('ofLayers');
  if (ofLayers) out.ofLayers = ofLayers;

  const ofPriceZoom = p.get('ofZoom');
  if (ofPriceZoom != null) out.ofPriceZoom = Number(ofPriceZoom);

  const ofViewSec = p.get('ofView');
  if (ofViewSec != null) out.ofViewSec = Number(ofViewSec);

  const ofFollow = p.get('ofLive');
  if (ofFollow === '0' || ofFollow === 'false') out.ofFollowLive = false;
  if (ofFollow === '1' || ofFollow === 'true') out.ofFollowLive = true;

  return out;
}

/**
 * Build URL search string from current UI state.
 * @param {Record<string, unknown>} state
 */
export function buildUrlState(state) {
  const p = new URLSearchParams();
  const s = { ...DEFAULTS, ...state };

  if (s.asset) p.set('asset', String(s.asset));
  if (s.chartMode && s.chartMode !== DEFAULTS.chartMode) p.set('mode', s.chartMode);
  if (s.timeframe && s.timeframe !== DEFAULTS.timeframe) p.set('tf', s.timeframe);
  if (s.priceMode && s.priceMode !== DEFAULTS.priceMode) p.set('price', s.priceMode);
  if (s.bookDepth && s.bookDepth !== DEFAULTS.bookDepth) p.set('depth', String(s.bookDepth));
  if (s.alertBpsThreshold != null && s.alertBpsThreshold !== DEFAULTS.alertBpsThreshold) {
    p.set('alertBps', String(s.alertBpsThreshold));
  }
  if (s.density && s.density !== DEFAULTS.density) p.set('density', s.density);
  if (s.sessionPreset && s.sessionPreset !== DEFAULTS.sessionPreset) {
    p.set('session', s.sessionPreset);
  }
  if (s.showVolume === false) p.set('vol', '0');
  if (s.activeWatchlist) p.set('watchlist', s.activeWatchlist);
  if (Array.isArray(s.pinnedVenues) && s.pinnedVenues.length) {
    p.set('pinned', s.pinnedVenues.join(','));
  }
  if (Array.isArray(s.hiddenVenues) && s.hiddenVenues.length) {
    p.set('hidden', s.hiddenVenues.join(','));
  }
  if (s.grafanaUrl) p.set('grafana', s.grafanaUrl);
  if (s.analyticsTab === 'hidden') {
    p.set('tab', 'hidden');
  }
  // Do not emit legacy flow|pulse section params; open pane is the default.
  if (s.analyticsOpen === false) p.set('dock', '0');
  if (s.largeTradeUsd != null && s.largeTradeUsd !== DEFAULTS.largeTradeUsd) {
    p.set('largeUsd', String(s.largeTradeUsd));
  }
  if (
    s.pulseSpikeThreshold != null &&
    s.pulseSpikeThreshold !== DEFAULTS.pulseSpikeThreshold
  ) {
    p.set('pulseAlert', String(s.pulseSpikeThreshold));
  }
  if (s.ofTick != null && s.ofTick !== DEFAULTS.ofTick) p.set('ofTick', String(s.ofTick));
  if (s.ofHeat != null && s.ofHeat !== DEFAULTS.ofHeat) p.set('ofHeat', String(s.ofHeat));
  if (s.ofBubbleMinUsd != null && s.ofBubbleMinUsd !== DEFAULTS.ofBubbleMinUsd) {
    p.set('ofBubble', String(s.ofBubbleMinUsd));
  }
  if (s.ofLayers != null && s.ofLayers !== DEFAULTS.ofLayers) {
    p.set('ofLayers', String(s.ofLayers));
  }
  if (s.ofPriceZoom != null && s.ofPriceZoom !== DEFAULTS.ofPriceZoom) {
    p.set('ofZoom', String(s.ofPriceZoom));
  }
  if (s.ofViewSec != null && s.ofViewSec !== DEFAULTS.ofViewSec) {
    p.set('ofView', String(s.ofViewSec));
  }
  if (s.ofFollowLive === false) p.set('ofLive', '0');

  return p.toString();
}

/**
 * Replace browser URL without navigation.
 * @param {Record<string, unknown>} state
 */
export function syncUrl(state) {
  const qs = buildUrlState(state);
  const url = qs ? `${window.location.pathname}?${qs}` : window.location.pathname;
  window.history.replaceState(null, '', url);
}
