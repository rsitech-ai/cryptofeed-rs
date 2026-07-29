/** Persist/load market-panel UI preferences in localStorage. */

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
  marketsGroup: 'asset', // asset | venue | all
  marketsLiveFilter: 'all', // all | live | offline
  marketsKindFilter: 'all', // all | spot | perp
};

/**
 * @returns {typeof DEFAULTS}
 */
export function loadSettings() {
  try {
    const raw = localStorage.getItem(KEY);
    if (!raw) return { ...DEFAULTS };
    const parsed = JSON.parse(raw);
    return {
      ...DEFAULTS,
      ...parsed,
      hiddenVenues: Array.isArray(parsed?.hiddenVenues) ? parsed.hiddenVenues : [],
      bookDepth: clampInt(parsed?.bookDepth, 5, 50, DEFAULTS.bookDepth),
      tapeLimit: clampInt(parsed?.tapeLimit, 20, 500, DEFAULTS.tapeLimit),
      pollFocusMs: clampInt(parsed?.pollFocusMs, 80, 2000, DEFAULTS.pollFocusMs),
      pollMultiMs: clampInt(parsed?.pollMultiMs, 100, 5000, DEFAULTS.pollMultiMs),
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
    };
  } catch {
    return { ...DEFAULTS };
  }
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

function clampInt(v, min, max, fallback) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(max, Math.max(min, Math.round(n)));
}
