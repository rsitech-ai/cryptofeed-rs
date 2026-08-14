export function normalizeDerivatives(raw) {
  if (!raw || raw.schema_version !== 1) return emptyDerivatives('derivatives_payload_invalid');
  return {
    available: raw.status === 'live',
    status: raw.status,
    revision: Number.isSafeInteger(raw.revision) ? raw.revision : 0,
    funding: raw.funding && typeof raw.funding.rate === 'string' ? raw.funding : null,
    openInterest: raw.open_interest && typeof raw.open_interest.quantity === 'string'
      ? raw.open_interest : null,
    divergence: raw.funding_divergence || null,
    liquidations: Array.isArray(raw.liquidations) ? raw.liquidations : [],
    reason: null,
    venue: raw.venue ?? null,
    symbol: raw.symbol ?? null,
  };
}

export function emptyDerivatives(reason = 'derivatives_loading') {
  return { available: false, status: 'unavailable', revision: 0, funding: null, openInterest: null, divergence: null, liquidations: [], reason, venue: null, symbol: null };
}

/**
 * Try the focused market first, then live perps for the same asset.
 * @param {string} focusVenue
 * @param {string} focusSymbol
 * @param {Array<{ venue: string, symbol: string, kind?: string, live?: boolean }>} mapped
 */
export function derivativesFallbackTargets(focusVenue, focusSymbol, mapped = []) {
  const out = [];
  const seen = new Set();
  const push = (venue, symbol) => {
    if (!venue || !symbol) return;
    const key = `${venue}|${symbol}`;
    if (seen.has(key)) return;
    seen.add(key);
    out.push({ venue, symbol });
  };
  push(focusVenue, focusSymbol);
  for (const row of mapped) {
    if (row?.kind === 'perp' && row.live !== false) push(row.venue, row.symbol);
  }
  return out.slice(0, 4);
}
