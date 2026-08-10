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
  };
}

export function emptyDerivatives(reason = 'derivatives_loading') {
  return { available: false, status: 'unavailable', revision: 0, funding: null, openInterest: null, divergence: null, liquidations: [], reason };
}
