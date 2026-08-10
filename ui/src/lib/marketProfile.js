const EMPTY = Object.freeze({
  available: false,
  status: 'unavailable',
  reason: 'profile_payload_missing',
  revision: 0,
  basis: null,
  valueAreaBps: null,
  startNs: null,
  endNs: null,
  high: null,
  low: null,
  vah: null,
  val: null,
  poc: null,
  range: null,
  volume: null,
  tpoCount: null,
  rotationFactor: null,
});

/**
 * Validate the daemon profile DTO without converting exact decimal strings.
 * @param {any} raw
 */
export function normalizeMarketProfile(raw) {
  if (!raw || typeof raw !== 'object') return { ...EMPTY };
  if (raw.schema_version !== 1) {
    return { ...EMPTY, reason: 'profile_schema_unsupported' };
  }
  const status = ['live', 'final', 'degraded', 'unavailable'].includes(raw.status)
    ? raw.status
    : 'unavailable';
  const exact = (value) => (typeof value === 'string' && value.length ? value : null);
  const integer = (value) => (Number.isSafeInteger(value) ? value : null);
  const metricsPresent =
    exact(raw.vah) != null &&
    exact(raw.val) != null &&
    exact(raw.poc) != null &&
    exact(raw.range) != null &&
    exact(raw.total_volume) != null &&
    integer(raw.tpo_count) != null &&
    integer(raw.rotation_factor) != null;
  return {
    available: status !== 'unavailable' && metricsPresent,
    status,
    reason: typeof raw.reason === 'string' && raw.reason ? raw.reason : null,
    revision: integer(raw.revision) ?? 0,
    basis: raw.basis === 'volume' || raw.basis === 'tpo' ? raw.basis : null,
    valueAreaBps: integer(raw.value_area_bps),
    startNs: integer(raw.start_ts_ns),
    endNs: integer(raw.end_ts_ns),
    high: exact(raw.high),
    low: exact(raw.low),
    vah: exact(raw.vah),
    val: exact(raw.val),
    poc: exact(raw.poc),
    range: exact(raw.range),
    volume: exact(raw.total_volume),
    tpoCount: integer(raw.tpo_count),
    rotationFactor: integer(raw.rotation_factor),
  };
}

export function emptyMarketProfile(reason = 'profile_payload_missing') {
  return { ...EMPTY, reason };
}
