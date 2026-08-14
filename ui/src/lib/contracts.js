/** Small UI/daemon boundary contracts shared by the SPA and unit tests. */

export function isCurrentMarket(requestVenue, requestSymbol, selectedVenue, selectedSymbol) {
  return requestVenue === selectedVenue && requestSymbol === selectedSymbol;
}

export class Book404Gate {
  constructor(holdMs = 10_000) {
    this.holdMs = Math.max(0, Number(holdMs) || 0);
    /** @type {Map<string, number>} */
    this._until = new Map();
  }

  _key(venue, symbol) {
    return `${venue}|${symbol}`;
  }

  suppress(venue, symbol, now = Date.now()) {
    this._until.set(this._key(venue, symbol), now + this.holdMs);
  }

  isSuppressed(venue, symbol, now = Date.now()) {
    const key = this._key(venue, symbol);
    const until = this._until.get(key);
    if (until == null) return false;
    if (now < until) return true;
    this._until.delete(key);
    return false;
  }

  clear(venue, symbol) {
    this._until.delete(this._key(venue, symbol));
  }

  clearAll() {
    this._until.clear();
  }
}

/**
 * Skip L2 polls for quotes-only venues. Chrome logs every 404 as a console
 * error; `valid_books === 0` from `/v1/status` is the honest skip signal.
 *
 * @param {{
 *   isFocus?: boolean,
 *   validBooks?: number|null,
 *   suppressed?: boolean,
 *   knownBook?: boolean,
 * }} [opts]
 */
export function shouldPollVenueBook(opts = {}) {
  if (opts.suppressed) return false;
  if (opts.knownBook) return true;
  if (opts.validBooks != null && Number(opts.validBooks) <= 0) return false;
  return true;
}

function scalar(value) {
  if (value == null) return undefined;
  if (typeof value === 'string' || typeof value === 'number') return String(value);
  const inner = value.value ?? value;
  const lo = Number(inner.coefficient_lo);
  const hi = Number(inner.coefficient_hi ?? 0);
  const scale = Number(inner.scale);
  if (!Number.isSafeInteger(lo) || hi !== 0 || !Number.isInteger(scale) || scale < 0) {
    return undefined;
  }
  const negative = lo < 0;
  const digits = String(Math.abs(lo)).padStart(scale + 1, '0');
  if (scale === 0) return `${negative ? '-' : ''}${digits}`;
  return `${negative ? '-' : ''}${digits.slice(0, -scale)}.${digits.slice(-scale)}`;
}

function timestampNs(value) {
  if (value == null) return undefined;
  if (typeof value === 'number' || typeof value === 'string') return Number(value);
  return Number(value.ns);
}

export function normalizeReplayEntry(row) {
  if (!row || typeof row !== 'object') return null;
  if (row.kind === 'trade' || row.kind === 'quote') return row;
  const payload = row.payload;
  if (!payload || typeof payload !== 'object') return null;
  const common = {
    venue: row.venue,
    symbol: row.symbol,
    exchange_ts_ns: timestampNs(row.exchange_ts),
    receive_ts_ns: timestampNs(row.receive_ts) ?? 0,
  };
  if (payload.trade) {
    const price = scalar(payload.trade.price);
    const quantity = scalar(payload.trade.quantity);
    if (price == null || quantity == null) return null;
    return {
      kind: 'trade',
      ...common,
      price,
      quantity,
      aggressor: payload.trade.aggressor ?? 'unknown',
      trade_id: payload.trade.trade_id,
    };
  }
  if (payload.quote) {
    const bid_price = scalar(payload.quote.bid_price);
    const ask_price = scalar(payload.quote.ask_price);
    if (bid_price == null || ask_price == null) return null;
    return {
      kind: 'quote',
      ...common,
      bid_price,
      bid_quantity: scalar(payload.quote.bid_quantity),
      ask_price,
      ask_quantity: scalar(payload.quote.ask_quantity),
    };
  }
  return null;
}

export function normalizeReplayEntries(rows) {
  return (rows || []).map(normalizeReplayEntry).filter(Boolean);
}
