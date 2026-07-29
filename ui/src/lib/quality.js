/** Data quality helpers for market rows. */

import { nsToSec } from './format.js';

const STALE_SEC = 2;

/**
 * @param {object} row - market row
 * @param {object|null} statusVenue - venue status from /v1/status
 * @param {number|null} lastTapeSec - last trade/quote timestamp for this venue+symbol
 * @param {boolean} hasBook - whether L2 book is available
 * @param {boolean} quotesOnly - venue only has quotes, no trades
 */
export function marketQuality(row, statusVenue, lastTapeSec, hasBook, quotesOnly) {
  const now = Math.floor(Date.now() / 1000);
  const stale = lastTapeSec != null && now - lastTapeSec > STALE_SEC;
  const noL2 = !hasBook && !statusVenue?.book_available;
  const lagMs = statusVenue?.feed_lag_ms ?? statusVenue?.lag_ms ?? null;

  /** @type {string[]} */
  const badges = [];
  if (quotesOnly) badges.push('quotes');
  if (noL2) badges.push('no-l2');
  if (stale) badges.push('stale');
  if (lagMs != null && lagMs > 2000) badges.push('lag');

  return { badges, stale, noL2, quotesOnly, lagMs };
}

/**
 * Derive last tape timestamp from entries.
 * @param {object[]} entries
 */
export function lastTapeSec(entries) {
  let max = null;
  for (const e of entries || []) {
    const sec = nsToSec(e.exchange_ts_ns ?? e.receive_ts_ns);
    if (sec != null && (max == null || sec > max)) max = sec;
  }
  return max;
}

/**
 * Check if venue status indicates quotes-only feed.
 * @param {object|null} statusVenue
 */
export function isQuotesOnly(statusVenue) {
  if (!statusVenue) return false;
  if (statusVenue.quotes_only === true) return true;
  if (statusVenue.has_trades === false && statusVenue.has_quotes === true) return true;
  return false;
}
