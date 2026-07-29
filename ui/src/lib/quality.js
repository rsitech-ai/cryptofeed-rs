/** Debounced quality badges so STALE/LAG don't mount/unmount every second. */

import { nsToSec } from './format.js';

/** Raw stale threshold (seconds since last tape). */
const STALE_SEC = 8;
/** Lag threshold before a lag badge is considered. */
const LAG_MS = 3000;
/** Must stay bad this long before the badge appears. */
const SHOW_HOLD_MS = 4000;
/** Must stay clear this long before the badge disappears. */
const CLEAR_HOLD_MS = 6000;

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
  // Only show no-l2 when daemon explicitly says so — inferred !hasBook flaps during warmup.
  if (statusVenue?.book_available === false) badges.push('no-l2');
  if (stale) badges.push('stale');
  if (lagMs != null && lagMs > LAG_MS) badges.push('lag');

  return { badges, stale, noL2, quotesOnly, lagMs };
}

/**
 * Hysteresis gate for badge lists keyed by market id.
 * Stable badges only flip after SHOW_HOLD_MS / CLEAR_HOLD_MS.
 */
export class QualityBadgeGate {
  constructor(opts = {}) {
    this.showHoldMs = opts.showHoldMs ?? SHOW_HOLD_MS;
    this.clearHoldMs = opts.clearHoldMs ?? CLEAR_HOLD_MS;
    /** @type {Map<string, { shown: Set<string>, pendingOn: Map<string, number>, pendingOff: Map<string, number> }>} */
    this._state = new Map();
  }

  /**
   * @param {string} key
   * @param {string[]} rawBadges
   * @param {number} [now]
   * @returns {string[]}
   */
  stabilize(key, rawBadges, now = Date.now()) {
    const raw = new Set((rawBadges || []).filter(Boolean));
    let st = this._state.get(key);
    if (!st) {
      st = { shown: new Set(), pendingOn: new Map(), pendingOff: new Map() };
      this._state.set(key, st);
    }

    // Candidates to show: in raw but not shown
    for (const b of raw) {
      st.pendingOff.delete(b);
      if (st.shown.has(b)) continue;
      if (!st.pendingOn.has(b)) st.pendingOn.set(b, now);
      else if (now - st.pendingOn.get(b) >= this.showHoldMs) {
        st.shown.add(b);
        st.pendingOn.delete(b);
      }
    }

    // Candidates to clear: shown but not in raw
    for (const b of [...st.shown]) {
      if (raw.has(b)) {
        st.pendingOff.delete(b);
        continue;
      }
      if (!st.pendingOff.has(b)) st.pendingOff.set(b, now);
      else if (now - st.pendingOff.get(b) >= this.clearHoldMs) {
        st.shown.delete(b);
        st.pendingOff.delete(b);
      }
    }

    // Drop pendingOn for badges no longer raw
    for (const b of [...st.pendingOn.keys()]) {
      if (!raw.has(b)) st.pendingOn.delete(b);
    }

    return [...st.shown];
  }

  clear() {
    this._state.clear();
  }
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

/**
 * Debounce boolean live flags so legend / markets chips don't blink.
 */
export class LiveFlagGate {
  /**
   * @param {{ showHoldMs?: number, clearHoldMs?: number }} [opts]
   */
  constructor(opts = {}) {
    this.showHoldMs = opts.showHoldMs ?? 1200;
    this.clearHoldMs = opts.clearHoldMs ?? 3500;
    /** @type {Map<string, { live: boolean, since: number }>} */
    this._state = new Map();
  }

  /**
   * @param {string} key
   * @param {boolean} rawLive
   * @param {number} [now]
   */
  stabilize(key, rawLive, now = Date.now()) {
    const want = !!rawLive;
    let st = this._state.get(key);
    if (!st) {
      st = { live: want, since: now };
      this._state.set(key, st);
      return want;
    }
    if (want === st.live) {
      st.since = now;
      return st.live;
    }
    const hold = want ? this.showHoldMs : this.clearHoldMs;
    // Track how long we've disagreed — store flipCandidateSince on first disagree
    if (st._flipAt == null) st._flipAt = now;
    if (now - st._flipAt >= hold) {
      st.live = want;
      st.since = now;
      st._flipAt = null;
    }
    return st.live;
  }

  clear() {
    this._state.clear();
  }
}
