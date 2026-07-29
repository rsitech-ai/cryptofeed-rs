/** Track cross-venue max−min bps history for sparkline + alerts. */

const MAX_POINTS = 600;

/**
 * @typedef {{ time: number, bps: number, max: number, min: number, highVenue: string|null, lowVenue: string|null }} BpsPoint
 */

export class DiscrepancyTracker {
  constructor() {
    /** @type {BpsPoint[]} */
    this.history = [];
    this.lastAlertAt = 0;
  }

  clear() {
    this.history = [];
  }

  /**
   * Record a snapshot from MultiVenueTracker discrepancy + series.
   * @param {object|null} discrepancy
   * @param {Array<{ venue: string, last: number|null, hidden?: boolean }>} series
   * @param {number} [nowSec]
   */
  push(discrepancy, series, nowSec = Math.floor(Date.now() / 1000)) {
    if (discrepancy?.bps == null) return;
    const bps = discrepancy?.bps;
    if (bps == null || !Number.isFinite(bps)) return;

    const visible = (series || []).filter((s) => !s.hidden && s.last != null);
    let highVenue = null;
    let lowVenue = null;
    if (visible.length >= 2) {
      let max = -Infinity;
      let min = Infinity;
      for (const s of visible) {
        const p = s.last;
        if (p > max) {
          max = p;
          highVenue = s.venue;
        }
        if (p < min) {
          min = p;
          lowVenue = s.venue;
        }
      }
    }

    const last = this.history[this.history.length - 1];
    if (last && last.time === nowSec) {
      last.bps = bps;
      last.max = discrepancy.max;
      last.min = discrepancy.min;
      last.highVenue = highVenue;
      last.lowVenue = lowVenue;
    } else {
      this.history.push({
        time: nowSec,
        bps,
        max: discrepancy.max,
        min: discrepancy.min,
        highVenue,
        lowVenue,
      });
    }

    if (this.history.length > MAX_POINTS) {
      this.history = this.history.slice(-MAX_POINTS);
    }
  }

  /** @returns {BpsPoint[]} */
  points() {
    return this.history;
  }

  /**
   * @param {number} thresholdBps
   * @param {number} [cooldownSec]
   */
  shouldAlert(thresholdBps, cooldownSec = 30) {
    const last = this.history[this.history.length - 1];
    if (!last || last.bps <= thresholdBps) return null;
    const now = Math.floor(Date.now() / 1000);
    if (now - this.lastAlertAt < cooldownSec) return null;
    this.lastAlertAt = now;
    return last;
  }
}
