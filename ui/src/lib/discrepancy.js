/** Track cross-venue max−min bps history for sparkline + alerts. */

import { DEFAULT_HISTORY_SECS, bpsMaxPoints, clampHistorySecs, retentionCutoff } from './history.js';

/**
 * @typedef {{ time: number, bps: number, max: number, min: number, highVenue: string|null, lowVenue: string|null }} BpsPoint
 */

export class DiscrepancyTracker {
  /** @param {number} [historySecs] */
  constructor(historySecs = DEFAULT_HISTORY_SECS) {
    /** @type {BpsPoint[]} */
    this.history = [];
    this.lastAlertAt = 0;
    this.historySecs = clampHistorySecs(historySecs);
    this.maxPoints = bpsMaxPoints(this.historySecs);
  }

  /** @param {unknown} secs */
  setHistorySecs(secs) {
    this.historySecs = clampHistorySecs(secs, this.historySecs);
    this.maxPoints = bpsMaxPoints(this.historySecs);
    this.trim();
  }

  clear() {
    this.history = [];
  }

  trim() {
    if (!this.history.length) return;
    const tip = this.history[this.history.length - 1].time;
    const cutoff = retentionCutoff(tip, this.historySecs);
    if (cutoff > 0) {
      this.history = this.history.filter((p) => p.time >= cutoff);
    }
    if (this.history.length > this.maxPoints) {
      this.history = this.history.slice(-this.maxPoints);
    }
  }

  /**
   * Latest series sample time (exchange/bucket sec). Prefer this over wall clock
   * so the bps pane does not extend the shared time scale into empty future.
   * @param {Array<{ data?: Array<{ time: number }>, lastTime?: number|null }>} series
   */
  static dataTimeSec(series) {
    let maxT = 0;
    for (const s of series || []) {
      if (s?.lastTime != null && Number.isFinite(s.lastTime) && s.lastTime > maxT) {
        maxT = s.lastTime;
      }
      const data = s?.data;
      if (data?.length) {
        const t = data[data.length - 1]?.time;
        if (Number.isFinite(t) && t > maxT) maxT = t;
      }
    }
    return maxT > 0 ? maxT : null;
  }

  /**
   * Record a snapshot from MultiVenueTracker discrepancy + series.
   * @param {object|null} discrepancy
   * @param {Array<{ venue: string, last: number|null, hidden?: boolean, data?: Array<{time:number}>, lastTime?: number|null }>} series
   * @param {number} [nowSec] data-domain seconds; defaults to series last time (not wall clock)
   */
  push(discrepancy, series, nowSec) {
    if (discrepancy?.bps == null) return;
    const bps = discrepancy?.bps;
    if (bps == null || !Number.isFinite(bps)) return;

    if (nowSec == null || !Number.isFinite(nowSec)) {
      nowSec = DiscrepancyTracker.dataTimeSec(series) ?? Math.floor(Date.now() / 1000);
    }

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

    if (this.history.length > this.maxPoints) {
      this.history = this.history.slice(-this.maxPoints);
    } else {
      this.trim();
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
