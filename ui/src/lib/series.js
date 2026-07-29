import { nsToSec } from './format.js';
import { colorForVenue } from './assets.js';

/**
 * Multi-venue last-price series from tape trades + quote mids.
 * Buckets by interval; line value = last price in bucket (stable, no OHLC spikes).
 * Also accumulates per-venue trade volume / count for the compare legend.
 */
export class MultiVenueTracker {
  constructor(intervalSec = 1) {
    this.intervalSec = intervalSec;
    /** @type {Map<string, VenueState>} */
    this.venues = new Map();
  }

  /**
   * @param {Array<{ venue: string, symbol: string, live?: boolean }>} targets
   */
  syncTargets(targets) {
    const keep = new Set(targets.map((t) => t.venue));
    for (const id of [...this.venues.keys()]) {
      if (!keep.has(id)) this.venues.delete(id);
    }
    targets.forEach((t, i) => {
      let st = this.venues.get(t.venue);
      if (!st) {
        st = newVenueState(t.venue, t.symbol, colorForVenue(t.venue, i));
        this.venues.set(t.venue, st);
      } else if (st.symbol !== t.symbol) {
        // Asset remapped to a different symbol on same venue — reset series.
        st = newVenueState(t.venue, t.symbol, st.color || colorForVenue(t.venue, i));
        this.venues.set(t.venue, st);
      }
      st.live = !!t.live;
      st.symbol = t.symbol;
    });
  }

  setInterval(intervalSec) {
    if (intervalSec === this.intervalSec) return;
    this.intervalSec = intervalSec;
    for (const st of this.venues.values()) rebuildVenue(st, intervalSec);
  }

  reset() {
    for (const st of this.venues.values()) {
      Object.assign(st, newVenueState(st.venue, st.symbol, st.color));
    }
  }

  clear() {
    this.venues.clear();
  }

  /**
   * @param {string} venue
   * @param {Array<object>} entries
   */
  ingest(venue, entries) {
    const st = this.venues.get(venue);
    if (!st) return 0;
    let added = 0;
    for (const e of entries || []) {
      const sec = nsToSec(e.exchange_ts_ns ?? e.receive_ts_ns);
      if (sec == null) continue;

      if (e.kind === 'trade') {
        const key = tradeKey(e);
        if (st.seen.has(key)) continue;
        st.seen.add(key);
        const price = Number(e.price);
        const qty = Number(e.quantity);
        if (!Number.isFinite(price) || price <= 0) continue;
        applyPrice(st, sec, price, this.intervalSec);
        if (Number.isFinite(qty) && qty > 0) {
          st.tradeVolume += qty;
          st.tradeCount += 1;
          const bucket = Math.floor(sec / this.intervalSec) * this.intervalSec;
          st.volBuckets.set(bucket, (st.volBuckets.get(bucket) || 0) + qty);
        }
        added += 1;
      } else if (e.kind === 'quote') {
        const bid = Number(e.bid_price);
        const ask = Number(e.ask_price);
        if (!Number.isFinite(bid) || !Number.isFinite(ask) || bid <= 0 || ask <= 0) continue;
        const mid = (bid + ask) / 2;
        const key = `q|${e.receive_ts_ns}|${mid}`;
        if (st.seen.has(key)) continue;
        st.seen.add(key);
        applyPrice(st, sec, mid, this.intervalSec);
        added += 1;
      }
    }
    trimSeen(st);
    return added;
  }

  /** Seed from book BBO mid (no tape yet). */
  touch(venue, price, sec = Math.floor(Date.now() / 1000)) {
    const st = this.venues.get(venue);
    if (!st) return;
    const n = Number(price);
    if (!Number.isFinite(n) || n <= 0) return;
    applyPrice(st, sec, n, this.intervalSec);
  }

  /**
   * @param {'absolute'|'percent'} mode
   * @param {{ hidden?: Set<string> }} [opts]
   * @returns {{ series: Array<object>, discrepancy: object|null, pointCount: number, aggregate: { volume:number, trades:number } }}
   */
  snapshot(mode = 'percent', opts = {}) {
    const hidden = opts.hidden || new Set();
    const series = [];
    let pointCount = 0;
    const lasts = [];
    let aggVol = 0;
    let aggTrades = 0;

    for (const st of this.venues.values()) {
      const points = [...st.buckets.entries()]
        .sort((a, b) => a[0] - b[0])
        .map(([time, price]) => ({ time, price }));

      aggVol += st.tradeVolume;
      aggTrades += st.tradeCount;

      if (!points.length) {
        series.push({
          venue: st.venue,
          symbol: st.symbol,
          color: st.color,
          live: st.live,
          hidden: hidden.has(st.venue),
          data: [],
          last: st.lastPrice,
          pct: null,
          baseline: st.baseline,
          tradeVolume: st.tradeVolume,
          tradeCount: st.tradeCount,
          volumeData: volumeSeries(st),
        });
        continue;
      }

      const baseline = st.baseline ?? points[0].price;
      const data =
        mode === 'percent'
          ? points.map((p) => ({
              time: p.time,
              value: ((p.price / baseline) - 1) * 100,
            }))
          : points.map((p) => ({
              time: p.time,
              value: p.price,
            }));

      const last = points[points.length - 1].price;
      const pct = baseline ? ((last / baseline) - 1) * 100 : null;
      pointCount += data.length;
      if (!hidden.has(st.venue)) lasts.push(last);

      series.push({
        venue: st.venue,
        symbol: st.symbol,
        color: st.color,
        live: st.live,
        hidden: hidden.has(st.venue),
        data: hidden.has(st.venue) ? [] : data,
        last,
        pct,
        baseline,
        tradeVolume: st.tradeVolume,
        tradeCount: st.tradeCount,
        volumeData: hidden.has(st.venue) ? [] : volumeSeries(st),
      });
    }

    series.sort((a, b) => a.venue.localeCompare(b.venue));

    let discrepancy = null;
    if (lasts.length >= 2) {
      const max = Math.max(...lasts);
      const min = Math.min(...lasts);
      const mid = (max + min) / 2;
      const visiblePct = series.filter((s) => !s.hidden && s.pct != null).map((s) => s.pct);
      discrepancy = {
        max,
        min,
        abs: max - min,
        bps: mid > 0 ? ((max - min) / mid) * 10000 : null,
        pctSpan:
          visiblePct.length >= 2
            ? Math.max(...visiblePct) - Math.min(...visiblePct)
            : null,
      };
    }

    return {
      series,
      discrepancy,
      pointCount,
      aggregate: { volume: aggVol, trades: aggTrades },
    };
  }
}

/**
 * @typedef {{
 *   venue: string,
 *   symbol: string,
 *   color: string,
 *   live: boolean,
 *   seen: Set<string>,
 *   buckets: Map<number, number>,
 *   volBuckets: Map<number, number>,
 *   baseline: number|null,
 *   lastPrice: number|null,
 *   tradeVolume: number,
 *   tradeCount: number,
 *   samples: Array<{sec:number, price:number}>,
 * }} VenueState
 */

function newVenueState(venue, symbol, color) {
  return {
    venue,
    symbol,
    color,
    live: true,
    seen: new Set(),
    buckets: new Map(),
    volBuckets: new Map(),
    baseline: null,
    lastPrice: null,
    tradeVolume: 0,
    tradeCount: 0,
    samples: [],
  };
}

function tradeKey(t) {
  if (t.trade_id != null) return `t|${t.venue}|${t.trade_id}`;
  return `t|${t.venue}|${t.receive_ts_ns}|${t.price}|${t.quantity}`;
}

function applyPrice(st, sec, price, intervalSec, recordSample = true) {
  const bucket = Math.floor(sec / intervalSec) * intervalSec;
  st.buckets.set(bucket, price);
  if (st.baseline == null) st.baseline = price;
  st.lastPrice = price;
  if (recordSample) {
    st.samples.push({ sec, price });
    if (st.samples.length > 20000) {
      st.samples = st.samples.slice(-12000);
    }
  }
}

function volumeSeries(st) {
  return [...st.volBuckets.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([time, value]) => ({ time, value, color: 'rgba(240,185,11,0.55)' }));
}

function rebuildVenue(st, intervalSec) {
  const samples = st.samples;
  st.buckets.clear();
  st.baseline = null;
  st.lastPrice = null;
  for (const s of samples) applyPrice(st, s.sec, s.price, intervalSec, false);
}

function trimSeen(st) {
  if (st.seen.size > 30000) {
    st.seen = new Set([...st.seen].slice(-15000));
  }
  if (st.buckets.size > 5000) {
    const keys = [...st.buckets.keys()].sort((a, b) => a - b);
    const drop = keys.slice(0, keys.length - 4000);
    for (const k of drop) st.buckets.delete(k);
  }
  if (st.volBuckets.size > 5000) {
    const keys = [...st.volBuckets.keys()].sort((a, b) => a - b);
    const drop = keys.slice(0, keys.length - 4000);
    for (const k of drop) st.volBuckets.delete(k);
  }
}

export const TIMEFRAMES = [
  { id: '1s', label: '1s', sec: 1 },
  { id: '5s', label: '5s', sec: 5 },
  { id: '15s', label: '15s', sec: 15 },
  { id: '1m', label: '1m', sec: 60 },
  { id: '5m', label: '5m', sec: 300 },
];
