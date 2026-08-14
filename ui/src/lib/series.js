import { nsToSec } from './format.js';
import { colorForVenue } from './assets.js';
import {
  DEFAULT_HISTORY_SECS,
  CHART_DISPLAY_MAX_POINTS,
  clampHistorySecs,
  downsampleByAge,
  downsampleForChart,
  retentionCutoff,
  trimTimeMap,
  venueBucketBudget,
  venueSampleBudget,
} from './history.js';

/**
 * Multi-venue last-price series from tape trades + quote mids.
 * Buckets by interval; line value = last price in bucket (stable, no OHLC spikes).
 * Tracks USD notional volume (price * qty) and trade intensity per venue.
 * Retains ~historySecs of buckets (default 2h); session window clips stats.
 * Pass `chartWindowSec` (typically historySecs) so the chart can pan/zoom
 * retained history instead of only the live session sliver.
 */
export class MultiVenueTracker {
  constructor(intervalSec = 1, historySecs = DEFAULT_HISTORY_SECS) {
    this.intervalSec = intervalSec;
    this.historySecs = clampHistorySecs(historySecs);
    /** @type {Map<string, VenueState>} */
    this.venues = new Map();
    this.sessionStartSec = Math.floor(Date.now() / 1000);
  }

  /** @param {unknown} secs */
  setHistorySecs(secs) {
    const next = clampHistorySecs(secs, this.historySecs);
    if (next === this.historySecs) return;
    this.historySecs = next;
    for (const st of this.venues.values()) trimVenue(st, this.intervalSec, this.historySecs);
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
    this.sessionStartSec = Math.floor(Date.now() / 1000);
  }

  clear() {
    this.venues.clear();
    this.sessionStartSec = Math.floor(Date.now() / 1000);
  }

  /**
   * @param {string} venue
   * @param {Array<object>} entries
   */
  ingest(venue, entries) {
    const st = this.venues.get(venue);
    if (!st) return 0;
    let added = 0;
    // Daemon tape snapshots are newest-first; derive baseline/last in event
    // order rather than HTTP response order.
    const chronological = [...(entries || [])].sort(compareTapeTime);
    let needsChronologicalRebuild = false;
    for (const e of chronological) {
      const sec = nsToSec(e.exchange_ts_ns ?? e.receive_ts_ns);
      const orderNs = Number(e.exchange_ts_ns ?? e.receive_ts_ns);
      if (sec == null) continue;

      if (e.kind === 'trade') {
        const key = tradeKey(e);
        if (st.seen.has(key)) continue;
        st.seen.add(key);
        const price = Number(e.price);
        const qty = Number(e.quantity);
        if (!Number.isFinite(price) || price <= 0) continue;
        if (Number.isFinite(orderNs) && st.lastSampleNs != null && orderNs < st.lastSampleNs) {
          needsChronologicalRebuild = true;
        }
        applyPrice(st, sec, price, this.intervalSec, true, orderNs);
        if (Number.isFinite(qty) && qty > 0) {
          const notional = price * qty;
          st.tradeVolume += qty;
          st.tradeNotional += notional;
          st.tradeCount += 1;
          st.firstTradeSec =
            st.firstTradeSec == null ? sec : Math.min(st.firstTradeSec, sec);
          st.lastTradeSec = st.lastTradeSec == null ? sec : Math.max(st.lastTradeSec, sec);
          const bucket = Math.floor(sec / this.intervalSec) * this.intervalSec;
          st.volBuckets.set(bucket, (st.volBuckets.get(bucket) || 0) + notional);
          st.tradeBuckets.set(bucket, (st.tradeBuckets.get(bucket) || 0) + 1);
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
        if (Number.isFinite(orderNs) && st.lastSampleNs != null && orderNs < st.lastSampleNs) {
          needsChronologicalRebuild = true;
        }
        applyPrice(st, sec, mid, this.intervalSec, true, orderNs);
        added += 1;
      }
    }
    if (needsChronologicalRebuild) rebuildVenue(st, this.intervalSec);
    trimVenue(st, this.intervalSec, this.historySecs);
    return added;
  }

  touch(venue, price, sec = Math.floor(Date.now() / 1000)) {
    const st = this.venues.get(venue);
    if (!st) return;
    const n = Number(price);
    if (!Number.isFinite(n) || n <= 0) return;
    applyPrice(st, sec, n, this.intervalSec, true, sec * 1e9);
    trimVenue(st, this.intervalSec, this.historySecs);
  }

  /**
   * Window stats for a venue (USD notional + trades).
   * @param {VenueState} st
   * @param {number} windowSec
   * @param {number} [nowSec]
   */
  venueWindowStats(st, windowSec, nowSec = Math.floor(Date.now() / 1000)) {
    const since = nowSec - Math.max(1, windowSec);
    let notional = 0;
    let trades = 0;
    for (const [bucket, val] of st.volBuckets) {
      if (bucket >= since) {
        notional += val;
        trades += st.tradeBuckets?.get(bucket) || 0;
      }
    }
    // Fallback: if no per-bucket trade counts, estimate from session ratio
    if (trades === 0 && st.tradeCount > 0 && st.lastTradeSec != null && st.lastTradeSec >= since) {
      const span = Math.max(1, (st.lastTradeSec - (st.firstTradeSec ?? since)) + 1);
      const frac = Math.min(1, windowSec / span);
      trades = Math.round(st.tradeCount * frac);
      if (notional === 0) notional = st.tradeNotional * frac;
    }
    return { notional, trades, tradesPerMin: (trades / windowSec) * 60 };
  }

  /**
   * @param {'absolute'|'percent'} mode
   * @param {{ hidden?: Set<string>, windowSec?: number, chartWindowSec?: number }} [opts]
   */
  snapshot(mode = 'percent', opts = {}) {
    const hidden = opts.hidden || new Set();
    const windowSec = opts.windowSec ?? 300;
    const chartWindowSec =
      opts.chartWindowSec != null && Number.isFinite(Number(opts.chartWindowSec))
        ? Math.max(0, Number(opts.chartWindowSec))
        : windowSec;
    const series = [];
    let pointCount = 0;
    const lasts = [];
    let aggNotional = 0;
    let aggTrades = 0;
    let aggQty = 0;

    // Clip to session window ending at latest *data* time (not wall clock) so
    // the chart stays pinned to the live edge without empty future whitespace.
    let latestSec = 0;
    for (const st of this.venues.values()) {
      for (const t of st.buckets.keys()) {
        if (t > latestSec) latestSec = t;
      }
    }
    const sinceSec =
      chartWindowSec > 0 && latestSec > 0 ? latestSec - Math.max(1, chartWindowSec) : 0;

    for (const st of this.venues.values()) {
      const win = this.venueWindowStats(st, windowSec, latestSec || undefined);
      const points = [...st.buckets.entries()]
        .filter(([time]) => time >= sinceSec)
        .sort((a, b) => a[0] - b[0])
        .map(([time, price]) => ({ time, price }));

      aggNotional += st.tradeNotional;
      aggTrades += st.tradeCount;
      aggQty += st.tradeVolume;

      const tpm = st.tradeCount > 0 && st.firstTradeSec != null && st.lastTradeSec != null
        ? (st.tradeCount / Math.max(1, st.lastTradeSec - st.firstTradeSec + 1)) * 60
        : win.tradesPerMin;

      if (!points.length) {
        series.push({
          venue: st.venue,
          symbol: st.symbol,
          color: st.color,
          live: st.live,
          hidden: hidden.has(st.venue),
          data: [],
          last: st.lastPrice,
          lastTime: null,
          pct: null,
          baseline: st.baseline,
          tradeVolume: st.tradeVolume,
          tradeNotional: st.tradeNotional,
          tradeCount: st.tradeCount,
          tradesPerMin: tpm,
          windowNotional: win.notional,
          windowTrades: win.trades,
          volumeData: volumeSeries(st, sinceSec),
        });
        continue;
      }

      const baseline = st.baseline ?? points[0].price;
      const rawData =
        mode === 'percent'
          ? points.map((p) => ({
              time: p.time,
              value: (p.price / baseline - 1) * 100,
            }))
          : points.map((p) => ({
              time: p.time,
              value: p.price,
            }));
      // Display downsample — keep chart paint ≤ CHART_DISPLAY_MAX_POINTS even on 1h view.
      const data = downsampleForChart(rawData, chartWindowSec || windowSec, CHART_DISPLAY_MAX_POINTS);

      const last = points[points.length - 1].price;
      const lastTime = points[points.length - 1].time;
      const pct = baseline ? (last / baseline - 1) * 100 : null;
      pointCount += data.length;
      if (!hidden.has(st.venue)) lasts.push(last);

      const volRaw = hidden.has(st.venue) ? [] : volumeSeries(st, sinceSec);
      const volumeData = downsampleForChart(volRaw, chartWindowSec || windowSec, CHART_DISPLAY_MAX_POINTS);

      series.push({
        venue: st.venue,
        symbol: st.symbol,
        color: st.color,
        live: st.live,
        hidden: hidden.has(st.venue),
        data: hidden.has(st.venue) ? [] : data,
        last,
        lastTime,
        pct,
        baseline,
        tradeVolume: st.tradeVolume,
        tradeNotional: st.tradeNotional,
        tradeCount: st.tradeCount,
        tradesPerMin: tpm,
        windowNotional: win.notional,
        windowTrades: win.trades,
        volumeData,
      });
    }

    series.sort((a, b) => a.venue.localeCompare(b.venue));

    let discrepancy = null;
    if (lasts.length >= 2) {
      const max = Math.max(...lasts);
      const min = Math.min(...lasts);
      const mid = (max + min) / 2;
      const visibleSeries = series.filter((s) => !s.hidden && s.last != null);
      let highVenue = null;
      let lowVenue = null;
      for (const s of visibleSeries) {
        if (s.last === max) highVenue = s.venue;
        if (s.last === min) lowVenue = s.venue;
      }
      const visiblePct = visibleSeries.filter((s) => s.pct != null).map((s) => s.pct);
      discrepancy = {
        max,
        min,
        abs: max - min,
        bps: mid > 0 ? ((max - min) / mid) * 10000 : null,
        highVenue,
        lowVenue,
        pctSpan:
          visiblePct.length >= 2 ? Math.max(...visiblePct) - Math.min(...visiblePct) : null,
      };
    }

    return {
      series,
      discrepancy,
      pointCount,
      aggregate: {
        volume: aggQty,
        notional: aggNotional,
        trades: aggTrades,
      },
    };
  }
}

function compareTapeTime(a, b) {
  const at = Number(a?.exchange_ts_ns ?? a?.receive_ts_ns);
  const bt = Number(b?.exchange_ts_ns ?? b?.receive_ts_ns);
  if (!Number.isFinite(at) && !Number.isFinite(bt)) return 0;
  if (!Number.isFinite(at)) return -1;
  if (!Number.isFinite(bt)) return 1;
  return at - bt;
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
 *   tradeBuckets: Map<number, number>,
 *   baseline: number|null,
 *   lastPrice: number|null,
 *   tradeVolume: number,
 *   tradeNotional: number,
 *   tradeCount: number,
 *   firstTradeSec: number|null,
 *   lastTradeSec: number|null,
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
    tradeBuckets: new Map(),
    baseline: null,
    lastPrice: null,
    tradeVolume: 0,
    tradeNotional: 0,
    tradeCount: 0,
    firstTradeSec: null,
    lastTradeSec: null,
    samples: [],
    lastSampleNs: null,
  };
}

function tradeKey(t) {
  if (t.trade_id != null) return `t|${t.venue}|${t.trade_id}`;
  return `t|${t.venue}|${t.receive_ts_ns}|${t.price}|${t.quantity}`;
}

function applyPrice(st, sec, price, intervalSec, recordSample = true, orderNs = sec * 1e9) {
  const bucket = Math.floor(sec / intervalSec) * intervalSec;
  st.buckets.set(bucket, price);
  if (st.baseline == null) st.baseline = price;
  st.lastPrice = price;
  if (recordSample) {
    st.samples.push({ sec, price, orderNs });
    if (Number.isFinite(orderNs)) st.lastSampleNs = orderNs;
  }
}

function volumeSeries(st, sinceSec = 0) {
  return [...st.volBuckets.entries()]
    .filter(([time]) => time >= sinceSec)
    .sort((a, b) => a[0] - b[0])
    .map(([time, value]) => ({ time, value, color: 'rgba(240,185,11,0.55)' }));
}

function rebuildVenue(st, intervalSec) {
  const samples = [...st.samples].sort((a, b) => (a.orderNs || 0) - (b.orderNs || 0));
  st.samples = samples;
  st.buckets.clear();
  st.baseline = null;
  st.lastPrice = null;
  st.lastSampleNs = null;
  for (const s of samples) {
    applyPrice(st, s.sec, s.price, intervalSec, false, s.orderNs);
    if (Number.isFinite(s.orderNs)) st.lastSampleNs = s.orderNs;
  }
}

/**
 * Time-based retention (~historySecs) + sample downsample for memory.
 * @param {VenueState} st
 * @param {number} intervalSec
 * @param {number} historySecs
 */
function trimVenue(st, intervalSec, historySecs) {
  let latest = 0;
  for (const t of st.buckets.keys()) {
    if (t > latest) latest = t;
  }
  for (const s of st.samples) {
    if (s.sec > latest) latest = s.sec;
  }
  const cutoff = retentionCutoff(latest || Math.floor(Date.now() / 1000), historySecs);

  if (cutoff > 0) {
    trimTimeMap(st.buckets, cutoff);
    trimTimeMap(st.volBuckets, cutoff);
    if (st.tradeBuckets) trimTimeMap(st.tradeBuckets, cutoff);
    if (st.samples.length) {
      st.samples = st.samples.filter((s) => s.sec >= cutoff);
    }
  }

  const sampleCap = venueSampleBudget(historySecs);
  // Proactive downsample once past 85% of budget (avoid waiting for cliff).
  if (st.samples.length > Math.floor(sampleCap * 0.85)) {
    const tip = latest || st.samples[st.samples.length - 1]?.sec || 0;
    const asPoints = st.samples.map((s) => ({ time: s.sec, price: s.price, orderNs: s.orderNs }));
    const sparse = downsampleByAge(asPoints, {
      tipSec: tip,
      recentSec: Math.min(300, historySecs),
      midSec: Math.min(1200, historySecs),
      recentStep: Math.max(1, intervalSec),
      midStep: Math.max(5, intervalSec * 5),
      oldStep: Math.max(15, intervalSec * 15),
      maxPoints: sampleCap,
    });
    st.samples = sparse.map((p) => ({ sec: p.time, price: p.price, orderNs: p.orderNs }));
    // Keep dense line buckets. Rebuilding from downsampled samples collapsed a
    // 2h 1s series to ~1k bars and made the chart look like a right-edge sliver.
  }

  // Dedup keys grow with every quote tick — trim aggressively.
  if (st.seen.size > 12000) {
    st.seen = new Set([...st.seen].slice(-6000));
  }

  // Hard safety if time tip is missing / stalled.
  const maxBuckets = venueBucketBudget(historySecs, intervalSec);
  if (st.buckets.size > maxBuckets) {
    const keys = [...st.buckets.keys()].sort((a, b) => a - b);
    const drop = keys.slice(0, keys.length - maxBuckets);
    for (const k of drop) st.buckets.delete(k);
  }
  if (st.volBuckets.size > maxBuckets) {
    const keys = [...st.volBuckets.keys()].sort((a, b) => a - b);
    const drop = keys.slice(0, keys.length - maxBuckets);
    for (const k of drop) st.volBuckets.delete(k);
  }
  if (st.tradeBuckets && st.tradeBuckets.size > maxBuckets) {
    const keys = [...st.tradeBuckets.keys()].sort((a, b) => a - b);
    const drop = keys.slice(0, keys.length - maxBuckets);
    for (const k of drop) st.tradeBuckets.delete(k);
  }
}

export const TIMEFRAMES = [
  { id: '1s', label: '1s', sec: 1 },
  { id: '5s', label: '5s', sec: 5 },
  { id: '15s', label: '15s', sec: 15 },
  { id: '1m', label: '1m', sec: 60 },
  { id: '5m', label: '5m', sec: 300 },
];
