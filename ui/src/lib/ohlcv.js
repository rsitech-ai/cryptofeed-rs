import { nsToSec } from './format.js';
import {
  DEFAULT_HISTORY_SECS,
  clampHistorySecs,
  retentionCutoff,
  trimTimeMap,
} from './history.js';

/**
 * Aggregate trades into OHLCV buckets keyed by UTC second buckets.
 * Dedupes by trade_id (or venue+ts+price+qty fallback).
 * Retains ~historySecs of candles; session window only clips the view.
 */
export class CandleBuilder {
  constructor(intervalSec = 1, historySecs = DEFAULT_HISTORY_SECS) {
    this.intervalSec = intervalSec;
    this.historySecs = clampHistorySecs(historySecs);
    this.seen = new Set();
    /** @type {Map<number, { time:number, open:number, high:number, low:number, close:number, volume:number, trades:number, buyVol:number, sellVol:number }>} */
    this.buckets = new Map();
    this.sessionHigh = null;
    this.sessionLow = null;
    this.sessionVolume = 0;
    this.sessionNotional = 0;
    this.sessionTrades = 0;
    this.sessionBuyVol = 0;
    this.sessionSellVol = 0;
    this.lastPrice = null;
    this.prevPrice = null;
    this._lastAppliedNs = null;
    this._needsChronologicalRebuild = false;
  }

  /** @param {unknown} secs */
  setHistorySecs(secs) {
    const next = clampHistorySecs(secs, this.historySecs);
    if (next === this.historySecs) return;
    this.historySecs = next;
    this.trimHistory();
  }

  setInterval(intervalSec) {
    if (intervalSec === this.intervalSec) return;
    this.intervalSec = intervalSec;
    this.rebuildFromTrades();
  }

  reset() {
    this.seen.clear();
    this.buckets.clear();
    this._trades = [];
    this.sessionHigh = null;
    this.sessionLow = null;
    this.sessionVolume = 0;
    this.sessionNotional = 0;
    this.sessionTrades = 0;
    this.sessionBuyVol = 0;
    this.sessionSellVol = 0;
    this.lastPrice = null;
    this.prevPrice = null;
    this._lastAppliedNs = null;
    this._needsChronologicalRebuild = false;
  }

  tradeKey(t) {
    if (t.trade_id != null) return `${t.venue}|${t.trade_id}`;
    return `${t.venue}|${t.receive_ts_ns}|${t.price}|${t.quantity}`;
  }

  ingest(entries) {
    if (!this._trades) this._trades = [];
    let added = 0;
    // `/v1/tape` is newest-first. Apply unseen events chronologically so
    // bucket open/close and the headline last price describe event time.
    const chronological = [...(entries || [])].sort(compareTapeTime);
    for (const e of chronological) {
      if (e.kind !== 'trade') continue;
      const key = this.tradeKey(e);
      if (this.seen.has(key)) continue;
      this.seen.add(key);
      const price = Number(e.price);
      const qty = Number(e.quantity);
      const sec = nsToSec(e.exchange_ts_ns ?? e.receive_ts_ns);
      if (!Number.isFinite(price) || !Number.isFinite(qty) || sec == null) continue;
      const trade = {
        sec,
        orderNs: Number(e.exchange_ts_ns ?? e.receive_ts_ns),
        price,
        qty,
        buy: e.aggressor === 'buy',
      };
      this._trades.push(trade);
      if (
        Number.isFinite(trade.orderNs) &&
        this._lastAppliedNs != null &&
        trade.orderNs < this._lastAppliedNs
      ) {
        this._needsChronologicalRebuild = true;
      }
      this._applyTrade(trade);
      added += 1;
    }
    if (this._needsChronologicalRebuild) {
      this._trades.sort((a, b) => (a.orderNs || 0) - (b.orderNs || 0));
      this._needsChronologicalRebuild = false;
      this.rebuildFromTrades();
    }
    this.trimHistory();
    return added;
  }

  /**
   * Drop trades/buckets older than historySecs. Prefer keeping raw prints for
   * the full retention window so timeframe changes can rebuild; if over the
   * trade-count budget, drop oldest raw prints first (OHLCV buckets still hold
   * history until a TF rebuild).
   */
  trimHistory() {
    if (!this._trades) this._trades = [];
    let latest = 0;
    for (const c of this.buckets.values()) {
      if (c.time > latest) latest = c.time;
    }
    for (const t of this._trades) {
      if (t.sec > latest) latest = t.sec;
    }
    if (!latest) latest = Math.floor(Date.now() / 1000);
    const cutoff = retentionCutoff(latest, this.historySecs);
    if (cutoff > 0) {
      trimTimeMap(this.buckets, cutoff);
      this._trades = this._trades.filter((t) => t.sec >= cutoff);
    }
    // ~15 trades/sec budget across historySecs; hard cap for bursty focus venues.
    const maxRaw = Math.min(80000, Math.max(12000, Math.floor(this.historySecs * 15)));
    if (this._trades.length > maxRaw) {
      this._trades = this._trades.slice(-maxRaw);
    }
    if (this.seen.size > 40000) {
      this.seen = new Set([...this.seen].slice(-20000));
    }
  }

  /** Seed last price from book mid when no trades yet. */
  touchPrice(price) {
    const n = Number(price);
    if (!Number.isFinite(n)) return;
    if (this.lastPrice == null) {
      this.lastPrice = n;
      this.prevPrice = n;
    }
    if (this.sessionHigh == null || n > this.sessionHigh) this.sessionHigh = n;
    if (this.sessionLow == null || n < this.sessionLow) this.sessionLow = n;
  }

  _applyTrade(trade) {
    const { sec, price, qty, buy } = trade;
    const notional = price * qty;
    const bucket = Math.floor(sec / this.intervalSec) * this.intervalSec;
    let c = this.buckets.get(bucket);
    if (!c) {
      c = {
        time: bucket,
        open: price,
        high: price,
        low: price,
        close: price,
        volume: 0,
        notional: 0,
        trades: 0,
        buyVol: 0,
        sellVol: 0,
        buyNotional: 0,
        sellNotional: 0,
      };
      this.buckets.set(bucket, c);
    }
    c.high = Math.max(c.high, price);
    c.low = Math.min(c.low, price);
    c.close = price;
    c.volume += qty;
    c.notional += notional;
    c.trades += 1;
    if (buy) {
      c.buyVol += qty;
      c.buyNotional += notional;
    } else {
      c.sellVol += qty;
      c.sellNotional += notional;
    }

    this.prevPrice = this.lastPrice ?? price;
    this.lastPrice = price;
    this.sessionVolume += qty;
    this.sessionNotional = (this.sessionNotional || 0) + notional;
    this.sessionTrades += 1;
    if (buy) this.sessionBuyVol += qty;
    else this.sessionSellVol += qty;
    if (this.sessionHigh == null || price > this.sessionHigh) this.sessionHigh = price;
    if (this.sessionLow == null || price < this.sessionLow) this.sessionLow = price;
    if (Number.isFinite(trade.orderNs)) this._lastAppliedNs = trade.orderNs;
  }

  rebuildFromTrades() {
    const trades = [...(this._trades || [])].sort(
      (a, b) => (a.orderNs || 0) - (b.orderNs || 0),
    );
    this._trades = trades;
    this.buckets.clear();
    this.sessionHigh = null;
    this.sessionLow = null;
    this.sessionVolume = 0;
    this.sessionNotional = 0;
    this.sessionTrades = 0;
    this.sessionBuyVol = 0;
    this.sessionSellVol = 0;
    const lp = this.lastPrice;
    const pp = this.prevPrice;
    this.lastPrice = null;
    this.prevPrice = null;
    this._lastAppliedNs = null;
    for (const t of trades) this._applyTrade(t);
    if (this.lastPrice == null && lp != null) {
      this.lastPrice = lp;
      this.prevPrice = pp;
    }
  }

  candles(windowSec = 0) {
    const all = [...this.buckets.values()].sort((a, b) => a.time - b.time);
    if (!windowSec || windowSec <= 0 || !all.length) return all;
    const tip = all[all.length - 1].time;
    const since = tip - Math.max(1, windowSec);
    return all.filter((c) => c.time >= since);
  }

  volumeBars(windowSec = 0) {
    const up = '#02c076';
    const down = '#f6465d';
    return this.candles(windowSec).map((c) => ({
      time: c.time,
      value: c.notional ?? c.volume,
      color: c.close >= c.open ? up : down,
    }));
  }

  /**
   * USD notional / trade count for buckets with time >= nowSec - windowSec.
   * @param {number} windowSec
   * @param {number} [nowSec]
   */
  windowStats(windowSec, nowSec = Math.floor(Date.now() / 1000)) {
    const since = nowSec - Math.max(1, windowSec);
    let volume = 0;
    let notional = 0;
    let trades = 0;
    for (const c of this.buckets.values()) {
      if (c.time >= since) {
        volume += c.volume;
        notional += c.notional ?? c.volume * c.close;
        trades += c.trades;
      }
    }
    return { volume, notional, trades, windowSec, tradesPerMin: (trades / windowSec) * 60 };
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

export const TIMEFRAMES = [
  { id: '1s', label: '1s', sec: 1 },
  { id: '5s', label: '5s', sec: 5 },
  { id: '15s', label: '15s', sec: 15 },
  { id: '1m', label: '1m', sec: 60 },
  { id: '5m', label: '5m', sec: 300 },
];

/** Map chart timeframe → stats window length (seconds). */
export function statsWindowForTf(tfId) {
  switch (tfId) {
    case '1s':
      return 60;
    case '5s':
      return 5 * 60;
    case '15s':
      return 15 * 60;
    case '1m':
      return 60 * 60;
    case '5m':
      return 5 * 60 * 60;
    default:
      return 60;
  }
}
