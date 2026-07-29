import { nsToSec } from './format.js';

/**
 * Aggregate trades into OHLCV buckets keyed by UTC second buckets.
 * Dedupes by trade_id (or venue+ts+price+qty fallback).
 */
export class CandleBuilder {
  constructor(intervalSec = 1) {
    this.intervalSec = intervalSec;
    this.seen = new Set();
    /** @type {Map<number, { time:number, open:number, high:number, low:number, close:number, volume:number, trades:number, buyVol:number, sellVol:number }>} */
    this.buckets = new Map();
    this.sessionHigh = null;
    this.sessionLow = null;
    this.sessionVolume = 0;
    this.sessionTrades = 0;
    this.sessionBuyVol = 0;
    this.sessionSellVol = 0;
    this.lastPrice = null;
    this.prevPrice = null;
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
    this.sessionTrades = 0;
    this.sessionBuyVol = 0;
    this.sessionSellVol = 0;
    this.lastPrice = null;
    this.prevPrice = null;
  }

  tradeKey(t) {
    if (t.trade_id != null) return `${t.venue}|${t.trade_id}`;
    return `${t.venue}|${t.receive_ts_ns}|${t.price}|${t.quantity}`;
  }

  ingest(entries) {
    if (!this._trades) this._trades = [];
    let added = 0;
    for (const e of entries || []) {
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
        price,
        qty,
        buy: e.aggressor === 'buy',
      };
      this._trades.push(trade);
      this._applyTrade(trade);
      added += 1;
    }
    // Cap memory
    if (this.seen.size > 20000) {
      const keep = [...this.seen].slice(-10000);
      this.seen = new Set(keep);
    }
    if (this._trades.length > 15000) {
      this._trades = this._trades.slice(-10000);
      this.rebuildFromTrades();
    }
    return added;
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
        trades: 0,
        buyVol: 0,
        sellVol: 0,
      };
      this.buckets.set(bucket, c);
    }
    c.high = Math.max(c.high, price);
    c.low = Math.min(c.low, price);
    c.close = price;
    c.volume += qty;
    c.trades += 1;
    if (buy) c.buyVol += qty;
    else c.sellVol += qty;

    this.prevPrice = this.lastPrice ?? price;
    this.lastPrice = price;
    this.sessionVolume += qty;
    this.sessionTrades += 1;
    if (buy) this.sessionBuyVol += qty;
    else this.sessionSellVol += qty;
    if (this.sessionHigh == null || price > this.sessionHigh) this.sessionHigh = price;
    if (this.sessionLow == null || price < this.sessionLow) this.sessionLow = price;
  }

  rebuildFromTrades() {
    const trades = this._trades || [];
    this.buckets.clear();
    this.sessionHigh = null;
    this.sessionLow = null;
    this.sessionVolume = 0;
    this.sessionTrades = 0;
    this.sessionBuyVol = 0;
    this.sessionSellVol = 0;
    const lp = this.lastPrice;
    const pp = this.prevPrice;
    this.lastPrice = null;
    this.prevPrice = null;
    for (const t of trades) this._applyTrade(t);
    if (this.lastPrice == null && lp != null) {
      this.lastPrice = lp;
      this.prevPrice = pp;
    }
  }

  candles() {
    return [...this.buckets.values()].sort((a, b) => a.time - b.time);
  }

  volumeBars() {
    const up = '#02c076';
    const down = '#f6465d';
    return this.candles().map((c) => ({
      time: c.time,
      value: c.volume,
      color: c.close >= c.open ? up : down,
    }));
  }

  /**
   * Volume / trade count for buckets with time >= nowSec - windowSec.
   * @param {number} windowSec
   * @param {number} [nowSec]
   */
  windowStats(windowSec, nowSec = Math.floor(Date.now() / 1000)) {
    const since = nowSec - Math.max(1, windowSec);
    let volume = 0;
    let trades = 0;
    for (const c of this.buckets.values()) {
      if (c.time >= since) {
        volume += c.volume;
        trades += c.trades;
      }
    }
    return { volume, trades, windowSec };
  }
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
