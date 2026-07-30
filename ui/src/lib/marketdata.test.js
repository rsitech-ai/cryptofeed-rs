import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CandleBuilder } from './ohlcv.js';
import { MultiVenueTracker } from './series.js';

const trade = (id, price, ns) => ({
  kind: 'trade',
  venue: 'okx',
  symbol: 'BTC-USDT',
  trade_id: id,
  price: String(price),
  quantity: '1',
  exchange_ts_ns: ns,
  receive_ts_ns: ns,
});

describe('newest-first tape batches', () => {
  it('builds chronological OHLC close and last price', () => {
    const b = new CandleBuilder(60);
    b.ingest([
      trade('new', 103, 103_000_000_000),
      trade('mid', 102, 102_000_000_000),
      trade('old', 101, 101_000_000_000),
    ]);
    assert.equal(b.lastPrice, 103);
    assert.equal(b.candles()[0].open, 101);
    assert.equal(b.candles()[0].close, 103);
  });

  it('derives the multi-venue last price from the newest event', () => {
    const tracker = new MultiVenueTracker(60);
    tracker.syncTargets([{ venue: 'okx', symbol: 'BTC-USDT', live: true }]);
    tracker.ingest('okx', [
      trade('new', 103, 103_000_000_000),
      trade('mid', 102, 102_000_000_000),
      trade('old', 101, 101_000_000_000),
    ]);
    assert.equal(tracker.snapshot('absolute').series[0].last, 103);
  });

  it('does not regress close/last when an older unseen page arrives later', () => {
    const b = new CandleBuilder(60);
    b.ingest([trade('new', 103, 103_000_000_000)]);
    b.ingest([trade('old', 101, 101_000_000_000)]);
    assert.equal(b.lastPrice, 103);
    assert.equal(b.candles()[0].close, 103);

    const tracker = new MultiVenueTracker(60);
    tracker.syncTargets([{ venue: 'okx', symbol: 'BTC-USDT', live: true }]);
    tracker.ingest('okx', [trade('new', 103, 103_000_000_000)]);
    tracker.ingest('okx', [trade('old', 101, 101_000_000_000)]);
    assert.equal(tracker.snapshot('absolute').series[0].last, 103);
  });

  it('candle view window clips without wiping retained buckets', () => {
    const b = new CandleBuilder(1, 3600);
    const tip = 50_000;
    for (let t = tip - 2000; t <= tip; t += 1) {
      const ns = t * 1e9;
      b.ingest([
        {
          kind: 'trade',
          venue: 'okx',
          trade_id: `t${t}`,
          price: '100',
          quantity: '1',
          exchange_ts_ns: ns,
          receive_ts_ns: ns,
        },
      ]);
    }
    assert.ok(b.buckets.size >= 2000);
    const view = b.candles(300);
    assert.ok(view.length <= 301);
    assert.equal(view[view.length - 1].time, tip);
    assert.ok(b.buckets.has(tip - 1500), 'underlying 1h buffer kept');
  });
});
