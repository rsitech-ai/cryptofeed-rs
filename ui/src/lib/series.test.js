import test from 'node:test';
import assert from 'node:assert/strict';
import { MultiVenueTracker } from './series.js';

test('snapshot clips series to session window ending at latest data time', () => {
  const tr = new MultiVenueTracker(1);
  tr.syncTargets([{ venue: 'binance-spot', symbol: 'BTCUSDT', live: true }]);
  // Simulate 10 minutes of buckets ending at t=10_000
  for (let t = 9400; t <= 10000; t += 1) {
    tr.touch('binance-spot', 100 + (t % 7) * 0.01, t);
  }
  const snap = tr.snapshot('absolute', { windowSec: 300 });
  const row = snap.series.find((s) => s.venue === 'binance-spot');
  assert.ok(row);
  assert.ok(row.data.length > 0);
  assert.equal(row.lastTime, 10000);
  assert.ok(row.data[0].time >= 9700, `window start ${row.data[0].time}`);
  assert.equal(row.data[row.data.length - 1].time, 10000);
  // Must not leave a gap to wall clock — last point is data tip
  assert.equal(row.data[row.data.length - 1].value, row.last);
});

test('ingest trade advances lastTime', () => {
  const tr = new MultiVenueTracker(1);
  tr.syncTargets([{ venue: 'v', symbol: 'BTCUSDT', live: true }]);
  const ns = 1_700_000_000_000_000_000;
  tr.ingest('v', [
    {
      kind: 'trade',
      venue: 'v',
      trade_id: '1',
      price: '42000',
      quantity: '0.01',
      exchange_ts_ns: ns,
      receive_ts_ns: ns,
      aggressor: 'buy',
    },
  ]);
  const snap = tr.snapshot('absolute', { windowSec: 300 });
  assert.equal(snap.series[0].last, 42000);
  assert.equal(snap.series[0].lastTime, Math.floor(ns / 1e9));
});

test('tracker retains ~1h buckets while session snapshot clips view', () => {
  const tr = new MultiVenueTracker(1, 3600);
  tr.syncTargets([{ venue: 'binance-spot', symbol: 'BTCUSDT', live: true }]);
  const tip = 100_000;
  for (let t = tip - 4000; t <= tip; t += 1) {
    tr.touch('binance-spot', 100 + (t % 5) * 0.01, t);
  }
  const st = tr.venues.get('binance-spot');
  assert.ok(st.buckets.size >= 3600, `retained ${st.buckets.size}`);
  assert.ok(st.buckets.size <= 4120, `over-retained ${st.buckets.size}`);
  const view = tr.snapshot('absolute', { windowSec: 300 });
  const row = view.series[0];
  assert.ok(row.data[0].time >= tip - 300);
  assert.equal(row.data[row.data.length - 1].time, tip);
  // Full buffer still present after view clip
  assert.ok(st.buckets.has(tip - 3500));
});
