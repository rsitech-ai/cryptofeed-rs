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

test('snapshot can emit full retained history while stats stay on the session window', () => {
  const tr = new MultiVenueTracker(1, 3600);
  tr.syncTargets([{ venue: 'binance-spot', symbol: 'BTCUSDT', live: true }]);
  const tip = 200_000;
  for (let t = tip - 2000; t <= tip; t += 1) {
    tr.touch('binance-spot', 100 + (t % 5) * 0.01, t);
  }
  const view = tr.snapshot('absolute', { windowSec: 300, chartWindowSec: 3600 });
  const row = view.series[0];
  assert.ok(row.data[0].time <= tip - 1500, `chart start ${row.data[0].time}`);
  assert.equal(row.data[row.data.length - 1].time, tip);
  const sessionOnly = tr.snapshot('absolute', { windowSec: 300 });
  assert.ok(sessionOnly.series[0].data[0].time >= tip - 300);
});

test('2h retention keeps ~7200 1s buckets (no 4200 cap)', () => {
  const tr = new MultiVenueTracker(1, 7200);
  tr.syncTargets([{ venue: 'binance-spot', symbol: 'BTCUSDT', live: true }]);
  const tip = 400_000;
  for (let t = tip - 7200; t <= tip; t += 1) {
    tr.touch('binance-spot', 100 + (t % 5) * 0.01, t);
  }
  const st = tr.venues.get('binance-spot');
  assert.ok(st.buckets.size >= 7200, `retained ${st.buckets.size}`);
  assert.ok(st.buckets.size <= 7400, `over-retained ${st.buckets.size}`);
});

test('1h session snapshot display-downsamples to chart budget', () => {
  const tr = new MultiVenueTracker(1, 3600);
  tr.syncTargets([{ venue: 'binance-spot', symbol: 'BTCUSDT', live: true }]);
  const tip = 200_000;
  for (let t = tip - 3600; t <= tip; t += 1) {
    tr.touch('binance-spot', 100 + (t % 9) * 0.01, t);
  }
  const view = tr.snapshot('absolute', { windowSec: 3600 });
  const row = view.series[0];
  assert.ok(row.data.length <= 900, `display pts ${row.data.length}`);
  assert.equal(row.data[row.data.length - 1].time, tip);
  assert.equal(row.last, row.data[row.data.length - 1].value);
});

test('venue samples downsample under sustained quote/trade flood', () => {
  const tr = new MultiVenueTracker(1, 3600);
  tr.syncTargets([{ venue: 'v', symbol: 'BTCUSDT', live: true }]);
  const tip = 300_000;
  // Flood with unique trade_ids (simulates hours of prints)
  for (let i = 0; i < 20000; i++) {
    const sec = tip - 20000 + i;
    tr.ingest('v', [
      {
        kind: 'trade',
        venue: 'v',
        trade_id: `id-${i}`,
        price: String(100 + (i % 11) * 0.01),
        quantity: '0.01',
        exchange_ts_ns: sec * 1e9,
        receive_ts_ns: sec * 1e9,
        aggressor: 'buy',
      },
    ]);
  }
  const st = tr.venues.get('v');
  assert.ok(st.samples.length <= 8000, `samples ${st.samples.length}`);
  assert.ok(st.buckets.size <= 4200, `buckets ${st.buckets.size}`);
  assert.ok(st.seen.size <= 12000, `seen ${st.seen.size}`);
});
