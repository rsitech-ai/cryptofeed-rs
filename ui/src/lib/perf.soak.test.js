/**
 * Simulated ≥30 min / 1h soak: multi-venue ingest + depth/depth rings stay bounded.
 * Run: node --test src/lib/perf.soak.test.js
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import { MultiVenueTracker } from './series.js';
import { CandleBuilder } from './ohlcv.js';
import { DiscrepancyTracker } from './discrepancy.js';
import {
  CHART_DISPLAY_MAX_POINTS,
  SeriesHistoryPolicy,
  TAPE_OF_MAX,
  compactDepthHistory,
  depthHistoryBudget,
  downsampleForChart,
  tapeMaxEntries,
  venueSampleBudget,
} from './history.js';
import { pushDepthHistory, sampleBookDepth } from './orderflow.js';

function fakeBook(mid, t) {
  const bids = [];
  const asks = [];
  for (let i = 0; i < 40; i++) {
    bids.push({ price: String(mid - 0.01 * (i + 1)), quantity: String(0.2 + (i % 5) * 0.05) });
    asks.push({ price: String(mid + 0.01 * i), quantity: String(0.2 + (i % 4) * 0.05) });
  }
  return { bids, asks, venue: 'binance-spot', symbol: 'BTCUSDT', t };
}

test('soak ~1h multi-venue: buffers stay under hard caps', () => {
  const historySecs = 3600;
  const policy = new SeriesHistoryPolicy(historySecs);
  const tracker = new MultiVenueTracker(1, historySecs);
  const candles = new CandleBuilder(1, historySecs);
  const disc = new DiscrepancyTracker(historySecs);
  const venues = [
    'binance-spot',
    'coinbase',
    'okx-spot',
    'bybit-spot',
    'bitfinex',
    'kraken',
    'bitstamp',
    'gemini',
    'binance-usdm',
    'okx-swap',
    'bybit-linear',
    'binance-coinm',
    'gate-spot',
  ];
  tracker.syncTargets(venues.map((v) => ({ venue: v, symbol: 'BTCUSDT', live: true })));

  /** @type {Map<string, object>} */
  const focusTape = new Map();
  /** @type {object[]} */
  let depth = [];
  const tip0 = 1_700_000_000;
  const steps = 3600; // 1 simulated hour @ 1 Hz

  for (let i = 0; i < steps; i++) {
    const sec = tip0 + i;
    const mid = 64000 + Math.sin(i / 40) * 25 + (i % 7) * 0.1;
    for (let vi = 0; vi < venues.length; vi++) {
      const venue = venues[vi];
      const px = mid + (vi - 6) * 0.15;
      tracker.ingest(venue, [
        {
          kind: 'trade',
          venue,
          trade_id: `${venue}-${i}`,
          price: String(px),
          quantity: '0.01',
          exchange_ts_ns: sec * 1e9,
          receive_ts_ns: sec * 1e9,
          aggressor: i % 2 ? 'buy' : 'sell',
        },
        {
          kind: 'quote',
          venue,
          bid_price: String(px - 0.05),
          ask_price: String(px + 0.05),
          receive_ts_ns: sec * 1e9 + 1,
        },
      ]);
    }
    // Focus tape ring (same algorithm as App — capped)
    const trade = {
      kind: 'trade',
      venue: 'binance-spot',
      trade_id: `focus-${i}`,
      price: String(mid),
      quantity: '0.02',
      exchange_ts_ns: sec * 1e9,
      receive_ts_ns: sec * 1e9,
      aggressor: 'buy',
    };
    focusTape.set(`t:${trade.trade_id}`, trade);
    if (focusTape.size > policy.tapeMaxEntries()) {
      const keys = [...focusTape.keys()];
      for (const k of keys.slice(0, focusTape.size - policy.tapeMaxEntries())) {
        focusTape.delete(k);
      }
    }
    candles.ingest([trade]);

    const sample = sampleBookDepth(fakeBook(mid, sec * 1000), {
      t: sec * 1000,
      tick: 0.01,
      maxLevels: 48,
    });
    depth = pushDepthHistory(depth, sample, policy.depthBudget().maxCols, {
      historySecs,
      gapMs: 450,
    });

    const snap = tracker.snapshot('percent', { windowSec: 3600 });
    disc.push(snap.discrepancy, snap.series, sec);
  }

  // Final compact (as App would after overshoot)
  depth = compactDepthHistory(depth, historySecs);

  let linePts = 0;
  let maxSamples = 0;
  let maxSeen = 0;
  for (const st of tracker.venues.values()) {
    linePts += st.buckets.size;
    maxSamples = Math.max(maxSamples, st.samples.length);
    maxSeen = Math.max(maxSeen, st.seen.size);
  }
  const snap = tracker.snapshot('percent', { windowSec: 3600 });
  const displayPts = snap.series.reduce((s, r) => s + (r.data?.length || 0), 0);

  assert.ok(focusTape.size <= TAPE_OF_MAX, `focusTape ${focusTape.size}`);
  assert.ok(focusTape.size <= tapeMaxEntries(historySecs), `focusTape policy`);
  assert.ok(depth.length <= depthHistoryBudget(historySecs).maxCols, `depth ${depth.length}`);
  assert.ok(maxSamples <= venueSampleBudget(historySecs), `samples ${maxSamples}`);
  assert.ok(maxSeen <= 12000, `seen ${maxSeen}`);
  assert.ok(linePts <= venues.length * 4200, `linePts ${linePts}`);
  assert.ok(displayPts <= venues.length * CHART_DISPLAY_MAX_POINTS, `display ${displayPts}`);
  assert.ok(disc.points().length <= disc.maxPoints, `bps ${disc.points().length}`);
  assert.ok((candles._trades?.length || 0) <= 12000, `candle trades ${candles._trades?.length}`);
  // Tip still present — trim must not corrupt live edge
  assert.ok(snap.series.every((r) => r.last != null));
  assert.equal(
    Math.max(...snap.series.map((r) => r.lastTime || 0)),
    tip0 + steps - 1,
  );
});

test('chart display downsample is stable under repeated calls', () => {
  const tip = 10_000;
  const pts = [];
  for (let t = tip - 3600; t <= tip; t++) pts.push({ time: t, value: t });
  const a = downsampleForChart(pts, 3600);
  const b = downsampleForChart(a, 3600);
  assert.ok(a.length <= CHART_DISPLAY_MAX_POINTS);
  assert.equal(a.length, b.length);
  assert.equal(a[a.length - 1].time, tip);
});
