/**
 * Unit tests for Binance Top Movers formulas.
 * Run: node --test ui/src/lib/topMovers.test.js
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  HIGH_VOL_BANDS,
  HIGH_VOL_MULT,
  RISE_FALL_BANDS,
  aggregateCandles,
  classifyPctBand,
  detectTopMovers,
  isLargeOrder,
  isPullback,
  isRally,
  passesHighVolGate,
  pctChange,
  priceAtOrBefore,
  utcDayBounds,
} from './topMovers.js';

describe('pctChange / classifyPctBand', () => {
  it('computes percent change', () => {
    assert.ok(Math.abs(pctChange(100, 107) - 7) < 1e-12);
    assert.ok(Math.abs(pctChange(100, 93) - -7) < 1e-12);
    assert.equal(pctChange(0, 1), null);
  });

  it('maps rise/fall bands per FAQ', () => {
    assert.equal(classifyPctBand(2.9, RISE_FALL_BANDS), null);
    assert.equal(classifyPctBand(3, RISE_FALL_BANDS)?.id, 'small');
    assert.equal(classifyPctBand(6.99, RISE_FALL_BANDS)?.id, 'small');
    assert.equal(classifyPctBand(7, RISE_FALL_BANDS)?.id, 'mid');
    assert.equal(classifyPctBand(10.99, RISE_FALL_BANDS)?.id, 'mid');
    assert.equal(classifyPctBand(11, RISE_FALL_BANDS)?.id, 'high');
    assert.equal(classifyPctBand(50, RISE_FALL_BANDS)?.id, 'high');
  });

  it('maps high-vol bands per FAQ', () => {
    assert.equal(classifyPctBand(6.9, HIGH_VOL_BANDS), null);
    assert.equal(classifyPctBand(7, HIGH_VOL_BANDS)?.id, 'small');
    assert.equal(classifyPctBand(11, HIGH_VOL_BANDS)?.id, 'mid');
    assert.equal(classifyPctBand(15, HIGH_VOL_BANDS)?.id, 'high');
  });
});

describe('Pullback / Rally', () => {
  it('Pullback: day high ≥8% above open and close within 5% of high', () => {
    assert.equal(isPullback({ dayOpen: 100, dayHigh: 110, close: 106 }), true); // +10% day, −3.6% from high
    assert.equal(isPullback({ dayOpen: 100, dayHigh: 107, close: 106 }), false); // day up only 7%
    assert.equal(isPullback({ dayOpen: 100, dayHigh: 120, close: 110 }), false); // 8.3% off high
  });

  it('Rally: day low ≤−8% from open and close ≥5% above low', () => {
    assert.equal(isRally({ dayOpen: 100, dayLow: 90, close: 95 }), true);
    assert.equal(isRally({ dayOpen: 100, dayLow: 93, close: 98 }), false); // only −7%
    assert.equal(isRally({ dayOpen: 100, dayLow: 90, close: 93 }), false); // only +3.3% bounce
  });
});

describe('High-vol gate / Large order', () => {
  it('requires current vol ≥ 50× average of priors', () => {
    const priors = Array(24).fill(10);
    assert.equal(passesHighVolGate(10 * HIGH_VOL_MULT, priors), true);
    assert.equal(passesHighVolGate(10 * HIGH_VOL_MULT - 1, priors), false);
    assert.equal(passesHighVolGate(500, []), false);
  });

  it('large order uses 50× average qty', () => {
    assert.equal(isLargeOrder(50, 1), true);
    assert.equal(isLargeOrder(49, 1), false);
  });
});

describe('candle helpers', () => {
  it('priceAtOrBefore picks last close ≤ sec', () => {
    const c = [
      { time: 100, close: 1 },
      { time: 200, close: 2 },
      { time: 300, close: 3 },
    ];
    assert.equal(priceAtOrBefore(c, 250), 2);
    assert.equal(priceAtOrBefore(c, 99), null);
  });

  it('aggregateCandles rolls into 15m buckets', () => {
    const base = 1_700_000_000;
    const start = Math.floor(base / 900) * 900;
    const c = [
      { time: start + 1, open: 10, high: 11, low: 9, close: 10.5, volume: 1 },
      { time: start + 100, open: 10.5, high: 12, low: 10, close: 11, volume: 2 },
      { time: start + 900, open: 11, high: 11.5, low: 10.5, close: 11.2, volume: 3 },
    ];
    const out = aggregateCandles(c, 900);
    assert.equal(out.length, 2);
    assert.equal(out[0].high, 12);
    assert.equal(out[0].volume, 3);
    assert.equal(out[1].volume, 3);
  });

  it('utcDayBounds is UTC midnight', () => {
    const { startSec, endSec } = utcDayBounds(1_700_006_400); // fixed epoch
    assert.equal(endSec - startSec, 86399);
    assert.equal(startSec % 86400, 0);
  });
});

describe('detectTopMovers', () => {
  const NS = 1e9;

  function mkCandles(pairs) {
    return pairs.map(([time, open, high, low, close, volume = 1]) => ({
      time,
      open,
      high,
      low,
      close,
      volume,
      notional: volume,
      trades: 1,
    }));
  }

  it('flags [Mid] 5min Rise at +8%', () => {
    const now = 2_000_000;
    const candles = mkCandles([
      [now - 300, 100, 100, 100, 100],
      [now - 60, 100, 108, 100, 108],
      [now, 108, 108, 108, 108],
    ]);
    const { statuses, metrics } = detectTopMovers({ candles, nowSec: now });
    assert.ok(metrics.pct5m != null && metrics.pct5m >= 7);
    const rise = statuses.find((s) => s.id === 'mid_5m_rise');
    assert.ok(rise, `expected mid 5m rise, got ${statuses.map((s) => s.id).join(',')}`);
    assert.match(rise.label, /\[Mid\] 5min Rise/);
  });

  it('flags New 24hr High when last 1m makes the day high', () => {
    const dayStart = Math.floor(2_000_000 / 86400) * 86400;
    const now = dayStart + 3600;
    const candles = mkCandles([
      [dayStart + 10, 100, 105, 99, 104],
      [now - 120, 104, 106, 103, 105], // prior to last 1m
      [now - 30, 104, 110, 104, 110],
      [now, 110, 110, 110, 110],
    ]);
    const { statuses } = detectTopMovers({ candles, nowSec: now });
    assert.ok(statuses.some((s) => s.id === 'new_24hr_high'));
  });

  it('does not flag New High/Low on cold-start (<1m of day history)', () => {
    const dayStart = Math.floor(2_100_000 / 86400) * 86400;
    const now = dayStart + 30;
    const candles = mkCandles([
      [now - 10, 100, 101, 99, 100],
      [now, 100, 100, 100, 100],
    ]);
    const { statuses } = detectTopMovers({ candles, nowSec: now });
    assert.equal(statuses.some((s) => s.id === 'new_24hr_high'), false);
    assert.equal(statuses.some((s) => s.id === 'new_24hr_low'), false);
  });

  it('flags Pullback after ≥8% day up with close near high', () => {
    const dayStart = Math.floor(3_000_000 / 86400) * 86400;
    const now = dayStart + 7200;
    const candles = mkCandles([
      [dayStart + 1, 100, 100, 100, 100],
      [dayStart + 1000, 100, 112, 100, 112],
      [now, 112, 112, 108, 108], // within 5% of 112
    ]);
    const { statuses } = detectTopMovers({ candles, nowSec: now });
    assert.ok(statuses.some((s) => s.id === 'pullback'));
  });

  it('flags Large Buy when qty ≥ 50× average', () => {
    const now = 4_000_000;
    const candles = mkCandles([[now, 100, 100, 100, 100]]);
    const tape = [];
    for (let i = 0; i < 20; i++) {
      tape.push({
        kind: 'trade',
        aggressor: 'sell',
        price: '100',
        quantity: '1',
        exchange_ts_ns: (now - i) * NS,
      });
    }
    tape.unshift({
      kind: 'trade',
      aggressor: 'buy',
      price: '100',
      quantity: '60',
      exchange_ts_ns: now * NS,
    });
    const { statuses } = detectTopMovers({ candles, tape, nowSec: now });
    assert.ok(statuses.some((s) => s.id === 'large_buy'));
  });

  it('marks 7d/30d coverage gaps without inventing statuses', () => {
    const now = 5_000_000;
    const candles = mkCandles([
      [now - 3600, 100, 100, 100, 100],
      [now, 100, 100, 100, 100],
    ]);
    const { statuses, coverage } = detectTopMovers({ candles, nowSec: now, historySecs: 3600 });
    assert.equal(coverage.has7d, false);
    assert.equal(coverage.has30d, false);
    assert.equal(statuses.some((s) => /7day|30day/.test(s.id)), false);
    assert.ok(coverage.notes.some((n) => /7d/.test(n)));
  });

  it('fires High Vol up when 15m move + volume gate pass', () => {
    const now = 6_000_000;
    const bucket = 900;
    const curStart = Math.floor(now / bucket) * bucket;
    const candles = [];
    // 24 prior quiet buckets
    for (let i = 24; i >= 1; i--) {
      const t = curStart - i * bucket;
      candles.push({
        time: t,
        open: 100,
        high: 100.1,
        low: 99.9,
        close: 100,
        volume: 10,
        notional: 10,
        trades: 1,
      });
    }
    // current: +8% with huge volume
    candles.push({
      time: curStart,
      open: 100,
      high: 109,
      low: 100,
      close: 108,
      volume: 10 * HIGH_VOL_MULT,
      notional: 10 * HIGH_VOL_MULT,
      trades: 1,
    });
    candles.push({
      time: now,
      open: 108,
      high: 108,
      low: 108,
      close: 108,
      volume: 1,
      notional: 1,
      trades: 1,
    });
    const { statuses, coverage } = detectTopMovers({ candles, nowSec: now });
    assert.equal(coverage.highVolReady, true);
    assert.ok(
      statuses.some((s) => s.id === 'small_vol_up'),
      statuses.map((s) => s.id).join(','),
    );
  });
});
