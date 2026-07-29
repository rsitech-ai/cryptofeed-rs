/**
 * Unit tests for order-flow + pulse math (node:test).
 * Run: node --test ui/src/lib/*.test.js
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  aggressorSign,
  bookPressure,
  buildHeatmapGrid,
  computeCvd,
  detectFlowHeuristics,
  domLadder,
  heatIntensity,
  heatmapColor,
  ladderLevels,
  levelImbalancePct,
  parseOfLayers,
  pushDepthHistory,
  resolveTick,
  sampleBookDepth,
  serializeOfLayers,
  tradeBubbles,
  tradeNotional,
  volumeAtPrice,
  volumeBarsFromTape,
  sparkPath,
} from './orderflow.js';
import {
  bookImbalanceFromSnap,
  computePulse,
  median,
  pulseSpike,
  pushPulseHistory,
  venueHeat,
} from './pulse.js';

const NS = 1e9;

function trade(opts) {
  return {
    kind: 'trade',
    aggressor: opts.side,
    price: String(opts.price),
    quantity: String(opts.qty),
    notional: opts.notional != null ? String(opts.notional) : undefined,
    exchange_ts_ns: (opts.sec ?? 1000) * NS,
    receive_ts_ns: (opts.sec ?? 1000) * NS,
    trade_id: opts.id ?? `${opts.side}-${opts.sec}-${opts.price}`,
  };
}

describe('tradeNotional / aggressorSign', () => {
  it('prefers server notional', () => {
    assert.equal(tradeNotional({ notional: '12.5', price: '100', quantity: '1' }), 12.5);
  });
  it('falls back to price*qty', () => {
    assert.equal(tradeNotional({ price: '100', quantity: '0.5' }), 50);
  });
  it('maps aggressor', () => {
    assert.equal(aggressorSign('buy'), 1);
    assert.equal(aggressorSign('sell'), -1);
    assert.equal(aggressorSign(null), 0);
  });
});

describe('computeCvd', () => {
  it('accumulates buy+ / sell− in USD', () => {
    const tape = [
      trade({ side: 'buy', price: 100, qty: 2, sec: 1 }), // +200
      trade({ side: 'sell', price: 100, qty: 1, sec: 2 }), // -100
      trade({ side: 'buy', price: 100, qty: 0.5, sec: 3 }), // +50
    ];
    const r = computeCvd(tape);
    assert.equal(r.cvd, 150);
    assert.equal(r.buyUsd, 250);
    assert.equal(r.sellUsd, 100);
    assert.equal(r.trades, 3);
    assert.equal(r.points.length, 3);
    assert.equal(r.points[2].cvd, 150);
  });

  it('filters by window', () => {
    const tape = [
      trade({ side: 'buy', price: 10, qty: 1, sec: 100 }),
      trade({ side: 'sell', price: 10, qty: 1, sec: 200 }),
    ];
    const r = computeCvd(tape, { windowSec: 50, nowSec: 210 });
    assert.equal(r.trades, 1);
    assert.equal(r.cvd, -10);
  });
});

describe('volumeAtPrice', () => {
  it('aggregates buy/sell at price buckets', () => {
    const tape = [
      trade({ side: 'buy', price: 100.01, qty: 1, sec: 1 }),
      trade({ side: 'sell', price: 100.01, qty: 2, sec: 2 }),
      trade({ side: 'buy', price: 99.5, qty: 3, sec: 3 }),
    ];
    const vap = volumeAtPrice(tape, { tickSize: 0.01 });
    const top = vap.find((r) => Math.abs(r.price - 100.01) < 1e-9);
    assert.ok(top);
    assert.equal(top.buyQty, 1);
    assert.equal(top.sellQty, 2);
    assert.equal(top.delta, tradeNotional(tape[0]) - tradeNotional(tape[1]));
  });
});

describe('book pressure / ladder', () => {
  const book = {
    bids: [
      { price: '100', quantity: '2' },
      { price: '99', quantity: '3' },
    ],
    asks: [
      { price: '101', quantity: '1' },
      { price: '102', quantity: '1' },
    ],
  };

  it('computes USD pressure and imbalance', () => {
    const p = bookPressure(book, 2);
    // bid: 100*2 + 99*3 = 497; ask: 101*1 + 102*1 = 203
    assert.equal(p.bidUsd, 497);
    assert.equal(p.askUsd, 203);
    assert.ok(p.imbalancePct > 0);
    assert.ok(Math.abs(p.bidPct + p.askPct - 100) < 1e-9);
  });

  it('builds cumulative ladder', () => {
    const L = ladderLevels(book, 2);
    assert.equal(L.bids[1].cumQty, 5);
    assert.equal(L.asks[1].cumQty, 2);
  });

  it('level imbalance', () => {
    assert.equal(levelImbalancePct(3, 1), 50);
    assert.equal(levelImbalancePct(0, 0), 0);
  });
});

describe('heuristics', () => {
  it('flags large trades above threshold', () => {
    const tape = [trade({ side: 'buy', price: 50000, qty: 2, sec: 1 })]; // 100k
    const h = detectFlowHeuristics(tape, null, { largeUsd: 25000 });
    assert.ok(h.some((x) => x.kind === 'large' && x.heuristic === true));
  });
});

describe('sparkPath', () => {
  it('returns path for 2+ points', () => {
    const d = sparkPath([1, 2, 3]);
    assert.match(d, /^M/);
    assert.ok(d.includes('L'));
  });
  it('empty for short series', () => {
    assert.equal(sparkPath([1]), '');
  });
});

describe('heatmap / depth ring', () => {
  it('samples book depth and builds grid', () => {
    const book = {
      bids: [
        { price: '100', quantity: '5' },
        { price: '99.9', quantity: '2' },
      ],
      asks: [
        { price: '100.1', quantity: '4' },
        { price: '100.2', quantity: '1' },
      ],
    };
    let hist = [];
    for (let i = 0; i < 5; i++) {
      const s = sampleBookDepth(book, { t: 1000 + i * 200, tick: 0.1 });
      assert.ok(s);
      assert.ok(s.bids.size >= 1);
      hist = pushDepthHistory(hist, s, 10);
    }
    assert.equal(hist.length, 5);
    const grid = buildHeatmapGrid(hist, { rows: 20 });
    assert.ok(grid);
    assert.ok(grid.maxVal > 0);
    assert.ok(grid.grid.length === grid.rows * grid.cols);
  });

  it('builds trade bubbles and volume bars', () => {
    const tape = [
      trade({ side: 'buy', price: 100, qty: 2, sec: 10 }),
      trade({ side: 'sell', price: 100, qty: 1, sec: 10 }),
      trade({ side: 'buy', price: 100.1, qty: 3, sec: 11 }),
    ];
    const bubbles = tradeBubbles(tape, { tick: 0.1, bucketMs: 1000 });
    assert.ok(bubbles.length >= 1);
    assert.ok(bubbles.some((b) => b.buyUsd > 0));
    const bars = volumeBarsFromTape(tape, { bucketSec: 1 });
    assert.ok(bars.length >= 1);
  });

  it('maps intensity to blue→red palette', () => {
    const cold = heatmapColor(0.05);
    const hot = heatmapColor(0.95);
    assert.ok(cold[2] > cold[0]); // bluish
    assert.ok(hot[0] > hot[2]); // reddish
  });

  it('applies heat intensity gain', () => {
    const soft = heatIntensity(50, 100, 0.5);
    const hard = heatIntensity(50, 100, 2);
    assert.ok(hard > soft);
    assert.ok(soft > 0 && hard <= 1);
  });

  it('builds classic DOM ladder bid|price|ask', () => {
    const book = {
      bids: [
        { price: '100', quantity: '2' },
        { price: '99', quantity: '3' },
      ],
      asks: [
        { price: '101', quantity: '1' },
        { price: '102', quantity: '4' },
      ],
    };
    const L = domLadder(book, { depth: 4, tick: 1 });
    assert.ok(L.rows.length >= 2);
    assert.equal(L.bestBid, 100);
    assert.equal(L.bestAsk, 101);
    const topBid = L.rows.find((r) => r.price === 100);
    assert.ok(topBid && topBid.bidQty === 2);
    const topAsk = L.rows.find((r) => r.price === 101);
    assert.ok(topAsk && topAsk.askQty === 1);
  });

  it('parses layer flags and resolves tick', () => {
    const L = parseOfLayers('heat,bubbles,cvd');
    assert.equal(L.heat, true);
    assert.equal(L.vap, false);
    assert.equal(serializeOfLayers(L), 'heat,bubbles,cvd');
    assert.equal(resolveTick('auto', { bids: [{ price: '100' }, { price: '99' }], asks: [] }), 1);
    assert.equal(resolveTick('0.5', null), 0.5);
  });
});

describe('pulse', () => {
  it('median + heat + aggregate', () => {
    assert.equal(median([1, 3, 2]), 2);
    assert.equal(median([1, 2, 3, 4]), 2.5);
    const heat = venueHeat({ tradesPerMin: 80, usdPerMin: 500000, imbalancePct: 20, live: true });
    assert.ok(heat > 40 && heat <= 100);
    const pulse = computePulse(
      [
        { venue: 'a', live: true, tradesPerMin: 60, usdPerMin: 100000, spreadBps: 1, imbalancePct: 10 },
        { venue: 'b', live: true, tradesPerMin: 30, usdPerMin: 50000, spreadBps: 2, imbalancePct: -5 },
      ],
      { crossBps: 8 },
    );
    assert.ok(pulse.tradesPerMin === 90);
    assert.ok(pulse.usdPerMin === 150000);
    assert.equal(pulse.crossBps, 8);
    assert.ok(pulse.score >= 0 && pulse.score <= 100);
    assert.equal(pulse.chips.length, 2);
  });

  it('book imbalance from snap', () => {
    const book = {
      bids: [{ price: '100', quantity: '10' }],
      asks: [{ price: '101', quantity: '2' }],
    };
    const imb = bookImbalanceFromSnap(book, 1);
    assert.ok(imb != null && imb > 0);
  });

  it('spike detection', () => {
    let hist = [];
    for (let i = 0; i < 10; i++) hist = pushPulseHistory(hist, { score: 40, tradesPerMin: 10, usdPerMin: 1 });
    hist = pushPulseHistory(hist, { score: 90, tradesPerMin: 200, usdPerMin: 1e6 });
    assert.equal(pulseSpike(hist, 72), true);
    assert.equal(pulseSpike(hist.slice(0, 10), 72), false);
  });
});
