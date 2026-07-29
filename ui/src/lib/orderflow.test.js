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
  clampPriceZoom,
  clampViewSec,
  computeCvd,
  cobColumn,
  computePriceWindow,
  densifyDepthHistory,
  detectFlowHeuristics,
  domLadder,
  flowMarkers,
  footprintClusters,
  heatIntensity,
  heatmapBaselineRgba,
  heatmapColor,
  ladderLevels,
  levelImbalancePct,
  nearestWalls,
  ohlcBucketsFromTape,
  parseOfLayers,
  priceAxisPadPx,
  pushDepthHistory,
  quantizePrice,
  resolveTick,
  restingAtPrice,
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

  it('Y-domain for ~64k BTC includes 64288, not a 4k remnant scale', () => {
    const mid = 64287.99;
    const book = {
      bids: Array.from({ length: 24 }, (_, i) => ({
        price: String(mid - 0.01 * (i + 1)),
        quantity: String(0.4 + (i % 5) * 0.1),
      })),
      asks: Array.from({ length: 24 }, (_, i) => ({
        price: String(mid + 0.01 * i),
        quantity: String(0.35 + (i % 4) * 0.1),
      })),
    };
    let hist = [];
    for (let i = 0; i < 8; i++) {
      hist = pushDepthHistory(hist, sampleBookDepth(book, { t: 1000 + i * 250, tick: 0.01 }), 32);
    }
    const win = computePriceWindow(hist, { focusPrice: mid, tick: 0.01, minTicks: 48, maxBps: 12 });
    assert.ok(win, 'price window required');
    // Must be real BTC spot scale — not mid%10000 (~4287) and not a clipped label remnant.
    assert.ok(win.priceMin < mid && win.priceMax > mid);
    assert.ok(win.priceMin > 60000, `priceMin=${win.priceMin} must stay near 64k`);
    assert.ok(win.priceMax < 70000, `priceMax=${win.priceMax} must stay near 64k`);
    assert.ok(win.priceMin <= 64200 || win.priceMax >= 64300 || (win.priceMin < mid && win.priceMax > mid));
    // Window must hug book depth — not the old ±25bps (~±$160) void that crushed heat to a filament.
    const half = (win.priceMax - win.priceMin) / 2;
    assert.ok(half < mid * 0.0015, `half-span ${half} too wide (empty canvas)`);
    assert.ok(half > 0.2, `half-span ${half} too tight`);

    const grid = buildHeatmapGrid(hist, {
      rows: 60,
      priceMin: win.priceMin,
      priceMax: win.priceMax,
    });
    assert.ok(grid);
    assert.ok(grid.priceMin > 60000 && grid.priceMax < 70000);
    assert.ok(grid.maxVal > 0);
    // Axis pad must fit "64,287.99" — pad of 58px clips the leading 6 → looks like 4,287.
    const pad = priceAxisPadPx(win.priceMax, 1);
    assert.ok(pad >= 72, `pad ${pad} too narrow; clips leading digits of 64k labels`);
    const pad2 = priceAxisPadPx(win.priceMax, 2);
    assert.ok(pad2 >= 140, `dpr=2 pad ${pad2} too narrow`);
  });

  it('caps coarse-tick low-price books to a useful visible range', () => {
    const book = {
      bids: [{ price: '100.00', quantity: '1.5' }, { price: '99.50', quantity: '0.5' }],
      asks: [{ price: '101.00', quantity: '1.25' }, { price: '101.50', quantity: '0.5' }],
    };
    const sample = sampleBookDepth(book, { t: 1, tick: 0.5 });
    const win = computePriceWindow([sample], { focusPrice: 100.5, tick: 0.5 });

    assert.ok(win.priceMin <= 99.5 && win.priceMax >= 101.5, 'book walls must remain visible');
    assert.ok(
      win.priceMin >= 98.5 && win.priceMax <= 102.5,
      `unexpected empty range ${win.priceMin}–${win.priceMax}`,
    );
  });

  it('stabilizes quantize Map keys and resting lookup at 64k', () => {
    const px = 64287.99;
    const tick = 0.01;
    const q = quantizePrice(px, tick);
    assert.equal(q, 64287.99);
    // Drift that previously broke Map.get for resting tooltip.
    const drifted = Math.round(px / tick) * tick;
    assert.equal(quantizePrice(drifted, tick), 64287.99);

    const book = {
      bids: [{ price: '64287.99', quantity: '1.5' }, { price: '64287.98', quantity: '2' }],
      asks: [{ price: '64288.00', quantity: '1.2' }, { price: '64288.01', quantity: '0.8' }],
    };
    const sample = sampleBookDepth(book, { tick: 0.01, t: 1 });
    assert.ok(sample);
    const hit = restingAtPrice(sample, 64287.993, 0.01);
    assert.ok(hit.bidUsd > 0, 'resting bid must be non-zero near BBO');
    assert.ok(hit.price != null && hit.price > 60000);
    const miss = restingAtPrice(sample, 64159.28, 0.01);
    assert.equal(miss.bidUsd, 0);
    assert.equal(miss.askUsd, 0);
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

  it('densifies history with hold-last across time gaps', () => {
    const mid = 100;
    const book = {
      bids: [{ price: '99.9', quantity: '1' }],
      asks: [{ price: '100.1', quantity: '1' }],
    };
    const a = sampleBookDepth(book, { t: 1000, tick: 0.1 });
    const b = sampleBookDepth(book, { t: 3000, tick: 0.1 });
    const dense = densifyDepthHistory([a, b], { bucketMs: 250, maxCols: 20 });
    assert.ok(dense.length >= 8);
    assert.equal(dense[0].t, 1000);
    assert.ok(dense.every((s) => s.bids?.size || s.asks?.size));
  });

  it('fills full Y range and hold-last gap columns', () => {
    const mid = 64250;
    const book = {
      bids: Array.from({ length: 12 }, (_, i) => ({
        price: String(mid - 0.1 * (i + 1)),
        quantity: '1',
      })),
      asks: Array.from({ length: 12 }, (_, i) => ({
        price: String(mid + 0.1 * i),
        quantity: '1',
      })),
    };
    let hist = [];
    for (let i = 0; i < 6; i++) {
      hist = pushDepthHistory(hist, sampleBookDepth(book, { t: 1000 + i * 250, tick: 0.1 }), 32);
    }
    // Simulate SSE gap → hold-last fillers
    hist = pushDepthHistory(
      hist,
      sampleBookDepth(book, { t: 1000 + 6 * 250 + 2000, tick: 0.1 }),
      64,
      { gapMs: 400 },
    );
    assert.ok(hist.length > 7, 'gap fill should insert hold-last samples');

    const win = computePriceWindow(hist, { focusPrice: mid, tick: 0.1, zoom: 1.5 });
    assert.ok(win);
    const grid = buildHeatmapGrid(hist, {
      rows: 40,
      priceMin: win.priceMin,
      priceMax: win.priceMax,
    });
    assert.ok(grid);
    let nonzero = 0;
    for (let i = 0; i < grid.grid.length; i++) if (grid.grid[i] > 0) nonzero++;
    const fillRatio = nonzero / grid.grid.length;
    assert.ok(fillRatio > 0.7, `expected full-height heat fill, got ${fillRatio}`);
  });

  it('clamps price/time zoom helpers', () => {
    assert.equal(clampPriceZoom(0.1), 0.25);
    assert.equal(clampPriceZoom(99), 6);
    assert.equal(clampViewSec(5), 15);
    assert.equal(clampViewSec(99999), 3600);
  });

  it('maps intensity to blue→red palette', () => {
    const cold = heatmapColor(0.05);
    const hot = heatmapColor(0.95);
    assert.ok(cold[2] > cold[0]); // bluish
    assert.ok(hot[0] > hot[2]); // reddish
    const base = heatmapBaselineRgba();
    assert.ok(base[2] > base[0], 'baseline must be dark blue, not black');
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
    assert.equal(L.cob, false);
    assert.ok(serializeOfLayers(L).includes('heat'));
    assert.equal(resolveTick('auto', { bids: [{ price: '100' }, { price: '99' }], asks: [] }), 1);
    assert.equal(resolveTick('0.5', null), 0.5);
    const full = parseOfLayers(null);
    assert.equal(full.cob, true);
    assert.equal(full.markers, true);
  });

  it('builds candles, footprint, COB, markers, nearest walls', () => {
    const tape = [
      trade({ side: 'buy', price: 64200, qty: 0.5, sec: 100 }),
      trade({ side: 'sell', price: 64199, qty: 0.4, sec: 101 }),
      trade({ side: 'buy', price: 64201, qty: 2, sec: 105 }),
    ];
    const ohlc = ohlcBucketsFromTape(tape, { bucketSec: 5 });
    assert.ok(ohlc.length >= 1);
    assert.ok(ohlc[0].h >= ohlc[0].l);
    const fp = footprintClusters(tape, { tick: 1, bucketSec: 5 });
    assert.ok(fp.length >= 1);
    const book = {
      bids: [
        { price: '64200', quantity: '1' },
        { price: '64199', quantity: '2' },
      ],
      asks: [
        { price: '64201', quantity: '1.5' },
        { price: '64202', quantity: '1' },
      ],
    };
    const sample = sampleBookDepth(book, { tick: 1, t: 1 });
    const cob = cobColumn(sample, { priceMin: 64190, priceMax: 64210 });
    assert.ok(cob.rows.length >= 2);
    assert.ok(cob.maxUsd > 0);
    const walls = nearestWalls(sample, 64200.5);
    assert.ok(walls.bidUsd > 0);
    assert.ok(walls.askUsd > 0);
    const marks = flowMarkers(
      [trade({ side: 'buy', price: 64200, qty: 1, sec: 1, notional: 50000 })],
      book,
      { largeUsd: 10000 },
    );
    assert.ok(marks.some((m) => m.honest && m.marker));
  });

  it('filters DOM outlier stubs far from BBO', () => {
    const book = {
      bids: [
        { price: '64203.68', quantity: '1' },
        { price: '64203.50', quantity: '2' },
        { price: '64000.00', quantity: '99' }, // far stub
      ],
      asks: [
        { price: '64203.69', quantity: '1' },
        { price: '64204.00', quantity: '1' },
        { price: '65000.00', quantity: '99' },
      ],
    };
    const L = domLadder(book, { depth: 8, tick: 0.01 });
    assert.ok(L.rows.every((r) => Math.abs(r.price - 64203.68) < 50));
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
