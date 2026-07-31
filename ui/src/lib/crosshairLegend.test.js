import { describe, it, mock } from 'node:test';
import assert from 'node:assert/strict';
import {
  setCrosshairOnCharts,
  wireCrosshairSync,
} from './chartSync.js';
import {
  buildHoverLegend,
  formatHoverTime,
  samplePointAtTime,
} from './crosshairLegend.js';

function fakeCrosshairChart() {
  /** @type {Array<(param: any) => void>} */
  let handlers = [];
  const series = {
    dataByIndex: mock.fn(() => ({ value: 42 })),
  };
  const scale = {
    timeToIndex: mock.fn(() => 3),
    timeToCoordinate: mock.fn(() => 120),
  };
  const chart = {
    timeScale: () => scale,
    subscribeCrosshairMove: mock.fn((h) => {
      handlers.push(h);
    }),
    unsubscribeCrosshairMove: mock.fn((h) => {
      handlers = handlers.filter((x) => x !== h);
    }),
    setCrosshairPosition: mock.fn(),
    clearCrosshairPosition: mock.fn(),
  };
  return {
    chart,
    series,
    scale,
    emit: (param) => {
      for (const h of handlers.slice()) h(param);
    },
  };
}

describe('wireCrosshairSync', () => {
  it('fans setCrosshairPosition to peer charts and clears on leave', () => {
    const a = fakeCrosshairChart();
    const b = fakeCrosshairChart();
    const guard = { active: false };
    const moves = [];
    const dispose = wireCrosshairSync(
      [
        { chart: a.chart, series: a.series },
        { chart: b.chart, series: b.series },
      ],
      guard,
      { onMove: (p) => moves.push(p.time) },
    );

    const seriesData = new Map([[a.series, { value: 10 }]]);
    a.emit({ time: 1000, point: { x: 40, y: 20 }, seriesData });
    assert.equal(b.chart.setCrosshairPosition.mock.callCount(), 1);
    assert.deepEqual(b.chart.setCrosshairPosition.mock.calls[0].arguments[1], 1000);
    assert.equal(moves.at(-1), 1000);

    a.emit({ time: null, point: undefined, seriesData: new Map() });
    assert.equal(a.chart.clearCrosshairPosition.mock.callCount(), 1);
    assert.equal(b.chart.clearCrosshairPosition.mock.callCount(), 1);
    assert.equal(moves.at(-1), null);

    dispose();
    a.emit({ time: 2000, point: { x: 1, y: 1 }, seriesData });
    assert.equal(b.chart.setCrosshairPosition.mock.callCount(), 1);
  });

  it('ignores re-entrant moves while guard.active', () => {
    const a = fakeCrosshairChart();
    const b = fakeCrosshairChart();
    const guard = { active: true };
    wireCrosshairSync(
      [
        { chart: a.chart, series: a.series },
        { chart: b.chart, series: b.series },
      ],
      guard,
    );
    a.emit({ time: 5, point: { x: 1, y: 1 }, seriesData: new Map() });
    assert.equal(b.chart.setCrosshairPosition.mock.callCount(), 0);
  });
});

describe('setCrosshairOnCharts', () => {
  it('clears every chart when time is null', () => {
    const a = fakeCrosshairChart();
    const b = fakeCrosshairChart();
    setCrosshairOnCharts(
      [
        { chart: a.chart, series: a.series },
        { chart: b.chart, series: b.series },
      ],
      null,
      { active: false },
    );
    assert.equal(a.chart.clearCrosshairPosition.mock.callCount(), 1);
    assert.equal(b.chart.clearCrosshairPosition.mock.callCount(), 1);
  });
});

describe('crosshairLegend', () => {
  it('samplePointAtTime returns last bar at or before t', () => {
    const pts = [
      { time: 10, value: 1 },
      { time: 12, value: 2 },
      { time: 15, value: 3 },
    ];
    assert.deepEqual(samplePointAtTime(pts, 14), { time: 12, value: 2 });
    assert.deepEqual(samplePointAtTime(pts, 9), null);
    assert.deepEqual(samplePointAtTime(pts, 15), { time: 15, value: 3 });
  });

  it('buildHoverLegend formats venues + indicators', () => {
    const legend = buildHoverLegend({
      timeSec: 1_700_000_000,
      priceMode: 'percent',
      venues: [
        {
          venue: 'binance-spot',
          color: '#60a5fa',
          data: [{ time: 1_700_000_000, value: 0.125 }],
        },
        { venue: 'hidden', hidden: true, data: [{ time: 1_700_000_000, value: 9 }] },
      ],
      pulseHistory: [{ t: 1_700_000_000_000, score: 72.4 }],
      imbalanceHistory: [{ t: 1_700_000_000_000, imbalancePct: -12.3 }],
      cvdPoints: [{ time: 1_700_000_000, value: -45200 }],
      histogram: [{ sec: 1_700_000_000, buyUsd: 12000, sellUsd: 8300 }],
    });
    assert.equal(legend.venues.length, 1);
    assert.equal(legend.venues[0].text, '+0.125%');
    assert.equal(legend.indicators.find((i) => i.id === 'pulse').text, '72');
    assert.equal(legend.indicators.find((i) => i.id === 'imb').text, '-12.3%');
    assert.match(legend.indicators.find((i) => i.id === 'cvd').text, /\$/);
    assert.match(legend.timeLabel, /\d{2}:\d{2}:\d{2}/);
  });

  it('formatHoverTime handles invalid input', () => {
    assert.equal(formatHoverTime(null), '—');
    assert.equal(formatHoverTime(Number.NaN), '—');
  });
});
