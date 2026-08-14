import test from 'node:test';
import assert from 'node:assert/strict';
import {
  ALERTS_MAX,
  CHART_DISPLAY_MAX_POINTS,
  DEFAULT_HISTORY_SECS,
  SeriesHistoryPolicy,
  TAPE_DOM_MAX,
  TAPE_OF_MAX,
  bpsMaxPoints,
  clampHistorySecs,
  compactDepthHistory,
  depthHistoryBudget,
  downsampleByAge,
  downsampleForChart,
  retentionCutoff,
  strideDownsample,
  tapeMaxEntries,
  trimTimeMap,
  venueBucketBudget,
  venueSampleBudget,
} from './history.js';

test('clampHistorySecs bounds to 300–7200 with default 7200', () => {
  assert.equal(clampHistorySecs(undefined), DEFAULT_HISTORY_SECS);
  assert.equal(DEFAULT_HISTORY_SECS, 7200);
  assert.equal(clampHistorySecs('bad'), DEFAULT_HISTORY_SECS);
  assert.equal(clampHistorySecs(60), 300);
  assert.equal(clampHistorySecs(3600), 3600);
  assert.equal(clampHistorySecs(99999), 7200);
});

test('retentionCutoff keeps historySecs behind tip', () => {
  assert.equal(retentionCutoff(10_000, 3600), 6400);
  assert.equal(retentionCutoff(0, 3600), 0);
});

test('trimTimeMap drops keys before cutoff', () => {
  const m = new Map([
    [100, 'a'],
    [200, 'b'],
    [300, 'c'],
  ]);
  assert.equal(trimTimeMap(m, 200), 1);
  assert.deepEqual([...m.keys()], [200, 300]);
});

test('downsampleByAge keeps dense recent and coarser older', () => {
  const tip = 10_000;
  /** @type {Array<{ time: number, v: number }>} */
  const pts = [];
  for (let t = tip - 3600; t <= tip; t += 1) {
    pts.push({ time: t, v: t });
  }
  const out = downsampleByAge(pts, {
    tipSec: tip,
    recentSec: 300,
    midSec: 1200,
    recentStep: 1,
    midStep: 5,
    oldStep: 15,
  });
  assert.ok(out.length < pts.length, `downsampled ${out.length} < ${pts.length}`);
  assert.equal(out[out.length - 1].time, tip);
  // Recent 300s @ 1s ≈ 301 points
  const recent = out.filter((p) => p.time >= tip - 300);
  assert.ok(recent.length >= 290 && recent.length <= 301, `recent ${recent.length}`);
  // Old region should be much sparser than 1s
  const old = out.filter((p) => p.time < tip - 1200);
  const oldSpan = tip - 1200 - (tip - 3600);
  assert.ok(old.length < oldSpan / 2, `old denser than expected: ${old.length}`);
});

test('downsampleByAge respects maxPoints hard cap', () => {
  const tip = 50_000;
  const pts = [];
  for (let t = tip - 3600; t <= tip; t += 1) pts.push({ time: t, v: t });
  const out = downsampleByAge(pts, {
    tipSec: tip,
    recentSec: 600,
    midSec: 1800,
    recentStep: 1,
    midStep: 2,
    oldStep: 3,
    maxPoints: 500,
  });
  assert.ok(out.length <= 500, `got ${out.length}`);
  assert.equal(out[out.length - 1].time, tip);
});

test('downsampleForChart caps 1h window to CHART_DISPLAY_MAX_POINTS', () => {
  const tip = 100_000;
  const pts = [];
  for (let t = tip - 3600; t <= tip; t += 1) pts.push({ time: t, value: t });
  const out = downsampleForChart(pts, 3600, CHART_DISPLAY_MAX_POINTS);
  assert.ok(out.length <= CHART_DISPLAY_MAX_POINTS, `${out.length} > ${CHART_DISPLAY_MAX_POINTS}`);
  assert.equal(out[out.length - 1].time, tip);
  assert.ok(out[0].time >= tip - 3600);
});

test('downsampleForChart keeps the session window 1s-dense when retention is longer', () => {
  const tip = 100_000;
  const pts = [];
  for (let t = tip - 2000; t <= tip; t += 1) pts.push({ time: t, value: t });
  const out = downsampleForChart(pts, 7200, CHART_DISPLAY_MAX_POINTS, 300);
  assert.ok(out.length <= CHART_DISPLAY_MAX_POINTS, `${out.length} > ${CHART_DISPLAY_MAX_POINTS}`);
  assert.equal(out[out.length - 1].time, tip);
  const recent = out.filter((p) => p.time >= tip - 300);
  assert.ok(recent.length >= 290 && recent.length <= 301, `recent ${recent.length}`);
  assert.ok(out[0].time < tip - 300, `older tail missing: start ${out[0].time}`);
});

test('strideDownsample preserves ends', () => {
  const pts = Array.from({ length: 100 }, (_, i) => ({ time: i }));
  const out = strideDownsample(pts, 10);
  assert.ok(out.length <= 11);
  assert.equal(out[0].time, 0);
  assert.equal(out[out.length - 1].time, 99);
});

test('depthHistoryBudget targets ~1h with tight column cap', () => {
  const b = depthHistoryBudget(3600);
  assert.equal(b.historySecs, 3600);
  assert.ok(b.maxCols >= 360 && b.maxCols <= 1800, `maxCols ${b.maxCols}`);
  assert.equal(b.recentStepMs, 250);
  assert.equal(b.oldStepMs, 5000);
});

test('compactDepthHistory trims to budget and preserves tip', () => {
  const tip = 1_000_000;
  /** @type {Array<{ t: number, mid: number }>} */
  const hist = [];
  // 1h of 100ms samples — far over budget
  for (let t = tip - 3_600_000; t <= tip; t += 100) {
    hist.push({ t, mid: 100 + (t % 7) });
  }
  const out = compactDepthHistory(hist, 3600, tip);
  const budget = depthHistoryBudget(3600);
  assert.ok(out.length <= budget.maxCols, `${out.length} > ${budget.maxCols}`);
  assert.equal(out[out.length - 1].t, tip);
  assert.ok(out[0].t >= tip - 3_600_000);
});

test('venueBucketBudget covers a 2h 1s window', () => {
  assert.ok(venueBucketBudget(3600, 1) >= 3720);
  assert.ok(venueBucketBudget(7200, 1) >= 7320);
  assert.ok(venueBucketBudget(7200, 1) < 8000);
});

test('tape/bps/sample budgets stay hard-capped for 24/7', () => {
  assert.ok(tapeMaxEntries(3600) <= TAPE_OF_MAX);
  assert.ok(tapeMaxEntries(7200) <= TAPE_OF_MAX);
  assert.ok(bpsMaxPoints(3600) <= CHART_DISPLAY_MAX_POINTS + 120);
  assert.ok(venueSampleBudget(3600) <= 8000);
  assert.ok(TAPE_DOM_MAX <= 200);
  assert.ok(ALERTS_MAX <= 40);
});

test('SeriesHistoryPolicy exposes tape/bps/depth knobs', () => {
  const p = new SeriesHistoryPolicy(3600);
  assert.equal(p.tapeKeepSec(), 3660);
  assert.equal(p.tapeMaxEntries(), tapeMaxEntries(3600));
  assert.equal(p.bpsMaxPoints(), bpsMaxPoints(3600));
  assert.equal(p.venueSampleBudget(), venueSampleBudget(3600));
  assert.equal(p.depthBudget().maxCols, depthHistoryBudget(3600).maxCols);
  assert.equal(p.setHistorySecs(120), 300);
});
