import test from 'node:test';
import assert from 'node:assert/strict';
import {
  DEFAULT_HISTORY_SECS,
  SeriesHistoryPolicy,
  bpsMaxPoints,
  clampHistorySecs,
  compactDepthHistory,
  depthHistoryBudget,
  downsampleByAge,
  retentionCutoff,
  tapeMaxEntries,
  trimTimeMap,
  venueSampleBudget,
} from './history.js';

test('clampHistorySecs bounds to 300–7200 with default 3600', () => {
  assert.equal(clampHistorySecs(undefined), DEFAULT_HISTORY_SECS);
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

test('depthHistoryBudget targets ~1h with column downsample', () => {
  const b = depthHistoryBudget(3600);
  assert.equal(b.historySecs, 3600);
  assert.ok(b.maxCols >= 2000 && b.maxCols <= 4200, `maxCols ${b.maxCols}`);
  assert.equal(b.recentStepMs, 200);
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

test('SeriesHistoryPolicy exposes tape/bps/depth knobs', () => {
  const p = new SeriesHistoryPolicy(3600);
  assert.equal(p.tapeKeepSec(), 3660);
  assert.equal(p.tapeMaxEntries(), tapeMaxEntries(3600));
  assert.equal(p.bpsMaxPoints(), bpsMaxPoints(3600));
  assert.equal(p.venueSampleBudget(), venueSampleBudget(3600));
  assert.equal(p.depthBudget().maxCols, depthHistoryBudget(3600).maxCols);
  assert.equal(p.setHistorySecs(120), 300);
});
