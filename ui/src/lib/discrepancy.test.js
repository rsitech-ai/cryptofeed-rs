import test from 'node:test';
import assert from 'node:assert/strict';
import { DiscrepancyTracker } from './discrepancy.js';

test('bps history uses series data time, not wall clock', () => {
  const t = new DiscrepancyTracker();
  const series = [
    {
      venue: 'a',
      last: 100,
      hidden: false,
      data: [
        { time: 1000, value: 0 },
        { time: 1050, value: 0.1 },
      ],
    },
    {
      venue: 'b',
      last: 101,
      hidden: false,
      data: [
        { time: 1000, value: 0 },
        { time: 1048, value: 0.2 },
      ],
    },
  ];
  t.push({ bps: 12, max: 101, min: 100 }, series);
  const pts = t.points();
  assert.equal(pts.length, 1);
  assert.equal(pts[0].time, 1050, 'should use max series last time');
  assert.equal(pts[0].bps, 12);
});

test('dataTimeSec prefers lastTime field', () => {
  assert.equal(
    DiscrepancyTracker.dataTimeSec([{ lastTime: 42, data: [{ time: 1, value: 0 }] }]),
    42,
  );
  assert.equal(DiscrepancyTracker.dataTimeSec([{ data: [] }, { data: null }]), null);
});

test('explicit nowSec still wins', () => {
  const t = new DiscrepancyTracker();
  t.push(
    { bps: 5, max: 1, min: 1 },
    [{ venue: 'a', last: 1, data: [{ time: 999, value: 0 }] }],
    777,
  );
  assert.equal(t.points()[0].time, 777);
});
