import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { stepHoldSeries, tapeTipSec } from './indicatorSeries.js';

describe('stepHoldSeries', () => {
  it('fills a 1s grid with last-known values', () => {
    const out = stepHoldSeries(
      [
        { t: 100, v: 10 },
        { t: 103, v: 30 },
      ],
      100,
      105,
    );
    assert.deepEqual(
      out.map((p) => [p.time, p.value]),
      [
        [100, 10],
        [101, 10],
        [102, 10],
        [103, 30],
        [104, 30],
        [105, 30],
      ],
    );
  });

  it('backfills the first sample so the full window is covered', () => {
    const out = stepHoldSeries([{ t: 50, v: 7 }], 48, 52);
    assert.deepEqual(
      out.map((p) => [p.time, p.value]),
      [
        [48, 7],
        [49, 7],
        [50, 7],
        [51, 7],
        [52, 7],
      ],
    );
  });

  it('returns empty for invalid windows', () => {
    assert.deepEqual(stepHoldSeries([{ t: 1, v: 1 }], 5, 4), []);
  });
});

describe('tapeTipSec', () => {
  it('returns the newest exchange second', () => {
    assert.equal(
      tapeTipSec([
        { exchange_ts_ns: 1_000_000_000 },
        { exchange_ts_ns: 5_000_000_000 },
        { receive_ts_ns: 4_000_000_000 },
      ]),
      5,
    );
  });

  it('returns null for empty tape', () => {
    assert.equal(tapeTipSec([]), null);
    assert.equal(tapeTipSec(null), null);
  });
});
