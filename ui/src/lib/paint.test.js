/**
 * Unit tests for paint gate / EMA helpers.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createPaintGate, ema } from './paint.js';

describe('ema', () => {
  it('bootstraps from null', () => {
    assert.equal(ema(null, 100), 100);
  });

  it('moves toward target', () => {
    const next = ema(100, 200, 0.5);
    assert.equal(next, 150);
  });
});

describe('createPaintGate', () => {
  it('coalesces multiple schedules into one flush', async () => {
    let n = 0;
    const gate = createPaintGate(() => {
      n += 1;
    }, { minIntervalMs: 40 });
    gate.schedule();
    gate.schedule();
    gate.schedule();
    await new Promise((r) => setTimeout(r, 80));
    assert.equal(n, 1);
    gate.dispose();
  });

  it('flushNow runs immediately', () => {
    let n = 0;
    const gate = createPaintGate(() => {
      n += 1;
    }, { minIntervalMs: 5000 });
    gate.flushNow();
    assert.equal(n, 1);
    gate.dispose();
  });
});
