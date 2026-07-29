/**
 * Unit tests for quality badge / live hysteresis.
 * Run: node --test ui/src/lib/*.test.js
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { QualityBadgeGate, LiveFlagGate, marketQuality } from './quality.js';

describe('marketQuality', () => {
  it('marks stale only after STALE_SEC window', () => {
    const now = Math.floor(Date.now() / 1000);
    const fresh = marketQuality({}, null, now - 1, true, false);
    assert.equal(fresh.badges.includes('stale'), false);
    const stale = marketQuality({}, null, now - 20, true, false);
    assert.equal(stale.badges.includes('stale'), true);
  });
});

describe('QualityBadgeGate', () => {
  it('debounces badge show/hide', () => {
    const g = new QualityBadgeGate({ showHoldMs: 100, clearHoldMs: 100 });
    const t0 = 1_000_000;
    assert.deepEqual(g.stabilize('a', ['stale'], t0), []);
    assert.deepEqual(g.stabilize('a', ['stale'], t0 + 50), []);
    assert.deepEqual(g.stabilize('a', ['stale'], t0 + 120), ['stale']);
    // Clear requires hold
    assert.deepEqual(g.stabilize('a', [], t0 + 130), ['stale']);
    assert.deepEqual(g.stabilize('a', [], t0 + 250), []);
  });
});

describe('LiveFlagGate', () => {
  it('debounces live false→true and true→false', () => {
    const g = new LiveFlagGate({ showHoldMs: 100, clearHoldMs: 100 });
    const t0 = 1_000_000;
    assert.equal(g.stabilize('v', true, t0), true);
    assert.equal(g.stabilize('v', false, t0 + 10), true);
    assert.equal(g.stabilize('v', false, t0 + 120), false);
    assert.equal(g.stabilize('v', true, t0 + 130), false);
    assert.equal(g.stabilize('v', true, t0 + 250), true);
  });

  it('cancels a stale flip candidate when the raw state recovers', () => {
    const g = new LiveFlagGate({ showHoldMs: 100, clearHoldMs: 100 });
    const t0 = 2_000_000;
    assert.equal(g.stabilize('v', true, t0), true);
    assert.equal(g.stabilize('v', false, t0 + 10), true);
    assert.equal(g.stabilize('v', true, t0 + 80), true);
    // A new false blip starts a fresh hold; it must not reuse t0+10.
    assert.equal(g.stabilize('v', false, t0 + 90), true);
    assert.equal(g.stabilize('v', false, t0 + 150), true);
    assert.equal(g.stabilize('v', false, t0 + 200), false);
  });
});
