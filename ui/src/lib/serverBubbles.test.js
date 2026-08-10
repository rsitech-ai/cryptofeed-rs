import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { normalizeBubbleSnapshot } from './serverBubbles.js';

describe('normalizeBubbleSnapshot', () => {
  it('keeps Rust tier, shape, phase, and exact labels', () => {
    const result = normalizeBubbleSnapshot({
      schema_version: 1, status: 'live', mode: 'delta', revision: 3,
      bubbles: [{ id: 7, candle_end_ns: 2_000_000_000, anchor_price: '100.25',
        strength: '2.500', delta: '-2.500', total_volume: '3.000', tier: 'f3',
        shape: 'diamond', direction: 'sell', visual_size: 42, phase: 'final' }],
    });
    assert.equal(result.available, true);
    assert.equal(result.bubbles[0].tier, 'f3');
    assert.equal(result.bubbles[0].shape, 'diamond');
    assert.equal(result.bubbles[0].phase, 'final');
    assert.equal(result.bubbles[0].exactDelta, '-2.500');
  });
});
