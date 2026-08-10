import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { emptyStructuralLevelSnapshot, normalizeStructuralLevelSnapshot } from './structuralLevels.js';

describe('normalizeStructuralLevelSnapshot', () => {
  it('keeps only exact supported server levels', () => {
    const result = normalizeStructuralLevelSnapshot({
      schema_version: 1,
      status: 'live',
      revision: 7,
      levels: [
        {
          id: 1,
          kind: 'naked',
          state: 'active',
          source_bubble_id: 9,
          direction: 'buy',
          tier: 'f3',
          price: '100.25',
          strength: '42.5',
          created_at_ns: 2_000_000,
        },
        { id: 2, kind: 'invented', state: 'active', price: '101', strength: '1' },
        { id: 3, kind: 'top_day', state: 'active', price: 'nan', strength: '1' },
      ],
    });
    assert.equal(result.available, true);
    assert.equal(result.levels.length, 1);
    assert.deepEqual(result.levels[0], {
      id: 1,
      kind: 'naked',
      state: 'active',
      sourceBubbleId: 9,
      direction: 'buy',
      tier: 'f3',
      price: 100.25,
      strength: 42.5,
      exactPrice: '100.25',
      exactStrength: '42.5',
      createdAt: 2,
      touchedAt: null,
      windowStart: null,
      expiresAt: null,
      server: true,
    });
  });

  it('fails closed for an invalid payload', () => {
    assert.deepEqual(
      normalizeStructuralLevelSnapshot({ schema_version: 2 }),
      emptyStructuralLevelSnapshot('structural_level_payload_invalid'),
    );
  });
});
