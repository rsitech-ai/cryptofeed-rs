import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { normalizeOfLayers } from './settings.js';

describe('normalizeOfLayers', () => {
  it('migrates pre-structural-level settings exactly once', () => {
    assert.equal(
      normalizeOfLayers('heat,bubbles,mid', 1),
      'heat,bubbles,mid,levels',
    );
  });

  it('respects an intentional levels-off choice after migration', () => {
    assert.equal(normalizeOfLayers('heat,bubbles,mid', 2), 'heat,bubbles,mid');
  });
});
