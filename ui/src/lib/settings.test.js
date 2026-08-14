import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { normalizeOfLayers, safeHttpUrl } from './settings.js';

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

describe('safeHttpUrl', () => {
  it('allows http(s) and rejects javascript/data/relative values', () => {
    assert.equal(safeHttpUrl('http://127.0.0.1:3000/d/x'), 'http://127.0.0.1:3000/d/x');
    assert.equal(safeHttpUrl('https://grafana.example/d/x'), 'https://grafana.example/d/x');
    assert.equal(safeHttpUrl('javascript:alert(1)'), '');
    assert.equal(safeHttpUrl('data:text/html,hi'), '');
    assert.equal(safeHttpUrl('/relative'), '');
    assert.equal(safeHttpUrl(''), '');
    assert.equal(safeHttpUrl('javascript:alert(1)', 'http://127.0.0.1:3000/'), 'http://127.0.0.1:3000/');
  });
});
