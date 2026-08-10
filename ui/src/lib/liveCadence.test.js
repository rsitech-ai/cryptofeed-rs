import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { LiveCadence } from './liveCadence.js';

describe('LiveCadence', () => {
  it('applies immediately then bounds each independent render surface', () => {
    const cadence = new LiveCadence({ status: 1000, analytics: 500, series: 250 });
    assert.equal(cadence.allow('status', 100), true);
    assert.equal(cadence.allow('status', 999), false);
    assert.equal(cadence.allow('status', 1100), true);
    assert.equal(cadence.allow('analytics', 100), true);
    assert.equal(cadence.allow('analytics', 599), false);
    assert.equal(cadence.allow('analytics', 600), true);
    assert.equal(cadence.allow('series', 100), true);
    assert.equal(cadence.allow('series', 350), true);
  });

  it('resets on focus changes so the new market renders immediately', () => {
    const cadence = new LiveCadence({ analytics: 500 });
    cadence.allow('analytics', 100);
    assert.equal(cadence.allow('analytics', 200), false);
    cadence.reset();
    assert.equal(cadence.allow('analytics', 200), true);
  });
});
