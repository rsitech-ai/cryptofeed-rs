import { it } from 'node:test';
import assert from 'node:assert/strict';
import { normalizeDepthHistory } from './serverDepthHistory.js';

it('maps exact server depth samples to bounded heatmap MBP maps', () => {
  const rows = normalizeDepthHistory({ schema_version: 1, samples: [{ event_ts_ns: 1_000_000, epoch: 2,
    bids: [{ price: '100.00', quantity: '2.0' }], asks: [{ price: '101.00', quantity: '1.0' }] }] });
  assert.equal(rows[0].bids.get(100), 200);
  assert.equal(rows[0].mid, 100.5);
  assert.equal(rows[0].epoch, 2);
});
