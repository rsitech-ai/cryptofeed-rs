import assert from 'node:assert/strict';
import test from 'node:test';

import { emptyDomSnapshot, normalizeDomSnapshot } from './serverDom.js';

test('normalizes exact server DOM columns without recomputing them', () => {
  const snapshot = normalizeDomSnapshot({
    schema_version: 1,
    status: 'live',
    revision: 7,
    rows: [{
      price: '100.00',
      bid_quantity: '3.000',
      ask_quantity: '0.000',
      bid_cumulative_notional: '300.00000',
      ask_cumulative_notional: '0',
      imbalance_bps: 10000,
      mbp_delta_quantity: '1.000',
      mbp_delta_notional: '100.00000',
      buy_executed_notional: '25.00000',
      sell_executed_notional: '0',
      unknown_executed_notional: '0',
      total_executed_notional: '25.00000',
      executed_delta_notional: '25.00000',
    }],
  });

  assert.equal(snapshot.available, true);
  assert.equal(snapshot.rows[0].priceExact, '100.00');
  assert.equal(snapshot.rows[0].mbpDeltaExact, '100.00000');
  assert.equal(snapshot.rows[0].buyExecutedExact, '25.00000');
});

test('fails closed when the DOM contract is unavailable', () => {
  assert.deepEqual(emptyDomSnapshot('offline'), {
    available: false,
    status: 'unavailable',
    reason: 'offline',
    revision: 0,
    rows: [],
  });
});
