import { it } from 'node:test';
import assert from 'node:assert/strict';
import { normalizeDerivatives } from './derivatives.js';

it('normalizes exchange-reported derivative state without inferred liquidations', () => {
  const state = normalizeDerivatives({ schema_version: 1, status: 'live', revision: 4,
    funding: { rate: '0.0001', stale: false },
    open_interest: { quantity: '100.0', change: '2.0', stale: false },
    funding_divergence: null,
    liquidations: [{ price: '100', quantity: '1', side: 'sell', event_ts_ns: 1 }] });
  assert.equal(state.funding.rate, '0.0001');
  assert.equal(state.openInterest.change, '2.0');
  assert.equal(state.liquidations.length, 1);
});
