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

it('fails closed with an explicit reason when the payload is unsupported', async () => {
  const { emptyDerivatives, derivativesFallbackTargets } = await import('./derivatives.js');
  const empty = emptyDerivatives('derivatives_loading');
  assert.equal(empty.available, false);
  assert.equal(empty.reason, 'derivatives_loading');
  assert.equal(empty.liquidations.length, 0);
  const targets = derivativesFallbackTargets('binance-spot', 'BTCUSDT', [
    { venue: 'binance-spot', symbol: 'BTCUSDT', kind: 'spot', live: true },
    { venue: 'binance-usdm', symbol: 'BTCUSDT', kind: 'perp', live: true },
    { venue: 'okx-swap', symbol: 'BTC-USDT-SWAP', kind: 'perp', live: true },
  ]);
  assert.deepEqual(targets.map((t) => t.venue), ['binance-spot', 'binance-usdm', 'okx-swap']);
});
