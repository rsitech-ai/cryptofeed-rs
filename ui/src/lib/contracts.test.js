import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  Book404Gate,
  isCurrentMarket,
  normalizeReplayEntries,
  shouldPollVenueBook,
} from './contracts.js';

describe('isCurrentMarket', () => {
  it('rejects a response after venue or symbol focus changes', () => {
    assert.equal(isCurrentMarket('okx', 'BTC-USDT', 'okx', 'BTC-USDT'), true);
    assert.equal(isCurrentMarket('okx', 'BTC-USDT', 'kraken', 'BTC/USD'), false);
    assert.equal(isCurrentMarket('okx', 'BTC-USDT', 'okx', 'ETH-USDT'), false);
  });
});

describe('Book404Gate', () => {
  it('scopes suppression by venue and symbol and expires it', () => {
    const gate = new Book404Gate(1_000);
    gate.suppress('okx', 'BTC-USDT', 10_000);
    assert.equal(gate.isSuppressed('okx', 'BTC-USDT', 10_500), true);
    assert.equal(gate.isSuppressed('okx', 'ETH-USDT', 10_500), false);
    assert.equal(gate.isSuppressed('kraken', 'BTC-USDT', 10_500), false);
    assert.equal(gate.isSuppressed('okx', 'BTC-USDT', 11_001), false);
  });

  it('clears a temporary suppression after a successful book response', () => {
    const gate = new Book404Gate(10_000);
    gate.suppress('okx', 'BTC-USDT', 1_000);
    gate.clear('okx', 'BTC-USDT');
    assert.equal(gate.isSuppressed('okx', 'BTC-USDT', 1_001), false);
  });
});

describe('shouldPollVenueBook', () => {
  it('skips quotes-only venues once status reports zero books', () => {
    assert.equal(shouldPollVenueBook({ validBooks: 0, isFocus: false }), false);
    assert.equal(shouldPollVenueBook({ validBooks: 0, isFocus: true }), false);
    assert.equal(shouldPollVenueBook({ validBooks: 4, isFocus: false }), true);
  });

  it('still polls unknown status and known books', () => {
    assert.equal(shouldPollVenueBook({ validBooks: null, isFocus: false }), true);
    assert.equal(shouldPollVenueBook({ validBooks: 0, knownBook: true }), true);
    assert.equal(shouldPollVenueBook({ validBooks: 4, suppressed: true }), false);
  });
});

describe('normalizeReplayEntries', () => {
  it('accepts normalized tape entries and unwraps MFNE JSON envelopes', () => {
    const entries = normalizeReplayEntries([
      { kind: 'trade', venue: 'okx', price: '100', quantity: '2', receive_ts_ns: 10 },
      {
        venue: 'binance-spot',
        receive_ts: { ns: 20 },
        exchange_ts: { ns: 19 },
        payload: {
          trade: {
            price: '101',
            quantity: '3',
            aggressor: 'buy',
            trade_id: 't-1',
          },
        },
      },
      {
        venue: 'kraken',
        receive_ts: { ns: 30 },
        payload: {
          quote: {
            bid_price: '99',
            bid_quantity: '1',
            ask_price: '100',
            ask_quantity: '2',
          },
        },
      },
    ]);

    assert.equal(entries.length, 3);
    assert.deepEqual(entries[1], {
      kind: 'trade',
      venue: 'binance-spot',
      symbol: undefined,
      price: '101',
      quantity: '3',
      aggressor: 'buy',
      trade_id: 't-1',
      exchange_ts_ns: 19,
      receive_ts_ns: 20,
    });
    assert.equal(entries[2].kind, 'quote');
    assert.equal(entries[2].bid_price, '99');
  });

  it('drops non-market envelopes instead of sending malformed rows downstream', () => {
    assert.deepEqual(normalizeReplayEntries([{ payload: { heartbeat: {} } }, null]), []);
  });
});
