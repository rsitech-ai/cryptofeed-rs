/**
 * Unit tests for SSE dispatch (node:test).
 * Run: node --test ui/src/lib/*.test.js
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { dispatchStreamMessage } from './stream.js';

describe('dispatchStreamMessage', () => {
  it('applies combined focus payload from daemon SSE', () => {
    /** @type {object[]} */
    const calls = [];
    dispatchStreamMessage(
      {
        ts_ns: 1,
        status: { live: true },
        focus: {
          venue: 'binance-spot',
          symbol: 'BTCUSDT',
          book: { venue: 'binance-spot', bids: [{ price: '1', quantity: '2' }], asks: [] },
          tape: [{ kind: 'trade', price: '1', quantity: '0.1' }],
        },
      },
      {
        onStatus: (s) => calls.push(['status', s]),
        onFocus: (f) => calls.push(['focus', f.venue, f.symbol, !!f.book, f.tape.length]),
        onBook: (v, s, b) => calls.push(['book', v, s, b.bids?.length]),
        onTape: (v, s, e) => calls.push(['tape', v, s, e.length]),
      },
    );
    assert.deepEqual(calls[0], ['status', { live: true }]);
    assert.ok(calls.some((c) => c[0] === 'focus'));
    assert.ok(calls.some((c) => c[0] === 'book' && c[1] === 'binance-spot'));
    assert.ok(calls.some((c) => c[0] === 'tape' && c[3] === 1));
  });

  it('still handles typed messages', () => {
    let book = null;
    dispatchStreamMessage(
      { type: 'book', venue: 'okx', symbol: 'BTC-USDT', book: { bids: [], asks: [] } },
      { onBook: (v, s, b) => { book = { v, s, b }; } },
    );
    assert.equal(book?.v, 'okx');
    assert.ok(book?.b);
  });
});
