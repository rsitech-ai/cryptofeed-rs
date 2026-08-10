/**
 * Unit tests for SSE dispatch (node:test).
 * Run: node --test ui/src/lib/*.test.js
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { dispatchStreamMessage, StreamClient } from './stream.js';

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
          profile: { schema_version: 1, status: 'live', revision: 1 },
        },
      },
      {
        onStatus: (s) => calls.push(['status', s]),
        onFocus: (f) => calls.push(['focus', f.venue, f.symbol, !!f.book, f.tape.length, f.profile?.revision]),
        onBook: (v, s, b) => calls.push(['book', v, s, b.bids?.length]),
        onTape: (v, s, e) => calls.push(['tape', v, s, e.length]),
      },
    );
    assert.deepEqual(calls[0], ['status', { live: true }]);
    assert.ok(calls.some((c) => c[0] === 'focus'));
    assert.ok(calls.some((c) => c[0] === 'focus' && c[5] === 1));
    // Combined focus must not also fire onBook/onTape (double-apply → flicker).
    assert.equal(calls.filter((c) => c[0] === 'book').length, 0);
    assert.equal(calls.filter((c) => c[0] === 'tape').length, 0);
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

describe('StreamClient disconnect silent', () => {
  it('silent disconnect does not fire onDisconnect', () => {
    let disconnects = 0;
    const s = new StreamClient({
      onDisconnect: () => {
        disconnects += 1;
      },
    });
    s.connected = true;
    s.disconnect({ silent: true });
    assert.equal(disconnects, 0);
    assert.equal(s.connected, false);
    s.connected = true;
    s.disconnect();
    assert.equal(disconnects, 1);
  });
});

describe('StreamClient reconnect state', () => {
  it('notifies onConnect again after EventSource reconnects so sticky UI state clears', () => {
    const OriginalEventSource = globalThis.EventSource;
    class FakeEventSource {
      static CONNECTING = 0;
      static OPEN = 1;
      static CLOSED = 2;
      constructor() {
        this.readyState = FakeEventSource.CONNECTING;
      }
      addEventListener() {}
      close() {
        this.readyState = FakeEventSource.CLOSED;
      }
    }
    globalThis.EventSource = FakeEventSource;
    try {
      let connects = 0;
      let reconnecting = 0;
      const s = new StreamClient({
        onConnect: () => { connects += 1; },
        onReconnecting: () => { reconnecting += 1; },
      });
      assert.equal(s.connect({ venue: 'okx', symbol: 'BTC-USDT' }), true);
      s.es.readyState = FakeEventSource.OPEN;
      s.es.onopen();
      s.es.readyState = FakeEventSource.CONNECTING;
      s.es.onerror();
      assert.equal(s.connected, true);
      assert.equal(reconnecting, 1);
      s.es.readyState = FakeEventSource.OPEN;
      s.es.onopen();
      assert.equal(connects, 2);
      assert.equal(s.reconnectCount, 1);
    } finally {
      globalThis.EventSource = OriginalEventSource;
    }
  });
});
