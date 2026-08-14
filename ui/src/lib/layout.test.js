import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { LAYOUT_DEFAULTS, beginAxisDrag, clampLayoutNum, normalizeLayout } from './layout.js';

describe('normalizeLayout', () => {
  it('applies defaults and clamps out-of-range values', () => {
    assert.deepEqual(normalizeLayout({}), LAYOUT_DEFAULTS);
    const next = normalizeLayout({
      layoutBookPx: 50,
      layoutRightPx: 900,
      layoutDockPx: 200,
      layoutMainFrac: 2,
      layoutBpsPx: 12,
    });
    assert.equal(next.bookPx, 180);
    assert.equal(next.rightPx, 480);
    assert.equal(next.dockPx, 200);
    assert.equal(next.mainFrac, 0.82);
    assert.equal(next.bpsPx, 40);
  });
});

describe('clampLayoutNum', () => {
  it('falls back when the value is not finite', () => {
    assert.equal(clampLayoutNum('x', 0, 10, 5), 5);
  });
});

describe('beginAxisDrag', () => {
  it('reports a clamped size as the pointer moves', () => {
    const changes = [];
    /** @type {Record<string, Function>} */
    const listeners = {};
    const prev = globalThis.window;
    globalThis.window = {
      addEventListener: (type, fn) => {
        listeners[type] = fn;
      },
      removeEventListener: (type) => {
        delete listeners[type];
      },
    };
    try {
      beginAxisDrag(
        { preventDefault() {}, clientX: 10, clientY: 0 },
        {
          axis: 'x',
          startValue: 250,
          min: 180,
          max: 420,
          onChange: (n) => changes.push(n),
        },
      );
      listeners.pointermove({ clientX: 80, clientY: 0 });
      assert.equal(changes.at(-1), 320);
      listeners.pointermove({ clientX: 900, clientY: 0 });
      assert.equal(changes.at(-1), 420);
    } finally {
      if (prev === undefined) delete globalThis.window;
      else globalThis.window = prev;
    }
  });
});
