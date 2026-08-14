import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { LAYOUT_DEFAULTS, beginAxisDrag, clampLayoutNum, layoutToSettings, normalizeLayout } from './layout.js';

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

describe('layoutToSettings', () => {
  it('emits persistable layout* keys from the canonical defaults', () => {
    assert.deepEqual(layoutToSettings(), {
      layoutBookPx: 250,
      layoutRightPx: 310,
      layoutDockPx: 220,
      layoutMainFrac: 0.58,
      layoutBpsPx: 64,
      layoutCasPulse: 1,
      layoutCasImb: 1,
      layoutCasCvd: 1,
      layoutCasVol: 1.15,
    });
  });
});

describe('clampLayoutNum', () => {
  it('falls back when the value is not finite', () => {
    assert.equal(clampLayoutNum('x', 0, 10, 5), 5);
  });
});

function withFakeWindow(run) {
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
    return run(listeners);
  } finally {
    if (prev === undefined) delete globalThis.window;
    else globalThis.window = prev;
  }
}

describe('beginAxisDrag', () => {
  it('reports a clamped size as the pointer moves', () => {
    const changes = [];
    withFakeWindow((listeners) => {
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
    });
  });

  it('applies scale for fractional axes and commits once on pointercancel', () => {
    const changes = [];
    let ended = 0;
    withFakeWindow((listeners) => {
      beginAxisDrag(
        { preventDefault() {}, clientX: 0, clientY: 100 },
        {
          axis: 'y',
          startValue: 0.5,
          min: 0.28,
          max: 0.82,
          scale: 0.01,
          round: false,
          onChange: (n) => changes.push(n),
          onEnd: () => {
            ended += 1;
          },
        },
      );
      listeners.pointermove({ clientX: 0, clientY: 120 });
      assert.equal(changes.at(-1), 0.7);
      listeners.pointercancel({});
      assert.equal(ended, 1);
      listeners.pointerup?.({});
      assert.equal(ended, 1);
    });
  });
});
