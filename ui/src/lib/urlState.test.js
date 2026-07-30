/**
 * URL dock-tab mapping for single-pane Flow & Pulse.
 * Run: node --test ui/src/lib/urlState.test.js
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { buildUrlState, parseUrlState } from './urlState.js';

describe('urlState Flow & Pulse dock tabs', () => {
  it('buildUrlState omits default open-pane tab', () => {
    const qs = buildUrlState({ analyticsTab: 'both', analyticsOpen: true, asset: 'BTC' });
    assert.equal(qs.includes('tab='), false);
  });

  it('buildUrlState emits tab=hidden when dock hidden', () => {
    const qs = buildUrlState({ analyticsTab: 'hidden', analyticsOpen: false, asset: 'BTC' });
    assert.match(qs, /tab=hidden/);
  });

  it('parseUrlState maps legacy flow|pulse|orderflow to open pane', () => {
    const prev = globalThis.window;
    for (const tab of ['flow', 'pulse', 'orderflow', 'both']) {
      globalThis.window = { location: { search: `?tab=${tab}` } };
      const p = parseUrlState();
      assert.equal(p.analyticsTab, 'both', tab);
    }
    globalThis.window = { location: { search: '?tab=hidden' } };
    assert.equal(parseUrlState().analyticsTab, 'hidden');
    if (prev === undefined) delete globalThis.window;
    else globalThis.window = prev;
  });

  it('round-trips historySecs in URL state', () => {
    const qs = buildUrlState({ asset: 'BTC', historySecs: 3600 });
    // default 3600 is omitted
    assert.equal(qs.includes('historySecs='), false);
    const qs2 = buildUrlState({ asset: 'BTC', historySecs: 1800 });
    assert.match(qs2, /historySecs=1800/);
    const prev = globalThis.window;
    globalThis.window = { location: { search: '?historySecs=1800' } };
    assert.equal(parseUrlState().historySecs, 1800);
    if (prev === undefined) delete globalThis.window;
    else globalThis.window = prev;
  });
});
