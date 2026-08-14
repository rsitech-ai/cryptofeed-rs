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
    const qs = buildUrlState({ asset: 'BTC', historySecs: 7200 });
    // default 7200 is omitted
    assert.equal(qs.includes('historySecs='), false);
    const qsHour = buildUrlState({ asset: 'BTC', historySecs: 3600 });
    assert.match(qsHour, /historySecs=3600/);
    const qs2 = buildUrlState({ asset: 'BTC', historySecs: 1800 });
    assert.match(qs2, /historySecs=1800/);
    const prev = globalThis.window;
    globalThis.window = { location: { search: '?historySecs=1800' } };
    assert.equal(parseUrlState().historySecs, 1800);
    if (prev === undefined) delete globalThis.window;
    else globalThis.window = prev;
  });

  it('round-trips Market Profile basis and bubble mode', () => {
    const qs = buildUrlState({ asset: 'BTC', profileBasis: 'tpo', ofBubbleMode: 'delta' });
    assert.match(qs, /profile=tpo/);
    assert.match(qs, /ofBubbleMode=delta/);
    const prev = globalThis.window;
    globalThis.window = { location: { search: '?profile=tpo&ofBubbleMode=delta' } };
    const parsed = parseUrlState();
    assert.equal(parsed.profileBasis, 'tpo');
    assert.equal(parsed.ofBubbleMode, 'delta');
    if (prev === undefined) delete globalThis.window;
    else globalThis.window = prev;
  });

  it('drops javascript grafana URLs from shareable state', () => {
    const qs = buildUrlState({ asset: 'BTC', grafanaUrl: 'javascript:alert(1)' });
    assert.equal(qs.includes('grafana='), false);
    const prev = globalThis.window;
    globalThis.window = { location: { search: '?grafana=javascript:alert(1)' } };
    assert.equal(parseUrlState().grafanaUrl, undefined);
    globalThis.window = { location: { search: '?grafana=http://127.0.0.1:3000/d/x' } };
    assert.equal(parseUrlState().grafanaUrl, 'http://127.0.0.1:3000/d/x');
    if (prev === undefined) delete globalThis.window;
    else globalThis.window = prev;
  });
});
