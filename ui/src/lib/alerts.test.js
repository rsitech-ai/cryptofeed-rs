import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  ALERT_AUTO_DISMISS_MS,
  ALERT_VISIBLE_MAX,
  createAlert,
  daemonAlertPayload,
} from './alerts.js';

describe('daemonAlertPayload', () => {
  it('maps cross-venue discrepancy and lag alerts to the daemon contract', () => {
    assert.deepEqual(
      daemonAlertPayload({ type: 'bps', bps: 4.2, threshold: 3 }),
      { kind: 'discrepancy', bps: 4.2, message: 'Cross-venue discrepancy above 3 bps' },
    );
    assert.deepEqual(
      daemonAlertPayload({ type: 'lag', venue: 'okx', lagMs: 2500 }),
      { kind: 'lag', message: 'okx feed lag 2500ms' },
    );
  });

  it('does not claim daemon delivery for unsupported pulse alerts', () => {
    assert.equal(daemonAlertPayload({ type: 'pulse', score: 90 }), null);
  });
});

describe('alert toast UX knobs', () => {
  it('auto-dismisses after ~5s and caps visible stack to 3', () => {
    assert.equal(ALERT_AUTO_DISMISS_MS, 5000);
    assert.equal(ALERT_VISIBLE_MAX, 3);
  });

  it('createAlert stamps a creation time for auto-dismiss', () => {
    const before = Date.now();
    const a = createAlert('bps', 'Cross-venue Δ 12 bps', 'High: a · Low: b');
    const after = Date.now();
    assert.ok(a.ts >= before && a.ts <= after);
    assert.equal(a.kind, 'bps');
    assert.ok(a.id);
  });
});
