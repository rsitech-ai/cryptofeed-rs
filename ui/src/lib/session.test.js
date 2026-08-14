import test from 'node:test';
import assert from 'node:assert/strict';
import { SESSION_PRESETS, sessionWindowSec } from './session.js';

test('session presets include 2h for soak-length chart windows', () => {
  assert.deepEqual(
    SESSION_PRESETS.map((s) => s.id),
    ['1m', '5m', '1h', '2h'],
  );
  assert.equal(sessionWindowSec('2h'), 7200);
  assert.equal(sessionWindowSec('5m'), 300);
  assert.equal(sessionWindowSec('nope'), 300);
});
