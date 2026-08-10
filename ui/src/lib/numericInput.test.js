import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { normalizeNumericDraft, numericCommit } from './numericInput.js';

class FakeInput {
  constructor() {
    this.value = '';
    this.listeners = new Map();
    this.blurred = false;
  }

  addEventListener(name, handler) {
    this.listeners.set(name, handler);
  }

  removeEventListener(name) {
    this.listeners.delete(name);
  }

  dispatch(name, event = {}) {
    this.listeners.get(name)?.({ currentTarget: this, key: event.key, preventDefault() {} });
  }

  blur() {
    this.blurred = true;
    this.dispatch('blur');
  }
}

describe('normalizeNumericDraft', () => {
  it('clamps finite numeric drafts and rejects empty or non-numeric input', () => {
    assert.equal(normalizeNumericDraft('17', { min: 5, max: 50, integer: true }), 17);
    assert.equal(normalizeNumericDraft('99', { min: 5, max: 50, integer: true }), 50);
    assert.equal(normalizeNumericDraft('14.6', { min: 5, max: 50, integer: true }), 15);
    assert.equal(normalizeNumericDraft('', { min: 0, max: 10 }), null);
    assert.equal(normalizeNumericDraft('NaN', { min: 0, max: 10 }), null);
  });
});

describe('numericCommit', () => {
  it('preserves a focused draft across live prop updates and commits it on blur', () => {
    const input = new FakeInput();
    const commits = [];
    const action = numericCommit(input, {
      value: 16,
      min: 5,
      max: 50,
      integer: true,
      onCommit: (value) => commits.push(value),
    });

    assert.equal(input.value, '16');
    input.dispatch('focus');
    input.value = '17';
    action.update({ value: 32, min: 5, max: 50, integer: true, onCommit: (value) => commits.push(value) });
    assert.equal(input.value, '17');

    input.dispatch('blur');
    assert.equal(input.value, '17');
    assert.deepEqual(commits, [17]);
  });

  it('restores the latest proven value when the draft is invalid', () => {
    const input = new FakeInput();
    const commits = [];
    numericCommit(input, {
      value: 72,
      min: 10,
      max: 100,
      onCommit: (value) => commits.push(value),
    });

    input.dispatch('focus');
    input.value = '';
    input.dispatch('blur');

    assert.equal(input.value, '72');
    assert.deepEqual(commits, []);
  });

  it('commits with Enter and does not emit the same value again on blur', () => {
    const input = new FakeInput();
    const commits = [];
    numericCommit(input, {
      value: 0,
      min: 0,
      max: 1_000_000_000,
      onCommit: (value) => commits.push(value),
    });

    input.dispatch('focus');
    input.value = '25000';
    input.dispatch('keydown', { key: 'Enter' });

    assert.equal(input.blurred, true);
    assert.deepEqual(commits, [25000]);
  });
});
