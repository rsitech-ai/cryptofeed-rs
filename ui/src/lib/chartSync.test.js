import { describe, it, mock } from 'node:test';
import assert from 'node:assert/strict';
import { createRangeActivity, wireVisibleLogicalRangeSync } from './chartSync.js';

function fakeChart() {
  let handler = null;
  const scale = {
    setVisibleLogicalRange: mock.fn(),
    subscribeVisibleLogicalRangeChange: mock.fn((next) => {
      handler = next;
    }),
    unsubscribeVisibleLogicalRangeChange: mock.fn((current) => {
      if (handler === current) handler = null;
    }),
  };

  return {
    chart: { timeScale: () => scale },
    scale,
    emit: (range) => handler?.(range),
  };
}

describe('wireVisibleLogicalRangeSync', () => {
  it('mirrors logical ranges and stops touching the target after disposal', () => {
    const source = fakeChart();
    const target = fakeChart();
    const dispose = wireVisibleLogicalRangeSync(source.chart, target.chart, { active: false });

    source.emit({ from: 1, to: 5 });
    assert.deepEqual(target.scale.setVisibleLogicalRange.mock.calls[0].arguments, [{ from: 1, to: 5 }]);

    dispose();
    source.emit({ from: 6, to: 9 });

    assert.equal(source.scale.unsubscribeVisibleLogicalRangeChange.mock.callCount(), 1);
    assert.equal(target.scale.setVisibleLogicalRange.mock.callCount(), 1);
  });

  it('ignores null ranges and re-entrant updates', () => {
    const source = fakeChart();
    const target = fakeChart();
    const guard = { active: false };
    wireVisibleLogicalRangeSync(source.chart, target.chart, guard);

    source.emit(null);
    guard.active = true;
    source.emit({ from: 1, to: 2 });

    assert.equal(target.scale.setVisibleLogicalRange.mock.callCount(), 0);
  });
});

describe('createRangeActivity', () => {
  it('distinguishes user movement from synchronization and programmatic movement', () => {
    const activity = createRangeActivity();
    assert.equal(activity.isUserDriven(), true);

    activity.syncGuard.active = true;
    assert.equal(activity.isUserDriven(), false);
    activity.syncGuard.active = false;

    activity.runProgrammatic(() => {
      assert.equal(activity.isUserDriven(), false);
    });
    assert.equal(activity.isUserDriven(), true);
  });

  it('restores user detection when a programmatic operation throws', () => {
    const activity = createRangeActivity();
    assert.throws(() => activity.runProgrammatic(() => {
      throw new Error('expected');
    }));
    assert.equal(activity.isUserDriven(), true);
  });
});
