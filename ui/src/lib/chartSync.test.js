import { describe, it, mock } from 'node:test';
import assert from 'node:assert/strict';
import {
  applyVisibleTimeRange,
  createRangeActivity,
  setVisibleTimeRangeSafe,
  visibleTimeRangesNearlyEqual,
  wireChartTimeScales,
  wireVisibleLogicalRangeSync,
  wireVisibleTimeRangeSync,
} from './chartSync.js';

function fakeChart(kind = 'logical', initialVisible = null) {
  /** @type {Array<(range: any) => void>} */
  let handlers = [];
  let visible = initialVisible;
  const scale = {
    setVisibleLogicalRange: mock.fn((range) => {
      visible = range;
    }),
    setVisibleRange: mock.fn((range) => {
      visible = range;
    }),
    getVisibleRange: mock.fn(() => visible),
    subscribeVisibleLogicalRangeChange: mock.fn((next) => {
      if (kind === 'logical') handlers.push(next);
    }),
    unsubscribeVisibleLogicalRangeChange: mock.fn((current) => {
      handlers = handlers.filter((h) => h !== current);
    }),
    subscribeVisibleTimeRangeChange: mock.fn((next) => {
      if (kind === 'time') handlers.push(next);
    }),
    unsubscribeVisibleTimeRangeChange: mock.fn((current) => {
      handlers = handlers.filter((h) => h !== current);
    }),
  };

  return {
    chart: { timeScale: () => scale },
    scale,
    emit: (range) => {
      for (const h of handlers.slice()) h(range);
    },
  };
}

describe('visibleTimeRangesNearlyEqual', () => {
  it('tolerates small float noise and rejects real moves', () => {
    assert.equal(
      visibleTimeRangesNearlyEqual({ from: 100, to: 200 }, { from: 100.01, to: 200.02 }, 0.05),
      true,
    );
    assert.equal(
      visibleTimeRangesNearlyEqual({ from: 100, to: 200 }, { from: 101, to: 201 }, 0.05),
      false,
    );
  });
});

describe('setVisibleTimeRangeSafe', () => {
  it('skips setVisibleRange when the scale already shows the window', () => {
    const target = fakeChart('time', { from: 10, to: 40 });
    assert.equal(setVisibleTimeRangeSafe(target.scale, 10.01, 40.02), true);
    assert.equal(target.scale.setVisibleRange.mock.callCount(), 0);
  });
});

describe('wireVisibleLogicalRangeSync', () => {
  it('mirrors logical ranges and stops touching the target after disposal', () => {
    const source = fakeChart('logical');
    const target = fakeChart('logical');
    const dispose = wireVisibleLogicalRangeSync(source.chart, target.chart, { active: false });

    source.emit({ from: 1, to: 5 });
    assert.deepEqual(target.scale.setVisibleLogicalRange.mock.calls[0].arguments, [{ from: 1, to: 5 }]);

    dispose();
    source.emit({ from: 6, to: 9 });

    assert.equal(source.scale.unsubscribeVisibleLogicalRangeChange.mock.callCount(), 1);
    assert.equal(target.scale.setVisibleLogicalRange.mock.callCount(), 1);
  });

  it('ignores null ranges and re-entrant updates', () => {
    const source = fakeChart('logical');
    const target = fakeChart('logical');
    const guard = { active: false };
    wireVisibleLogicalRangeSync(source.chart, target.chart, guard);

    source.emit(null);
    guard.active = true;
    source.emit({ from: 1, to: 2 });

    assert.equal(target.scale.setVisibleLogicalRange.mock.callCount(), 0);
  });
});

describe('wireVisibleTimeRangeSync', () => {
  it('mirrors wall-clock ranges via setVisibleRange', () => {
    const source = fakeChart('time');
    const target = fakeChart('time');
    const dispose = wireVisibleTimeRangeSync(source.chart, target.chart, { active: false });

    source.emit({ from: 100, to: 200 });
    assert.deepEqual(target.scale.setVisibleRange.mock.calls[0].arguments, [{ from: 100, to: 200 }]);

    dispose();
    source.emit({ from: 300, to: 400 });
    assert.equal(source.scale.unsubscribeVisibleTimeRangeChange.mock.callCount(), 1);
    assert.equal(target.scale.setVisibleRange.mock.callCount(), 1);
  });

  it('suppresses redundant near-identical ranges', () => {
    const source = fakeChart('time');
    const target = fakeChart('time');
    wireVisibleTimeRangeSync(source.chart, target.chart, { active: false });
    source.emit({ from: 100, to: 200 });
    source.emit({ from: 100.01, to: 200.01 });
    assert.equal(target.scale.setVisibleRange.mock.callCount(), 1);
  });

  it('skips invalid ranges', () => {
    const source = fakeChart('time');
    const target = fakeChart('time');
    wireVisibleTimeRangeSync(source.chart, target.chart, { active: false });
    source.emit({ from: 10, to: 5 });
    source.emit(null);
    assert.equal(target.scale.setVisibleRange.mock.callCount(), 0);
  });
});

describe('wireChartTimeScales', () => {
  it('fans time sync from source to multiple targets', () => {
    const source = fakeChart('time');
    const a = fakeChart('time');
    const b = fakeChart('time');
    wireChartTimeScales(source.chart, [a.chart, b.chart], { active: false }, { mode: 'time' });
    source.emit({ from: 1, to: 9 });
    assert.equal(a.scale.setVisibleRange.mock.callCount(), 1);
    assert.equal(b.scale.setVisibleRange.mock.callCount(), 1);
  });

  it('optionally wires bidirectional time sync', () => {
    const source = fakeChart('time');
    const child = fakeChart('time');
    wireChartTimeScales(source.chart, [child.chart], { active: false }, {
      mode: 'time',
      bidirectional: true,
    });
    child.emit({ from: 50, to: 80 });
    assert.deepEqual(source.scale.setVisibleRange.mock.calls[0].arguments, [{ from: 50, to: 80 }]);
  });
});

describe('applyVisibleTimeRange', () => {
  it('sets the same wall window on every chart', () => {
    const a = fakeChart('time');
    const b = fakeChart('time');
    applyVisibleTimeRange([a.chart, b.chart], { fromSec: 10, toSec: 40 });
    assert.deepEqual(a.scale.setVisibleRange.mock.calls[0].arguments, [{ from: 10, to: 40 }]);
    assert.deepEqual(b.scale.setVisibleRange.mock.calls[0].arguments, [{ from: 10, to: 40 }]);
  });

  it('does not re-apply when charts already show the window', () => {
    const a = fakeChart('time', { from: 10, to: 40 });
    applyVisibleTimeRange([a.chart], { fromSec: 10, toSec: 40 });
    assert.equal(a.scale.setVisibleRange.mock.callCount(), 0);
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
