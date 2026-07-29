/**
 * Keep two Lightweight Charts time scales aligned and return an explicit
 * disposer. Call the disposer before removing either chart.
 *
 * @param {any} source
 * @param {any} target
 * @param {{ active: boolean }} guard
 * @returns {() => void}
 */
export function wireVisibleLogicalRangeSync(source, target, guard) {
  const sourceTimeScale = source.timeScale();
  const targetTimeScale = target.timeScale();

  const onVisibleLogicalRangeChange = (range) => {
    if (!range || guard.active) return;
    guard.active = true;
    try {
      targetTimeScale.setVisibleLogicalRange(range);
    } finally {
      guard.active = false;
    }
  };

  sourceTimeScale.subscribeVisibleLogicalRangeChange(onVisibleLogicalRangeChange);

  let disposed = false;
  return () => {
    if (disposed) return;
    disposed = true;
    sourceTimeScale.unsubscribeVisibleLogicalRangeChange(onVisibleLogicalRangeChange);
  };
}

export function createRangeActivity() {
  const syncGuard = { active: false };
  let programmaticDepth = 0;

  return {
    syncGuard,
    isUserDriven() {
      return !syncGuard.active && programmaticDepth === 0;
    },
    runProgrammatic(operation) {
      programmaticDepth += 1;
      try {
        return operation();
      } finally {
        programmaticDepth -= 1;
      }
    },
  };
}
