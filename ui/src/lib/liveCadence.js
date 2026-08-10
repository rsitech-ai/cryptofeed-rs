/** Bounds expensive Svelte publications while retaining the freshest SSE frame. */
export class LiveCadence {
  constructor(intervals = {}) {
    this.intervals = { ...intervals };
    this.last = new Map();
  }

  allow(surface, now = Date.now()) {
    const interval = Math.max(0, Number(this.intervals[surface]) || 0);
    const previous = this.last.get(surface);
    if (previous != null && now - previous < interval) return false;
    this.last.set(surface, now);
    return true;
  }

  reset() {
    this.last.clear();
  }
}
