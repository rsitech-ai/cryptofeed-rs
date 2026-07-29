<script>
  import { fmtTotal } from '../lib/format.js';

  let { book = null, depth = 16 } = $props();

  function xPos(px, data) {
    return ((px - data.minPx) / data.span) * 100;
  }

  function yPos(cum, data) {
    return 100 - (cum / data.maxCum) * 95;
  }

  function buildArea(pts, data, side) {
    if (!pts.length) return '';
    const sorted = side === 'bid' ? [...pts].reverse() : pts;
    let d = `M ${xPos(sorted[0].px, data)} 100`;
    for (const p of sorted) {
      d += ` L ${xPos(p.px, data)} ${yPos(p.cum, data)}`;
    }
    d += ` L ${xPos(sorted[sorted.length - 1].px, data)} 100 Z`;
    return d;
  }

  let chartData = $derived.by(() => {
    const asks = [...(book?.asks || [])].slice(0, depth);
    const bids = [...(book?.bids || [])].slice(0, depth);
    if (!asks.length && !bids.length) return null;

    let askCum = 0;
    const askPts = asks.map((l) => {
      const px = Number(l.price);
      const qty = Number(l.quantity) || 0;
      askCum += qty;
      return { px, cum: askCum };
    });

    let bidCum = 0;
    const bidPts = bids.map((l) => {
      const px = Number(l.price);
      const qty = Number(l.quantity) || 0;
      bidCum += qty;
      return { px, cum: bidCum };
    });

    const maxCum = Math.max(askCum, bidCum, 1e-12);
    const allPx = [...askPts.map((p) => p.px), ...bidPts.map((p) => p.px)];
    const minPx = Math.min(...allPx);
    const maxPx = Math.max(...allPx);
    const span = maxPx - minPx || 1;

    return { askPts, bidPts, maxCum, minPx, span };
  });
</script>

<div class="depth-chart" aria-label="Cumulative depth chart">
  {#if chartData}
    <svg viewBox="0 0 100 100" preserveAspectRatio="none">
      <defs>
        <linearGradient id="bidGrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="rgba(2,192,118,0.35)" />
          <stop offset="100%" stop-color="rgba(2,192,118,0.05)" />
        </linearGradient>
        <linearGradient id="askGrad" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="rgba(246,70,93,0.35)" />
          <stop offset="100%" stop-color="rgba(246,70,93,0.05)" />
        </linearGradient>
      </defs>
      {#if chartData.bidPts.length}
        <path fill="url(#bidGrad)" d={buildArea(chartData.bidPts, chartData, 'bid')} />
      {/if}
      {#if chartData.askPts.length}
        <path fill="url(#askGrad)" d={buildArea(chartData.askPts, chartData, 'ask')} />
      {/if}
    </svg>
    <div class="labels">
      <span class="bid">bid {fmtTotal(chartData.bidPts.at(-1)?.cum ?? 0)}</span>
      <span class="ask">ask {fmtTotal(chartData.askPts.at(-1)?.cum ?? 0)}</span>
    </div>
  {:else}
    <div class="empty">depth chart — waiting for book</div>
  {/if}
</div>

<style>
  .depth-chart {
    position: relative;
    height: 72px;
    flex-shrink: 0;
    border-bottom: 1px solid var(--border);
    background: var(--panel-2);
  }

  svg {
    width: 100%;
    height: 100%;
    display: block;
  }

  .labels {
    position: absolute;
    inset: 0;
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    padding: 0.2rem 0.4rem;
    pointer-events: none;
    font-family: var(--mono);
    font-size: 0.55rem;
  }

  .labels .bid {
    color: var(--bid);
  }
  .labels .ask {
    color: var(--ask);
  }

  .empty {
    display: grid;
    place-items: center;
    height: 100%;
    color: var(--muted);
    font-family: var(--mono);
    font-size: 0.62rem;
  }
</style>
