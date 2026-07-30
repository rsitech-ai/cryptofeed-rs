<script>
  import { fmtPrice, fmtTotal } from '../lib/format.js';

  let { book = null, depth = 16, interactive = true } = $props();

  let tip = $state(/** @type {string} */ (''));
  let tipX = $state(0);
  let tipY = $state(0);

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
      return { px, cum: askCum, qty };
    });

    let bidCum = 0;
    const bidPts = bids.map((l) => {
      const px = Number(l.price);
      const qty = Number(l.quantity) || 0;
      bidCum += qty;
      return { px, cum: bidCum, qty };
    });

    const maxCum = Math.max(askCum, bidCum, 1e-12);
    const allPx = [...askPts.map((p) => p.px), ...bidPts.map((p) => p.px)];
    const minPx = Math.min(...allPx);
    const maxPx = Math.max(...allPx);
    const span = maxPx - minPx || 1;

    return { askPts, bidPts, maxCum, minPx, maxPx, span };
  });

  /** @param {MouseEvent & { currentTarget: HTMLElement }} e */
  function onMove(e) {
    if (!interactive || !chartData) {
      tip = '';
      return;
    }
    const rect = e.currentTarget.getBoundingClientRect();
    const t = Math.min(1, Math.max(0, (e.clientX - rect.left) / Math.max(1, rect.width)));
    const px = chartData.minPx + t * chartData.span;
    const mid = (chartData.minPx + chartData.maxPx) / 2;
    const side = px <= mid ? 'bid' : 'ask';
    const pts = side === 'bid' ? chartData.bidPts : chartData.askPts;
    let nearest = pts[0];
    let best = Infinity;
    for (const p of pts) {
      const d = Math.abs(p.px - px);
      if (d < best) {
        best = d;
        nearest = p;
      }
    }
    if (!nearest) {
      tip = '';
      return;
    }
    tip = `${side.toUpperCase()} ${fmtPrice(nearest.px, 2)} · size ${fmtTotal(nearest.qty)} · cum ${fmtTotal(nearest.cum)}`;
    tipX = e.clientX + 12;
    tipY = e.clientY + 12;
  }

  function onLeave() {
    tip = '';
  }
</script>

<div
  class="depth-chart"
  class:interactive
  aria-label="Cumulative depth chart"
  role={interactive ? 'img' : undefined}
  onmousemove={interactive ? onMove : undefined}
  onmouseleave={interactive ? onLeave : undefined}
>
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

{#if tip}
  <div class="depth-tip" style={`left:${tipX}px;top:${tipY}px`} role="tooltip">{tip}</div>
{/if}

<style>
  .depth-chart {
    position: relative;
    height: 80px;
    min-height: 80px;
    flex-shrink: 0;
    background: var(--panel-2);
  }
  .depth-chart.interactive {
    cursor: crosshair;
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

  .depth-tip {
    position: fixed;
    z-index: 90;
    pointer-events: none;
    padding: 0.28rem 0.45rem;
    background: rgba(18, 22, 28, 0.96);
    border: 1px solid rgba(240, 185, 11, 0.35);
    border-radius: 2px;
    color: var(--text);
    font-family: var(--mono);
    font-size: 0.58rem;
    box-shadow: 0 4px 16px rgba(0, 0, 0, 0.45);
  }
</style>
