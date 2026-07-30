<script>
  import { fmtPrice } from '../lib/format.js';

  let {
    history = [],
    threshold = 15,
    alertActive = false,
    highlightVenues = [],
    onThreshold = () => {},
    onSpikeClick = () => {},
  } = $props();

  let sparkPath = $derived.by(() => {
    const pts = history || [];
    if (pts.length < 2) return '';
    const maxBps = Math.max(threshold * 1.5, ...pts.map((p) => p.bps), 1);
    const w = 100;
    const h = 28;
    const step = w / Math.max(pts.length - 1, 1);
    return pts
      .map((p, i) => {
        const x = i * step;
        const y = h - (p.bps / maxBps) * h;
        return `${i === 0 ? 'M' : 'L'} ${x.toFixed(1)} ${y.toFixed(1)}`;
      })
      .join(' ');
  });

  let currentBps = $derived(history.length ? history[history.length - 1].bps : null);
</script>

<section class="disc-panel" class:alert={alertActive}>
  <div class="head">
    <span class="title">Venue Δ workspace</span>
    <label class="thresh">
      alert ≥
      <input
        type="number"
        min="1"
        max="500"
        step="1"
        value={threshold}
        onchange={(e) => onThreshold(Number(e.currentTarget.value))}
      />
      bps
    </label>
    {#if alertActive}
      <span class="alert-badge">ALERT</span>
    {/if}
  </div>

  <div class="spark-wrap">
    {#if history.length >= 2}
      <svg viewBox="0 0 100 28" preserveAspectRatio="none" class="spark">
        <line
          x1="0"
          y1={28 - (threshold / Math.max(threshold * 1.5, ...history.map((p) => p.bps), 1)) * 28}
          x2="100"
          y2={28 - (threshold / Math.max(threshold * 1.5, ...history.map((p) => p.bps), 1)) * 28}
          stroke="rgba(246,70,93,0.5)"
          stroke-dasharray="2,2"
          stroke-width="0.5"
        />
        <path d={sparkPath} fill="none" stroke="var(--accent)" stroke-width="1" />
        {#each history.filter((_, i) => i % Math.max(1, Math.floor(history.length / 8)) === 0 || i === history.length - 1) as pt, idx}
          <circle
            cx={(idx / Math.max(history.length - 1, 1)) * 100}
            cy={28 - (pt.bps / Math.max(threshold * 1.5, ...history.map((p) => p.bps), 1)) * 28}
            r="1.5"
            fill={pt.bps > threshold ? 'var(--ask)' : 'var(--accent)'}
            role="button"
            tabindex="0"
            onclick={() => onSpikeClick(pt)}
            onkeydown={(e) => e.key === 'Enter' && onSpikeClick(pt)}
          />
        {/each}
      </svg>
    {:else}
      <div class="empty">accumulating Δ bps history…</div>
    {/if}
  </div>

  <div class="footer">
    {#if highlightVenues.length}
      <span class="hl">
        {#each highlightVenues as v}
          <span class="venue-tag">{v}</span>
        {/each}
      </span>
    {:else}
      <span class="current muted-note">Δ history · alert threshold above</span>
    {/if}
  </div>
</section>

<style>
  .disc-panel {
    border-bottom: 1px solid var(--border);
    background: var(--panel-2);
    flex-shrink: 0;
    padding: 0.3rem 0.5rem;
  }

  .disc-panel.alert {
    background: rgba(246, 70, 93, 0.08);
    border-color: rgba(246, 70, 93, 0.35);
  }

  .head {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-bottom: 0.25rem;
  }

  .title {
    font-size: 0.65rem;
    font-weight: 600;
    color: var(--text-dim);
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }

  .thresh {
    display: flex;
    align-items: center;
    gap: 0.25rem;
    font-family: var(--mono);
    font-size: 0.62rem;
    color: var(--muted);
    margin-left: auto;
  }

  .thresh input {
    width: 2.5rem;
    background: var(--bg);
    border: 1px solid var(--border);
    color: var(--text);
    padding: 0.1rem 0.2rem;
    font-family: var(--mono);
    font-size: 0.62rem;
  }

  .alert-badge {
    font-family: var(--mono);
    font-size: 0.58rem;
    font-weight: 700;
    color: var(--ask);
    border: 1px solid rgba(246, 70, 93, 0.5);
    padding: 0.05rem 0.3rem;
  }

  .spark-wrap {
    height: 28px;
  }

  .spark {
    width: 100%;
    height: 28px;
    display: block;
    cursor: crosshair;
  }

  .empty {
    font-family: var(--mono);
    font-size: 0.62rem;
    color: var(--muted);
    text-align: center;
    line-height: 28px;
  }

  .footer {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-top: 0.2rem;
    font-family: var(--mono);
    font-size: 0.62rem;
  }

  .current {
    color: var(--text-dim);
  }
  .muted-note {
    color: var(--muted);
    opacity: 0.9;
  }

  .hl {
    display: flex;
    gap: 0.25rem;
    margin-left: auto;
  }

  .venue-tag {
    color: var(--accent);
    border: 1px solid rgba(240, 185, 11, 0.35);
    padding: 0.02rem 0.25rem;
    font-size: 0.58rem;
  }
</style>
