<script>
  import { fmtPrice, fmtSecClock } from '../lib/format.js';
  import { numericCommit } from '../lib/numericInput.js';

  let {
    history = [],
    threshold = 15,
    alertActive = false,
    highlightVenues = [],
    onThreshold = () => {},
    onSpikeClick = () => {},
  } = $props();

  /** @type {{ pt: object, xPct: number, yPct: number } | null} */
  let hover = $state(null);

  let scaleMax = $derived(
    Math.max(threshold * 1.5, ...(history || []).map((p) => p.bps), 1),
  );

  let sparkPath = $derived.by(() => {
    const pts = history || [];
    if (pts.length < 2) return '';
    const w = 100;
    const h = 28;
    const step = w / Math.max(pts.length - 1, 1);
    return pts
      .map((p, i) => {
        const x = i * step;
        const y = h - (p.bps / scaleMax) * h;
        return `${i === 0 ? 'M' : 'L'} ${x.toFixed(1)} ${y.toFixed(1)}`;
      })
      .join(' ');
  });

  let current = $derived(history.length ? history[history.length - 1] : null);
  let tip = $derived(hover?.pt || current);

  function pointAt(clientX, svgEl) {
    const pts = history || [];
    if (!pts.length || !svgEl) return null;
    const rect = svgEl.getBoundingClientRect();
    if (rect.width <= 0) return null;
    const xPct = Math.min(1, Math.max(0, (clientX - rect.left) / rect.width));
    const idx = Math.round(xPct * (pts.length - 1));
    const pt = pts[idx];
    if (!pt) return null;
    return {
      pt,
      xPct: (idx / Math.max(pts.length - 1, 1)) * 100,
      yPct: 100 - (pt.bps / scaleMax) * 100,
    };
  }

  function onSparkMove(e) {
    hover = pointAt(e.clientX, e.currentTarget);
  }

  function onSparkLeave() {
    hover = null;
  }

  function tipLines(pt) {
    if (!pt) return [];
    return [
      `Δ ${Number(pt.bps).toFixed(2)} bps`,
      `High ${pt.highVenue || '—'} @ ${fmtPrice(pt.max, 2)}`,
      `Low ${pt.lowVenue || '—'} @ ${fmtPrice(pt.min, 2)}`,
      `Alert ≥ ${threshold} bps`,
      `Sample ${fmtSecClock(pt.time)}`,
    ];
  }
</script>

<section
  class="disc-panel"
  class:alert={alertActive}
  title={tip ? tipLines(tip).join(' · ') : 'Venue & Workspace Δ bps'}
>
  <div class="head">
    <span class="title">Venue &amp; Workspace</span>
    <label class="thresh">
      alert ≥
      <input
        type="number"
        min="1"
        max="500"
        step="1"
        use:numericCommit={{ value: threshold, min: 1, max: 500, onCommit: onThreshold }}
      />
      bps
    </label>
    {#if alertActive}
      <span class="alert-badge">ALERT</span>
    {/if}
  </div>

  <div class="spark-wrap">
    {#if history.length >= 2}
      <svg
        viewBox="0 0 100 28"
        preserveAspectRatio="none"
        class="spark"
        role="img"
        aria-label="Cross-venue Δ bps history"
        onmousemove={onSparkMove}
        onmouseleave={onSparkLeave}
      >
        <line
          x1="0"
          y1={28 - (threshold / scaleMax) * 28}
          x2="100"
          y2={28 - (threshold / scaleMax) * 28}
          stroke="rgba(246,70,93,0.5)"
          stroke-dasharray="2,2"
          stroke-width="0.5"
        />
        <path d={sparkPath} fill="none" stroke="var(--accent)" stroke-width="1" />
        {#each history as pt, i}
          {#if i % Math.max(1, Math.floor(history.length / 8)) === 0 || i === history.length - 1}
            <circle
              cx={(i / Math.max(history.length - 1, 1)) * 100}
              cy={28 - (pt.bps / scaleMax) * 28}
              r={hover?.pt === pt ? 2.4 : 1.5}
              fill={pt.bps > threshold ? 'var(--ask)' : 'var(--accent)'}
              role="button"
              tabindex="0"
              onclick={() => onSpikeClick(pt)}
              onkeydown={(e) => e.key === 'Enter' && onSpikeClick(pt)}
            />
          {/if}
        {/each}
        {#if hover}
          <circle
            cx={hover.xPct}
            cy={28 - (hover.pt.bps / scaleMax) * 28}
            r="2.6"
            fill="none"
            stroke="var(--text)"
            stroke-width="0.7"
          />
        {/if}
      </svg>
      {#if hover}
        <div
          class="tip"
          style={`left: ${Math.min(78, Math.max(2, hover.xPct - 12))}%`}
          role="tooltip"
        >
          {#each tipLines(hover.pt) as line}
            <div>{line}</div>
          {/each}
        </div>
      {/if}
    {:else}
      <div class="empty">accumulating Δ bps history…</div>
    {/if}
  </div>

  <div class="footer">
    {#if tip}
      <span class="current" class:hot={tip.bps > threshold}>
        Δ {tip.bps.toFixed(2)} bps
        <span class="muted-note">· {fmtSecClock(tip.time)}</span>
      </span>
      <span class="hl">
        {#if tip.highVenue}<span class="venue-tag high" title="High venue">{tip.highVenue}</span>{/if}
        {#if tip.lowVenue}<span class="venue-tag low" title="Low venue">{tip.lowVenue}</span>{/if}
      </span>
    {:else if highlightVenues.length}
      <span class="hl">
        {#each highlightVenues as v}
          <span class="venue-tag">{v}</span>
        {/each}
      </span>
    {:else}
      <span class="current muted-note">Hover Δ history for sample details</span>
    {/if}
  </div>
</section>

<style>
  .disc-panel {
    border-bottom: 1px solid var(--border);
    background: var(--panel-2);
    flex-shrink: 0;
    padding: 0.3rem 0.5rem;
    position: relative;
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
    position: relative;
  }

  .spark {
    width: 100%;
    height: 28px;
    display: block;
    cursor: crosshair;
  }

  .tip {
    position: absolute;
    top: calc(100% + 2px);
    z-index: 5;
    min-width: 11rem;
    padding: 0.28rem 0.4rem;
    background: var(--panel);
    border: 1px solid var(--border);
    box-shadow: 0 4px 14px rgba(0, 0, 0, 0.45);
    font-family: var(--mono);
    font-size: 0.58rem;
    color: var(--text-dim);
    line-height: 1.35;
    pointer-events: none;
  }

  .tip div:first-child {
    color: var(--text);
    font-weight: 600;
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
    min-height: 1rem;
  }

  .current {
    color: var(--text-dim);
  }
  .current.hot {
    color: var(--ask);
  }
  .muted-note {
    color: var(--muted);
    opacity: 0.9;
  }

  .hl {
    display: flex;
    gap: 0.25rem;
    margin-left: auto;
    flex-wrap: wrap;
    justify-content: flex-end;
  }

  .venue-tag {
    color: var(--accent);
    border: 1px solid rgba(240, 185, 11, 0.35);
    padding: 0.02rem 0.25rem;
    font-size: 0.58rem;
  }
  .venue-tag.high {
    color: var(--ask);
    border-color: rgba(246, 70, 93, 0.45);
  }
  .venue-tag.low {
    color: var(--bid);
    border-color: rgba(2, 192, 118, 0.45);
  }
</style>
