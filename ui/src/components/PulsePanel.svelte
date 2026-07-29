<script>
  import { fmtUsd, fmtPrice } from '../lib/format.js';
  import { sparkPath } from '../lib/orderflow.js';

  let {
    pulse = null,
    history = [],
    alertActive = false,
    spikeThreshold = 72,
    asset = 'BTC',
    focusVenue = '',
    metricFilter = '',
    onSpikeThreshold = () => {},
    onChipClick = () => {},
    onMetricClick = () => {},
  } = $props();

  let scoreSpark = $derived(
    sparkPath((history || []).map((p) => p.score), { w: 160, h: 36 }),
  );

  let score = $derived(pulse?.score ?? null);
  let chips = $derived.by(() => {
    let list = pulse?.chips || [];
    if (metricFilter === 'heat') {
      list = [...list].sort((a, b) => (b.heat || 0) - (a.heat || 0));
    } else if (metricFilter === 'spread') {
      list = [...list].sort((a, b) => (b.spreadBps || 0) - (a.spreadBps || 0));
    } else if (metricFilter === 'imb') {
      list = [...list].sort((a, b) => Math.abs(b.imbalancePct || 0) - Math.abs(a.imbalancePct || 0));
    } else if (metricFilter === 'usd') {
      list = [...list].sort((a, b) => (b.usdPerMin || 0) - (a.usdPerMin || 0));
    } else if (metricFilter === 'tpm') {
      list = [...list].sort((a, b) => (b.tradesPerMin || 0) - (a.tradesPerMin || 0));
    }
    return list;
  });

  let maxHeat = $derived(Math.max(1, ...chips.map((c) => c.heat || 0)));

  function metricActive(id) {
    return metricFilter === id;
  }
</script>

<section class="pulse" aria-label="Market pulse" class:alert={alertActive}>
  <div class="head">
    <div class="title-row">
      <span class="title">Market Pulse · {asset}</span>
      {#if alertActive}
        <span class="alert-badge">SPIKE</span>
      {/if}
      {#if metricFilter}
        <button type="button" class="filter-clear" onclick={() => onMetricClick(metricFilter)}>
          filter: {metricFilter} ✕
        </button>
      {/if}
    </div>
    <label class="thresh" title="Pulse spike alert threshold (0–100)">
      alert ≥
      <input
        type="number"
        min="10"
        max="100"
        step="1"
        value={spikeThreshold}
        onchange={(e) => onSpikeThreshold(Number(e.currentTarget.value))}
      />
    </label>
  </div>

  <div class="metrics">
    <button type="button" class="metric" class:active={metricActive('tpm')} onclick={() => onMetricClick('tpm')} title="Sort venues by trades/min">
      <span class="lbl">Trades/min</span>
      <span class="val">{pulse?.tradesPerMin != null ? pulse.tradesPerMin.toFixed(1) : '—'}</span>
    </button>
    <button type="button" class="metric" class:active={metricActive('usd')} onclick={() => onMetricClick('usd')} title="Sort venues by USD/min">
      <span class="lbl">USD/min</span>
      <span class="val">{pulse?.usdPerMin != null ? fmtUsd(pulse.usdPerMin) : '—'}</span>
    </button>
    <button type="button" class="metric" class:active={metricActive('cross')} onclick={() => onMetricClick('cross')} title="Cross-venue discrepancy">
      <span class="lbl">Cross Δ</span>
      <span class="val" class:hot={pulse?.crossBps != null && pulse.crossBps > 10}>
        {pulse?.crossBps != null ? pulse.crossBps.toFixed(1) + ' bps' : '—'}
      </span>
    </button>
    <button type="button" class="metric" class:active={metricActive('spread')} onclick={() => onMetricClick('spread')} title="Sort by median spread">
      <span class="lbl">Med spread</span>
      <span class="val">
        {pulse?.medianSpread != null ? pulse.medianSpread.toFixed(2) + ' bps' : '—'}
      </span>
    </button>
    <button type="button" class="metric" class:active={metricActive('imb')} onclick={() => onMetricClick('imb')} title="Sort by book imbalance">
      <span class="lbl">Book imb</span>
      <span
        class="val"
        class:up={pulse?.bookImbalance > 5}
        class:down={pulse?.bookImbalance < -5}
      >
        {pulse?.bookImbalance != null ? pulse.bookImbalance.toFixed(1) + '%' : '—'}
      </span>
    </button>
    <button type="button" class="metric score" class:active={metricActive('heat')} onclick={() => onMetricClick('heat')} title="Sort by pulse heat">
      <span class="lbl">Pulse score</span>
      <span class="val big">{score != null ? score.toFixed(0) : '—'}</span>
    </button>
  </div>

  <div class="spark-wrap" title="Pulse score (last N samples)">
    {#if scoreSpark}
      <svg viewBox="0 0 160 36" preserveAspectRatio="none">
        <line
          x1="0"
          y1={36 - (spikeThreshold / 100) * 34 - 1}
          x2="160"
          y2={36 - (spikeThreshold / 100) * 34 - 1}
          stroke="rgba(246,70,93,0.45)"
          stroke-dasharray="2,2"
          stroke-width="0.6"
        />
        <path d={scoreSpark} fill="none" stroke="var(--accent)" stroke-width="1.4" />
      </svg>
    {:else}
      <div class="empty">accumulating pulse history…</div>
    {/if}
  </div>

  <div class="chips" aria-label="Per-venue activity heat">
    {#each chips as c (c.venue + '|' + (c.symbol || ''))}
      <button
        type="button"
        class="chip"
        class:offline={!c.live}
        class:focus={c.venue === focusVenue}
        class:spike={(c.heat || 0) >= spikeThreshold}
        style={`--heat:${Math.round(c.heat)}; --vc:${c.color || 'var(--accent)'}; --heatpct:${Math.max(6, ((c.heat || 0) / maxHeat) * 100)}`}
        title="{c.venue} · heat {c.heat.toFixed(0)} · {c.tradesPerMin?.toFixed?.(1) ?? '—'} tpm · {c.usdPerMin != null ? fmtUsd(c.usdPerMin) : '—'}/m — click to focus"
        onclick={() => onChipClick(c.venue, c.symbol)}
      >
        <span class="heat-bar" style={`width:var(--heatpct)%`}></span>
        <span class="vname">{c.venue}</span>
        <span class="vheat">{c.heat.toFixed(0)}</span>
      </button>
    {:else}
      <div class="empty">no venue pulse yet — waiting for multi-venue tape/books</div>
    {/each}
  </div>

  <div class="foot">
    {pulse?.venueCount ?? 0} live venues · click chip → focus book/tape/orderflow · click metric → sort
    {#if pulse?.medianSpread != null}
      · med BBO {fmtPrice(pulse.medianSpread, 2)} bps
    {/if}
  </div>
</section>

<style>
  .pulse {
    height: 100%;
    min-height: 0;
    display: flex;
    flex-direction: column;
    gap: 0.45rem;
    padding: 0.5rem 0.7rem;
    background: var(--panel);
    overflow: auto;
  }
  .pulse.alert {
    box-shadow: inset 0 0 0 1px rgba(246, 70, 93, 0.45);
    animation: pulseFlash 1.4s ease-in-out 2;
  }
  @keyframes pulseFlash {
    0%, 100% { background: var(--panel); }
    50% { background: rgba(246, 70, 93, 0.08); }
  }
  .head {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 0.5rem;
    flex-shrink: 0;
  }
  .title-row { display: flex; align-items: center; gap: 0.5rem; flex-wrap: wrap; }
  .title { font-size: 0.8rem; font-weight: 600; }
  .alert-badge {
    font-family: var(--mono);
    font-size: 0.58rem;
    font-weight: 700;
    color: var(--ask);
    border: 1px solid rgba(246, 70, 93, 0.5);
    padding: 0.05rem 0.35rem;
    border-radius: 2px;
    animation: pulseblink 1.2s ease-in-out infinite;
  }
  @keyframes pulseblink {
    50% { opacity: 0.55; }
  }
  .filter-clear {
    font-family: var(--mono);
    font-size: 0.58rem;
    color: var(--accent);
    background: rgba(240, 185, 11, 0.08);
    border: 1px solid rgba(240, 185, 11, 0.35);
    padding: 0.05rem 0.35rem;
    cursor: pointer;
    border-radius: 2px;
  }
  .thresh {
    font-size: 0.62rem;
    color: var(--muted);
    font-family: var(--mono);
    display: flex;
    align-items: center;
    gap: 0.2rem;
  }
  .thresh input {
    width: 3.5rem;
    background: var(--panel-2);
    border: 1px solid var(--border);
    border-radius: 2px;
    padding: 0.1rem 0.25rem;
    font-family: var(--mono);
    font-size: 0.62rem;
  }

  .metrics {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(6.5rem, 1fr));
    gap: 0.35rem;
    flex-shrink: 0;
  }
  .metric {
    display: flex;
    flex-direction: column;
    gap: 0.08rem;
    align-items: flex-start;
    background: var(--panel-2);
    border: 1px solid var(--border);
    border-radius: 3px;
    padding: 0.35rem 0.45rem;
    cursor: pointer;
    color: inherit;
    text-align: left;
  }
  .metric:hover { border-color: rgba(240, 185, 11, 0.35); }
  .metric.active {
    border-color: rgba(240, 185, 11, 0.55);
    box-shadow: inset 0 0 0 1px rgba(240, 185, 11, 0.2);
  }
  .metric .lbl {
    font-size: 0.55rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.03em;
  }
  .metric .val {
    font-family: var(--mono);
    font-size: 0.95rem;
    font-weight: 600;
  }
  .metric .val.big { font-size: 1.3rem; color: var(--accent); }
  .metric .val.hot { color: var(--ask); }
  .metric .val.up { color: var(--bid); }
  .metric .val.down { color: var(--ask); }

  .spark-wrap {
    height: 48px;
    background: var(--panel-2);
    border: 1px solid var(--border);
    flex-shrink: 0;
  }
  .spark-wrap svg { width: 100%; height: 100%; display: block; }

  .chips {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(9.5rem, 1fr));
    gap: 0.4rem;
    flex: 1;
    align-content: flex-start;
    min-height: 0;
  }
  .chip {
    position: relative;
    display: flex;
    align-items: center;
    gap: 0.4rem;
    padding: 0.4rem 0.55rem;
    background: var(--panel-2);
    border: 1px solid var(--border);
    border-radius: 3px;
    cursor: pointer;
    overflow: hidden;
    min-height: 2.2rem;
  }
  .chip:hover { border-color: var(--vc); }
  .chip.focus {
    border-color: var(--accent);
    box-shadow: inset 0 0 0 1px rgba(240, 185, 11, 0.25);
  }
  .chip.spike {
    animation: chipSpike 0.9s ease-out 1;
  }
  @keyframes chipSpike {
    0% { filter: brightness(1.4); }
    100% { filter: brightness(1); }
  }
  .chip.offline { opacity: 0.45; }
  .heat-bar {
    position: absolute;
    left: 0; top: 0; bottom: 0;
    background: color-mix(in srgb, var(--vc) 32%, transparent);
    pointer-events: none;
    /* Instant heat bar — transition fights 10Hz pulse updates */
  }
  .vname {
    position: relative;
    font-family: var(--mono);
    font-size: 0.7rem;
    color: var(--text);
  }
  .vheat {
    position: relative;
    margin-left: auto;
    font-family: var(--mono);
    font-size: 0.72rem;
    font-weight: 700;
    color: var(--accent);
  }

  .foot {
    flex-shrink: 0;
    font-size: 0.55rem;
    color: var(--muted);
    font-family: var(--mono);
    border-top: 1px solid var(--border);
    padding-top: 0.3rem;
  }
  .empty {
    color: var(--muted);
    font-family: var(--mono);
    font-size: 0.65rem;
    padding: 0.5rem;
  }

  @media (max-width: 720px) {
    .metrics { grid-template-columns: repeat(3, 1fr); }
    .chips { grid-template-columns: repeat(auto-fill, minmax(7.5rem, 1fr)); }
  }
</style>
