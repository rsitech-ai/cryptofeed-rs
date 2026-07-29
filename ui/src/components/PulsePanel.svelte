<script>
  import { fmtUsd, fmtPrice } from '../lib/format.js';
  import { sparkPath } from '../lib/orderflow.js';

  let {
    pulse = null,
    history = [],
    alertActive = false,
    spikeThreshold = 72,
    asset = 'BTC',
    onSpikeThreshold = () => {},
    onChipClick = () => {},
  } = $props();

  let scoreSpark = $derived(
    sparkPath((history || []).map((p) => p.score), { w: 160, h: 36 }),
  );

  let score = $derived(pulse?.score ?? null);
  let chips = $derived(pulse?.chips || []);
</script>

<section class="pulse" aria-label="Market pulse" class:alert={alertActive}>
  <div class="head">
    <div class="title-row">
      <span class="title">Market Pulse · {asset}</span>
      {#if alertActive}
        <span class="alert-badge">SPIKE</span>
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
    <div class="metric">
      <span class="lbl">Trades/min</span>
      <span class="val">{pulse?.tradesPerMin != null ? pulse.tradesPerMin.toFixed(1) : '—'}</span>
    </div>
    <div class="metric">
      <span class="lbl">USD/min</span>
      <span class="val">{pulse?.usdPerMin != null ? fmtUsd(pulse.usdPerMin) : '—'}</span>
    </div>
    <div class="metric">
      <span class="lbl">Cross Δ</span>
      <span class="val" class:hot={pulse?.crossBps != null && pulse.crossBps > 10}>
        {pulse?.crossBps != null ? pulse.crossBps.toFixed(1) + ' bps' : '—'}
      </span>
    </div>
    <div class="metric">
      <span class="lbl">Med spread</span>
      <span class="val">
        {pulse?.medianSpread != null ? pulse.medianSpread.toFixed(2) + ' bps' : '—'}
      </span>
    </div>
    <div class="metric">
      <span class="lbl">Book imb</span>
      <span
        class="val"
        class:up={pulse?.bookImbalance > 5}
        class:down={pulse?.bookImbalance < -5}
      >
        {pulse?.bookImbalance != null ? pulse.bookImbalance.toFixed(1) + '%' : '—'}
      </span>
    </div>
    <div class="metric score">
      <span class="lbl">Pulse score</span>
      <span class="val big">{score != null ? score.toFixed(0) : '—'}</span>
    </div>
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
    {#each chips as c}
      <button
        type="button"
        class="chip"
        class:offline={!c.live}
        style={`--heat:${Math.round(c.heat)}; --vc:${c.color || 'var(--accent)'}`}
        title="{c.venue} · heat {c.heat.toFixed(0)} · {c.tradesPerMin?.toFixed?.(1) ?? '—'} tpm · {c.usdPerMin != null ? fmtUsd(c.usdPerMin) : '—'}/m"
        onclick={() => onChipClick(c.venue, c.symbol)}
      >
        <span class="heat-bar" style={`width:${Math.max(4, c.heat)}%`}></span>
        <span class="vname">{c.venue}</span>
        <span class="vheat">{c.heat.toFixed(0)}</span>
      </button>
    {:else}
      <div class="empty">no venue pulse yet — waiting for multi-venue tape/books</div>
    {/each}
  </div>

  <div class="foot">
    {pulse?.venueCount ?? 0} live venues · activity heat from trades/min + USD/min + imbalance + spread
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
    gap: 0.4rem;
    padding: 0.45rem 0.6rem;
    background: var(--panel);
    overflow: auto;
  }
  .pulse.alert {
    box-shadow: inset 0 0 0 1px rgba(246, 70, 93, 0.45);
  }
  .head {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 0.5rem;
    flex-shrink: 0;
  }
  .title-row { display: flex; align-items: center; gap: 0.5rem; }
  .title { font-size: 0.75rem; font-weight: 600; }
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
    display: flex;
    flex-wrap: wrap;
    gap: 0.55rem 1.1rem;
    flex-shrink: 0;
  }
  .metric { display: flex; flex-direction: column; gap: 0.05rem; }
  .metric .lbl {
    font-size: 0.55rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.03em;
  }
  .metric .val {
    font-family: var(--mono);
    font-size: 0.9rem;
    font-weight: 600;
  }
  .metric .val.big { font-size: 1.25rem; color: var(--accent); }
  .metric .val.hot { color: var(--ask); }
  .metric .val.up { color: var(--bid); }
  .metric .val.down { color: var(--ask); }

  .spark-wrap {
    height: 44px;
    background: var(--panel-2);
    border: 1px solid var(--border);
    flex-shrink: 0;
  }
  .spark-wrap svg { width: 100%; height: 100%; display: block; }

  .chips {
    display: flex;
    flex-wrap: wrap;
    gap: 0.35rem;
    flex: 1;
    align-content: flex-start;
    min-height: 0;
  }
  .chip {
    position: relative;
    display: flex;
    align-items: center;
    gap: 0.4rem;
    padding: 0.25rem 0.45rem;
    background: var(--panel-2);
    border: 1px solid var(--border);
    border-radius: 3px;
    cursor: pointer;
    overflow: hidden;
    min-width: 7.5rem;
  }
  .chip:hover { border-color: var(--vc); }
  .chip.offline { opacity: 0.45; }
  .heat-bar {
    position: absolute;
    left: 0; top: 0; bottom: 0;
    background: color-mix(in srgb, var(--vc) 28%, transparent);
    pointer-events: none;
  }
  .vname {
    position: relative;
    font-family: var(--mono);
    font-size: 0.65rem;
    color: var(--text);
  }
  .vheat {
    position: relative;
    margin-left: auto;
    font-family: var(--mono);
    font-size: 0.65rem;
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
</style>
