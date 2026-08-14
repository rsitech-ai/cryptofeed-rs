<script>
  import { displayPair, fmtPrice, fmtUsd, fmtCount, fmtWindowLabel, fmtTradesPerMin } from '../lib/format.js';
  import { SESSION_PRESETS } from '../lib/session.js';

  let {
    asset = '',
    venue = '',
    symbol = '',
    chartMode = 'lines',
    lastPrice = null,
    priceDir = 0,
    bid = null,
    ask = null,
    mid = null,
    spread = null,
    spreadBps = null,
    sessionHigh = null,
    sessionLow = null,
    sessionVolume = null,
    sessionNotional = null,
    sessionTrades = null,
    windowVolume = null,
    windowNotional = null,
    windowTrades = null,
    windowSec = 60,
    sessionPreset = '5m',
    eventsPerSec = null,
    venueLive = false,
    mappedVenues = 0,
    liveMapped = 0,
    crossBps = null,
    multiNotional = null,
    multiTrades = null,
    multiTradesPerMin = null,
    statsMode = 'window',
    density = 'comfortable',
    grafanaUrl = '',
    streamMode = 'poll',
    streamReconnecting = false,
    onStatsMode = () => {},
    onSessionPreset = () => {},
    onDensity = () => {},
    onGrafana = () => {},
  } = $props();

  let vol = $derived(statsMode === 'session' ? (sessionNotional ?? sessionVolume) : (windowNotional ?? windowVolume));
  let trades = $derived(statsMode === 'session' ? sessionTrades : windowTrades);
  let volLbl = $derived(
    statsMode === 'session'
      ? 'Vol USD (session)'
      : `Vol USD (${fmtWindowLabel(windowSec)})`,
  );
  let tradesLbl = $derived(
    statsMode === 'session'
      ? 'Trades (session)'
      : `Trades (${fmtWindowLabel(windowSec)})`,
  );
</script>

<header class="header">
  <div class="pair-block">
    <div class="pair-name">{asset || displayPair(symbol)}</div>
    <div class="watching" title={chartMode === 'lines' ? 'Multi-venue overlay' : chartMode === 'orderflow' ? 'L2+tape heatmap + DOM (not MBO)' : 'Single-venue focus'}>
      {#if chartMode === 'lines'}
        <span class="watch-label">watching</span>
        <span class="watch-main">{asset || '—'}</span>
        <span class="watch-meta">across {mappedVenues} venues</span>
        <span class="watch-live" class:ok={liveMapped > 0}>{liveMapped} live</span>
      {:else if chartMode === 'orderflow'}
        <span class="watch-label">order flow · L2+tape</span>
        <span class="watch-main mono">{venue || '—'}</span>
        <span class="watch-meta mono">{symbol || displayPair(symbol)}</span>
      {:else}
        <span class="watch-label">focus</span>
        <span class="watch-main mono">{venue || '—'}</span>
        <span class="watch-meta mono">{symbol || displayPair(symbol)}</span>
      {/if}
    </div>
    <div class="pair-meta">
      <span class="venue-chip" class:live={venueLive} title="Book / tape focus venue">{venue || '—'}</span>
      <span class="muted mono">{symbol}</span>
      <span
        class="stream-chip"
        class:soft={streamReconnecting && streamMode === 'sse'}
        title={streamReconnecting ? 'SSE reconnecting (UI stays mounted)' : 'Data transport'}
      >{streamMode === 'sse' ? 'SSE' : 'poll'}</span>
      <span class="derivatives-chip" title="Exchange-reported funding, open interest, and liquidations are in the Derivatives strip under the chart">Funding · OI · Liq</span>
    </div>
  </div>

  <div class="last-block">
    <div class="last-price" class:up={priceDir > 0} class:down={priceDir < 0} class:flat={priceDir === 0}>
      {lastPrice != null ? fmtPrice(lastPrice, 2) : '—'}
    </div>
  </div>

  <div class="stats">
    <div class="stat">
      <div class="lbl">Bid</div>
      <div class="val bid">{bid != null ? fmtPrice(bid, 2) : '—'}</div>
    </div>
    <div class="stat">
      <div class="lbl">Ask</div>
      <div class="val ask">{ask != null ? fmtPrice(ask, 2) : '—'}</div>
    </div>
    <div class="stat">
      <div class="lbl">Mid</div>
      <div class="val">{mid != null ? fmtPrice(mid, 2) : '—'}</div>
    </div>
    <div class="stat">
      <div class="lbl">Book spread</div>
      <div class="val">
        {spread != null ? fmtPrice(spread, 2) : '—'}
        {#if spreadBps != null}
          <span class="muted">({spreadBps < 0.01 ? spreadBps.toFixed(4) : spreadBps.toFixed(2)} bps)</span>
        {/if}
      </div>
    </div>
    <div class="stat">
      <div class="lbl">Cross-venue Δ</div>
      <div class="val">{crossBps != null ? crossBps.toFixed(2) + ' bps' : '—'}</div>
    </div>
    <div class="stat">
      <div class="lbl">Session High</div>
      <div class="val bid">{sessionHigh != null ? fmtPrice(sessionHigh, 2) : '—'}</div>
    </div>
    <div class="stat">
      <div class="lbl">Session Low</div>
      <div class="val ask">{sessionLow != null ? fmtPrice(sessionLow, 2) : '—'}</div>
    </div>
    <button class="stat clickable" class:active={statsMode === 'window'} onclick={() => onStatsMode('window')}>
      <div class="lbl">{volLbl}</div>
      <div class="val">{vol != null ? fmtUsd(vol) : '—'}</div>
    </button>
    <button class="stat clickable" class:active={statsMode === 'session'} onclick={() => onStatsMode('session')}>
      <div class="lbl">{tradesLbl}</div>
      <div class="val">{trades != null ? fmtCount(trades) : '—'}</div>
    </button>
    <div class="stat" title="USD notional sum across mapped venues">
      <div class="lbl">Multi USD / #</div>
      <div class="val">
        {multiNotional != null ? fmtUsd(multiNotional) : '—'}
        <span class="muted">/ {multiTrades != null ? fmtCount(multiTrades) : '—'}</span>
        {#if multiTradesPerMin != null}
          <span class="muted"> · {fmtTradesPerMin(multiTrades, windowSec)}</span>
        {/if}
      </div>
    </div>
    <div class="stat">
      <div class="lbl">Events/s</div>
      <div class="val">{eventsPerSec != null ? eventsPerSec.toFixed(1) : '—'}</div>
    </div>
    <div class="session-presets">
      {#each SESSION_PRESETS as sp}
        <button
          type="button"
          class:active={sessionPreset === sp.id}
          onclick={() => onSessionPreset(sp.id)}
          title="Chart view + stats window"
        >{sp.label}</button>
      {/each}
    </div>
    <button type="button" class="icon-btn" onclick={() => onDensity()} title="Toggle compact/comfortable density">
      {density === 'compact' ? '▣' : '▢'}
    </button>
    {#if grafanaUrl}
      <button type="button" class="icon-btn grafana" onclick={() => onGrafana()} title="Open Grafana dashboard">Grafana</button>
    {/if}
  </div>
</header>

<style>
  .header {
    display: flex;
    align-items: stretch;
    gap: 1.25rem;
    padding: var(--header-pad, 0.4rem 0.75rem);
    background: var(--panel);
    border-bottom: 1px solid var(--border);
    min-height: var(--header-h, 56px);
    flex-shrink: 0;
  }

  .pair-block {
    display: flex;
    flex-direction: column;
    justify-content: center;
    min-width: 11rem;
    gap: 0.1rem;
  }

  .pair-name {
    font-size: 1.05rem;
    font-weight: 700;
    letter-spacing: 0.01em;
    color: var(--text);
    line-height: 1.1;
  }

  .watching {
    display: flex;
    align-items: baseline;
    flex-wrap: wrap;
    gap: 0.28rem;
    line-height: 1.2;
  }

  .watch-label {
    font-size: 0.58rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--muted);
    font-weight: 600;
  }

  .watch-main {
    font-size: 0.78rem;
    font-weight: 700;
    color: var(--accent);
  }

  .watch-main.mono,
  .mono {
    font-family: var(--mono);
  }

  .watch-meta {
    font-family: var(--mono);
    font-size: 0.68rem;
    color: var(--text-dim);
  }

  .watch-live {
    font-family: var(--mono);
    font-size: 0.62rem;
    color: var(--muted);
    border: 1px solid var(--border);
    padding: 0.02rem 0.28rem;
  }

  .watch-live.ok {
    color: var(--bid);
    border-color: rgba(2, 192, 118, 0.35);
  }

  .pair-meta {
    display: flex;
    gap: 0.35rem;
    align-items: center;
    flex-wrap: wrap;
    margin-top: 0.05rem;
  }

  .venue-chip {
    font-family: var(--mono);
    font-size: 0.65rem;
    color: var(--muted);
    border: 1px solid var(--border);
    padding: 0.02rem 0.3rem;
    text-transform: lowercase;
  }

  .venue-chip.live {
    color: var(--bid);
    border-color: rgba(2, 192, 118, 0.35);
  }

  .stream-chip {
    font-family: var(--mono);
    font-size: 0.55rem;
    color: var(--muted);
    border: 1px solid var(--border);
    padding: 0.02rem 0.25rem;
    min-width: 2.4rem;
    text-align: center;
  }

  .stream-chip.soft {
    opacity: 0.75;
    border-style: dashed;
  }

  .derivatives-chip {
    font-family: var(--mono);
    font-size: 0.52rem;
    color: var(--text);
    border: 1px solid var(--border);
    padding: 0.02rem 0.28rem;
    white-space: nowrap;
  }

  .last-block {
    display: flex;
    align-items: center;
    padding-right: 0.5rem;
    border-right: 1px solid var(--border);
  }

  .last-price {
    font-family: var(--mono);
    font-size: 1.35rem;
    font-weight: 700;
    letter-spacing: -0.02em;
    line-height: 1;
  }

  .last-price.up { color: var(--bid); }
  .last-price.down { color: var(--ask); }
  .last-price.flat { color: var(--text); }

  .stats {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.85rem 1.25rem;
    flex: 1;
    min-width: 0;
  }

  .stat {
    background: transparent;
    border: none;
    padding: 0;
    text-align: left;
    color: inherit;
  }

  .stat.clickable {
    cursor: pointer;
    border-radius: 2px;
    padding: 0.1rem 0.25rem;
    border: 1px solid transparent;
  }
  .stat.clickable:hover {
    background: var(--panel-2);
    border-color: var(--border);
  }
  .stat.clickable.active {
    border-color: rgba(240, 185, 11, 0.35);
    background: rgba(240, 185, 11, 0.08);
  }

  .stat .lbl {
    font-size: 0.65rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.04em;
    margin-bottom: 0.1rem;
  }

  .stat .val {
    font-family: var(--mono);
    font-size: 0.78rem;
    color: var(--text);
    white-space: nowrap;
  }

  .stat .val.bid { color: var(--bid); }
  .stat .val.ask { color: var(--ask); }

  .muted {
    color: var(--muted);
    font-size: 0.68rem;
    font-family: var(--mono);
  }

  .session-presets {
    display: flex;
    gap: 0.1rem;
  }

  .session-presets button {
    background: transparent;
    border: 1px solid transparent;
    color: var(--muted);
    font-family: var(--mono);
    font-size: 0.62rem;
    padding: 0.1rem 0.3rem;
    cursor: pointer;
  }

  .session-presets button.active {
    color: var(--accent);
    border-color: rgba(240, 185, 11, 0.35);
  }

  .icon-btn {
    background: transparent;
    border: 1px solid var(--border);
    color: var(--muted);
    font-family: var(--mono);
    font-size: 0.62rem;
    padding: 0.15rem 0.4rem;
    cursor: pointer;
  }

  .icon-btn.grafana {
    color: var(--accent);
    border-color: rgba(240, 185, 11, 0.35);
  }

  @media (max-width: 720px) {
    .header {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      align-items: center;
      gap: 0.6rem 0.8rem;
    }

    .pair-block {
      min-width: 0;
    }

    .last-block {
      border-right: none;
      padding-right: 0;
    }

    .stats {
      grid-column: 1 / -1;
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 0.55rem 0.75rem;
      width: 100%;
    }

    .stat,
    .session-presets,
    .icon-btn {
      min-width: 0;
    }
  }

  @media (max-width: 480px) {
    .stats {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
  }
</style>
