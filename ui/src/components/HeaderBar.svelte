<script>
  import { displayPair, fmtPrice, fmtQty, fmtCount, fmtWindowLabel } from '../lib/format.js';

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
    sessionTrades = null,
    windowVolume = null,
    windowTrades = null,
    windowSec = 60,
    eventsPerSec = null,
    venueLive = false,
    mappedVenues = 0,
    liveMapped = 0,
    crossBps = null,
    multiVolume = null,
    multiTrades = null,
    statsMode = 'window', // window | session
    onStatsMode = () => {},
  } = $props();

  let vol = $derived(statsMode === 'session' ? sessionVolume : windowVolume);
  let trades = $derived(statsMode === 'session' ? sessionTrades : windowTrades);
  let volLbl = $derived(
    statsMode === 'session' ? 'Vol (session)' : `Vol (${fmtWindowLabel(windowSec)})`,
  );
  let tradesLbl = $derived(
    statsMode === 'session' ? 'Trades (session)' : `Trades (${fmtWindowLabel(windowSec)})`,
  );
</script>

<header class="header">
  <div class="pair-block">
    <div class="pair-name">{asset || displayPair(symbol)}</div>
    <div class="watching" title={chartMode === 'lines' ? 'Multi-venue overlay' : 'Single-venue focus'}>
      {#if chartMode === 'lines'}
        <span class="watch-label">watching</span>
        <span class="watch-main">{asset || '—'}</span>
        <span class="watch-meta">across {mappedVenues} venues</span>
        <span class="watch-live" class:ok={liveMapped > 0}>
          {liveMapped} live
        </span>
      {:else}
        <span class="watch-label">focus</span>
        <span class="watch-main mono">{venue || '—'}</span>
        <span class="watch-meta mono">{symbol || displayPair(symbol)}</span>
      {/if}
    </div>
    <div class="pair-meta">
      <span class="venue-chip" class:live={venueLive} title="Book / tape focus venue">
        {venue || '—'}
      </span>
      <span class="muted mono">{symbol}</span>
      {#if chartMode === 'lines'}
        <span class="muted">focus book</span>
      {:else}
        <span class="muted">{liveMapped}/{mappedVenues} venues mapped</span>
      {/if}
    </div>
  </div>

  <div class="last-block">
    <div
      class="last-price"
      class:up={priceDir > 0}
      class:down={priceDir < 0}
      class:flat={priceDir === 0}
    >
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
      <div class="val">
        {#if crossBps != null}
          {crossBps.toFixed(2)} bps
        {:else}
          —
        {/if}
      </div>
    </div>
    <div class="stat">
      <div class="lbl">Session High</div>
      <div class="val bid">{sessionHigh != null ? fmtPrice(sessionHigh, 2) : '—'}</div>
    </div>
    <div class="stat">
      <div class="lbl">Session Low</div>
      <div class="val ask">{sessionLow != null ? fmtPrice(sessionLow, 2) : '—'}</div>
    </div>
    <button class="stat clickable" class:active={statsMode === 'window'} onclick={() => onStatsMode('window')} title="Volume / trades in timeframe window for focus venue">
      <div class="lbl">{volLbl}</div>
      <div class="val">{vol != null ? fmtQty(vol) : '—'}</div>
    </button>
    <button class="stat clickable" class:active={statsMode === 'session'} onclick={() => onStatsMode('session')} title="Toggle session vs window stats">
      <div class="lbl">{tradesLbl}</div>
      <div class="val">{trades != null ? fmtCount(trades) : '—'}</div>
    </button>
    <div class="stat" title="Sum across mapped venues (native qty units — not USD-normalized)">
      <div class="lbl">Multi vol / #</div>
      <div class="val">
        {multiVolume != null ? fmtQty(multiVolume) : '—'}
        <span class="muted">/ {multiTrades != null ? fmtCount(multiTrades) : '—'}</span>
      </div>
    </div>
    <div class="stat">
      <div class="lbl">Events/s</div>
      <div class="val">{eventsPerSec != null ? eventsPerSec.toFixed(1) : '—'}</div>
    </div>
  </div>
</header>

<style>
  .header {
    display: flex;
    align-items: stretch;
    gap: 1.25rem;
    padding: 0.4rem 0.75rem;
    background: var(--panel);
    border-bottom: 1px solid var(--border);
    min-height: 56px;
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

  .last-price.up {
    color: var(--bid);
  }
  .last-price.down {
    color: var(--ask);
  }
  .last-price.flat {
    color: var(--text);
  }

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

  .stat .val.bid {
    color: var(--bid);
  }
  .stat .val.ask {
    color: var(--ask);
  }
  .muted {
    color: var(--muted);
    font-size: 0.68rem;
    font-family: var(--mono);
  }
</style>
