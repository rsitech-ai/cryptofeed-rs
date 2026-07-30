<script>
  import { fmtPrice, fmtUsd } from '../lib/format.js';
  import {
    bookPressure,
    computeCvd,
    detectFlowHeuristics,
    sparkPath,
    tradeNotional,
    volumeAtPrice,
  } from '../lib/orderflow.js';
  import DomLadder from './DomLadder.svelte';
  import DepthChart from './DepthChart.svelte';

  let {
    /** @type {'both' | 'flow' | 'pulse'} */
    section = 'both',
    book = null,
    tape = [],
    depth = 16,
    lastPrice = null,
    windowSec = 300,
    largeUsd = 25000,
    imbalanceHistory = [],
    tickOpt = 'auto',
    /** When chart already shows DOM, skip ladder to avoid duplication. */
    showLadder = true,
    pulse = null,
    history = [],
    alertActive = false,
    spikeThreshold = 72,
    asset = 'BTC',
    focusVenue = '',
    metricFilter = '',
    onLargeUsd = () => {},
    onDepth = null,
    onSpikeThreshold = () => {},
    onChipClick = () => {},
    onMetricClick = () => {},
    onSection = () => {},
    onToggle = null,
  } = $props();

  let pressure = $derived(bookPressure(book, depth));
  let cvd = $derived(computeCvd(tape, { windowSec }));
  let vap = $derived(volumeAtPrice(tape, { windowSec, maxBuckets: 24 }));
  let heuristics = $derived(detectFlowHeuristics(tape, book, { largeUsd, windowSec }));

  let imbSpark = $derived(sparkPath(imbalanceHistory.map((p) => p.imbalancePct), { w: 100, h: 22 }));
  let cvdSpark = $derived(sparkPath(cvd.points.map((p) => p.cvd), { w: 120, h: 24 }));
  let scoreSpark = $derived(sparkPath((history || []).map((p) => p.score), { w: 140, h: 28 }));

  let histMax = $derived(
    Math.max(1, ...cvd.histogram.map((h) => Math.max(h.buyUsd, h.sellUsd))),
  );

  let bookVap = $derived.by(() => {
    if (vap.length) return [];
    const bids = book?.bids || [];
    const asks = book?.asks || [];
    /** @type {Array<{ price: number, buyUsd: number, sellUsd: number, delta: number }>} */
    const rows = [];
    for (const l of bids.slice(0, Math.min(depth, 12))) {
      const price = Number(l.price);
      const qty = Number(l.quantity) || 0;
      if (!Number.isFinite(price) || qty <= 0) continue;
      const usd = price * qty;
      rows.push({ price, buyUsd: usd, sellUsd: 0, delta: usd });
    }
    for (const l of asks.slice(0, Math.min(depth, 12))) {
      const price = Number(l.price);
      const qty = Number(l.quantity) || 0;
      if (!Number.isFinite(price) || qty <= 0) continue;
      const usd = price * qty;
      rows.push({ price, buyUsd: 0, sellUsd: usd, delta: -usd });
    }
    return rows.sort((a, b) => b.price - a.price).slice(0, 18);
  });
  let vapRows = $derived(vap.length ? vap.slice(0, 18) : bookVap);
  let vapMaxEff = $derived(
    Math.max(1, ...vapRows.map((r) => Math.max(r.buyUsd || 0, r.sellUsd || 0))),
  );

  let largeTrades = $derived(
    (tape || [])
      .filter((e) => e.kind === 'trade')
      .filter((e) => (tradeNotional(e) ?? 0) >= largeUsd)
      .slice(0, 6),
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

  function barW(v, max) {
    return `${Math.min(100, (v / max) * 100)}%`;
  }

  function metricActive(id) {
    return metricFilter === id;
  }

  let showFlow = $derived(section === 'both' || section === 'flow');
  let showPulse = $derived(section === 'both' || section === 'pulse');
  let flowWide = $derived(section === 'flow');
  let pulseWide = $derived(section === 'pulse');
</script>

<section class="fp" class:alert={alertActive} aria-label="Flow and Pulse dock">
  <div class="fp-chrome">
    <div class="chrome-left">
      <span class="brand">Flow &amp; Pulse</span>
      <span class="asset">{asset}</span>
      {#if alertActive}
        <span class="spike-badge">SPIKE</span>
      {/if}
      {#if metricFilter}
        <button type="button" class="filter-clear" onclick={() => onMetricClick(metricFilter)}>
          {metricFilter} ✕
        </button>
      {/if}
    </div>
    <div class="chrome-tabs" role="tablist" aria-label="Dock section">
      <button
        type="button"
        role="tab"
        class:active={section === 'flow'}
        aria-selected={section === 'flow'}
        title="Flow section (F)"
        onclick={() => onSection('flow')}
      >Flow</button>
      <button
        type="button"
        role="tab"
        class:active={section === 'both'}
        aria-selected={section === 'both'}
        title="Unified view"
        onclick={() => onSection('both')}
      >Both</button>
      <button
        type="button"
        role="tab"
        class:active={section === 'pulse'}
        aria-selected={section === 'pulse'}
        title="Pulse section (P)"
        onclick={() => onSection('pulse')}
      >Pulse</button>
    </div>
    <div class="chrome-right">
      <label class="thresh" title="Large trade threshold (USD)">
        ≥$
        <input
          type="number"
          min="0"
          step="1000"
          value={largeUsd}
          onchange={(e) => onLargeUsd(Number(e.currentTarget.value))}
        />
      </label>
      <label class="thresh" title="Pulse spike alert (0–100)">
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
      {#if onToggle}
        <button type="button" class="hide-btn" onclick={onToggle} title="Hide dock (Esc)">▾</button>
      {/if}
    </div>
  </div>

  <div
    class="fp-body"
    class:both={section === 'both'}
    class:flow-only={section === 'flow'}
    class:pulse-only={section === 'pulse'}
  >
    {#if showFlow}
      <div class="col flow-col" class:wide={flowWide}>
        <div class="col-head">
          <span class="col-title">Depth / CVD</span>
          <span class="meta">{Math.round(windowSec / 60)}m · {vap.length ? 'tape VAP' : 'book profile'}</span>
        </div>

        <div class="stat-row">
          <div class="stat">
            <span class="lbl">CVD</span>
            <span class="val" class:up={cvd.cvd > 0} class:down={cvd.cvd < 0}>{fmtUsd(cvd.cvd)}</span>
          </div>
          <div class="stat">
            <span class="lbl">Buys</span>
            <span class="val bid">{fmtUsd(cvd.buyUsd)}</span>
          </div>
          <div class="stat">
            <span class="lbl">Sells</span>
            <span class="val ask">{fmtUsd(cvd.sellUsd)}</span>
          </div>
          <div class="stat">
            <span class="lbl">Trades</span>
            <span class="val">{cvd.trades}</span>
          </div>
          <div class="stat">
            <span class="lbl">Imb</span>
            <span class="val accent">{pressure.imbalancePct.toFixed(1)}%</span>
          </div>
          <div class="stat">
            <span class="lbl">Last</span>
            <span class="val">{lastPrice != null ? fmtPrice(lastPrice, 2) : '—'}</span>
          </div>
        </div>

        <div class="pressure">
          <div class="bar">
            <div class="bid" style={`width:${pressure.bidPct}%`}></div>
            <div class="ask" style={`width:${pressure.askPct}%`}></div>
          </div>
          <div class="plabels">
            <span class="bid">Bid {fmtUsd(pressure.bidUsd)}</span>
            <span class="ask">Ask {fmtUsd(pressure.askUsd)}</span>
          </div>
        </div>

        <div class="sparks">
          <div class="spark" title="Depth imbalance">
            {#if imbSpark}
              <svg viewBox="0 0 100 22" preserveAspectRatio="none">
                <path d={imbSpark} fill="none" stroke="var(--accent)" stroke-width="1.2" />
              </svg>
            {:else}
              <span class="muted">imb…</span>
            {/if}
          </div>
          <div class="spark" title="CVD">
            {#if cvdSpark}
              <svg viewBox="0 0 120 24" preserveAspectRatio="none">
                <path d={cvdSpark} fill="none" stroke={cvd.cvd >= 0 ? 'var(--bid)' : 'var(--ask)'} stroke-width="1.3" />
              </svg>
            {:else}
              <span class="muted">cvd…</span>
            {/if}
          </div>
        </div>

        <div class="hist" aria-label="Buy vs sell histogram">
          {#each cvd.histogram.slice(-36) as h}
            <div class="hcol">
              <div class="buy" style={`height:${Math.max(2, (h.buyUsd / histMax) * 100)}%`}></div>
              <div class="sell" style={`height:${Math.max(2, (h.sellUsd / histMax) * 100)}%`}></div>
            </div>
          {:else}
            <span class="muted">buy/sell hist…</span>
          {/each}
        </div>

        {#if showLadder && flowWide}
          <div class="ladder-block">
            <DepthChart {book} {depth} />
            <div class="dom-wrap">
              <DomLadder {book} {depth} {tickOpt} {lastPrice} onDepth={onDepth} showCum={true} />
            </div>
          </div>
        {:else if showLadder && section === 'both'}
          <div class="dom-wrap slim">
            <DomLadder {book} depth={Math.min(depth, 16)} {tickOpt} {lastPrice} onDepth={onDepth} showCum={false} />
          </div>
        {/if}

        <div class="vap-mini" aria-label="Volume at price">
          <div class="vap-cols"><span>Sell</span><span>Px</span><span>Buy</span><span>Δ</span></div>
          <div class="vap">
            {#each vapRows as row}
              <div class="vrow">
                <div class="sell-bar-wrap">
                  <div class="sell-bar" style={`width:${barW(row.sellUsd, vapMaxEff)}`}></div>
                  <span>{fmtUsd(row.sellUsd)}</span>
                </div>
                <span class="px">{fmtPrice(row.price, 2)}</span>
                <div class="buy-bar-wrap">
                  <div class="buy-bar" style={`width:${barW(row.buyUsd, vapMaxEff)}`}></div>
                  <span>{fmtUsd(row.buyUsd)}</span>
                </div>
                <span class="delta" class:up={row.delta > 0} class:down={row.delta < 0}>{fmtUsd(row.delta)}</span>
              </div>
            {:else}
              <div class="empty">VAP…</div>
            {/each}
          </div>
        </div>
      </div>
    {/if}

    {#if showPulse}
      <div class="col pulse-col" class:wide={pulseWide}>
        <div class="col-head">
          <span class="col-title">Multi-venue heat</span>
          <span class="meta">{pulse?.venueCount ?? 0} live · click chip → focus</span>
        </div>

        <div class="pulse-metrics">
          <button type="button" class="metric score" class:active={metricActive('heat')} onclick={() => onMetricClick('heat')} title="Sort by heat">
            <span class="lbl">Score</span>
            <span class="val big">{score != null ? score.toFixed(0) : '—'}</span>
          </button>
          <button type="button" class="metric" class:active={metricActive('tpm')} onclick={() => onMetricClick('tpm')} title="Sort by trades/min">
            <span class="lbl">Trades/m</span>
            <span class="val">{pulse?.tradesPerMin != null ? pulse.tradesPerMin.toFixed(1) : '—'}</span>
          </button>
          <button type="button" class="metric" class:active={metricActive('usd')} onclick={() => onMetricClick('usd')} title="Sort by USD/min">
            <span class="lbl">USD/m</span>
            <span class="val">{pulse?.usdPerMin != null ? fmtUsd(pulse.usdPerMin) : '—'}</span>
          </button>
          <button type="button" class="metric" class:active={metricActive('cross')} onclick={() => onMetricClick('cross')} title="Cross-venue Δ">
            <span class="lbl">Cross Δ</span>
            <span class="val" class:hot={pulse?.crossBps != null && pulse.crossBps > 10}>
              {pulse?.crossBps != null ? pulse.crossBps.toFixed(1) + 'b' : '—'}
            </span>
          </button>
          <button type="button" class="metric" class:active={metricActive('spread')} onclick={() => onMetricClick('spread')} title="Median spread">
            <span class="lbl">Spread</span>
            <span class="val">
              {pulse?.medianSpread != null ? pulse.medianSpread.toFixed(2) + 'b' : '—'}
            </span>
          </button>
          <button type="button" class="metric" class:active={metricActive('imb')} onclick={() => onMetricClick('imb')} title="Book imbalance">
            <span class="lbl">Book imb</span>
            <span class="val" class:up={pulse?.bookImbalance > 5} class:down={pulse?.bookImbalance < -5}>
              {pulse?.bookImbalance != null ? pulse.bookImbalance.toFixed(1) + '%' : '—'}
            </span>
          </button>
        </div>

        <div class="spark pulse-spark" title="Pulse score history">
          {#if scoreSpark}
            <svg viewBox="0 0 140 28" preserveAspectRatio="none">
              <line
                x1="0"
                y1={28 - (spikeThreshold / 100) * 26 - 1}
                x2="140"
                y2={28 - (spikeThreshold / 100) * 26 - 1}
                stroke="rgba(246,70,93,0.45)"
                stroke-dasharray="2,2"
                stroke-width="0.6"
              />
              <path d={scoreSpark} fill="none" stroke="var(--accent)" stroke-width="1.4" />
            </svg>
          {:else}
            <span class="muted">pulse history…</span>
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
              title="{c.venue} · heat {c.heat.toFixed(0)} · {c.tradesPerMin?.toFixed?.(1) ?? '—'} tpm · {c.usdPerMin != null ? fmtUsd(c.usdPerMin) : '—'}/m"
              onclick={() => onChipClick(c.venue, c.symbol)}
            >
              <span class="heat-bar" style={`width:var(--heatpct)%`}></span>
              <span class="vname">{c.venue}</span>
              <span class="vheat">{c.heat.toFixed(0)}</span>
            </button>
          {:else}
            <div class="empty">waiting for multi-venue tape/books…</div>
          {/each}
        </div>
      </div>
    {/if}

    {#if section === 'both' || section === 'flow'}
      <div class="col alerts-col">
        <div class="col-head">
          <span class="col-title">Flags</span>
          <span class="meta">≥{fmtUsd(largeUsd)}</span>
        </div>
        <div class="heuristics">
          {#each heuristics.slice(-8) as h}
            <span class="badge" class:buy={h.side === 'buy'} class:sell={h.side === 'sell'} title={h.label}>
              {h.kind}
            </span>
          {:else}
            <span class="muted">no sweep/absorption</span>
          {/each}
        </div>
        <div class="large-list">
          {#each largeTrades as e}
            <div class="lt" class:buy={e.aggressor === 'buy'} class:sell={e.aggressor === 'sell'}>
              <span>{fmtPrice(e.price, 2)}</span>
              <span>{fmtUsd(tradeNotional(e))}</span>
              <span>{e.aggressor || '?'}</span>
            </div>
          {:else}
            <div class="empty">no large prints</div>
          {/each}
        </div>
      </div>
    {/if}
  </div>
</section>

<style>
  .fp {
    height: 100%;
    min-height: 0;
    display: flex;
    flex-direction: column;
    background: var(--panel);
    overflow: hidden;
  }
  .fp.alert {
    box-shadow: inset 0 0 0 1px rgba(246, 70, 93, 0.4);
  }

  .fp-chrome {
    display: flex;
    align-items: center;
    gap: 0.55rem;
    padding: 0.2rem 0.5rem;
    border-bottom: 1px solid var(--border);
    background: linear-gradient(180deg, rgba(30, 35, 41, 0.95), var(--panel-2));
    flex-shrink: 0;
  }
  .chrome-left {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    min-width: 0;
  }
  .brand {
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.02em;
    color: var(--text);
  }
  .asset {
    font-family: var(--mono);
    font-size: 0.62rem;
    color: var(--accent);
    padding: 0.05rem 0.3rem;
    border: 1px solid rgba(240, 185, 11, 0.3);
    border-radius: 2px;
  }
  .spike-badge {
    font-family: var(--mono);
    font-size: 0.55rem;
    font-weight: 700;
    color: var(--ask);
    border: 1px solid rgba(246, 70, 93, 0.5);
    padding: 0.05rem 0.3rem;
    border-radius: 2px;
    animation: blink 1.2s ease-in-out infinite;
  }
  @keyframes blink {
    50% { opacity: 0.5; }
  }
  .filter-clear {
    font-family: var(--mono);
    font-size: 0.55rem;
    color: var(--accent);
    background: rgba(240, 185, 11, 0.08);
    border: 1px solid rgba(240, 185, 11, 0.35);
    padding: 0.05rem 0.3rem;
    cursor: pointer;
    border-radius: 2px;
  }
  .chrome-tabs {
    display: flex;
    gap: 0.15rem;
    margin-left: 0.25rem;
  }
  .chrome-tabs button {
    background: transparent;
    border: 1px solid transparent;
    color: var(--muted);
    font-family: var(--mono);
    font-size: 0.62rem;
    padding: 0.12rem 0.4rem;
    cursor: pointer;
    border-radius: 2px;
  }
  .chrome-tabs button:hover { color: var(--text); background: var(--panel); }
  .chrome-tabs button.active {
    color: var(--accent);
    border-color: rgba(240, 185, 11, 0.4);
    background: rgba(240, 185, 11, 0.06);
  }
  .chrome-right {
    margin-left: auto;
    display: flex;
    align-items: center;
    gap: 0.55rem;
  }
  .thresh {
    font-size: 0.58rem;
    color: var(--muted);
    font-family: var(--mono);
    display: flex;
    align-items: center;
    gap: 0.15rem;
  }
  .thresh input {
    width: 3.6rem;
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 2px;
    padding: 0.08rem 0.2rem;
    font-family: var(--mono);
    font-size: 0.58rem;
    color: var(--text);
  }
  .hide-btn {
    background: transparent;
    border: 1px solid transparent;
    color: var(--muted);
    font-size: 0.72rem;
    padding: 0.05rem 0.35rem;
    cursor: pointer;
    border-radius: 2px;
  }
  .hide-btn:hover { color: var(--text); background: var(--panel); }

  .fp-body {
    flex: 1;
    min-height: 0;
    display: grid;
    gap: 0;
  }
  .fp-body.both {
    /* Balanced Flow | Pulse heat | Alerts — less empty heat whitespace */
    grid-template-columns: minmax(280px, 1.2fr) minmax(320px, 1.35fr) minmax(150px, 0.7fr);
  }
  .fp-body.flow-only {
    grid-template-columns: minmax(0, 1.6fr) minmax(140px, 0.55fr);
  }
  .fp-body.pulse-only {
    grid-template-columns: 1fr;
  }

  .col {
    display: flex;
    flex-direction: column;
    min-width: 0;
    min-height: 0;
    padding: 0.3rem 0.45rem;
    border-right: 1px solid var(--border);
  }
  .col:last-child { border-right: none; }
  .col-head {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    gap: 0.35rem;
    margin-bottom: 0.2rem;
    flex-shrink: 0;
  }
  .col-title {
    font-size: 0.62rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim, var(--muted));
  }
  .meta, .muted {
    color: var(--muted);
    font-size: 0.55rem;
    font-family: var(--mono);
  }

  .stat-row {
    display: flex;
    flex-wrap: wrap;
    gap: 0.35rem 0.65rem;
    margin-bottom: 0.25rem;
    flex-shrink: 0;
  }
  .stat { display: flex; flex-direction: column; gap: 0.02rem; }
  .stat .lbl {
    font-size: 0.5rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.03em;
  }
  .stat .val {
    font-family: var(--mono);
    font-size: 0.78rem;
    font-weight: 600;
  }
  .stat .val.up, .delta.up { color: var(--bid); }
  .stat .val.down, .delta.down { color: var(--ask); }
  .stat .val.bid { color: var(--bid); }
  .stat .val.ask { color: var(--ask); }
  .stat .val.accent { color: var(--accent); }

  .pressure { margin-bottom: 0.2rem; flex-shrink: 0; }
  .pressure .bar { display: flex; height: 6px; border-radius: 1px; overflow: hidden; }
  .pressure .bid { background: rgba(2, 192, 118, 0.55); }
  .pressure .ask { background: rgba(246, 70, 93, 0.55); }
  .plabels {
    display: flex;
    justify-content: space-between;
    font-family: var(--mono);
    font-size: 0.52rem;
    margin-top: 0.1rem;
  }
  .plabels .bid { color: var(--bid); }
  .plabels .ask { color: var(--ask); }

  .sparks {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.25rem;
    margin-bottom: 0.2rem;
    flex-shrink: 0;
  }
  .spark {
    height: 24px;
    background: var(--panel-2);
    border: 1px solid var(--border);
  }
  .spark svg { width: 100%; height: 100%; display: block; }
  .pulse-spark { height: 28px; margin-bottom: 0.25rem; flex-shrink: 0; }

  .hist {
    display: flex;
    align-items: flex-end;
    gap: 1px;
    height: 36px;
    background: var(--panel-2);
    border: 1px solid var(--border);
    padding: 2px;
    margin-bottom: 0.25rem;
    flex-shrink: 0;
  }
  .hcol {
    flex: 1 1 0;
    display: flex;
    flex-direction: column-reverse;
    min-width: 2px;
    max-width: 10px;
    height: 100%;
  }
  .hcol .buy { background: rgba(2, 192, 118, 0.65); width: 100%; }
  .hcol .sell { background: rgba(246, 70, 93, 0.65); width: 100%; }

  .ladder-block {
    display: grid;
    grid-template-rows: auto minmax(80px, 1fr);
    gap: 0.2rem;
    min-height: 0;
    flex: 1;
  }
  .dom-wrap {
    flex: 1;
    min-height: 72px;
    overflow: hidden;
    border: 1px solid var(--border);
    background: var(--panel-2);
    margin-bottom: 0.2rem;
  }
  .dom-wrap.slim { max-height: 120px; flex: 0 1 120px; }

  .vap-mini {
    flex: 1;
    min-height: 0;
    display: flex;
    flex-direction: column;
  }
  .flow-col.wide .vap-mini { flex: 0.85; }
  .vap-cols {
    display: grid;
    grid-template-columns: 1fr 0.65fr 1fr 0.6fr;
    font-size: 0.5rem;
    color: var(--muted);
    text-transform: uppercase;
    flex-shrink: 0;
  }
  .vap {
    overflow: auto;
    flex: 1;
    min-height: 0;
    font-family: var(--mono);
    font-size: 0.58rem;
  }
  .vrow {
    display: grid;
    grid-template-columns: 1fr 0.65fr 1fr 0.6fr;
    align-items: center;
    gap: 0.15rem;
    padding: 0.02rem 0;
  }
  .sell-bar-wrap, .buy-bar-wrap {
    position: relative;
    height: 12px;
    display: flex;
    align-items: center;
  }
  .sell-bar-wrap { justify-content: flex-end; }
  .sell-bar, .buy-bar {
    position: absolute;
    top: 0; bottom: 0;
    opacity: 0.32;
  }
  .sell-bar { right: 0; background: var(--ask); }
  .buy-bar { left: 0; background: var(--bid); }
  .sell-bar-wrap span, .buy-bar-wrap span {
    position: relative;
    z-index: 1;
    font-size: 0.52rem;
  }
  .vrow .px { text-align: center; font-weight: 600; }
  .vrow .delta { text-align: right; font-size: 0.52rem; }

  .pulse-metrics {
    display: grid;
    grid-template-columns: repeat(6, minmax(0, 1fr));
    gap: 0.25rem;
    margin-bottom: 0.25rem;
    flex-shrink: 0;
  }
  .metric {
    display: flex;
    flex-direction: column;
    gap: 0.02rem;
    align-items: flex-start;
    background: var(--panel-2);
    border: 1px solid var(--border);
    border-radius: 2px;
    padding: 0.2rem 0.3rem;
    cursor: pointer;
    color: inherit;
    text-align: left;
  }
  .metric:hover { border-color: rgba(240, 185, 11, 0.35); }
  .metric.active {
    border-color: rgba(240, 185, 11, 0.55);
    box-shadow: inset 0 0 0 1px rgba(240, 185, 11, 0.15);
  }
  .metric .lbl {
    font-size: 0.48rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.03em;
  }
  .metric .val {
    font-family: var(--mono);
    font-size: 0.78rem;
    font-weight: 600;
  }
  .metric .val.big { font-size: 1.05rem; color: var(--accent); }
  .metric .val.hot { color: var(--ask); }
  .metric .val.up { color: var(--bid); }
  .metric .val.down { color: var(--ask); }

  .chips {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(6.8rem, 1fr));
    gap: 0.22rem;
    flex: 1;
    align-content: start;
    justify-content: stretch;
    min-height: 0;
    overflow: auto;
    padding-bottom: 0.15rem;
  }
  .fp-body.both .pulse-col {
    display: flex;
    flex-direction: column;
    min-height: 0;
  }
  .fp-body.both .chips {
    grid-template-columns: repeat(auto-fill, minmax(6.4rem, 1fr));
  }
  .chip {
    position: relative;
    display: flex;
    align-items: center;
    gap: 0.3rem;
    padding: 0.28rem 0.4rem;
    background: var(--panel-2);
    border: 1px solid var(--border);
    border-radius: 2px;
    cursor: pointer;
    overflow: hidden;
    min-height: 1.7rem;
  }
  .chip:hover { border-color: var(--vc); }
  .chip.focus {
    border-color: var(--accent);
    box-shadow: inset 0 0 0 1px rgba(240, 185, 11, 0.25);
  }
  .chip.spike { animation: chipSpike 0.9s ease-out 1; }
  @keyframes chipSpike {
    0% { filter: brightness(1.35); }
    100% { filter: brightness(1); }
  }
  .chip.offline { opacity: 0.42; }
  .heat-bar {
    position: absolute;
    left: 0; top: 0; bottom: 0;
    background: color-mix(in srgb, var(--vc) 30%, transparent);
    pointer-events: none;
  }
  .vname {
    position: relative;
    font-family: var(--mono);
    font-size: 0.62rem;
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

  .alerts-col { background: var(--panel-2); }
  .heuristics {
    display: flex;
    flex-wrap: wrap;
    gap: 0.2rem;
    margin-bottom: 0.3rem;
    flex-shrink: 0;
  }
  .badge {
    font-family: var(--mono);
    font-size: 0.52rem;
    padding: 0.04rem 0.25rem;
    border: 1px solid var(--border);
    border-radius: 2px;
    background: var(--panel);
    color: var(--text-dim, var(--muted));
  }
  .badge.buy { border-color: rgba(2, 192, 118, 0.4); color: var(--bid); }
  .badge.sell { border-color: rgba(246, 70, 93, 0.4); color: var(--ask); }
  .large-list {
    overflow: auto;
    flex: 1;
    min-height: 0;
    font-family: var(--mono);
    font-size: 0.58rem;
  }
  .lt {
    display: grid;
    grid-template-columns: 1fr 1fr 0.45fr;
    padding: 0.04rem 0;
    color: var(--text-dim, var(--muted));
  }
  .lt.buy { color: var(--bid); }
  .lt.sell { color: var(--ask); }
  .empty {
    color: var(--muted);
    font-family: var(--mono);
    font-size: 0.58rem;
    padding: 0.35rem 0;
  }

  @media (max-width: 1100px) {
    .fp-body.both {
      grid-template-columns: 1fr 1fr;
      grid-template-rows: minmax(0, 1.1fr) minmax(0, 0.9fr);
    }
    .fp-body.both .alerts-col { grid-column: 1 / -1; max-height: 90px; }
    .pulse-metrics { grid-template-columns: repeat(3, 1fr); }
    .chrome-right .thresh:first-child { display: none; }
  }
  @media (max-width: 720px) {
    .fp-body.both,
    .fp-body.flow-only {
      grid-template-columns: 1fr;
    }
    .fp-body.both .alerts-col { grid-column: auto; }
    .chrome-tabs { display: none; }
  }
</style>
