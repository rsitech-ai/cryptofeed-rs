<script>
  import { fmtPrice, fmtUsd } from '../lib/format.js';
  import {
    bookPressure,
    computeCvd,
    detectFlowHeuristics,
    tradeNotional,
    volumeAtPrice,
  } from '../lib/orderflow.js';

  let {
    book = null,
    tape = [],
    depth = 16,
    windowSec = 300,
    largeUsd = 25000,
    /**
     * When false (Order Flow chart mode), skip full VAP —
     * the heatmap already owns VAP sidebar. Dock keeps CVD/Imb metrics + chips.
     * Sparks / buy-sell hist live under the main chart (ChartAnalyticsStrip).
     */
    showTapeProfile = true,
    pulse = null,
    alertActive = false,
    spikeThreshold = 72,
    asset = 'BTC',
    focusVenue = '',
    metricFilter = '',
    onLargeUsd = () => {},
    onSpikeThreshold = () => {},
    onChipClick = () => {},
    onMetricClick = () => {},
    onToggle = null,
  } = $props();

  let tip = $state(/** @type {string} */ (''));
  let tipX = $state(0);
  let tipY = $state(0);

  let pressure = $derived(bookPressure(book, depth));
  let cvd = $derived(computeCvd(tape, { windowSec }));
  let vap = $derived(volumeAtPrice(tape, { windowSec, maxBuckets: 24 }));
  let heuristics = $derived(detectFlowHeuristics(tape, book, { largeUsd, windowSec }));

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
    return rows.sort((a, b) => b.price - a.price).slice(0, 22);
  });
  let vapRows = $derived(vap.length ? vap.slice(0, 22) : bookVap);
  let vapMaxEff = $derived(
    Math.max(1, ...vapRows.map((r) => Math.max(r.buyUsd || 0, r.sellUsd || 0))),
  );

  let largeTrades = $derived(
    (tape || [])
      .filter((e) => e.kind === 'trade')
      .filter((e) => (tradeNotional(e) ?? 0) >= largeUsd)
      .slice(0, 24),
  );
  /** When no prints hit the large threshold, fill Flags with top notional (not a full tape copy). */
  let topPrints = $derived(
    (tape || [])
      .filter((e) => e.kind === 'trade')
      .map((e) => ({ e, n: tradeNotional(e) ?? 0 }))
      .filter((x) => x.n > 0)
      .sort((a, b) => b.n - a.n)
      .slice(0, 18),
  );
  let flagRows = $derived(
    largeTrades.length
      ? largeTrades.map((e) => ({ e, n: tradeNotional(e) ?? 0, large: true }))
      : topPrints.map((x) => ({ ...x, large: false })),
  );
  let flagsAreLarge = $derived(largeTrades.length > 0);

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

  /** @param {MouseEvent} e @param {string} text */
  function showTip(e, text) {
    tip = text;
    tipX = e.clientX + 12;
    tipY = e.clientY + 12;
  }

  function hideTip() {
    tip = '';
  }
</script>

<section class="fp" class:alert={alertActive} class:compact={!showTapeProfile} aria-label="Flow and Pulse dock">
  <div class="fp-chrome">
    <div class="chrome-left">
      <span class="brand">Flow &amp; Pulse</span>
      <span class="asset">{asset}</span>
      {#if alertActive}
        <span class="spike-badge">SPIKE</span>
      {/if}
      {#if metricFilter}
        <button type="button" class="filter-clear" onclick={() => onMetricClick(metricFilter)}>
          sort:{metricFilter} ✕
        </button>
      {/if}
      <span class="chrome-meta">
        {Math.round(windowSec / 60)}m
        · {showTapeProfile ? (vap.length ? 'tape VAP' : 'book profile') : 'OF owns VAP'}
      </span>
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

  <div class="fp-body">
    <!-- Col 1: Focus flow (complementary — no Last/Trades/depth; no VAP when OF) -->
    <div class="col flow-col">
      <div class="col-head">
        <span class="col-title">Focus flow</span>
        <span class="meta">{showTapeProfile ? 'CVD · profile' : 'CVD summary'}</span>
      </div>

      <div class="stat-row" role="group" aria-label="Flow stats">
        <button
          type="button"
          class="stat clickable"
          title="Cumulative volume delta"
          onmouseenter={(e) => showTip(e, `CVD ${fmtUsd(cvd.cvd)} · buys ${fmtUsd(cvd.buyUsd)} · sells ${fmtUsd(cvd.sellUsd)} · ${cvd.trades} trades`)}
          onmousemove={(e) => showTip(e, `CVD ${fmtUsd(cvd.cvd)} · buys ${fmtUsd(cvd.buyUsd)} · sells ${fmtUsd(cvd.sellUsd)} · ${cvd.trades} trades`)}
          onmouseleave={hideTip}
        >
          <span class="lbl">CVD</span>
          <span class="val" class:up={cvd.cvd > 0} class:down={cvd.cvd < 0}>{fmtUsd(cvd.cvd)}</span>
        </button>
        <div class="stat">
          <span class="lbl">Buys</span>
          <span class="val bid">{fmtUsd(cvd.buyUsd)}</span>
        </div>
        <div class="stat">
          <span class="lbl">Sells</span>
          <span class="val ask">{fmtUsd(cvd.sellUsd)}</span>
        </div>
        <div
          class="stat"
          role="img"
          aria-label="Book imbalance"
          onmouseenter={(e) => showTip(e, `Focus book imbalance ${pressure.imbalancePct.toFixed(1)}% · bid ${fmtUsd(pressure.bidUsd)} · ask ${fmtUsd(pressure.askUsd)}`)}
          onmousemove={(e) => showTip(e, `Focus book imbalance ${pressure.imbalancePct.toFixed(1)}% · bid ${fmtUsd(pressure.bidUsd)} · ask ${fmtUsd(pressure.askUsd)}`)}
          onmouseleave={hideTip}
        >
          <span class="lbl">Imb</span>
          <span class="val accent">{pressure.imbalancePct.toFixed(1)}%</span>
        </div>
      </div>

      <div
        class="pressure"
        role="img"
        aria-label="Bid ask pressure"
        onmouseenter={(e) => showTip(e, `Bid ${fmtUsd(pressure.bidUsd)} · Ask ${fmtUsd(pressure.askUsd)}`)}
        onmousemove={(e) => showTip(e, `Bid ${fmtUsd(pressure.bidUsd)} · Ask ${fmtUsd(pressure.askUsd)}`)}
        onmouseleave={hideTip}
      >
        <div class="bar">
          <div class="bid" style={`width:${pressure.bidPct}%`}></div>
          <div class="ask" style={`width:${pressure.askPct}%`}></div>
        </div>
        <div class="plabels">
          <span class="bid">Bid {fmtUsd(pressure.bidUsd)}</span>
          <span class="ask">Ask {fmtUsd(pressure.askUsd)}</span>
        </div>
      </div>

      {#if showTapeProfile}
        <div class="vap-mini" aria-label="Volume at price">
          <div class="vap-cols"><span>Sell</span><span>Px</span><span>Buy</span><span>Δ</span></div>
          <div class="vap">
            {#each vapRows as row}
              <button
                type="button"
                class="vrow"
                title={`${fmtPrice(row.price, 2)} · Δ ${fmtUsd(row.delta)}`}
                onmouseenter={(e) => showTip(e, `${fmtPrice(row.price, 2)} · buy ${fmtUsd(row.buyUsd)} · sell ${fmtUsd(row.sellUsd)} · Δ ${fmtUsd(row.delta)}`)}
                onmousemove={(e) => showTip(e, `${fmtPrice(row.price, 2)} · buy ${fmtUsd(row.buyUsd)} · sell ${fmtUsd(row.sellUsd)} · Δ ${fmtUsd(row.delta)}`)}
                onmouseleave={hideTip}
              >
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
              </button>
            {:else}
              <div class="empty">VAP…</div>
            {/each}
          </div>
        </div>
      {/if}
    </div>

    <!-- Col 2: Multi-venue heat (no Cross Δ — header owns it) -->
    <div class="col pulse-col">
      <div class="col-head">
        <span class="col-title">Multi-venue heat</span>
        <span class="meta">{pulse?.venueCount ?? 0} live · chip → focus</span>
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
        <button type="button" class="metric" class:active={metricActive('spread')} onclick={() => onMetricClick('spread')} title="Median spread across live venues">
          <span class="lbl">Med spread</span>
          <span class="val">
            {pulse?.medianSpread != null ? pulse.medianSpread.toFixed(2) + 'b' : '—'}
          </span>
        </button>
        <button type="button" class="metric" class:active={metricActive('imb')} onclick={() => onMetricClick('imb')} title="Avg book imbalance across venues">
          <span class="lbl">Avg imb</span>
          <span class="val" class:up={pulse?.bookImbalance > 5} class:down={pulse?.bookImbalance < -5}>
            {pulse?.bookImbalance != null ? pulse.bookImbalance.toFixed(1) + '%' : '—'}
          </span>
        </button>
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
            title="{c.venue} · heat {c.heat.toFixed(0)} · {c.tradesPerMin?.toFixed?.(1) ?? '—'} tpm · {c.usdPerMin != null ? fmtUsd(c.usdPerMin) : '—'}/m · imb {(c.imbalancePct ?? 0).toFixed?.(1) ?? c.imbalancePct}%"
            onclick={() => onChipClick(c.venue, c.symbol)}
            onmouseenter={(e) => showTip(e, `${c.venue} · heat ${c.heat.toFixed(0)} · ${c.tradesPerMin?.toFixed?.(1) ?? '—'} tpm · ${c.usdPerMin != null ? fmtUsd(c.usdPerMin) : '—'}/m`)}
            onmousemove={(e) => showTip(e, `${c.venue} · heat ${c.heat.toFixed(0)} · ${c.tradesPerMin?.toFixed?.(1) ?? '—'} tpm · ${c.usdPerMin != null ? fmtUsd(c.usdPerMin) : '—'}/m`)}
            onmouseleave={hideTip}
          >
            <span class="heat-bar" style={`width:var(--heatpct)%`}></span>
            <span class="vname">{c.venue}</span>
            <span class="vheat">{c.heat.toFixed(0)}</span>
            <span class="vmeta">{c.tradesPerMin != null ? c.tradesPerMin.toFixed(0) + '/m' : ''}</span>
          </button>
        {:else}
          <div class="empty">waiting for multi-venue tape/books…</div>
        {/each}
      </div>
    </div>

    <!-- Col 3: Flags / large or top prints -->
    <div class="col alerts-col">
      <div class="col-head">
        <span class="col-title">{flagsAreLarge ? 'Large' : 'Top prints'}</span>
        <span class="meta">{flagsAreLarge ? `≥${fmtUsd(largeUsd)}` : 'by USD'} · {flagRows.length}</span>
      </div>
      <div class="heuristics">
        {#each heuristics.slice(-12) as h}
          <span class="badge" class:buy={h.side === 'buy'} class:sell={h.side === 'sell'} title={h.label}>
            {h.kind}
          </span>
        {:else}
          <span class="muted">no sweep/absorption</span>
        {/each}
      </div>
      <div class="large-head">
        <span>Px</span><span>USD</span><span>Side</span>
      </div>
      <div class="large-list" aria-label={flagsAreLarge ? 'Large prints' : 'Top prints by notional'}>
        {#each flagRows as row}
          <div
            class="lt"
            class:buy={row.e.aggressor === 'buy'}
            class:sell={row.e.aggressor === 'sell'}
            class:dim={!row.large}
            title={`${fmtPrice(row.e.price, 2)} · ${fmtUsd(row.n)} · ${row.e.aggressor || '?'}`}
          >
            <span>{fmtPrice(row.e.price, 2)}</span>
            <span>{fmtUsd(row.n)}</span>
            <span>{row.e.aggressor || '?'}</span>
          </div>
        {:else}
          <div class="empty">no prints</div>
        {/each}
      </div>
    </div>
  </div>

  {#if tip}
    <div class="fp-tip" style={`left:${tipX}px;top:${tipY}px`} role="tooltip">{tip}</div>
  {/if}
</section>

<style>
  .fp {
    height: 100%;
    min-height: 0;
    display: flex;
    flex-direction: column;
    background: var(--panel);
    overflow: hidden;
    position: relative;
  }
  .fp.alert {
    box-shadow: inset 0 0 0 1px rgba(246, 70, 93, 0.4);
  }

  .fp-chrome {
    display: flex;
    align-items: center;
    gap: 0.45rem;
    padding: 0.16rem 0.45rem;
    border-bottom: 1px solid var(--border);
    background: linear-gradient(180deg, rgba(30, 35, 41, 0.95), var(--panel-2));
    flex-shrink: 0;
  }
  .chrome-left {
    display: flex;
    align-items: center;
    gap: 0.35rem;
    min-width: 0;
    flex-wrap: wrap;
  }
  .brand {
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 0.02em;
    color: var(--text);
  }
  .asset {
    font-family: var(--mono);
    font-size: 0.6rem;
    color: var(--accent);
    padding: 0.02rem 0.28rem;
    border: 1px solid rgba(240, 185, 11, 0.3);
    border-radius: 2px;
  }
  .chrome-meta {
    font-family: var(--mono);
    font-size: 0.5rem;
    color: var(--muted);
  }
  .spike-badge {
    font-family: var(--mono);
    font-size: 0.52rem;
    font-weight: 700;
    color: var(--ask);
    border: 1px solid rgba(246, 70, 93, 0.5);
    padding: 0.02rem 0.28rem;
    border-radius: 2px;
    animation: blink 1.2s ease-in-out infinite;
  }
  @keyframes blink {
    50% { opacity: 0.5; }
  }
  .filter-clear {
    font-family: var(--mono);
    font-size: 0.52rem;
    color: var(--accent);
    background: rgba(240, 185, 11, 0.08);
    border: 1px solid rgba(240, 185, 11, 0.35);
    padding: 0.02rem 0.28rem;
    cursor: pointer;
    border-radius: 2px;
  }
  .chrome-right {
    margin-left: auto;
    display: flex;
    align-items: center;
    gap: 0.45rem;
    flex-shrink: 0;
  }
  .thresh {
    font-size: 0.55rem;
    color: var(--muted);
    font-family: var(--mono);
    display: flex;
    align-items: center;
    gap: 0.12rem;
  }
  .thresh input {
    width: 3.4rem;
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 2px;
    padding: 0.06rem 0.18rem;
    font-family: var(--mono);
    font-size: 0.55rem;
    color: var(--text);
  }
  .hide-btn {
    background: transparent;
    border: 1px solid transparent;
    color: var(--muted);
    font-size: 0.7rem;
    padding: 0.02rem 0.3rem;
    cursor: pointer;
    border-radius: 2px;
  }
  .hide-btn:hover { color: var(--text); background: var(--panel); }

  .fp-body {
    flex: 1;
    min-height: 0;
    display: grid;
    grid-template-columns: minmax(240px, 1.1fr) minmax(300px, 1.4fr) minmax(132px, 0.52fr);
    gap: 0;
    align-items: stretch;
  }
  .fp.compact .fp-body {
    grid-template-columns: minmax(168px, 0.58fr) minmax(360px, 1.7fr) minmax(140px, 0.58fr);
  }

  .col {
    display: flex;
    flex-direction: column;
    min-width: 0;
    min-height: 0;
    padding: 0.28rem 0.4rem;
    border-right: 1px solid var(--border);
  }
  .col:last-child { border-right: none; }
  .col-head {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    gap: 0.3rem;
    margin-bottom: 0.16rem;
    flex-shrink: 0;
  }
  .col-title {
    font-size: 0.58rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim, var(--muted));
  }
  .meta, .muted {
    color: var(--muted);
    font-size: 0.52rem;
    font-family: var(--mono);
  }

  .stat-row {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 0.2rem 0.35rem;
    margin-bottom: 0.2rem;
    flex-shrink: 0;
  }
  .stat {
    display: flex;
    flex-direction: column;
    gap: 0;
    background: transparent;
    border: none;
    padding: 0;
    color: inherit;
    text-align: left;
    min-width: 0;
  }
  .stat.clickable { cursor: help; }
  .stat .lbl {
    font-size: 0.48rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.03em;
  }
  .stat .val {
    font-family: var(--mono);
    font-size: 0.74rem;
    font-weight: 600;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .stat .val.up, .delta.up { color: var(--bid); }
  .stat .val.down, .delta.down { color: var(--ask); }
  .stat .val.bid { color: var(--bid); }
  .stat .val.ask { color: var(--ask); }
  .stat .val.accent { color: var(--accent); }

  .pressure { margin-bottom: 0.18rem; flex-shrink: 0; cursor: help; }
  .pressure .bar { display: flex; height: 6px; border-radius: 1px; overflow: hidden; }
  .pressure .bid { background: rgba(2, 192, 118, 0.55); }
  .pressure .ask { background: rgba(246, 70, 93, 0.55); }
  .plabels {
    display: flex;
    justify-content: space-between;
    font-family: var(--mono);
    font-size: 0.5rem;
    margin-top: 0.06rem;
  }
  .plabels .bid { color: var(--bid); }
  .plabels .ask { color: var(--ask); }

  .vap-mini {
    flex: 1;
    min-height: 0;
    display: flex;
    flex-direction: column;
  }
  .vap-cols {
    display: grid;
    grid-template-columns: 1fr 0.65fr 1fr 0.55fr;
    font-size: 0.48rem;
    color: var(--muted);
    text-transform: uppercase;
    flex-shrink: 0;
    padding-bottom: 0.04rem;
  }
  .vap {
    overflow: auto;
    flex: 1;
    min-height: 0;
    font-family: var(--mono);
    font-size: 0.56rem;
  }
  .vrow {
    display: grid;
    grid-template-columns: 1fr 0.65fr 1fr 0.55fr;
    align-items: center;
    gap: 0.12rem;
    padding: 0.01rem 0;
    width: 100%;
    border: none;
    background: transparent;
    color: inherit;
    font: inherit;
    cursor: help;
    text-align: left;
  }
  .vrow:hover { background: rgba(240, 185, 11, 0.04); }
  .sell-bar-wrap, .buy-bar-wrap {
    position: relative;
    height: 11px;
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
    font-size: 0.5rem;
  }
  .vrow .px { text-align: center; font-weight: 600; }
  .vrow .delta { text-align: right; font-size: 0.5rem; }

  .pulse-metrics {
    display: grid;
    grid-template-columns: repeat(5, minmax(0, 1fr));
    gap: 0.18rem;
    margin-bottom: 0.18rem;
    flex-shrink: 0;
  }
  .metric {
    display: flex;
    flex-direction: column;
    gap: 0;
    align-items: flex-start;
    background: var(--panel-2);
    border: 1px solid var(--border);
    border-radius: 2px;
    padding: 0.16rem 0.22rem;
    cursor: pointer;
    color: inherit;
    text-align: left;
    min-height: 2.15rem;
    min-width: 0;
  }
  .metric:hover { border-color: rgba(240, 185, 11, 0.35); }
  .metric.active {
    border-color: rgba(240, 185, 11, 0.55);
    box-shadow: inset 0 0 0 1px rgba(240, 185, 11, 0.15);
  }
  .metric .lbl {
    font-size: 0.46rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.03em;
  }
  .metric .val {
    font-family: var(--mono);
    font-size: 0.72rem;
    font-weight: 600;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 100%;
  }
  .metric .val.big { font-size: 0.95rem; color: var(--accent); }
  .metric .val.up { color: var(--bid); }
  .metric .val.down { color: var(--ask); }

  .chips {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(8.4rem, 1fr));
    gap: 0.16rem;
    flex: 1 1 auto;
    align-content: stretch;
    justify-content: stretch;
    min-height: 0;
    overflow: auto;
    grid-auto-rows: 1fr;
  }
  .chip {
    position: relative;
    display: grid;
    grid-template-columns: 1fr auto;
    grid-template-rows: auto auto;
    align-content: center;
    gap: 0 0.25rem;
    padding: 0.28rem 0.35rem;
    background: var(--panel-2);
    border: 1px solid var(--border);
    border-radius: 2px;
    cursor: pointer;
    overflow: hidden;
    min-height: 2.35rem;
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
    grid-column: 1;
    grid-row: 1;
    font-family: var(--mono);
    font-size: 0.6rem;
    color: var(--text);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .vheat {
    position: relative;
    grid-column: 2;
    grid-row: 1 / span 2;
    align-self: center;
    font-family: var(--mono);
    font-size: 0.72rem;
    font-weight: 700;
    color: var(--accent);
  }
  .vmeta {
    position: relative;
    grid-column: 1;
    grid-row: 2;
    font-family: var(--mono);
    font-size: 0.48rem;
    color: var(--muted);
  }

  .alerts-col { background: var(--panel-2); }
  .heuristics {
    display: flex;
    flex-wrap: wrap;
    gap: 0.15rem;
    margin-bottom: 0.2rem;
    flex-shrink: 0;
    min-height: 1.1rem;
  }
  .badge {
    font-family: var(--mono);
    font-size: 0.5rem;
    padding: 0.02rem 0.22rem;
    border: 1px solid var(--border);
    border-radius: 2px;
    background: var(--panel);
    color: var(--text-dim, var(--muted));
  }
  .badge.buy { border-color: rgba(2, 192, 118, 0.4); color: var(--bid); }
  .badge.sell { border-color: rgba(246, 70, 93, 0.4); color: var(--ask); }
  .large-head {
    display: grid;
    grid-template-columns: 1fr 1fr 0.45fr;
    font-size: 0.46rem;
    color: var(--muted);
    text-transform: uppercase;
    flex-shrink: 0;
    padding-bottom: 0.04rem;
  }
  .large-list {
    overflow: auto;
    flex: 1;
    min-height: 0;
    font-family: var(--mono);
    font-size: 0.56rem;
  }
  .lt {
    display: grid;
    grid-template-columns: 1fr 1fr 0.45fr;
    padding: 0.04rem 0;
    color: var(--text-dim, var(--muted));
  }
  .lt.buy { color: var(--bid); }
  .lt.sell { color: var(--ask); }
  .lt.dim { opacity: 0.72; }
  .empty {
    color: var(--muted);
    font-family: var(--mono);
    font-size: 0.55rem;
    padding: 0.25rem 0;
  }

  .fp-tip {
    position: fixed;
    z-index: 80;
    pointer-events: none;
    max-width: 22rem;
    padding: 0.25rem 0.4rem;
    background: rgba(18, 22, 28, 0.96);
    border: 1px solid rgba(240, 185, 11, 0.35);
    border-radius: 2px;
    color: var(--text);
    font-family: var(--mono);
    font-size: 0.55rem;
    box-shadow: 0 4px 16px rgba(0, 0, 0, 0.45);
  }

  @media (max-width: 1100px) {
    .fp-body,
    .fp.compact .fp-body {
      grid-template-columns: 1fr 1fr;
      grid-template-rows: minmax(140px, 1.2fr) minmax(96px, 0.8fr);
    }
    .alerts-col {
      grid-column: 1 / -1;
      max-height: 100px;
      border-right: none;
      border-top: 1px solid var(--border);
    }
    .pulse-metrics { grid-template-columns: repeat(5, 1fr); }
    .stat-row { grid-template-columns: repeat(4, 1fr); }
    .chrome-right .thresh:first-child { display: none; }
    .chips { grid-auto-rows: minmax(1.85rem, auto); }
  }
  @media (max-width: 720px) {
    .fp-body,
    .fp.compact .fp-body {
      grid-template-columns: 1fr;
      grid-template-rows: none;
      overflow: auto;
    }
    .col {
      border-right: none;
      border-bottom: 1px solid var(--border);
      min-height: 160px;
    }
    .alerts-col {
      grid-column: auto;
      max-height: none;
      min-height: 110px;
    }
    .pulse-metrics { grid-template-columns: repeat(3, 1fr); }
  }
</style>
