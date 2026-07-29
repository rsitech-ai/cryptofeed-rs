<script>
  import { fmtPrice, fmtQty, fmtUsd, fmtTotal } from '../lib/format.js';
  import {
    bookPressure,
    computeCvd,
    detectFlowHeuristics,
    ladderLevels,
    levelImbalancePct,
    sparkPath,
    tradeNotional,
    volumeAtPrice,
  } from '../lib/orderflow.js';
  import DepthChart from './DepthChart.svelte';

  let {
    book = null,
    tape = [],
    depth = 16,
    lastPrice = null,
    windowSec = 300,
    largeUsd = 25000,
    imbalanceHistory = [],
    onLargeUsd = () => {},
    onDepth = null,
  } = $props();

  let ladder = $derived(ladderLevels(book, depth));
  let pressure = $derived(bookPressure(book, depth));
  let cvd = $derived(computeCvd(tape, { windowSec }));
  let vap = $derived(volumeAtPrice(tape, { windowSec, maxBuckets: 28 }));
  let heuristics = $derived(detectFlowHeuristics(tape, book, { largeUsd, windowSec }));

  let levelRows = $derived.by(() => {
    const n = Math.max(ladder.asks.length, ladder.bids.length);
    const rows = [];
    for (let i = 0; i < n; i++) {
      const bid = ladder.bids[i] || null;
      const ask = ladder.asks[i] || null;
      const imb = levelImbalancePct(bid?.qty ?? 0, ask?.qty ?? 0);
      rows.push({ i, bid, ask, imb });
    }
    return rows;
  });

  let imbSpark = $derived(sparkPath(imbalanceHistory.map((p) => p.imbalancePct), { w: 120, h: 28 }));
  let cvdSpark = $derived(sparkPath(cvd.points.map((p) => p.cvd), { w: 140, h: 32 }));

  let histMax = $derived(
    Math.max(1, ...cvd.histogram.map((h) => Math.max(h.buyUsd, h.sellUsd))),
  );

  let vapMax = $derived(
    Math.max(1, ...vap.map((r) => Math.max(r.buyUsd, r.sellUsd))),
  );

  let largeTrades = $derived(
    (tape || [])
      .filter((e) => e.kind === 'trade')
      .filter((e) => (tradeNotional(e) ?? 0) >= largeUsd)
      .slice(0, 8),
  );

  function barW(v, max) {
    return `${Math.min(100, (v / max) * 100)}%`;
  }
</script>

<section class="of" aria-label="Order flow with depth">
  <div class="of-grid">
    <!-- Depth ladder + pressure -->
    <div class="pane ladder-pane">
      <div class="pane-head">
        <span class="title">Depth ladder</span>
        {#if onDepth}
          <div class="depth-btns">
            {#each [8, 16, 24] as d}
              <button type="button" class:active={depth === d} onclick={() => onDepth(d)}>{d}</button>
            {/each}
          </div>
        {/if}
      </div>
      <DepthChart {book} {depth} />
      <div class="pressure-usd">
        <div class="bar">
          <div class="bid" style={`width:${pressure.bidPct}%`}></div>
          <div class="ask" style={`width:${pressure.askPct}%`}></div>
        </div>
        <div class="plabels">
          <span class="bid">Bid {fmtUsd(pressure.bidUsd)}</span>
          <span class="imb" title="(bid−ask)/(bid+ask)">imb {pressure.imbalancePct.toFixed(1)}%</span>
          <span class="ask">Ask {fmtUsd(pressure.askUsd)}</span>
        </div>
      </div>
      <div class="imb-spark" title="Depth imbalance over time">
        {#if imbSpark}
          <svg viewBox="0 0 120 28" preserveAspectRatio="none">
            <path d={imbSpark} fill="none" stroke="var(--accent)" stroke-width="1.2" />
          </svg>
        {:else}
          <span class="muted">imbalance spark…</span>
        {/if}
      </div>
      <div class="ladder-cols">
        <span>Bid</span><span>Qty</span><span>Imb%</span><span>Qty</span><span>Ask</span>
      </div>
      <div class="ladder">
        {#each levelRows as row}
          <div class="lrow">
            <span class="bid px">{row.bid ? fmtPrice(row.bid.price, 2) : ''}</span>
            <span class="bid qty">{row.bid ? fmtQty(row.bid.qty) : ''}</span>
            <span
              class="imb"
              class:pos={row.imb > 5}
              class:neg={row.imb < -5}
            >{row.bid || row.ask ? row.imb.toFixed(0) : ''}</span>
            <span class="ask qty">{row.ask ? fmtQty(row.ask.qty) : ''}</span>
            <span class="ask px">{row.ask ? fmtPrice(row.ask.price, 2) : ''}</span>
          </div>
        {:else}
          <div class="empty">waiting for book…</div>
        {/each}
      </div>
    </div>

    <!-- CVD + flow -->
    <div class="pane flow-pane">
      <div class="pane-head">
        <span class="title">Trade flow / CVD</span>
        <label class="thresh" title="Large trade notional threshold (USD)">
          ≥$
          <input
            type="number"
            min="0"
            step="1000"
            value={largeUsd}
            onchange={(e) => onLargeUsd(Number(e.currentTarget.value))}
          />
        </label>
      </div>
      <div class="cvd-stats">
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
        <div class="stat last">
          <span class="lbl">Last</span>
          <span class="val">{lastPrice != null ? fmtPrice(lastPrice, 2) : '—'}</span>
        </div>
      </div>
      <div class="cvd-spark">
        {#if cvdSpark}
          <svg viewBox="0 0 140 32" preserveAspectRatio="none" aria-label="CVD sparkline">
            <path d={cvdSpark} fill="none" stroke={cvd.cvd >= 0 ? 'var(--bid)' : 'var(--ask)'} stroke-width="1.4" />
          </svg>
        {:else}
          <span class="muted">CVD accumulating…</span>
        {/if}
      </div>
      <div class="hist" aria-label="Buy vs sell histogram">
        {#each cvd.histogram.slice(-40) as h}
          <div class="hcol">
            <div class="buy" style={`height:${(h.buyUsd / histMax) * 100}%`}></div>
            <div class="sell" style={`height:${(h.sellUsd / histMax) * 100}%`}></div>
          </div>
        {:else}
          <span class="muted">buy/sell histogram…</span>
        {/each}
      </div>
      <div class="heuristics">
        <span class="htag">Heuristics</span>
        {#each heuristics.slice(-6) as h}
          <span class="badge" class:buy={h.side === 'buy'} class:sell={h.side === 'sell'} title={h.label}>
            {h.kind}
          </span>
        {:else}
          <span class="muted">no large/sweep/absorption flags</span>
        {/each}
      </div>
      {#if largeTrades.length}
        <div class="large-list">
          {#each largeTrades as e}
            <div class="lt" class:buy={e.aggressor === 'buy'} class:sell={e.aggressor === 'sell'}>
              <span>{fmtPrice(e.price, 2)}</span>
              <span>{fmtUsd(tradeNotional(e))}</span>
              <span>{e.aggressor || '?'}</span>
            </div>
          {/each}
        </div>
      {/if}
    </div>

    <!-- VAP footprint-lite -->
    <div class="pane vap-pane">
      <div class="pane-head">
        <span class="title">VAP <em>(trade-aggregated)</em></span>
        <span class="meta">not MBO footprint</span>
      </div>
      <div class="vap-cols"><span>Sell $</span><span>Price</span><span>Buy $</span><span>Δ</span></div>
      <div class="vap">
        {#each vap as row}
          <div class="vrow">
            <div class="sell-bar-wrap">
              <div class="sell-bar" style={`width:${barW(row.sellUsd, vapMax)}`}></div>
              <span>{fmtUsd(row.sellUsd)}</span>
            </div>
            <span class="px">{fmtPrice(row.price, 2)}</span>
            <div class="buy-bar-wrap">
              <div class="buy-bar" style={`width:${barW(row.buyUsd, vapMax)}`}></div>
              <span>{fmtUsd(row.buyUsd)}</span>
            </div>
            <span class="delta" class:up={row.delta > 0} class:down={row.delta < 0}>{fmtUsd(row.delta)}</span>
          </div>
        {:else}
          <div class="empty">volume-at-price from tape window…</div>
        {/each}
      </div>
      <div class="foot-note">
        Cum bid {fmtTotal(ladder.bidCumQty)} / ask {fmtTotal(ladder.askCumQty)} · window {Math.round(windowSec / 60)}m
      </div>
    </div>
  </div>
</section>

<style>
  .of {
    height: 100%;
    min-height: 0;
    background: var(--panel);
    overflow: hidden;
  }
  .of-grid {
    display: grid;
    grid-template-columns: 1.1fr 1fr 1.05fr;
    height: 100%;
    min-height: 0;
    gap: 0;
  }
  .pane {
    display: flex;
    flex-direction: column;
    min-height: 0;
    min-width: 0;
    border-right: 1px solid var(--border);
    padding: 0.35rem 0.45rem;
  }
  .pane:last-child { border-right: none; }
  .pane-head {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 0.4rem;
    margin-bottom: 0.25rem;
    flex-shrink: 0;
  }
  .title {
    font-size: 0.72rem;
    font-weight: 600;
  }
  .title em {
    font-style: normal;
    color: var(--muted);
    font-weight: 500;
    font-size: 0.62rem;
  }
  .meta, .muted { color: var(--muted); font-size: 0.62rem; font-family: var(--mono); }
  .depth-btns { display: flex; gap: 0.15rem; }
  .depth-btns button {
    background: transparent;
    border: 1px solid transparent;
    color: var(--muted);
    font-family: var(--mono);
    font-size: 0.62rem;
    padding: 0.05rem 0.3rem;
    cursor: pointer;
    border-radius: 2px;
  }
  .depth-btns button.active {
    color: var(--accent);
    border-color: rgba(240, 185, 11, 0.35);
  }
  .thresh {
    font-size: 0.62rem;
    color: var(--muted);
    font-family: var(--mono);
    display: flex;
    align-items: center;
    gap: 0.15rem;
  }
  .thresh input {
    width: 5.5rem;
    background: var(--panel-2);
    border: 1px solid var(--border);
    border-radius: 2px;
    padding: 0.1rem 0.25rem;
    font-family: var(--mono);
    font-size: 0.62rem;
  }

  .pressure-usd { margin: 0.25rem 0; flex-shrink: 0; }
  .pressure-usd .bar { display: flex; height: 8px; border-radius: 2px; overflow: hidden; }
  .pressure-usd .bid { background: rgba(2, 192, 118, 0.55); }
  .pressure-usd .ask { background: rgba(246, 70, 93, 0.55); }
  .plabels {
    display: flex;
    justify-content: space-between;
    font-family: var(--mono);
    font-size: 0.58rem;
    margin-top: 0.15rem;
  }
  .plabels .bid, .bid { color: var(--bid); }
  .plabels .ask, .ask { color: var(--ask); }
  .plabels .imb { color: var(--accent); }

  .imb-spark {
    height: 28px;
    margin-bottom: 0.25rem;
    background: var(--panel-2);
    border: 1px solid var(--border);
    flex-shrink: 0;
  }
  .imb-spark svg { width: 100%; height: 100%; display: block; }

  .ladder-cols, .vap-cols {
    display: grid;
    font-size: 0.55rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.03em;
    flex-shrink: 0;
    margin-bottom: 0.1rem;
  }
  .ladder-cols { grid-template-columns: 1.1fr 0.8fr 0.55fr 0.8fr 1.1fr; }
  .vap-cols { grid-template-columns: 1fr 0.7fr 1fr 0.7fr; }

  .ladder, .vap {
    overflow: auto;
    flex: 1;
    min-height: 0;
    font-family: var(--mono);
    font-size: 0.65rem;
  }
  .lrow {
    display: grid;
    grid-template-columns: 1.1fr 0.8fr 0.55fr 0.8fr 1.1fr;
    line-height: 1.35;
    padding: 0.02rem 0;
  }
  .lrow .qty { text-align: right; color: var(--text-dim); }
  .lrow .imb { text-align: center; color: var(--muted); }
  .lrow .imb.pos { color: var(--bid); }
  .lrow .imb.neg { color: var(--ask); }
  .lrow .ask.px { text-align: right; }

  .cvd-stats {
    display: flex;
    flex-wrap: wrap;
    gap: 0.45rem 0.75rem;
    margin-bottom: 0.3rem;
    flex-shrink: 0;
  }
  .stat { display: flex; flex-direction: column; gap: 0.05rem; }
  .stat .lbl { font-size: 0.55rem; color: var(--muted); text-transform: uppercase; }
  .stat .val { font-family: var(--mono); font-size: 0.85rem; font-weight: 600; }
  .stat .val.up, .delta.up { color: var(--bid); }
  .stat .val.down, .delta.down { color: var(--ask); }
  .stat .val.bid { color: var(--bid); }
  .stat .val.ask { color: var(--ask); }

  .cvd-spark {
    height: 32px;
    background: var(--panel-2);
    border: 1px solid var(--border);
    margin-bottom: 0.3rem;
    flex-shrink: 0;
  }
  .cvd-spark svg { width: 100%; height: 100%; }

  .hist {
    display: flex;
    align-items: flex-end;
    gap: 1px;
    height: 48px;
    background: var(--panel-2);
    border: 1px solid var(--border);
    padding: 2px;
    margin-bottom: 0.35rem;
    flex-shrink: 0;
  }
  .hcol {
    flex: 1;
    display: flex;
    flex-direction: column;
    justify-content: flex-end;
    min-width: 0;
    height: 100%;
  }
  .hcol .buy { background: rgba(2, 192, 118, 0.65); min-height: 0; }
  .hcol .sell { background: rgba(246, 70, 93, 0.65); min-height: 0; }

  .heuristics {
    display: flex;
    flex-wrap: wrap;
    gap: 0.25rem;
    align-items: center;
    margin-bottom: 0.3rem;
    flex-shrink: 0;
  }
  .htag {
    font-size: 0.55rem;
    color: var(--muted);
    text-transform: uppercase;
    margin-right: 0.2rem;
  }
  .badge {
    font-family: var(--mono);
    font-size: 0.58rem;
    padding: 0.05rem 0.3rem;
    border: 1px solid var(--border);
    border-radius: 2px;
    background: var(--panel-2);
    color: var(--text-dim);
  }
  .badge.buy { border-color: rgba(2, 192, 118, 0.4); color: var(--bid); }
  .badge.sell { border-color: rgba(246, 70, 93, 0.4); color: var(--ask); }

  .large-list {
    overflow: auto;
    flex: 1;
    min-height: 0;
    font-family: var(--mono);
    font-size: 0.62rem;
  }
  .lt {
    display: grid;
    grid-template-columns: 1fr 1fr 0.5fr;
    padding: 0.05rem 0;
    color: var(--text-dim);
  }
  .lt.buy { color: var(--bid); }
  .lt.sell { color: var(--ask); }

  .vrow {
    display: grid;
    grid-template-columns: 1fr 0.7fr 1fr 0.7fr;
    align-items: center;
    gap: 0.2rem;
    padding: 0.04rem 0;
  }
  .sell-bar-wrap, .buy-bar-wrap {
    position: relative;
    height: 14px;
    display: flex;
    align-items: center;
  }
  .sell-bar-wrap { justify-content: flex-end; }
  .sell-bar, .buy-bar {
    position: absolute;
    top: 0; bottom: 0;
    opacity: 0.35;
  }
  .sell-bar { right: 0; background: var(--ask); }
  .buy-bar { left: 0; background: var(--bid); }
  .sell-bar-wrap span, .buy-bar-wrap span {
    position: relative;
    z-index: 1;
    font-size: 0.58rem;
  }
  .vrow .px { text-align: center; font-weight: 600; }
  .vrow .delta { text-align: right; font-size: 0.58rem; }

  .foot-note {
    flex-shrink: 0;
    font-size: 0.55rem;
    color: var(--muted);
    font-family: var(--mono);
    margin-top: 0.25rem;
    padding-top: 0.2rem;
    border-top: 1px solid var(--border);
  }
  .empty {
    color: var(--muted);
    font-size: 0.65rem;
    padding: 0.5rem 0;
    font-family: var(--mono);
  }

  @media (max-width: 1100px) {
    .of-grid { grid-template-columns: 1fr 1fr; grid-template-rows: 1fr 1fr; }
    .vap-pane { grid-column: 1 / -1; }
  }
  @media (max-width: 720px) {
    .of-grid { grid-template-columns: 1fr; }
  }
</style>
