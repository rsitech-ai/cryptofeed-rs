<script>
  /**
   * Professional DOM ladder: bid size | price | ask size.
   * Keyed by price; size bars + cumulative; paint-friendly (no list remount flicker).
   */
  import { fmtPrice, fmtQty, fmtUsd } from '../lib/format.js';
  import { domLadder, resolveTick } from '../lib/orderflow.js';

  let {
    book = null,
    depth = 16,
    tickOpt = 'auto',
    lastPrice = null,
    showCum = true,
    onDepth = null,
  } = $props();

  let tick = $derived(resolveTick(tickOpt, book));
  let ladder = $derived(domLadder(book, { depth, tick }));
  let hasBook = $derived(!!(book?.bids?.length || book?.asks?.length));

  function barPct(usd, max) {
    return `${Math.min(100, ((usd || 0) / (max || 1)) * 100)}%`;
  }

  function isInside(price) {
    const bb = ladder.bestBid;
    const ba = ladder.bestAsk;
    if (bb != null && Math.abs(price - bb) < tick * 0.5) return 'bid';
    if (ba != null && Math.abs(price - ba) < tick * 0.5) return 'ask';
    return '';
  }
</script>

<section class="dom" aria-label="Depth of market ladder">
  <div class="head">
    <span class="title">DOM</span>
    <span class="meta" title="L2 snapshot ladder — not MBO queue">bid · price · ask</span>
    {#if onDepth}
      <div class="depth-btns">
        {#each [8, 16, 24, 32, 48] as d}
          <button type="button" class:active={depth === d} onclick={() => onDepth(d)}>{d}</button>
        {/each}
      </div>
    {/if}
  </div>

  {#if !hasBook}
    <div class="badge-warn" title="Focus venue has no L2 book">no L2</div>
    <div class="empty">waiting for book…</div>
  {:else}
    <div class="cols" class:cum={showCum}>
      <span>Bid</span>
      {#if showCum}<span>Cum$</span>{/if}
      <span>Price</span>
      {#if showCum}<span>Cum$</span>{/if}
      <span>Ask</span>
      <span>Imb</span>
    </div>
    <div class="rows">
      {#each ladder.rows as row (row.key)}
        {@const inside = isInside(row.price)}
        <div
          class="row"
          class:cum={showCum}
          class:inside-bid={inside === 'bid'}
          class:inside-ask={inside === 'ask'}
          class:near={lastPrice != null && Math.abs(row.price - lastPrice) <= tick * 1.5}
        >
          <div class="cell bid">
            <div class="bar" style={`width:${barPct(row.bidUsd, ladder.maxUsd)}`}></div>
            <span class="sz">{row.bidQty > 0 ? fmtQty(row.bidQty) : ''}</span>
          </div>
          {#if showCum}
            <span class="cum bid">{row.bidCumUsd > 0 ? fmtUsd(row.bidCumUsd) : ''}</span>
          {/if}
          <span class="px">{fmtPrice(row.price, tick >= 1 ? 0 : tick >= 0.1 ? 1 : 2)}</span>
          {#if showCum}
            <span class="cum ask">{row.askCumUsd > 0 ? fmtUsd(row.askCumUsd) : ''}</span>
          {/if}
          <div class="cell ask">
            <div class="bar" style={`width:${barPct(row.askUsd, ladder.maxUsd)}`}></div>
            <span class="sz">{row.askQty > 0 ? fmtQty(row.askQty) : ''}</span>
          </div>
          <span
            class="imb"
            class:pos={row.imbPct > 8}
            class:neg={row.imbPct < -8}
          >{row.bidQty || row.askQty ? row.imbPct.toFixed(0) : ''}</span>
        </div>
      {/each}
    </div>
    <div class="foot">
      <span class="bid">bid {ladder.bestBid != null ? fmtPrice(ladder.bestBid, 2) : '—'}</span>
      <span class="mid">{ladder.mid != null ? fmtPrice(ladder.mid, 2) : '—'}</span>
      <span class="ask">ask {ladder.bestAsk != null ? fmtPrice(ladder.bestAsk, 2) : '—'}</span>
      <span class="tick">tick {tick}</span>
    </div>
  {/if}
</section>

<style>
  .dom {
    display: flex;
    flex-direction: column;
    height: 100%;
    min-height: 0;
    background: var(--panel);
    font-family: var(--mono);
  }
  .head {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    padding: 0.3rem 0.45rem;
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
  }
  .title {
    font-size: 0.72rem;
    font-weight: 600;
    color: var(--accent);
    font-family: var(--sans, system-ui, sans-serif);
  }
  .meta { font-size: 0.55rem; color: var(--muted); }
  .depth-btns { margin-left: auto; display: flex; gap: 0.12rem; }
  .depth-btns button {
    background: transparent;
    border: 1px solid transparent;
    color: var(--muted);
    font-family: var(--mono);
    font-size: 0.58rem;
    padding: 0.05rem 0.28rem;
    cursor: pointer;
    border-radius: 2px;
  }
  .depth-btns button.active {
    color: var(--accent);
    border-color: rgba(240, 185, 11, 0.35);
  }
  .badge-warn {
    margin: 0.35rem 0.45rem 0;
    align-self: flex-start;
    font-size: 0.58rem;
    padding: 0.08rem 0.35rem;
    border: 1px solid rgba(246, 70, 93, 0.45);
    color: var(--ask);
    border-radius: 2px;
    background: rgba(246, 70, 93, 0.08);
  }
  .cols {
    display: grid;
    grid-template-columns: 1fr 0.9fr 1fr 0.45fr;
    gap: 0.15rem;
    padding: 0.2rem 0.4rem;
    font-size: 0.52rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.03em;
    flex-shrink: 0;
  }
  .cols.cum { grid-template-columns: 1fr 0.7fr 0.85fr 0.7fr 1fr 0.4fr; }
  .rows { flex: 1; min-height: 0; overflow: auto; padding: 0 0.25rem 0.25rem; }
  .row {
    display: grid;
    grid-template-columns: 1fr 0.9fr 1fr 0.45fr;
    gap: 0.15rem;
    align-items: center;
    line-height: 1.35;
    font-size: 0.62rem;
    border-radius: 1px;
  }
  .row.cum { grid-template-columns: 1fr 0.7fr 0.85fr 0.7fr 1fr 0.4fr; }
  .row.inside-bid { background: rgba(2, 192, 118, 0.08); }
  .row.inside-ask { background: rgba(246, 70, 93, 0.08); }
  .row.near .px { color: var(--accent); font-weight: 700; }
  .cell {
    position: relative;
    height: 15px;
    display: flex;
    align-items: center;
    min-width: 0;
  }
  .cell.bid { justify-content: flex-end; }
  .cell.ask { justify-content: flex-start; }
  .cell .bar {
    position: absolute;
    top: 1px;
    bottom: 1px;
    opacity: 0.28;
    pointer-events: none;
  }
  .cell.bid .bar { right: 0; background: var(--bid); }
  .cell.ask .bar { left: 0; background: var(--ask); }
  .cell .sz { position: relative; z-index: 1; padding: 0 0.15rem; }
  .cell.bid .sz { color: var(--bid); }
  .cell.ask .sz { color: var(--ask); }
  .px { text-align: center; font-weight: 600; color: var(--text); }
  .cum { font-size: 0.55rem; color: var(--text-dim); text-align: right; }
  .cum.ask { text-align: left; color: rgba(246, 70, 93, 0.75); }
  .cum.bid { color: rgba(2, 192, 118, 0.75); }
  .imb { text-align: center; color: var(--muted); font-size: 0.55rem; }
  .imb.pos { color: var(--bid); }
  .imb.neg { color: var(--ask); }
  .foot {
    display: flex;
    gap: 0.55rem;
    padding: 0.25rem 0.45rem;
    border-top: 1px solid var(--border);
    font-size: 0.55rem;
    flex-shrink: 0;
  }
  .foot .bid { color: var(--bid); }
  .foot .ask { color: var(--ask); }
  .foot .mid { color: var(--accent); margin-left: auto; }
  .foot .tick { color: var(--muted); }
  .empty {
    color: var(--muted);
    font-size: 0.68rem;
    padding: 1rem 0.5rem;
    text-align: center;
  }
</style>
