<script>
  import { fmtPrice, fmtQty, fmtTotal } from '../lib/format.js';

  let {
    book = null,
    lastPrice = null,
    priceDir = 0,
    depth = 16,
    onDepth = null,
  } = $props();

  let levels = $derived.by(() => {
    const asks = [...(book?.asks || [])].slice(0, depth);
    const bids = [...(book?.bids || [])].slice(0, depth);
    return { asks, bids };
  });

  let withTotals = $derived.by(() => {
    let askCum = 0;
    const asks = levels.asks.map((l) => {
      const qty = Number(l.quantity) || 0;
      askCum += qty;
      return { ...l, qty, total: askCum };
    });
    let bidCum = 0;
    const bids = levels.bids.map((l) => {
      const qty = Number(l.quantity) || 0;
      bidCum += qty;
      return { ...l, qty, total: bidCum };
    });
    const maxTotal = Math.max(
      asks.length ? asks[asks.length - 1].total : 0,
      bids.length ? bids[bids.length - 1].total : 0,
      1e-12,
    );
    return { asks, bids, maxTotal, askCum, bidCum };
  });

  let pressure = $derived.by(() => {
    const buy = withTotals.bidCum;
    const sell = withTotals.askCum;
    const sum = buy + sell;
    if (sum <= 0) return { buyPct: 50, sellPct: 50 };
    const buyPct = (buy / sum) * 100;
    return { buyPct, sellPct: 100 - buyPct };
  });

  function barPct(total, max) {
    return `${Math.min(100, (total / max) * 100)}%`;
  }
</script>

<section class="book">
  <div class="book-head">
    <div class="title-row">
      <span class="title">Order Book</span>
      {#if onDepth}
        <div class="depth-btns">
          {#each [8, 16, 24] as d}
            <button type="button" class:active={depth === d} onclick={() => onDepth(d)}>{d}</button>
          {/each}
        </div>
      {/if}
    </div>
    <span class="cols">
      <span>Price</span>
      <span>Amount</span>
      <span>Total</span>
    </span>
  </div>

  <div class="asks">
    {#each [...withTotals.asks].reverse() as lvl}
      <div class="row ask">
        <div class="depth" style={`width:${barPct(lvl.total, withTotals.maxTotal)}`}></div>
        <span class="px">{fmtPrice(lvl.price, 2)}</span>
        <span class="qty">{fmtQty(lvl.qty)}</span>
        <span class="tot">{fmtTotal(lvl.total)}</span>
      </div>
    {:else}
      <div class="empty">waiting for asks…</div>
    {/each}
  </div>

  <div class="bbo">
    <span
      class="last"
      class:up={priceDir > 0}
      class:down={priceDir < 0}
    >
      {lastPrice != null ? fmtPrice(lastPrice, 2) : '—'}
      {#if priceDir > 0}↑{:else if priceDir < 0}↓{/if}
    </span>
    {#if book?.bids?.[0] && book?.asks?.[0]}
      <span class="spread-note">
        spread {fmtPrice(Number(book.asks[0].price) - Number(book.bids[0].price), 2)}
      </span>
    {/if}
  </div>

  <div class="bids">
    {#each withTotals.bids as lvl}
      <div class="row bid">
        <div class="depth" style={`width:${barPct(lvl.total, withTotals.maxTotal)}`}></div>
        <span class="px">{fmtPrice(lvl.price, 2)}</span>
        <span class="qty">{fmtQty(lvl.qty)}</span>
        <span class="tot">{fmtTotal(lvl.total)}</span>
      </div>
    {:else}
      <div class="empty">waiting for bids…</div>
    {/each}
  </div>

  <div class="pressure">
    <div class="buy-bar" style={`width:${pressure.buyPct}%`}></div>
    <div class="sell-bar" style={`width:${pressure.sellPct}%`}></div>
    <div class="labels">
      <span class="bid">B {pressure.buyPct.toFixed(1)}%</span>
      <span class="ask">S {pressure.sellPct.toFixed(1)}%</span>
    </div>
  </div>
</section>

<style>
  .book {
    display: flex;
    flex-direction: column;
    min-height: 0;
    height: 100%;
    background: var(--panel);
    border-right: 1px solid var(--border);
  }

  .book-head {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
    padding: 0.4rem 0.5rem 0.3rem;
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
  }

  .title-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 0.5rem;
  }

  .title {
    font-size: 0.72rem;
    font-weight: 600;
    color: var(--text);
  }

  .depth-btns {
    display: flex;
    gap: 0.15rem;
  }

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
  .depth-btns button:hover {
    color: var(--text);
    background: var(--panel-2);
  }
  .depth-btns button.active {
    color: var(--accent);
    border-color: rgba(240, 185, 11, 0.35);
  }

  .cols {
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    font-size: 0.62rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.03em;
  }

  .cols span:nth-child(2),
  .cols span:nth-child(3) {
    text-align: right;
  }

  .asks,
  .bids {
    overflow: hidden;
    font-family: var(--mono);
    font-size: 0.72rem;
    flex: 1;
    min-height: 0;
  }

  .asks {
    display: flex;
    flex-direction: column;
    justify-content: flex-end;
  }

  .row {
    position: relative;
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    padding: 0.04rem 0.5rem;
    line-height: 1.35;
  }

  .depth {
    position: absolute;
    top: 0;
    right: 0;
    bottom: 0;
    pointer-events: none;
  }

  .ask .depth {
    background: var(--ask-depth);
  }
  .bid .depth {
    background: var(--bid-depth);
  }

  .px,
  .qty,
  .tot {
    position: relative;
    z-index: 1;
  }

  .qty,
  .tot {
    text-align: right;
    color: var(--text-dim);
  }

  .ask .px {
    color: var(--ask);
  }
  .bid .px {
    color: var(--bid);
  }

  .bbo {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 0.5rem;
    padding: 0.35rem 0.5rem;
    border-top: 1px solid var(--border);
    border-bottom: 1px solid var(--border);
    background: var(--panel-2);
    flex-shrink: 0;
  }

  .last {
    font-family: var(--mono);
    font-size: 1rem;
    font-weight: 700;
  }
  .last.up {
    color: var(--bid);
  }
  .last.down {
    color: var(--ask);
  }

  .spread-note {
    font-family: var(--mono);
    font-size: 0.65rem;
    color: var(--muted);
  }

  .pressure {
    position: relative;
    display: flex;
    height: 18px;
    flex-shrink: 0;
    border-top: 1px solid var(--border);
  }

  .buy-bar {
    background: rgba(2, 192, 118, 0.35);
  }
  .sell-bar {
    background: rgba(246, 70, 93, 0.35);
  }

  .labels {
    position: absolute;
    inset: 0;
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0 0.45rem;
    font-family: var(--mono);
    font-size: 0.65rem;
    font-weight: 600;
  }
  .labels .bid {
    color: var(--bid);
  }
  .labels .ask {
    color: var(--ask);
  }

  .empty {
    color: var(--muted);
    font-size: 0.7rem;
    padding: 0.5rem;
    font-family: var(--mono);
  }
</style>
