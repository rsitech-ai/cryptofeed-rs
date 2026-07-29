<script>
  import { fmtPrice, fmtQty, fmtTsUtcLabel } from '../lib/format.js';

  let {
    tape = [],
    tradeCount = null,
    volume = null,
    selectedTradeId = null,
    onSelectTrade = () => {},
  } = $props();

  let trades = $derived(
    (tape || []).filter((e) => e.kind === 'trade').slice(0, 120),
  );

  function rowKey(e) {
    return `${e.venue || ''}|${e.trade_id ?? ''}|${e.receive_ts_ns}`;
  }
</script>

<section class="trades">
  <div class="head">
    <div class="title-row">
      <span class="title">Market Trades</span>
      <span class="meta" title="From /v1/tape?kind=trade (newest first, UTC)">
        {#if tradeCount != null}
          {tradeCount} shown
        {:else}
          {trades.length}
        {/if}
        {#if volume != null}
          · vol {fmtQty(volume)}
        {/if}
      </span>
    </div>
    <div class="cols">
      <span>Price</span>
      <span>Amount</span>
      <span>Time (UTC)</span>
    </div>
  </div>
  <div class="list">
    {#each trades as e (rowKey(e))}
      <button
        type="button"
        class="row"
        class:buy={e.aggressor === 'buy'}
        class:sell={e.aggressor === 'sell'}
        class:active={selectedTradeId != null && String(e.trade_id) === String(selectedTradeId)}
        onclick={() => onSelectTrade(e)}
        title="Click to mark time on chart"
      >
        <span class="px">{fmtPrice(e.price, 2)}</span>
        <span class="qty">{fmtQty(e.quantity)}</span>
        <span class="ts">{fmtTsUtcLabel(e.exchange_ts_ns ?? e.receive_ts_ns)}</span>
      </button>
    {:else}
      <div class="empty">no trades yet</div>
    {/each}
  </div>
</section>

<style>
  .trades {
    display: flex;
    flex-direction: column;
    min-height: 0;
    height: 100%;
    background: var(--panel);
  }

  .head {
    padding: 0.4rem 0.5rem 0.25rem;
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
  }

  .title-row {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    gap: 0.5rem;
    margin-bottom: 0.25rem;
  }

  .title {
    font-size: 0.72rem;
    font-weight: 600;
  }

  .meta {
    font-family: var(--mono);
    font-size: 0.62rem;
    color: var(--muted);
  }

  .cols {
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    font-size: 0.62rem;
    color: var(--muted);
    text-transform: uppercase;
  }

  .cols span:nth-child(2),
  .cols span:nth-child(3) {
    text-align: right;
  }

  .list {
    overflow: auto;
    font-family: var(--mono);
    font-size: 0.72rem;
    flex: 1;
    min-height: 0;
  }

  .row {
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    width: 100%;
    padding: 0.05rem 0.5rem;
    line-height: 1.4;
    background: transparent;
    border: none;
    border-bottom: 1px solid transparent;
    color: inherit;
    font: inherit;
    cursor: pointer;
    text-align: left;
    border-radius: 0;
  }

  .row:hover {
    background: var(--panel-2);
  }

  .row.active {
    background: rgba(240, 185, 11, 0.12);
    border-bottom-color: rgba(240, 185, 11, 0.25);
  }

  .qty,
  .ts {
    text-align: right;
    color: var(--text-dim);
  }

  .row.buy .px {
    color: var(--bid);
  }
  .row.sell .px {
    color: var(--ask);
  }

  .empty {
    color: var(--muted);
    padding: 0.6rem 0.5rem;
    font-size: 0.7rem;
  }
</style>
