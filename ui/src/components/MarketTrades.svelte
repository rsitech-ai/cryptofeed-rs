<script>
  import { fmtPrice, fmtQty, fmtUsd, fmtTsUtcLabel } from '../lib/format.js';

  let {
    tape = [],
    tradeCount = null,
    volume = null,
    notional = null,
    selectedTradeId = null,
    minUsd = 0,
    sideFilter = 'all',
    aggregatePrints = false,
    onSelectTrade = () => {},
    onFilters = () => {},
  } = $props();

  let trades = $derived.by(() => {
    let rows = (tape || []).filter((e) => e.kind === 'trade');

    if (sideFilter === 'buy') rows = rows.filter((e) => e.aggressor === 'buy');
    else if (sideFilter === 'sell') rows = rows.filter((e) => e.aggressor === 'sell');

    if (minUsd > 0) {
      rows = rows.filter((e) => {
        const px = Number(e.price);
        const qty = Number(e.quantity);
        return Number.isFinite(px) && Number.isFinite(qty) && px * qty >= minUsd;
      });
    }

    if (aggregatePrints) {
      /** @type {Map<string, object>} */
      const m = new Map();
      for (const e of rows) {
        const px = Number(e.price).toFixed(2);
        const key = `${e.aggressor || 'x'}|${px}`;
        const cur = m.get(key);
        const qty = Number(e.quantity) || 0;
        const price = Number(e.price) || 0;
        if (cur) {
          cur.quantity = (Number(cur.quantity) || 0) + qty;
          cur._count = (cur._count || 1) + 1;
          cur.receive_ts_ns = Math.max(cur.receive_ts_ns || 0, e.receive_ts_ns || 0);
        } else {
          m.set(key, { ...e, _count: 1 });
        }
      }
      rows = [...m.values()];
    }

    return rows.slice(0, 120);
  });

  function rowKey(e) {
    return `${e.venue || ''}|${e.trade_id ?? ''}|${e.receive_ts_ns}|${e.price}`;
  }

  function tradeNotional(e) {
    const px = Number(e.price);
    const qty = Number(e.quantity);
    if (!Number.isFinite(px) || !Number.isFinite(qty)) return null;
    return px * qty;
  }
</script>

<section class="trades">
  <div class="head">
    <div class="title-row">
      <span class="title">Market Trades</span>
      <span class="meta" title="USD notional from tape">
        {#if tradeCount != null}{tradeCount} raw · {/if}{trades.length} shown
        {#if notional != null} · {fmtUsd(notional)}{:else if volume != null} · {fmtQty(volume)} qty{/if}
      </span>
    </div>
    <div class="filters">
      <label title="Min trade size in USD">
        Min $
        <input
          type="number"
          min="0"
          step="100"
          value={minUsd}
          onchange={(e) => onFilters({ minUsd: Number(e.currentTarget.value) })}
        />
      </label>
      <select
        value={sideFilter}
        onchange={(e) => onFilters({ sideFilter: e.currentTarget.value })}
      >
        <option value="all">All</option>
        <option value="buy">Buy</option>
        <option value="sell">Sell</option>
      </select>
      <label class="chk">
        <input
          type="checkbox"
          checked={aggregatePrints}
          onchange={(e) => onFilters({ aggregatePrints: e.currentTarget.checked })}
        />
        Prints
      </label>
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
        title="{tradeNotional(e) != null ? fmtUsd(tradeNotional(e)) : ''}{e._count > 1 ? ' · ' + e._count + ' prints' : ''}"
      >
        <span class="px">{fmtPrice(e.price, 2)}</span>
        <span class="qty">{fmtQty(e.quantity)}{#if e._count > 1}<span class="cnt">×{e._count}</span>{/if}</span>
        <span class="ts">{fmtTsUtcLabel(e.exchange_ts_ns ?? e.receive_ts_ns)}</span>
      </button>
    {:else}
      <div class="empty">no trades match filters</div>
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
    padding: var(--panel-pad, 0.4rem 0.5rem 0.25rem);
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

  .filters {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    margin-bottom: 0.25rem;
    flex-wrap: wrap;
  }

  .filters label {
    display: flex;
    align-items: center;
    gap: 0.2rem;
    font-family: var(--mono);
    font-size: 0.6rem;
    color: var(--muted);
  }

  .filters input[type='number'] {
    width: 3.5rem;
    background: var(--bg);
    border: 1px solid var(--border);
    color: var(--text);
    font-family: var(--mono);
    font-size: 0.62rem;
    padding: 0.1rem 0.2rem;
  }

  .filters select {
    background: var(--bg);
    border: 1px solid var(--border);
    color: var(--text);
    font-family: var(--mono);
    font-size: 0.62rem;
    padding: 0.1rem 0.25rem;
  }

  .filters .chk {
    cursor: pointer;
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
    font-size: var(--row-font, 0.72rem);
    flex: 1;
    min-height: 0;
  }

  .row {
    display: grid;
    grid-template-columns: 1fr 1fr 1fr;
    width: 100%;
    padding: var(--row-pad, 0.05rem 0.5rem);
    line-height: 1.4;
    background: transparent;
    border: none;
    border-bottom: 1px solid transparent;
    color: inherit;
    font: inherit;
    cursor: pointer;
    text-align: left;
  }

  .row:hover { background: var(--panel-2); }

  .row.active {
    background: rgba(240, 185, 11, 0.12);
    border-bottom-color: rgba(240, 185, 11, 0.25);
  }

  .qty, .ts {
    text-align: right;
    color: var(--text-dim);
  }

  .cnt {
    margin-left: 0.2rem;
    color: var(--muted);
    font-size: 0.58rem;
  }

  .row.buy .px { color: var(--bid); }
  .row.sell .px { color: var(--ask); }

  .empty {
    color: var(--muted);
    padding: 0.6rem 0.5rem;
    font-size: 0.7rem;
  }
</style>
