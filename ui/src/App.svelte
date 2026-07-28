<script>
  import { onMount } from 'svelte';

  let status = $state(null);
  let instruments = $state({ venues: [] });
  let book = $state(null);
  let tape = $state([]);
  let error = $state('');
  let selectedVenue = $state('');
  let selectedSymbol = $state('');
  let depth = $state(20);

  const apiBase = '';

  async function fetchJson(path) {
    const res = await fetch(`${apiBase}${path}`);
    if (!res.ok) throw new Error(`${path} → ${res.status}`);
    return res.json();
  }

  function pickDefaults(data) {
    if (!selectedVenue && data.venues?.length) {
      selectedVenue = data.venues[0].id;
      const syms = data.venues[0].symbols || [];
      selectedSymbol = syms[0]?.symbol || 'BTC-USD';
    }
  }

  async function refreshStatus() {
    status = await fetchJson('/v1/status');
  }

  async function refreshInstruments() {
    instruments = await fetchJson('/v1/instruments');
    pickDefaults(instruments);
  }

  async function refreshBook() {
    if (!selectedVenue) return;
    const q = new URLSearchParams({
      venue: selectedVenue,
      symbol: selectedSymbol,
      depth: String(depth),
    });
    try {
      book = await fetchJson(`/v1/books?${q}`);
    } catch {
      book = null;
    }
  }

  async function refreshTape() {
    if (!selectedVenue) return;
    const q = new URLSearchParams({
      venue: selectedVenue,
      symbol: selectedSymbol,
      limit: '80',
    });
    try {
      const data = await fetchJson(`/v1/tape?${q}`);
      tape = data.entries || [];
    } catch {
      tape = [];
    }
  }

  async function tickSlow() {
    try {
      await refreshStatus();
      await refreshInstruments();
      error = '';
    } catch (e) {
      error = String(e.message || e);
    }
  }

  async function tickFast() {
    try {
      await Promise.all([refreshBook(), refreshTape()]);
    } catch (e) {
      error = String(e.message || e);
    }
  }

  onMount(() => {
    tickSlow();
    tickFast();
    const slow = setInterval(tickSlow, 2000);
    const fast = setInterval(tickFast, 100); // 10 Hz books/tape
    return () => {
      clearInterval(slow);
      clearInterval(fast);
    };
  });

  function venueSymbols(id) {
    const v = instruments.venues?.find((x) => x.id === id);
    return v?.symbols || [];
  }

  function maxQty(levels) {
    let m = 0;
    for (const l of levels || []) m = Math.max(m, Number(l.quantity) || 0);
    return m || 1;
  }

  function barWidth(qty, max) {
    return `${Math.min(100, (Number(qty) / max) * 100)}%`;
  }

  function fmtTs(ns) {
    if (ns == null) return '';
    const ms = Number(ns) / 1e6;
    const d = new Date(ms);
    return d.toISOString().slice(11, 23);
  }
</script>

<div class="shell">
  <header class="top">
    <div class="brand">
      <span class="mark">MF</span>
      <div>
        <div class="title">marketfeed</div>
        <div class="sub">live view · loopback</div>
      </div>
    </div>
    <div class="health">
      {#if status}
        <span class:ok={status.live} class:bad={!status.live}>live</span>
        <span class:ok={status.ready} class:bad={!status.ready}>ready</span>
        <span class="muted">{status.lifecycle}</span>
        <span class="muted">{status.uptime_secs}s</span>
        {#if status.disk_pressure}
          <span class="bad">disk</span>
        {/if}
      {:else}
        <span class="muted">connecting…</span>
      {/if}
    </div>
    <div class="controls">
      <label>
        venue
        <select bind:value={selectedVenue} onchange={() => {
          const syms = venueSymbols(selectedVenue);
          selectedSymbol = syms[0]?.symbol || selectedSymbol;
          tickFast();
        }}>
          {#each instruments.venues || [] as v}
            <option value={v.id}>{v.id}</option>
          {/each}
        </select>
      </label>
      <label>
        symbol
        <select bind:value={selectedSymbol} onchange={tickFast}>
          {#each venueSymbols(selectedVenue) as s}
            <option value={s.symbol}>{s.symbol}</option>
          {/each}
        </select>
      </label>
    </div>
  </header>

  {#if error}
    <div class="err">{error}</div>
  {/if}

  <main class="grid">
    <section class="panel venues">
      <h2>venues</h2>
      <div class="venue-grid">
        {#each status?.venues || [] as v}
          <button
            class:active={v.id === selectedVenue}
            onclick={() => {
              selectedVenue = v.id;
              const syms = venueSymbols(v.id);
              if (syms[0]) selectedSymbol = syms[0].symbol;
              tickFast();
            }}
          >
            <div class="row">
              <strong>{v.id}</strong>
              <span class:ok={v.live} class:bad={!v.live}>{v.live ? 'LIVE' : 'DOWN'}</span>
            </div>
            <div class="meta">
              <span>{v.adapter}</span>
              <span>evt {v.events_dispatched}</span>
              <span>rc {v.reconnects}</span>
              <span>drop {v.events_dropped}</span>
            </div>
          </button>
        {/each}
      </div>
    </section>

    <section class="panel book">
      <h2>order book <span class="muted">{selectedSymbol}</span></h2>
      {#if book}
        {@const maxAsk = maxQty(book.asks)}
        {@const maxBid = maxQty(book.bids)}
        <div class="ladder">
          <div class="side asks">
            {#each [...(book.asks || [])].reverse() as lvl}
              <div class="lvl ask">
                <div class="bar" style={`width:${barWidth(lvl.quantity, maxAsk)}`}></div>
                <span class="px">{lvl.price}</span>
                <span class="qty">{lvl.quantity}</span>
              </div>
            {/each}
          </div>
          <div class="spread">
            {#if book.bids?.[0] && book.asks?.[0]}
              <span>{book.bids[0].price}</span>
              <span class="muted">×</span>
              <span>{book.asks[0].price}</span>
            {:else}
              <span class="muted">no bbo</span>
            {/if}
          </div>
          <div class="side bids">
            {#each book.bids || [] as lvl}
              <div class="lvl bid">
                <div class="bar" style={`width:${barWidth(lvl.quantity, maxBid)}`}></div>
                <span class="px">{lvl.price}</span>
                <span class="qty">{lvl.quantity}</span>
              </div>
            {/each}
          </div>
        </div>
      {:else}
        <div class="empty">waiting for book snapshot…</div>
      {/if}
    </section>

    <section class="panel tape">
      <h2>tape</h2>
      <div class="tape-list">
        {#each tape as e}
          {#if e.kind === 'trade'}
            <div class="trow trade" class:buy={e.aggressor === 'buy'} class:sell={e.aggressor === 'sell'}>
              <span class="ts">{fmtTs(e.receive_ts_ns)}</span>
              <span class="side">{e.aggressor}</span>
              <span class="px">{e.price}</span>
              <span class="qty">{e.quantity}</span>
            </div>
          {:else}
            <div class="trow quote">
              <span class="ts">{fmtTs(e.receive_ts_ns)}</span>
              <span class="side muted">q</span>
              <span class="px bidc">{e.bid_price}</span>
              <span class="px askc">{e.ask_price}</span>
            </div>
          {/if}
        {:else}
          <div class="empty">no tape yet</div>
        {/each}
      </div>
    </section>
  </main>
</div>

<style>
  .shell {
    height: 100%;
    display: flex;
    flex-direction: column;
    min-height: 0;
  }

  .top {
    display: flex;
    align-items: center;
    gap: 1.25rem;
    padding: 0.75rem 1rem;
    border-bottom: 1px solid var(--border);
    background: rgba(10, 12, 16, 0.85);
    backdrop-filter: blur(8px);
  }

  .brand {
    display: flex;
    gap: 0.7rem;
    align-items: center;
    min-width: 11rem;
  }

  .mark {
    font-family: var(--mono);
    font-weight: 700;
    letter-spacing: 0.04em;
    background: linear-gradient(135deg, #1a2030, #0f131a);
    border: 1px solid #2a3344;
    color: var(--accent);
    width: 2.2rem;
    height: 2.2rem;
    display: grid;
    place-items: center;
  }

  .title {
    font-weight: 650;
    letter-spacing: 0.02em;
  }

  .sub {
    color: var(--muted);
    font-size: 0.75rem;
  }

  .health {
    display: flex;
    gap: 0.65rem;
    font-family: var(--mono);
    font-size: 0.78rem;
    text-transform: uppercase;
  }

  .ok {
    color: var(--live);
  }
  .bad {
    color: var(--down);
  }
  .muted {
    color: var(--muted);
  }

  .controls {
    margin-left: auto;
    display: flex;
    gap: 0.75rem;
  }

  label {
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
    font-size: 0.7rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }

  .err {
    background: #2a1216;
    color: #ffb4b8;
    padding: 0.5rem 1rem;
    font-family: var(--mono);
    font-size: 0.8rem;
  }

  .grid {
    flex: 1;
    min-height: 0;
    display: grid;
    grid-template-columns: minmax(220px, 280px) minmax(280px, 1fr) minmax(280px, 1fr);
    gap: 1px;
    background: var(--border);
  }

  .panel {
    background: var(--panel);
    min-height: 0;
    display: flex;
    flex-direction: column;
    padding: 0.75rem;
  }

  h2 {
    margin: 0 0 0.6rem;
    font-size: 0.75rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--muted);
    font-weight: 600;
  }

  .venue-grid {
    display: flex;
    flex-direction: column;
    gap: 0.45rem;
    overflow: auto;
  }

  .venue-grid button {
    text-align: left;
    padding: 0.55rem 0.6rem;
  }

  .row {
    display: flex;
    justify-content: space-between;
    gap: 0.5rem;
    font-family: var(--mono);
    font-size: 0.82rem;
  }

  .meta {
    display: flex;
    flex-wrap: wrap;
    gap: 0.45rem;
    margin-top: 0.35rem;
    color: var(--muted);
    font-family: var(--mono);
    font-size: 0.68rem;
  }

  .ladder {
    font-family: var(--mono);
    font-size: 0.8rem;
    overflow: auto;
  }

  .lvl {
    position: relative;
    display: grid;
    grid-template-columns: 1fr 1fr;
    padding: 0.12rem 0.25rem;
  }

  .lvl .bar {
    position: absolute;
    inset: 0 auto 0 0;
    opacity: 0.9;
    pointer-events: none;
  }

  .ask .bar {
    background: var(--ask-dim);
  }
  .bid .bar {
    background: var(--bid-dim);
  }

  .px,
  .qty {
    position: relative;
    z-index: 1;
  }

  .ask .px {
    color: var(--ask);
  }
  .bid .px {
    color: var(--bid);
  }

  .qty {
    text-align: right;
    color: var(--text);
  }

  .spread {
    display: flex;
    justify-content: center;
    gap: 0.5rem;
    padding: 0.45rem 0;
    border-top: 1px solid var(--border);
    border-bottom: 1px solid var(--border);
    margin: 0.25rem 0;
    font-weight: 600;
  }

  .tape-list {
    overflow: auto;
    font-family: var(--mono);
    font-size: 0.76rem;
  }

  .trow {
    display: grid;
    grid-template-columns: 5.5rem 3rem 1fr 1fr;
    gap: 0.35rem;
    padding: 0.15rem 0.2rem;
    border-bottom: 1px solid rgba(30, 36, 48, 0.7);
  }

  .trow.buy .side,
  .trow.buy .px {
    color: var(--bid);
  }
  .trow.sell .side,
  .trow.sell .px {
    color: var(--ask);
  }
  .bidc {
    color: var(--bid);
  }
  .askc {
    color: var(--ask);
  }

  .empty {
    color: var(--muted);
    font-family: var(--mono);
    font-size: 0.8rem;
    padding: 1rem 0;
  }

  @media (max-width: 960px) {
    .grid {
      grid-template-columns: 1fr;
      grid-template-rows: auto 50vh 40vh;
    }
    .top {
      flex-wrap: wrap;
    }
    .controls {
      margin-left: 0;
      width: 100%;
    }
  }
</style>
