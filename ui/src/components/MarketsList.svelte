<script>
  import { listMarkets, kindLabel } from '../lib/assets.js';
  import { displayPair, fmtPrice, fmtPct } from '../lib/format.js';
  import { loadSettings, saveSettings } from '../lib/settings.js';

  let {
    instruments = { venues: [] },
    status = null,
    selectedVenue = '',
    selectedSymbol = '',
    selectedAsset = '',
    /** @type {Array<{venue:string,last:number|null,pct:number|null}>} */
    quotes = [],
    /** @type {Map<string, {badges:string[]}>} */
    qualityMap = new Map(),
    searchRef = $bindable(null),
    watchlists = [],
    activeWatchlist = '',
    onSelect = () => {},
    onAsset = () => {},
    onWatchlist = () => {},
    onSaveWatchlist = () => {},
  } = $props();

  const saved = loadSettings();
  let groupMode = $state(saved.marketsGroup || 'asset'); // asset | venue | all
  let filter = $state('');
  let liveFilter = $state(saved.marketsLiveFilter || 'all'); // all | live | offline
  let kindFilter = $state(saved.marketsKindFilter || 'all'); // all | spot | perp
  /** Explicitly collapsed / expanded section keys. */
  let collapsed = $state(new Set());
  let expanded = $state(new Set());

  let quoteMap = $derived.by(() => {
    const m = new Map();
    for (const q of quotes || []) m.set(q.venue, q);
    return m;
  });

  let allRows = $derived(listMarkets(instruments, status));

  let filtered = $derived.by(() => {
    const q = filter.trim().toLowerCase();
    return allRows.filter((r) => {
      if (liveFilter === 'live' && !r.live) return false;
      if (liveFilter === 'offline' && r.live) return false;
      if (kindFilter === 'spot' && r.kind !== 'spot') return false;
      if (kindFilter === 'perp' && r.kind !== 'perp') return false;
      if (!q) return true;
      return (
        r.venue.toLowerCase().includes(q) ||
        r.symbol.toLowerCase().includes(q) ||
        r.adapter.toLowerCase().includes(q) ||
        (r.asset && r.asset.toLowerCase().includes(q)) ||
        kindLabel(r.kind).includes(q)
      );
    });
  });

  let sections = $derived.by(() => {
    if (groupMode === 'all') {
      return [
        {
          key: 'all',
          title: 'All markets',
          meta: `${filtered.length}`,
          rows: sortRows(filtered),
        },
      ];
    }

    /** @type {Map<string, typeof filtered>} */
    const buckets = new Map();
    for (const r of filtered) {
      const key =
        groupMode === 'asset' ? r.asset || 'UNKNOWN' : r.venue || 'UNKNOWN';
      if (!buckets.has(key)) buckets.set(key, []);
      buckets.get(key).push(r);
    }

    const keys = [...buckets.keys()].sort((a, b) => {
      if (groupMode === 'asset') {
        const rank = { BTC: 0, ETH: 1, SOL: 2, XRP: 3, BNB: 4 };
        const prefer = (rank[a] ?? 50) - (rank[b] ?? 50) || a.localeCompare(b);
        if (a === selectedAsset) return -1;
        if (b === selectedAsset) return 1;
        return prefer;
      }
      if (a === selectedVenue) return -1;
      if (b === selectedVenue) return 1;
      return a.localeCompare(b);
    });

    return keys.map((key) => {
      const rows = sortRows(buckets.get(key));
      const live = rows.filter((r) => r.live).length;
      if (groupMode === 'asset') {
        return {
          key,
          title: key,
          meta: `${live}/${rows.length}`,
          accent: key === selectedAsset,
          rows,
        };
      }
      const kind = rows[0]?.kind || 'other';
      const liveOk = rows.some((r) => r.live);
      return {
        key,
        title: key,
        meta: `${kindLabel(kind)} · ${liveOk ? 'live' : 'off'}`,
        accent: key === selectedVenue,
        rows,
      };
    });
  });

  let totals = $derived.by(() => {
    const live = filtered.filter((r) => r.live).length;
    return { total: filtered.length, live };
  });

  function sortRows(rows) {
    // Stable order — do NOT sort by live (live/stale flips were reordering rows).
    return [...rows].sort((a, b) => {
      if (a.asset === selectedAsset && b.asset !== selectedAsset) return -1;
      if (b.asset === selectedAsset && a.asset !== selectedAsset) return 1;
      return (
        a.venue.localeCompare(b.venue) || a.symbol.localeCompare(b.symbol)
      );
    });
  }

  function persistView(patch) {
    saveSettings(patch);
  }

  function setGroup(mode) {
    groupMode = mode;
    persistView({ marketsGroup: mode });
  }

  function setLive(f) {
    liveFilter = f;
    persistView({ marketsLiveFilter: f });
  }

  function setKind(f) {
    kindFilter = f;
    persistView({ marketsKindFilter: f });
  }

  function sectionOpen(sec) {
    if (groupMode === 'all') return true;
    if (expanded.has(sec.key)) return true;
    if (collapsed.has(sec.key)) return false;
    if (filter.trim()) return true;
    if (sections.length <= 5) return true;
    return !!sec.accent;
  }

  function toggleSection(key) {
    const sec = sections.find((s) => s.key === key);
    const open = sec ? sectionOpen(sec) : false;
    const nextExp = new Set(expanded);
    const nextCol = new Set(collapsed);
    if (open) {
      nextExp.delete(key);
      nextCol.add(key);
    } else {
      nextCol.delete(key);
      nextExp.add(key);
    }
    expanded = nextExp;
    collapsed = nextCol;
  }

  function onSectionClick(sec) {
    if (groupMode === 'asset' && sec.key && sec.key !== 'UNKNOWN' && sec.key !== selectedAsset) {
      onAsset(sec.key);
      const nextExp = new Set(expanded);
      nextExp.add(sec.key);
      expanded = nextExp;
      const nextCol = new Set(collapsed);
      nextCol.delete(sec.key);
      collapsed = nextCol;
      return;
    }
    toggleSection(sec.key);
  }

  function onRowClick(r) {
    onSelect(r.venue, r.symbol);
  }

  function rowQuote(r) {
    if (r.asset !== selectedAsset) return null;
    return quoteMap.get(r.venue) || null;
  }
</script>

<section class="markets">
  <div class="head">
    <div class="title-row">
      <span class="title">Markets</span>
      <span class="count">{totals.live}/{totals.total}</span>
    </div>
    <div class="tabs">
      <button type="button" class:active={groupMode === 'asset'} onclick={() => setGroup('asset')}>By Asset</button>
      <button type="button" class:active={groupMode === 'venue'} onclick={() => setGroup('venue')}>By Venue</button>
      <button type="button" class:active={groupMode === 'all'} onclick={() => setGroup('all')}>All</button>
    </div>
    <input
      type="search"
      placeholder="Search asset, venue, symbol… (/)"
      bind:value={filter}
      bind:this={searchRef}
      spellcheck="false"
    />
    {#if watchlists.length}
      <div class="watchlists">
        {#each watchlists as wl}
          <button type="button" class:active={activeWatchlist === wl.id} onclick={() => onWatchlist(wl.id)}>{wl.name}</button>
        {/each}
        <button type="button" class="save-wl" onclick={() => onSaveWatchlist()} title="Save current asset as watchlist">+</button>
      </div>
    {:else}
      <button type="button" class="save-wl solo" onclick={() => onSaveWatchlist()}>Save watchlist</button>
    {/if}
    <div class="chips">
      <button type="button" class:active={liveFilter === 'all'} onclick={() => setLive('all')}>All</button>
      <button type="button" class:active={liveFilter === 'live'} onclick={() => setLive('live')}>
        <span class="dot ok"></span>Live
      </button>
      <button type="button" class:active={liveFilter === 'offline'} onclick={() => setLive('offline')}>
        <span class="dot bad"></span>Off
      </button>
      <span class="sep"></span>
      <button type="button" class:active={kindFilter === 'all'} onclick={() => setKind('all')}>Any</button>
      <button type="button" class:active={kindFilter === 'spot'} onclick={() => setKind('spot')}>Spot</button>
      <button type="button" class:active={kindFilter === 'perp'} onclick={() => setKind('perp')}>Perp</button>
    </div>
  </div>

  <div class="cols">
    <span>{groupMode === 'venue' ? 'Symbol' : 'Market'}</span>
    <span>{groupMode === 'asset' ? 'Venue' : groupMode === 'venue' ? 'Kind' : 'Venue'}</span>
    <span>Last</span>
    <span>%</span>
    <span></span>
  </div>

  <div class="list">
    {#each sections as sec (sec.key)}
      {#if groupMode !== 'all'}
        <button
          type="button"
          class="section"
          class:accent={sec.accent}
          onclick={() => onSectionClick(sec)}
        >
          <span class="chev">{sectionOpen(sec) ? '▾' : '▸'}</span>
          <span class="sec-title">{sec.title}</span>
          <span class="sec-meta">{sec.meta}</span>
          {#if groupMode === 'asset' && sec.key === selectedAsset}
            <span class="watching">watching</span>
          {/if}
        </button>
      {/if}

      {#if sectionOpen(sec)}
        {#each sec.rows as r (r.venue + '|' + r.symbol)}
          {@const q = rowQuote(r)}
          <button
            type="button"
            class="row"
            class:active={r.venue === selectedVenue && r.symbol === selectedSymbol}
            class:asset-match={r.asset === selectedAsset}
            onclick={() => onRowClick(r)}
          >
            <span class="sym" title={r.symbol}>
              {#if groupMode === 'asset'}
                {displayPair(r.symbol)}
              {:else if groupMode === 'venue'}
                <span class="asset-tag">{r.asset || '?'}</span>
                {displayPair(r.symbol)}
              {:else}
                <span class="asset-tag">{r.asset || '?'}</span>
                {displayPair(r.symbol)}
              {/if}
            </span>
            <span class="venue" title={groupMode === 'venue' ? kindLabel(r.kind) : r.venue}>
              {#if groupMode === 'venue'}
                <span class="kind">{kindLabel(r.kind)}</span>
              {:else}
                {r.venue}
              {/if}
            </span>
            <span class="px">
              {q?.last != null ? fmtPrice(q.last, 2) : '—'}
            </span>
            <span class="pct" class:up={(q?.pct ?? 0) > 0} class:down={(q?.pct ?? 0) < 0}>
              {q?.pct != null ? fmtPct(q.pct, 2) : '—'}
            </span>
            <span class="chip" class:ok={r.live} class:bad={!r.live} title={r.live ? 'live' : 'offline'}>
              {r.live ? '●' : '○'}
            </span>
            <span class="badges">
              {#each (qualityMap.get(r.venue + '|' + r.symbol)?.badges || []) as badge}
                <span class="badge" class:warn={badge === 'stale' || badge === 'lag'} title={badge}>{badge}</span>
              {/each}
            </span>
          </button>
        {:else}
          <div class="empty">no markets</div>
        {/each}
      {/if}
    {:else}
      <div class="empty">no instruments</div>
    {/each}
  </div>
</section>

<style>
  .markets {
    display: flex;
    flex-direction: column;
    min-height: 0;
    height: 100%;
    background: var(--panel);
    border-bottom: 1px solid var(--border);
  }

  .head {
    display: flex;
    flex-direction: column;
    gap: 0.28rem;
    padding: 0.4rem 0.45rem 0.35rem;
    flex-shrink: 0;
  }

  .title-row {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 0.4rem;
  }

  .title {
    font-size: 0.72rem;
    font-weight: 600;
  }

  .count {
    font-family: var(--mono);
    font-size: 0.62rem;
    color: var(--muted);
  }

  .tabs {
    display: flex;
    gap: 0.15rem;
  }

  .tabs button,
  .chips button {
    background: transparent;
    border: 1px solid transparent;
    color: var(--muted);
    font-family: var(--mono);
    font-size: 0.62rem;
    padding: 0.12rem 0.35rem;
    cursor: pointer;
    border-radius: 2px;
  }

  .tabs button:hover,
  .chips button:hover {
    color: var(--text);
    background: var(--panel-2);
  }

  .tabs button.active {
    color: var(--accent);
    border-color: rgba(240, 185, 11, 0.35);
    background: rgba(240, 185, 11, 0.08);
  }

  .chips {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.12rem;
  }

  .chips button.active {
    color: var(--text);
    border-color: var(--border);
    background: var(--panel-2);
  }

  .chips .dot {
    display: inline-block;
    width: 5px;
    height: 5px;
    border-radius: 50%;
    margin-right: 0.2rem;
    vertical-align: middle;
  }
  .chips .dot.ok {
    background: var(--bid);
  }
  .chips .dot.bad {
    background: var(--ask);
  }

  .sep {
    width: 1px;
    height: 0.85rem;
    background: var(--border);
    margin: 0 0.2rem;
  }

  input {
    width: 100%;
    background: var(--bg);
    border: 1px solid var(--border);
    color: var(--text);
    font-size: 0.7rem;
    padding: 0.26rem 0.4rem;
    border-radius: 2px;
    outline: none;
    font-family: var(--mono);
  }

  input:focus {
    border-color: #2b3139;
  }

  .cols {
    display: grid;
    grid-template-columns: minmax(0, 1.15fr) minmax(0, 1.25fr) 3.4rem 2.6rem minmax(2rem, auto);
    padding: 0.12rem 0.45rem;
    font-size: 0.58rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.03em;
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
    gap: 0.2rem;
  }

  .list {
    overflow: auto;
    flex: 1;
    min-height: 0;
  }

  .section {
    display: flex;
    align-items: center;
    gap: 0.35rem;
    width: 100%;
    text-align: left;
    background: var(--panel-2);
    border: none;
    border-bottom: 1px solid var(--border);
    border-top: 1px solid transparent;
    padding: 0.22rem 0.45rem;
    cursor: pointer;
    color: var(--text-dim);
    font: inherit;
    position: sticky;
    top: 0;
    z-index: 1;
  }

  .section.accent {
    color: var(--text);
    border-left: 2px solid var(--accent);
    padding-left: calc(0.45rem - 2px);
  }

  .chev {
    font-size: 0.6rem;
    color: var(--muted);
    width: 0.7rem;
  }

  .sec-title {
    font-family: var(--mono);
    font-size: 0.68rem;
    font-weight: 700;
  }

  .sec-meta {
    font-family: var(--mono);
    font-size: 0.58rem;
    color: var(--muted);
    margin-left: auto;
  }

  .watching {
    font-family: var(--mono);
    font-size: 0.55rem;
    color: #0b0e11;
    background: var(--accent);
    padding: 0.02rem 0.28rem;
    border-radius: 1px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.02em;
  }

  .row {
    display: grid;
    grid-template-columns: minmax(0, 1.15fr) minmax(0, 1.25fr) 3.4rem 2.6rem minmax(2rem, auto);
    width: 100%;
    text-align: left;
    background: transparent;
    border: none;
    border-bottom: 1px solid rgba(30, 35, 41, 0.55);
    padding: 0.22rem 0.45rem;
    cursor: pointer;
    color: inherit;
    font: inherit;
    border-radius: 0;
    gap: 0.2rem;
    align-items: center;
  }

  .row:hover {
    background: var(--panel-2);
  }

  .row.active {
    background: rgba(240, 185, 11, 0.1);
    box-shadow: inset 2px 0 0 var(--accent);
  }

  .row.asset-match:not(.active) .sym {
    color: var(--text);
  }

  .sym {
    font-family: var(--mono);
    font-size: 0.68rem;
    font-weight: 600;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    display: flex;
    align-items: baseline;
    gap: 0.25rem;
    min-width: 0;
  }

  .asset-tag {
    color: var(--muted);
    font-weight: 500;
    font-size: 0.58rem;
    flex-shrink: 0;
  }

  .venue {
    font-family: var(--mono);
    font-size: 0.6rem;
    color: var(--muted);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .kind {
    color: var(--text-dim);
    text-transform: lowercase;
  }

  .px {
    font-family: var(--mono);
    font-size: 0.62rem;
    text-align: right;
    color: var(--text-dim);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .pct {
    font-family: var(--mono);
    font-size: 0.6rem;
    text-align: right;
    color: var(--muted);
  }
  .pct.up {
    color: var(--bid);
  }
  .pct.down {
    color: var(--ask);
  }

  .chip {
    text-align: center;
    font-size: 0.58rem;
    line-height: 1;
  }
  .chip.ok {
    color: var(--bid);
  }
  .chip.bad {
    color: var(--ask);
  }

  .badge {
    font-family: var(--mono);
    font-size: 0.48rem;
    text-transform: uppercase;
    color: var(--muted);
    border: 1px solid var(--border);
    padding: 0 0.15rem;
    line-height: 1.2;
    min-width: 2.4rem;
    text-align: center;
    /* Avoid layout jump when STALE/LAG mount — opacity only when empty slot unused */
  }

  .badge.warn {
    color: #fb923c;
    border-color: rgba(251, 146, 60, 0.4);
  }

  .badges {
    display: inline-flex;
    gap: 0.12rem;
    min-width: 5.2rem;
    justify-content: flex-start;
    align-items: center;
  }

  .watchlists {
    display: flex;
    gap: 0.12rem;
    flex-wrap: wrap;
  }

  .watchlists button,
  .save-wl {
    background: transparent;
    border: 1px solid var(--border);
    color: var(--muted);
    font-family: var(--mono);
    font-size: 0.58rem;
    padding: 0.08rem 0.3rem;
    cursor: pointer;
  }

  .watchlists button.active {
    color: var(--accent);
    border-color: rgba(240, 185, 11, 0.35);
  }

  .save-wl.solo {
    align-self: flex-start;
    margin-top: 0.15rem;
  }

  .empty {
    color: var(--muted);
    font-size: 0.68rem;
    padding: 0.55rem 0.5rem;
    font-family: var(--mono);
  }
</style>
