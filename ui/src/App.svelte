<script>
  import { onMount } from 'svelte';
  import { bookQuery, fetchJson, tapeQuery } from './lib/api.js';
  import { assetCoverage, listAssets, mapAssetToVenues } from './lib/assets.js';
  import { CandleBuilder, statsWindowForTf } from './lib/ohlcv.js';
  import { MultiVenueTracker, TIMEFRAMES } from './lib/series.js';
  import { loadSettings, saveSettings } from './lib/settings.js';
  import { nsToSec } from './lib/format.js';
  import HeaderBar from './components/HeaderBar.svelte';
  import OrderBook from './components/OrderBook.svelte';
  import PriceChart from './components/PriceChart.svelte';
  import MarketTrades from './components/MarketTrades.svelte';
  import MarketsList from './components/MarketsList.svelte';
  import StatusBar from './components/StatusBar.svelte';

  const initial = loadSettings();

  let status = $state(null);
  let instruments = $state({ venues: [] });
  let book = $state(null);
  let tape = $state([]);
  let error = $state('');
  let connected = $state(false);

  let selectedAsset = $state(initial.asset || 'BTC');
  let selectedVenue = $state('');
  let selectedSymbol = $state('');
  let timeframe = $state(initial.timeframe || '1s');
  let chartMode = $state(initial.chartMode || 'lines');
  let priceMode = $state(initial.priceMode || 'percent');
  let showVolume = $state(initial.showVolume !== false);
  let bookDepth = $state(initial.bookDepth || 16);
  let tapeLimit = $state(initial.tapeLimit || 120);
  let pollFocusMs = $state(initial.pollFocusMs || 120);
  let pollMultiMs = $state(initial.pollMultiMs || 220);
  let hiddenVenues = $state(new Set(initial.hiddenVenues || []));
  let statsMode = $state('window');
  let highlightSec = $state(null);
  let selectedTradeId = $state(null);

  let lineSeries = $state([]);
  let discrepancy = $state(null);
  let multiAggregate = $state({ volume: 0, trades: 0 });
  let candles = $state([]);
  let volumeBars = $state([]);
  let eventsPerSec = $state(null);
  let priceDir = $state(0);
  let lastTradePrice = $state(null);
  let sessionHigh = $state(null);
  let sessionLow = $state(null);
  let sessionVolume = $state(null);
  let sessionTrades = $state(null);
  let windowVolume = $state(null);
  let windowTrades = $state(null);

  const tracker = new MultiVenueTracker(1);
  const candleBuilder = new CandleBuilder(1);
  let lastEvents = null;
  let lastEventsAt = 0;
  let assetKey = '';
  let focusKey = '';
  let multiBusy = false;
  let rr = 0;
  let focusTimer = null;
  let multiTimer = null;

  let assets = $derived(listAssets(instruments));
  let coverage = $derived(assetCoverage(instruments, status));
  let mapped = $derived(mapAssetToVenues(instruments, selectedAsset, status));
  let liveMapped = $derived(mapped.filter((m) => m.live).length);
  let marketQuotes = $derived(
    (lineSeries || []).map((s) => ({ venue: s.venue, last: s.last ?? null, pct: s.pct ?? null })),
  );
  let windowSec = $derived(statsWindowForTf(timeframe));
  let tapeVol = $derived(
    (tape || [])
      .filter((e) => e.kind === 'trade')
      .reduce((s, e) => s + (Number(e.quantity) || 0), 0),
  );
  let tapeTradeCount = $derived((tape || []).filter((e) => e.kind === 'trade').length);

  function persist(patch) {
    saveSettings(patch);
  }

  function syncLineView() {
    const snap = tracker.snapshot(priceMode, { hidden: hiddenVenues });
    lineSeries = snap.series;
    discrepancy = snap.discrepancy;
    multiAggregate = snap.aggregate || { volume: 0, trades: 0 };
  }

  function syncCandleView() {
    candles = candleBuilder.candles();
    volumeBars = candleBuilder.volumeBars();
    lastTradePrice = candleBuilder.lastPrice;
    sessionHigh = candleBuilder.sessionHigh;
    sessionLow = candleBuilder.sessionLow;
    sessionVolume = candleBuilder.sessionVolume;
    sessionTrades = candleBuilder.sessionTrades;
    const w = candleBuilder.windowStats(statsWindowForTf(timeframe));
    windowVolume = w.volume;
    windowTrades = w.trades;
  }

  function ensureFocusVenue() {
    if (!mapped.length) return;
    const still = mapped.find((m) => m.venue === selectedVenue);
    if (still) {
      selectedSymbol = still.symbol;
      return;
    }
    const prefer =
      mapped.find((m) => m.venue === 'binance-spot') ||
      mapped.find((m) => m.live) ||
      mapped[0];
    selectedVenue = prefer.venue;
    selectedSymbol = prefer.symbol;
  }

  function onAssetChange(asset) {
    if (asset === selectedAsset) return;
    selectedAsset = asset;
    persist({ asset });
    ensureFocusVenue();
    resetAssetSeries(true);
    tickFocus();
    tickMulti();
  }

  function resetAssetSeries(force = false) {
    const key = selectedAsset;
    if (!force && key === assetKey) return;
    assetKey = key;
    tracker.clear();
    tracker.syncTargets(mapAssetToVenues(instruments, selectedAsset, status));
    tracker.setInterval(TIMEFRAMES.find((t) => t.id === timeframe)?.sec || 1);
    lineSeries = [];
    discrepancy = null;
    multiAggregate = { volume: 0, trades: 0 };
    resetFocusSeries(true);
  }

  function resetFocusSeries(force = false) {
    const key = `${selectedVenue}|${selectedSymbol}`;
    if (!force && key === focusKey) return;
    focusKey = key;
    candleBuilder.reset();
    candles = [];
    volumeBars = [];
    lastTradePrice = null;
    sessionHigh = null;
    sessionLow = null;
    sessionVolume = null;
    sessionTrades = null;
    windowVolume = null;
    windowTrades = null;
    priceDir = 0;
    book = null;
    tape = [];
    highlightSec = null;
    selectedTradeId = null;
  }

  function applyTimeframe(id) {
    timeframe = id;
    persist({ timeframe: id });
    const sec = TIMEFRAMES.find((t) => t.id === id)?.sec || 1;
    tracker.setInterval(sec);
    candleBuilder.setInterval(sec);
    syncLineView();
    syncCandleView();
  }

  function selectMarket(venue, symbol) {
    selectedVenue = venue;
    selectedSymbol = symbol;
    const hit = mapAssetToVenues(instruments, selectedAsset, status).find(
      (m) => m.venue === venue && m.symbol === symbol,
    );
    if (!hit) {
      for (const a of assets) {
        const m = mapAssetToVenues(instruments, a, status).find(
          (x) => x.venue === venue && x.symbol === symbol,
        );
        if (m) {
          selectedAsset = a;
          persist({ asset: a });
          assetKey = '';
          resetAssetSeries();
          break;
        }
      }
    }
    resetFocusSeries();
    tickFocus();
  }

  function toggleVenue(venue) {
    const next = new Set(hiddenVenues);
    if (next.has(venue)) next.delete(venue);
    else next.add(venue);
    hiddenVenues = next;
    persist({ hiddenVenues: [...next] });
    syncLineView();
  }

  function focusFromLegend(venue, symbol) {
    selectMarket(venue, symbol);
  }

  function onSelectTrade(e) {
    selectedTradeId = e.trade_id ?? null;
    highlightSec = nsToSec(e.exchange_ts_ns ?? e.receive_ts_ns);
  }

  function setBookDepth(n) {
    const v = Math.min(50, Math.max(5, Math.round(Number(n) || 16)));
    bookDepth = v;
    persist({ bookDepth: v });
    refreshBook();
  }

  function setTapeLimit(n) {
    const v = Math.min(500, Math.max(20, Math.round(Number(n) || 120)));
    tapeLimit = v;
    persist({ tapeLimit: v });
  }

  function rescheduleFocus(ms) {
    const v = Math.min(2000, Math.max(80, Math.round(Number(ms) || 120)));
    pollFocusMs = v;
    persist({ pollFocusMs: v });
    if (focusTimer) clearInterval(focusTimer);
    focusTimer = setInterval(tickFocus, v);
  }

  function rescheduleMulti(ms) {
    const v = Math.min(5000, Math.max(100, Math.round(Number(ms) || 220)));
    pollMultiMs = v;
    persist({ pollMultiMs: v });
    if (multiTimer) clearInterval(multiTimer);
    multiTimer = setInterval(tickMulti, v);
  }

  async function refreshStatus() {
    status = await fetchJson('/v1/status');
    connected = true;
    const v = (status.venues || []).find((x) => x.id === selectedVenue);
    if (v) {
      const now = performance.now();
      if (lastEvents != null && lastEventsAt > 0) {
        const dt = (now - lastEventsAt) / 1000;
        if (dt > 0.15) {
          eventsPerSec = Math.max(0, (v.events_dispatched - lastEvents) / dt);
        }
      }
      lastEvents = v.events_dispatched;
      lastEventsAt = now;
    }
  }

  async function refreshInstruments() {
    instruments = await fetchJson('/v1/instruments');
    if (!assets.includes(selectedAsset) && assets.length) {
      selectedAsset = assets[0];
      assetKey = '';
    }
    ensureFocusVenue();
    if (selectedAsset !== assetKey) {
      resetAssetSeries(true);
    } else {
      tracker.syncTargets(mapAssetToVenues(instruments, selectedAsset, status));
    }
  }

  async function refreshBook() {
    if (!selectedVenue || !selectedSymbol) return;
    try {
      book = await fetchJson(bookQuery(selectedVenue, selectedSymbol, bookDepth));
      const b = Number(book?.bids?.[0]?.price);
      const a = Number(book?.asks?.[0]?.price);
      if (Number.isFinite(b) && Number.isFinite(a)) {
        const midPx = (b + a) / 2;
        candleBuilder.touchPrice(midPx);
        tracker.touch(selectedVenue, midPx);
        syncCandleView();
        syncLineView();
      }
    } catch {
      book = null;
    }
  }

  async function refreshFocusTape() {
    if (!selectedVenue || !selectedSymbol) return;
    try {
      // Trades-only ring for panel + OHLCV; quotes still arrive via multi poll / book mid.
      const data = await fetchJson(
        tapeQuery(selectedVenue, selectedSymbol, tapeLimit, 'trade'),
      );
      tape = data.entries || [];
      const prev = candleBuilder.lastPrice;
      candleBuilder.ingest(tape);
      tracker.ingest(selectedVenue, tape);
      syncCandleView();
      syncLineView();
      if (candleBuilder.lastPrice != null && prev != null) {
        if (candleBuilder.lastPrice > prev) priceDir = 1;
        else if (candleBuilder.lastPrice < prev) priceDir = -1;
      }
    } catch {
      tape = [];
    }
  }

  async function tickMulti() {
    if (multiBusy) return;
    const targets = mapped.filter((m) => m.live || m.live == null);
    if (!targets.length) return;
    multiBusy = true;
    try {
      tracker.syncTargets(mapped);
      const batchSize = 4;
      const start = rr % targets.length;
      rr = (rr + batchSize) % Math.max(targets.length, 1);
      const batch = [];
      for (let i = 0; i < Math.min(batchSize, targets.length); i++) {
        batch.push(targets[(start + i) % targets.length]);
      }
      await Promise.all(
        batch.map(async (t) => {
          try {
            // Mixed tape for lines (quotes fill gaps); trades also accumulate volume.
            const data = await fetchJson(tapeQuery(t.venue, t.symbol, Math.min(80, tapeLimit)));
            tracker.ingest(t.venue, data.entries || []);
          } catch {
            /* venue tape may be empty */
          }
        }),
      );
      syncLineView();
    } finally {
      multiBusy = false;
    }
  }

  async function tickSlow() {
    try {
      await refreshInstruments();
      await refreshStatus();
      error = '';
    } catch (e) {
      connected = false;
      error = String(e.message || e);
    }
  }

  async function tickFocus() {
    try {
      await Promise.all([refreshBook(), refreshFocusTape()]);
    } catch (e) {
      error = String(e.message || e);
    }
  }

  onMount(() => {
    const tfSec = TIMEFRAMES.find((t) => t.id === timeframe)?.sec || 1;
    tracker.setInterval(tfSec);
    candleBuilder.setInterval(tfSec);

    tickSlow()
      .then(() => Promise.all([tickFocus(), tickMulti()]))
      .catch(() => {});
    const slow = setInterval(tickSlow, 2000);
    const mid = setInterval(() => {
      refreshStatus().catch(() => {});
    }, 1000);
    focusTimer = setInterval(tickFocus, pollFocusMs);
    multiTimer = setInterval(tickMulti, pollMultiMs);
    return () => {
      clearInterval(slow);
      clearInterval(mid);
      if (focusTimer) clearInterval(focusTimer);
      if (multiTimer) clearInterval(multiTimer);
    };
  });

  let bid = $derived(book?.bids?.[0] ? Number(book.bids[0].price) : null);
  let ask = $derived(book?.asks?.[0] ? Number(book.asks[0].price) : null);
  let mid = $derived(
    bid != null && ask != null && Number.isFinite(bid) && Number.isFinite(ask)
      ? (bid + ask) / 2
      : null,
  );
  let spread = $derived(
    bid != null && ask != null && Number.isFinite(bid) && Number.isFinite(ask)
      ? ask - bid
      : null,
  );
  let spreadBps = $derived(
    mid != null && spread != null && mid > 0 ? (spread / mid) * 10000 : null,
  );
  let lastPrice = $derived(lastTradePrice ?? mid);
  let venueLive = $derived(
    !!(status?.venues || []).find((v) => v.id === selectedVenue)?.live,
  );
  let crossBps = $derived(discrepancy?.bps ?? null);
</script>

<div class="terminal">
  <HeaderBar
    asset={selectedAsset}
    venue={selectedVenue}
    symbol={selectedSymbol}
    {chartMode}
    {lastPrice}
    {priceDir}
    {bid}
    {ask}
    {mid}
    {spread}
    {spreadBps}
    {sessionHigh}
    {sessionLow}
    {sessionVolume}
    {sessionTrades}
    {windowVolume}
    {windowTrades}
    {windowSec}
    {eventsPerSec}
    {venueLive}
    mappedVenues={mapped.length}
    {liveMapped}
    {crossBps}
    multiVolume={multiAggregate.volume}
    multiTrades={multiAggregate.trades}
    {statsMode}
    onStatsMode={(m) => (statsMode = m)}
  />

  <div class="workspace">
    <aside class="col-book">
      <OrderBook
        {book}
        {lastPrice}
        {priceDir}
        depth={bookDepth}
        onDepth={setBookDepth}
      />
    </aside>

    <section class="col-chart">
      <PriceChart
        series={lineSeries}
        {candles}
        {volumeBars}
        {chartMode}
        {priceMode}
        {timeframe}
        asset={selectedAsset}
        {discrepancy}
        {assets}
        {coverage}
        {showVolume}
        {bookDepth}
        {tapeLimit}
        {pollFocusMs}
        {pollMultiMs}
        focusVenue={selectedVenue}
        {highlightSec}
        onTimeframe={applyTimeframe}
        onChartMode={(m) => {
          chartMode = m;
          persist({ chartMode: m });
        }}
        onPriceMode={(m) => {
          priceMode = m;
          persist({ priceMode: m });
          syncLineView();
        }}
        onAsset={onAssetChange}
        onToggleVenue={toggleVenue}
        onFocusVenue={focusFromLegend}
        onShowVolume={(v) => {
          showVolume = v;
          persist({ showVolume: v });
        }}
        onBookDepth={setBookDepth}
        onTapeLimit={setTapeLimit}
        onPollFocus={rescheduleFocus}
        onPollMulti={rescheduleMulti}
      />
    </section>

    <aside class="col-right">
      <div class="markets-pane">
        <MarketsList
          {instruments}
          {status}
          {selectedVenue}
          {selectedSymbol}
          selectedAsset={selectedAsset}
          quotes={marketQuotes}
          onSelect={selectMarket}
          onAsset={onAssetChange}
        />
      </div>
      <div class="trades-pane">
        <MarketTrades
          {tape}
          tradeCount={tapeTradeCount}
          volume={tapeVol}
          {selectedTradeId}
          onSelectTrade={onSelectTrade}
        />
      </div>
    </aside>
  </div>

  <StatusBar {status} {error} {connected} />
</div>

<style>
  .terminal {
    height: 100%;
    display: flex;
    flex-direction: column;
    min-height: 0;
    background: var(--bg);
  }

  .workspace {
    flex: 1;
    min-height: 0;
    display: grid;
    grid-template-columns: minmax(220px, 280px) minmax(0, 1fr) minmax(280px, 340px);
  }

  .col-book,
  .col-chart,
  .col-right {
    min-height: 0;
    min-width: 0;
  }

  .col-right {
    display: flex;
    flex-direction: column;
    border-left: 1px solid var(--border);
  }

  .markets-pane {
    flex: 0 0 48%;
    min-height: 0;
  }

  .trades-pane {
    flex: 1;
    min-height: 0;
  }

  @media (max-width: 1100px) {
    .workspace {
      grid-template-columns: minmax(200px, 240px) minmax(0, 1fr);
      grid-template-rows: 1fr 40vh;
    }
    .col-right {
      grid-column: 1 / -1;
      flex-direction: row;
      border-left: none;
      border-top: 1px solid var(--border);
    }
    .markets-pane,
    .trades-pane {
      flex: 1;
    }
  }

  @media (max-width: 720px) {
    .workspace {
      grid-template-columns: 1fr;
      grid-template-rows: 45vh 40vh auto;
    }
    .col-right {
      flex-direction: column;
      max-height: 50vh;
    }
  }
</style>
