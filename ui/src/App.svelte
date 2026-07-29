<script>
  import { onMount } from 'svelte';
  import { bookQuery, fetchJson, tapeQuery } from './lib/api.js';
  import { assetCoverage, listAssets, mapAssetToVenues } from './lib/assets.js';
  import { CandleBuilder } from './lib/ohlcv.js';
  import { MultiVenueTracker, TIMEFRAMES } from './lib/series.js';
  import { loadSettings, saveSettings, saveWatchlist } from './lib/settings.js';
  import { syncUrl } from './lib/urlState.js';
  import { sessionWindowSec } from './lib/session.js';
  import { DiscrepancyTracker } from './lib/discrepancy.js';
  import { StreamClient } from './lib/stream.js';
  import { createAlert, sendWebhook, testDaemonAlert } from './lib/alerts.js';
  import { marketQuality, lastTapeSec, isQuotesOnly } from './lib/quality.js';
  import { nsToSec } from './lib/format.js';
  import HeaderBar from './components/HeaderBar.svelte';
  import OrderBook from './components/OrderBook.svelte';
  import PriceChart from './components/PriceChart.svelte';
  import MarketTrades from './components/MarketTrades.svelte';
  import MarketsList from './components/MarketsList.svelte';
  import StatusBar from './components/StatusBar.svelte';
  import DiscrepancyPanel from './components/DiscrepancyPanel.svelte';
  import AlertToast from './components/AlertToast.svelte';
  import ReplayScrubber from './components/ReplayScrubber.svelte';

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
  let pinnedVenues = $state(new Set(initial.pinnedVenues || []));
  let statsMode = $state('window');
  let highlightSec = $state(null);
  let selectedTradeId = $state(null);
  let alertBpsThreshold = $state(initial.alertBpsThreshold ?? 15);
  let density = $state(initial.density || 'comfortable');
  let sessionPreset = $state(initial.sessionPreset || '5m');
  let grafanaUrl = $state(initial.grafanaUrl || '');
  let webhookUrl = $state(initial.webhookUrl || '');
  let watchlists = $state(initial.watchlists || []);
  let activeWatchlist = $state(initial.activeWatchlist || '');
  let tapeMinUsd = $state(initial.tapeMinUsd || 0);
  let tapeSideFilter = $state(initial.tapeSideFilter || 'all');
  let tapeAggregatePrints = $state(initial.tapeAggregatePrints || false);

  let lineSeries = $state([]);
  let discrepancy = $state(null);
  let multiAggregate = $state({ volume: 0, notional: 0, trades: 0 });
  let bpsHistory = $state([]);
  let highlightVenues = $state([]);
  let bpsAlertActive = $state(false);
  let alerts = $state([]);
  let streamMode = $state('poll');
  let replayMode = $state(false);
  let venueTapeFreshness = $state(new Map());
  let venueBooks = $state(new Map());

  let candles = $state([]);
  let volumeBars = $state([]);
  let eventsPerSec = $state(null);
  let priceDir = $state(0);
  let lastTradePrice = $state(null);
  let sessionHigh = $state(null);
  let sessionLow = $state(null);
  let sessionVolume = $state(null);
  let sessionNotional = $state(null);
  let sessionTrades = $state(null);
  let windowVolume = $state(null);
  let windowNotional = $state(null);
  let windowTrades = $state(null);

  let marketSearchRef = $state(null);

  const tracker = new MultiVenueTracker(1);
  const candleBuilder = new CandleBuilder(1);
  const discTracker = new DiscrepancyTracker();
  const stream = new StreamClient({
    onTape: (venue, symbol, entries) => {
      if (replayMode) return;
      if (venue === selectedVenue && symbol === selectedSymbol) {
        tape = entries;
        candleBuilder.ingest(entries);
        syncCandleView();
      }
      tracker.ingest(venue, entries);
      updateFreshness(venue, symbol, entries);
      syncLineView();
    },
    onBook: (venue, symbol, data) => {
      if (replayMode) return;
      venueBooks.set(`${venue}|${symbol}`, true);
      venueBooks = venueBooks;
      if (venue === selectedVenue && symbol === selectedSymbol) book = data;
    },
    onStatus: (s) => {
      status = s;
      if (s?.grafana_url && !grafanaUrl) grafanaUrl = s.grafana_url;
    },
    onConnect: () => { streamMode = 'sse'; },
    onDisconnect: () => { streamMode = 'poll'; },
  });

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
  let sessionSec = $derived(sessionWindowSec(sessionPreset));
  let marketQuotes = $derived(
    (lineSeries || []).map((s) => ({
      venue: s.venue,
      last: s.last ?? null,
      pct: s.pct ?? null,
      tradesPerMin: s.tradesPerMin ?? null,
      notional: s.tradeNotional ?? null,
    })),
  );
  let tapeVol = $derived(
    (tape || [])
      .filter((e) => e.kind === 'trade')
      .reduce((s, e) => s + (Number(e.quantity) || 0), 0),
  );
  let tapeNotional = $derived(
    (tape || [])
      .filter((e) => e.kind === 'trade')
      .reduce((s, e) => {
        const px = Number(e.price);
        const qty = Number(e.quantity);
        return s + (Number.isFinite(px) && Number.isFinite(qty) ? px * qty : 0);
      }, 0),
  );
  let tapeTradeCount = $derived((tape || []).filter((e) => e.kind === 'trade').length);

  let qualityMap = $derived.by(() => {
    const m = new Map();
    for (const row of mapped) {
      const sv = (status?.venues || []).find((v) => v.id === row.venue);
      const fresh = venueTapeFreshness.get(`${row.venue}|${row.symbol}`);
      const hasBook = venueBooks.has(`${row.venue}|${row.symbol}`) || (row.venue === selectedVenue && !!book);
      const qo = isQuotesOnly(sv);
      m.set(`${row.venue}|${row.symbol}`, marketQuality(row, sv, fresh, hasBook, qo));
    }
    return m;
  });

  let venueHealth = $derived(
    (status?.venues || []).map((v) => ({
      venue: v.id,
      reconnects: v.reconnects ?? v.reconnect_count ?? 0,
      gaps: v.gaps ?? v.sequence_gaps ?? 0,
      invalidations: v.book_invalidations ?? v.invalidations ?? 0,
      lagMs: v.feed_lag_ms ?? v.lag_ms ?? null,
      bad:
        (v.reconnects ?? v.reconnect_count ?? 0) > 0 ||
        (v.gaps ?? v.sequence_gaps ?? 0) > 0 ||
        (v.feed_lag_ms ?? v.lag_ms ?? 0) > 2000,
    })),
  );

  let multiTradesPerMin = $derived(
    multiAggregate.trades > 0 ? (multiAggregate.trades / sessionSec) * 60 : null,
  );

  function persist(patch) {
    const next = saveSettings(patch);
    syncUrl(next);
  }

  function syncLineView() {
    const snap = tracker.snapshot(priceMode, { hidden: hiddenVenues, windowSec: sessionSec });
    lineSeries = snap.series;
    discrepancy = snap.discrepancy;
    multiAggregate = snap.aggregate || { volume: 0, notional: 0, trades: 0 };
    discTracker.push(snap.discrepancy, snap.series);
    bpsHistory = discTracker.points();
    checkBpsAlert();
  }

  function syncCandleView() {
    candles = candleBuilder.candles();
    volumeBars = candleBuilder.volumeBars();
    lastTradePrice = candleBuilder.lastPrice;
    sessionHigh = candleBuilder.sessionHigh;
    sessionLow = candleBuilder.sessionLow;
    sessionVolume = candleBuilder.sessionVolume;
    sessionNotional = candleBuilder.sessionNotional;
    sessionTrades = candleBuilder.sessionTrades;
    const w = candleBuilder.windowStats(sessionSec);
    windowVolume = w.volume;
    windowNotional = w.notional;
    windowTrades = w.trades;
  }

  function updateFreshness(venue, symbol, entries) {
    const sec = lastTapeSec(entries);
    if (sec != null) {
      venueTapeFreshness.set(`${venue}|${symbol}`, sec);
      venueTapeFreshness = venueTapeFreshness;
    }
  }

  function checkBpsAlert() {
    const hit = discTracker.shouldAlert(alertBpsThreshold);
    if (!hit) {
      bpsAlertActive = discrepancy?.bps != null && discrepancy.bps > alertBpsThreshold;
      return;
    }
    bpsAlertActive = true;
    const a = createAlert(
      'bps',
      `Cross-venue Δ ${hit.bps.toFixed(2)} bps`,
      `High: ${hit.highVenue || '?'} · Low: ${hit.lowVenue || '?'}`,
    );
    alerts = [...alerts, a];
    fireAlert(a, { type: 'bps', bps: hit.bps, threshold: alertBpsThreshold });
  }

  async function fireAlert(alert, payload) {
    if (webhookUrl) await sendWebhook(webhookUrl, { ...payload, alert });
    await testDaemonAlert(payload);
  }

  function dismissAlert(id) {
    alerts = alerts.map((a) => (a.id === id ? { ...a, dismissed: true } : a));
  }

  function ensureFocusVenue() {
    if (!mapped.length) return;
    const pinned = mapped.find((m) => pinnedVenues.has(m.venue));
    const still = mapped.find((m) => m.venue === selectedVenue);
    if (still) {
      selectedSymbol = still.symbol;
      return;
    }
    const prefer =
      pinned ||
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
    if (!replayMode) {
      tickFocus();
      tickMulti();
    }
    reconnectStream();
  }

  function resetAssetSeries(force = false) {
    const key = selectedAsset;
    if (!force && key === assetKey) return;
    assetKey = key;
    tracker.clear();
    discTracker.clear();
    tracker.syncTargets(mapAssetToVenues(instruments, selectedAsset, status));
    tracker.setInterval(TIMEFRAMES.find((t) => t.id === timeframe)?.sec || 1);
    lineSeries = [];
    discrepancy = null;
    bpsHistory = [];
    multiAggregate = { volume: 0, notional: 0, trades: 0 };
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
    sessionNotional = null;
    sessionTrades = null;
    windowVolume = null;
    windowNotional = null;
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
    if (!replayMode) tickFocus();
  }

  function toggleVenue(venue) {
    const next = new Set(hiddenVenues);
    if (next.has(venue)) next.delete(venue);
    else next.add(venue);
    hiddenVenues = next;
    persist({ hiddenVenues: [...next] });
    syncLineView();
  }

  function onSpikeClick(pt) {
    highlightVenues = [pt.highVenue, pt.lowVenue].filter(Boolean);
    if (pt.highVenue) selectMarket(pt.highVenue, mapped.find((m) => m.venue === pt.highVenue)?.symbol || selectedSymbol);
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
    if (replayMode) return;
    const v = Math.min(2000, Math.max(80, Math.round(Number(ms) || 120)));
    pollFocusMs = v;
    persist({ pollFocusMs: v });
    if (focusTimer) clearInterval(focusTimer);
    focusTimer = setInterval(tickFocus, v);
  }

  function rescheduleMulti(ms) {
    if (replayMode) return;
    const v = Math.min(5000, Math.max(100, Math.round(Number(ms) || 220)));
    pollMultiMs = v;
    persist({ pollMultiMs: v });
    if (multiTimer) clearInterval(multiTimer);
    multiTimer = setInterval(tickMulti, v);
  }

  function reconnectStream() {
    stream.disconnect();
    if (replayMode) return;
    stream.connect({ asset: selectedAsset, venues: mapped.map((m) => m.venue) });
  }

  async function refreshStatus() {
    status = await fetchJson('/v1/status');
    connected = true;
    if (status?.grafana_url && !grafanaUrl) {
      grafanaUrl = status.grafana_url;
      persist({ grafanaUrl: grafanaUrl });
    }
    const v = (status.venues || []).find((x) => x.id === selectedVenue);
    if (v) {
      const now = performance.now();
      if (lastEvents != null && lastEventsAt > 0) {
        const dt = (now - lastEventsAt) / 1000;
        if (dt > 0.15) eventsPerSec = Math.max(0, (v.events_dispatched - lastEvents) / dt);
      }
      lastEvents = v.events_dispatched;
      lastEventsAt = now;
      const lag = v.feed_lag_ms ?? v.lag_ms;
      if (lag != null && lag > 2000) {
        const existing = alerts.find((a) => a.kind === 'lag' && !a.dismissed);
        if (!existing) {
          const a = createAlert('lag', `Feed lag ${lag}ms`, selectedVenue);
          alerts = [...alerts, a];
          fireAlert(a, { type: 'lag', venue: selectedVenue, lagMs: lag });
        }
      }
    }
  }

  async function refreshInstruments() {
    instruments = await fetchJson('/v1/instruments');
    if (!assets.includes(selectedAsset) && assets.length) {
      selectedAsset = assets[0];
      assetKey = '';
    }
    ensureFocusVenue();
    if (selectedAsset !== assetKey) resetAssetSeries(true);
    else tracker.syncTargets(mapAssetToVenues(instruments, selectedAsset, status));
  }

  async function refreshBook() {
    if (!selectedVenue || !selectedSymbol || replayMode) return;
    try {
      book = await fetchJson(bookQuery(selectedVenue, selectedSymbol, bookDepth));
      venueBooks.set(`${selectedVenue}|${selectedSymbol}`, true);
      venueBooks = venueBooks;
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
    if (!selectedVenue || !selectedSymbol || replayMode) return;
    try {
      const data = await fetchJson(tapeQuery(selectedVenue, selectedSymbol, tapeLimit, 'trade'));
      tape = data.entries || [];
      updateFreshness(selectedVenue, selectedSymbol, tape);
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
    if (replayMode || multiBusy || streamMode === 'sse') return;
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
            const data = await fetchJson(tapeQuery(t.venue, t.symbol, Math.min(80, tapeLimit)));
            tracker.ingest(t.venue, data.entries || []);
            updateFreshness(t.venue, t.symbol, data.entries || []);
          } catch {
            /* empty venue */
          }
        }),
      );
      syncLineView();
    } finally {
      multiBusy = false;
    }
  }

  async function tickSlow() {
    if (replayMode) return;
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
    if (replayMode) return;
    try {
      await Promise.all([refreshBook(), refreshFocusTape()]);
    } catch (e) {
      error = String(e.message || e);
    }
  }

  function handleReplayMode(on) {
    replayMode = on;
    if (on) {
      stream.disconnect();
      if (focusTimer) clearInterval(focusTimer);
      if (multiTimer) clearInterval(multiTimer);
    } else {
      focusTimer = setInterval(tickFocus, pollFocusMs);
      multiTimer = setInterval(tickMulti, pollMultiMs);
      reconnectStream();
      tickFocus();
      tickMulti();
    }
  }

  function handleReplayEntries(entries) {
    for (const e of entries) {
      if (e.kind === 'trade' || e.kind === 'quote') {
        const venue = e.venue || selectedVenue;
        tracker.ingest(venue, [e]);
        if (venue === selectedVenue) candleBuilder.ingest([e]);
      }
    }
    syncLineView();
    syncCandleView();
  }

  function onKeydown(ev) {
    if (ev.target?.matches('input, textarea, select')) return;
    if (ev.key === '/') {
      ev.preventDefault();
      marketSearchRef?.focus();
    }
    const idx = Number(ev.key);
    if (idx >= 1 && idx <= TIMEFRAMES.length) {
      applyTimeframe(TIMEFRAMES[idx - 1].id);
    }
  }

  function toggleDensity() {
    density = density === 'compact' ? 'comfortable' : 'compact';
    persist({ density });
  }

  function openGrafana() {
    if (grafanaUrl) window.open(grafanaUrl, '_blank', 'noopener');
  }

  function handleSaveWatchlist() {
    const name = prompt('Watchlist name', selectedAsset + ' watch');
    if (!name) return;
    const wl = saveWatchlist(name, [selectedAsset]);
    watchlists = wl.watchlists;
    activeWatchlist = wl.activeWatchlist;
  }

  function handleWatchlist(id) {
    activeWatchlist = id;
    persist({ activeWatchlist: id });
    const wl = watchlists.find((w) => w.id === id);
    if (wl?.assets?.[0]) onAssetChange(wl.assets[0]);
  }

  onMount(() => {
    syncUrl(loadSettings());
    const tfSec = TIMEFRAMES.find((t) => t.id === timeframe)?.sec || 1;
    tracker.setInterval(tfSec);
    candleBuilder.setInterval(tfSec);

    window.addEventListener('keydown', onKeydown);

    tickSlow()
      .then(async () => {
        const ok = await stream.probe();
        if (ok) reconnectStream();
        await Promise.all([tickFocus(), tickMulti()]);
      })
      .catch(() => {});

    const slow = setInterval(tickSlow, 2000);
    const mid = setInterval(() => { if (!replayMode) refreshStatus().catch(() => {}); }, 1000);
    focusTimer = setInterval(tickFocus, pollFocusMs);
    multiTimer = setInterval(tickMulti, pollMultiMs);

    return () => {
      window.removeEventListener('keydown', onKeydown);
      clearInterval(slow);
      clearInterval(mid);
      if (focusTimer) clearInterval(focusTimer);
      if (multiTimer) clearInterval(multiTimer);
      stream.disconnect();
    };
  });

  let bid = $derived(book?.bids?.[0] ? Number(book.bids[0].price) : null);
  let ask = $derived(book?.asks?.[0] ? Number(book.asks[0].price) : null);
  let mid = $derived(
    bid != null && ask != null && Number.isFinite(bid) && Number.isFinite(ask) ? (bid + ask) / 2 : null,
  );
  let spread = $derived(
    bid != null && ask != null && Number.isFinite(bid) && Number.isFinite(ask) ? ask - bid : null,
  );
  let spreadBps = $derived(mid != null && spread != null && mid > 0 ? (spread / mid) * 10000 : null);
  let lastPrice = $derived(lastTradePrice ?? mid);
  let venueLive = $derived(!!(status?.venues || []).find((v) => v.id === selectedVenue)?.live);
  let crossBps = $derived(discrepancy?.bps ?? null);
</script>

<div class="terminal" class:density-compact={density === 'compact'}>
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
    {sessionNotional}
    {sessionTrades}
    {windowVolume}
    {windowNotional}
    {windowTrades}
    windowSec={sessionSec}
    {sessionPreset}
    {eventsPerSec}
    {venueLive}
    mappedVenues={mapped.length}
    {liveMapped}
    {crossBps}
    multiNotional={multiAggregate.notional}
    multiTrades={multiAggregate.trades}
    {multiTradesPerMin}
    {statsMode}
    {density}
    {grafanaUrl}
    {streamMode}
    onStatsMode={(m) => (statsMode = m)}
    onSessionPreset={(id) => { sessionPreset = id; persist({ sessionPreset: id }); syncLineView(); syncCandleView(); }}
    onDensity={toggleDensity}
    onGrafana={openGrafana}
  />

  <AlertToast {alerts} onDismiss={dismissAlert} />

  <div class="workspace">
    <aside class="col-book">
      <OrderBook {book} {lastPrice} {priceDir} depth={bookDepth} onDepth={setBookDepth} />
    </aside>

    <section class="col-chart">
      {#if chartMode === 'lines'}
        <DiscrepancyPanel
          history={bpsHistory}
          threshold={alertBpsThreshold}
          alertActive={bpsAlertActive}
          {highlightVenues}
          onThreshold={(n) => { alertBpsThreshold = n; persist({ alertBpsThreshold: n }); }}
          onSpikeClick={onSpikeClick}
        />
      {/if}
      <PriceChart
        series={lineSeries}
        {candles}
        {volumeBars}
        {bpsHistory}
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
        alertBpsThreshold={alertBpsThreshold}
        webhookUrl={webhookUrl}
        focusVenue={selectedVenue}
        {highlightVenues}
        {highlightSec}
        sessionWindowSec={sessionSec}
        onTimeframe={applyTimeframe}
        onChartMode={(m) => { chartMode = m; persist({ chartMode: m }); }}
        onPriceMode={(m) => { priceMode = m; persist({ priceMode: m }); syncLineView(); }}
        onAsset={onAssetChange}
        onToggleVenue={toggleVenue}
        onFocusVenue={(v, s) => selectMarket(v, s)}
        onShowVolume={(v) => { showVolume = v; persist({ showVolume: v }); }}
        onBookDepth={setBookDepth}
        onTapeLimit={setTapeLimit}
        onPollFocus={rescheduleFocus}
        onPollMulti={rescheduleMulti}
        onAlertBps={(n) => { alertBpsThreshold = n; persist({ alertBpsThreshold: n }); }}
        onWebhook={(u) => { webhookUrl = u; persist({ webhookUrl: u }); }}
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
          {qualityMap}
          bind:searchRef={marketSearchRef}
          {watchlists}
          {activeWatchlist}
          onSelect={selectMarket}
          onAsset={onAssetChange}
          onWatchlist={handleWatchlist}
          onSaveWatchlist={handleSaveWatchlist}
        />
      </div>
      <div class="trades-pane">
        <MarketTrades
          {tape}
          tradeCount={tapeTradeCount}
          volume={tapeVol}
          notional={tapeNotional}
          {selectedTradeId}
          minUsd={tapeMinUsd}
          sideFilter={tapeSideFilter}
          aggregatePrints={tapeAggregatePrints}
          onSelectTrade={onSelectTrade}
          onFilters={(f) => {
            if (f.minUsd != null) { tapeMinUsd = f.minUsd; persist({ tapeMinUsd: f.minUsd }); }
            if (f.sideFilter) { tapeSideFilter = f.sideFilter; persist({ tapeSideFilter: f.sideFilter }); }
            if (f.aggregatePrints != null) { tapeAggregatePrints = f.aggregatePrints; persist({ tapeAggregatePrints: f.aggregatePrints }); }
          }}
        />
      </div>
    </aside>
  </div>

  <ReplayScrubber
    {replayMode}
    onReplayMode={handleReplayMode}
    onEntries={handleReplayEntries}
    onPosition={() => {}}
  />

  <StatusBar {status} {error} {connected} {streamMode} {venueHealth} />
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

  .col-book, .col-chart, .col-right { min-height: 0; min-width: 0; }

  .col-chart { display: flex; flex-direction: column; min-height: 0; }

  .col-right {
    display: flex;
    flex-direction: column;
    border-left: 1px solid var(--border);
  }

  .markets-pane { flex: 0 0 48%; min-height: 0; }
  .trades-pane { flex: 1; min-height: 0; }

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
    .markets-pane, .trades-pane { flex: 1; }
  }

  @media (max-width: 720px) {
    .workspace {
      grid-template-columns: 1fr;
      grid-template-rows: 45vh 40vh auto;
    }
    .col-right { flex-direction: column; max-height: 50vh; }
  }
</style>
