<script>
  import { onMount } from 'svelte';
  import { bookQuery, fetchJson, tapeQuery } from './lib/api.js';
  import { assetCoverage, colorForVenue, listAssets, mapAssetToVenues } from './lib/assets.js';
  import { CandleBuilder } from './lib/ohlcv.js';
  import { MultiVenueTracker, TIMEFRAMES } from './lib/series.js';
  import { loadSettings, saveSettings, saveWatchlist } from './lib/settings.js';
  import { syncUrl } from './lib/urlState.js';
  import { sessionWindowSec } from './lib/session.js';
  import { DiscrepancyTracker } from './lib/discrepancy.js';
  import { StreamClient } from './lib/stream.js';
  import { createAlert, sendWebhook, testDaemonAlert } from './lib/alerts.js';
  import {
    marketQuality,
    lastTapeSec,
    isQuotesOnly,
    QualityBadgeGate,
    LiveFlagGate,
  } from './lib/quality.js';
  import { nsToSec } from './lib/format.js';
  import {
    bookPressure,
    clampPriceZoom,
    clampViewSec,
    computeCvd,
    pushDepthHistory,
    pushImbalanceHistory,
    resolveTick,
    sampleBookDepth,
  } from './lib/orderflow.js';
  import {
    setCrosshairOnCharts,
    timeToCoordinateSafe,
    wireCrosshairSync,
  } from './lib/chartSync.js';
  import { buildHoverLegend } from './lib/crosshairLegend.js';
  import {
    ALERTS_MAX,
    clampHistorySecs,
    DEFAULT_HISTORY_SECS,
    SeriesHistoryPolicy,
    TAPE_DOM_MAX,
    TAPE_OF_MAX,
  } from './lib/history.js';
  import {
    bookImbalanceFromSnap,
    computePulse,
    pulseSpike,
    pushPulseHistory,
    spreadBpsFromBook,
  } from './lib/pulse.js';
  import { createPaintGate } from './lib/paint.js';
  import { tapeTipSec } from './lib/indicatorSeries.js';
  import { Book404Gate, isCurrentMarket } from './lib/contracts.js';
  import HeaderBar from './components/HeaderBar.svelte';
  import OrderBook from './components/OrderBook.svelte';
  import PriceChart from './components/PriceChart.svelte';
  import MarketTrades from './components/MarketTrades.svelte';
  import MarketsList from './components/MarketsList.svelte';
  import StatusBar from './components/StatusBar.svelte';
  import DiscrepancyPanel from './components/DiscrepancyPanel.svelte';
  import AlertToast from './components/AlertToast.svelte';
  import ReplayScrubber from './components/ReplayScrubber.svelte';
  import OrderFlowHeatmap from './components/OrderFlowHeatmap.svelte';
  import DomLadder from './components/DomLadder.svelte';
  import FlowPulseDock from './components/FlowPulseDock.svelte';
  import ChartAnalyticsStrip from './components/ChartAnalyticsStrip.svelte';
  import ChartHoverLegend from './components/ChartHoverLegend.svelte';

  const initial = loadSettings();

  let status = $state(null);
  let instruments = $state({ venues: [] });
  let book = $state(null);
  let tape = $state([]);
  /** Order-flow tape projection (hard-capped; not the full retention Map). */
  let ofTape = $state([]);
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
  let pollFocusMs = $state(initial.pollFocusMs || 180);
  let pollMultiMs = $state(initial.pollMultiMs || 280);
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
  // Single fixed Flow & Pulse panel: only open vs hidden (legacy flow|pulse → open).
  let analyticsTab = $state(
    normalizeDockTab(initial.analyticsTab) === 'hidden' ? 'hidden' : 'both',
  );
  let analyticsOpen = $state(
    normalizeDockTab(initial.analyticsTab) === 'hidden'
      ? false
      : initial.analyticsOpen !== false,
  );
  let largeTradeUsd = $state(initial.largeTradeUsd ?? 25000);
  let pulseSpikeThreshold = $state(initial.pulseSpikeThreshold ?? 72);
  let ofTick = $state(initial.ofTick ?? 'auto');
  let ofHeat = $state(initial.ofHeat ?? 1);
  let ofBubbleMinUsd = $state(initial.ofBubbleMinUsd ?? 50);
  let ofLayers = $state(
    initial.ofLayers ?? 'heat,bubbles,mid,vap,cvd,vol,cob,candles,markers',
  );
  let ofPriceZoom = $state(clampPriceZoom(initial.ofPriceZoom, 1));
  let ofViewSec = $state(
    initial.ofViewSec == null ? null : clampViewSec(initial.ofViewSec, sessionWindowSec(initial.sessionPreset || '5m')),
  );
  let ofFollowLive = $state(initial.ofFollowLive !== false);
  const initialHistorySecs = clampHistorySecs(initial.historySecs, DEFAULT_HISTORY_SECS);
  let historySecs = $state(initialHistorySecs);
  const historyPolicy = new SeriesHistoryPolicy(initialHistorySecs);

  let lineSeries = $state([]);
  let discrepancy = $state(null);
  let multiAggregate = $state({ volume: 0, notional: 0, trades: 0 });
  let bpsHistory = $state([]);
  let highlightVenues = $state([]);
  let bpsAlertActive = $state(false);
  let alerts = $state([]);
  let streamMode = $state('poll');
  /** Soft reconnect hint — does not tear down main UI. */
  let streamReconnecting = $state(false);
  let replayMode = $state(false);
  const badgeGate = new QualityBadgeGate();
  const liveGate = new LiveFlagGate();
  let streamDisconnectTimer = 0;
  let venueTapeFreshness = $state(new Map());
  let venueBooks = $state(new Map());
  /** @type {Map<string, object>} */
  let venueBookSnaps = $state(new Map());
  let imbalanceHistory = $state([]);
  /** @type {Array<object>} L2 depth ring for order-flow heatmap */
  let depthHistory = $state([]);
  let pulseHistory = $state([]);
  /** Visible plot time window from Lines/Candles (unix sec); null → session/OF window. */
  let chartVisibleRange = $state(/** @type {{ fromSec: number, toSec: number }|null} */ (null));
  /** Main LWC chart handle for under-chart pane time-scale sync (null in OF). */
  let mainPriceChart = $state(/** @type {any} */ (null));
  /** @type {Array<{ id: string, chart: any, series: any }>} */
  let mainCrosshairHandles = $state([]);
  /** @type {Array<{ id: string, chart: any, series: any }>} */
  let paneCrosshairHandles = $state([]);
  /** @type {ReturnType<typeof buildHoverLegend>|null} */
  let hoverLegend = $state(null);
  let stackXhairX = $state(/** @type {number|null} */ (null));
  let stackXhairTop = $state(0);
  let stackXhairHeight = $state(0);
  let plotStackEl = $state(/** @type {HTMLElement|null} */ (null));
  const crosshairGuard = { active: false };
  /** @type {(() => void)|null} */
  let crosshairDispose = null;
  let pulseAlertActive = $state(false);
  let lastPulseAlertAt = 0;
  let pulseMetricFilter = $state('');
  let lastDepthSampleAt = 0;
  /** Accumulated focus trades for Order Flow vol/CVD/VAP (SSE batches alone are too short). */
  /** @type {Map<string, object>} */
  let focusTapeRing = new Map();
  /** Mutable depth ring; published to $state on a paint gate to cut identity churn. */
  /** @type {Array<object>} */
  let depthRing = [];
  let depthRingDirty = false;
  /** Temporary venue+symbol 404 backoff; successful responses clear it. */
  const book404Gate = new Book404Gate();
  let focusGeneration = 0;
  /** @type {object|null} */
  let pendingFocusBook = null;
  /** @type {{ display: object[], of: object[], delta: object[] }|null} */
  let pendingFocusTape = null;
  let snapsDirty = false;
  let tabHidden = false;

  /** Exchange/receive tip in epoch ms — shared clock with Lines/CVD. */
  function exchangeTipMs() {
    const tip = tapeTipSec(ofTape.length ? ofTape : tape);
    if (tip != null) return tip * 1000;
    if (chartVisibleRange && Number.isFinite(chartVisibleRange.toSec)) {
      return Number(chartVisibleRange.toSec) * 1000;
    }
    return Date.now();
  }

  const bookPaint = createPaintGate(() => {
    if (pendingFocusBook) {
      book = pendingFocusBook;
      const pressure = bookPressure(book, bookDepth);
      const last = imbalanceHistory[imbalanceHistory.length - 1];
      const tipMs = exchangeTipMs();
      // ~1 Hz samples so ring can span the session window without thrashing.
      if (!last || tipMs - last.t >= 1000) {
        const maxPts = Math.min(900, Math.max(180, Math.ceil(sessionSec) + 30));
        imbalanceHistory = pushImbalanceHistory(
          imbalanceHistory,
          pressure.imbalancePct,
          maxPts,
          tipMs,
        );
      }
      pendingFocusBook = null;
    }
  }, { minIntervalMs: 120 });

  const tapePaint = createPaintGate(() => {
    if (pendingFocusTape) {
      // Display tape is DOM-capped; OF gets a separate hard-capped projection.
      tape = pendingFocusTape.display;
      ofTape = pendingFocusTape.of;
      if (pendingFocusTape.delta?.length) {
        candleBuilder.ingest(pendingFocusTape.delta);
        syncCandleView();
      }
      pendingFocusTape = null;
    }
  }, { minIntervalMs: 100 });

  const linePaint = createPaintGate(() => {
    flushLineView();
  }, { minIntervalMs: 100 });

  const snapsPaint = createPaintGate(() => {
    if (!snapsDirty) return;
    snapsDirty = false;
    venueBookSnaps = new Map(venueBookSnaps);
  }, { minIntervalMs: 120 });

  const depthPaint = createPaintGate(() => {
    if (!depthRingDirty) return;
    depthRingDirty = false;
    depthHistory = depthRing;
  }, { minIntervalMs: 120 });

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

  const tracker = new MultiVenueTracker(1, initialHistorySecs);
  const candleBuilder = new CandleBuilder(1, initialHistorySecs);
  const discTracker = new DiscrepancyTracker(initialHistorySecs);
  const stream = new StreamClient({
    onTape: (venue, symbol, entries) => {
      if (replayMode) return;
      applyFocusTape(venue, symbol, entries);
      tracker.ingest(venue, entries);
      updateFreshness(venue, symbol, entries);
      syncLineView();
    },
    onBook: (venue, symbol, data) => {
      if (replayMode) return;
      applyFocusBook(venue, symbol, data);
    },
    onFocus: (f) => {
      if (replayMode || !f) return;
      const expected = mapped.some((m) => m.venue === f.venue && m.symbol === f.symbol);
      if (!expected) return;
      if (f.book) applyFocusBook(f.venue, f.symbol, f.book);
      if (f.tape?.length) {
        applyFocusTape(f.venue, f.symbol, f.tape);
        tracker.ingest(f.venue, f.tape);
        updateFreshness(f.venue, f.symbol, f.tape);
        syncLineView();
      }
    },
    onStatus: (s) => {
      applyStatus(s);
    },
    onConnect: () => {
      if (streamDisconnectTimer) {
        clearTimeout(streamDisconnectTimer);
        streamDisconnectTimer = 0;
      }
      streamReconnecting = false;
      streamMode = 'sse';
    },
    onDisconnect: () => {
      // Debounce poll fallback so brief EventSource blips never flip the chip / remount UX.
      streamReconnecting = true;
      if (streamDisconnectTimer) clearTimeout(streamDisconnectTimer);
      streamDisconnectTimer = setTimeout(() => {
        streamDisconnectTimer = 0;
        if (!stream.connected) {
          streamMode = 'poll';
          streamReconnecting = false;
        }
      }, 2800);
    },
    onReconnecting: () => {
      streamReconnecting = true;
    },
  });

  function applyFocusBook(venue, symbol, data) {
    if (!data) return;
    const key = `${venue}|${symbol}`;
    if (!venueBooks.has(key)) {
      venueBooks.set(key, true);
      venueBooks = new Map(venueBooks);
    }
    venueBookSnaps.set(key, data);
    snapsDirty = true;
    snapsPaint.schedule();
    if (venue === selectedVenue && symbol === selectedSymbol) {
      pendingFocusBook = data;
      bookPaint.schedule();
      sampleDepth(data);
    }
  }

  function applyFocusTape(venue, symbol, entries) {
    if (venue === selectedVenue && symbol === selectedSymbol) {
      pendingFocusTape = mergeFocusTape(entries || []);
      tapePaint.schedule();
    }
  }

  /**
   * Merge SSE/poll tape batches into a capped ring so OF vol/CVD/VAP
   * (and candles) survive mode switches — without publishing 10k+ rows to DOM.
   * @param {object[]} entries
   * @returns {{ display: object[], of: object[], delta: object[] }}
   */
  function mergeFocusTape(entries) {
    /** @type {object[]} */
    const delta = [];
    for (const e of entries || []) {
      if (!e || e.kind !== 'trade') continue;
      const id =
        e.trade_id != null
          ? `t:${e.trade_id}`
          : `f:${e.exchange_ts_ns}|${e.price}|${e.quantity}|${e.aggressor || ''}`;
      if (!focusTapeRing.has(id)) delta.push(e);
      focusTapeRing.set(id, e);
    }
    const keepSec = Math.min(historyPolicy.tapeKeepSec(), 1800);
    const cutoff = Math.floor(Date.now() / 1000) - keepSec;
    for (const [k, e] of focusTapeRing) {
      const sec = nsToSec(e.exchange_ts_ns ?? e.receive_ts_ns);
      if (sec != null && sec < cutoff) focusTapeRing.delete(k);
    }
    const maxEntries = Math.min(historyPolicy.tapeMaxEntries(), TAPE_OF_MAX);
    if (focusTapeRing.size > maxEntries) {
      const sorted = [...focusTapeRing.entries()].sort(
        (a, b) =>
          (Number(a[1].exchange_ts_ns ?? a[1].receive_ts_ns) || 0) -
          (Number(b[1].exchange_ts_ns ?? b[1].receive_ts_ns) || 0),
      );
      for (const [k] of sorted.slice(0, sorted.length - maxEntries)) focusTapeRing.delete(k);
    }
    const newestFirst = [...focusTapeRing.values()].sort(
      (a, b) =>
        (Number(b.exchange_ts_ns ?? b.receive_ts_ns) || 0) -
        (Number(a.exchange_ts_ns ?? a.receive_ts_ns) || 0),
    );
    const domCap = Math.min(TAPE_DOM_MAX, Math.max(40, tapeLimit || 120));
    return {
      display: newestFirst.slice(0, domCap),
      of: newestFirst.slice(0, TAPE_OF_MAX),
      delta,
    };
  }

  function pushAlert(alert) {
    const next = [...alerts, alert];
    alerts = next.length > ALERTS_MAX ? next.slice(next.length - ALERTS_MAX) : next;
  }

  function sampleDepth(data) {
    if (tabHidden) return;
    const now = Date.now();
    // Cap sample rate so the heatmap ring stays smooth without thrashing.
    // Slower when OF chart isn't visible.
    const minGap = chartMode === 'orderflow' ? 200 : 400;
    if (now - lastDepthSampleAt < minGap) return;
    lastDepthSampleAt = now;
    const tick = resolveTick(ofTick, data);
    const sample = sampleBookDepth(data, {
      t: now,
      tick,
      // Prefer deep L2 walls (SSE now sends ~48; poll path uses heatBookDepth).
      maxLevels: Math.min(48, Math.max(32, bookDepth * 2)),
    });
    if (!sample) return;
    const budget = historyPolicy.depthBudget();
    // Mutable ring + throttled publish — avoids remount/jitter from identity churn.
    depthRing = pushDepthHistory(depthRing, sample, budget.maxCols, {
      gapMs: 450,
      historySecs,
    });
    depthRingDirty = true;
    depthPaint.schedule();
  }

  /** Deep L2 poll for Order Flow even when SSE focus is fresh (walls need depth). */
  const HEAT_BOOK_DEPTH = 50;

  /**
   * Dock visibility only: legacy `flow`/`pulse`/`orderflow`/`both` → open panel;
   * `hidden` stays hidden. No mutually exclusive layouts.
   */
  function normalizeDockTab(tab) {
    if (tab === 'hidden') return 'hidden';
    if (
      tab === 'orderflow' ||
      tab === 'flow' ||
      tab === 'pulse' ||
      tab === 'both'
    ) {
      return 'both';
    }
    return 'both';
  }
  let lastHeatBookAt = 0;
  async function refreshHeatBook() {
    if (chartMode !== 'orderflow' || !selectedVenue || !selectedSymbol || replayMode) return;
    const requestVenue = selectedVenue;
    const requestSymbol = selectedSymbol;
    const requestGeneration = focusGeneration;
    const now = Date.now();
    if (now - lastHeatBookAt < 350) return;
    lastHeatBookAt = now;
    try {
      const data = await fetchJson(
        bookQuery(requestVenue, requestSymbol, HEAT_BOOK_DEPTH),
      );
      if (
        data &&
        requestGeneration === focusGeneration &&
        isCurrentMarket(requestVenue, requestSymbol, selectedVenue, selectedSymbol)
      ) {
        book404Gate.clear(requestVenue, requestSymbol);
        applyFocusBook(requestVenue, requestSymbol, data);
      }
    } catch {
      /* keep last good depth ring */
    }
  }

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
  /** Shared time window for under-chart Pulse/Imb/CVD/hist strip. */
  let stripVisibleRange = $derived(
    chartMode === 'orderflow' ? null : chartVisibleRange,
  );
  let stripWindowSec = $derived(ofViewSec ?? sessionSec);

  /**
   * @param {Array<{ id?: string, chart?: any, series?: any }>|null|undefined} a
   * @param {Array<{ id?: string, chart?: any, series?: any }>|null|undefined} b
   */
  function handlesEqual(a, b) {
    if (a === b) return true;
    if (!a || !b || a.length !== b.length) return false;
    for (let i = 0; i < a.length; i += 1) {
      if (a[i]?.id !== b[i]?.id || a[i]?.chart !== b[i]?.chart || a[i]?.series !== b[i]?.series) {
        return false;
      }
    }
    return true;
  }

  function allCrosshairHandles() {
    return [...(mainCrosshairHandles || []), ...(paneCrosshairHandles || [])].filter(
      (h) => h?.chart && h?.series,
    );
  }

  function clearHoverUi() {
    hoverLegend = null;
    stackXhairX = null;
  }

  /** @param {number|null|undefined} clientX */
  function updateStackXhair(clientX) {
    if (!plotStackEl || clientX == null || !Number.isFinite(clientX)) {
      stackXhairX = null;
      return;
    }
    const rect = plotStackEl.getBoundingClientRect();
    const x = clientX - rect.left;
    if (x < 0 || x > rect.width) {
      stackXhairX = null;
      return;
    }
    stackXhairX = x;
    const hosts = plotStackEl.querySelectorAll('.chart-host, .of-heat-main, .cas');
    let top = rect.height;
    let bottom = 0;
    for (const node of hosts) {
      const r = node.getBoundingClientRect();
      top = Math.min(top, r.top - rect.top);
      bottom = Math.max(bottom, r.bottom - rect.top);
    }
    if (!(bottom > top)) {
      top = 0;
      bottom = rect.height;
    }
    stackXhairTop = Math.max(0, top);
    stackXhairHeight = Math.max(0, bottom - top);
  }

  /** @param {number|string|null|undefined} timeSec */
  function buildLegendAt(timeSec) {
    const t = Number(timeSec);
    if (!Number.isFinite(t)) {
      hoverLegend = null;
      return;
    }
    const tapeSrc = ofTape.length ? ofTape : tape;
    const cvd = computeCvd(tapeSrc, {
      windowSec: Math.max(1, Number(stripWindowSec) || sessionSec || 300),
      nowSec: t,
    });
    hoverLegend = buildHoverLegend({
      timeSec: t,
      priceMode,
      venues: (lineSeries || []).map((s) => ({
        venue: s.venue,
        color: s.color,
        hidden: s.hidden,
        data: s.data,
        last: s.last,
        pct: s.pct,
      })),
      pulseHistory,
      imbalanceHistory,
      cvdPoints: (cvd.points || []).map((p) => ({ time: p.sec, value: p.cvd })),
      histogram: cvd.histogram || [],
    });
  }

  /** @param {{ time: number|string|null, point: { x: number, y: number }|null, source: any, param: any }} payload */
  function onSharedCrosshairMove(payload) {
    if (payload?.time == null) {
      clearHoverUi();
      return;
    }
    buildLegendAt(payload.time);
    let clientX = null;
    try {
      const el = payload.source?.chartElement?.();
      if (el && payload.point && Number.isFinite(payload.point.x)) {
        clientX = el.getBoundingClientRect().left + payload.point.x;
      }
    } catch {
      /* ignore */
    }
    if (clientX == null) {
      const main = mainCrosshairHandles?.[0]?.chart || payload.source;
      const x = timeToCoordinateSafe(main, payload.time);
      try {
        const el = main?.chartElement?.();
        if (el && x != null) clientX = el.getBoundingClientRect().left + x;
      } catch {
        /* ignore */
      }
    }
    updateStackXhair(clientX);
  }

  function rewireCrosshairSync() {
    crosshairDispose?.();
    crosshairDispose = null;
    const panes = (paneCrosshairHandles || []).filter((h) => h?.chart && h?.series);
    if (chartMode === 'orderflow') {
      if (panes.length) {
        crosshairDispose = wireCrosshairSync(panes, crosshairGuard, {
          onMove: onSharedCrosshairMove,
        });
      }
      return;
    }
    const handles = allCrosshairHandles();
    if (!handles.length) {
      clearHoverUi();
      return;
    }
    crosshairDispose = wireCrosshairSync(handles, crosshairGuard, {
      onMove: onSharedCrosshairMove,
    });
  }

  /** @param {{ timeSec: number, clientX: number }|null} payload */
  function onOfCrosshair(payload) {
    if (!payload || payload.timeSec == null) {
      clearHoverUi();
      setCrosshairOnCharts(paneCrosshairHandles, null, crosshairGuard);
      return;
    }
    buildLegendAt(payload.timeSec);
    updateStackXhair(payload.clientX);
    setCrosshairOnCharts(paneCrosshairHandles, payload.timeSec, crosshairGuard);
  }

  $effect(() => {
    mainCrosshairHandles;
    paneCrosshairHandles;
    chartMode;
    rewireCrosshairSync();
    try {
      globalThis.__mfCrosshairDebug = {
        chartMode,
        mainHandles: (mainCrosshairHandles || []).map((h) => h.id),
        paneHandles: (paneCrosshairHandles || []).map((h) => h.id),
        hasLegend: !!hoverLegend,
        xhairX: stackXhairX,
        /** Soak/browser proof helper — drives legend + overlay + LWC sync. */
        force(timeSec, clientX = null) {
          const t = Math.floor(Number(timeSec));
          if (!Number.isFinite(t)) {
            clearHoverUi();
            setCrosshairOnCharts(allCrosshairHandles(), null, crosshairGuard);
            return false;
          }
          buildLegendAt(t);
          if (clientX != null) updateStackXhair(clientX);
          else if (plotStackEl) {
            const rect = plotStackEl.getBoundingClientRect();
            updateStackXhair(rect.left + rect.width * 0.58);
          }
          setCrosshairOnCharts(allCrosshairHandles(), t, crosshairGuard);
          return true;
        },
      };
    } catch {
      /* ignore */
    }
    return () => {
      crosshairDispose?.();
      crosshairDispose = null;
    };
  });

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
      const key = `${row.venue}|${row.symbol}`;
      const raw = marketQuality(row, sv, fresh, hasBook, qo);
      const badges = badgeGate.stabilize(key, raw.badges);
      m.set(key, { ...raw, badges });
    }
    return m;
  });

  let venueHealth = $derived(
    (status?.venues || []).map((v) => {
      const lagMs = v.feed_lag_ms ?? v.lag_ms ?? null;
      const reconnects = v.reconnects ?? v.reconnect_count ?? 0;
      const gaps = v.gaps ?? v.sequence_gaps ?? 0;
      const invalidations = v.book_invalidations ?? v.invalidations ?? 0;
      // Avoid permanent "bad" from cumulative reconnect counter — only flag
      // sustained lag / active gap pressure so the health strip doesn't thrash.
      const bad = gaps > 2 || invalidations > 2 || (lagMs != null && lagMs > 4000);
      return {
        venue: v.id,
        reconnects,
        gaps,
        invalidations,
        lagMs: lagMs != null ? Math.round(lagMs / 50) * 50 : null,
        bad,
      };
    }),
  );

  let multiTradesPerMin = $derived(
    multiAggregate.trades > 0 ? (multiAggregate.trades / sessionSec) * 60 : null,
  );

  let pulse = $derived.by(() => {
    const venues = (lineSeries || []).map((s, i) => {
      const snap = venueBookSnaps.get(`${s.venue}|${s.symbol}`);
      const focusSnap =
        s.venue === selectedVenue && s.symbol === selectedSymbol ? book : null;
      const b = snap || focusSnap;
      const winNotional = Number(s.windowNotional) || 0;
      const winTrades = Number(s.windowTrades) || 0;
      const usdPerMin = sessionSec > 0 ? (winNotional / sessionSec) * 60 : 0;
      return {
        venue: s.venue,
        symbol: s.symbol,
        live: s.live,
        color: s.color || colorForVenue(s.venue, i),
        tradesPerMin: s.tradesPerMin ?? (sessionSec > 0 ? (winTrades / sessionSec) * 60 : 0),
        usdPerMin,
        spreadBps: spreadBpsFromBook(b),
        imbalancePct: bookImbalanceFromSnap(b, Math.min(10, bookDepth)),
        last: s.last ?? null,
      };
    });
    return computePulse(venues, {
      crossBps: discrepancy?.bps ?? null,
      windowSec: sessionSec,
    });
  });

  function applyStatus(s) {
    if (!s) return;
    // Stabilize per-venue live flags before publishing so Markets/legend don't blink.
    if (Array.isArray(s.venues)) {
      s = {
        ...s,
        venues: s.venues.map((v) => ({
          ...v,
          live: liveGate.stabilize(v.id, !!v.live),
        })),
      };
    }
    status = s;
    if (s?.grafana_base_url && !grafanaUrl) grafanaUrl = s.grafana_base_url;
  }

  function persist(patch) {
    const next = saveSettings(patch);
    syncUrl(next);
  }

  function syncLineView(immediate = false) {
    if (immediate) {
      flushLineView();
      return;
    }
    linePaint.schedule();
  }

  function flushLineView() {
    const snap = tracker.snapshot(priceMode, { hidden: hiddenVenues, windowSec: sessionSec });
    lineSeries = snap.series;
    discrepancy = snap.discrepancy;
    multiAggregate = snap.aggregate || { volume: 0, notional: 0, trades: 0 };
    discTracker.push(snap.discrepancy, snap.series);
    bpsHistory = discTracker.points();
    checkBpsAlert();
    syncPulseHistory();
    publishHistoryDebug();
  }

  function syncPulseHistory() {
    // pulse is derived; sample current score into history (throttled by callers).
    const p = pulse;
    if (!p || p.score == null) return;
    const tipMs = exchangeTipMs();
    const last = pulseHistory[pulseHistory.length - 1];
    if (last && tipMs - last.t < 1500) return;
    pulseHistory = pushPulseHistory(
      pulseHistory,
      {
        score: p.score,
        tradesPerMin: p.tradesPerMin,
        usdPerMin: p.usdPerMin,
      },
      Math.min(800, Math.max(120, Math.ceil(sessionSec / 1.5) + 40)),
      tipMs,
    );
    checkPulseAlert();
  }

  function checkPulseAlert() {
    const hit = pulseSpike(pulseHistory, pulseSpikeThreshold);
    pulseAlertActive = hit;
    if (!hit) return;
    if (Date.now() - lastPulseAlertAt < 45000) return;
    lastPulseAlertAt = Date.now();
    const score = pulseHistory[pulseHistory.length - 1]?.score;
    const a = createAlert(
      'info',
      `Pulse spike ${score != null ? score.toFixed(0) : '?'}`,
      `${selectedAsset} multi-venue activity heat · in-app/webhook only`,
    );
    pushAlert(a);
    fireAlert(a, {
      type: 'pulse',
      kind: 'pulse',
      asset: selectedAsset,
      score,
      threshold: pulseSpikeThreshold,
    });
  }

  function syncCandleView() {
    // Clip display to session window; underlying builder retains ~historySecs.
    candles = candleBuilder.candles(sessionSec);
    volumeBars = candleBuilder.volumeBars(sessionSec);
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
    pushAlert(a);
    fireAlert(a, { type: 'bps', bps: hit.bps, threshold: alertBpsThreshold });
  }

  async function fireAlert(alert, payload) {
    if (webhookUrl) await sendWebhook(webhookUrl, { ...payload, alert });
    const delivery = await testDaemonAlert(payload);
    if (!delivery.ok && !delivery.skipped) {
      error = `Alert delivery failed${delivery.status ? ` (${delivery.status})` : ''}`;
    }
  }

  function dismissAlert(id) {
    // Hard-remove so auto-dismissed toasts do not pile up under ALERTS_MAX.
    alerts = alerts.filter((a) => a.id !== id);
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
    pulseHistory = [];
    pulseAlertActive = false;
    venueBookSnaps = new Map();
    book404Gate.clearAll();
    resetFocusSeries(true);
  }

  function resetFocusSeries(force = false) {
    const key = `${selectedVenue}|${selectedSymbol}`;
    if (!force && key === focusKey) return;
    focusKey = key;
    focusGeneration += 1;
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
    ofTape = [];
    highlightSec = null;
    selectedTradeId = null;
    imbalanceHistory = [];
    depthRing = [];
    depthRingDirty = false;
    depthHistory = [];
    focusTapeRing = new Map();
    lastDepthSampleAt = 0;
    pendingFocusBook = null;
    pendingFocusTape = null;
    bookPaint.flushNow();
    tapePaint.flushNow();
    depthPaint.flushNow();
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
    if (!replayMode) {
      tickFocus();
      reconnectStream();
    }
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
    if (replayMode) {
      stream.disconnect({ silent: true });
      return;
    }
    // Silent close avoids SSE→poll chip flip while swapping focus params.
    stream.connect({
      asset: selectedAsset,
      venue: selectedVenue || undefined,
      symbol: selectedSymbol || undefined,
      venues: mapped.map((m) => m.venue),
    });
  }

  function setChartMode(m) {
    // Mode switches must not wipe series — shared buffers live above PriceChart.
    chartMode = m;
    const patch = { chartMode: m };
    // Order Flow chart keeps the Flow & Pulse dock open (single pane).
    if (m === 'orderflow') {
      analyticsTab = 'both';
      analyticsOpen = true;
      patch.analyticsTab = 'both';
      patch.analyticsOpen = true;
      chartVisibleRange = null;
      mainPriceChart = null;
      mainCrosshairHandles = [];
      clearHoverUi();
    }
    persist(patch);
    // Re-project views from retained buffers (no re-fetch from zero).
    syncLineView(true);
    syncCandleView();
    if (depthRing.length) {
      depthHistory = depthRing;
      depthRingDirty = false;
    }
    publishHistoryDebug();
  }

  function publishHistoryDebug() {
    try {
      let linePts = 0;
      let lineSpan = 0;
      for (const st of tracker.venues.values()) {
        linePts += st.buckets.size;
        let minT = Infinity;
        let maxT = 0;
        for (const t of st.buckets.keys()) {
          if (t < minT) minT = t;
          if (t > maxT) maxT = t;
        }
        if (maxT > minT && Number.isFinite(minT)) {
          lineSpan = Math.max(lineSpan, maxT - minT);
        }
      }
      const candleN = candleBuilder.buckets.size;
      // @ts-ignore
      globalThis.__mfHistoryDebug = {
        historySecs,
        chartMode,
        lineBucketPts: linePts,
        lineSpanSec: lineSpan,
        candleBuckets: candleN,
        focusTape: focusTapeRing.size,
        tapeDom: tape.length,
        ofTape: ofTape.length,
        depthCols: depthRing.length || depthHistory.length,
        bpsPts: discTracker.points().length,
        alerts: alerts.length,
        sessionSec,
        wallSec: Math.floor(Date.now() / 1000),
        tabHidden,
      };
    } catch {
      /* ignore */
    }
  }

  function applyHistorySecs(secs) {
    historySecs = clampHistorySecs(secs, historySecs);
    historyPolicy.setHistorySecs(historySecs);
    tracker.setHistorySecs(historySecs);
    candleBuilder.setHistorySecs(historySecs);
    discTracker.setHistorySecs(historySecs);
    persist({ historySecs });
    syncLineView(true);
    syncCandleView();
    publishHistoryDebug();
  }

  function forceLiveRefresh() {
    if (replayMode) return;
    tickFocus();
    tickMulti();
    refreshStatus().catch(() => {});
  }

  async function refreshStatus() {
    const next = await fetchJson('/v1/status');
    connected = true;
    applyStatus(next);
    const v = (status?.venues || []).find((x) => x.id === selectedVenue);
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
          pushAlert(a);
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
    // SSE focus already delivers books — skip redundant poll to cut double-apply flicker.
    // Order Flow still refreshes deep L2 via refreshHeatBook.
    if (streamMode === 'sse' && stream.focusFresh(1200) && chartMode !== 'orderflow') return;
    const requestVenue = selectedVenue;
    const requestSymbol = selectedSymbol;
    const requestGeneration = focusGeneration;
    if (book404Gate.isSuppressed(requestVenue, requestSymbol)) return;
    try {
      const depth = chartMode === 'orderflow' ? Math.max(bookDepth, HEAT_BOOK_DEPTH) : bookDepth;
      const data = await fetchJson(bookQuery(requestVenue, requestSymbol, depth));
      if (
        requestGeneration !== focusGeneration ||
        !isCurrentMarket(requestVenue, requestSymbol, selectedVenue, selectedSymbol)
      ) return;
      book404Gate.clear(requestVenue, requestSymbol);
      applyFocusBook(requestVenue, requestSymbol, data);
      const b = Number(data?.bids?.[0]?.price);
      const a = Number(data?.asks?.[0]?.price);
      if (Number.isFinite(b) && Number.isFinite(a)) {
        const midPx = (b + a) / 2;
        candleBuilder.touchPrice(midPx);
        tracker.touch(requestVenue, midPx);
        syncCandleView();
        syncLineView();
      }
    } catch (e) {
      if (String(e?.message || e).includes('→ 404')) {
        book404Gate.suppress(requestVenue, requestSymbol);
      }
      // Keep last good book — never blank the panel on transient 404/errors.
    }
  }

  async function refreshFocusTape() {
    if (!selectedVenue || !selectedSymbol || replayMode) return;
    // When SSE is actively delivering focus tape, skip redundant poll to cut load —
    // but always poll if focus is stale (broken SSE used to starve the tape).
    if (streamMode === 'sse' && stream.focusFresh(1200)) return;
    const requestVenue = selectedVenue;
    const requestSymbol = selectedSymbol;
    const requestGeneration = focusGeneration;
    try {
      const lim = chartMode === 'orderflow' ? Math.max(tapeLimit, 400) : tapeLimit;
      const data = await fetchJson(tapeQuery(requestVenue, requestSymbol, lim, 'trade'));
      if (
        requestGeneration !== focusGeneration ||
        !isCurrentMarket(requestVenue, requestSymbol, selectedVenue, selectedSymbol)
      ) return;
      const entries = data.entries || [];
      updateFreshness(requestVenue, requestSymbol, entries);
      const prev = candleBuilder.lastPrice;
      applyFocusTape(requestVenue, requestSymbol, entries);
      tracker.ingest(requestVenue, entries);
      syncLineView();
      if (candleBuilder.lastPrice != null && prev != null) {
        if (candleBuilder.lastPrice > prev) priceDir = 1;
        else if (candleBuilder.lastPrice < prev) priceDir = -1;
      }
    } catch {
      // Keep last good tape.
    }
  }

  async function tickMulti() {
    if (replayMode || multiBusy || tabHidden) return;
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
      // Always poll multi-venue books. Skip focus tape only when SSE focus is fresh;
      // never skip other venues (pulse + markets workspace depend on them).
      const sseFresh = streamMode === 'sse' && stream.focusFresh(1500);
      const statusVenues = status?.venues || [];
      await Promise.all(
        batch.map(async (t) => {
          try {
            const isFocus = t.venue === selectedVenue && t.symbol === selectedSymbol;
            const sv = statusVenues.find((v) => v.id === t.venue);
            const bookOk =
              !book404Gate.isSuppressed(t.venue, t.symbol) &&
              (sv?.book_available !== false || isFocus || venueBooks.has(`${t.venue}|${t.symbol}`));
            const tasks = [];
            if (!(isFocus && sseFresh)) {
              tasks.push(
                fetchJson(tapeQuery(t.venue, t.symbol, Math.min(80, tapeLimit), 'trade')).catch(() => null),
              );
            } else {
              tasks.push(Promise.resolve(null));
            }
            if (bookOk) {
              tasks.push(
                fetch(`${bookQuery(t.venue, t.symbol, Math.min(10, bookDepth))}`)
                  .then(async (res) => {
                    if (res.status === 404) {
                      book404Gate.suppress(t.venue, t.symbol);
                      return null;
                    }
                    if (!res.ok) return null;
                    book404Gate.clear(t.venue, t.symbol);
                    return res.json();
                  })
                  .catch(() => null),
              );
            } else {
              tasks.push(Promise.resolve(null));
            }
            const [tapeData, bookData] = await Promise.all(tasks);
            const stillMapped = mapped.some(
              (m) => m.venue === t.venue && m.symbol === t.symbol,
            );
            if (!stillMapped) return;
            if (tapeData?.entries) {
              tracker.ingest(t.venue, tapeData.entries);
              updateFreshness(t.venue, t.symbol, tapeData.entries);
              if (
                isFocus &&
                isCurrentMarket(t.venue, t.symbol, selectedVenue, selectedSymbol)
              ) {
                applyFocusTape(t.venue, t.symbol, tapeData.entries);
              }
            }
            if (bookData) {
              applyFocusBook(t.venue, t.symbol, bookData);
            }
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
    if (replayMode || tabHidden) return;
    try {
      await Promise.all([refreshBook(), refreshFocusTape(), refreshHeatBook()]);
    } catch (e) {
      error = String(e.message || e);
    }
  }

  function handleReplayMode(on) {
    replayMode = on;
    if (on) {
      stream.disconnect({ silent: true });
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
    // Legacy F/B/P: open the single pane (no layout swap). Esc hides.
    if (ev.key === 'f' || ev.key === 'F' || ev.key === 'b' || ev.key === 'B' || ev.key === 'p' || ev.key === 'P') {
      ev.preventDefault();
      setAnalyticsTab('both');
    }
    if (ev.key === 'Escape' && analyticsOpen) {
      analyticsOpen = false;
      analyticsTab = 'hidden';
      persist({ analyticsOpen: false, analyticsTab: 'hidden' });
    }
    const idx = Number(ev.key);
    if (idx >= 1 && idx <= TIMEFRAMES.length) {
      applyTimeframe(TIMEFRAMES[idx - 1].id);
    }
  }

  function setAnalyticsTab(tab) {
    if (tab === 'hidden') {
      analyticsOpen = false;
      analyticsTab = 'hidden';
      persist({ analyticsOpen: false, analyticsTab: 'hidden' });
      return;
    }
    analyticsOpen = true;
    analyticsTab = 'both';
    persist({ analyticsOpen: true, analyticsTab: 'both' });
  }

  function toggleAnalyticsDock() {
    if (analyticsOpen && analyticsTab !== 'hidden') {
      setAnalyticsTab('hidden');
    } else {
      setAnalyticsTab('both');
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
    // Single-pane dock: coerce legacy flow|pulse prefs to open unified view.
    if (analyticsTab !== 'hidden') {
      analyticsTab = 'both';
      analyticsOpen = true;
      persist({ analyticsTab: 'both', analyticsOpen: true });
    } else {
      syncUrl(loadSettings());
    }
    const tfSec = TIMEFRAMES.find((t) => t.id === timeframe)?.sec || 1;
    tracker.setInterval(tfSec);
    candleBuilder.setInterval(tfSec);

    window.addEventListener('keydown', onKeydown);
    const onVisibility = () => {
      tabHidden = document.visibilityState === 'hidden';
      if (!tabHidden) forceLiveRefresh();
    };
    document.addEventListener('visibilitychange', onVisibility);

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
      document.removeEventListener('visibilitychange', onVisibility);
      clearInterval(slow);
      clearInterval(mid);
      if (focusTimer) clearInterval(focusTimer);
      if (multiTimer) clearInterval(multiTimer);
      if (streamDisconnectTimer) clearTimeout(streamDisconnectTimer);
      bookPaint.dispose();
      tapePaint.dispose();
      linePaint.dispose();
      snapsPaint.dispose();
      depthPaint.dispose();
      stream.disconnect({ silent: true });
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
  let hasFocusL2 = $derived(
    !!(book?.bids?.length || book?.asks?.length) ||
      venueBooks.has(`${selectedVenue}|${selectedSymbol}`),
  );

  function patchOfSettings(patch) {
    const tickChanged = patch.ofTick != null && String(patch.ofTick) !== String(ofTick);
    if (patch.ofTick != null) ofTick = String(patch.ofTick);
    if (patch.ofHeat != null) ofHeat = Number(patch.ofHeat);
    if (patch.ofBubbleMinUsd != null) ofBubbleMinUsd = Number(patch.ofBubbleMinUsd);
    if (patch.ofLayers != null) ofLayers = String(patch.ofLayers);
    if (patch.ofPriceZoom != null) ofPriceZoom = clampPriceZoom(patch.ofPriceZoom, ofPriceZoom);
    if (patch.ofViewSec !== undefined) {
      ofViewSec =
        patch.ofViewSec == null || patch.ofViewSec === ''
          ? null
          : clampViewSec(patch.ofViewSec, sessionSec);
    }
    if (patch.ofFollowLive != null) ofFollowLive = !!patch.ofFollowLive;
    if (tickChanged) {
      depthRing = [];
      depthRingDirty = false;
      depthHistory = [];
    }
    persist({
      ofTick,
      ofHeat,
      ofBubbleMinUsd,
      ofLayers,
      ofPriceZoom,
      ofViewSec,
      ofFollowLive,
    });
  }
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
    streamReconnecting={streamReconnecting}
    onStatsMode={(m) => (statsMode = m)}
    onSessionPreset={(id) => { sessionPreset = id; persist({ sessionPreset: id }); syncLineView(); syncCandleView(); }}
    onDensity={toggleDensity}
    onGrafana={openGrafana}
  />

  <AlertToast {alerts} onDismiss={dismissAlert} />

  <div class="workspace">
    <aside class="col-book">
      <OrderBook
        {book}
        {lastPrice}
        {priceDir}
        depth={bookDepth}
        onDepth={setBookDepth}
        showDepthChart={chartMode !== 'orderflow'}
      />
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
      <div class="plot-stack" bind:this={plotStackEl}>
      <!-- Single PriceChart instance — remounting on mode switch caused chart flicker. -->
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
        toolbarOnly={chartMode === 'orderflow'}
        hoverLegend={chartMode === 'orderflow' ? null : hoverLegend}
        onTimeframe={applyTimeframe}
        onChartMode={setChartMode}
        onPriceMode={(m) => { priceMode = m; persist({ priceMode: m }); syncLineView(true); }}
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
        onVisibleTimeRange={(r) => {
          if (r && Number.isFinite(r.fromSec) && Number.isFinite(r.toSec)) {
            chartVisibleRange = r;
          }
        }}
        onMainChart={(c) => {
          mainPriceChart = c;
        }}
        onCrosshairHandles={(handles) => {
          if (!handlesEqual(mainCrosshairHandles, handles)) {
            mainCrosshairHandles = handles;
          }
        }}
      />
      {#if chartMode === 'orderflow'}
        <div class="of-chart-stack">
          <div class="of-heat-main">
            <ChartHoverLegend legend={hoverLegend} />
            <OrderFlowHeatmap
              {depthHistory}
              tape={ofTape}
              windowSec={ofViewSec ?? sessionSec}
              venue={selectedVenue}
              symbol={selectedSymbol}
              {lastPrice}
              hasL2={hasFocusL2}
              {ofTick}
              {ofHeat}
              {ofBubbleMinUsd}
              {ofLayers}
              {largeTradeUsd}
              priceZoom={ofPriceZoom}
              followLive={ofFollowLive}
              onSettings={patchOfSettings}
              onCrosshair={onOfCrosshair}
            />
          </div>
          <div class="of-dom-side">
            <DomLadder
              {book}
              depth={Math.max(bookDepth, 32)}
              tickOpt={ofTick}
              {lastPrice}
              onDepth={setBookDepth}
            />
          </div>
        </div>
      {/if}
      <ChartAnalyticsStrip
        tape={ofTape.length ? ofTape : tape}
        {imbalanceHistory}
        {pulseHistory}
        windowSec={stripWindowSec}
        visibleRange={stripVisibleRange}
        spikeThreshold={pulseSpikeThreshold}
        showVolumeHist={true}
        mainChart={chartMode === 'orderflow' ? null : mainPriceChart}
        onCrosshairHandles={(handles) => {
          if (!handlesEqual(paneCrosshairHandles, handles)) {
            paneCrosshairHandles = handles;
          }
        }}
      />
      {#if stackXhairX != null}
        <div
          class="stack-xhair"
          style={`left:${stackXhairX}px;top:${stackXhairTop}px;height:${stackXhairHeight}px`}
          aria-hidden="true"
        ></div>
      {/if}
      </div>
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

  <div class="analytics-dock" class:open={analyticsOpen && analyticsTab !== 'hidden'}>
    {#if !(analyticsOpen && analyticsTab !== 'hidden')}
      <div class="dock-tabs collapsed">
        <span class="dock-title">Flow &amp; Pulse</span>
        <span class="dock-hint">F show · Esc hide</span>
        <button type="button" class="dock-toggle" onclick={toggleAnalyticsDock}>▴ Show</button>
      </div>
    {:else}
      <div class="dock-body">
        <FlowPulseDock
          {book}
          tape={ofTape.length ? ofTape : tape}
          depth={Math.max(bookDepth, 32)}
          windowSec={ofViewSec ?? sessionSec}
          largeUsd={largeTradeUsd}
          showTapeProfile={chartMode !== 'orderflow'}
          {pulse}
          alertActive={pulseAlertActive}
          spikeThreshold={pulseSpikeThreshold}
          asset={selectedAsset}
          focusVenue={selectedVenue}
          metricFilter={pulseMetricFilter}
          onLargeUsd={(n) => { largeTradeUsd = n; persist({ largeTradeUsd: n }); }}
          onSpikeThreshold={(n) => { pulseSpikeThreshold = n; persist({ pulseSpikeThreshold: n }); }}
          onChipClick={(v, s) => { if (v && s) selectMarket(v, s); }}
          onMetricClick={(m) => {
            pulseMetricFilter = pulseMetricFilter === m ? '' : m;
          }}
          onToggle={toggleAnalyticsDock}
        />
      </div>
    {/if}
  </div>

  <ReplayScrubber
    {replayMode}
    onReplayMode={handleReplayMode}
    onEntries={handleReplayEntries}
    onPosition={() => {}}
  />

  <StatusBar {status} {error} {connected} {streamMode} streamReconnecting={streamReconnecting} {venueHealth} />
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

  .plot-stack {
    position: relative;
    flex: 1;
    min-height: 0;
    display: flex;
    flex-direction: column;
  }

  .stack-xhair {
    position: absolute;
    top: 0;
    width: 1px;
    background: rgba(234, 236, 239, 0.82);
    box-shadow: 0 0 0 0.5px rgba(15, 19, 24, 0.4);
    pointer-events: none;
    z-index: 6;
  }

  .of-chart-stack {
    flex: 1;
    min-height: 0;
    display: grid;
    grid-template-columns: minmax(0, 1fr) minmax(220px, 300px);
    border-top: 1px solid var(--border);
  }
  .of-heat-main, .of-dom-side {
    position: relative;
    min-width: 0;
    min-height: 0;
    overflow: hidden;
  }
  .of-dom-side {
    border-left: 1px solid var(--border);
  }

  .col-right {
    display: flex;
    flex-direction: column;
    border-left: 1px solid var(--border);
  }

  .markets-pane { flex: 0 0 48%; min-height: 0; }
  .trades-pane { flex: 1; min-height: 0; }

  .analytics-dock {
    flex-shrink: 0;
    border-top: 1px solid var(--border);
    background: var(--panel-2);
    display: flex;
    flex-direction: column;
    max-height: 38vh;
  }
  .analytics-dock.open { min-height: 220px; height: 32vh; }
  .dock-tabs {
    display: flex;
    align-items: center;
    gap: 0.25rem;
    padding: 0.2rem 0.45rem;
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
    background: var(--panel);
  }
  .dock-title {
    font-size: 0.68rem;
    font-weight: 600;
    color: var(--text);
    letter-spacing: 0.02em;
  }
  .dock-hint {
    margin-left: 0.5rem;
    font-size: 0.55rem;
    color: var(--muted);
    font-family: var(--mono);
  }
  .dock-toggle {
    margin-left: auto;
    background: transparent;
    border: 1px solid transparent;
    color: var(--muted);
    font-family: var(--mono);
    font-size: 0.68rem;
    padding: 0.15rem 0.45rem;
    cursor: pointer;
    border-radius: 2px;
  }
  .dock-toggle:hover { color: var(--text); background: var(--panel-2); }
  .dock-body {
    flex: 1;
    min-height: 0;
    overflow: hidden;
  }

  @media (max-width: 1100px) {
    .workspace {
      grid-template-columns: minmax(200px, 240px) minmax(0, 1fr);
      grid-template-rows: minmax(220px, 1fr) minmax(160px, 40%);
      overflow-y: auto;
    }
    .col-right {
      grid-column: 1 / -1;
      flex-direction: row;
      border-left: none;
      border-top: 1px solid var(--border);
    }
    .markets-pane, .trades-pane { flex: 1; }
    .analytics-dock.open { max-height: 34vh; min-height: 200px; height: 30vh; }
    .dock-hint { display: none; }
    .of-chart-stack { grid-template-columns: minmax(0, 1fr) minmax(180px, 240px); }
  }

  @media (max-width: 720px) {
    .workspace {
      grid-template-columns: 1fr;
      grid-template-rows: minmax(300px, 45vh) minmax(300px, 40vh) minmax(320px, 50vh);
    }
    .col-right { flex-direction: column; max-height: 50vh; }
    .of-chart-stack {
      grid-template-columns: 1fr;
      grid-template-rows: minmax(180px, 1fr) minmax(120px, 38%);
    }
    .of-dom-side { border-left: none; border-top: 1px solid var(--border); }
  }
</style>
