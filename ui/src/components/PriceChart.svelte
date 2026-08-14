<script>
  import { onMount } from 'svelte';
  import {
    createChart,
    LineSeries,
    CandlestickSeries,
    HistogramSeries,
    createSeriesMarkers,
    ColorType,
  } from 'lightweight-charts';
  import { TIMEFRAMES } from '../lib/series.js';
  import { fmtPrice, fmtPct, fmtUsd, fmtCount, fmtTradesPerMin } from '../lib/format.js';
  import {
    applyVisibleTimeRange,
    createRangeActivity,
    liveVisibleWindow,
    seriesTimeExtent,
    shouldFitLiveContent,
    visibleTimeRangesNearlyEqual,
    wireChartTimeScales,
  } from '../lib/chartSync.js';
  import { CHART_DISPLAY_MAX_POINTS, downsampleForChart } from '../lib/history.js';
  import { stepHoldSeries } from '../lib/indicatorSeries.js';
  import { numericCommit } from '../lib/numericInput.js';
  import { beginAxisDrag } from '../lib/layout.js';
  import ChartHoverLegend from './ChartHoverLegend.svelte';

  /** Shared right-scale width so main + BPS plot areas stay aligned. */
  const PRICE_SCALE_MIN_WIDTH = 72;

  let {
    series = [],
    candles = [],
    volumeBars = [],
    bpsHistory = [],
    chartMode = 'lines',
    priceMode = 'percent',
    timeframe = '1s',
    asset = 'BTC',
    discrepancy = null,
    assets = [],
    coverage = [],
    showVolume = true,
    showBpsPane = true,
    bookDepth = 16,
    tapeLimit = 120,
    pollFocusMs = 120,
    pollMultiMs = 220,
    alertBpsThreshold = 15,
    webhookUrl = '',
    focusVenue = '',
    highlightVenues = [],
    highlightSec = null,
    sessionWindowSec = 300,
    historySecs = 7200,
    bpsHeight = 64,
    toolbarOnly = false,
    hoverLegend = null,
    /**
     * Parent sets true while the user is inspecting (crosshair hover / pointer
     * down on the plot stack). Freezes live scroll without latched unfollow.
     */
    inspecting = false,
    onTimeframe = () => {},
    onChartMode = () => {},
    onPriceMode = () => {},
    onAsset = () => {},
    onToggleVenue = () => {},
    onFocusVenue = () => {},
    onShowVolume = () => {},
    onBookDepth = () => {},
    onTapeLimit = () => {},
    onPollFocus = () => {},
    onPollMulti = () => {},
    onAlertBps = () => {},
    onWebhook = () => {},
    onVisibleTimeRange = () => {},
    /** Notify parent when the main LWC chart is created/destroyed (for pane sync). */
    onMainChart = () => {},
    /** Emit [{id, chart, series}] for multi-pane crosshair sync. */
    onCrosshairHandles = () => {},
    /** Notify parent when follow-live pin changes (Live button / user pan). */
    onFollowLive = () => {},
    onHistorySecs = () => {},
    onBpsHeight = () => {},
    onTestAlert = () => {},
  } = $props();

  let host = $state(null);
  let bpsHost = $state(null);
  let ready = $state(false);
  let chart = null;
  let bpsChart = null;
  /** @type {Map<string, any>} */
  let lineSeries = new Map();
  let candleSeries = null;
  let volumeSeries = null;
  let bpsLineSeries = null;
  let markersApi = null;
  let markerSeries = null;
  let fitKey = '';
  /** When true, keep time scale pinned to the newest bar (fit / last session). */
  let followLive = $state(true);
  let showSettings = $state(false);
  const rangeActivity = createRangeActivity();
  /** @type {Array<() => void>} */
  let syncDisposers = [];
  let chartInteractionDisposer = null;
  let bpsInteractionDisposer = null;
  let hostPointerDisposer = null;
  let volMarginsOn = null;
  let lastScrollAt = 0;
  /** Ignore range-change unfollow until this time (async LWC callbacks). */
  let programmaticUntil = 0;
  let userPanUntil = 0;
  let chartPaintRaf = 0;
  /** @type {object|null} pending chart payload for coalesced paint */
  let pendingChart = null;
  /** @type {Map<string, { first: number, last: number, fullAt: number }>} painted window tip per venue */
  let lastLineWindow = new Map();
  /** @type {{ first: number, last: number, fullAt: number }|null} */
  let lastCandleWindow = null;
  let lastVolFullAt = 0;
  let lastBpsFullAt = 0;
  let lastBpsTip = null;
  /** @type {{ first: number, last: number, fullAt: number }|null} */
  let lastBpsWindow = null;
  /** @type {{ from: number, to: number }|null} last wall range pushed onto BPS */
  let lastBpsVisibleRange = null;
  /** Wall-clock window captured when inspect freeze starts. */
  /** @type {{ from: number, to: number }|null} */
  let frozenVisibleRange = null;
  let bpsRangeRaf = 0;
  /** Full setData at most this often when only the left edge slides. */
  const FULL_SET_MIN_MS = 12000;
  let tabHidden = false;
  let hiddenPollTimer = 0;
  let hostResizeObserver = null;
  let bpsSplitDragging = $state(false);

  const chartOpts = {
    layout: {
      background: { type: ColorType.Solid, color: '#12161c' },
      textColor: '#848e9c',
      fontSize: 11,
      fontFamily: 'IBM Plex Mono, SF Mono, Menlo, Consolas, monospace',
      attributionLogo: false,
    },
    attributionLogo: false,
    grid: {
      vertLines: { color: '#1a1f27' },
      horzLines: { color: '#1a1f27' },
    },
    crosshair: {
      mode: 0,
      // Vert line drawn by App `.stack-xhair` overlay across panes.
      vertLine: {
        visible: false,
        labelVisible: true,
        labelBackgroundColor: '#2b3139',
        color: '#474d57',
        width: 1,
      },
      horzLine: { color: '#474d57', labelBackgroundColor: '#2b3139', width: 1 },
    },
    timeScale: {
      borderColor: '#1e2329',
      timeVisible: true,
      secondsVisible: true,
      // Keep live edge tight — large rightOffset reads as "empty future".
      rightOffset: 2,
      barSpacing: 5,
    },
    handleScroll: { mouseWheel: true, pressedMouseMove: true },
    handleScale: { axisPressedMouseMove: true, mouseWheel: true, pinch: true },
  };

  function unwireSync() {
    for (const dispose of syncDisposers.splice(0)) dispose();
  }

  /** Primary series used for setCrosshairPosition on the main chart. */
  function primarySeriesApi() {
    if (!chart) return null;
    if (chartMode === 'candles') return candleSeries;
    if (focusVenue && lineSeries.has(focusVenue)) return lineSeries.get(focusVenue);
    for (const row of series) {
      if (row?.hidden) continue;
      const s = lineSeries.get(row.venue);
      if (s) return s;
    }
    for (const s of lineSeries.values()) return s;
    return null;
  }

  function publishCrosshairHandles() {
    /** @type {Array<{ id: string, chart: any, series: any }>} */
    const handles = [];
    const mainSeries = primarySeriesApi();
    if (chart && mainSeries) {
      handles.push({ id: 'main', chart, series: mainSeries });
    }
    if (bpsChart && bpsLineSeries) {
      handles.push({ id: 'bps', chart: bpsChart, series: bpsLineSeries });
    }
    try {
      onCrosshairHandles(handles);
    } catch {
      /* ignore */
    }
  }

  function markProgrammatic(ms = 180) {
    programmaticUntil = performance.now() + ms;
  }

  function observeUserRange(target) {
    const timeScale = target.timeScale();
    const onVisibleLogicalRangeChange = () => {
      emitVisibleTimeRange();
      // Programmatic scroll/fitContent and cross-pane sync must not latch unfollow.
      if (performance.now() < programmaticUntil) return;
      if (!rangeActivity.isUserDriven()) return;
      // Only unfollow when the user recently panned/zoomed (wheel/drag).
      if (performance.now() > userPanUntil) return;
      if (followLive) {
        followLive = false;
        try {
          onFollowLive(false);
        } catch {
          /* ignore */
        }
      }
    };
    timeScale.subscribeVisibleLogicalRangeChange(onVisibleLogicalRangeChange);
    return () => {
      timeScale.unsubscribeVisibleLogicalRangeChange(onVisibleLogicalRangeChange);
    };
  }

  function emitVisibleTimeRange() {
    if (!chart || toolbarOnly) return;
    try {
      const r = chart.timeScale().getVisibleRange?.();
      if (!r) return;
      const fromSec = Number(r.from);
      const toSec = Number(r.to);
      if (!Number.isFinite(fromSec) || !Number.isFinite(toSec) || toSec <= fromSec) return;
      onVisibleTimeRange({ fromSec, toSec });
    } catch {
      /* ignore */
    }
  }

  function wireHostPanGestures(el) {
    if (!el) return () => {};
    const onWheel = () => {
      userPanUntil = performance.now() + 800;
    };
    const onPointerDown = () => {
      userPanUntil = performance.now() + 1500;
    };
    el.addEventListener('wheel', onWheel, { passive: true });
    el.addEventListener('pointerdown', onPointerDown);
    return () => {
      el.removeEventListener('wheel', onWheel);
      el.removeEventListener('pointerdown', onPointerDown);
    };
  }

  /** One-way: mirror main wall-clock window onto BPS only when it actually moves. */
  function syncBpsVisibleRange(force = false) {
    if (!chart || !bpsChart) return;
    try {
      const r = chart.timeScale().getVisibleRange?.();
      if (!r) return;
      const from = Number(r.from);
      const to = Number(r.to);
      if (!Number.isFinite(from) || !Number.isFinite(to) || to <= from) return;
      const next = { from, to };
      if (!force && visibleTimeRangesNearlyEqual(lastBpsVisibleRange, next)) return;
      applyVisibleTimeRange([bpsChart], { fromSec: from, toSec: to }, rangeActivity.syncGuard);
      lastBpsVisibleRange = next;
    } catch {
      /* ignore */
    }
  }

  function scheduleBpsVisibleRangeSync(force = false) {
    if (force) {
      if (bpsRangeRaf) {
        cancelAnimationFrame(bpsRangeRaf);
        bpsRangeRaf = 0;
      }
      syncBpsVisibleRange(true);
      return;
    }
    if (bpsRangeRaf) return;
    bpsRangeRaf = requestAnimationFrame(() => {
      bpsRangeRaf = 0;
      syncBpsVisibleRange(false);
    });
  }

  function pinToLive() {
    followLive = true;
    try {
      onFollowLive(true);
    } catch {
      /* ignore */
    }
    markProgrammatic(250);
    rangeActivity.runProgrammatic(() => {
      applyFollowLiveWindow(series, candles, chartMode);
    });
  }

  /** Live edge advances only when pinned and not temporarily inspecting. */
  function shouldFollowLive() {
    return followLive && !inspecting;
  }

  /** Copy main's wall-clock window onto BPS after fitContent / setVisibleRange. */
  function syncStackedToMain() {
    if (!chart) return;
    try {
      const r = chart.timeScale().getVisibleRange?.();
      const from = Number(r?.from);
      const to = Number(r?.to);
      if (!Number.isFinite(from) || !Number.isFinite(to) || to <= from) return;
      lastBpsVisibleRange = { from, to };
      if (bpsChart) {
        applyVisibleTimeRange(
          [bpsChart],
          { fromSec: from, toSec: to },
          rangeActivity.syncGuard,
        );
      }
    } catch {
      /* scale may not be ready */
    }
  }

  /**
   * Fit the shared time window to available history (or the last sessionSec
   * of it). `fitContent` when history is shorter than the session — otherwise
   * `scrollToRealTime` / fixed barSpacing leave empty logical indices on the left.
   */
  function applyFollowLiveWindow(rows, candleRows, mode) {
    if (!chart) return;
    const extent =
      mode === 'candles'
        ? seriesTimeExtent(null, candleRows)
        : seriesTimeExtent(rows);
    const win = liveVisibleWindow(extent?.first, extent?.last, sessionWindowSec);
    const fit = shouldFitLiveContent(extent?.first, extent?.last, sessionWindowSec);
    if (!win || fit) {
      try {
        chart.timeScale().fitContent();
      } catch {
        try {
          chart.timeScale().scrollToRealTime();
        } catch {
          /* ignore */
        }
      }
      // fitContent applies on the next layout pass — copy the resulting
      // wall-clock window onto BPS after it exists.
      requestAnimationFrame(() => syncStackedToMain());
      return;
    }
    applyVisibleTimeRange(
      [chart, bpsChart].filter(Boolean),
      { fromSec: win.from, toSec: win.to },
      rangeActivity.syncGuard,
    );
    lastBpsVisibleRange = { from: win.from, to: win.to };
  }

  function captureFrozenVisibleRange() {
    if (!chart) return;
    try {
      const r = chart.timeScale().getVisibleRange?.();
      if (!r) return;
      const from = Number(r.from);
      const to = Number(r.to);
      if (!Number.isFinite(from) || !Number.isFinite(to) || to <= from) return;
      frozenVisibleRange = { from, to };
    } catch {
      /* ignore */
    }
  }

  function applyFrozenVisibleRange() {
    if (!chart || !frozenVisibleRange) return;
    markProgrammatic(80);
    rangeActivity.runProgrammatic(() => {
      applyVisibleTimeRange(
        [chart, bpsChart].filter(Boolean),
        { fromSec: frozenVisibleRange.from, toSec: frozenVisibleRange.to },
        rangeActivity.syncGuard,
      );
      lastBpsVisibleRange = { ...frozenVisibleRange };
    });
  }

  $effect(() => {
    if (inspecting) {
      if (!frozenVisibleRange) captureFrozenVisibleRange();
    } else if (frozenVisibleRange) {
      frozenVisibleRange = null;
    }
  });

  function clearMarkers() {
    if (markersApi) {
      try { markersApi.setMarkers([]); } catch { /* series may already be gone */ }
    }
    markersApi = null;
    markerSeries = null;
  }

  onMount(() => {
    const onVis = () => {
      tabHidden = document.visibilityState === 'hidden';
    };
    const onKeys = (ev) => {
      if (ev.target?.matches?.('input, textarea, select')) return;
      if (ev.key === '?' || (ev.shiftKey && ev.key === '/')) {
        ev.preventDefault();
        showSettings = !showSettings;
      }
    };
    onVis();
    document.addEventListener('visibilitychange', onVis);
    window.addEventListener('keydown', onKeys);
    return () => {
      document.removeEventListener('visibilitychange', onVis);
      window.removeEventListener('keydown', onKeys);
      ready = false;
      if (chartPaintRaf) cancelAnimationFrame(chartPaintRaf);
      chartPaintRaf = 0;
      if (hiddenPollTimer) clearTimeout(hiddenPollTimer);
      destroyCharts();
    };
  });

  function destroyCharts() {
    if (chartPaintRaf) cancelAnimationFrame(chartPaintRaf);
    chartPaintRaf = 0;
    if (bpsRangeRaf) cancelAnimationFrame(bpsRangeRaf);
    bpsRangeRaf = 0;
    pendingChart = null;
    unwireSync();
    hostPointerDisposer?.();
    hostPointerDisposer = null;
    hostResizeObserver?.disconnect();
    hostResizeObserver = null;
    bpsInteractionDisposer?.();
    bpsInteractionDisposer = null;
    chartInteractionDisposer?.();
    chartInteractionDisposer = null;
    clearMarkers();
    bpsChart?.remove();
    bpsChart = null;
    lastBpsWindow = null;
    lastBpsVisibleRange = null;
    if (chart) {
      chart.remove();
      chart = null;
      try {
        onMainChart(null);
      } catch {
        /* ignore */
      }
    }
    lineSeries.clear();
    lastLineWindow.clear();
    lastCandleWindow = null;
    candleSeries = null;
    volumeSeries = null;
    bpsLineSeries = null;
    volMarginsOn = null;
    fitKey = '';
    followLive = true;
    try {
      onCrosshairHandles([]);
    } catch {
      /* ignore */
    }
  }

  // Create/destroy chart when toolbarOnly toggles (single App instance).
  $effect(() => {
    if (toolbarOnly) {
      if (chart) {
        ready = false;
        destroyCharts();
      }
      return;
    }
    if (!host || chart) return;
    chart = createChart(host, {
      ...chartOpts,
      autoSize: true,
      rightPriceScale: {
        borderColor: '#1e2329',
        scaleMargins: { top: 0.06, bottom: showVolume ? 0.18 : 0.06 },
        entireTextOnly: true,
        minimumWidth: PRICE_SCALE_MIN_WIDTH,
      },
    });
    chartInteractionDisposer = observeUserRange(chart);
    hostPointerDisposer = wireHostPanGestures(host);
    hostResizeObserver?.disconnect();
    hostResizeObserver = new ResizeObserver(() => {
      if (!shouldFollowLive()) return;
      markProgrammatic(120);
      rangeActivity.runProgrammatic(() => {
        applyFollowLiveWindow(series, candles, chartMode);
      });
    });
    try {
      hostResizeObserver.observe(host);
    } catch {
      /* ignore */
    }
    followLive = true;
    ready = true;
    try {
      onMainChart(chart);
    } catch {
      /* ignore */
    }
    publishCrosshairHandles();
  });

  $effect(() => {
    if (!ready || chartMode !== 'lines' || !showBpsPane) {
      if (bpsChart) {
        unwireSync();
        if (bpsRangeRaf) {
          cancelAnimationFrame(bpsRangeRaf);
          bpsRangeRaf = 0;
        }
        bpsChart.remove();
        bpsChart = null;
        bpsLineSeries = null;
        lastBpsWindow = null;
        lastBpsVisibleRange = null;
        publishCrosshairHandles();
      }
      return;
    }
    if (!bpsHost || bpsChart) return;
    bpsChart = createChart(bpsHost, {
      ...chartOpts,
      autoSize: true,
      // Slave pane: fixed scale chrome so auto-scale label width can't shove the plot.
      rightPriceScale: {
        borderColor: '#1e2329',
        scaleMargins: { top: 0.12, bottom: 0.12 },
        entireTextOnly: true,
        minimumWidth: PRICE_SCALE_MIN_WIDTH,
      },
      timeScale: {
        ...chartOpts.timeScale,
        // Keep ticks, but BPS never drives interaction — main owns the window.
        rightOffset: 2,
        barSpacing: 5,
      },
      handleScroll: false,
      handleScale: false,
    });
    bpsLineSeries = bpsChart.addSeries(LineSeries, {
      color: '#f6465d',
      lineWidth: 1,
      // Fixed-width labels (no " bps" suffix) — width thrash was shifting the plot.
      priceFormat: { type: 'custom', formatter: (v) => Number(v).toFixed(2), minMove: 0.01 },
    });
    // One-way time sync: logical sync desynced BPS wall-clock from Lines when
    // bar densities differed; bidirectional let BPS yank the main plot.
    syncDisposers = [
      wireChartTimeScales(chart, [bpsChart], rangeActivity.syncGuard, {
        mode: 'time',
        bidirectional: false,
      }),
    ];
    lastBpsVisibleRange = null;
    // Do NOT observeUserRange on BPS — async LWC callbacks after sync can
    // latch unfollow on the main chart when the user recently panned.
    publishCrosshairHandles();
    scheduleBpsVisibleRangeSync(true);
  });

  function ensureCandleSeries() {
    if (!chart || candleSeries) return;
    candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: '#02c076',
      downColor: '#f6465d',
      borderUpColor: '#02c076',
      borderDownColor: '#f6465d',
      wickUpColor: '#02c076',
      wickDownColor: '#f6465d',
      priceFormat: { type: 'price', precision: 2, minMove: 0.01 },
    });
  }

  function ensureVolumeSeries() {
    if (!chart || volumeSeries) return;
    volumeSeries = chart.addSeries(HistogramSeries, {
      priceFormat: { type: 'custom', formatter: (v) => fmtUsd(v), minMove: 1 },
      priceScaleId: 'vol',
      lastValueVisible: false,
      priceLineVisible: false,
    });
    chart.priceScale('vol').applyOptions({
      scaleMargins: { top: 0.78, bottom: 0 },
      borderVisible: false,
    });
  }

  function clearVolume() {
    if (volumeSeries && chart) {
      try { chart.removeSeries(volumeSeries); } catch { /* ignore */ }
      volumeSeries = null;
    }
  }

  function clearLines() {
    for (const s of lineSeries.values()) {
      if (markerSeries === s) clearMarkers();
      try { chart?.removeSeries(s); } catch { /* ignore */ }
    }
    lineSeries.clear();
  }

  function clearCandles() {
    if (candleSeries && chart) {
      if (markerSeries === candleSeries) clearMarkers();
      try { chart.removeSeries(candleSeries); } catch { /* ignore */ }
      candleSeries = null;
    }
  }

  function priceFormatForMode(mode) {
    if (mode === 'percent') {
      return { type: 'custom', formatter: (v) => `${v.toFixed(3)}%`, minMove: 0.001 };
    }
    return { type: 'price', precision: 2, minMove: 0.01 };
  }

  function applyVolumeData(bars) {
    if (!showVolume) {
      clearVolume();
      if (volMarginsOn !== false) {
        chart?.applyOptions({ rightPriceScale: { scaleMargins: { top: 0.06, bottom: 0.06 } } });
        volMarginsOn = false;
      }
      return;
    }
    ensureVolumeSeries();
    if (volMarginsOn !== true) {
      chart.applyOptions({ rightPriceScale: { scaleMargins: { top: 0.06, bottom: 0.18 } } });
      volMarginsOn = true;
    }
    const list = bars || [];
    if (!list.length) {
      volumeSeries.setData([]);
      lastVolFullAt = performance.now();
      return;
    }
    const now = performance.now();
    const tip = list[list.length - 1];
    // Prefer update when tip advances; full replace on a throttle for trim.
    if (now - lastVolFullAt > FULL_SET_MIN_MS) {
      volumeSeries.setData(list);
      lastVolFullAt = now;
    } else {
      try {
        volumeSeries.update(tip);
      } catch {
        volumeSeries.setData(list);
        lastVolFullAt = now;
      }
    }
  }

  /**
   * Incremental chart write: update tip when possible; throttle full setData
   * so sliding session windows don't rewrite thousands of points every second.
   * @param {any} seriesApi
   * @param {Array<{time:number}>} data
   * @param {{ first: number, last: number, fullAt: number }|null|undefined} prev
   * @returns {{ first: number, last: number, fullAt: number }|null}
   */
  function writeSeriesData(seriesApi, data, prev) {
    if (!data.length) {
      seriesApi.setData([]);
      return null;
    }
    const first = data[0].time;
    const last = data[data.length - 1];
    const now = performance.now();
    const tipAdvanced = prev && last.time >= prev.last;
    const leftStable = prev && prev.first === first;
    const recentFull = prev && now - prev.fullAt < FULL_SET_MIN_MS;
    if (tipAdvanced && (leftStable || recentFull)) {
      try {
        seriesApi.update(last);
        return { first: prev.first, last: last.time, fullAt: prev.fullAt };
      } catch {
        /* fall through to setData */
      }
    }
    seriesApi.setData(data);
    return { first, last: last.time, fullAt: now };
  }

  function setHighlight(primarySeries, sec) {
    if (!primarySeries || sec == null || !Number.isFinite(sec)) {
      clearMarkers();
      return;
    }
    if (markerSeries && markerSeries !== primarySeries) clearMarkers();
    if (!markersApi) {
      try {
        markersApi = createSeriesMarkers(primarySeries, []);
        markerSeries = primarySeries;
      } catch {
        clearMarkers();
        return;
      }
    }
    try {
      markersApi.setMarkers([{ time: sec, position: 'aboveBar', color: '#f0b90b', shape: 'arrowDown', text: 'trade' }]);
    } catch {
      clearMarkers();
    }
  }

  function paintChart() {
    if (!ready || !chart) return;

    const mode = pendingChart?.mode ?? chartMode;
    const pmode = pendingChart?.pmode ?? priceMode;
    const tf = pendingChart?.tf ?? timeframe;
    const a = pendingChart?.a ?? asset;
    const volOn = pendingChart?.volOn ?? showVolume;
    const rows = pendingChart?.rows ?? series;
    const candleRows = pendingChart?.candles ?? candles;
    const volBarsIn = pendingChart?.volumeBars ?? volumeBars;
    const bps = pendingChart?.bps ?? bpsHistory;
    const hl = pendingChart?.hl ?? highlightSec;
    const key = `${mode}|${pmode}|${tf}|${a}|${volOn}`;
    const needRefit = key !== fitKey;

    if (mode === 'candles') {
      clearLines();
      lastLineWindow.clear();
      ensureCandleSeries();
      const mapped = candleRows.length
        ? candleRows.map((x) => ({
            time: x.time,
            open: x.open,
            high: x.high,
            low: x.low,
            close: x.close,
          }))
        : [];
      const span = mapped.length
        ? Math.max(1, mapped[mapped.length - 1].time - mapped[0].time)
        : sessionWindowSec;
      const painted = downsampleForChart(mapped, span, CHART_DISPLAY_MAX_POINTS);
      if (painted.length) {
        lastCandleWindow = writeSeriesData(candleSeries, painted, lastCandleWindow);
      } else {
        candleSeries.setData([]);
        lastCandleWindow = null;
      }
      const volSpan = volBarsIn?.length
        ? Math.max(1, volBarsIn[volBarsIn.length - 1].time - volBarsIn[0].time)
        : span;
      applyVolumeData(downsampleForChart(volBarsIn || [], volSpan, CHART_DISPLAY_MAX_POINTS));
      setHighlight(candleSeries, hl);
    } else if (mode === 'lines') {
      clearCandles();
      lastCandleWindow = null;
      const want = new Set(rows.map((r) => r.venue));

      for (const id of [...lineSeries.keys()]) {
        if (!want.has(id)) {
          const removed = lineSeries.get(id);
          if (markerSeries === removed) clearMarkers();
          try { chart.removeSeries(removed); } catch { /* ignore */ }
          lineSeries.delete(id);
          lastLineWindow.delete(id);
        }
      }

      const fmt = priceFormatForMode(pmode);
      let primary = null;
      for (const row of rows) {
        let s = lineSeries.get(row.venue);
        const isHl = highlightVenues.includes(row.venue);
        if (!s) {
          s = chart.addSeries(LineSeries, {
            color: row.color,
            lineWidth: row.venue === focusVenue || isHl ? 3 : 2,
            priceLineVisible: false,
            lastValueVisible: true,
            crosshairMarkerVisible: true,
            crosshairMarkerRadius: 3,
            priceFormat: fmt,
          });
          lineSeries.set(row.venue, s);
          lastLineWindow.delete(row.venue);
        } else {
          s.applyOptions({
            color: row.color,
            priceFormat: fmt,
            lineWidth: row.venue === focusVenue || isHl ? 3 : 2,
            visible: !row.hidden,
          });
        }
        const data = row.data || [];
        if (!data.length) {
          s.setData([]);
          lastLineWindow.delete(row.venue);
        } else {
          const next = writeSeriesData(s, data, lastLineWindow.get(row.venue));
          if (next) lastLineWindow.set(row.venue, next);
          else lastLineWindow.delete(row.venue);
        }
        if (!primary && data.length && !row.hidden) primary = s;
        if (row.venue === focusVenue && data.length) primary = s;
      }

      let bars = volBarsIn;
      if (!bars?.length) {
        const focus = rows.find((r) => r.venue === focusVenue);
        bars = focus?.volumeData?.length ? focus.volumeData : mergeVolume(rows.filter((r) => !r.hidden));
      }
      const volSpan = bars?.length > 1
        ? Math.max(1, bars[bars.length - 1].time - bars[0].time)
        : sessionWindowSec;
      applyVolumeData(downsampleForChart(bars || [], volSpan, CHART_DISPLAY_MAX_POINTS));
      setHighlight(primary, hl);

      if (bpsLineSeries && bpsChart) {
        // Cover main retention window with step-hold so time-range sync can
        // always apply the Lines visible window (no 1s-only BPS pane).
        let fromSec = null;
        let toSec = null;
        for (const row of rows) {
          for (const pt of row.data || []) {
            const t = Number(pt.time);
            if (!Number.isFinite(t)) continue;
            if (fromSec == null || t < fromSec) fromSec = t;
            if (toSec == null || t > toSec) toSec = t;
          }
        }
        const sparse = (bps || [])
          .filter((p) => p && Number.isFinite(p.time) && Number.isFinite(p.bps))
          .map((p) => ({ t: Number(p.time), v: Number(p.bps) }));
        const held =
          fromSec != null && toSec != null && toSec >= fromSec
            ? stepHoldSeries(sparse, fromSec, toSec)
            : sparse.map((p) => ({ time: p.t, value: p.v }));
        const bpsSpan =
          held.length > 1 ? Math.max(1, held[held.length - 1].time - held[0].time) : sessionWindowSec;
        const bpsData = downsampleForChart(held, bpsSpan, CHART_DISPLAY_MAX_POINTS);
        let didFullSet = false;
        if (!bpsData.length) {
          bpsLineSeries.setData([]);
          lastBpsTip = null;
          lastBpsWindow = null;
          lastBpsFullAt = performance.now();
          didFullSet = true;
        } else {
          const prev = lastBpsWindow;
          const tip = bpsData[bpsData.length - 1];
          const now = performance.now();
          const tipOnly =
            prev &&
            tip.time === prev.last &&
            bpsData[0].time === prev.first &&
            now - prev.fullAt < FULL_SET_MIN_MS;
          if (tipOnly) {
            try {
              bpsLineSeries.update(tip);
              lastBpsTip = tip.time;
              lastBpsWindow = { first: prev.first, last: tip.time, fullAt: prev.fullAt };
            } catch {
              bpsLineSeries.setData(bpsData);
              lastBpsTip = tip.time;
              lastBpsWindow = { first: bpsData[0].time, last: tip.time, fullAt: now };
              lastBpsFullAt = now;
              didFullSet = true;
            }
          } else {
            const next = writeSeriesData(bpsLineSeries, bpsData, prev);
            lastBpsWindow = next;
            lastBpsTip = tip.time;
            lastBpsFullAt = next?.fullAt ?? now;
            // writeSeriesData may have used update — only force-relock after setData.
            didFullSet = !(
              prev &&
              next &&
              next.fullAt === prev.fullAt &&
              next.last >= prev.last
            );
          }
        }
        // Full setData resets LWC visible range — re-lock once. Tip updates
        // rely on the one-way subscribe (coalesced) so we don't fight live pin.
        if (didFullSet) scheduleBpsVisibleRangeSync(true);
        else scheduleBpsVisibleRangeSync(false);
      }
    }

    if (needRefit) {
      fitKey = key;
      followLive = true;
      markProgrammatic(250);
      requestAnimationFrame(() => {
        markProgrammatic(250);
        rangeActivity.runProgrammatic(() => {
          applyFollowLiveWindow(rows, candleRows, mode);
        });
      });
    } else if (inspecting) {
      // New bars would otherwise slide a fixed logical window forward in wall
      // time — re-assert the captured wall-clock range every paint.
      if (!frozenVisibleRange) captureFrozenVisibleRange();
      applyFrozenVisibleRange();
    } else if (shouldFollowLive()) {
      const now = performance.now();
      if (now - lastScrollAt > 250) {
        lastScrollAt = now;
        markProgrammatic(120);
        rangeActivity.runProgrammatic(() => {
          applyFollowLiveWindow(rows, candleRows, mode);
        });
      }
    }

    // Debug probe for soak/browser proofs (last data tip vs visible logical range).
    try {
      let dataLast = null;
      if (mode === 'candles' && lastCandleWindow) dataLast = lastCandleWindow.last;
      else {
        for (const w of lastLineWindow.values()) {
          if (dataLast == null || w.last > dataLast) dataLast = w.last;
        }
      }
      const logical = chart?.timeScale()?.getVisibleLogicalRange?.() || null;
      // @ts-ignore
      const visible = chart?.timeScale()?.getVisibleRange?.() || null;
      globalThis.__mfChartDebug = {
        mode,
        followLive,
        inspecting,
        shouldFollow: shouldFollowLive(),
        frozen: frozenVisibleRange,
        dataLast,
        logical,
        visible: visible
          ? { from: Number(visible.from), to: Number(visible.to) }
          : null,
        wallSec: Math.floor(Date.now() / 1000),
        gapSec:
          dataLast != null ? Math.floor(Date.now() / 1000) - dataLast : null,
        logicalFrom: logical ? Number(logical.from) : null,
      };
    } catch {
      /* ignore */
    }

    emitVisibleTimeRange();
    publishCrosshairHandles();
  }

  $effect(() => {
    if (!ready || !chart || toolbarOnly) return;
    // Capture reactive deps; coalesce paints via rAF.
    pendingChart = {
      mode: chartMode,
      pmode: priceMode,
      tf: timeframe,
      a: asset,
      volOn: showVolume,
      rows: series,
      candles,
      volumeBars,
      bps: bpsHistory,
      hl: highlightSec,
    };
    highlightVenues;
    focusVenue;
    inspecting;
    followLive;
    sessionWindowSec;
    // Background tabs: paint at most ~2 Hz to cut CPU while retaining buffers.
    if (tabHidden) {
      if (!hiddenPollTimer) {
        hiddenPollTimer = setTimeout(() => {
          hiddenPollTimer = 0;
          paintChart();
        }, 500);
      }
      return;
    }
    if (!chartPaintRaf) {
      chartPaintRaf = requestAnimationFrame(() => {
        chartPaintRaf = 0;
        paintChart();
      });
    }
  });

  function mergeVolume(rows) {
    const m = new Map();
    for (const r of rows) {
      for (const p of r.volumeData || []) {
        m.set(p.time, (m.get(p.time) || 0) + p.value);
      }
    }
    return [...m.entries()].sort((a, b) => a[0] - b[0]).map(([time, value]) => ({ time, value, color: 'rgba(240,185,11,0.45)' }));
  }

  function onLegendClick(row, ev) {
    if (ev.shiftKey || ev.metaKey || ev.ctrlKey) {
      onFocusVenue(row.venue, row.symbol);
      return;
    }
    onToggleVenue(row.venue);
  }

  /** Rich hover details for venue legend rows (Δ role, last, flow). */
  function legendHoverTitle(row) {
    const parts = [row.venue];
    if (row.symbol) parts.push(row.symbol);
    parts.push(row.live ? 'live' : 'offline');
    if (row.hidden) parts.push('hidden');
    if (row.last != null) parts.push(`last ${fmtPrice(row.last, 2)}`);
    if (row.pct != null) parts.push(fmtPct(row.pct, 3));
    parts.push(`USD ${fmtUsd(row.windowNotional ?? row.tradeNotional ?? 0)}`);
    parts.push(`qty ${row.tradeVolume ?? 0}`);
    parts.push(fmtTradesPerMin(row.windowTrades ?? row.tradeCount ?? 0, sessionWindowSec));
    if (discrepancy?.bps != null) {
      if (row.venue === discrepancy.highVenue) {
        parts.push(`Δ high · ${discrepancy.bps.toFixed(2)} bps`);
      } else if (row.venue === discrepancy.lowVenue) {
        parts.push(`Δ low · ${discrepancy.bps.toFixed(2)} bps`);
      } else {
        parts.push(`cross Δ ${discrepancy.bps.toFixed(2)} bps (alert ≥ ${alertBpsThreshold})`);
      }
    } else if (highlightVenues.includes(row.venue)) {
      parts.push('Δ highlight');
    }
    if (row.venue === focusVenue) parts.push('focus');
    return parts.join(' · ');
  }
</script>

<section class="chart-panel" class:toolbar-only={toolbarOnly}>
  <div class="toolbar">
    <div class="left">
      <div class="assets">
        {#each coverage.length ? coverage : assets.map((a) => ({ asset: a, total: 0, live: 0 })) as c}
          <button type="button" class:active={asset === c.asset} onclick={() => onAsset(c.asset)} title={c.total ? `${c.asset}: ${c.live}/${c.total} live` : c.asset}>
            {c.asset}
            {#if c.total}<span class="cov" class:partial={c.live < c.total} class:ok={c.live === c.total && c.total > 0}>{c.live}/{c.total}</span>{/if}
          </button>
        {/each}
      </div>
      <div class="modes">
        <button type="button" class:active={chartMode === 'lines'} onclick={() => onChartMode('lines')}>Lines</button>
        <button type="button" class:active={chartMode === 'candles'} onclick={() => onChartMode('candles')}>Candles</button>
        <button type="button" class:active={chartMode === 'orderflow'} onclick={() => onChartMode('orderflow')} title="L2+tape liquidity heatmap + DOM (not MBO)">Order Flow</button>
      </div>
      <div class="modes">
        <button type="button" class:active={priceMode === 'percent'} onclick={() => onPriceMode('percent')}>%</button>
        <button type="button" class:active={priceMode === 'absolute'} onclick={() => onPriceMode('absolute')}>Price</button>
      </div>
      <div class="modes">
        <button type="button" class:active={showVolume} onclick={() => onShowVolume(!showVolume)} title="USD volume subplot">Vol</button>
        <button
          type="button"
          class:active={followLive}
          class:live-pin={followLive}
          onclick={() => pinToLive()}
          title="Pin to live edge and fit X to the session window (or shorter available history)"
        >Live</button>
        <button type="button" class:active={showSettings} onclick={() => (showSettings = !showSettings)}>⚙</button>
      </div>
      {#if discrepancy}
        <span
          class="disc"
          class:alert={discrepancy.bps != null && discrepancy.bps > alertBpsThreshold}
          title={[
            discrepancy.bps != null ? `Δ ${discrepancy.bps.toFixed(2)} bps` : null,
            `abs ${fmtPrice(discrepancy.abs, 2)}`,
            `high ${discrepancy.highVenue || '—'} @ ${fmtPrice(discrepancy.max, 2)}`,
            `low ${discrepancy.lowVenue || '—'} @ ${fmtPrice(discrepancy.min, 2)}`,
            `alert ≥ ${alertBpsThreshold} bps`,
          ].filter(Boolean).join(' · ')}
        >
          Δ {fmtPrice(discrepancy.abs, 2)}
          {#if discrepancy.bps != null}<span class="muted">({discrepancy.bps.toFixed(2)} bps)</span>{/if}
        </span>
      {/if}
    </div>
    <div class="tfs">
      {#each TIMEFRAMES as tf, i}
        <button type="button" class:active={timeframe === tf.id} onclick={() => onTimeframe(tf.id)} title={`Shortcut: ${i + 1}`}>{tf.label}</button>
      {/each}
    </div>
  </div>

  {#if showSettings}
    <div class="settings">
      <label>Book depth <input type="number" min="5" max="50" use:numericCommit={{ value: bookDepth, min: 5, max: 50, integer: true, onCommit: onBookDepth }} /></label>
      <label>Tape limit <input type="number" min="20" max="500" use:numericCommit={{ value: tapeLimit, min: 20, max: 500, integer: true, onCommit: onTapeLimit }} /></label>
      <label>Focus poll ms <input type="number" min="80" max="2000" step="20" use:numericCommit={{ value: pollFocusMs, min: 80, max: 2000, integer: true, onCommit: onPollFocus }} /></label>
      <label>Multi poll ms <input type="number" min="100" max="5000" step="20" use:numericCommit={{ value: pollMultiMs, min: 100, max: 5000, integer: true, onCommit: onPollMulti }} /></label>
      <label>Alert bps <input type="number" min="1" max="500" use:numericCommit={{ value: alertBpsThreshold, min: 1, max: 500, onCommit: onAlertBps }} /></label>
      <label>Webhook <input type="url" placeholder="https://…" value={webhookUrl} onchange={(e) => onWebhook(e.currentTarget.value)} /></label>
      <label>History sec <input type="number" min="300" max="7200" step="300" use:numericCommit={{ value: historySecs, min: 300, max: 7200, integer: true, onCommit: onHistorySecs }} /></label>
      <button type="button" class="test-alert" onclick={() => onTestAlert()}>Test alert</button>
      <p class="keys-hint">Keys: <kbd>/</kbd> search · <kbd>1</kbd>–<kbd>5</kbd> TF · <kbd>F</kbd> dock · <kbd>Esc</kbd> hide dock · <kbd>?</kbd> settings</p>
    </div>
  {/if}

  {#if !toolbarOnly}
    {#if chartMode === 'lines'}
      <div class="legend">
        {#each series as row (row.venue)}
          <button
            type="button"
            class="leg"
            class:dim={!row.data?.length || row.hidden}
            class:focus={row.venue === focusVenue}
            class:hl={highlightVenues.includes(row.venue)}
            onclick={(e) => onLegendClick(row, e)}
            title={legendHoverTitle(row)}
          >
            <span class="swatch" style={`background:${row.color}`}></span>
            <span class="name">{row.venue}</span>
            <span class="live-dot" class:ok={row.live} class:bad={!row.live}>{row.live ? '●' : '○'}</span>
            <span class="px" style={`color:${row.color}`}>{row.last != null ? fmtPrice(row.last, 2) : '—'}</span>
            <span class="pct" class:up={row.pct > 0} class:down={row.pct < 0}>{row.pct != null ? fmtPct(row.pct, 3) : '—'}</span>
            <span class="vol">{fmtUsd(row.windowNotional ?? row.tradeNotional ?? 0)}/{fmtTradesPerMin(row.windowTrades ?? row.tradeCount ?? 0, sessionWindowSec)}</span>
          </button>
        {/each}
      </div>
    {/if}

    <div class="chart-wrap">
      <ChartHoverLegend legend={hoverLegend} />
      <div class="chart-host" bind:this={host}></div>
      {#if chartMode === 'lines' && showBpsPane}
        <button
          type="button"
          class="bps-splitter"
          class:dragging={bpsSplitDragging}
          aria-label="Resize discrepancy pane"
          title="Drag to resize Δbps pane"
          onpointerdown={(event) => {
            bpsSplitDragging = true;
            beginAxisDrag(event, {
              axis: 'y',
              startValue: bpsHeight,
              min: 40,
              max: 160,
              onChange: onBpsHeight,
              onEnd: () => {
                bpsSplitDragging = false;
              },
            });
          }}
        ></button>
        <div class="bps-host" bind:this={bpsHost} style={`height:${bpsHeight}px`}></div>
      {/if}
      {#if chartMode === 'lines' && !series.some((s) => s.data?.length && !s.hidden)}
        <div class="overlay">streaming multi-venue prices…</div>
      {:else if chartMode === 'candles' && !candles.length}
        <div class="overlay">accumulating trades for candles…</div>
      {/if}
    </div>
  {/if}
</section>

<style>
  .chart-panel {
    display: flex;
    flex-direction: column;
    min-height: 0;
    flex: 1 1 0;
    height: auto;
    background: var(--panel);
  }
  .chart-panel.toolbar-only {
    height: auto;
    flex: 0 0 auto;
  }

  .toolbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.6rem;
    padding: var(--panel-pad, 0.3rem 0.5rem);
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
    flex-wrap: wrap;
  }

  .left { display: flex; align-items: center; gap: 0.55rem; min-width: 0; flex-wrap: wrap; }
  .assets, .modes, .tfs { display: flex; gap: 0.12rem; }

  .assets button, .modes button, .tfs button {
    background: transparent;
    border: 1px solid transparent;
    color: var(--muted);
    font-family: var(--mono);
    font-size: 0.7rem;
    padding: 0.15rem 0.4rem;
    cursor: pointer;
    border-radius: 2px;
  }

  .assets button:hover, .modes button:hover, .tfs button:hover { color: var(--text); background: var(--panel-2); }
  .assets button.active { color: #0b0e11; background: var(--accent); font-weight: 700; }
  .modes button.active, .tfs button.active { color: var(--accent); border-color: rgba(240, 185, 11, 0.35); background: rgba(240, 185, 11, 0.08); }
  .modes button.live-pin { color: var(--bid); border-color: rgba(2, 192, 118, 0.45); background: rgba(2, 192, 118, 0.1); }

  .assets button .cov { margin-left: 0.2rem; font-size: 0.55rem; opacity: 0.75; }
  .assets button:not(.active) .cov.partial { color: var(--ask); }
  .assets button:not(.active) .cov.ok { color: var(--bid); }

  .disc { font-family: var(--mono); font-size: 0.7rem; color: var(--text-dim); }
  .disc.alert { color: var(--ask); }
  .muted { color: var(--muted); }

  .settings {
    display: flex; flex-wrap: wrap; gap: 0.75rem 1rem; align-items: center;
    padding: 0.35rem 0.55rem; border-bottom: 1px solid var(--border); background: var(--panel-2);
  }
  .settings label { display: flex; align-items: center; gap: 0.35rem; font-size: 0.68rem; color: var(--muted); font-family: var(--mono); }
  .settings input { background: var(--bg); border: 1px solid var(--border); color: var(--text); padding: 0.15rem 0.3rem; font-family: var(--mono); font-size: 0.7rem; }
  .settings input[type='url'] { width: 10rem; }
  .settings .test-alert {
    font-family: var(--mono);
    font-size: 0.68rem;
    color: var(--text);
    background: var(--bg);
    border: 1px solid var(--border);
    padding: 0.12rem 0.4rem;
    cursor: pointer;
  }
  .settings .test-alert:hover { border-color: var(--accent); }
  .settings .keys-hint {
    flex: 1 1 100%;
    margin: 0;
    font-family: var(--mono);
    font-size: 0.58rem;
    color: var(--muted);
  }
  .settings kbd {
    font-family: var(--mono);
    font-size: 0.58rem;
    border: 1px solid var(--border);
    padding: 0 0.22rem;
    color: var(--text);
  }

  .legend {
    display: flex; flex-wrap: wrap; gap: 0.35rem 0.55rem;
    padding: 0.3rem 0.55rem; border-bottom: 1px solid var(--border);
    max-height: 5.2rem; overflow: auto; flex-shrink: 0;
  }

  .leg {
    display: flex; align-items: baseline; gap: 0.3rem;
    font-family: var(--mono); font-size: 0.68rem;
    background: transparent; border: 1px solid transparent; color: inherit;
    cursor: pointer; padding: 0.1rem 0.25rem; border-radius: 2px;
  }
  .leg:hover { background: var(--panel-2); border-color: var(--border); }
  .leg.dim { opacity: 0.4; }
  .leg.focus, .leg.hl { border-color: rgba(240, 185, 11, 0.4); background: rgba(240, 185, 11, 0.08); }

  .swatch { width: 8px; height: 8px; border-radius: 1px; flex-shrink: 0; }
  .name { color: var(--muted); max-width: 7.5rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .live-dot { font-size: 0.5rem; }
  .live-dot.ok { color: var(--bid); }
  .live-dot.bad { color: var(--ask); }
  .px { font-weight: 600; min-width: 5.5rem; text-align: right; font-variant-numeric: tabular-nums; }
  .pct { color: var(--muted); min-width: 4.2rem; text-align: right; font-variant-numeric: tabular-nums; }
  .pct.up { color: var(--bid); }
  .pct.down { color: var(--ask); }
  .vol { color: var(--muted); font-size: 0.6rem; min-width: 5.5rem; font-variant-numeric: tabular-nums; }

  .chart-wrap { position: relative; flex: 1; min-height: 0; display: flex; flex-direction: column; }
  .chart-host { flex: 1; min-height: 0; position: relative; }
  .bps-splitter {
    flex-shrink: 0;
    height: 6px;
    padding: 0;
    cursor: row-resize;
    background: #171c23;
    border: 0;
    border-top: 1px solid var(--border);
  }
  .bps-splitter:hover,
  .bps-splitter.dragging,
  .bps-splitter:focus-visible {
    background: rgba(240, 185, 11, 0.22);
  }
  .bps-host { height: 64px; flex-shrink: 0; }

  .overlay {
    position: absolute; inset: 0; display: grid; place-items: center;
    color: var(--muted); font-family: var(--mono); font-size: 0.75rem;
    pointer-events: none; background: rgba(18, 22, 28, 0.35);
  }
</style>
