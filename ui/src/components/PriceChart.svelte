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
  import { createRangeActivity, wireVisibleLogicalRangeSync } from '../lib/chartSync.js';

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
    toolbarOnly = false,
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
  /** When true, keep time scale pinned to the newest bar (scrollToRealTime). */
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
  /** Full setData at most this often when only the left edge slides. */
  const FULL_SET_MIN_MS = 12000;
  let tabHidden = false;
  let hiddenPollTimer = 0;

  const chartOpts = {
    layout: {
      background: { type: ColorType.Solid, color: '#12161c' },
      textColor: '#848e9c',
      fontSize: 11,
      fontFamily: 'IBM Plex Mono, SF Mono, Menlo, Consolas, monospace',
    },
    grid: {
      vertLines: { color: '#1a1f27' },
      horzLines: { color: '#1a1f27' },
    },
    crosshair: {
      mode: 0,
      vertLine: { color: '#474d57', labelBackgroundColor: '#2b3139', width: 1 },
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

  function markProgrammatic(ms = 180) {
    programmaticUntil = performance.now() + ms;
  }

  function observeUserRange(target) {
    const timeScale = target.timeScale();
    const onVisibleLogicalRangeChange = () => {
      // Programmatic scroll/fitContent and cross-pane sync must not latch unfollow.
      if (performance.now() < programmaticUntil) return;
      if (!rangeActivity.isUserDriven()) return;
      // Only unfollow when the user recently panned/zoomed (wheel/drag).
      if (performance.now() > userPanUntil) return;
      followLive = false;
    };
    timeScale.subscribeVisibleLogicalRangeChange(onVisibleLogicalRangeChange);
    return () => {
      timeScale.unsubscribeVisibleLogicalRangeChange(onVisibleLogicalRangeChange);
    };
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

  function pinToLive() {
    followLive = true;
    markProgrammatic(250);
    rangeActivity.runProgrammatic(() => {
      chart?.timeScale().scrollToRealTime();
      bpsChart?.timeScale().scrollToRealTime();
    });
  }

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
    onVis();
    document.addEventListener('visibilitychange', onVis);
    return () => {
      document.removeEventListener('visibilitychange', onVis);
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
    pendingChart = null;
    unwireSync();
    hostPointerDisposer?.();
    hostPointerDisposer = null;
    bpsInteractionDisposer?.();
    bpsInteractionDisposer = null;
    chartInteractionDisposer?.();
    chartInteractionDisposer = null;
    clearMarkers();
    bpsChart?.remove();
    bpsChart = null;
    chart?.remove();
    chart = null;
    lineSeries.clear();
    lastLineWindow.clear();
    lastCandleWindow = null;
    candleSeries = null;
    volumeSeries = null;
    bpsLineSeries = null;
    volMarginsOn = null;
    fitKey = '';
    followLive = true;
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
        scaleMargins: { top: 0.08, bottom: showVolume ? 0.28 : 0.08 },
        entireTextOnly: true,
      },
    });
    chartInteractionDisposer = observeUserRange(chart);
    hostPointerDisposer = wireHostPanGestures(host);
    followLive = true;
    ready = true;
  });

  $effect(() => {
    if (!ready || chartMode !== 'lines' || !showBpsPane) {
      if (bpsChart) {
        unwireSync();
        bpsInteractionDisposer?.();
        bpsInteractionDisposer = null;
        bpsChart.remove();
        bpsChart = null;
        bpsLineSeries = null;
      }
      return;
    }
    if (!bpsHost || bpsChart) return;
    bpsChart = createChart(bpsHost, {
      ...chartOpts,
      autoSize: true,
      rightPriceScale: {
        borderColor: '#1e2329',
        scaleMargins: { top: 0.1, bottom: 0.1 },
      },
    });
    bpsLineSeries = bpsChart.addSeries(LineSeries, {
      color: '#f6465d',
      lineWidth: 1,
      priceFormat: { type: 'custom', formatter: (v) => v.toFixed(2) + ' bps', minMove: 0.01 },
    });
    syncDisposers = [
      wireVisibleLogicalRangeSync(chart, bpsChart, rangeActivity.syncGuard),
      wireVisibleLogicalRangeSync(bpsChart, chart, rangeActivity.syncGuard),
    ];
    bpsInteractionDisposer = observeUserRange(bpsChart);
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
        chart?.applyOptions({ rightPriceScale: { scaleMargins: { top: 0.08, bottom: 0.08 } } });
        volMarginsOn = false;
      }
      return;
    }
    ensureVolumeSeries();
    if (volMarginsOn !== true) {
      chart.applyOptions({ rightPriceScale: { scaleMargins: { top: 0.08, bottom: 0.28 } } });
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
      if (mapped.length) {
        lastCandleWindow = writeSeriesData(candleSeries, mapped, lastCandleWindow);
      } else {
        candleSeries.setData([]);
        lastCandleWindow = null;
      }
      applyVolumeData(volBarsIn);
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
      applyVolumeData(bars);
      setHighlight(primary, hl);

      if (bpsLineSeries && bpsChart) {
        const bpsData = (bps || []).map((p) => ({ time: p.time, value: p.bps }));
        if (!bpsData.length) {
          bpsLineSeries.setData([]);
          lastBpsTip = null;
          lastBpsFullAt = performance.now();
        } else {
          const tip = bpsData[bpsData.length - 1];
          const now = performance.now();
          if (
            lastBpsTip != null &&
            tip.time >= lastBpsTip &&
            now - lastBpsFullAt < FULL_SET_MIN_MS
          ) {
            try {
              bpsLineSeries.update(tip);
              lastBpsTip = tip.time;
            } catch {
              bpsLineSeries.setData(bpsData);
              lastBpsTip = tip.time;
              lastBpsFullAt = now;
            }
          } else {
            bpsLineSeries.setData(bpsData);
            lastBpsTip = tip.time;
            lastBpsFullAt = now;
          }
        }
      }
    }

    if (needRefit) {
      fitKey = key;
      followLive = true;
      markProgrammatic(250);
      requestAnimationFrame(() => {
        markProgrammatic(250);
        rangeActivity.runProgrammatic(() => {
          // Prefer scrollToRealTime over fitContent — fitContent zooms to full
          // history and fights the live-edge pin as points keep arriving.
          chart?.timeScale().scrollToRealTime();
          bpsChart?.timeScale().scrollToRealTime();
        });
      });
    } else if (followLive) {
      // Keep newest bar at the right edge; throttle to cut jank.
      const now = performance.now();
      if (now - lastScrollAt > 250) {
        lastScrollAt = now;
        markProgrammatic(120);
        rangeActivity.runProgrammatic(() => {
          chart.timeScale().scrollToRealTime();
          bpsChart?.timeScale().scrollToRealTime();
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
      globalThis.__mfChartDebug = {
        mode,
        followLive,
        dataLast,
        logical,
        wallSec: Math.floor(Date.now() / 1000),
        gapSec:
          dataLast != null ? Math.floor(Date.now() / 1000) - dataLast : null,
      };
    } catch {
      /* ignore */
    }
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
          title="Pin time scale to live edge (scrollToRealTime)"
        >Live</button>
        <button type="button" class:active={showSettings} onclick={() => (showSettings = !showSettings)}>⚙</button>
      </div>
      {#if discrepancy}
        <span class="disc" class:alert={discrepancy.bps != null && discrepancy.bps > alertBpsThreshold}>
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
      <label>Book depth <input type="number" min="5" max="50" value={bookDepth} onchange={(e) => onBookDepth(Number(e.currentTarget.value))} /></label>
      <label>Tape limit <input type="number" min="20" max="500" value={tapeLimit} onchange={(e) => onTapeLimit(Number(e.currentTarget.value))} /></label>
      <label>Focus poll ms <input type="number" min="80" max="2000" step="20" value={pollFocusMs} onchange={(e) => onPollFocus(Number(e.currentTarget.value))} /></label>
      <label>Multi poll ms <input type="number" min="100" max="5000" step="20" value={pollMultiMs} onchange={(e) => onPollMulti(Number(e.currentTarget.value))} /></label>
      <label>Alert bps <input type="number" min="1" max="500" value={alertBpsThreshold} onchange={(e) => onAlertBps(Number(e.currentTarget.value))} /></label>
      <label>Webhook <input type="url" placeholder="https://…" value={webhookUrl} onchange={(e) => onWebhook(e.currentTarget.value)} /></label>
      <span class="hint">Legend: USD vol · trades/min · raw qty in tooltip · Telegram skipped</span>
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
            title="USD {fmtUsd(row.tradeNotional ?? 0)} · raw qty {row.tradeVolume ?? 0}"
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
      <div class="chart-host" bind:this={host}></div>
      {#if chartMode === 'lines' && showBpsPane}
        <div class="bps-host" bind:this={bpsHost}></div>
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
    height: 100%;
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
  .settings .hint { font-size: 0.62rem; color: var(--muted); font-family: var(--mono); }

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
  .bps-host { height: 64px; flex-shrink: 0; border-top: 1px solid var(--border); }

  .overlay {
    position: absolute; inset: 0; display: grid; place-items: center;
    color: var(--muted); font-family: var(--mono); font-size: 0.75rem;
    pointer-events: none; background: rgba(18, 22, 28, 0.35);
  }
</style>
