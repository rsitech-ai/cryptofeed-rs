<script>
  import { onMount } from 'svelte';
  import {
    createChart,
    LineSeries,
    HistogramSeries,
    ColorType,
    LineStyle,
  } from 'lightweight-charts';
  import { computeCvd } from '../lib/orderflow.js';
  import { CHART_DISPLAY_MAX_POINTS, downsampleForChart } from '../lib/history.js';
  import {
    applyVisibleTimeRange,
    createRangeActivity,
    wireChartTimeScales,
  } from '../lib/chartSync.js';
  import { fmtUsd } from '../lib/format.js';

  let {
    tape = [],
    imbalanceHistory = [],
    pulseHistory = [],
    windowSec = 300,
    /** @type {{ fromSec: number, toSec: number }|null} */
    visibleRange = null,
    spikeThreshold = 72,
    showVolumeHist = true,
    /**
     * Main Lightweight Charts instance (Lines/Candles). Null in Order Flow
     * mode — panes then follow `visibleRange` / `windowSec`.
     * @type {any}
     */
    mainChart = null,
  } = $props();

  let pulseHost = $state(null);
  let imbHost = $state(null);
  let cvdHost = $state(null);
  let volHost = $state(null);
  let ready = $state(false);

  /** @type {any} */
  let pulseChart = null;
  /** @type {any} */
  let imbChart = null;
  /** @type {any} */
  let cvdChart = null;
  /** @type {any} */
  let volChart = null;
  /** @type {any} */
  let pulseSeries = null;
  /** @type {any} */
  let pulseThresholdLine = null;
  /** @type {any} */
  let imbSeries = null;
  /** @type {any} */
  let cvdSeries = null;
  /** @type {any} */
  let buySeries = null;
  /** @type {any} */
  let sellSeries = null;

  const rangeActivity = createRangeActivity();
  /** @type {(() => void)|null} */
  let syncDispose = null;
  let paintRaf = 0;
  let pending = false;
  const FULL_SET_MIN_MS = 12000;
  /** @type {{ first: number, last: number, fullAt: number }|null} */
  let lastPulseWin = null;
  /** @type {{ first: number, last: number, fullAt: number }|null} */
  let lastImbWin = null;
  /** @type {{ first: number, last: number, fullAt: number }|null} */
  let lastCvdWin = null;
  let lastVolFullAt = 0;
  let lastAppliedRangeKey = '';

  const paneOpts = {
    layout: {
      background: { type: ColorType.Solid, color: '#12161c' },
      textColor: '#848e9c',
      fontSize: 10,
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
    rightPriceScale: {
      borderColor: '#1e2329',
      scaleMargins: { top: 0.12, bottom: 0.12 },
      entireTextOnly: true,
    },
    timeScale: {
      borderColor: '#1e2329',
      timeVisible: true,
      secondsVisible: true,
      rightOffset: 2,
      barSpacing: 5,
    },
    handleScroll: { mouseWheel: true, pressedMouseMove: true },
    handleScale: { axisPressedMouseMove: true, mouseWheel: true, pinch: true },
  };

  function rangeBounds() {
    if (
      visibleRange &&
      Number.isFinite(visibleRange.fromSec) &&
      Number.isFinite(visibleRange.toSec) &&
      visibleRange.toSec > visibleRange.fromSec
    ) {
      return { fromSec: visibleRange.fromSec, toSec: visibleRange.toSec };
    }
    const toSec = Math.floor(Date.now() / 1000);
    return { fromSec: toSec - Math.max(1, Number(windowSec) || 300), toSec };
  }

  /**
   * Series retention window — wide enough that pan/zoom still has bars under
   * the viewport. Visible window is applied separately via time-scale sync.
   */
  function dataBounds() {
    const vis = rangeBounds();
    const span = Math.max(1, Number(windowSec) || 300, vis.toSec - vis.fromSec);
    return { fromSec: vis.toSec - span, toSec: vis.toSec };
  }

  /**
   * Incremental write matching PriceChart #20 discipline.
   * @param {any} seriesApi
   * @param {Array<{time:number,value:number}>} data
   * @param {{ first: number, last: number, fullAt: number }|null|undefined} prev
   */
  function writeSeriesData(seriesApi, data, prev) {
    if (!seriesApi) return null;
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
        /* fall through */
      }
    }
    seriesApi.setData(data);
    return { first, last: last.time, fullAt: now };
  }

  /**
   * @param {Array<{t:number,v:number}>} pts  `t` in unix seconds
   * @param {number} fromSec
   * @param {number} toSec
   */
  function toLineData(pts, fromSec, toSec) {
    const win = Math.max(1, toSec - fromSec);
    /** @type {Array<{time:number,value:number}>} */
    const raw = [];
    let prevT = -Infinity;
    for (const p of pts || []) {
      if (!p || !Number.isFinite(p.t) || !Number.isFinite(p.v)) continue;
      const t = Math.floor(p.t);
      if (t < fromSec - 2 || t > toSec + 2) continue;
      if (t === prevT) {
        raw[raw.length - 1] = { time: t, value: p.v };
      } else if (t > prevT) {
        raw.push({ time: t, value: p.v });
        prevT = t;
      }
    }
    return downsampleForChart(raw, win, CHART_DISPLAY_MAX_POINTS);
  }

  function childCharts() {
    return [pulseChart, imbChart, cvdChart, showVolumeHist ? volChart : null].filter(Boolean);
  }

  function unwireSync() {
    syncDispose?.();
    syncDispose = null;
  }

  function rewireSync() {
    unwireSync();
    const kids = childCharts();
    if (!kids.length) return;
    if (mainChart) {
      // Time-range sync: Pulse/Imb/CVD density ≠ price bars. Bidirectional so
      // pan/zoom on any under-pane (or main) stays locked.
      syncDispose = wireChartTimeScales(mainChart, kids, rangeActivity.syncGuard, {
        mode: 'time',
        bidirectional: true,
      });
      try {
        const r = mainChart.timeScale().getVisibleRange?.();
        if (r) {
          applyVisibleTimeRange(
            kids,
            { fromSec: Number(r.from), toSec: Number(r.to) },
            rangeActivity.syncGuard,
          );
        }
      } catch {
        /* ignore */
      }
    }
  }

  function destroyCharts() {
    if (paintRaf) cancelAnimationFrame(paintRaf);
    paintRaf = 0;
    pending = false;
    unwireSync();
    pulseThresholdLine = null;
    pulseSeries = null;
    imbSeries = null;
    cvdSeries = null;
    buySeries = null;
    sellSeries = null;
    pulseChart?.remove();
    imbChart?.remove();
    cvdChart?.remove();
    volChart?.remove();
    pulseChart = imbChart = cvdChart = volChart = null;
    lastPulseWin = lastImbWin = lastCvdWin = null;
    lastVolFullAt = 0;
    lastAppliedRangeKey = '';
    ready = false;
  }

  function createPane(host, { timeVisible }) {
    return createChart(host, {
      ...paneOpts,
      autoSize: true,
      timeScale: {
        ...paneOpts.timeScale,
        visible: timeVisible,
        timeVisible,
        secondsVisible: timeVisible,
        borderVisible: true,
      },
    });
  }

  onMount(() => {
    return () => destroyCharts();
  });

  $effect(() => {
    if (!pulseHost || !imbHost || !cvdHost) return;
    if (showVolumeHist && !volHost) return;
    if (pulseChart) return;

    pulseChart = createPane(pulseHost, { timeVisible: false });
    pulseSeries = pulseChart.addSeries(LineSeries, {
      color: '#f0b90b',
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: true,
      priceFormat: { type: 'custom', formatter: (v) => Number(v).toFixed(0), minMove: 1 },
    });
    pulseThresholdLine = pulseSeries.createPriceLine({
      price: spikeThreshold,
      color: 'rgba(246,70,93,0.55)',
      lineWidth: 1,
      lineStyle: LineStyle.Dashed,
      axisLabelVisible: false,
      title: '',
    });

    imbChart = createPane(imbHost, { timeVisible: false });
    imbSeries = imbChart.addSeries(LineSeries, {
      color: '#3861fb',
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: true,
      priceFormat: { type: 'custom', formatter: (v) => `${Number(v).toFixed(1)}%`, minMove: 0.1 },
    });

    cvdChart = createPane(cvdHost, { timeVisible: !showVolumeHist });
    cvdSeries = cvdChart.addSeries(LineSeries, {
      color: '#02c076',
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: true,
      priceFormat: {
        type: 'custom',
        formatter: (v) => fmtUsd(v),
        minMove: 1,
      },
    });

    if (showVolumeHist && volHost) {
      volChart = createPane(volHost, { timeVisible: true });
      buySeries = volChart.addSeries(HistogramSeries, {
        color: 'rgba(2,192,118,0.75)',
        priceFormat: { type: 'custom', formatter: (v) => fmtUsd(Math.abs(v)), minMove: 1 },
        lastValueVisible: false,
        priceLineVisible: false,
      });
      sellSeries = volChart.addSeries(HistogramSeries, {
        color: 'rgba(246,70,93,0.75)',
        priceFormat: { type: 'custom', formatter: (v) => fmtUsd(Math.abs(v)), minMove: 1 },
        lastValueVisible: false,
        priceLineVisible: false,
      });
    }

    ready = true;
    rewireSync();
    schedulePaint();
  });

  $effect(() => {
    mainChart;
    if (!ready) return;
    rewireSync();
  });

  $effect(() => {
    spikeThreshold;
    if (pulseThresholdLine) {
      try {
        pulseThresholdLine.applyOptions({ price: spikeThreshold });
      } catch {
        /* ignore */
      }
    }
  });

  function schedulePaint() {
    pending = true;
    if (paintRaf) return;
    paintRaf = requestAnimationFrame(() => {
      paintRaf = 0;
      if (!pending) return;
      pending = false;
      paint();
    });
  }

  function paint() {
    if (!ready || !pulseSeries || !imbSeries || !cvdSeries) return;
    const range = dataBounds();
    const vis = rangeBounds();
    const win = Math.max(1, range.toSec - range.fromSec);

    const pulsePts = (pulseHistory || [])
      .filter((p) => p && Number.isFinite(p.t) && Number.isFinite(p.score))
      .map((p) => ({ t: p.t / 1000, v: Number(p.score) }));
    const imbPts = (imbalanceHistory || [])
      .filter((p) => p && Number.isFinite(p.t) && Number.isFinite(p.imbalancePct))
      .map((p) => ({ t: p.t / 1000, v: Number(p.imbalancePct) }));

    const cvd = computeCvd(tape, { windowSec: win, nowSec: range.toSec });
    const cvdPts = (cvd.points || []).map((p) => ({ t: p.sec, v: p.cvd }));

    lastPulseWin = writeSeriesData(pulseSeries, toLineData(pulsePts, range.fromSec, range.toSec), lastPulseWin);
    lastImbWin = writeSeriesData(imbSeries, toLineData(imbPts, range.fromSec, range.toSec), lastImbWin);

    const cvdData = toLineData(cvdPts, range.fromSec, range.toSec);
    lastCvdWin = writeSeriesData(cvdSeries, cvdData, lastCvdWin);
    if (cvdSeries && cvdData.length) {
      const tip = cvdData[cvdData.length - 1].value;
      try {
        cvdSeries.applyOptions({ color: tip >= 0 ? '#02c076' : '#f6465d' });
      } catch {
        /* ignore */
      }
    }

    if (showVolumeHist && buySeries && sellSeries) {
      const hist = (cvd.histogram || []).filter(
        (h) => h.sec >= range.fromSec - 2 && h.sec <= range.toSec + 2,
      );
      const buyRaw = hist.map((h) => ({ time: h.sec, value: h.buyUsd || 0, color: 'rgba(2,192,118,0.75)' }));
      const sellRaw = hist.map((h) => ({
        time: h.sec,
        value: -(h.sellUsd || 0),
        color: 'rgba(246,70,93,0.75)',
      }));
      const buyData = downsampleForChart(buyRaw, win, CHART_DISPLAY_MAX_POINTS);
      const sellData = downsampleForChart(sellRaw, win, CHART_DISPLAY_MAX_POINTS);
      const now = performance.now();
      if (now - lastVolFullAt > FULL_SET_MIN_MS || !buyData.length) {
        buySeries.setData(buyData);
        sellSeries.setData(sellData);
        lastVolFullAt = now;
      } else {
        try {
          if (buyData.length) buySeries.update(buyData[buyData.length - 1]);
          if (sellData.length) sellSeries.update(sellData[sellData.length - 1]);
        } catch {
          buySeries.setData(buyData);
          sellSeries.setData(sellData);
          lastVolFullAt = now;
        }
      }
    }

    // OF / no-main: lock panes to the session/ofView window.
    if (!mainChart) {
      const key = `${vis.fromSec}:${vis.toSec}`;
      if (key !== lastAppliedRangeKey) {
        lastAppliedRangeKey = key;
        applyVisibleTimeRange(childCharts(), vis, rangeActivity.syncGuard);
      }
    } else {
      lastAppliedRangeKey = '';
    }
  }

  $effect(() => {
    tape;
    imbalanceHistory;
    pulseHistory;
    windowSec;
    visibleRange;
    showVolumeHist;
    if (!ready) return;
    schedulePaint();
  });
</script>

<section class="cas" aria-label="Chart flow and pulse analytics">
  <div class="pane pulse">
    <span class="pane-lbl">Pulse</span>
    <div class="pane-host" bind:this={pulseHost}></div>
  </div>
  <div class="pane imb">
    <span class="pane-lbl">Imb</span>
    <div class="pane-host" bind:this={imbHost}></div>
  </div>
  <div class="pane cvd">
    <span class="pane-lbl">CVD</span>
    <div class="pane-host" bind:this={cvdHost}></div>
  </div>
  {#if showVolumeHist}
    <div class="pane vol">
      <span class="pane-lbl">Buy / Sell</span>
      <div class="pane-host" bind:this={volHost}></div>
    </div>
  {/if}
</section>

<style>
  .cas {
    flex-shrink: 0;
    border-top: 1px solid var(--border);
    background: var(--panel);
    display: flex;
    flex-direction: column;
    min-height: 0;
  }

  .pane {
    position: relative;
    flex-shrink: 0;
    height: 52px;
    border-top: 1px solid var(--border);
    background: var(--panel);
  }

  .pane:first-child {
    border-top: none;
  }

  .pane.vol {
    height: 64px;
  }

  .pane-lbl {
    position: absolute;
    top: 3px;
    left: 6px;
    z-index: 2;
    font-size: 0.55rem;
    font-family: var(--mono);
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.04em;
    pointer-events: none;
    background: rgba(18, 22, 28, 0.72);
    padding: 0 3px;
  }

  .pane-host {
    width: 100%;
    height: 100%;
  }
</style>
