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
  import { stepHoldSeries, tapeTipSec } from '../lib/indicatorSeries.js';
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
    /** Emit [{id, chart, series}] for multi-pane crosshair sync. */
    onCrosshairHandles = () => {},
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

  // Panes are slaves of the main chart: no independent pan/zoom.
  const paneOpts = {
    layout: {
      background: { type: ColorType.Solid, color: '#12161c' },
      textColor: '#848e9c',
      fontSize: 10,
      fontFamily: 'IBM Plex Mono, SF Mono, Menlo, Consolas, monospace',
      attributionLogo: false,
    },
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
    rightPriceScale: {
      borderColor: '#1e2329',
      scaleMargins: { top: 0.12, bottom: 0.12 },
      entireTextOnly: true,
      // Match main/BPS so stacked plot areas share the same left edge.
      minimumWidth: 72,
    },
    timeScale: {
      borderColor: '#1e2329',
      timeVisible: true,
      secondsVisible: true,
      rightOffset: 2,
      barSpacing: 5,
    },
    handleScroll: false,
    handleScale: false,
  };

  function tipSec() {
    const fromTape = tapeTipSec(tape);
    if (fromTape != null) return fromTape;
    if (
      visibleRange &&
      Number.isFinite(visibleRange.toSec)
    ) {
      return Math.floor(visibleRange.toSec);
    }
    return Math.floor(Date.now() / 1000);
  }

  function rangeBounds() {
    if (
      visibleRange &&
      Number.isFinite(visibleRange.fromSec) &&
      Number.isFinite(visibleRange.toSec) &&
      visibleRange.toSec > visibleRange.fromSec
    ) {
      return { fromSec: visibleRange.fromSec, toSec: visibleRange.toSec };
    }
    const toSec = tipSec();
    return { fromSec: toSec - Math.max(1, Number(windowSec) || 300), toSec };
  }

  /**
   * Live-anchored retention on the exchange tip clock so pan/zoom still has
   * bars. Visible window is applied separately via one-way time-scale sync.
   */
  function dataBounds() {
    const end = tipSec();
    const vis = rangeBounds();
    const span = Math.max(
      1,
      Number(windowSec) || 300,
      vis.toSec - vis.fromSec,
      end - vis.fromSec,
    );
    return { fromSec: end - span, toSec: end };
  }

  /**
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
    if (tipAdvanced && (leftStable || recentFull) && data.length === 1) {
      try {
        seriesApi.update(last);
        return { first: prev.first, last: last.time, fullAt: prev.fullAt };
      } catch {
        /* fall through */
      }
    }
    // Step-hold grids change left edge often — full setData keeps LWC happy.
    seriesApi.setData(data);
    return { first, last: last.time, fullAt: now };
  }

  function childCharts() {
    return [pulseChart, imbChart, cvdChart, showVolumeHist ? volChart : null].filter(Boolean);
  }

  function mainVisibleSec() {
    if (!mainChart) return null;
    try {
      const r = mainChart.timeScale().getVisibleRange?.();
      if (!r) return null;
      const fromSec = Number(r.from);
      const toSec = Number(r.to);
      if (!Number.isFinite(fromSec) || !Number.isFinite(toSec) || toSec <= fromSec) return null;
      return { fromSec, toSec };
    } catch {
      return null;
    }
  }

  /** One-way: force every under-pane onto the main (or OF) visible window. */
  function lockPanesToMain() {
    const kids = childCharts();
    if (!kids.length) return;
    const fromMain = mainVisibleSec();
    if (fromMain) {
      applyVisibleTimeRange(kids, fromMain, rangeActivity.syncGuard);
      return;
    }
    const vis = rangeBounds();
    applyVisibleTimeRange(kids, vis, rangeActivity.syncGuard);
  }

  function publishPaneDebug() {
    try {
      const read = (c) => {
        if (!c) return null;
        const ts = c.timeScale();
        let visible = null;
        let logical = null;
        try {
          const r = ts.getVisibleRange?.();
          if (r) {
            const from = Number(r.from);
            const to = Number(r.to);
            if (Number.isFinite(from) && Number.isFinite(to) && to > from) {
              visible = { from, to };
            }
          }
        } catch {
          /* ignore */
        }
        try {
          const r = ts.getVisibleLogicalRange?.();
          if (r) logical = { from: Number(r.from), to: Number(r.to) };
        } catch {
          /* ignore */
        }
        return { visible, logical };
      };
      globalThis.__mfPaneDebug = {
        main: read(mainChart),
        pulse: read(pulseChart),
        imb: read(imbChart),
        cvd: read(cvdChart),
        vol: read(volChart),
        tipSec: tipSec(),
        wallSec: Math.floor(Date.now() / 1000),
      };
    } catch {
      /* ignore */
    }
  }

  function unwireSync() {
    syncDispose?.();
    syncDispose = null;
  }

  function paneCrosshairHandles() {
    /** @type {Array<{ id: string, chart: any, series: any }>} */
    const out = [];
    if (pulseChart && pulseSeries) out.push({ id: 'pulse', chart: pulseChart, series: pulseSeries });
    if (imbChart && imbSeries) out.push({ id: 'imb', chart: imbChart, series: imbSeries });
    if (cvdChart && cvdSeries) out.push({ id: 'cvd', chart: cvdChart, series: cvdSeries });
    if (volChart && buySeries) out.push({ id: 'vol', chart: volChart, series: buySeries });
    return out;
  }

  function publishCrosshairHandles() {
    try {
      onCrosshairHandles(paneCrosshairHandles());
    } catch {
      /* ignore */
    }
  }

  function rewireSync() {
    unwireSync();
    const kids = childCharts();
    if (!kids.length) return;
    if (mainChart) {
      // ONE-WAY main → panes. Bidirectional let sparse Pulse/Imb zoom the
      // main Lines chart down to a 1s window (user-visible desync).
      const baseDispose = wireChartTimeScales(mainChart, kids, rangeActivity.syncGuard, {
        mode: 'time',
        bidirectional: false,
      });
      const onMainRange = () => {
        lockPanesToMain();
        publishPaneDebug();
      };
      mainChart.timeScale().subscribeVisibleTimeRangeChange(onMainRange);
      syncDispose = () => {
        try {
          mainChart.timeScale().unsubscribeVisibleTimeRangeChange(onMainRange);
        } catch {
          /* ignore */
        }
        baseDispose();
      };
      lockPanesToMain();
      publishPaneDebug();
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
    try {
      onCrosshairHandles([]);
    } catch {
      /* ignore */
    }
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
    publishCrosshairHandles();
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

    // Pulse / Imb: wall/exchange ms → seconds, then step-hold onto 1s grid so
    // setVisibleRange(main) always has covering bars (no 1s-only panes).
    const pulsePts = (pulseHistory || [])
      .filter((p) => p && Number.isFinite(p.t) && Number.isFinite(p.score))
      .map((p) => ({ t: p.t / 1000, v: Number(p.score) }));
    const imbPts = (imbalanceHistory || [])
      .filter((p) => p && Number.isFinite(p.t) && Number.isFinite(p.imbalancePct))
      .map((p) => ({ t: p.t / 1000, v: Number(p.imbalancePct) }));

    const pulseData = downsampleForChart(
      stepHoldSeries(pulsePts, range.fromSec, range.toSec),
      win,
      CHART_DISPLAY_MAX_POINTS,
    );
    const imbData = downsampleForChart(
      stepHoldSeries(imbPts, range.fromSec, range.toSec),
      win,
      CHART_DISPLAY_MAX_POINTS,
    );

    const cvd = computeCvd(tape, { windowSec: win, nowSec: range.toSec });
    const cvdPts = (cvd.points || []).map((p) => ({ t: p.sec, v: p.cvd }));
    // CVD is already ~1s exchange buckets; step-hold fills gaps for lockstep.
    const cvdData = downsampleForChart(
      stepHoldSeries(cvdPts, range.fromSec, range.toSec),
      win,
      CHART_DISPLAY_MAX_POINTS,
    );

    lastPulseWin = writeSeriesData(pulseSeries, pulseData, lastPulseWin);
    lastImbWin = writeSeriesData(imbSeries, imbData, lastImbWin);
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
      // Histograms: zero-fill missing seconds so vol pane covers main window.
      const buyByT = new Map(buyRaw.map((b) => [b.time, b]));
      const sellByT = new Map(sellRaw.map((b) => [b.time, b]));
      /** @type {Array<{time:number,value:number,color:string}>} */
      const buyFilled = [];
      /** @type {Array<{time:number,value:number,color:string}>} */
      const sellFilled = [];
      for (let t = Math.floor(range.fromSec); t <= Math.floor(range.toSec); t += 1) {
        buyFilled.push(
          buyByT.get(t) || { time: t, value: 0, color: 'rgba(2,192,118,0.75)' },
        );
        sellFilled.push(
          sellByT.get(t) || { time: t, value: 0, color: 'rgba(246,70,93,0.75)' },
        );
      }
      const buyData = downsampleForChart(buyFilled, win, CHART_DISPLAY_MAX_POINTS);
      const sellData = downsampleForChart(sellFilled, win, CHART_DISPLAY_MAX_POINTS);
      buySeries.setData(buyData);
      sellSeries.setData(sellData);
      lastVolFullAt = performance.now();
    }

    // Always re-lock after setData — LWC resets visible range on full writes.
    if (mainChart) {
      lockPanesToMain();
      lastAppliedRangeKey = '';
    } else {
      const key = `${vis.fromSec}:${vis.toSec}`;
      if (key !== lastAppliedRangeKey) {
        lastAppliedRangeKey = key;
        applyVisibleTimeRange(childCharts(), vis, rangeActivity.syncGuard);
      }
    }

    publishPaneDebug();
    try {
      if (globalThis.__mfPaneDebug) {
        globalThis.__mfPaneDebug.counts = {
          pulsePts: pulsePts.length,
          imbPts: imbPts.length,
          cvdPts: cvdPts.length,
          pulseBars: pulseData.length,
          imbBars: imbData.length,
          cvdBars: cvdData.length,
        };
        globalThis.__mfPaneDebug.dataBounds = range;
        globalThis.__mfPaneDebug.visBounds = vis;
      }
    } catch {
      /* ignore */
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
    flex: 1 1 160px;
    min-height: 112px;
    overflow: hidden;
    border-top: 1px solid var(--border);
    background: var(--panel);
    display: flex;
    flex-direction: column;
    min-height: 0;
  }

  .pane {
    position: relative;
    flex: 1 1 52px;
    min-height: 24px;
    border-top: 1px solid var(--border);
    background: var(--panel);
  }

  .pane:first-child {
    border-top: none;
  }

  .pane.vol {
    flex-basis: 64px;
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
