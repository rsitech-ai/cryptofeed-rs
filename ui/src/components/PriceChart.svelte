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
  import { fmtPrice, fmtPct, fmtQty, fmtCount } from '../lib/format.js';

  let {
    /** @type {Array<{venue:string,symbol:string,color:string,live:boolean,hidden?:boolean,data:Array<{time:number,value:number}>,last:number|null,pct:number|null,tradeVolume?:number,tradeCount?:number,volumeData?:Array}>} */
    series = [],
    candles = [],
    volumeBars = [],
    chartMode = 'lines', // 'lines' | 'candles'
    priceMode = 'percent', // 'percent' | 'absolute'
    timeframe = '1s',
    asset = 'BTC',
    discrepancy = null,
    assets = [],
    /** @type {Array<{asset:string,total:number,live:number,venues:string[]}>} */
    coverage = [],
    showVolume = true,
    bookDepth = 16,
    tapeLimit = 120,
    pollFocusMs = 120,
    pollMultiMs = 220,
    focusVenue = '',
    highlightSec = null,
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
  } = $props();

  let host = $state(null);
  let ready = $state(false);
  let chart = null;
  /** @type {Map<string, any>} */
  let lineSeries = new Map();
  let candleSeries = null;
  let volumeSeries = null;
  let markersApi = null;
  let fitKey = '';
  let userInteracted = false;
  let showSettings = $state(false);

  onMount(() => {
    if (!host) return;
    chart = createChart(host, {
      autoSize: true,
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
      rightPriceScale: {
        borderColor: '#1e2329',
        scaleMargins: { top: 0.08, bottom: showVolume ? 0.28 : 0.08 },
        entireTextOnly: true,
      },
      timeScale: {
        borderColor: '#1e2329',
        timeVisible: true,
        secondsVisible: true,
        rightOffset: 4,
        barSpacing: 6,
        fixLeftEdge: false,
        lockVisibleTimeRangeOnResize: true,
      },
      handleScroll: { mouseWheel: true, pressedMouseMove: true },
      handleScale: { axisPressedMouseMove: true, mouseWheel: true, pinch: true },
    });

    chart.timeScale().subscribeVisibleLogicalRangeChange(() => {
      userInteracted = true;
    });

    ready = true;
    return () => {
      ready = false;
      chart?.remove();
      chart = null;
      lineSeries.clear();
      candleSeries = null;
      volumeSeries = null;
      markersApi = null;
    };
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
      priceFormat: { type: 'volume' },
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
      try {
        chart.removeSeries(volumeSeries);
      } catch {
        /* ignore */
      }
      volumeSeries = null;
    }
  }

  function clearLines() {
    for (const s of lineSeries.values()) {
      try {
        chart?.removeSeries(s);
      } catch {
        /* ignore */
      }
    }
    lineSeries.clear();
  }

  function clearCandles() {
    if (candleSeries && chart) {
      try {
        chart.removeSeries(candleSeries);
      } catch {
        /* ignore */
      }
      candleSeries = null;
      markersApi = null;
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
      chart?.applyOptions({
        rightPriceScale: { scaleMargins: { top: 0.08, bottom: 0.08 } },
      });
      return;
    }
    ensureVolumeSeries();
    chart.applyOptions({
      rightPriceScale: { scaleMargins: { top: 0.08, bottom: 0.28 } },
    });
    volumeSeries.setData(bars || []);
  }

  function setHighlight(primarySeries, sec) {
    if (!primarySeries || sec == null || !Number.isFinite(sec)) {
      if (markersApi) {
        try {
          markersApi.setMarkers([]);
        } catch {
          /* ignore */
        }
      }
      return;
    }
    if (!markersApi) {
      try {
        markersApi = createSeriesMarkers(primarySeries, []);
      } catch {
        markersApi = null;
        return;
      }
    }
    markersApi.setMarkers([
      {
        time: sec,
        position: 'aboveBar',
        color: '#f0b90b',
        shape: 'arrowDown',
        text: 'trade',
      },
    ]);
  }

  $effect(() => {
    if (!ready || !chart) return;

    const mode = chartMode;
    const pmode = priceMode;
    const tf = timeframe;
    const a = asset;
    const volOn = showVolume;
    const key = `${mode}|${pmode}|${tf}|${a}|${volOn}`;
    const needRefit = key !== fitKey;
    const hl = highlightSec;

    if (mode === 'candles') {
      clearLines();
      ensureCandleSeries();
      const c = candles;
      if (!c.length) {
        candleSeries.setData([]);
      } else {
        candleSeries.setData(
          c.map((x) => ({
            time: x.time,
            open: x.open,
            high: x.high,
            low: x.low,
            close: x.close,
          })),
        );
      }
      applyVolumeData(volumeBars);
      setHighlight(candleSeries, hl);
    } else {
      clearCandles();
      const rows = series;
      const want = new Set(rows.map((r) => r.venue));

      for (const id of [...lineSeries.keys()]) {
        if (!want.has(id)) {
          try {
            chart.removeSeries(lineSeries.get(id));
          } catch {
            /* ignore */
          }
          lineSeries.delete(id);
        }
      }

      const fmt = priceFormatForMode(pmode);
      let primary = null;
      for (const row of rows) {
        let s = lineSeries.get(row.venue);
        if (!s) {
          s = chart.addSeries(LineSeries, {
            color: row.color,
            lineWidth: row.venue === focusVenue ? 3 : 2,
            priceLineVisible: false,
            lastValueVisible: true,
            crosshairMarkerVisible: true,
            crosshairMarkerRadius: 3,
            priceFormat: fmt,
            autoscaleInfoProvider: (original) => {
              const res = original();
              if (!res?.priceRange) return res;
              const { minValue, maxValue } = res.priceRange;
              const pad = Math.max((maxValue - minValue) * 0.12, pmode === 'percent' ? 0.02 : 0.5);
              return {
                priceRange: {
                  minValue: minValue - pad,
                  maxValue: maxValue + pad,
                },
              };
            },
          });
          lineSeries.set(row.venue, s);
        } else {
          s.applyOptions({
            color: row.color,
            priceFormat: fmt,
            lineWidth: row.venue === focusVenue ? 3 : 2,
            visible: !row.hidden,
          });
        }

        if (row.data?.length) s.setData(row.data);
        else s.setData([]);
        if (!primary && row.data?.length && !row.hidden) primary = s;
        if (row.venue === focusVenue && row.data?.length) primary = s;
      }

      // Aggregate focus volume for lines mode: prefer focus venue volumeData, else sum.
      let bars = volumeBars;
      if (!bars?.length) {
        const focus = rows.find((r) => r.venue === focusVenue);
        bars = focus?.volumeData?.length
          ? focus.volumeData
          : mergeVolume(rows.filter((r) => !r.hidden));
      }
      applyVolumeData(bars);
      setHighlight(primary, hl);
    }

    if (needRefit) {
      fitKey = key;
      userInteracted = false;
      requestAnimationFrame(() => {
        chart?.timeScale().fitContent();
      });
    } else if (!userInteracted) {
      chart.timeScale().scrollToRealTime();
    }
  });

  function mergeVolume(rows) {
    /** @type {Map<number, number>} */
    const m = new Map();
    for (const r of rows) {
      for (const p of r.volumeData || []) {
        m.set(p.time, (m.get(p.time) || 0) + p.value);
      }
    }
    return [...m.entries()]
      .sort((a, b) => a[0] - b[0])
      .map(([time, value]) => ({ time, value, color: 'rgba(240,185,11,0.45)' }));
  }

  function onLegendClick(row, ev) {
    if (ev.shiftKey || ev.metaKey || ev.ctrlKey) {
      onFocusVenue(row.venue, row.symbol);
      return;
    }
    onToggleVenue(row.venue);
  }
</script>

<section class="chart-panel">
  <div class="toolbar">
    <div class="left">
      <div class="assets" title="Select base asset — venues covering each coin">
        {#each coverage.length ? coverage : assets.map((a) => ({ asset: a, total: 0, live: 0 })) as c}
          <button
            type="button"
            class:active={asset === c.asset}
            onclick={() => onAsset(c.asset)}
            title={c.total ? `${c.asset}: ${c.live}/${c.total} venues live` : c.asset}
          >
            {c.asset}
            {#if c.total}
              <span class="cov" class:partial={c.live < c.total} class:ok={c.live === c.total && c.total > 0}>
                {c.live}/{c.total}
              </span>
            {/if}
          </button>
        {/each}
      </div>
      <div class="modes">
        <button type="button" class:active={chartMode === 'lines'} onclick={() => onChartMode('lines')}>Lines</button>
        <button type="button" class:active={chartMode === 'candles'} onclick={() => onChartMode('candles')}>Candles</button>
      </div>
      <div class="modes">
        <button type="button" class:active={priceMode === 'percent'} onclick={() => onPriceMode('percent')}>%</button>
        <button type="button" class:active={priceMode === 'absolute'} onclick={() => onPriceMode('absolute')}>Price</button>
      </div>
      <div class="modes">
        <button type="button" class:active={showVolume} onclick={() => onShowVolume(!showVolume)} title="Toggle volume subplot">Vol</button>
        <button type="button" class:active={showSettings} onclick={() => (showSettings = !showSettings)} title="Panel settings">⚙</button>
      </div>
      {#if discrepancy}
        <span class="disc" title="Max−min across visible venues at latest print">
          Δ {fmtPrice(discrepancy.abs, 2)}
          {#if discrepancy.bps != null}
            <span class="muted">({discrepancy.bps.toFixed(2)} bps)</span>
          {/if}
        </span>
      {/if}
    </div>
    <div class="tfs">
      {#each TIMEFRAMES as tf}
        <button type="button" class:active={timeframe === tf.id} onclick={() => onTimeframe(tf.id)}>
          {tf.label}
        </button>
      {/each}
    </div>
  </div>

  {#if showSettings}
    <div class="settings">
      <label>
        Book depth
        <input type="number" min="5" max="50" value={bookDepth} onchange={(e) => onBookDepth(Number(e.currentTarget.value))} />
      </label>
      <label>
        Tape limit
        <input type="number" min="20" max="500" value={tapeLimit} onchange={(e) => onTapeLimit(Number(e.currentTarget.value))} />
      </label>
      <label>
        Focus poll ms
        <input type="number" min="80" max="2000" step="20" value={pollFocusMs} onchange={(e) => onPollFocus(Number(e.currentTarget.value))} />
      </label>
      <label>
        Multi poll ms
        <input type="number" min="100" max="5000" step="20" value={pollMultiMs} onchange={(e) => onPollMulti(Number(e.currentTarget.value))} />
      </label>
      <span class="hint">Click legend = toggle series · Shift+click = focus book/tape</span>
    </div>
  {/if}

  {#if chartMode === 'lines'}
    <div class="legend">
      {#each series as row}
        <button
          type="button"
          class="leg"
          class:dim={!row.data?.length || row.hidden}
          class:focus={row.venue === focusVenue}
          onclick={(e) => onLegendClick(row, e)}
          title="Click to toggle · Shift+click to focus book/tape"
        >
          <span class="swatch" style={`background:${row.color}`}></span>
          <span class="name">{row.venue}</span>
          <span class="live-dot" class:ok={row.live} class:bad={!row.live}>{row.live ? '●' : '○'}</span>
          <span class="px" style={`color:${row.color}`}>
            {row.last != null ? fmtPrice(row.last, 2) : '—'}
          </span>
          <span class="pct" class:up={row.pct > 0} class:down={row.pct < 0}>
            {row.pct != null ? fmtPct(row.pct, 3) : '—'}
          </span>
          <span class="vol">
            {fmtQty(row.tradeVolume || 0)}/{fmtCount(row.tradeCount || 0)}
          </span>
        </button>
      {/each}
    </div>
  {/if}

  <div class="chart-wrap">
    <div class="chart-host" bind:this={host}></div>
    {#if chartMode === 'lines' && !series.some((s) => s.data?.length && !s.hidden)}
      <div class="overlay">streaming multi-venue prices…</div>
    {:else if chartMode === 'candles' && !candles.length}
      <div class="overlay">accumulating trades for candles…</div>
    {/if}
  </div>
</section>

<style>
  .chart-panel {
    display: flex;
    flex-direction: column;
    min-height: 0;
    height: 100%;
    background: var(--panel);
  }

  .toolbar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.6rem;
    padding: 0.3rem 0.5rem;
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
    flex-wrap: wrap;
  }

  .left {
    display: flex;
    align-items: center;
    gap: 0.55rem;
    min-width: 0;
    flex-wrap: wrap;
  }

  .assets,
  .modes,
  .tfs {
    display: flex;
    gap: 0.12rem;
  }

  .assets button,
  .modes button,
  .tfs button {
    background: transparent;
    border: 1px solid transparent;
    color: var(--muted);
    font-family: var(--mono);
    font-size: 0.7rem;
    padding: 0.15rem 0.4rem;
    cursor: pointer;
    border-radius: 2px;
  }

  .assets button:hover,
  .modes button:hover,
  .tfs button:hover {
    color: var(--text);
    background: var(--panel-2);
  }

  .assets button.active {
    color: #0b0e11;
    background: var(--accent);
    font-weight: 700;
  }

  .assets button .cov {
    margin-left: 0.2rem;
    font-size: 0.55rem;
    opacity: 0.75;
    font-weight: 500;
  }

  .assets button.active .cov {
    opacity: 0.85;
  }

  .assets button .cov.ok {
    opacity: 1;
  }

  .assets button:not(.active) .cov.partial {
    color: var(--ask);
  }

  .assets button:not(.active) .cov.ok {
    color: var(--bid);
  }

  .modes button.active,
  .tfs button.active {
    color: var(--accent);
    border-color: rgba(240, 185, 11, 0.35);
    background: rgba(240, 185, 11, 0.08);
  }

  .disc {
    font-family: var(--mono);
    font-size: 0.7rem;
    color: var(--text-dim);
  }
  .muted {
    color: var(--muted);
  }

  .settings {
    display: flex;
    flex-wrap: wrap;
    gap: 0.75rem 1rem;
    align-items: center;
    padding: 0.35rem 0.55rem;
    border-bottom: 1px solid var(--border);
    background: var(--panel-2);
    flex-shrink: 0;
  }

  .settings label {
    display: flex;
    align-items: center;
    gap: 0.35rem;
    font-size: 0.68rem;
    color: var(--muted);
    font-family: var(--mono);
  }

  .settings input {
    width: 4.5rem;
    background: var(--bg);
    border: 1px solid var(--border);
    color: var(--text);
    padding: 0.15rem 0.3rem;
    border-radius: 2px;
    font-family: var(--mono);
    font-size: 0.7rem;
  }

  .settings .hint {
    font-size: 0.62rem;
    color: var(--muted);
    font-family: var(--mono);
  }

  .legend {
    display: flex;
    flex-wrap: wrap;
    gap: 0.35rem 0.55rem;
    padding: 0.3rem 0.55rem;
    border-bottom: 1px solid var(--border);
    max-height: 5.2rem;
    overflow: auto;
    flex-shrink: 0;
  }

  .leg {
    display: flex;
    align-items: baseline;
    gap: 0.3rem;
    font-family: var(--mono);
    font-size: 0.68rem;
    background: transparent;
    border: 1px solid transparent;
    color: inherit;
    cursor: pointer;
    padding: 0.1rem 0.25rem;
    border-radius: 2px;
  }
  .leg:hover {
    background: var(--panel-2);
    border-color: var(--border);
  }
  .leg.dim {
    opacity: 0.4;
  }
  .leg.focus {
    border-color: rgba(240, 185, 11, 0.4);
    background: rgba(240, 185, 11, 0.08);
  }

  .swatch {
    width: 8px;
    height: 8px;
    border-radius: 1px;
    flex-shrink: 0;
  }

  .name {
    color: var(--muted);
    max-width: 7.5rem;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .live-dot {
    font-size: 0.5rem;
    line-height: 1;
  }
  .live-dot.ok {
    color: var(--bid);
  }
  .live-dot.bad {
    color: var(--ask);
  }

  .px {
    font-weight: 600;
  }

  .pct {
    color: var(--muted);
  }
  .pct.up {
    color: var(--bid);
  }
  .pct.down {
    color: var(--ask);
  }

  .vol {
    color: var(--muted);
    font-size: 0.6rem;
  }

  .chart-wrap {
    position: relative;
    flex: 1;
    min-height: 0;
  }

  .chart-host {
    position: absolute;
    inset: 0;
  }

  .overlay {
    position: absolute;
    inset: 0;
    display: grid;
    place-items: center;
    color: var(--muted);
    font-family: var(--mono);
    font-size: 0.75rem;
    pointer-events: none;
    background: rgba(18, 22, 28, 0.35);
  }
</style>
