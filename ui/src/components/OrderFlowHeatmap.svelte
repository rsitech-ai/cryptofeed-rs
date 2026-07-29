<script>
  import { onMount } from 'svelte';
  import { fmtPrice, fmtUsd } from '../lib/format.js';
  import { createPaintGate, ema } from '../lib/paint.js';
  import {
    buildHeatmapGrid,
    clampPriceZoom,
    clampViewSec,
    cobColumn,
    computeCvd,
    computePriceWindow,
    densifyDepthHistory,
    flowMarkers,
    footprintClusters,
    heatIntensity,
    heatmapBaselineRgba,
    heatmapColor,
    nearestWalls,
    ohlcBucketsFromTape,
    parseOfLayers,
    priceAxisPadPx,
    resolveTick,
    restingAtPrice,
    serializeOfLayers,
    tradeBubbles,
    volumeAtPrice,
    volumeBarsFromTape,
  } from '../lib/orderflow.js';

  let {
    depthHistory = [],
    tape = [],
    windowSec = 300,
    venue = '',
    symbol = '',
    lastPrice = null,
    hasL2 = true,
    ofTick = 'auto',
    ofHeat = 1,
    ofBubbleMinUsd = 50,
    ofLayers = 'heat,bubbles,mid,vap,cvd,vol,cob,candles,markers',
    largeTradeUsd = 15000,
    priceZoom = 1,
    followLive = true,
    onSettings = () => {},
  } = $props();

  let wrap = $state(null);
  let canvas = $state(null);
  let layersOpen = $state(false);
  /** @type {{ x: number, y: number, lines: string[] }|null} */
  let hover = $state(null);

  const TICK_OPTS = [
    { v: 'auto', label: 'auto' },
    { v: '0.1', label: '0.1' },
    { v: '1', label: '1' },
    { v: '5', label: '5' },
    { v: '10', label: '10' },
    { v: '50', label: '50' },
    { v: '100', label: '100' },
  ];

  const LAYER_KEYS = [
    { k: 'heat', label: 'Heat' },
    { k: 'bubbles', label: 'Bubbles' },
    { k: 'mid', label: 'Bid/Ask' },
    { k: 'candles', label: 'Candles' },
    { k: 'footprint', label: 'Footprint' },
    { k: 'markers', label: 'Large/Sweep' },
    { k: 'cob', label: 'COB' },
    { k: 'vap', label: 'Profile' },
    { k: 'cvd', label: 'CVD' },
    { k: 'vol', label: 'Volume' },
  ];

  /** @param {Array<object>} history */
  function latestBookFromHistory(history) {
    const s = history.at(-1);
    if (!s) return null;
    return {
      bids: [...(s.bids?.entries?.() || [])].map(([price, usd]) => ({
        price,
        quantity: usd / (price || 1),
      })),
      asks: [...(s.asks?.entries?.() || [])].map(([price, usd]) => ({
        price,
        quantity: usd / (price || 1),
      })),
    };
  }

  let layers = $derived(parseOfLayers(ofLayers));
  let tick = $derived(resolveTick(ofTick, latestBookFromHistory(depthHistory)));

  let bubbles = $derived(
    tradeBubbles(tape, { windowSec, tick, bucketMs: 350, maxBubbles: 280 }).filter(
      (b) => b.totalUsd >= ofBubbleMinUsd,
    ),
  );
  let volBars = $derived(volumeBarsFromTape(tape, { windowSec, bucketSec: 1 }));
  let cvdData = $derived(computeCvd(tape, { windowSec }));
  let vapRows = $derived(volumeAtPrice(tape, { windowSec, tickSize: tick, maxBuckets: 48 }));
  let candles = $derived(ohlcBucketsFromTape(tape, { windowSec, bucketSec: 5, maxBars: 60 }));
  let clusters = $derived(
    footprintClusters(tape, { windowSec, tick, bucketSec: 15, maxCells: 360 }),
  );
  let markers = $derived(
    flowMarkers(tape, latestBookFromHistory(depthHistory), {
      windowSec,
      largeUsd: largeTradeUsd,
    }),
  );

  /** Stable y-scale (EMA) so walls don't jump every sample. */
  let scaleLo = null;
  let scaleHi = null;
  /** Pan anchor (ms) when not following live. */
  let viewTMax = $state(null);
  /** @type {ReturnType<typeof buildHeatmapGrid>|null} */
  let lastGoodGrid = null;
  /** @type {HTMLCanvasElement|null} */
  let frameBuf = null;
  /** @type {HTMLCanvasElement|null} */
  let heatLayer = null;
  /** @type {ImageData|null} */
  let imageBuf = null;
  let ro = null;
  let lastPaintKey = '';
  let pinchDist0 = 0;
  let zoomPersistTimer = 0;
  let localPriceZoom = $state(/** @type {number|null} */ (null));
  let localViewSec = $state(/** @type {number|null} */ (null));

  const gate = createPaintGate(() => paint(), { minIntervalMs: 110 });

  let zoomFactor = $derived(clampPriceZoom(localPriceZoom ?? priceZoom, 1));
  let viewSec = $derived(clampViewSec(localViewSec ?? windowSec, 300));

  $effect(() => {
    // Keep local overrides in sync when parent/URL pushes new values.
    priceZoom;
    windowSec;
    localPriceZoom = null;
    localViewSec = null;
  });

  function windowLabel(sec) {
    if (sec < 60) return `${sec}s`;
    if (sec < 3600) return `${Math.round(sec / 60)}m`;
    return `${(sec / 3600).toFixed(sec % 3600 === 0 ? 0 : 1)}h`;
  }

  function ensureCanvas(slot, w, h) {
    if (!slot) slot = document.createElement('canvas');
    if (slot.width !== w || slot.height !== h) {
      slot.width = w;
      slot.height = h;
    }
    return slot;
  }

  function ensureImageData(w, h) {
    if (!imageBuf || imageBuf.width !== w || imageBuf.height !== h) {
      heatLayer = ensureCanvas(heatLayer, w, h);
      imageBuf = heatLayer.getContext('2d').createImageData(w, h);
    }
    return imageBuf;
  }

  /**
   * Map price → y within heat band.
   * @param {number} px
   * @param {number} priceMin
   * @param {number} priceMax
   * @param {number} padT
   * @param {number} heatH
   */
  function priceY(px, priceMin, priceMax, padT, heatH) {
    const span = priceMax - priceMin || 1;
    return padT + ((priceMax - px) / span) * heatH;
  }

  /**
   * Map timestamp → x within heat band.
   * @param {number} t
   * @param {number} tMin
   * @param {number} spanT
   * @param {number} padL
   * @param {number} heatW
   */
  function timeX(t, tMin, spanT, padL, heatW) {
    return padL + ((t - tMin) / spanT) * heatW;
  }

  function paintHeatLayer(octx, grid, layout, dpr) {
    const { padL, padT, heatW, heatH, ofHeatGain } = layout;
    const heatWInt = Math.max(1, Math.ceil(heatW));
    const heatHInt = Math.max(1, Math.ceil(heatH));
    const { priceMin, priceMax, tMin, tMax, maxVal } = grid;
    const spanT = Math.max(1, tMax - tMin);
    const cellW = heatW / grid.cols;
    const cellH = heatH / grid.rows;
    const img = ensureImageData(heatWInt, heatHInt);
    const data = img.data;
    const [bR, bG, bB, bA] = heatmapBaselineRgba();

    for (let i = 0; i < data.length; i += 4) {
      data[i] = bR;
      data[i + 1] = bG;
      data[i + 2] = bB;
      data[i + 3] = bA;
    }

    for (let r = 0; r < grid.rows; r++) {
      for (let c = 0; c < grid.cols; c++) {
        const v = grid.grid[r * grid.cols + c];
        if (!(v > 0)) continue;
        const intensity = heatIntensity(v, maxVal, ofHeatGain);
        const [R, G, B, A] = heatmapColor(intensity);
        const x0 = Math.floor(c * cellW);
        const x1 = Math.floor((c + 1) * cellW);
        const y0 = Math.floor(r * cellH);
        const y1 = Math.floor((r + 1) * cellH);
        const a = A / 255;
        for (let y = y0; y < y1 && y < img.height; y++) {
          for (let x = x0; x < x1 && x < img.width; x++) {
            const idx = (y * img.width + x) * 4;
            data[idx] = Math.round(data[idx] * (1 - a) + R * a);
            data[idx + 1] = Math.round(data[idx + 1] * (1 - a) + G * a);
            data[idx + 2] = Math.round(data[idx + 2] * (1 - a) + B * a);
            data[idx + 3] = 255;
          }
        }
      }
    }

    heatLayer = ensureCanvas(heatLayer, heatWInt, heatHInt);
    heatLayer.getContext('2d').putImageData(img, 0, 0);
    octx.drawImage(heatLayer, padL, padT, heatW, heatH);

    // Soft BBO spread ribbon (bidAskPath)
    if (layers.mid && grid.bidAskPath?.length > 1) {
      const spanP = priceMax - priceMin || 1;
      octx.beginPath();
      for (let i = 0; i < grid.bidAskPath.length; i++) {
        const p = grid.bidAskPath[i];
        if (p.bestAsk == null) continue;
        const x = timeX(p.t, tMin, spanT, padL, heatW);
        const y = priceY(p.bestAsk, priceMin, priceMax, padT, heatH);
        if (i === 0) octx.moveTo(x, y);
        else octx.lineTo(x, y);
      }
      for (let i = grid.bidAskPath.length - 1; i >= 0; i--) {
        const p = grid.bidAskPath[i];
        if (p.bestBid == null) continue;
        const x = timeX(p.t, tMin, spanT, padL, heatW);
        const y = priceY(p.bestBid, priceMin, priceMax, padT, heatH);
        octx.lineTo(x, y);
      }
      octx.closePath();
      octx.fillStyle = 'rgba(240,185,11,0.06)';
      octx.fill();

      // Bookmap-style bid (green) / ask (magenta) lines
      octx.lineWidth = 1.35 * dpr;
      for (const side of ['bid', 'ask']) {
        octx.beginPath();
        let started = false;
        for (let i = 0; i < grid.bidAskPath.length; i++) {
          const p = grid.bidAskPath[i];
          const px = side === 'bid' ? p.bestBid : p.bestAsk;
          if (px == null) continue;
          const x = timeX(p.t, tMin, spanT, padL, heatW);
          const y = priceY(px, priceMin, priceMax, padT, heatH);
          if (!started) {
            octx.moveTo(x, y);
            started = true;
          } else octx.lineTo(x, y);
        }
        octx.strokeStyle = side === 'bid' ? 'rgba(2,192,118,0.95)' : 'rgba(200,80,220,0.95)';
        octx.stroke();
      }
    }

    // Soft last/mid guide
    if (layers.mid && grid.midPath.length > 1) {
      octx.beginPath();
      octx.strokeStyle = 'rgba(255,255,255,0.35)';
      octx.lineWidth = 0.9 * dpr;
      for (let i = 0; i < grid.midPath.length; i++) {
        const p = grid.midPath[i];
        const x = timeX(p.t, tMin, spanT, padL, heatW);
        const y = priceY(p.mid, priceMin, priceMax, padT, heatH);
        if (i === 0) octx.moveTo(x, y);
        else octx.lineTo(x, y);
      }
      octx.stroke();
    }
  }

  function paintBubbles(octx, layout, dpr) {
    const { padL, padT, heatW, heatH, priceMin, priceMax, tMin, tMax } = layout;
    const spanT = Math.max(1, tMax - tMin);
    const maxBubbleUsd = Math.max(1, ...bubbles.map((b) => b.totalUsd), ofBubbleMinUsd || 1);

    for (const b of bubbles) {
      if (b.t < tMin || b.t > tMax) continue;
      if (b.price < priceMin || b.price > priceMax) continue;
      const x = timeX(b.t, tMin, spanT, padL, heatW);
      const y = priceY(b.price, priceMin, priceMax, padT, heatH);
      // Log-ish sizing so mid-size prints stay visible next to whales.
      const r = Math.max(
        3.5 * dpr,
        Math.min(26 * dpr, (Math.log1p(b.totalUsd) / Math.log1p(maxBubbleUsd)) * 22 * dpr),
      );
      const buyFrac = b.totalUsd > 0 ? b.buyUsd / b.totalUsd : 0.5;

      octx.beginPath();
      octx.arc(x, y, r, Math.PI, 0, false);
      octx.closePath();
      octx.fillStyle = `rgba(246,70,93,${0.4 + (1 - buyFrac) * 0.45})`;
      octx.fill();

      octx.beginPath();
      octx.arc(x, y, r, 0, Math.PI, false);
      octx.closePath();
      octx.fillStyle = `rgba(2,192,118,${0.4 + buyFrac * 0.45})`;
      octx.fill();

      octx.beginPath();
      octx.moveTo(x - r, y);
      octx.lineTo(x + r, y);
      octx.strokeStyle = 'rgba(12,16,22,0.55)';
      octx.lineWidth = 1;
      octx.stroke();

      octx.beginPath();
      octx.arc(x, y, r, 0, Math.PI * 2);
      octx.strokeStyle = 'rgba(255,255,255,0.28)';
      octx.lineWidth = 1;
      octx.stroke();
    }
  }

  function paintCandles(octx, layout, dpr) {
    const { padL, padT, heatW, heatH, priceMin, priceMax, tMin, tMax } = layout;
    if (!candles.length) return;
    const spanT = Math.max(1, tMax - tMin);
    const bucketMs = Math.max(1000, (candles[1]?.sec - candles[0]?.sec || 5) * 1000);
    const barW = Math.max(3 * dpr, Math.min(14 * dpr, (bucketMs / spanT) * heatW * 0.55));

    for (const c of candles) {
      const tMs = c.sec * 1000;
      if (tMs < tMin || tMs > tMax) continue;
      if (c.h < priceMin || c.l > priceMax) continue;
      const x = timeX(tMs + bucketMs / 2, tMin, spanT, padL, heatW);
      const yO = priceY(c.o, priceMin, priceMax, padT, heatH);
      const yC = priceY(c.c, priceMin, priceMax, padT, heatH);
      const yH = priceY(c.h, priceMin, priceMax, padT, heatH);
      const yL = priceY(c.l, priceMin, priceMax, padT, heatH);
      const up = c.c >= c.o;
      const col = up ? 'rgba(2,192,118,0.55)' : 'rgba(246,70,93,0.55)';
      octx.strokeStyle = col;
      octx.lineWidth = 1 * dpr;
      octx.beginPath();
      octx.moveTo(x, yH);
      octx.lineTo(x, yL);
      octx.stroke();
      const top = Math.min(yO, yC);
      const bodyH = Math.max(1.5 * dpr, Math.abs(yC - yO));
      octx.fillStyle = col;
      octx.fillRect(x - barW / 2, top, barW, bodyH);
    }
  }

  function paintFootprint(octx, layout, dpr) {
    const { padL, padT, heatW, heatH, priceMin, priceMax, tMin, tMax } = layout;
    if (!clusters.length) return;
    const spanT = Math.max(1, tMax - tMin);
    const maxCell = Math.max(1, ...clusters.map((c) => c.totalUsd));
    const bucketMs = 15000;
    const cellW = Math.max(10 * dpr, Math.min(36 * dpr, (bucketMs / spanT) * heatW * 0.7));

    for (const c of clusters) {
      if (c.t < tMin || c.t > tMax) continue;
      if (c.price < priceMin || c.price > priceMax) continue;
      const x = timeX(c.t + bucketMs / 2, tMin, spanT, padL, heatW);
      const y = priceY(c.price, priceMin, priceMax, padT, heatH);
      const w = (c.totalUsd / maxCell) * cellW;
      const h = Math.max(2 * dpr, 3.5 * dpr);
      const bidW = c.totalUsd > 0 ? (c.bidUsd / c.totalUsd) * w : w / 2;
      const askW = w - bidW;
      octx.fillStyle = 'rgba(2,192,118,0.45)';
      octx.fillRect(x - w / 2, y - h / 2, bidW, h);
      octx.fillStyle = 'rgba(246,70,93,0.45)';
      octx.fillRect(x - w / 2 + bidW, y - h / 2, askW, h);
      if (Math.abs(c.delta) / c.totalUsd > 0.55 && c.totalUsd > maxCell * 0.2) {
        octx.strokeStyle = 'rgba(255,255,255,0.35)';
        octx.strokeRect(x - w / 2, y - h / 2, w, h);
      }
    }
  }

  function paintMarkers(octx, layout, dpr) {
    const { padL, padT, heatW, heatH, priceMin, priceMax, tMin, tMax } = layout;
    if (!markers.length) return;
    const spanT = Math.max(1, tMax - tMin);
    for (const m of markers) {
      if (m.price == null || m.ts == null) continue;
      const tMs = m.ts * 1000;
      if (tMs < tMin || tMs > tMax) continue;
      if (m.price < priceMin || m.price > priceMax) continue;
      const x = timeX(tMs, tMin, spanT, padL, heatW);
      const y = priceY(m.price, priceMin, priceMax, padT, heatH);
      const r = Math.max(5 * dpr, Math.min(14 * dpr, Math.log1p(m.usd) * 0.9 * dpr));
      const col =
        m.side === 'buy' ? 'rgba(2,192,118,0.9)' : m.side === 'sell' ? 'rgba(246,70,93,0.9)' : 'rgba(240,185,11,0.9)';
      octx.fillStyle = col;
      octx.strokeStyle = 'rgba(255,255,255,0.55)';
      octx.lineWidth = 1 * dpr;
      octx.beginPath();
      if (m.marker === 'triangle') {
        octx.moveTo(x, y - r);
        octx.lineTo(x + r, y + r);
        octx.lineTo(x - r, y + r);
        octx.closePath();
      } else if (m.marker === 'square') {
        octx.rect(x - r * 0.75, y - r * 0.75, r * 1.5, r * 1.5);
      } else {
        // diamond — large print / liq-style (honest: not exchange liquidation)
        octx.moveTo(x, y - r);
        octx.lineTo(x + r, y);
        octx.lineTo(x, y + r);
        octx.lineTo(x - r, y);
        octx.closePath();
      }
      octx.fill();
      octx.stroke();
    }
  }

  function paintCobColumn(octx, layout, dpr) {
    const { cobX, cobW, padT, heatH, priceMin, priceMax } = layout;
    if (!(cobW > 0)) return;
    const sample = depthHistory.at(-1);
    const cob = cobColumn(sample, { priceMin, priceMax, maxRows: 72 });

    octx.fillStyle = '#090d12';
    octx.fillRect(cobX, padT, cobW, heatH);
    octx.strokeStyle = 'rgba(255,255,255,0.07)';
    octx.strokeRect(cobX + 0.5, padT + 0.5, cobW - 1, heatH - 1);

    octx.font = `${9 * dpr}px IBM Plex Mono, SF Mono, Menlo, monospace`;
    octx.textAlign = 'center';
    octx.textBaseline = 'middle';
    octx.fillStyle = '#5e6673';
    octx.fillText('COB', cobX + cobW / 2, padT + 8 * dpr);

    const barMax = (cobW - 10 * dpr) * 0.9;
    const rowH = Math.max(2 * dpr, heatH / Math.max(cob.rows.length, 12));
    for (const row of cob.rows) {
      const y = priceY(row.price, priceMin, priceMax, padT, heatH);
      if (y < padT + 12 * dpr || y > padT + heatH - 4 * dpr) continue;
      const isAsk = row.askUsd >= row.bidUsd && row.askUsd > 0;
      const usd = Math.max(row.bidUsd, row.askUsd);
      const w = (usd / cob.maxUsd) * barMax;
      // Wall highlight
      const wall = usd >= cob.maxUsd * 0.55;
      octx.fillStyle = isAsk
        ? wall
          ? 'rgba(246,70,93,0.55)'
          : 'rgba(246,70,93,0.28)'
        : wall
          ? 'rgba(2,192,118,0.55)'
          : 'rgba(2,192,118,0.28)';
      octx.fillRect(cobX + 4 * dpr, y - rowH * 0.4, w, rowH * 0.8);
      octx.fillStyle = '#c9cdD3';
      octx.textAlign = 'right';
      octx.font = `${8 * dpr}px IBM Plex Mono, SF Mono, Menlo, monospace`;
      const label = usd >= 1000 ? `${(usd / 1000).toFixed(1)}k` : usd.toFixed(0);
      octx.fillText(label, cobX + cobW - 4 * dpr, y);
    }

    // Current price marker
    if (lastPrice != null && lastPrice >= priceMin && lastPrice <= priceMax) {
      const y = priceY(lastPrice, priceMin, priceMax, padT, heatH);
      octx.fillStyle = 'rgba(240,185,11,0.9)';
      octx.fillRect(cobX + 1, y - 1 * dpr, cobW - 2, 2 * dpr);
    }
  }

  function paintVapSidebar(octx, layout, dpr) {
    const { padT, heatH, vapX, vapW, priceMin, priceMax } = layout;
    if (vapW <= 0) return;

    octx.fillStyle = '#0a0e14';
    octx.fillRect(vapX, padT, vapW, heatH);
    octx.strokeStyle = 'rgba(255,255,255,0.06)';
    octx.lineWidth = 1;
    octx.strokeRect(vapX + 0.5, padT + 0.5, vapW - 1, heatH - 1);

    const midX = vapX + vapW / 2;
    octx.font = `${9 * dpr}px IBM Plex Mono, SF Mono, Menlo, monospace`;
    octx.textAlign = 'center';
    octx.textBaseline = 'middle';
    octx.fillStyle = '#5e6673';
    octx.fillText('VP', midX, padT + 8 * dpr);

    const inWin = vapRows.filter((r) => r.price >= priceMin && r.price <= priceMax);
    // Fallback: book depth profile when tape VAP empty in window.
    const sample = depthHistory.at(-1);
    const bookProfile = [];
    if (!inWin.length && sample) {
      for (const [px, usd] of sample.bids?.entries?.() || []) {
        if (px >= priceMin && px <= priceMax) bookProfile.push({ price: px, buyUsd: usd, sellUsd: 0 });
      }
      for (const [px, usd] of sample.asks?.entries?.() || []) {
        if (px >= priceMin && px <= priceMax) bookProfile.push({ price: px, buyUsd: 0, sellUsd: usd });
      }
    }
    const rows = inWin.length ? inWin : bookProfile;
    if (!rows.length) {
      octx.fillStyle = '#474d57';
      octx.font = `${8 * dpr}px IBM Plex Mono, SF Mono, Menlo, monospace`;
      octx.fillText('—', midX, padT + heatH / 2);
      return;
    }

    const maxUsd = Math.max(
      1,
      ...rows.map((r) => Math.max(r.buyUsd || 0, r.sellUsd || 0, (r.buyUsd || 0) + (r.sellUsd || 0))),
    );
    const barMaxW = (vapW - 8 * dpr) * 0.45;
    const h = Math.max(2 * dpr, Math.min(5 * dpr, heatH / Math.max(rows.length, 8)));

    for (const row of rows) {
      const y = priceY(row.price, priceMin, priceMax, padT, heatH);
      if (y < padT + 12 * dpr || y > padT + heatH - 2 * dpr) continue;
      const buyW = ((row.buyUsd || 0) / maxUsd) * barMaxW;
      const sellW = ((row.sellUsd || 0) / maxUsd) * barMaxW;
      octx.fillStyle = 'rgba(2,192,118,0.6)';
      octx.fillRect(midX + 1, y - h / 2, buyW, h);
      octx.fillStyle = 'rgba(246,70,93,0.6)';
      octx.fillRect(midX - 1 - sellW, y - h / 2, sellW, h);
    }

    octx.font = `${8 * dpr}px IBM Plex Mono, SF Mono, Menlo, monospace`;
    octx.fillStyle = '#474d57';
    octx.fillText(inWin.length ? 'tape' : 'book', midX, padT + heatH - 6 * dpr);
  }

  function paintVolSubplot(octx, layout, dpr) {
    const { padL, heatW, volTop, volH, tMin, tMax } = layout;
    if (volH <= 0) return;

    octx.fillStyle = '#0a0e14';
    octx.fillRect(padL, volTop, heatW, volH);
    octx.strokeStyle = 'rgba(255,255,255,0.05)';
    octx.strokeRect(padL + 0.5, volTop + 0.5, heatW - 1, volH - 1);

    const spanT = Math.max(1, tMax - tMin);
    const inWin = volBars.filter((v) => {
      const tMs = v.sec * 1000;
      return tMs >= tMin && tMs <= tMax;
    });
    const maxVol = Math.max(1, ...inWin.map((v) => v.totalUsd), 1);
    const barW = Math.max(1.5 * dpr, Math.min(10 * dpr, (1000 / spanT) * heatW * 0.75));

    for (const v of inWin) {
      const tMs = v.sec * 1000;
      const x = timeX(tMs, tMin, spanT, padL, heatW);
      const buyH = (v.buyUsd / maxVol) * (volH - 2);
      const sellH = (v.sellUsd / maxVol) * (volH - 2);
      octx.fillStyle = 'rgba(2,192,118,0.7)';
      octx.fillRect(x - barW / 2, volTop + volH - buyH, barW, buyH);
      octx.fillStyle = 'rgba(246,70,93,0.7)';
      octx.fillRect(x - barW / 2, volTop + volH - buyH - sellH, barW, sellH);
    }

    octx.fillStyle = '#474d57';
    octx.font = `${9 * dpr}px IBM Plex Mono, SF Mono, Menlo, monospace`;
    octx.textAlign = 'left';
    octx.textBaseline = 'top';
    octx.fillText(inWin.length ? 'vol' : 'vol (awaiting tape)', padL + 4 * dpr, volTop + 2 * dpr);
  }

  function paintCvdStrip(octx, layout, dpr) {
    const { padL, heatW, cvdTop, cvdH, tMin, tMax } = layout;
    if (cvdH <= 0) return;

    octx.fillStyle = '#080b10';
    octx.fillRect(padL, cvdTop, heatW, cvdH);
    octx.strokeStyle = 'rgba(255,255,255,0.05)';
    octx.strokeRect(padL + 0.5, cvdTop + 0.5, heatW - 1, cvdH - 1);

    const pts = cvdData.points;
    if (pts.length < 2) {
      octx.fillStyle = '#474d57';
      octx.font = `${9 * dpr}px IBM Plex Mono, SF Mono, Menlo, monospace`;
      octx.textAlign = 'left';
      octx.textBaseline = 'middle';
      octx.fillText('CVD accumulating…', padL + 6 * dpr, cvdTop + cvdH / 2);
      return;
    }

    const spanT = Math.max(1, tMax - tMin);
    const tStart = tMin / 1000;
    const tEnd = tMax / 1000;
    const inWindow = pts.filter((p) => p.sec >= tStart && p.sec <= tEnd);
    const series = inWindow.length >= 2 ? inWindow : pts.slice(-Math.min(pts.length, 120));

    let lo = Infinity;
    let hi = -Infinity;
    for (const p of series) {
      if (p.cvd < lo) lo = p.cvd;
      if (p.cvd > hi) hi = p.cvd;
    }
    if (!Number.isFinite(lo) || !Number.isFinite(hi)) return;
    const pad = Math.max(1, (hi - lo) * 0.08);
    lo -= pad;
    hi += pad;
    const span = hi - lo || 1;

    octx.beginPath();
    for (let i = 0; i < series.length; i++) {
      const p = series[i];
      const tMs = p.sec * 1000;
      const x = timeX(tMs, tMin, spanT, padL, heatW);
      const y = cvdTop + cvdH - ((p.cvd - lo) / span) * (cvdH - 4 * dpr) - 2 * dpr;
      if (i === 0) octx.moveTo(x, y);
      else octx.lineTo(x, y);
    }
    const lastCvd = series[series.length - 1]?.cvd ?? 0;
    octx.strokeStyle = lastCvd >= 0 ? 'rgba(2,192,118,0.85)' : 'rgba(246,70,93,0.85)';
    octx.lineWidth = 1.2 * dpr;
    octx.stroke();

    // Zero line when visible
    if (lo < 0 && hi > 0) {
      const zy = cvdTop + cvdH - ((0 - lo) / span) * (cvdH - 4 * dpr) - 2 * dpr;
      octx.beginPath();
      octx.moveTo(padL, zy);
      octx.lineTo(padL + heatW, zy);
      octx.strokeStyle = 'rgba(255,255,255,0.12)';
      octx.lineWidth = 1;
      octx.stroke();
    }

    octx.fillStyle = lastCvd >= 0 ? 'rgba(2,192,118,0.9)' : 'rgba(246,70,93,0.9)';
    octx.font = `${9 * dpr}px IBM Plex Mono, SF Mono, Menlo, monospace`;
    octx.textAlign = 'right';
    octx.textBaseline = 'top';
    octx.fillText(`CVD ${fmtUsd(lastCvd)}`, padL + heatW - 4 * dpr, cvdTop + 2 * dpr);
  }

  function paintGridLines(octx, layout) {
    const { padL, padT, heatW, heatH } = layout;
    octx.strokeStyle = 'rgba(255,255,255,0.04)';
    octx.lineWidth = 1;
    for (let i = 0; i <= 8; i++) {
      const y = padT + (heatH * i) / 8;
      octx.beginPath();
      octx.moveTo(padL, y);
      octx.lineTo(padL + heatW, y);
      octx.stroke();
    }
    for (let i = 0; i <= 10; i++) {
      const x = padL + (heatW * i) / 10;
      octx.beginPath();
      octx.moveTo(x, padT);
      octx.lineTo(x, padT + heatH);
      octx.stroke();
    }
  }

  function paintPriceLabels(octx, layout, dpr) {
    const { padL, padT, heatH, priceMin, priceMax } = layout;
    const spanP = priceMax - priceMin || 1;
    octx.fillStyle = '#848e9c';
    octx.font = `${11 * dpr}px IBM Plex Mono, SF Mono, Menlo, monospace`;
    octx.textAlign = 'right';
    octx.textBaseline = 'middle';
    for (let i = 0; i <= 6; i++) {
      const px = priceMax - (spanP * i) / 6;
      const y = padT + (heatH * i) / 6;
      octx.fillText(fmtPrice(px, 2), padL - 6 * dpr, y);
    }
  }

  function paint() {
    const el = canvas;
    const host = wrap;
    if (!el || !host) return;
    const dpr = Math.min(2, window.devicePixelRatio || 1);
    const wCss = host.clientWidth || 640;
    const hCss = host.clientHeight || 360;
    const w = Math.max(1, Math.floor(wCss * dpr));
    const h = Math.max(1, Math.floor(hCss * dpr));

    if (el.width !== w || el.height !== h) {
      el.width = w;
      el.height = h;
    }

    const ctx = el.getContext('2d', { alpha: false });
    if (!ctx) return;

    const showVap = layers.vap;
    const showCvd = layers.cvd;
    const showVol = layers.vol;
    const showCob = layers.cob;

    const focusHint =
      lastPrice != null && Number.isFinite(lastPrice)
        ? lastPrice
        : depthHistory.at(-1)?.mid != null
          ? depthHistory.at(-1).mid
          : 1000;
    // Wide enough for "64,287.99" — fixed 58px clipped the leading 6 → fake 4k axis.
    const padL = Math.max(72 * dpr, priceAxisPadPx(focusHint, dpr));
    const cobW = showCob ? 58 * dpr : 0;
    const vapW = showVap ? 64 * dpr : 0;
    const padR = 8 * dpr + cobW + vapW;
    const padT = 8 * dpr;
    const cvdH = showCvd ? Math.max(28 * dpr, h * 0.08) : 0;
    const volH = showVol ? Math.max(36 * dpr, h * 0.12) : 0;
    const padB = 4 * dpr + (cvdH > 0 ? cvdH + 4 * dpr : 0);
    const heatH = h - padT - volH - padB;
    const heatW = w - padL - padR;
    const cobX = padL + heatW + 3 * dpr;
    const vapX = cobX + cobW + (cobW > 0 ? 2 * dpr : 0);
    const volTop = padT + heatH + 6 * dpr;
    const cvdTop = showVol ? volTop + volH + 4 * dpr : volTop + 4 * dpr;

    frameBuf = ensureCanvas(frameBuf, w, h);
    const octx = frameBuf.getContext('2d', { alpha: false });
    if (!octx) return;

    octx.fillStyle = '#0c1016';
    octx.fillRect(0, 0, w, h);

    const rows = Math.min(140, Math.max(48, Math.floor(heatH / (1.5 * dpr))));
    const cols = Math.min(depthHistory.length, Math.max(48, Math.floor(heatW / (2.2 * dpr))));

    const win = computePriceWindow(depthHistory, {
      focusPrice: lastPrice,
      tick,
      lookback: Math.min(16, depthHistory.length) || 12,
      minTicks: 48,
      padFrac: 0.18,
      minBps: 0.6,
      maxBps: 8,
      zoom: zoomFactor,
    });

    let targetLo = win?.priceMin ?? (lastPrice != null ? lastPrice * 0.999 : null);
    let targetHi = win?.priceMax ?? (lastPrice != null ? lastPrice * 1.001 : null);

    const now = Date.now();
    let tMax = followLive || viewTMax == null ? now : viewTMax;
    if (followLive) viewTMax = null;
    let tMin = tMax - viewSec * 1000;
    let priceMin = lastPrice != null ? lastPrice * 0.9995 : 0;
    let priceMax = lastPrice != null ? lastPrice * 1.0005 : 1;

    if (targetLo != null && targetHi != null) {
      // Hard-reset EMA if scale drifted to a wrong regime (e.g. leftover / jump).
      if (scaleLo != null && scaleHi != null && lastPrice != null && Number.isFinite(lastPrice)) {
        const midScale = (scaleLo + scaleHi) / 2;
        if (midScale > 0 && Math.abs(lastPrice - midScale) / lastPrice > 0.02) {
          scaleLo = null;
          scaleHi = null;
        }
      }
      scaleLo = ema(scaleLo, targetLo, 0.22);
      scaleHi = ema(scaleHi, targetHi, 0.22);
      // Never let EMA lag leave the focus price outside the visible domain.
      if (lastPrice != null && Number.isFinite(lastPrice)) {
        if (lastPrice < scaleLo) scaleLo = lastPrice - (scaleHi - scaleLo) * 0.05;
        if (lastPrice > scaleHi) scaleHi = lastPrice + (scaleHi - scaleLo) * 0.05;
      }
    }

    // Slice history to the visible time window (keep continuity; don't clear ring).
    const histInView = depthHistory.filter((s) => s && s.t >= tMin - 2000 && s.t <= tMax + 500);
    const histRaw = histInView.length >= 2 ? histInView : depthHistory;
    const histForGrid = densifyDepthHistory(histRaw, {
      bucketMs: Math.max(120, Math.min(500, Math.floor((viewSec * 1000) / Math.max(64, cols)))),
      tMin: histRaw[0]?.t,
      tMax: histRaw.at(-1)?.t,
      maxCols: cols,
    });

    let grid = buildHeatmapGrid(histForGrid.length >= 2 ? histForGrid : histRaw, {
      rows,
      cols: Math.min((histForGrid.length >= 2 ? histForGrid : histRaw).length, cols),
      priceMin: scaleLo,
      priceMax: scaleHi,
    });
    if (grid) lastGoodGrid = grid;
    else if (lastGoodGrid) grid = lastGoodGrid;

    paintGridLines(octx, { padL, padT, heatW, heatH });

    if (grid) {
      priceMin = scaleLo ?? grid.priceMin;
      priceMax = scaleHi ?? grid.priceMax;
      // Keep heat columns / overlays time-aligned (index↔timestamp from ring).
      tMin = grid.tMin;
      tMax = Math.max(grid.tMax || tMin + 1, tMin + 1);

      const layout = {
        padL,
        padT,
        heatW,
        heatH,
        cobX,
        cobW,
        vapX,
        vapW,
        volTop,
        volH,
        cvdTop,
        cvdH,
        priceMin,
        priceMax,
        tMin,
        tMax,
        ofHeatGain: ofHeat,
      };

      // Paint heat against the visible Y domain (may differ slightly from grid domain).
      const heatGrid = {
        ...grid,
        priceMin,
        priceMax,
      };

      if (layers.heat) paintHeatLayer(octx, heatGrid, layout, dpr);
      else if (layers.mid || layers.candles) {
        paintHeatLayer(octx, { ...heatGrid, grid: new Float32Array(0) }, layout, dpr);
      }

      if (layers.candles) paintCandles(octx, layout, dpr);
      if (layers.footprint) paintFootprint(octx, layout, dpr);
      if (layers.bubbles) paintBubbles(octx, layout, dpr);
      if (layers.markers) paintMarkers(octx, layout, dpr);
      if (layers.cob) paintCobColumn(octx, layout, dpr);
      if (layers.vap) paintVapSidebar(octx, layout, dpr);
      if (layers.vol) paintVolSubplot(octx, layout, dpr);
      if (layers.cvd) paintCvdStrip(octx, layout, dpr);

      paintPriceLabels(octx, layout, dpr);

      // Last price tag on right of heat (Bookmap-style)
      if (lastPrice != null && lastPrice >= priceMin && lastPrice <= priceMax) {
        const y = priceY(lastPrice, priceMin, priceMax, padT, heatH);
        octx.fillStyle = 'rgba(240,185,11,0.92)';
        octx.fillRect(padL + heatW - 2 * dpr, y - 1 * dpr, 2 * dpr, 2 * dpr);
        octx.beginPath();
        octx.moveTo(padL, y);
        octx.lineTo(padL + heatW, y);
        octx.strokeStyle = 'rgba(240,185,11,0.2)';
        octx.setLineDash([4 * dpr, 4 * dpr]);
        octx.lineWidth = 1;
        octx.stroke();
        octx.setLineDash([]);
      }
    }

    octx.textAlign = 'left';
    octx.textBaseline = 'top';
    octx.fillStyle = '#5e6673';
    octx.font = `${10 * dpr}px IBM Plex Mono, SF Mono, Menlo, monospace`;
    octx.fillText(
      `L2+tape reconstruction · ${venue || '?'} ${symbol || ''} · not MBO · markers≠liquidations · ${windowLabel(viewSec)}${followLive ? '' : ' · paused'}`,
      padL,
      h - 12 * dpr,
    );

    ctx.drawImage(frameBuf, 0, 0);

    el._layout = {
      padL,
      padT,
      heatW,
      heatH,
      cobX,
      cobW,
      vapX,
      vapW,
      priceMin,
      priceMax,
      tMin,
      tMax,
      dpr,
      volTop,
      volH,
      cvdTop,
      cvdH,
      tick,
    };
  }

  function scheduleZoomPersist(patch) {
    if (zoomPersistTimer) clearTimeout(zoomPersistTimer);
    zoomPersistTimer = setTimeout(() => {
      zoomPersistTimer = 0;
      patchSettings(patch);
    }, 120);
  }

  function jumpToLive() {
    viewTMax = null;
    patchSettings({ ofFollowLive: true });
    gate.schedule();
  }

  function zoomPrice(dir) {
    const next = clampPriceZoom(zoomFactor * (dir < 0 ? 0.82 : 1.22), zoomFactor);
    localPriceZoom = next;
    scheduleZoomPersist({ ofPriceZoom: next });
    gate.schedule();
  }

  function zoomTime(dir) {
    const next = clampViewSec(viewSec * (dir < 0 ? 0.75 : 1.35), viewSec);
    localViewSec = next;
    scheduleZoomPersist({ ofViewSec: next, ofFollowLive: followLive });
    gate.schedule();
  }

  function onWheel(ev) {
    ev.preventDefault();
    const dir = ev.deltaY > 0 ? 1 : -1;
    if (ev.shiftKey || ev.altKey) zoomTime(dir);
    else zoomPrice(dir);
  }

  function onTouchStart(ev) {
    if (ev.touches.length === 2) {
      const [a, b] = ev.touches;
      pinchDist0 = Math.hypot(a.clientX - b.clientX, a.clientY - b.clientY);
    }
  }

  function onTouchMove(ev) {
    if (ev.touches.length !== 2 || !(pinchDist0 > 0)) return;
    ev.preventDefault();
    const [a, b] = ev.touches;
    const dist = Math.hypot(a.clientX - b.clientX, a.clientY - b.clientY);
    const ratio = dist / pinchDist0;
    if (ratio > 1.08) {
      zoomPrice(-1);
      pinchDist0 = dist;
    } else if (ratio < 0.92) {
      zoomPrice(1);
      pinchDist0 = dist;
    }
  }

  function onTouchEnd() {
    pinchDist0 = 0;
  }

  function nearestVapRow(price) {
    if (!vapRows.length) return null;
    let best = vapRows[0];
    let bestD = Math.abs(best.price - price);
    for (const r of vapRows) {
      const d = Math.abs(r.price - price);
      if (d < bestD) {
        bestD = d;
        best = r;
      }
    }
    return bestD < (tick || 0.1) * 2 ? best : null;
  }

  function onMove(ev) {
    const el = canvas;
    if (!el?._layout) return;
    const L = el._layout;
    const rect = el.getBoundingClientRect();
    const x = ((ev.clientX - rect.left) / rect.width) * el.width;
    const y = ((ev.clientY - rect.top) / rect.height) * el.height;
    if (x < L.padL || x > L.padL + L.heatW || y < L.padT || y > L.padT + L.heatH) {
      hover = null;
      return;
    }
    const spanP = L.priceMax - L.priceMin || 1;
    const spanT = Math.max(1, L.tMax - L.tMin);
    const price = L.priceMax - ((y - L.padT) / L.heatH) * spanP;
    const t = L.tMin + ((x - L.padL) / L.heatW) * spanT;

    let nearest = null;
    let bestDt = Infinity;
    for (const s of depthHistory) {
      const dt = Math.abs(s.t - t);
      if (dt < bestDt) {
        bestDt = dt;
        nearest = s;
      }
    }
    const qTick = nearest?.tick || L.tick || tick || 0.1;
    const resting = restingAtPrice(nearest, price, qTick);
    const walls = nearestWalls(nearest, price);
    const qpx = resting.price ?? walls.bidPrice ?? walls.askPrice ?? price;
    const bidSz = resting.bidUsd > 0 ? resting.bidUsd : walls.bidUsd;
    const askSz = resting.askUsd > 0 ? resting.askUsd : walls.askUsd;

    let bub = null;
    let best = Infinity;
    for (const b of bubbles) {
      const dx = (b.t - t) / spanT;
      const dy = (b.price - price) / spanP;
      const d = dx * dx + dy * dy;
      if (d < best) {
        best = d;
        bub = b;
      }
    }

    const lines = [
      `${fmtPrice(price, 2)} · ${new Date(t).toISOString().slice(11, 19)}Z`,
      `Resting bid ${fmtUsd(bidSz)}${
        walls.bidPrice != null ? ` @ ${fmtPrice(walls.bidPrice, 2)}` : ''
      } · ask ${fmtUsd(askSz)}${walls.askPrice != null ? ` @ ${fmtPrice(walls.askPrice, 2)}` : ''}`,
    ];
    if (bub && best < 0.006) {
      lines.push(
        `Print Δ ${fmtUsd(bub.delta)} · buy ${fmtUsd(bub.buyUsd)} / sell ${fmtUsd(bub.sellUsd)}`,
      );
    }
    let markHit = null;
    let markBest = Infinity;
    for (const m of markers) {
      if (m.price == null || m.ts == null) continue;
      const dx = (m.ts * 1000 - t) / spanT;
      const dy = (m.price - price) / spanP;
      const d = dx * dx + dy * dy;
      if (d < markBest) {
        markBest = d;
        markHit = m;
      }
    }
    if (markHit && markBest < 0.01) {
      lines.push(`${markHit.label} · ${markHit.note}`);
    }
    const vapHit = nearestVapRow(price);
    if (vapHit) {
      lines.push(
        `VAP buy ${fmtUsd(vapHit.buyUsd)} · sell ${fmtUsd(vapHit.sellUsd)} · Δ ${fmtUsd(vapHit.delta)}`,
      );
    }
    hover = {
      x: ev.clientX - rect.left + 12,
      y: ev.clientY - rect.top + 12,
      lines,
    };
  }

  function onLeave() {
    hover = null;
  }

  function patchSettings(patch) {
    onSettings(patch);
  }

  function onTickChange(ev) {
    patchSettings({ ofTick: ev.currentTarget.value });
  }

  function onHeatChange(ev) {
    patchSettings({ ofHeat: Number(ev.currentTarget.value) });
  }

  function onBubbleChange(ev) {
    const n = Number(ev.currentTarget.value);
    if (Number.isFinite(n) && n >= 0) patchSettings({ ofBubbleMinUsd: n });
  }

  /** @param {string} key */
  function toggleLayer(key) {
    const next = { ...layers, [key]: !layers[key] };
    patchSettings({ ofLayers: serializeOfLayers(next) });
  }

  $effect(() => {
    const key = [
      depthHistory.length,
      tape.length,
      viewSec,
      zoomFactor,
      followLive,
      lastPrice ?? '',
      venue,
      symbol,
      hasL2,
      ofTick,
      ofHeat,
      ofBubbleMinUsd,
      ofLayers,
    ].join('|');
    depthHistory;
    tape;
    viewSec;
    zoomFactor;
    followLive;
    lastPrice;
    ofTick;
    ofHeat;
    ofBubbleMinUsd;
    ofLayers;
    if (key !== lastPaintKey) lastPaintKey = key;
    gate.schedule();
  });

  onMount(() => {
    gate.schedule();
    ro = new ResizeObserver(() => gate.schedule());
    if (wrap) ro.observe(wrap);
    const el = canvas;
    const onWheelNative = (e) => onWheel(e);
    el?.addEventListener('wheel', onWheelNative, { passive: false });
    return () => {
      gate.dispose();
      ro?.disconnect();
      el?.removeEventListener('wheel', onWheelNative);
      if (zoomPersistTimer) clearTimeout(zoomPersistTimer);
    };
  });
</script>

<section class="of-heat" aria-label="Order flow liquidity heatmap">
  <div class="toolbar">
    <span class="title">Order Flow</span>
    <label class="ctl" title="Price tick quantization">
      tick
      <select value={ofTick} onchange={onTickChange}>
        {#each TICK_OPTS as opt}
          <option value={opt.v}>{opt.label}</option>
        {/each}
      </select>
    </label>
    <label class="ctl heat-ctl" title="Heat intensity gain (0.5–2.5)">
      heat
      <input type="range" min="0.5" max="2.5" step="0.1" value={ofHeat} oninput={onHeatChange} />
      <span class="val">{ofHeat.toFixed(1)}</span>
    </label>
    <label class="ctl" title="Minimum bubble notional (USD)">
      bubble≥$
      <input
        type="number"
        min="0"
        step="100"
        value={ofBubbleMinUsd}
        onchange={onBubbleChange}
      />
    </label>
    <div class="zoom-btns" title="Wheel = price zoom · Shift+wheel = time zoom · Pinch supported">
      <button type="button" onclick={() => zoomPrice(-1)} aria-label="Zoom price in">Y−</button>
      <button type="button" onclick={() => zoomPrice(1)} aria-label="Zoom price out">Y+</button>
      <button type="button" onclick={() => zoomTime(-1)} aria-label="Zoom time in">T−</button>
      <button type="button" onclick={() => zoomTime(1)} aria-label="Zoom time out">T+</button>
      <button
        type="button"
        class:live={followLive}
        onclick={jumpToLive}
        aria-label="Jump to live"
      >live</button>
    </div>
    <div class="layers-wrap">
      <button
        type="button"
        class="layers-btn"
        aria-expanded={layersOpen}
        onclick={() => (layersOpen = !layersOpen)}
      >
        layers
      </button>
      {#if layersOpen}
        <div class="layers-pop" role="group" aria-label="Visible layers">
          {#each LAYER_KEYS as lk}
            <label class="layer-row">
              <input
                type="checkbox"
                checked={layers[lk.k]}
                onchange={() => toggleLayer(lk.k)}
              />
              {lk.label}
            </label>
          {/each}
        </div>
      {/if}
    </div>
    <span class="win">{windowLabel(viewSec)} · z{zoomFactor.toFixed(2)}</span>
    {#if lastPrice != null}
      <span class="last">{fmtPrice(lastPrice, 2)}</span>
    {/if}
    <span class="honesty" title="Reconstructed from L2 books + aggressor tape — not MBO / not exchange liquidations">
      L2+tape · not MBO
    </span>
  </div>

  <div class="canvas-wrap" bind:this={wrap}>
    <canvas
      bind:this={canvas}
      onmousemove={onMove}
      onmouseleave={onLeave}
      ontouchstart={onTouchStart}
      ontouchmove={onTouchMove}
      ontouchend={onTouchEnd}
      aria-label="Liquidity heatmap with volume bubbles, VAP, CVD"
    ></canvas>
    {#if hover}
      <div class="tip" style={`left:${hover.x}px;top:${hover.y}px`}>
        {#each hover.lines as line}
          <div>{line}</div>
        {/each}
      </div>
    {/if}
    {#if !hasL2}
      <div class="overlay warn">no L2 book — heatmap unavailable</div>
    {:else if depthHistory.length < 2}
      <div class="overlay">sampling L2 depth into heatmap…</div>
    {/if}
  </div>
</section>

<style>
  .of-heat {
    display: flex;
    flex-direction: column;
    height: 100%;
    min-height: 0;
    background: var(--panel);
  }

  .toolbar {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.35rem 0.55rem;
    padding: 0.28rem 0.5rem;
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
  }

  .title {
    font-size: 0.72rem;
    font-weight: 600;
    color: var(--accent);
    margin-right: 0.15rem;
  }

  .ctl {
    display: inline-flex;
    align-items: center;
    gap: 0.2rem;
    font-family: var(--mono);
    font-size: 0.58rem;
    color: var(--muted);
    text-transform: lowercase;
  }

  .ctl select,
  .ctl input[type='number'] {
    background: var(--panel-2, #12161c);
    border: 1px solid var(--border);
    border-radius: 2px;
    padding: 0.08rem 0.22rem;
    font-family: var(--mono);
    font-size: 0.58rem;
    color: var(--text, #eaecef);
  }

  .ctl select {
    min-width: 3.2rem;
  }

  .ctl input[type='number'] {
    width: 4.5rem;
  }

  .heat-ctl input[type='range'] {
    width: 4.5rem;
    height: 12px;
    accent-color: var(--accent);
    cursor: pointer;
  }

  .heat-ctl .val {
    min-width: 1.6rem;
    color: var(--text-dim, #848e9c);
  }

  .zoom-btns {
    display: inline-flex;
    gap: 0.12rem;
    align-items: center;
  }

  .zoom-btns button {
    background: var(--panel-2, #12161c);
    border: 1px solid var(--border);
    border-radius: 2px;
    padding: 0.08rem 0.28rem;
    font-family: var(--mono);
    font-size: 0.55rem;
    color: var(--muted);
    cursor: pointer;
  }

  .zoom-btns button:hover {
    border-color: var(--accent);
    color: var(--text, #eaecef);
  }

  .zoom-btns button.live {
    color: var(--bid);
    border-color: rgba(2, 192, 118, 0.45);
  }

  .layers-wrap {
    position: relative;
  }

  .layers-btn {
    background: var(--panel-2, #12161c);
    border: 1px solid var(--border);
    border-radius: 2px;
    padding: 0.1rem 0.35rem;
    font-family: var(--mono);
    font-size: 0.58rem;
    color: var(--muted);
    cursor: pointer;
  }

  .layers-btn:hover {
    border-color: var(--accent);
    color: var(--text, #eaecef);
  }

  .layers-pop {
    position: absolute;
    top: calc(100% + 4px);
    left: 0;
    z-index: 10;
    background: rgba(18, 22, 28, 0.98);
    border: 1px solid var(--border);
    border-radius: 2px;
    padding: 0.35rem 0.45rem;
    min-width: 7.5rem;
    box-shadow: 0 6px 16px rgba(0, 0, 0, 0.4);
  }

  .layer-row {
    display: flex;
    align-items: center;
    gap: 0.35rem;
    font-family: var(--mono);
    font-size: 0.58rem;
    color: var(--text-dim, #848e9c);
    padding: 0.12rem 0;
    cursor: pointer;
    user-select: none;
  }

  .layer-row input {
    accent-color: var(--accent);
  }

  .win {
    font-family: var(--mono);
    font-size: 0.58rem;
    color: var(--muted);
    padding: 0.05rem 0.3rem;
    border: 1px solid var(--border);
    border-radius: 2px;
    background: var(--panel-2, #12161c);
  }

  .last {
    margin-left: auto;
    font-family: var(--mono);
    font-size: 0.82rem;
    font-weight: 600;
    color: var(--text, #eaecef);
  }

  .honesty {
    font-family: var(--mono);
    font-size: 0.55rem;
    color: var(--muted);
    border: 1px solid var(--border);
    border-radius: 2px;
    padding: 0.05rem 0.28rem;
    background: var(--panel-2, #12161c);
    white-space: nowrap;
  }

  .canvas-wrap {
    position: relative;
    flex: 1;
    min-height: 0;
  }

  canvas {
    width: 100%;
    height: 100%;
    display: block;
    cursor: crosshair;
  }

  .tip {
    position: absolute;
    z-index: 5;
    pointer-events: none;
    background: rgba(24, 28, 36, 0.94);
    border: 1px solid var(--border);
    padding: 0.35rem 0.45rem;
    font-family: var(--mono);
    font-size: 0.62rem;
    color: var(--text, #eaecef);
    max-width: 300px;
    border-radius: 2px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.35);
  }

  .overlay {
    position: absolute;
    inset: 0;
    display: grid;
    place-items: center;
    color: var(--muted);
    font-family: var(--mono);
    font-size: 0.72rem;
    pointer-events: none;
    background: rgba(12, 16, 22, 0.42);
    text-align: center;
    padding: 1rem;
  }

  .overlay.warn {
    color: var(--ask);
    background: rgba(12, 16, 22, 0.55);
  }
</style>
