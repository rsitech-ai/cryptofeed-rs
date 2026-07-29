<script>
  import { onMount } from 'svelte';
  import { fmtPrice, fmtUsd } from '../lib/format.js';
  import { createPaintGate, ema } from '../lib/paint.js';
  import {
    buildHeatmapGrid,
    heatmapColor,
    inferTickSize,
    tradeBubbles,
    volumeBarsFromTape,
  } from '../lib/orderflow.js';

  let {
    depthHistory = [],
    tape = [],
    windowSec = 300,
    venue = '',
    symbol = '',
    lastPrice = null,
  } = $props();

  let wrap = $state(null);
  let canvas = $state(null);
  /** @type {{ x: number, y: number, lines: string[] }|null} */
  let hover = $state(null);

  let tick = $derived(inferTickSize(depthHistory.at(-1) ? {
    bids: [...(depthHistory.at(-1)?.bids?.entries?.() || [])].map(([price, usd]) => ({
      price,
      quantity: usd / (price || 1),
    })),
    asks: [...(depthHistory.at(-1)?.asks?.entries?.() || [])].map(([price, usd]) => ({
      price,
      quantity: usd / (price || 1),
    })),
  } : null));

  let bubbles = $derived(
    tradeBubbles(tape, { windowSec, tick, bucketMs: 400, maxBubbles: 200 }),
  );
  let volBars = $derived(volumeBarsFromTape(tape, { windowSec, bucketSec: 1 }));

  /** Stable y-scale (EMA) so walls don't jump every sample. */
  let scaleLo = null;
  let scaleHi = null;
  /** @type {HTMLCanvasElement|null} */
  let frameBuf = null;
  /** @type {HTMLCanvasElement|null} */
  let heatLayer = null;
  /** @type {ImageData|null} */
  let imageBuf = null;
  let ro = null;
  let lastPaintKey = '';

  const gate = createPaintGate(() => paint(), { minIntervalMs: 110 });

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
      // createImageData needs a live 2d context; use heat layer once sized
      heatLayer = ensureCanvas(heatLayer, w, h);
      imageBuf = heatLayer.getContext('2d').createImageData(w, h);
    }
    return imageBuf;
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

    // Resize only when needed — setting width/height clears the bitmap (flash).
    if (el.width !== w || el.height !== h) {
      el.width = w;
      el.height = h;
    }

    const ctx = el.getContext('2d', { alpha: false });
    if (!ctx) return;

    const padL = 58 * dpr;
    const padR = 10 * dpr;
    const padT = 8 * dpr;
    const volH = Math.max(36 * dpr, h * 0.14);
    const padB = 4 * dpr;
    const heatH = h - padT - volH - padB - 4 * dpr;
    const heatW = w - padL - padR;
    const heatWInt = Math.max(1, Math.ceil(heatW));
    const heatHInt = Math.max(1, Math.ceil(heatH));

    // Draw into offscreen first, then blit — avoids mid-frame clear flash.
    frameBuf = ensureCanvas(frameBuf, w, h);
    const octx = frameBuf.getContext('2d', { alpha: false });
    if (!octx) return;

    octx.fillStyle = '#0c1016';
    octx.fillRect(0, 0, w, h);

    const rows = Math.min(120, Math.max(40, Math.floor(heatH / (2 * dpr))));
    const cols = Math.min(depthHistory.length, Math.max(32, Math.floor(heatW / (2 * dpr))));

    // Raw bounds from latest mid / lastPrice for EMA target.
    let targetLo = lastPrice != null ? lastPrice * 0.9975 : null;
    let targetHi = lastPrice != null ? lastPrice * 1.0025 : null;
    const latest = depthHistory.at(-1);
    if (latest?.mid != null && Number.isFinite(latest.mid)) {
      const pad = latest.mid * 0.0025;
      targetLo = latest.mid - pad;
      targetHi = latest.mid + pad;
    }

    let priceMin = lastPrice != null ? lastPrice * 0.998 : 0;
    let priceMax = lastPrice != null ? lastPrice * 1.002 : 1;
    let tMin = Date.now() - windowSec * 1000;
    let tMax = Date.now();

    // Prefer fixed EMA window around mid so y-axis is calm.
    if (targetLo != null && targetHi != null) {
      scaleLo = ema(scaleLo, targetLo, 0.08);
      scaleHi = ema(scaleHi, targetHi, 0.08);
      // Expand slightly if book walls exceed current window.
      let wallLo = Infinity;
      let wallHi = -Infinity;
      for (const s of depthHistory.slice(-Math.min(cols, depthHistory.length))) {
        for (const px of s.bids?.keys?.() || []) {
          if (px < wallLo) wallLo = px;
          if (px > wallHi) wallHi = px;
        }
        for (const px of s.asks?.keys?.() || []) {
          if (px < wallLo) wallLo = px;
          if (px > wallHi) wallHi = px;
        }
      }
      if (Number.isFinite(wallLo) && wallLo < scaleLo) scaleLo = ema(scaleLo, wallLo, 0.2);
      if (Number.isFinite(wallHi) && wallHi > scaleHi) scaleHi = ema(scaleHi, wallHi, 0.2);
    }

    const grid = buildHeatmapGrid(depthHistory, {
      rows,
      cols,
      priceMin: scaleLo,
      priceMax: scaleHi,
    });

    // Grid lines
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

    if (grid) {
      priceMin = grid.priceMin;
      priceMax = grid.priceMax;
      tMin = grid.tMin;
      tMax = grid.tMax || tMin + 1;
      const spanT = Math.max(1, tMax - tMin);
      const cellW = heatW / grid.cols;
      const cellH = heatH / grid.rows;
      const img = ensureImageData(heatWInt, heatHInt);
      const data = img.data;
      // Fill base (opaque) — no transparent flash
      for (let i = 0; i < data.length; i += 4) {
        data[i] = 8;
        data[i + 1] = 12;
        data[i + 2] = 22;
        data[i + 3] = 255;
      }
      for (let r = 0; r < grid.rows; r++) {
        for (let c = 0; c < grid.cols; c++) {
          const v = grid.grid[r * grid.cols + c];
          if (v <= 0) continue;
          const intensity = Math.min(1, Math.log1p(v) / Math.log1p(grid.maxVal));
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

      if (grid.midPath.length > 1) {
        octx.beginPath();
        octx.strokeStyle = 'rgba(240,185,11,0.85)';
        octx.lineWidth = 1.2 * dpr;
        for (let i = 0; i < grid.midPath.length; i++) {
          const p = grid.midPath[i];
          const x = padL + ((p.t - tMin) / spanT) * heatW;
          const y = padT + ((priceMax - p.mid) / (priceMax - priceMin || 1)) * heatH;
          if (i === 0) octx.moveTo(x, y);
          else octx.lineTo(x, y);
        }
        octx.stroke();
      }
    }

    const spanP = priceMax - priceMin || 1;
    const spanT = Math.max(1, tMax - tMin);

    const maxBubbleUsd = Math.max(1, ...bubbles.map((b) => b.totalUsd), 1);
    for (const b of bubbles) {
      if (b.t < tMin || b.t > tMax) continue;
      if (b.price < priceMin || b.price > priceMax) continue;
      const x = padL + ((b.t - tMin) / spanT) * heatW;
      const y = padT + ((priceMax - b.price) / spanP) * heatH;
      const r = Math.max(3 * dpr, Math.min(22 * dpr, Math.sqrt(b.totalUsd / maxBubbleUsd) * 20 * dpr));
      const buyFrac = b.totalUsd > 0 ? b.buyUsd / b.totalUsd : 0.5;

      octx.beginPath();
      octx.arc(x, y, r, Math.PI, 0, false);
      octx.closePath();
      octx.fillStyle = `rgba(246,70,93,${0.35 + (1 - buyFrac) * 0.45})`;
      octx.fill();

      octx.beginPath();
      octx.arc(x, y, r, 0, Math.PI, false);
      octx.closePath();
      octx.fillStyle = `rgba(2,192,118,${0.35 + buyFrac * 0.45})`;
      octx.fill();

      octx.beginPath();
      octx.moveTo(x - r, y);
      octx.lineTo(x + r, y);
      octx.strokeStyle = 'rgba(12,16,22,0.55)';
      octx.lineWidth = 1;
      octx.stroke();

      octx.beginPath();
      octx.arc(x, y, r, 0, Math.PI * 2);
      octx.strokeStyle = 'rgba(255,255,255,0.25)';
      octx.lineWidth = 1;
      octx.stroke();
    }

    const volTop = padT + heatH + 6 * dpr;
    octx.fillStyle = '#0a0e14';
    octx.fillRect(padL, volTop, heatW, volH);
    const maxVol = Math.max(1, ...volBars.map((v) => v.totalUsd));
    const barW = Math.max(1, heatW / Math.max(volBars.length, 1) - 1);
    for (const v of volBars) {
      const tMs = v.sec * 1000;
      if (tMs < tMin || tMs > tMax) continue;
      const x = padL + ((tMs - tMin) / spanT) * heatW;
      const buyH = (v.buyUsd / maxVol) * (volH - 2);
      const sellH = (v.sellUsd / maxVol) * (volH - 2);
      octx.fillStyle = 'rgba(2,192,118,0.7)';
      octx.fillRect(x, volTop + volH - buyH, barW, buyH);
      octx.fillStyle = 'rgba(246,70,93,0.7)';
      octx.fillRect(x, volTop + volH - buyH - sellH, barW, sellH);
    }

    octx.fillStyle = '#848e9c';
    octx.font = `${11 * dpr}px IBM Plex Mono, SF Mono, Menlo, monospace`;
    octx.textAlign = 'right';
    octx.textBaseline = 'middle';
    for (let i = 0; i <= 6; i++) {
      const px = priceMax - (spanP * i) / 6;
      const y = padT + (heatH * i) / 6;
      octx.fillText(fmtPrice(px, 2), padL - 6 * dpr, y);
    }

    octx.textAlign = 'left';
    octx.textBaseline = 'top';
    octx.fillStyle = '#5e6673';
    octx.font = `${10 * dpr}px IBM Plex Mono, SF Mono, Menlo, monospace`;
    octx.fillText(
      `L2 reconstructed · ${venue || '?'} ${symbol || ''} · not MBO`,
      padL,
      h - 12 * dpr,
    );

    // Single blit to visible canvas (no intermediate clear flash)
    ctx.drawImage(frameBuf, 0, 0);

    el._layout = { padL, padT, heatW, heatH, priceMin, priceMax, tMin, tMax, dpr, volTop, volH };
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
    const qpx = Math.round(price / (nearest?.tick || tick || 0.1)) * (nearest?.tick || tick || 0.1);
    const bidSz = nearest?.bids?.get?.(qpx) ?? 0;
    const askSz = nearest?.asks?.get?.(qpx) ?? 0;

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
      `Resting bid ${fmtUsd(bidSz)} · ask ${fmtUsd(askSz)} @ ~${fmtPrice(qpx, 2)}`,
    ];
    if (bub && best < 0.004) {
      lines.push(
        `Print Δ ${fmtUsd(bub.delta)} · buy ${fmtUsd(bub.buyUsd)} / sell ${fmtUsd(bub.sellUsd)}`,
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

  $effect(() => {
    const key = `${depthHistory.length}|${tape.length}|${windowSec}|${lastPrice ?? ''}|${venue}|${symbol}`;
    // Always schedule on dependency change; gate throttles paint Hz.
    depthHistory;
    tape;
    windowSec;
    lastPrice;
    if (key !== lastPaintKey) lastPaintKey = key;
    gate.schedule();
  });

  onMount(() => {
    gate.schedule();
    ro = new ResizeObserver(() => gate.schedule());
    if (wrap) ro.observe(wrap);
    return () => {
      gate.dispose();
      ro?.disconnect();
    };
  });
</script>

<section class="of-heat" aria-label="Order flow liquidity heatmap">
  <div class="head">
    <span class="title">Order Flow</span>
    <span class="meta">{venue} · {symbol} · L2 heatmap + trade bubbles (reconstructed)</span>
    {#if lastPrice != null}
      <span class="last">{fmtPrice(lastPrice, 2)}</span>
    {/if}
  </div>
  <div class="canvas-wrap" bind:this={wrap}>
    <canvas
      bind:this={canvas}
      onmousemove={onMove}
      onmouseleave={onLeave}
      aria-label="Liquidity heatmap with volume bubbles"
    ></canvas>
    {#if hover}
      <div class="tip" style={`left:${hover.x}px;top:${hover.y}px`}>
        {#each hover.lines as line}
          <div>{line}</div>
        {/each}
      </div>
    {/if}
    {#if depthHistory.length < 2}
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
  .head {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    padding: 0.3rem 0.55rem;
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
  }
  .title {
    font-size: 0.75rem;
    font-weight: 600;
    color: var(--accent);
  }
  .meta {
    font-family: var(--mono);
    font-size: 0.58rem;
    color: var(--muted);
  }
  .last {
    margin-left: auto;
    font-family: var(--mono);
    font-size: 0.85rem;
    font-weight: 600;
    color: var(--text);
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
    color: var(--text);
    max-width: 280px;
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
    font-size: 0.75rem;
    pointer-events: none;
    background: rgba(12, 16, 22, 0.35);
  }
</style>
