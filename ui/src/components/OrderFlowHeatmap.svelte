<script>
  import { onMount } from 'svelte';
  import { fmtPrice, fmtUsd } from '../lib/format.js';
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
  let tip = $state(null);
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

  let raf = 0;
  let ro = null;

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
    const ctx = el.getContext('2d');
    if (!ctx) return;

    const padL = 58 * dpr;
    const padR = 10 * dpr;
    const padT = 8 * dpr;
    const volH = Math.max(36 * dpr, h * 0.14);
    const padB = 4 * dpr;
    const heatH = h - padT - volH - padB - 4 * dpr;
    const heatW = w - padL - padR;

    ctx.fillStyle = '#0c1016';
    ctx.fillRect(0, 0, w, h);

    const grid = buildHeatmapGrid(depthHistory, {
      rows: Math.min(120, Math.max(40, Math.floor(heatH / (2 * dpr)))),
      cols: Math.min(depthHistory.length, Math.max(32, Math.floor(heatW / (2 * dpr)))),
    });

    // Grid lines
    ctx.strokeStyle = 'rgba(255,255,255,0.04)';
    ctx.lineWidth = 1;
    for (let i = 0; i <= 8; i++) {
      const y = padT + (heatH * i) / 8;
      ctx.beginPath();
      ctx.moveTo(padL, y);
      ctx.lineTo(padL + heatW, y);
      ctx.stroke();
    }
    for (let i = 0; i <= 10; i++) {
      const x = padL + (heatW * i) / 10;
      ctx.beginPath();
      ctx.moveTo(x, padT);
      ctx.lineTo(x, padT + heatH);
      ctx.stroke();
    }

    let priceMin = lastPrice != null ? lastPrice * 0.998 : 0;
    let priceMax = lastPrice != null ? lastPrice * 1.002 : 1;
    let tMin = Date.now() - windowSec * 1000;
    let tMax = Date.now();

    if (grid) {
      priceMin = grid.priceMin;
      priceMax = grid.priceMax;
      tMin = grid.tMin;
      tMax = grid.tMax || tMin + 1;
      const spanT = Math.max(1, tMax - tMin);
      const cellW = heatW / grid.cols;
      const cellH = heatH / grid.rows;
      const img = ctx.createImageData(Math.ceil(heatW), Math.ceil(heatH));
      // Fill transparent first
      for (let i = 0; i < img.data.length; i += 4) {
        img.data[i] = 8;
        img.data[i + 1] = 12;
        img.data[i + 2] = 22;
        img.data[i + 3] = 255;
      }
      // Draw cells into ImageData (nearest-neighbor stretch)
      for (let r = 0; r < grid.rows; r++) {
        for (let c = 0; c < grid.cols; c++) {
          const v = grid.grid[r * grid.cols + c];
          if (v <= 0) continue;
          // log scale so walls pop
          const intensity = Math.min(1, Math.log1p(v) / Math.log1p(grid.maxVal));
          const [R, G, B, A] = heatmapColor(intensity);
          const x0 = Math.floor(c * cellW);
          const x1 = Math.floor((c + 1) * cellW);
          const y0 = Math.floor(r * cellH);
          const y1 = Math.floor((r + 1) * cellH);
          for (let y = y0; y < y1 && y < img.height; y++) {
            for (let x = x0; x < x1 && x < img.width; x++) {
              const idx = (y * img.width + x) * 4;
              const a = A / 255;
              img.data[idx] = Math.round(img.data[idx] * (1 - a) + R * a);
              img.data[idx + 1] = Math.round(img.data[idx + 1] * (1 - a) + G * a);
              img.data[idx + 2] = Math.round(img.data[idx + 2] * (1 - a) + B * a);
              img.data[idx + 3] = 255;
            }
          }
        }
      }
      // Blit via temp canvas for crispness
      const tmp = document.createElement('canvas');
      tmp.width = img.width;
      tmp.height = img.height;
      tmp.getContext('2d').putImageData(img, 0, 0);
      ctx.drawImage(tmp, padL, padT, heatW, heatH);

      // Mid path
      if (grid.midPath.length > 1) {
        ctx.beginPath();
        ctx.strokeStyle = 'rgba(240,185,11,0.85)';
        ctx.lineWidth = 1.2 * dpr;
        for (let i = 0; i < grid.midPath.length; i++) {
          const p = grid.midPath[i];
          const x = padL + ((p.t - tMin) / spanT) * heatW;
          const y = padT + ((priceMax - p.mid) / (priceMax - priceMin || 1)) * heatH;
          if (i === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        }
        ctx.stroke();
      }
    }

    const spanP = priceMax - priceMin || 1;
    const spanT = Math.max(1, tMax - tMin);

    // Volume bubbles (split green/red)
    const maxBubbleUsd = Math.max(1, ...bubbles.map((b) => b.totalUsd), 1);
    for (const b of bubbles) {
      if (b.t < tMin || b.t > tMax) continue;
      if (b.price < priceMin || b.price > priceMax) continue;
      const x = padL + ((b.t - tMin) / spanT) * heatW;
      const y = padT + ((priceMax - b.price) / spanP) * heatH;
      const r = Math.max(3 * dpr, Math.min(22 * dpr, Math.sqrt(b.totalUsd / maxBubbleUsd) * 20 * dpr));
      const buyFrac = b.totalUsd > 0 ? b.buyUsd / b.totalUsd : 0.5;

      // Sell (top half tendency) / buy (bottom) — classic bookmap split sphere look
      ctx.beginPath();
      ctx.arc(x, y, r, Math.PI, 0, false);
      ctx.closePath();
      ctx.fillStyle = `rgba(246,70,93,${0.35 + (1 - buyFrac) * 0.45})`;
      ctx.fill();

      ctx.beginPath();
      ctx.arc(x, y, r, 0, Math.PI, false);
      ctx.closePath();
      ctx.fillStyle = `rgba(2,192,118,${0.35 + buyFrac * 0.45})`;
      ctx.fill();

      // Split line
      ctx.beginPath();
      ctx.moveTo(x - r, y);
      ctx.lineTo(x + r, y);
      ctx.strokeStyle = 'rgba(12,16,22,0.55)';
      ctx.lineWidth = 1;
      ctx.stroke();

      ctx.beginPath();
      ctx.arc(x, y, r, 0, Math.PI * 2);
      ctx.strokeStyle = 'rgba(255,255,255,0.25)';
      ctx.lineWidth = 1;
      ctx.stroke();
    }

    // Volume subplot
    const volTop = padT + heatH + 6 * dpr;
    ctx.fillStyle = '#0a0e14';
    ctx.fillRect(padL, volTop, heatW, volH);
    const maxVol = Math.max(1, ...volBars.map((v) => v.totalUsd));
    const barW = Math.max(1, heatW / Math.max(volBars.length, 1) - 1);
    for (const v of volBars) {
      const tMs = v.sec * 1000;
      if (tMs < tMin || tMs > tMax) continue;
      const x = padL + ((tMs - tMin) / spanT) * heatW;
      const buyH = (v.buyUsd / maxVol) * (volH - 2);
      const sellH = (v.sellUsd / maxVol) * (volH - 2);
      ctx.fillStyle = 'rgba(2,192,118,0.7)';
      ctx.fillRect(x, volTop + volH - buyH, barW, buyH);
      ctx.fillStyle = 'rgba(246,70,93,0.7)';
      ctx.fillRect(x, volTop + volH - buyH - sellH, barW, sellH);
    }

    // Price axis labels
    ctx.fillStyle = '#848e9c';
    ctx.font = `${11 * dpr}px IBM Plex Mono, SF Mono, Menlo, monospace`;
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    for (let i = 0; i <= 6; i++) {
      const px = priceMax - (spanP * i) / 6;
      const y = padT + (heatH * i) / 6;
      ctx.fillText(fmtPrice(px, 2), padL - 6 * dpr, y);
    }

    // Footer label
    ctx.textAlign = 'left';
    ctx.textBaseline = 'top';
    ctx.fillStyle = '#5e6673';
    ctx.font = `${10 * dpr}px IBM Plex Mono, SF Mono, Menlo, monospace`;
    ctx.fillText(
      `L2 reconstructed · ${venue || '?'} ${symbol || ''} · not MBO`,
      padL,
      h - 12 * dpr,
    );

    // Store layout for hover
    el._layout = { padL, padT, heatW, heatH, priceMin, priceMax, tMin, tMax, dpr, volTop, volH };
  }

  function schedule() {
    if (raf) cancelAnimationFrame(raf);
    raf = requestAnimationFrame(() => {
      raf = 0;
      paint();
    });
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

    // Nearest depth sample
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

    // Nearest bubble
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
    // Depend on reactive inputs
    depthHistory;
    tape;
    windowSec;
    lastPrice;
    schedule();
  });

  onMount(() => {
    schedule();
    ro = new ResizeObserver(() => schedule());
    if (wrap) ro.observe(wrap);
    return () => {
      if (raf) cancelAnimationFrame(raf);
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
      <div class="tip" style={`left:${hover.x}px;top:${hover.y}px`} bind:this={tip}>
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
