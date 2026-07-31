<script>
  import { fmtUsd } from '../lib/format.js';
  import { computeCvd, sparkPathTimed } from '../lib/orderflow.js';

  let {
    tape = [],
    imbalanceHistory = [],
    pulseHistory = [],
    windowSec = 300,
    /** @type {{ fromSec: number, toSec: number }|null} */
    visibleRange = null,
    spikeThreshold = 72,
    showVolumeHist = true,
  } = $props();

  let tip = $state('');
  let tipX = $state(0);
  let tipY = $state(0);

  let range = $derived.by(() => {
    if (
      visibleRange &&
      Number.isFinite(visibleRange.fromSec) &&
      Number.isFinite(visibleRange.toSec) &&
      visibleRange.toSec > visibleRange.fromSec
    ) {
      return { fromSec: visibleRange.fromSec, toSec: visibleRange.toSec };
    }
    const toSec = Math.floor(Date.now() / 1000);
    return { fromSec: toSec - Math.max(1, windowSec), toSec };
  });

  let winSec = $derived(Math.max(1, range.toSec - range.fromSec));

  let cvd = $derived(
    computeCvd(tape, {
      windowSec: winSec,
      nowSec: range.toSec,
    }),
  );

  let imbPts = $derived.by(() => {
    const fromMs = range.fromSec * 1000;
    const toMs = range.toSec * 1000;
    return (imbalanceHistory || [])
      .filter((p) => p && Number.isFinite(p.t) && p.t >= fromMs && p.t <= toMs + 500)
      .map((p) => ({ t: p.t / 1000, v: Number(p.imbalancePct) }));
  });

  let pulsePts = $derived.by(() => {
    const fromMs = range.fromSec * 1000;
    const toMs = range.toSec * 1000;
    return (pulseHistory || [])
      .filter((p) => p && Number.isFinite(p.t) && p.t >= fromMs && p.t <= toMs + 500)
      .map((p) => ({ t: p.t / 1000, v: Number(p.score) }));
  });

  let cvdPts = $derived(
    (cvd.points || []).map((p) => ({ t: p.sec, v: p.cvd })),
  );

  let imbSpark = $derived(
    sparkPathTimed(imbPts, { w: 200, h: 36, fromSec: range.fromSec, toSec: range.toSec }),
  );
  let cvdSpark = $derived(
    sparkPathTimed(cvdPts, { w: 200, h: 36, fromSec: range.fromSec, toSec: range.toSec }),
  );
  let pulseSpark = $derived(
    sparkPathTimed(pulsePts, { w: 200, h: 36, fromSec: range.fromSec, toSec: range.toSec }),
  );

  let hist = $derived((cvd.histogram || []).slice(-96));
  let histMax = $derived(
    Math.max(1, ...hist.map((h) => Math.max(h.buyUsd || 0, h.sellUsd || 0))),
  );

  let lastImb = $derived(imbPts.length ? imbPts[imbPts.length - 1].v : null);
  let lastPulse = $derived(pulsePts.length ? pulsePts[pulsePts.length - 1].v : null);

  /** @param {MouseEvent} e @param {string} text */
  function showTip(e, text) {
    tip = text;
    tipX = e.clientX + 12;
    tipY = e.clientY + 12;
  }

  function hideTip() {
    tip = '';
  }
</script>

<section class="cas" aria-label="Chart flow and pulse analytics">
  <div class="cas-row sparks">
    <div
      class="spark pulse"
      role="img"
      aria-label="Pulse score history"
      title="Pulse score history"
      onmouseenter={(e) =>
        showTip(
          e,
          lastPulse != null
            ? `Pulse ${Number(lastPulse).toFixed(0)} · alert ≥${spikeThreshold} · ${Math.round(winSec)}s`
            : 'Pulse…',
        )}
      onmousemove={(e) =>
        showTip(
          e,
          lastPulse != null
            ? `Pulse ${Number(lastPulse).toFixed(0)} · alert ≥${spikeThreshold} · ${Math.round(winSec)}s`
            : 'Pulse…',
        )}
      onmouseleave={hideTip}
    >
      <span class="spark-lbl">Pulse</span>
      {#if pulseSpark}
        <svg viewBox="0 0 200 36" preserveAspectRatio="none">
          <line
            x1="0"
            y1={36 - (spikeThreshold / 100) * 32 - 2}
            x2="200"
            y2={36 - (spikeThreshold / 100) * 32 - 2}
            stroke="rgba(246,70,93,0.45)"
            stroke-dasharray="2,2"
            stroke-width="0.7"
          />
          <path d={pulseSpark} fill="none" stroke="var(--accent)" stroke-width="1.6" />
        </svg>
      {:else}
        <span class="muted">pulse…</span>
      {/if}
    </div>

    <div
      class="spark imb"
      role="img"
      aria-label="Depth imbalance sparkline"
      title="Depth imbalance"
      onmouseenter={(e) =>
        showTip(e, lastImb != null ? `Imbalance ${Number(lastImb).toFixed(1)}%` : 'Imbalance…')}
      onmousemove={(e) =>
        showTip(e, lastImb != null ? `Imbalance ${Number(lastImb).toFixed(1)}%` : 'Imbalance…')}
      onmouseleave={hideTip}
    >
      <span class="spark-lbl">Imb</span>
      {#if imbSpark}
        <svg viewBox="0 0 200 36" preserveAspectRatio="none">
          <path d={imbSpark} fill="none" stroke="var(--accent)" stroke-width="1.5" />
        </svg>
      {:else}
        <span class="muted">imb…</span>
      {/if}
    </div>

    <div
      class="spark cvd"
      role="img"
      aria-label="CVD sparkline"
      title="CVD"
      onmouseenter={(e) => showTip(e, `CVD ${fmtUsd(cvd.cvd)}`)}
      onmousemove={(e) => showTip(e, `CVD ${fmtUsd(cvd.cvd)}`)}
      onmouseleave={hideTip}
    >
      <span class="spark-lbl">CVD</span>
      {#if cvdSpark}
        <svg viewBox="0 0 200 36" preserveAspectRatio="none">
          <path
            d={cvdSpark}
            fill="none"
            stroke={cvd.cvd >= 0 ? 'var(--bid)' : 'var(--ask)'}
            stroke-width="1.5"
          />
        </svg>
      {:else}
        <span class="muted">cvd…</span>
      {/if}
    </div>
  </div>

  {#if showVolumeHist}
    <div class="hist" aria-label="Buy vs sell histogram">
      {#each hist as h, i (i)}
        <button
          type="button"
          class="hcol"
          title={`Buy ${fmtUsd(h.buyUsd)} · Sell ${fmtUsd(h.sellUsd)}`}
          onmouseenter={(e) => showTip(e, `Buy ${fmtUsd(h.buyUsd)} · Sell ${fmtUsd(h.sellUsd)}`)}
          onmousemove={(e) => showTip(e, `Buy ${fmtUsd(h.buyUsd)} · Sell ${fmtUsd(h.sellUsd)}`)}
          onmouseleave={hideTip}
        >
          <div class="buy" style={`height:${Math.max(2, (h.buyUsd / histMax) * 100)}%`}></div>
          <div class="sell" style={`height:${Math.max(2, (h.sellUsd / histMax) * 100)}%`}></div>
        </button>
      {:else}
        <span class="muted hist-empty">buy/sell hist…</span>
      {/each}
    </div>
  {/if}

  {#if tip}
    <div class="cas-tip" style={`left:${tipX}px;top:${tipY}px`} role="tooltip">{tip}</div>
  {/if}
</section>

<style>
  .cas {
    flex-shrink: 0;
    border-top: 1px solid var(--border);
    background: var(--panel-2);
    padding: 0.22rem 0.4rem 0.28rem;
    display: flex;
    flex-direction: column;
    gap: 0.18rem;
    position: relative;
  }

  .cas-row.sparks {
    display: grid;
    grid-template-columns: minmax(0, 1.2fr) minmax(0, 1fr) minmax(0, 1fr);
    gap: 0.2rem;
  }

  .spark {
    position: relative;
    height: 36px;
    min-height: 32px;
    background: var(--panel);
    border: 1px solid var(--border);
    cursor: help;
  }

  .spark-lbl {
    position: absolute;
    top: 1px;
    left: 3px;
    z-index: 1;
    font-size: 0.42rem;
    font-family: var(--mono);
    color: var(--muted);
    text-transform: uppercase;
    pointer-events: none;
  }

  .spark svg {
    width: 100%;
    height: 100%;
    display: block;
  }

  .muted {
    color: var(--muted);
    font-size: 0.52rem;
    font-family: var(--mono);
    display: flex;
    align-items: center;
    justify-content: center;
    height: 100%;
  }

  .hist {
    display: flex;
    align-items: flex-end;
    gap: 1px;
    height: 44px;
    min-height: 36px;
    background: var(--panel);
    border: 1px solid var(--border);
    padding: 2px;
  }

  .hist-empty {
    width: 100%;
  }

  .hcol {
    flex: 1 1 0;
    display: flex;
    flex-direction: column-reverse;
    min-width: 2px;
    max-width: 10px;
    height: 100%;
    padding: 0;
    border: none;
    background: transparent;
    cursor: help;
  }

  .hcol .buy {
    background: rgba(2, 192, 118, 0.65);
    width: 100%;
  }
  .hcol .sell {
    background: rgba(246, 70, 93, 0.65);
    width: 100%;
  }

  .cas-tip {
    position: fixed;
    z-index: 80;
    pointer-events: none;
    max-width: 22rem;
    padding: 0.25rem 0.4rem;
    background: rgba(18, 22, 28, 0.96);
    border: 1px solid rgba(240, 185, 11, 0.35);
    border-radius: 2px;
    color: var(--text);
    font-family: var(--mono);
    font-size: 0.55rem;
    box-shadow: 0 4px 16px rgba(0, 0, 0, 0.45);
  }

  @media (max-width: 720px) {
    .cas-row.sparks {
      grid-template-columns: 1fr;
    }
  }
</style>
