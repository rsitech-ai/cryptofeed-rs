<script>
  import { emptyDerivatives } from '../lib/derivatives.js';
  let { data = emptyDerivatives() } = $props();
  let latestLiq = $derived(data.liquidations?.[0] || null);
</script>

<section class="derivatives" aria-label="Exchange-reported crypto derivatives state">
  <div class="label">
    <strong>Derivatives</strong>
    <span>exchange-reported</span>
  </div>
  <div class="metric" title="Latest funding rate from this venue">
    <span>Funding</span>
    <strong class:stale={data.funding?.stale}>{data.funding?.rate ?? '—'}</strong>
  </div>
  <div class="metric" title="Latest open interest and change from prior retained sample">
    <span>Open interest</span>
    <strong class:stale={data.openInterest?.stale}>{data.openInterest?.quantity ?? '—'}</strong>
    <small>{data.openInterest?.change != null ? `Δ ${data.openInterest.change}` : 'no prior sample'}</small>
  </div>
  <div class="metric" title="Fresh compatible funding rates with the exact same native symbol">
    <span>Funding divergence</span>
    <strong>{data.divergence?.spread ?? '—'}</strong>
    <small>{data.divergence ? `${data.divergence.compatible_venues} venues` : 'needs 2 compatible venues'}</small>
  </div>
  <div class="metric" title="Latest exchange-reported liquidation; never inferred from trades">
    <span>Liquidations</span>
    <strong>{data.liquidations?.length ?? 0} retained</strong>
    <small>{latestLiq ? `${latestLiq.side} ${latestLiq.quantity} @ ${latestLiq.price}` : 'none reported'}</small>
  </div>
</section>

<style>
  .derivatives { min-height: 42px; display: grid; grid-template-columns: 150px repeat(4,minmax(120px,1fr)); border-top:1px solid var(--border); background:#0d1218; font-family:var(--mono); }
  .label,.metric { padding:5px 9px; border-right:1px solid var(--border); display:flex; flex-direction:column; justify-content:center; gap:2px; min-width:0; }
  .label strong,.metric span { font-size:.55rem; text-transform:uppercase; letter-spacing:.06em; color:var(--muted); }
  .label span,.metric small { font-size:.52rem; color:var(--muted); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
  .metric strong { font-size:.68rem; color:#e7ebf1; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; font-variant-numeric:tabular-nums; }
  .metric strong.stale { color:#f0b90b; }
  @media(max-width:980px){.derivatives{grid-template-columns:132px repeat(4,minmax(120px,1fr));overflow-x:auto;}}
</style>
