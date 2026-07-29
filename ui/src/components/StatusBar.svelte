<script>
  import { fmtUtcClock } from '../lib/format.js';

  let {
    status = null,
    error = '',
    connected = false,
    streamMode = 'poll',
    streamReconnecting = false,
    venueHealth = [],
  } = $props();

  let clock = $state(fmtUtcClock());

  $effect(() => {
    const id = setInterval(() => {
      clock = fmtUtcClock();
    }, 1000);
    return () => clearInterval(id);
  });

  let liveVenues = $derived((status?.venues || []).filter((v) => v.live).length);
  let totalVenues = $derived((status?.venues || []).length);
</script>

<footer class="status">
  <div class="left">
    <span class="dot" class:on={connected} class:off={!connected}></span>
    <span>{connected ? 'connected' : 'disconnected'}</span>
    <span class="sep">|</span>
    <span title={streamReconnecting ? 'SSE reconnecting (UI stays mounted)' : ''}>
      {streamMode === 'sse' ? (streamReconnecting ? 'SSE…' : 'SSE') : 'poll'}
    </span>
    {#if status}
      <span class="sep">|</span>
      <span>lifecycle {status.lifecycle}</span>
      <span class="sep">|</span>
      <span>venues {liveVenues}/{totalVenues} live</span>
      <span class="sep">|</span>
      <span>uptime {status.uptime_secs}s</span>
    {/if}
    {#if error}
      <span class="sep">|</span>
      <span class="err">{error}</span>
    {/if}
  </div>

  {#if venueHealth.length}
    <div class="health-strip" title="Per-venue feed health">
      {#each venueHealth as v}
        <span class="vh" class:bad={v.bad} title="{v.venue}: reconnects={v.reconnects ?? 0} gaps={v.gaps ?? 0} invalidations={v.invalidations ?? 0} lag={v.lagMs ?? '—'}ms">
          <span class="vid">{v.venue}</span>
          {#if v.reconnects}<span class="tag">R{v.reconnects}</span>{/if}
          {#if v.gaps}<span class="tag warn">G{v.gaps}</span>{/if}
          {#if v.invalidations}<span class="tag warn">I{v.invalidations}</span>{/if}
          {#if v.lagMs != null}<span class="tag" class:bad={v.lagMs > 2000}>{v.lagMs}ms</span>{/if}
        </span>
      {/each}
    </div>
  {/if}

  <div class="right">
    <span>marketfeed</span>
    <span class="sep">|</span>
    <span>{clock}</span>
  </div>
</footer>

<style>
  .status {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 1rem;
    padding: 0.2rem 0.6rem;
    background: #0a0c0f;
    border-top: 1px solid var(--border);
    font-family: var(--mono);
    font-size: 0.65rem;
    color: var(--muted);
    flex-shrink: 0;
    min-height: 1.4rem;
  }

  .left,
  .right {
    display: flex;
    align-items: center;
    gap: 0.45rem;
    min-width: 0;
    flex-shrink: 0;
  }

  .health-strip {
    display: flex;
    gap: 0.35rem;
    overflow-x: auto;
    flex: 1;
    min-width: 0;
    padding: 0 0.25rem;
  }

  .vh {
    display: inline-flex;
    align-items: center;
    gap: 0.15rem;
    border: 1px solid var(--border);
    padding: 0.02rem 0.25rem;
    white-space: nowrap;
    min-width: 6.5rem;
  }

  .vh.bad {
    border-color: rgba(246, 70, 93, 0.4);
  }

  .vid {
    color: var(--text-dim);
    max-width: 5rem;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .tag {
    color: var(--muted);
    font-size: 0.58rem;
    min-width: 2.2rem;
    text-align: right;
    font-variant-numeric: tabular-nums;
  }

  .tag.warn {
    color: #fb923c;
  }

  .tag.bad {
    color: var(--ask);
  }

  .dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: var(--ask);
  }
  .dot.on { background: var(--bid); }
  .dot.off { background: var(--ask); }

  .sep { opacity: 0.4; }

  .err {
    color: var(--ask);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 16rem;
  }
</style>
