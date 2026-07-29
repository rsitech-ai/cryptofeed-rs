<script>
  import { fmtUtcClock } from '../lib/format.js';

  let {
    status = null,
    error = '',
    connected = false,
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
  }

  .left,
  .right {
    display: flex;
    align-items: center;
    gap: 0.45rem;
    min-width: 0;
  }

  .dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: var(--ask);
  }
  .dot.on {
    background: var(--bid);
  }
  .dot.off {
    background: var(--ask);
  }

  .sep {
    opacity: 0.4;
  }

  .err {
    color: var(--ask);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 28rem;
  }
</style>
