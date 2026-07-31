<script>
  import { ALERT_AUTO_DISMISS_MS, ALERT_VISIBLE_MAX } from '../lib/alerts.js';

  let {
    alerts = [],
    onDismiss = () => {},
    autoDismissMs = ALERT_AUTO_DISMISS_MS,
    maxVisible = ALERT_VISIBLE_MAX,
  } = $props();

  let visible = $derived(
    (alerts || []).filter((a) => !a.dismissed).slice(-maxVisible),
  );

  // Auto-dismiss each visible toast after ~5s from creation; X still works.
  $effect(() => {
    const timers = [];
    const now = Date.now();
    for (const a of visible) {
      const age = now - (Number(a.ts) || now);
      const remaining = Math.max(0, autoDismissMs - age);
      timers.push(setTimeout(() => onDismiss(a.id), remaining));
    }
    return () => {
      for (const t of timers) clearTimeout(t);
    };
  });
</script>

<div class="toasts" aria-live="polite">
  {#each visible as a (a.id)}
    <div class="toast" class:bps={a.kind === 'bps'} class:lag={a.kind === 'lag'}>
      <div class="body">
        <strong>{a.title}</strong>
        <span>{a.body}</span>
      </div>
      <button type="button" class="x" onclick={() => onDismiss(a.id)} aria-label="Dismiss">×</button>
    </div>
  {/each}
</div>

<style>
  .toasts {
    position: fixed;
    top: 3.5rem;
    right: 0.75rem;
    z-index: 100;
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
    max-width: 22rem;
    pointer-events: none;
  }

  .toast {
    display: flex;
    align-items: flex-start;
    gap: 0.5rem;
    padding: 0.45rem 0.55rem;
    background: var(--panel);
    border: 1px solid var(--border);
    border-left: 3px solid var(--accent);
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
    pointer-events: auto;
    font-size: 0.72rem;
    animation: toast-in 0.18s ease-out;
  }

  .toast.bps {
    border-left-color: var(--ask);
    background: rgba(246, 70, 93, 0.1);
  }

  .toast.lag {
    border-left-color: #fb923c;
  }

  .body {
    display: flex;
    flex-direction: column;
    gap: 0.15rem;
    flex: 1;
    font-family: var(--mono);
  }

  .body strong {
    color: var(--text);
    font-size: 0.68rem;
  }

  .body span {
    color: var(--text-dim);
    font-size: 0.65rem;
  }

  .x {
    background: transparent;
    border: none;
    color: var(--muted);
    cursor: pointer;
    font-size: 1rem;
    line-height: 1;
    padding: 0;
  }

  .x:hover {
    color: var(--text);
  }

  @keyframes toast-in {
    from {
      opacity: 0;
      transform: translateX(0.4rem);
    }
    to {
      opacity: 1;
      transform: translateX(0);
    }
  }
</style>
