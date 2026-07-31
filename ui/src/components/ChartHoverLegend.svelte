<script>
  /**
   * Dense top-left hover table for the multi-pane crosshair.
   * @type {{
   *   legend: {
   *     timeLabel: string,
   *     venues: Array<{ venue: string, color: string, text: string }>,
   *     indicators: Array<{ id: string, label: string, color: string, text: string }>,
   *   }|null,
   * }}
   */
  let { legend = null } = $props();
</script>

{#if legend}
  <div class="hover-legend" role="status" aria-live="polite">
    <div class="hl-time">{legend.timeLabel}</div>
    {#if legend.venues.length}
      <div class="hl-block">
        {#each legend.venues as row (row.venue)}
          <div class="hl-row">
            <span class="swatch" style={`background:${row.color}`}></span>
            <span class="name">{row.venue}</span>
            <span class="val" style={`color:${row.color}`}>{row.text}</span>
          </div>
        {/each}
      </div>
    {/if}
    {#if legend.indicators.length}
      <div class="hl-block indicators">
        {#each legend.indicators as row (row.id)}
          <div class="hl-row">
            <span class="name muted">{row.label}</span>
            <span class="val" style={`color:${row.color}`}>{row.text}</span>
          </div>
        {/each}
      </div>
    {/if}
  </div>
{/if}

<style>
  .hover-legend {
    position: absolute;
    top: 8px;
    left: 8px;
    z-index: 8;
    min-width: 9.5rem;
    max-width: min(16rem, 46%);
    max-height: min(70%, 22rem);
    overflow: auto;
    padding: 0.35rem 0.45rem;
    border: 1px solid rgba(71, 77, 87, 0.85);
    border-radius: 3px;
    background: rgba(15, 19, 24, 0.92);
    box-shadow: 0 4px 14px rgba(0, 0, 0, 0.45);
    font-family: var(--mono, 'IBM Plex Mono', SF Mono, Menlo, Consolas, monospace);
    font-size: 0.62rem;
    line-height: 1.25;
    color: var(--text, #eaecef);
    pointer-events: none;
    backdrop-filter: blur(4px);
  }

  .hl-time {
    color: var(--muted, #848e9c);
    margin-bottom: 0.25rem;
    font-variant-numeric: tabular-nums;
    letter-spacing: 0.01em;
  }

  .hl-block {
    display: flex;
    flex-direction: column;
    gap: 0.08rem;
  }

  .hl-block.indicators {
    margin-top: 0.3rem;
    padding-top: 0.28rem;
    border-top: 1px solid rgba(71, 77, 87, 0.55);
  }

  .hl-row {
    display: grid;
    grid-template-columns: 8px minmax(0, 1fr) auto;
    gap: 0.28rem;
    align-items: baseline;
  }

  .hl-block.indicators .hl-row {
    grid-template-columns: minmax(0, 1fr) auto;
  }

  .swatch {
    width: 7px;
    height: 7px;
    border-radius: 1px;
    align-self: center;
  }

  .name {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    color: var(--text-dim, #b7bdc6);
  }

  .name.muted {
    color: var(--muted, #848e9c);
    text-transform: uppercase;
    letter-spacing: 0.03em;
    font-size: 0.58rem;
  }

  .val {
    font-variant-numeric: tabular-nums;
    font-weight: 600;
    text-align: right;
    white-space: nowrap;
  }
</style>
