<script>
  import { emptyMarketProfile } from '../lib/marketProfile.js';
  import { fmtPrice, fmtQty } from '../lib/format.js';

  let {
    profile = emptyMarketProfile(),
    replayMode = false,
    profileBasis = 'volume',
    onBasis = () => {},
  } = $props();

  let rows = $derived([
    ['VAH', profile.vah, 'Value Area High'],
    ['VAL', profile.val, 'Value Area Low'],
    ['POC', profile.poc, 'Point of Control'],
    ['Range', profile.range, 'Session high minus low'],
    ['Volume', profile.volume, 'Exact traded base quantity'],
    ['TPO', profile.tpoCount, 'Total Time Price Opportunities'],
    ['Rotation', profile.rotationFactor, 'Rotation factor'],
  ]);

  function reasonLabel(reason) {
    if (replayMode) return 'Replay profile unavailable';
    if (reason === 'no_profile_trades') return 'Waiting for session trades';
    if (reason === 'catalog_not_authoritative') return 'Exact venue grid unavailable';
    if (reason === 'catalog_metadata_unavailable') return 'Instrument metadata unavailable';
    if (reason === 'venue_not_registered') return 'Venue session is starting';
    if (reason === 'profile_schema_unsupported') return 'Profile schema unsupported';
    return 'Market Profile unavailable';
  }

  function displayValue(label, value) {
    if (value == null || value === '') return '—';
    if (label === 'TPO' || label === 'Rotation') return String(value);
    if (label === 'Volume') return fmtQty(value);
    return fmtPrice(value, 2);
  }
</script>

<section
  class="profile-strip"
  class:degraded={profile.status === 'degraded'}
  aria-label="Current UTC session Market Profile"
>
  <div class="profile-context">
    <div class="profile-heading">
      <span class="eyebrow">Market Profile</span>
      <div class="basis-switch" aria-label="Value area basis">
        <button
          type="button"
          class:active={profileBasis === 'volume'}
          aria-pressed={profileBasis === 'volume'}
          onclick={() => onBasis('volume')}>VOL</button
        >
        <button
          type="button"
          class:active={profileBasis === 'tpo'}
          aria-pressed={profileBasis === 'tpo'}
          onclick={() => onBasis('tpo')}>TPO</button
        >
      </div>
    </div>
    {#if profile.available}
      <span class="basis">UTC · {profile.basis === 'tpo' ? 'TPO' : 'VOL'} · {(profile.valueAreaBps ?? 7000) / 100}%</span>
    {:else}
      <span class="unavailable">{reasonLabel(profile.reason)}</span>
    {/if}
  </div>
  <div class="metrics" aria-live="polite">
    {#each rows as row}
      <div class="metric" title={profile.available && row[1] != null ? `${row[2]} · exact ${row[1]}` : row[2]}>
        <span>{row[0]}</span>
        <strong>{profile.available && row[1] != null ? displayValue(row[0], row[1]) : '—'}</strong>
      </div>
    {/each}
  </div>
  {#if profile.status === 'degraded'}
    <span class="health" title={profile.reason || 'The last profile update was rejected'}>degraded</span>
  {/if}
</section>

<style>
  .profile-strip {
    min-height: 42px;
    display: grid;
    grid-template-columns: minmax(150px, 0.8fr) minmax(520px, 4fr) auto;
    align-items: stretch;
    border-top: 1px solid var(--border);
    background: #10141a;
    color: var(--text);
    font-family: var(--mono);
  }

  .profile-strip.degraded { box-shadow: inset 2px 0 0 #f0b90b; }

  .profile-context {
    display: flex;
    flex-direction: column;
    justify-content: center;
    gap: 2px;
    padding: 5px 10px;
    border-right: 1px solid var(--border);
    min-width: 0;
  }

  .profile-heading {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 6px;
  }

  .basis-switch {
    display: inline-flex;
    border: 1px solid var(--border);
    border-radius: 3px;
    overflow: hidden;
  }

  .basis-switch button {
    border: 0;
    border-right: 1px solid var(--border);
    padding: 2px 4px;
    background: transparent;
    color: var(--muted);
    font: 500 0.48rem/1 var(--mono);
    cursor: pointer;
  }

  .basis-switch button:last-child { border-right: 0; }
  .basis-switch button:hover { color: var(--text); }
  .basis-switch button:focus-visible { outline: 1px solid var(--accent); outline-offset: -1px; }
  .basis-switch button.active { background: #26313f; color: #f2f5f8; }

  .eyebrow {
    font-size: 0.61rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: #d7dde7;
  }

  .basis,
  .unavailable {
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
    font-size: 0.55rem;
    color: var(--muted);
  }

  .metrics {
    display: grid;
    grid-template-columns: repeat(7, minmax(62px, 1fr));
  }

  .metric {
    min-width: 0;
    padding: 5px 8px 4px;
    border-right: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    justify-content: center;
    gap: 2px;
  }

  .metric span {
    font-size: 0.52rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: var(--muted);
  }

  .metric strong {
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
    font-size: 0.68rem;
    font-weight: 600;
    color: #e7ebf1;
    font-variant-numeric: tabular-nums;
  }

  .health {
    align-self: center;
    margin: 0 8px;
    font-size: 0.52rem;
    color: #f0b90b;
    text-transform: uppercase;
  }

  @media (max-width: 980px) {
    .profile-strip {
      grid-template-columns: 132px minmax(490px, 1fr) auto;
      overflow-x: auto;
    }
  }
</style>
