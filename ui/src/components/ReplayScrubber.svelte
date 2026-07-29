<script>
  import { fetchJson } from '../lib/api.js';
  import { normalizeReplayEntries } from '../lib/contracts.js';

  let {
    replayMode = false,
    onReplayMode = () => {},
    onEntries = () => {},
    onPosition = () => {},
  } = $props();

  let apiAvailable = $state(null);
  let entries = $state([]);
  let replayFiles = $state([]);
  let selectedFile = $state('');
  let position = $state(0);
  let playing = $state(false);
  let error = $state('');
  let playTimer = null;

  $effect(() => {
    probeApi();
    return () => {
      if (playTimer) clearInterval(playTimer);
    };
  });

  async function probeApi() {
    try {
      const data = await fetchJson('/v1/replay/files');
      replayFiles = Array.isArray(data.files) ? data.files : [];
      selectedFile = replayFiles[0]?.name || '';
      apiAvailable = true;
    } catch {
      apiAvailable = false;
    }
  }

  async function loadFromApi(name) {
    error = '';
    try {
      const data = await fetchJson(`/v1/replay?file=${encodeURIComponent(name)}`);
      if (data.error) throw new Error(data.error);
      entries = normalizeReplayEntries(data.entries || data.events || []);
      position = 0;
      onReplayMode(true);
      emitUpTo(0);
    } catch (e) {
      error = String(e.message || e);
    }
  }

  async function onFilePick(ev) {
    const file = ev.currentTarget.files?.[0];
    if (!file) return;
    error = '';
    try {
      const text = await file.text();
      const lines = text.trim().split('\n').filter(Boolean);
      entries = normalizeReplayEntries(lines.map((l) => JSON.parse(l)));
      if (!entries.length) throw new Error('no trade or quote entries found');
      position = 0;
      onReplayMode(true);
      emitUpTo(0);
    } catch (e) {
      error = 'Failed to parse JSONL: ' + String(e.message || e);
    }
  }

  function emitUpTo(pos) {
    const slice = entries.slice(0, pos + 1);
    onEntries(slice);
    onPosition(pos, entries.length);
  }

  function scrub(ev) {
    position = Number(ev.currentTarget.value);
    emitUpTo(position);
  }

  function togglePlay() {
    playing = !playing;
    if (playTimer) clearInterval(playTimer);
    if (playing && entries.length) {
      playTimer = setInterval(() => {
        if (position >= entries.length - 1) {
          playing = false;
          clearInterval(playTimer);
          return;
        }
        position += 1;
        emitUpTo(position);
      }, 100);
    }
  }

  function exitReplay() {
    playing = false;
    if (playTimer) clearInterval(playTimer);
    entries = [];
    position = 0;
    onReplayMode(false);
  }
</script>

<section class="replay" class:active={replayMode}>
  <div class="head">
    <span class="title">Replay</span>
    {#if replayMode}
      <span class="badge">OFFLINE</span>
    {/if}
    <button type="button" class="exit" onclick={exitReplay} disabled={!replayMode}>Exit</button>
  </div>

  {#if apiAvailable === false}
    <p class="hint">
      /v1/replay not available — load JSONL locally (MFNE events one per line).
      Live polls pause in replay mode.
    </p>
  {:else if apiAvailable === null}
    <p class="hint">Checking replay API…</p>
  {:else}
    <p class="hint">Replay API available. Choose a daemon file or load local JSONL.</p>
  {/if}

  <div class="controls">
    <label class="file-btn">
      JSONL
      <input
        type="file"
        accept=".jsonl,.json,.log"
        aria-label="Load replay JSONL file"
        onchange={onFilePick}
      />
    </label>
    {#if apiAvailable && replayFiles.length}
      <select bind:value={selectedFile} aria-label="Daemon replay file">
        {#each replayFiles as file}
          <option value={file.name}>{file.name}</option>
        {/each}
      </select>
      <button type="button" onclick={() => loadFromApi(selectedFile)} disabled={!selectedFile}>
        Load
      </button>
    {/if}
    <button type="button" onclick={togglePlay} disabled={!entries.length}>
      {playing ? 'Pause' : 'Play'}
    </button>
    {#if entries.length}
      <input
        type="range"
        min="0"
        max={Math.max(entries.length - 1, 0)}
        value={position}
        oninput={scrub}
        class="scrub"
      />
      <span class="pos">{position + 1}/{entries.length}</span>
    {/if}
  </div>

  {#if error}
    <p class="err">{error}</p>
  {/if}
</section>

<style>
  .replay {
    border-top: 1px solid var(--border);
    padding: 0.35rem 0.5rem;
    background: var(--panel-2);
    flex-shrink: 0;
  }

  .replay.active {
    border-color: rgba(240, 185, 11, 0.35);
    background: rgba(240, 185, 11, 0.06);
  }

  .head {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    margin-bottom: 0.25rem;
  }

  .title {
    font-size: 0.65rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim);
  }

  .badge {
    font-family: var(--mono);
    font-size: 0.55rem;
    color: var(--accent);
    border: 1px solid rgba(240, 185, 11, 0.4);
    padding: 0.02rem 0.25rem;
  }

  .exit {
    margin-left: auto;
    background: transparent;
    border: 1px solid var(--border);
    color: var(--muted);
    font-family: var(--mono);
    font-size: 0.62rem;
    padding: 0.1rem 0.35rem;
    cursor: pointer;
  }

  .hint {
    font-family: var(--mono);
    font-size: 0.58rem;
    color: var(--muted);
    margin: 0 0 0.3rem;
    line-height: 1.35;
  }

  .controls {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    flex-wrap: wrap;
  }

  .file-btn {
    position: relative;
    overflow: hidden;
    font-family: var(--mono);
    font-size: 0.62rem;
    color: var(--text);
    border: 1px solid var(--border);
    padding: 0.12rem 0.4rem;
    cursor: pointer;
    background: var(--bg);
  }

  .file-btn input {
    position: absolute;
    inset: 0;
    width: 100%;
    height: 100%;
    opacity: 0;
    cursor: pointer;
  }

  .file-btn:focus-within {
    outline: 2px solid var(--accent);
    outline-offset: 2px;
  }

  .controls button,
  .controls select {
    background: transparent;
    border: 1px solid var(--border);
    color: var(--muted);
    font-family: var(--mono);
    font-size: 0.62rem;
    padding: 0.12rem 0.4rem;
    cursor: pointer;
  }

  .controls button:disabled {
    opacity: 0.4;
    cursor: not-allowed;
  }

  .scrub {
    flex: 1;
    min-width: 6rem;
    accent-color: var(--accent);
  }

  .pos {
    font-family: var(--mono);
    font-size: 0.62rem;
    color: var(--muted);
  }

  .err {
    color: var(--ask);
    font-family: var(--mono);
    font-size: 0.62rem;
    margin: 0.25rem 0 0;
  }
</style>
