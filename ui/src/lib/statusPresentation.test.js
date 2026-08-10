import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { statusPresentation } from './statusPresentation.js';

describe('statusPresentation', () => {
  it('marks local replay as offline and pauses live lifecycle claims', () => {
    assert.deepEqual(
      statusPresentation({
        replayMode: true,
        connected: true,
        status: { lifecycle: 'running' },
        streamMode: 'sse',
        streamReconnecting: false,
      }),
      {
        connectionLabel: 'offline replay',
        connectionOn: false,
        transportLabel: 'replay',
        lifecycleLabel: 'paused',
        showLiveStatus: false,
      },
    );
  });

  it('does not present stale lifecycle data as current after a status failure', () => {
    assert.deepEqual(
      statusPresentation({
        replayMode: false,
        connected: false,
        status: { lifecycle: 'running' },
        streamMode: 'poll',
        streamReconnecting: false,
      }),
      {
        connectionLabel: 'disconnected',
        connectionOn: false,
        transportLabel: 'poll',
        lifecycleLabel: 'unknown',
        showLiveStatus: false,
      },
    );
  });

  it('shows current lifecycle and reconnecting transport only with fresh status', () => {
    assert.deepEqual(
      statusPresentation({
        replayMode: false,
        connected: true,
        status: { lifecycle: 'running' },
        streamMode: 'sse',
        streamReconnecting: true,
      }),
      {
        connectionLabel: 'connected',
        connectionOn: true,
        transportLabel: 'SSE…',
        lifecycleLabel: 'running',
        showLiveStatus: true,
      },
    );
  });
});
