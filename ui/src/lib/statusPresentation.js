/**
 * Derive only status claims that are proven by the current UI mode and the
 * latest successful status request.
 * @param {{ replayMode: boolean, connected: boolean, status: object | null, streamMode: string, streamReconnecting: boolean }} input
 */
export function statusPresentation(input) {
  if (input.replayMode) {
    return {
      connectionLabel: 'offline replay',
      connectionOn: false,
      transportLabel: 'replay',
      lifecycleLabel: 'paused',
      showLiveStatus: false,
    };
  }

  const showLiveStatus = Boolean(input.connected && input.status);
  const transportLabel = input.streamMode === 'sse'
    ? (input.streamReconnecting ? 'SSE…' : 'SSE')
    : 'poll';

  return {
    connectionLabel: input.connected ? 'connected' : 'disconnected',
    connectionOn: Boolean(input.connected),
    transportLabel,
    lifecycleLabel: showLiveStatus ? String(input.status.lifecycle) : 'unknown',
    showLiveStatus,
  };
}
