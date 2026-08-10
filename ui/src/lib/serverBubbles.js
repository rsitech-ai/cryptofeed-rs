export function normalizeBubbleSnapshot(raw) {
  if (!raw || raw.schema_version !== 1 || !Array.isArray(raw.bubbles)) {
    return emptyBubbleSnapshot(null, 'bubble_payload_invalid');
  }
  const mode = raw.mode === 'delta' ? 'delta' : raw.mode === 'volume' ? 'volume' : null;
  const bubbles = raw.bubbles.flatMap((bubble) => {
    const price = Number(bubble?.anchor_price);
    const strength = Number(bubble?.strength);
    const endNs = Number(bubble?.candle_end_ns);
    if (!(price > 0) || !Number.isFinite(strength) || !Number.isFinite(endNs)) return [];
    return [{
      id: bubble.id,
      t: endNs / 1e6,
      price,
      strength,
      tier: ['f1', 'f2', 'f3'].includes(bubble.tier) ? bubble.tier : 'f1',
      shape: ['circle', 'square', 'diamond'].includes(bubble.shape) ? bubble.shape : 'circle',
      direction: ['buy', 'sell', 'neutral'].includes(bubble.direction) ? bubble.direction : 'neutral',
      visualSize: Math.max(4, Math.min(64, Number(bubble.visual_size) || 8)),
      phase: bubble.phase === 'final' ? 'final' : 'live',
      exactStrength: bubble.strength,
      exactDelta: bubble.delta,
      exactTotal: bubble.total_volume,
      server: true,
    }];
  });
  return {
    available: raw.status === 'live' || raw.status === 'degraded',
    status: raw.status,
    reason: raw.reason || null,
    mode,
    revision: Number.isSafeInteger(raw.revision) ? raw.revision : 0,
    bubbles,
  };
}

export function emptyBubbleSnapshot(mode = 'volume', reason = 'bubble_loading') {
  return { available: false, status: 'unavailable', reason, mode, revision: 0, bubbles: [] };
}
