const KINDS = new Set(['naked', 'reaction_high', 'reaction_low', 'top_day', 'top_week']);
const STATES = new Set(['active', 'touched']);
const DIRECTIONS = new Set(['buy', 'sell', 'neutral']);
const TIERS = new Set(['f1', 'f2', 'f3']);

export function normalizeStructuralLevelSnapshot(raw) {
  if (!raw || raw.schema_version !== 1 || !Array.isArray(raw.levels)) {
    return emptyStructuralLevelSnapshot('structural_level_payload_invalid');
  }
  const levels = raw.levels.flatMap((level) => {
    const price = Number(level?.price);
    const strength = Number(level?.strength);
    const createdNs = Number(level?.created_at_ns);
    if (
      !KINDS.has(level?.kind)
      || !STATES.has(level?.state)
      || !(price > 0)
      || !Number.isFinite(strength)
      || !Number.isFinite(createdNs)
    ) return [];
    const nsToMs = (value) => value == null ? null : Number(value) / 1e6;
    return [{
      id: level.id,
      kind: level.kind,
      state: level.state,
      sourceBubbleId: level.source_bubble_id,
      direction: DIRECTIONS.has(level.direction) ? level.direction : 'neutral',
      tier: TIERS.has(level.tier) ? level.tier : 'f1',
      price,
      strength,
      exactPrice: level.price,
      exactStrength: level.strength,
      createdAt: nsToMs(level.created_at_ns),
      touchedAt: nsToMs(level.touched_at_ns),
      windowStart: nsToMs(level.window_start_ns),
      expiresAt: nsToMs(level.expires_at_ns),
      server: true,
    }];
  });
  return {
    available: raw.status === 'live' || raw.status === 'degraded',
    status: raw.status,
    reason: raw.reason || null,
    revision: Number.isSafeInteger(raw.revision) ? raw.revision : 0,
    levels,
  };
}

export function emptyStructuralLevelSnapshot(reason = 'structural_level_loading') {
  return { available: false, status: 'unavailable', reason, revision: 0, levels: [] };
}
