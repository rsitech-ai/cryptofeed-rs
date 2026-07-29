const apiBase = '';

export async function fetchJson(path) {
  const res = await fetch(`${apiBase}${path}`);
  if (!res.ok) {
    let detail = '';
    try {
      const body = await res.json();
      detail = body?.error ? `: ${body.error}` : '';
    } catch {
      /* ignore */
    }
    throw new Error(`${path} → ${res.status}${detail}`);
  }
  return res.json();
}

export function bookQuery(venue, symbol, depth = 20) {
  const q = new URLSearchParams({
    venue,
    symbol,
    depth: String(depth),
  });
  return `/v1/books?${q}`;
}

/**
 * @param {string} venue
 * @param {string} symbol
 * @param {number} [limit]
 * @param {'trade'|'quote'|'all'|null} [kind]
 */
export function tapeQuery(venue, symbol, limit = 100, kind = null) {
  const q = new URLSearchParams({
    venue,
    symbol,
    limit: String(limit),
  });
  if (kind && kind !== 'all') q.set('kind', kind);
  return `/v1/tape?${q}`;
}
