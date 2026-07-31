/** In-app alerts + optional webhook delivery. */

/**
 * @typedef {{ id: string, kind: 'bps'|'lag'|'info', title: string, body: string, ts: number, dismissed?: boolean }} Alert
 */

/** Auto-dismiss toast lifetime (ms). */
export const ALERT_AUTO_DISMISS_MS = 5000;

/** Max toasts shown at once (newest). */
export const ALERT_VISIBLE_MAX = 3;

let nextId = 1;

/**
 * @param {string} kind
 * @param {string} title
 * @param {string} body
 * @returns {Alert}
 */
export function createAlert(kind, title, body) {
  return {
    id: String(nextId++),
    kind,
    title,
    body,
    ts: Date.now(),
  };
}

/**
 * POST alert JSON to webhook URL (browser-side).
 * @param {string} webhookUrl
 * @param {object} payload
 */
export async function sendWebhook(webhookUrl, payload) {
  if (!webhookUrl?.trim()) return { ok: false, reason: 'no url' };
  try {
    const res = await fetch(webhookUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      mode: 'no-cors',
    });
    return { ok: res.type === 'opaque' || res.ok };
  } catch (e) {
    return { ok: false, reason: String(e.message || e) };
  }
}

/**
 * Translate rich in-app alert data to the daemon's deliberately narrow API.
 * Pulse alerts are local analytics and are not a supported daemon kind.
 */
export function daemonAlertPayload(payload) {
  if (!payload || typeof payload !== 'object') return null;
  if (payload.kind === 'discrepancy' || payload.type === 'bps' || payload.kind === 'bps') {
    const bps = Number(payload.bps);
    return {
      kind: 'discrepancy',
      ...(Number.isFinite(bps) ? { bps } : {}),
      message:
        payload.message ||
        `Cross-venue discrepancy${payload.threshold != null ? ` above ${payload.threshold} bps` : ''}`,
    };
  }
  if (payload.kind === 'lag' || payload.type === 'lag') {
    return {
      kind: 'lag',
      message:
        payload.message ||
        `${payload.venue ? `${payload.venue} feed` : 'feed'} lag${payload.lagMs != null ? ` ${payload.lagMs}ms` : ''}`,
    };
  }
  return null;
}

/**
 * Test alert via daemon endpoint if present.
 * @param {object} payload
 */
export async function testDaemonAlert(payload) {
  const daemonPayload = daemonAlertPayload(payload);
  if (!daemonPayload) return { ok: false, skipped: true, reason: 'unsupported alert kind' };
  try {
    const res = await fetch('/v1/alerts/test', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(daemonPayload),
    });
    return { ok: res.ok, status: res.status };
  } catch (e) {
    return { ok: false, reason: String(e.message || e) };
  }
}
