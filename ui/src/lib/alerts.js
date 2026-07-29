/** In-app alerts + optional webhook delivery. */

/**
 * @typedef {{ id: string, kind: 'bps'|'lag'|'info', title: string, body: string, ts: number, dismissed?: boolean }} Alert
 */

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
 * Test alert via daemon endpoint if present.
 * @param {object} payload
 */
export async function testDaemonAlert(payload) {
  try {
    const res = await fetch('/v1/alerts/test', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    return { ok: res.ok, status: res.status };
  } catch (e) {
    return { ok: false, reason: String(e.message || e) };
  }
}
