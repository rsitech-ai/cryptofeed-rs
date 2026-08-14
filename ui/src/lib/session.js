/** Session stat windows (independent of chart timeframe). */

export const SESSION_PRESETS = [
  { id: '1m', label: '1m', sec: 60 },
  { id: '5m', label: '5m', sec: 300 },
  { id: '1h', label: '1h', sec: 3600 },
  { id: '2h', label: '2h', sec: 7200 },
];

/**
 * @param {string} id
 * @returns {number}
 */
export function sessionWindowSec(id) {
  return SESSION_PRESETS.find((s) => s.id === id)?.sec ?? 300;
}
