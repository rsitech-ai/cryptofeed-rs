const EXACT_FIELDS = [
  'price',
  'bid_quantity',
  'ask_quantity',
  'bid_cumulative_notional',
  'ask_cumulative_notional',
  'mbp_delta_quantity',
  'mbp_delta_notional',
  'buy_executed_notional',
  'sell_executed_notional',
  'unknown_executed_notional',
  'total_executed_notional',
  'executed_delta_notional',
];

function finiteExact(value) {
  if (typeof value !== 'string' || value.trim() === '') return null;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

export function normalizeDomSnapshot(raw) {
  if (!raw || raw.schema_version !== 1 || !Array.isArray(raw.rows)) {
    return emptyDomSnapshot('invalid_dom_payload');
  }
  const rows = raw.rows.flatMap((row) => {
    if (!row || EXACT_FIELDS.some((field) => finiteExact(row[field]) == null)) return [];
    const imbalanceBps = Number(row.imbalance_bps);
    if (!Number.isInteger(imbalanceBps) || Math.abs(imbalanceBps) > 10_000) return [];
    return [{
      price: Number(row.price),
      priceExact: row.price,
      bidQty: Number(row.bid_quantity),
      bidQtyExact: row.bid_quantity,
      askQty: Number(row.ask_quantity),
      askQtyExact: row.ask_quantity,
      bidUsd: Number(row.price) * Number(row.bid_quantity),
      askUsd: Number(row.price) * Number(row.ask_quantity),
      bidCumUsd: Number(row.bid_cumulative_notional),
      bidCumExact: row.bid_cumulative_notional,
      askCumUsd: Number(row.ask_cumulative_notional),
      askCumExact: row.ask_cumulative_notional,
      imbPct: imbalanceBps / 100,
      imbalanceBps,
      mbpDeltaQty: Number(row.mbp_delta_quantity),
      mbpDeltaQtyExact: row.mbp_delta_quantity,
      mbpDeltaUsd: Number(row.mbp_delta_notional),
      mbpDeltaExact: row.mbp_delta_notional,
      buyUsd: Number(row.buy_executed_notional),
      buyExecutedExact: row.buy_executed_notional,
      sellUsd: Number(row.sell_executed_notional),
      sellExecutedExact: row.sell_executed_notional,
      unknownUsd: Number(row.unknown_executed_notional),
      unknownExecutedExact: row.unknown_executed_notional,
      totalUsd: Number(row.total_executed_notional),
      totalExecutedExact: row.total_executed_notional,
      delta: Number(row.executed_delta_notional),
      executedDeltaExact: row.executed_delta_notional,
    }];
  });
  const status = typeof raw.status === 'string' ? raw.status : 'unavailable';
  return {
    available: status === 'live',
    status,
    reason: typeof raw.reason === 'string' ? raw.reason : null,
    revision: Number.isSafeInteger(raw.revision) ? raw.revision : 0,
    executionWindowSec: Number.isSafeInteger(raw.execution_window_sec)
      ? raw.execution_window_sec
      : 300,
    epoch: Number.isSafeInteger(raw.epoch) ? raw.epoch : 0,
    rows,
  };
}

export function emptyDomSnapshot(reason = 'dom_unavailable') {
  return {
    available: false,
    status: 'unavailable',
    reason,
    revision: 0,
    rows: [],
  };
}
