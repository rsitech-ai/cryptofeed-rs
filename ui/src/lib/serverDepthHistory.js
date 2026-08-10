export function normalizeDepthHistory(raw) {
  if (!raw || raw.schema_version !== 1 || !Array.isArray(raw.samples)) return [];
  return raw.samples.flatMap((sample) => {
    const t = Number(sample.event_ts_ns) / 1e6;
    if (!Number.isFinite(t)) return [];
    const side = (levels) => new Map((levels || []).flatMap((level) => {
      const price = Number(level.price);
      const quantity = Number(level.quantity);
      return price > 0 && quantity >= 0 ? [[price, price * quantity]] : [];
    }));
    const bids = side(sample.bids);
    const asks = side(sample.asks);
    const bestBid = bids.size ? Math.max(...bids.keys()) : null;
    const bestAsk = asks.size ? Math.min(...asks.keys()) : null;
    return [{ t, epoch: sample.epoch, bids, asks, mid: bestBid != null && bestAsk != null ? (bestBid + bestAsk) / 2 : null }];
  });
}
