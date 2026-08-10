import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { normalizeMarketProfile } from './marketProfile.js';

describe('normalizeMarketProfile', () => {
  it('preserves exact daemon strings and all seven session metrics', () => {
    const profile = normalizeMarketProfile({
      schema_version: 1,
      venue: 'binance-usdm',
      instrument: 1,
      symbol: 'BTCUSDT',
      revision: 4,
      status: 'live',
      basis: 'volume',
      value_area_bps: 7000,
      start_ts_ns: 10,
      end_ts_ns: 20,
      high: '101.00',
      low: '99.00',
      range: '2.00',
      total_volume: '3.250',
      poc: '100.00',
      vah: '101.00',
      val: '99.00',
      tpo_count: 5,
      rotation_factor: -2,
    });
    assert.equal(profile.available, true);
    assert.equal(profile.vah, '101.00');
    assert.equal(profile.val, '99.00');
    assert.equal(profile.poc, '100.00');
    assert.equal(profile.range, '2.00');
    assert.equal(profile.volume, '3.250');
    assert.equal(profile.tpoCount, 5);
    assert.equal(profile.rotationFactor, -2);
  });

  it('keeps unavailable reasons explicit instead of inventing zeroes', () => {
    const profile = normalizeMarketProfile({
      schema_version: 1,
      status: 'unavailable',
      reason: 'catalog_not_authoritative',
      revision: 0,
    });
    assert.equal(profile.available, false);
    assert.equal(profile.reason, 'catalog_not_authoritative');
    assert.equal(profile.volume, null);
    assert.equal(profile.tpoCount, null);
  });

  it('rejects malformed or future schema payloads', () => {
    assert.equal(normalizeMarketProfile(null).reason, 'profile_payload_missing');
    assert.equal(normalizeMarketProfile({ schema_version: 2 }).reason, 'profile_schema_unsupported');
  });
});
