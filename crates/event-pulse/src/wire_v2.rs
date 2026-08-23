//! Additive MechanicsInput V2 wire with explicit MARKET cursor and provenance.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::wire::{
    MAX_I64_U64, MAX_INPUT_BYTES, MechanicsInputRefV1, MechanicsInputV1, ReplayCatalogV1,
    WireError, parse_unique_json, validate_market_catalog_action,
};

const MAX_TIMESTAMP_MS: u64 = 9_223_372_036_854;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum MarketCursorV2 {
    Native {
        first_sequence: u64,
        last_sequence: u64,
    },
    Derived {
        raw_frame_seq: u64,
        action_index: u32,
        item_index: u32,
    },
}

impl MarketCursorV2 {
    pub fn validate_static(&self) -> Result<(), WireError> {
        match self {
            Self::Native {
                first_sequence,
                last_sequence,
            } if first_sequence <= last_sequence && *last_sequence <= MAX_I64_U64 => Ok(()),
            Self::Derived {
                raw_frame_seq,
                action_index,
                item_index,
            } if *raw_frame_seq > 0 && *action_index <= 65_534 && *item_index <= 65_535 => Ok(()),
            _ => Err(WireError::Cursor),
        }
    }

    pub fn native_range(&self) -> Option<(u64, u64)> {
        match self {
            Self::Native {
                first_sequence,
                last_sequence,
            } => Some((*first_sequence, *last_sequence)),
            Self::Derived { .. } => None,
        }
    }

    pub fn derived_coordinate(&self) -> Option<(u64, u32, u32)> {
        match self {
            Self::Derived {
                raw_frame_seq,
                action_index,
                item_index,
            } => Some((*raw_frame_seq, *action_index, *item_index)),
            Self::Native { .. } => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
pub enum SourceProvenanceV2 {
    None,
    BinanceBookTicker {
        update_id: u64,
        event_time_ms: u64,
        transaction_time_ms: u64,
    },
    BinanceBookDelta {
        first_update_id: u64,
        final_update_id: u64,
        previous_final_update_id: u64,
        event_time_ms: u64,
        transaction_time_ms: u64,
    },
    BinanceBookSnapshot {
        last_update_id: u64,
        event_time_ms: u64,
        transaction_time_ms: u64,
    },
    BinanceAggregateTrade {
        aggregate_trade_id: u64,
        event_time_ms: u64,
        trade_time_ms: u64,
    },
    BinanceOpenInterest {
        source_time_ms: u64,
    },
    BinanceForceOrder {
        event_time_ms: u64,
        order_trade_time_ms: u64,
    },
}

impl SourceProvenanceV2 {
    fn validate_bounds(&self) -> Result<(), WireError> {
        let timestamp = |value: u64| {
            if value <= MAX_TIMESTAMP_MS {
                Ok(())
            } else {
                Err(WireError::Time)
            }
        };
        let native = |value: u64| {
            if value <= MAX_I64_U64 {
                Ok(())
            } else {
                Err(WireError::Cursor)
            }
        };
        match self {
            Self::None => Ok(()),
            Self::BinanceBookTicker {
                event_time_ms,
                transaction_time_ms,
                ..
            } => {
                timestamp(*event_time_ms)?;
                timestamp(*transaction_time_ms)
            }
            Self::BinanceBookDelta {
                first_update_id,
                final_update_id,
                previous_final_update_id,
                event_time_ms,
                transaction_time_ms,
            } => {
                native(*first_update_id)?;
                native(*final_update_id)?;
                native(*previous_final_update_id)?;
                if first_update_id > final_update_id {
                    return Err(WireError::Cursor);
                }
                timestamp(*event_time_ms)?;
                timestamp(*transaction_time_ms)
            }
            Self::BinanceBookSnapshot {
                last_update_id,
                event_time_ms,
                transaction_time_ms,
            } => {
                native(*last_update_id)?;
                timestamp(*event_time_ms)?;
                timestamp(*transaction_time_ms)
            }
            Self::BinanceAggregateTrade {
                aggregate_trade_id,
                event_time_ms,
                trade_time_ms,
            } => {
                native(*aggregate_trade_id)?;
                timestamp(*event_time_ms)?;
                timestamp(*trade_time_ms)
            }
            Self::BinanceOpenInterest { source_time_ms } => timestamp(*source_time_ms),
            Self::BinanceForceOrder {
                event_time_ms,
                order_trade_time_ms,
            } => {
                timestamp(*event_time_ms)?;
                timestamp(*order_trade_time_ms)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MechanicsInputInnerV2 {
    Market {
        envelope: Box<marketfeed_model::EventEnvelope>,
        action_index: u32,
        catalog: ReplayCatalogV1,
        market_cursor: MarketCursorV2,
        source_provenance: SourceProvenanceV2,
        payload_hash: String,
    },
    NonMarket(Box<MechanicsInputV1>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MechanicsInputV2(MechanicsInputInnerV2);

#[derive(Debug, Clone, Copy)]
pub enum MechanicsInputRefV2<'a> {
    Market {
        envelope: &'a marketfeed_model::EventEnvelope,
        action_index: u32,
        catalog: &'a ReplayCatalogV1,
        market_cursor: &'a MarketCursorV2,
        source_provenance: &'a SourceProvenanceV2,
        payload_hash: &'a str,
    },
    NonMarket(MechanicsInputRefV1<'a>),
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
enum MarketSerialize<'a> {
    Market {
        envelope: &'a marketfeed_model::EventEnvelope,
        action_index: u32,
        catalog: &'a ReplayCatalogV1,
        market_cursor: &'a MarketCursorV2,
        source_provenance: &'a SourceProvenanceV2,
        payload_hash: &'a str,
    },
}

impl Serialize for MechanicsInputV2 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.0 {
            MechanicsInputInnerV2::Market {
                envelope,
                action_index,
                catalog,
                market_cursor,
                source_provenance,
                payload_hash,
            } => serde_json::to_value(MarketSerialize::Market {
                envelope,
                action_index: *action_index,
                catalog,
                market_cursor,
                source_provenance,
                payload_hash,
            })
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer),
            MechanicsInputInnerV2::NonMarket(input) => input.serialize(serializer),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MarketWireV2 {
    kind: MarketKind,
    envelope: serde_json::Value,
    action_index: u32,
    catalog: ReplayCatalogV1,
    market_cursor: MarketCursorV2,
    source_provenance: SourceProvenanceV2,
    payload_hash: String,
}

#[derive(Deserialize)]
enum MarketKind {
    #[serde(rename = "MARKET")]
    Market,
}

impl MechanicsInputV2 {
    #[allow(clippy::too_many_arguments)]
    pub fn market(
        envelope: marketfeed_model::EventEnvelope,
        action_index: u32,
        catalog: ReplayCatalogV1,
        market_cursor: MarketCursorV2,
        source_provenance: SourceProvenanceV2,
    ) -> Result<Self, WireError> {
        let mut input = Self(MechanicsInputInnerV2::Market {
            envelope: Box::new(envelope),
            action_index,
            catalog,
            market_cursor,
            source_provenance,
            payload_hash: String::new(),
        });
        input.validate_market_without_hash()?;
        let hash = input.expected_payload_hash()?;
        if let MechanicsInputInnerV2::Market { payload_hash, .. } = &mut input.0 {
            *payload_hash = hash;
        }
        if serde_json::to_vec(&input)
            .map_err(|_| WireError::Identity)?
            .len()
            > MAX_INPUT_BYTES
        {
            return Err(WireError::Identity);
        }
        Ok(input)
    }

    pub fn from_v1_non_market(input: MechanicsInputV1) -> Result<Self, WireError> {
        if matches!(input.view(), MechanicsInputRefV1::Market { .. }) {
            return Err(WireError::Identity);
        }
        input.validate_static()?;
        Ok(Self(MechanicsInputInnerV2::NonMarket(Box::new(input))))
    }

    pub fn from_json_line(bytes: &[u8]) -> Result<Self, WireError> {
        if bytes.len() > MAX_INPUT_BYTES {
            return Err(WireError::Identity);
        }
        let value = parse_unique_json(bytes)?;
        if serde_json::to_vec(&value).map_err(|_| WireError::Identity)? != bytes {
            return Err(WireError::Identity);
        }
        let kind = value
            .as_object()
            .and_then(|object| object.get("kind"))
            .and_then(serde_json::Value::as_str)
            .ok_or(WireError::Identity)?;
        if kind != "MARKET" {
            return Self::from_v1_non_market(MechanicsInputV1::from_epin_json(bytes)?);
        }
        let wire: MarketWireV2 = serde_json::from_value(value).map_err(|_| WireError::Identity)?;
        let _kind = wire.kind;
        let envelope: marketfeed_model::EventEnvelope =
            serde_json::from_value(wire.envelope.clone()).map_err(|_| WireError::Identity)?;
        if serde_json::to_value(&envelope).map_err(|_| WireError::Identity)? != wire.envelope {
            return Err(WireError::Identity);
        }
        let input = Self(MechanicsInputInnerV2::Market {
            envelope: Box::new(envelope),
            action_index: wire.action_index,
            catalog: wire.catalog,
            market_cursor: wire.market_cursor,
            source_provenance: wire.source_provenance,
            payload_hash: wire.payload_hash,
        });
        input.validate_static()?;
        Ok(input)
    }

    pub fn payload_hash(&self) -> &str {
        match &self.0 {
            MechanicsInputInnerV2::Market { payload_hash, .. } => payload_hash,
            MechanicsInputInnerV2::NonMarket(input) => input.payload_hash(),
        }
    }

    pub fn view(&self) -> MechanicsInputRefV2<'_> {
        match &self.0 {
            MechanicsInputInnerV2::Market {
                envelope,
                action_index,
                catalog,
                market_cursor,
                source_provenance,
                payload_hash,
            } => MechanicsInputRefV2::Market {
                envelope,
                action_index: *action_index,
                catalog,
                market_cursor,
                source_provenance,
                payload_hash,
            },
            MechanicsInputInnerV2::NonMarket(input) => MechanicsInputRefV2::NonMarket(input.view()),
        }
    }

    pub(crate) fn as_v1_non_market(&self) -> Option<&MechanicsInputV1> {
        match &self.0 {
            MechanicsInputInnerV2::NonMarket(input) => Some(input),
            MechanicsInputInnerV2::Market { .. } => None,
        }
    }

    pub fn validate_static(&self) -> Result<(), WireError> {
        match &self.0 {
            MechanicsInputInnerV2::Market { payload_hash, .. } => {
                self.validate_market_without_hash()?;
                if self.expected_payload_hash()? != *payload_hash {
                    return Err(WireError::Identity);
                }
                validate_hash(payload_hash)
            }
            MechanicsInputInnerV2::NonMarket(input) => input.validate_static(),
        }
    }

    fn expected_payload_hash(&self) -> Result<String, WireError> {
        match &self.0 {
            MechanicsInputInnerV2::NonMarket(input) => input.expected_payload_hash(),
            MechanicsInputInnerV2::Market { .. } => {
                let mut value = serde_json::to_value(self).map_err(|_| WireError::Identity)?;
                value
                    .as_object_mut()
                    .ok_or(WireError::Identity)?
                    .remove("payload_hash");
                Ok(format!(
                    "{:x}",
                    Sha256::digest(serde_json::to_vec(&value).map_err(|_| WireError::Identity)?)
                ))
            }
        }
    }

    fn validate_market_without_hash(&self) -> Result<(), WireError> {
        let MechanicsInputInnerV2::Market {
            envelope,
            action_index,
            catalog,
            market_cursor,
            source_provenance,
            ..
        } = &self.0
        else {
            return Err(WireError::Identity);
        };
        catalog.validate()?;
        validate_market_catalog_action(catalog, envelope, *action_index)?;
        market_cursor.validate_static()?;
        source_provenance.validate_bounds()?;
        validate_cursor_binding(envelope, *action_index, market_cursor)?;

        let source = catalog
            .venue_source(envelope.venue.0)
            .ok_or(WireError::Identity)?
            .source_id();
        let instrument = envelope
            .instrument
            .and_then(|id| catalog.instrument(id.0))
            .ok_or(WireError::Identity)?;
        let exact_instrument = match source {
            "binance_primary_public" | "binance_primary_market" => {
                instrument.venue() == "BINANCE" && instrument.venue_symbol() == "BNBUSDT"
            }
            "hyperliquid_confirmation" => {
                instrument.venue() == "HYPERLIQUID" && instrument.venue_symbol() == "BNB"
            }
            _ => false,
        } && instrument.base_asset() == "BNB"
            && instrument.quote_asset() == "USDT"
            && instrument.market_type() == "PERPETUAL";
        if !exact_instrument {
            return Err(WireError::Identity);
        }
        let exchange_ns = envelope.exchange_ts.ok_or(WireError::Time)?.0;
        match (&envelope.payload, source, source_provenance, market_cursor) {
            (
                marketfeed_model::MarketEvent::Quote(_),
                "binance_primary_public",
                SourceProvenanceV2::BinanceBookTicker {
                    transaction_time_ms,
                    ..
                },
                MarketCursorV2::Derived { .. },
            ) => require_exchange_time(*transaction_time_ms, exchange_ns),
            (
                marketfeed_model::MarketEvent::BookDelta(_),
                "binance_primary_public",
                SourceProvenanceV2::BinanceBookDelta {
                    first_update_id,
                    final_update_id,
                    transaction_time_ms,
                    ..
                },
                MarketCursorV2::Native {
                    first_sequence,
                    last_sequence,
                },
            ) if first_update_id == first_sequence && final_update_id == last_sequence => {
                require_exchange_time(*transaction_time_ms, exchange_ns)
            }
            (
                marketfeed_model::MarketEvent::BookSnapshot(_),
                "binance_primary_public",
                SourceProvenanceV2::BinanceBookSnapshot {
                    last_update_id,
                    transaction_time_ms,
                    ..
                },
                MarketCursorV2::Native {
                    first_sequence,
                    last_sequence,
                },
            ) if last_update_id == first_sequence && last_update_id == last_sequence => {
                require_exchange_time(*transaction_time_ms, exchange_ns)
            }
            (
                marketfeed_model::MarketEvent::Trade(_),
                "binance_primary_market",
                SourceProvenanceV2::BinanceAggregateTrade {
                    aggregate_trade_id,
                    trade_time_ms,
                    ..
                },
                MarketCursorV2::Native {
                    first_sequence,
                    last_sequence,
                },
            ) if aggregate_trade_id == first_sequence && aggregate_trade_id == last_sequence => {
                require_exchange_time(*trade_time_ms, exchange_ns)
            }
            (
                marketfeed_model::MarketEvent::OpenInterest(_),
                "binance_primary_market",
                SourceProvenanceV2::BinanceOpenInterest { source_time_ms },
                MarketCursorV2::Derived { .. },
            ) => require_exchange_time(*source_time_ms, exchange_ns),
            (
                marketfeed_model::MarketEvent::Liquidation(_),
                "binance_primary_market",
                SourceProvenanceV2::BinanceForceOrder {
                    order_trade_time_ms,
                    ..
                },
                MarketCursorV2::Derived { .. },
            ) => require_exchange_time(*order_trade_time_ms, exchange_ns),
            (
                marketfeed_model::MarketEvent::MarkPrice(_)
                | marketfeed_model::MarketEvent::IndexPrice(_),
                "hyperliquid_confirmation",
                SourceProvenanceV2::None,
                MarketCursorV2::Derived { .. },
            ) => Ok(()),
            _ => Err(WireError::Identity),
        }
    }
}

fn validate_cursor_binding(
    envelope: &marketfeed_model::EventEnvelope,
    action_index: u32,
    cursor: &MarketCursorV2,
) -> Result<(), WireError> {
    match (cursor, envelope.source_sequence) {
        (
            MarketCursorV2::Native {
                first_sequence,
                last_sequence,
            },
            Some(sequence),
        ) if *first_sequence == sequence.first && *last_sequence == sequence.last => Ok(()),
        (
            MarketCursorV2::Derived {
                raw_frame_seq,
                action_index: cursor_action,
                item_index,
            },
            None,
        ) if *raw_frame_seq == envelope.frame_seq
            && *cursor_action == action_index
            && *item_index == u32::from(envelope.event_index) =>
        {
            Ok(())
        }
        _ => Err(WireError::Cursor),
    }
}

fn require_exchange_time(source_ms: u64, exchange_ns: i64) -> Result<(), WireError> {
    let source_ns = i64::try_from(source_ms)
        .ok()
        .and_then(|value| value.checked_mul(1_000_000))
        .ok_or(WireError::Time)?;
    if source_ns == exchange_ns {
        Ok(())
    } else {
        Err(WireError::Time)
    }
}

fn validate_hash(value: &str) -> Result<(), WireError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        Ok(())
    } else {
        Err(WireError::Identity)
    }
}
