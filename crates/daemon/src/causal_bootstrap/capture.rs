//! Pure bootstrap construction for the opt-in public capture command. Metadata
//! acquisition and actual transport startup are separate, explicit operations.
use super::{BootstrapError, current_build, live::DriverPolicy};
use marketfeed_adapter_api::{
    ConcreteSubscription, ConcreteSubscriptionSet, DeliveryOptions, NativeObservationLimits,
    ObservedBatchLimits, VenueFactory,
};
use marketfeed_adapter_hyperliquid::HyperliquidFactory;
use marketfeed_engine::{CaptureConfig, causal::*};
use marketfeed_model::{CatalogVersion, CatalogView, InstrumentId, OverflowPolicy, SessionId};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, time::Duration};

#[path = "capture_discovery.rs"]
mod discovery;
pub use discovery::{AcquiredDiscovery, acquire_public};
#[path = "capture_run.rs"]
mod runtime;
pub use runtime::{projection_limits, replay_limits, run_public, verify};

pub const CAPTURE_REQUEST_ROLE: &str = "hyperliquid-causal-capture-request-v1";
pub const MAX_METADATA_BYTES: usize = 4 << 20;
const MAX_CAPTURE_FRAME_BYTES: usize = 64 << 10;
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketSelection {
    pub market: HyperliquidMarketDto,
    /// Exact native coins from discovery. No UI aliases or symbol rewriting.
    pub coins: Vec<String>,
    pub channels: Vec<ChannelDto>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureRequest {
    pub schema_version: u16,
    pub markets: Vec<MarketSelection>,
    pub duration_ms: u64,
    pub source_max_bytes: u64,
    pub target_max_bytes: u64,
}
impl CaptureRequest {
    pub fn validate(&self) -> Result<(), BootstrapError> {
        if self.schema_version != 1
            || self.markets.is_empty()
            || self.markets.len() > 2
            || !(1..=86_400_000).contains(&self.duration_ms)
            || !(64 << 20..=1 << 40).contains(&self.source_max_bytes)
            || !(64 << 20..=1 << 40).contains(&self.target_max_bytes)
        {
            return Err(BootstrapError::Unsupported(
                "capture request version/bounds",
            ));
        }
        let mut markets = BTreeSet::new();
        for selection in &self.markets {
            if !markets.insert(matches!(selection.market, HyperliquidMarketDto::Spot))
                || selection.coins.is_empty()
                || selection.coins.len() > 64
                || selection.channels.is_empty()
                || selection.channels.len() > 16
            {
                return Err(BootstrapError::Unsupported(
                    "capture market/selection bounds",
                ));
            }
            let mut coins = BTreeSet::new();
            for coin in &selection.coins {
                if coin.is_empty()
                    || coin.len() > 128
                    || !coins.insert(coin)
                    || coin.chars().any(char::is_control)
                {
                    return Err(BootstrapError::Unsupported("duplicate/invalid native coin"));
                }
            }
            let mut channels = BTreeSet::new();
            for channel in &selection.channels {
                channel.channel()?;
                if !channels.insert(serde_json::to_string(channel).map_err(CodecError::from)?) {
                    return Err(BootstrapError::Unsupported("duplicate selected channel"));
                }
            }
        }
        Ok(())
    }
}
/// Successful decoded HTTP bodies only. The caller records acquisition identity,
/// requests, status, headers and clocks separately in hash-bound public inputs.
struct DiscoveryBodies<'a> {
    pub spot: &'a [u8],
    pub perps: Option<&'a [u8]>,
}
fn prepare(
    request: &CaptureRequest,
    metadata: DiscoveryBodies<'_>,
    acquisition: PublicReproductionInputDto,
) -> Result<Vec<BootstrapDto>, BootstrapError> {
    request.validate()?;
    if metadata.spot.is_empty()
        || metadata.spot.len() > MAX_METADATA_BYTES
        || metadata
            .perps
            .is_some_and(|p| p.is_empty() || p.len() > MAX_METADATA_BYTES)
        || request
            .markets
            .iter()
            .any(|m| m.market == HyperliquidMarketDto::Perps)
            && metadata.perps.is_none()
    {
        return Err(BootstrapError::Unsupported(
            "discovery body bounds/coverage",
        ));
    }
    if acquisition.role != "hyperliquid-bootstrap-http-acquisition-v1" {
        return Err(BootstrapError::Unsupported(
            "missing acquisition evidence role",
        ));
    }
    let build = current_build()?;
    let recipe = serde_json::to_vec(request).map_err(CodecError::from)?;
    let mut result = Vec::new();
    for selection in &request.markets {
        let spot = selection.market == HyperliquidMarketDto::Spot;
        let factory = HyperliquidFactory { spot };
        let response = |body: &[u8]| marketfeed_adapter_api::HttpResponse {
            status: 200,
            headers: Vec::new(),
            body: body.to_vec().into(),
        };
        let mut responses = vec![response(metadata.spot)];
        if !spot {
            responses.push(response(metadata.perps.expect("coverage checked")));
        }
        let mut definitions = Vec::new();
        factory
            .parse_instruments(&responses, &mut definitions)
            .map_err(|e| BootstrapError::Factory(e.to_string()))?;
        if definitions.is_empty() || definitions.len() > 10_000 {
            return Err(BootstrapError::Unsupported("discovery catalog capacity"));
        }
        let catalog = CatalogView::with_instruments(
            factory.specification().id,
            CatalogVersion(1),
            definitions
                .into_iter()
                .enumerate()
                .map(|(index, definition)| {
                    definition.into_instrument(InstrumentId(index as u32 + 1), CatalogVersion(1))
                })
                .collect::<Vec<_>>(),
        );
        let mut selected = Vec::new();
        for coin in &selection.coins {
            let mut matches = catalog
                .instruments
                .iter()
                .filter(|instrument| instrument.key.native_symbol == *coin);
            let row = matches.next().ok_or(BootstrapError::Unsupported(
                "native coin absent from discovery",
            ))?;
            if matches.next().is_some() {
                return Err(BootstrapError::Unsupported("ambiguous native coin"));
            }
            selected.push(row.id);
        }
        let subscriptions = ConcreteSubscriptionSet {
            items: selected
                .iter()
                .flat_map(|instrument| {
                    selection.channels.iter().map(move |channel| {
                        Ok(ConcreteSubscription {
                            instrument: *instrument,
                            channel: channel.channel()?,
                            delivery: DeliveryOptions {
                                emit_book_snapshots: true,
                                emit_book_deltas: false,
                                emit_bbo: true,
                            },
                        })
                    })
                })
                .collect::<Result<Vec<_>, CodecError>>()?,
        };
        let mut plans = factory
            .plan(&subscriptions, &catalog)
            .map_err(|e| BootstrapError::Factory(e.to_string()))?;
        if plans.len() != 1 {
            return Err(BootstrapError::Unsupported(
                "one bounded native plan per market required",
            ));
        }
        let session = plans.remove(0);
        let topics = HyperliquidFactory::public_plan_topics(spot, &session, &catalog)
            .map_err(|e| BootstrapError::Factory(e.to_string()))?;
        if topics.len() > 64 {
            return Err(BootstrapError::Unsupported(
                "initial application wire bundle exceeds 64 writes",
            ));
        }
        let capture = CaptureConfig {
            venue: catalog.venue,
            session: SessionId(if spot { 31 } else { 32 }),
            dispatch_capacity: 256,
            overflow: OverflowPolicy::FailEngine,
            max_bytes: 4 << 20,
            max_entries: 8192,
        };
        let mut public_inputs = vec![
            PublicReproductionInputDto::new(HYPERLIQUID_SPOT_META_ROLE, metadata.spot),
            PublicReproductionInputDto::new(CAPTURE_REQUEST_ROLE, &recipe),
            acquisition.clone(),
        ];
        if !spot {
            public_inputs.push(PublicReproductionInputDto::new(
                HYPERLIQUID_PERP_METAS_ROLE,
                metadata.perps.expect("coverage checked"),
            ));
        }
        let mut dto = BootstrapDto {
            schema_version: 2,
            build: build.clone(),
            factory: FactoryDto::HyperliquidPublic {
                market: selection.market,
            },
            environment: BootstrapEnvironmentDto::Production,
            output_profile: OutputProfileDto::DiskNullDispatchV1,
            adapter_policy: AdapterEmissionPolicyDto::HyperliquidSelectedChannelsV1,
            protocol_topics: topics,
            session: (&session).into(),
            catalog: (&catalog).into(),
            capture: capture.into(),
            runner_policy: capture.runtime_policy(),
            owner_policy: Some(JournalOwnerPolicyDto {
                max_pending_operations: 64,
                max_pending_bytes: 8 << 20,
                max_input_bytes: 128 << 10,
                max_operation_bytes: 4 << 20,
                max_ingress_age: Duration::from_secs(10).into(),
                receipt_poll_interval: Duration::from_millis(1).into(),
            }),
            public_inputs,
        };
        dto.bind_observation_policy(NativeObservationPolicyDto::HyperliquidV1 {
            limits: ObservedBatchLimits {
                layout: NativeObservationLimits {
                    rows: 128,
                    associations: 256,
                    book_levels: 256,
                    mids: 0,
                    users: 256,
                    // Full recent-trade snapshots include native hashes and
                    // public participant strings. Bound their total retained
                    // text by the existing raw-frame ceiling, not a 4KiB sample.
                    string_bytes: MAX_CAPTURE_FRAME_BYTES,
                },
                encoded_bytes: 256 << 10,
            },
        })?;
        dto.bind_history_policy(HistoryRuntimePolicyDto {
            schema_version: 1,
            max_pending_observations: 256,
            max_pending_observation_bytes: 64 << 20,
            max_record_bytes: 4 << 20,
            max_finalization_age: Duration::from_secs(10).into(),
        })?;
        dto.bind_wire_policy(WirePolicyDto {
            schema_version: 1,
            max_pending_writes: 64,
            max_pending_bytes: 2 << 20,
            transport_controls:
                TransportControlsDto::ManagedConnectReadAutomaticPongCloseUnjournaledV1,
        })?;
        DriverPolicy {
            schema_version: 1,
            max_frame_bytes: MAX_CAPTURE_FRAME_BYTES as u64,
            connect_timeout_ms: 15_000,
            write_timeout_ms: 5_000,
            close_timeout_ms: 1_000,
            owner_poll_ms: 1,
            timer_interval_ms: 100,
            reconnect_delay_ms: 1_000,
            max_reconnects: 100,
            max_session_ms: request.duration_ms,
        }
        .bind(&mut dto)?;
        result.push(dto);
    }
    Ok(result)
}

#[cfg(all(test, feature = "causal-journal"))]
#[path = "capture_tests.rs"]
mod tests;
