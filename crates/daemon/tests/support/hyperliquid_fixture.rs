//! Pure shared fixtures. This module defines no tests or test-module includes.
//! Both integration targets retain the original inputs, budgets and watchdog.
use marketfeed_adapter_api::*;
use marketfeed_adapter_hyperliquid::HyperliquidFactory;
pub(crate) use marketfeed_daemon::causal_bootstrap::current_build;
use marketfeed_daemon::causal_bootstrap::replay::{MultiReplayLimits, ReplayResources};
use marketfeed_engine::{CaptureConfig, CaptureInput, causal::*};
use marketfeed_model::*;
pub(crate) use std::thread;
use std::{
    sync::OnceLock,
    time::{Duration, Instant},
};

pub(crate) const WAIT: Duration = Duration::from_secs(15);
pub(crate) const SPOT: &[u8] = br#"{"tokens":[{"name":"USDC","szDecimals":8,"weiDecimals":8,"index":0,"tokenId":"0x0"},{"name":"PURR","szDecimals":0,"weiDecimals":5,"index":7,"tokenId":"0x7"}],"universe":[{"name":"@12","tokens":[7,0],"index":12}]}"#;
pub(crate) const PERPS: &[u8] =
    br#"[{"collateralToken":0,"universe":[{"name":"BTC","szDecimals":5}]}]"#;
pub(crate) fn fixture(spot: bool) -> BootstrapDto {
    fixture_with_meta(spot, SPOT, PERPS, &[1])
}
pub(crate) fn fixture_with_meta(
    spot: bool,
    spot_meta: &[u8],
    perp_meta: &[u8],
    selected: &[u32],
) -> BootstrapDto {
    static BUILD: OnceLock<BootstrapBuildDto> = OnceLock::new();
    let factory = HyperliquidFactory { spot };
    let mut definitions = Vec::new();
    factory
        .parse_instruments(
            &[spot_meta, perp_meta].map(|bytes| HttpResponse {
                status: 200,
                headers: vec![],
                body: bytes.to_vec().into(),
            }),
            &mut definitions,
        )
        .unwrap();
    let catalog = CatalogView::with_instruments(
        factory.specification().id,
        CatalogVersion(7),
        definitions
            .into_iter()
            .enumerate()
            .map(|(i, d)| d.into_instrument(InstrumentId(i as u32 + 1), CatalogVersion(7)))
            .collect::<Vec<_>>(),
    );
    let mut channels = vec![
        Channel::Trades,
        Channel::Quote,
        Channel::L2Book {
            depth: Some(20),
            cadence: None,
        },
        Channel::Candles {
            interval: CandleInterval::M1,
        },
        Channel::MarkPrice,
        Channel::Statistics24h,
    ];
    if !spot {
        channels.extend([Channel::IndexPrice, Channel::Funding, Channel::OpenInterest]);
    }
    let request = ConcreteSubscriptionSet {
        items: channels
            .iter()
            .flat_map(|channel| {
                selected.iter().map(move |instrument| ConcreteSubscription {
                    instrument: InstrumentId(*instrument),
                    channel: channel.clone(),
                    delivery: DeliveryOptions {
                        emit_book_snapshots: true,
                        emit_book_deltas: false,
                        emit_bbo: true,
                    },
                })
            })
            .collect(),
    };
    let mut plans = factory.plan(&request, &catalog).unwrap();
    assert_eq!(plans.len(), 1);
    let spec = plans.remove(0);
    let capture = CaptureConfig {
        venue: catalog.venue,
        session: SessionId(31),
        dispatch_capacity: 64,
        overflow: OverflowPolicy::FailEngine,
        max_bytes: 1 << 20,
        max_entries: 2048,
    };
    let mut dto = BootstrapDto {
        schema_version: 2,
        build: BUILD.get_or_init(|| current_build().unwrap()).clone(),
        factory: FactoryDto::HyperliquidPublic {
            market: if spot {
                HyperliquidMarketDto::Spot
            } else {
                HyperliquidMarketDto::Perps
            },
        },
        environment: BootstrapEnvironmentDto::Production,
        output_profile: OutputProfileDto::DiskNullDispatchV1,
        adapter_policy: AdapterEmissionPolicyDto::HyperliquidSelectedChannelsV1,
        protocol_topics: HyperliquidFactory::public_plan_topics(spot, &spec, &catalog).unwrap(),
        session: (&spec).into(),
        catalog: (&catalog).into(),
        capture: capture.into(),
        runner_policy: capture.runtime_policy(),
        owner_policy: Some(JournalOwnerPolicyDto {
            max_pending_operations: 16,
            max_pending_bytes: 2 << 20,
            max_input_bytes: 8192,
            max_operation_bytes: 256 << 10,
            max_ingress_age: Duration::from_secs(10).into(),
            receipt_poll_interval: Duration::from_millis(1).into(),
        }),
        public_inputs: vec![
            PublicReproductionInputDto::new(HYPERLIQUID_SPOT_META_ROLE, spot_meta),
            PublicReproductionInputDto::new(HYPERLIQUID_PERP_METAS_ROLE, perp_meta),
        ],
    };
    dto.bind_history_policy(HistoryRuntimePolicyDto {
        schema_version: 1,
        max_pending_observations: 32,
        max_pending_observation_bytes: 4 << 20,
        max_record_bytes: 256 << 10,
        max_finalization_age: Duration::from_secs(2).into(),
    })
    .unwrap();
    dto
}
#[track_caller]
pub(crate) fn wait(mut check: impl FnMut() -> bool) {
    let until = Instant::now() + WAIT;
    while !check() {
        assert!(Instant::now() < until, "Hyperliquid causal watchdog");
        thread::sleep(Duration::from_millis(1));
    }
}
// Same deadline, predicate order and cadence as wait. Snapshot diagnostics only
// at the existing failure boundary; do not reset the clock or retry a source.
#[track_caller]
pub(crate) fn wait_inspect(mut check: impl FnMut() -> bool, inspect: impl FnOnce() -> String) {
    let until = Instant::now() + WAIT;
    while !check() {
        if Instant::now() >= until {
            panic!("Hyperliquid causal watchdog: {}", inspect());
        }
        thread::sleep(Duration::from_millis(1));
    }
}
pub(crate) fn text(payload: String, clock: i64) -> CaptureInput {
    CaptureInput::Text {
        payload: payload.into_bytes(),
        received: FrameStamp {
            receive_ts: TimestampNs(clock),
            mono_ns: clock as u64,
        },
    }
}
pub(crate) fn inputs(dto: &BootstrapDto) -> Vec<(u64, CaptureInput)> {
    let coin = &dto.catalog.instruments[0].key.native_symbol;
    let mut out = vec![(
        1,
        CaptureInput::Connected {
            connection: ConnectionId(1),
            now: TimestampNs(1),
        },
    )];
    out.extend(dto.protocol_topics.iter().map(|topic| (1, text(format!(r#"{{"channel":"subscriptionResponse","data":{{"method":"subscribe","subscription":{topic}}}}}"#), 2))));
    out.extend([
        (1, text(format!(r#"{{"channel":"l2Book","data":{{"coin":"{coin}","time":1,"levels":[[{{"px":"10","sz":"2","n":3}}],[{{"px":"11","sz":"1","n":2}}]]}}}}"#), 3)),
        (1, text(format!(r#"{{"channel":"activeAssetCtx","data":{{"coin":"{coin}","ctx":{{"dayNtlVlm":"100.01","prevDayPx":"9","markPx":"10.02","funding":"-0.00000125","openInterest":"12345.12345","oraclePx":"10.03"}}}}}}"#), 4)),
        (1, text(format!(r#"{{"channel":"trades","data":[{{"coin":"{coin}","side":"B","px":"9007199254740993.123456789012345678","sz":"0.00000000000000000001","time":1,"tid":1,"hash":"h1"}}]}}"#), 5)),
        (1, text(format!(r#"{{"channel":"bbo","data":{{"coin":"{coin}","time":1,"bbo":[{{"px":"10","sz":"2","n":1}},{{"px":"11","sz":"1","n":1}}]}}}}"#), 6)),
        (1, text(format!(r#"{{"channel":"candle","data":{{"t":0,"T":59999,"s":"{coin}","i":"1m","o":"10","h":"11","l":"9","c":"10.1","v":"123.45","n":5}}}}"#), 7)),
        (1, CaptureInput::TimerPoll { now: TimestampNs(20_000_000_001) }),
        (1, CaptureInput::TransportLost { reason: DisconnectReason::TransportError, now: TimestampNs(20_000_000_002) }),
        (2, CaptureInput::Connected { connection: ConnectionId(2), now: TimestampNs(20_000_000_003) }),
    ]);
    out
}
pub(crate) fn native_layout() -> ObservedBatchLimits {
    ObservedBatchLimits {
        layout: NativeObservationLimits {
            rows: 64,
            associations: 128,
            book_levels: 40,
            mids: 0,
            users: 128,
            string_bytes: 4096,
        },
        encoded_bytes: 64 << 10,
    }
}
pub(crate) fn native_fixture(spot: bool) -> BootstrapDto {
    let mut dto = fixture(spot);
    dto.bind_observation_policy(NativeObservationPolicyDto::HyperliquidV1 {
        limits: native_layout(),
    })
    .unwrap();
    dto
}
pub(crate) fn replay_limits() -> MultiReplayLimits {
    MultiReplayLimits {
        per_owner: OwnerHistoryLimits {
            max_retained_input_bytes: 4 << 20,
            max_input_bytes: 256 << 10,
            max_record_bytes: 256 << 10,
        },
        maximum: ReplayResources {
            owners: 8,
            bootstrap_payload_bytes: 8 << 20,
            pending_inputs: 128,
            pending_input_bytes: 8 << 20,
            active_operation_and_state_bytes: 8 << 20,
            source_owned_pending_inputs: 128,
            source_owned_pending_bytes: 8 << 20,
            capture_trace_reservation_bytes: 16 << 20,
            catalog_entries: 64,
            selected_instruments: 64,
            maintained_book_levels: 4096,
            cached_book_levels: 32768,
        },
    }
}
