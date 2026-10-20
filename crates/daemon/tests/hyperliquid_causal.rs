//! Actual Hyperliquid factories through the public MFCJ3 bootstrap/owner API.
//! Offline protocol fixtures: no transport, deployment or live-continuity claim.
#![cfg(feature = "causal-journal")]
use marketfeed_adapter_api::*;
use marketfeed_adapter_hyperliquid::HyperliquidFactory;
use marketfeed_daemon::causal_bootstrap::{
    PreparedBootstrap, current_build,
    owner::{InputSettlement, OwnerPhase, history::HistoryJournalOwner},
};
use marketfeed_engine::{
    CaptureConfig, CaptureDrainPolicy, CaptureInput, CaptureSession, causal::*,
};
use marketfeed_model::*;
use marketfeed_recording::{shared_batch::*, shared_feed::*};
use std::{
    sync::OnceLock,
    thread,
    time::{Duration, Instant},
};

const SPOT: &[u8] = br#"{"tokens":[{"name":"USDC","szDecimals":8,"weiDecimals":8,"index":0,"tokenId":"0x0"},{"name":"PURR","szDecimals":0,"weiDecimals":5,"index":7,"tokenId":"0x7"}],"universe":[{"name":"@12","tokens":[7,0],"index":12}]}"#;
const PERPS: &[u8] = br#"[{"collateralToken":0,"universe":[{"name":"BTC","szDecimals":5}]}]"#;
const WAIT: Duration = Duration::from_secs(15);
#[path = "support/multi_owner_replay.rs"]
mod multi_owner_tests;
#[path = "support/native_history.rs"]
mod native_history_tests;
#[path = "support/projection.rs"]
mod projection_tests;
#[path = "support/hyperliquid_replay.rs"]
mod replay_tests;
#[path = "support/source_view.rs"]
mod source_view_tests;
fn fixture(spot: bool) -> BootstrapDto {
    fixture_with_meta(spot, SPOT, PERPS, &[1])
}
fn fixture_with_meta(
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
fn wait(mut check: impl FnMut() -> bool) {
    let until = Instant::now() + WAIT;
    while !check() {
        assert!(Instant::now() < until, "Hyperliquid causal watchdog");
        thread::sleep(Duration::from_millis(1));
    }
}
// Same deadline, predicate order and cadence as wait. Snapshot diagnostics only
// at the existing failure boundary; do not reset the clock or retry a source.
#[track_caller]
fn wait_inspect(mut check: impl FnMut() -> bool, inspect: impl FnOnce() -> String) {
    let until = Instant::now() + WAIT;
    while !check() {
        if Instant::now() >= until {
            panic!("Hyperliquid causal watchdog: {}", inspect());
        }
        thread::sleep(Duration::from_millis(1));
    }
}
fn text(payload: String, clock: i64) -> CaptureInput {
    CaptureInput::Text {
        payload: payload.into_bytes(),
        received: FrameStamp {
            receive_ts: TimestampNs(clock),
            mono_ns: clock as u64,
        },
    }
}
fn inputs(dto: &BootstrapDto) -> Vec<(u64, CaptureInput)> {
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
fn boot(
    dto: BootstrapDto,
    path: &std::path::Path,
) -> (FeedBatchActor, HistoryJournalOwner, OperationIdentity) {
    let actor = FeedBatchActor::spawn(
        FeedWriter::create_profile(path, 64 << 20, FeedProfile::EngineCausalV3).unwrap(),
        BatchConfig {
            max_pending_operations: 32,
            max_pending_bytes: 8 << 20,
            max_group_operations: 32,
            max_group_bytes: 8 << 20,
            max_age: Duration::from_millis(1),
        },
    )
    .unwrap();
    let committed = PreparedBootstrap::new_history(dto, MAX_OPERATION_BYTES)
        .unwrap()
        .try_admit(&actor.handle())
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(
        committed.receipt().first_sequence,
        committed.receipt().last_sequence
    );
    let session = committed.instantiate().unwrap();
    let identity = session.identity().clone();
    (actor, session.spawn_history_owner().unwrap(), identity)
}
fn exercise(spot: bool) {
    let dto = fixture(spot);
    // The context wire is shared, but its semantic channels remain separate.
    assert_eq!(dto.protocol_topics.len(), 5);
    let factory = HyperliquidFactory { spot };
    let mut reference = CaptureSession::new(
        factory
            .create_session(
                dto.session.session_spec().unwrap(),
                dto.catalog.catalog().unwrap(),
            )
            .unwrap(),
        dto.capture.config().unwrap(),
    )
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let (actor, owner, identity) = boot(dto.clone(), &path);
    let mut expected = Vec::new();
    let mut released = Vec::new();
    for (connection, input) in inputs(&dto) {
        let capture = reference
            .execute_turn(input.clone(), CaptureDrainPolicy::NullDispatchTakeOutbound)
            .unwrap();
        let operation = OperationDto::from_capture(&capture, &identity).unwrap();
        assert!(operation.outcome.is_ok(), "{:?}", operation.outcome);
        let ticket = owner
            .ingress()
            .try_submit(ConnectionId(connection), input)
            .unwrap();
        assert!(matches!(ticket.wait(), InputSettlement::Committed { .. }));
        let mut result = None;
        wait(|| {
            result = owner.try_next();
            result.is_some()
        });
        let release = result.unwrap();
        assert_eq!(release.evidence(), &operation);
        released.push(release.released_items().cloned().collect::<Vec<_>>());
        expected.push(operation);
    }
    owner.close_ingress();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    let status = owner.shutdown();
    assert_eq!(status.owner.executed, expected.len() as u64);
    assert_eq!(status.owner.handed_off, expected.len() as u64);
    actor.shutdown().unwrap();
    let mut reader = FeedReader::open_profile(&path, None, FeedProfile::EngineCausalV3).unwrap();
    let Some(FeedRecord::Causal(bootstrap)) = reader.read_next().unwrap() else {
        panic!("bootstrap")
    };
    assert_eq!(decode_bootstrap(&bootstrap).unwrap(), dto);
    let mut history = OwnerHistory::from_bootstrap(
        &bootstrap,
        OwnerHistoryLimits {
            max_retained_input_bytes: 4 << 20,
            max_input_bytes: 256 << 10,
            max_record_bytes: 256 << 10,
        },
    )
    .unwrap();
    let mut completed = Vec::new();
    let mut handoffs = Vec::new();
    while let Some(record) = reader.read_next().unwrap() {
        let FeedRecord::Causal(envelope) = record else {
            panic!("causal record")
        };
        let event = decode_owner_history(&envelope, &identity).unwrap();
        if let OwnerEventDto::ExecutionCompleted { operation, .. } = &event.event {
            completed.push((**operation).clone());
        }
        if let Some(handoff) = history.apply(&envelope).unwrap() {
            handoffs.push(handoff.released_items().cloned().collect::<Vec<_>>());
        }
    }
    assert_eq!(completed, expected);
    assert_eq!(handoffs, released);
    assert_eq!(history.native_ordinal(), expected.len() as u64);
    assert_eq!(history.pending_inputs(), 0);
    assert!(history.uncertainties().is_empty());
    let market = released
        .iter()
        .flatten()
        .filter_map(|item| match item {
            ItemDto::Market(batch) => Some(batch.record().unwrap()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(market.iter().any(|record| matches!(record, FeedRecord::Batch(batch) if batch.events.iter().any(|e| matches!(&e.payload, MarketEvent::Trade(t) if t.price.0 == Fixed::new(9007199254740993123456789012345678, 18) && t.quantity.0 == Fixed::new(1, 20))))));
    assert!(market.iter().any(|record| matches!(record, FeedRecord::Batch(batch) if batch.events.iter().any(|e| matches!(e.payload, MarketEvent::Candle(_))))));
    assert_eq!(market.iter().any(|record| matches!(record, FeedRecord::Batch(batch) if batch.events.iter().any(|e| matches!(e.payload, MarketEvent::Funding(_))))), !spot);
}
#[test]
fn spot_actual_factory_persistent_owner_and_durable_history_agree() {
    exercise(true);
}
#[test]
fn perps_actual_factory_persistent_owner_and_durable_history_agree() {
    exercise(false);
}
#[test]
fn mismatched_market_policy_topics_delivery_and_catalog_fail_before_admission() {
    type Mutation = Box<dyn Fn(&mut BootstrapDto)>;
    let mutations: Vec<Mutation> = vec![
        Box::new(|d| {
            d.factory = FactoryDto::HyperliquidPublic {
                market: HyperliquidMarketDto::Perps,
            }
        }),
        Box::new(|d| d.adapter_policy = AdapterEmissionPolicyDto::CompleteNativeBundleV1),
        Box::new(|d| d.catalog.instruments[0].key.venue = "hyperliquid-perps".into()),
        Box::new(|d| d.catalog.instruments[0].key.kind = InstrumentKindDto::FutureLinear),
        Box::new(|d| d.catalog.instruments[0].status = InstrumentStatus::Delisted),
        Box::new(|d| d.catalog.instruments[0].inverse = true),
        Box::new(|d| d.protocol_topics.reverse()),
        Box::new(|d| d.protocol_topics[0].push(' ')),
        Box::new(|d| d.session.endpoint_name.push_str("?unexpected=1")),
        Box::new(|d| d.session.subscriptions[0].delivery.emit_book_deltas = true),
        Box::new(|d| d.session.subscriptions[0].delivery.emit_book_snapshots = false),
        Box::new(|d| d.session.subscriptions[0].delivery.emit_bbo = false),
        Box::new(|d| {
            d.session
                .subscriptions
                .push(d.session.subscriptions[0].clone())
        }),
        Box::new(|d| d.session.subscriptions[0].channel = ChannelDto::Funding),
        Box::new(|d| d.session.subscriptions[0].instrument = 99),
        Box::new(|d| d.environment = BootstrapEnvironmentDto::Sandbox),
    ];
    for (index, mutate) in mutations.into_iter().enumerate() {
        let mut dto = fixture(true);
        mutate(&mut dto);
        assert!(
            PreparedBootstrap::new_history(dto, MAX_OPERATION_BYTES).is_err(),
            "mutation {index}"
        );
    }
}

#[test]
fn malformed_late_trade_is_durable_failure_without_partial_trade_release() {
    for spot in [true, false] {
        let dto = fixture(spot);
        let coin = dto.catalog.instruments[0].key.native_symbol.clone();
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("feed");
        let (actor, owner, identity) = boot(dto, &path);
        let first = owner
            .ingress()
            .try_submit(
                ConnectionId(1),
                CaptureInput::Connected {
                    connection: ConnectionId(1),
                    now: TimestampNs(1),
                },
            )
            .unwrap();
        assert!(matches!(first.wait(), InputSettlement::Committed { .. }));
        wait(|| owner.try_next().is_some());
        let input = text(
            format!(
                r#"{{"channel":"trades","data":[{{"coin":"{coin}","side":"B","px":"10.25","sz":"1","hash":"h1","time":1,"tid":1}},{{"coin":"{coin}","side":"?","px":"11","sz":"1","hash":"h2","time":1,"tid":2}}]}}"#
            ),
            2,
        );
        assert!(matches!(
            owner
                .ingress()
                .try_submit(ConnectionId(1), input)
                .unwrap()
                .wait(),
            InputSettlement::Committed { .. }
        ));
        wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
        assert!(owner.try_next().is_none());
        assert!(owner.committed_snapshot().is_none());
        assert!(
            owner
                .ingress()
                .try_submit(
                    ConnectionId(1),
                    CaptureInput::TimerPoll {
                        now: TimestampNs(3)
                    }
                )
                .is_err()
        );
        let status = owner.shutdown();
        assert!(matches!(
            status.owner.failure,
            Some(marketfeed_daemon::causal_bootstrap::owner::OwnerFailure::Native(_))
        ));
        assert_eq!(status.owner.executed, 2);
        assert_eq!(status.owner.handed_off, 1);
        assert_eq!(status.owner.withheld, 1);
        actor.shutdown().unwrap();
        let mut reader =
            FeedReader::open_profile(&path, None, FeedProfile::EngineCausalV3).unwrap();
        let Some(FeedRecord::Causal(bootstrap)) = reader.read_next().unwrap() else {
            panic!("bootstrap")
        };
        let mut history = OwnerHistory::from_bootstrap(
            &bootstrap,
            OwnerHistoryLimits {
                max_retained_input_bytes: 4 << 20,
                max_input_bytes: 256 << 10,
                max_record_bytes: 256 << 10,
            },
        )
        .unwrap();
        let mut failed = 0;
        let mut handoffs = 0;
        while let Some(record) = reader.read_next().unwrap() {
            let FeedRecord::Causal(envelope) = record else {
                panic!("causal record")
            };
            let event = decode_owner_history(&envelope, &identity).unwrap();
            if let OwnerEventDto::ExecutionCompleted { operation, .. } = &event.event {
                if operation.outcome.is_err() {
                    failed += 1;
                    // The first valid-looking row must not escape the atomic
                    // decode failure of the entire native frame.
                    assert!(!operation.entries.iter().any(|entry| matches!(&entry.effect,
                        EffectDto::ProducedRecord(record) if matches!(record.record().unwrap(), FeedRecord::Batch(_)))));
                }
            }
            if history.apply(&envelope).unwrap().is_some() {
                handoffs += 1;
            }
        }
        assert_eq!(failed, 1);
        assert_eq!(handoffs, 1);
        assert!(history.is_terminal());
        assert!(history.uncertainties().is_empty());
        // A matching complete native Err is valid replay evidence. It must
        // not be mistaken for a replay mismatch or release its partial rows.
        let mut replay = marketfeed_daemon::causal_bootstrap::replay::HistoryReplay::open(
            &path,
            OwnerHistoryLimits {
                max_retained_input_bytes: 4 << 20,
                max_input_bytes: 256 << 10,
                max_record_bytes: 256 << 10,
            },
        )
        .unwrap();
        let mut replayed_handoffs = 0;
        while let Some(step) = replay.read_next().unwrap() {
            if step.handoff.is_some() {
                replayed_handoffs += 1;
            }
        }
        let progress = replay.progress().unwrap();
        assert_eq!(progress.native_ordinal, 2);
        assert_eq!(replayed_handoffs, 1);
        assert!(progress.terminal && progress.uncertainties.is_empty());
    }
}

#[test]
fn coordinated_coin_topics_and_catalog_edits_cannot_switch_native_market() {
    for spot in [true, false] {
        let mut dto = fixture(spot);
        let original = dto.catalog.instruments[0].key.native_symbol.clone();
        let wrong = if spot { "BTC" } else { "@12" };
        dto.catalog.instruments[0].key.native_symbol = wrong.into();
        for topic in &mut dto.protocol_topics {
            *topic = topic.replace(&original, wrong);
        }
        assert!(PreparedBootstrap::new_history(dto, MAX_OPERATION_BYTES).is_err());
        let mut dto = fixture(spot);
        dto.catalog.instruments[0].price_increment = Fixed::new(1, 17).into();
        assert!(PreparedBootstrap::new_history(dto, MAX_OPERATION_BYTES).is_err());
        let mut dto = fixture(spot);
        dto.public_inputs
            .retain(|input| input.role != HYPERLIQUID_SPOT_META_ROLE);
        assert!(PreparedBootstrap::new_history(dto, MAX_OPERATION_BYTES).is_err());
        let mut dto = fixture(spot);
        dto.public_inputs.push(dto.public_inputs[0].clone());
        assert!(PreparedBootstrap::new_history(dto, MAX_OPERATION_BYTES).is_err());
    }
    let mut dto = fixture(false);
    dto.public_inputs
        .retain(|input| input.role != HYPERLIQUID_PERP_METAS_ROLE);
    assert!(PreparedBootstrap::new_history(dto, MAX_OPERATION_BYTES).is_err());
}

#[test]
fn maximum_malformed_cadence_vector_is_rejected_before_native_construction() {
    let mut dto = fixture(false);
    let mut subscription = dto.session.subscriptions[0].clone();
    dto.session.subscriptions = (0..65536)
        .map(|seconds| {
            subscription.channel = ChannelDto::L2Book {
                depth: Some(20),
                cadence: Some(DurationDto {
                    seconds,
                    nanoseconds: 0,
                }),
            };
            subscription.clone()
        })
        .collect();
    assert!(PreparedBootstrap::new_history(dto, MAX_OPERATION_BYTES).is_err());
}

#[test]
fn builder_perp_context_subset_keeps_collateral_and_emits_only_requested_channels() {
    let mut dto = fixture(false);
    let body = br#"[{"collateralToken":0,"universe":[{"name":"BTC","szDecimals":5}]},{"collateralToken":7,"universe":[{"name":"xyz:ABC","szDecimals":2}]}]"#;
    let input = dto
        .public_inputs
        .iter_mut()
        .find(|input| input.role == HYPERLIQUID_PERP_METAS_ROLE)
        .unwrap();
    *input = PublicReproductionInputDto::new(HYPERLIQUID_PERP_METAS_ROLE, body);
    let factory = HyperliquidFactory::perps();
    let mut definitions = Vec::new();
    factory
        .parse_instruments(
            &[SPOT, body.as_slice()].map(|bytes| HttpResponse {
                status: 200,
                headers: vec![],
                body: bytes.to_vec().into(),
            }),
            &mut definitions,
        )
        .unwrap();
    let catalog = CatalogView::with_instruments(
        VenueId(22),
        CatalogVersion(7),
        definitions
            .into_iter()
            .enumerate()
            .map(|(index, definition)| {
                definition.into_instrument(InstrumentId(index as u32 + 11), CatalogVersion(7))
            })
            .collect::<Vec<_>>(),
    );
    assert_eq!(catalog.instruments[1].key.native_symbol, "xyz:ABC");
    assert_eq!(
        catalog.instruments[1].settlement,
        Some(AssetCode("PURR".into()))
    );
    assert_eq!(catalog.instruments[1].quote, AssetCode("PURR".into()));
    let spec = SessionSpec {
        endpoint_name: marketfeed_adapter_hyperliquid::WS_URL.into(),
        subscriptions: ConcreteSubscriptionSet {
            items: [Channel::Funding, Channel::OpenInterest]
                .into_iter()
                .map(|channel| ConcreteSubscription {
                    instrument: InstrumentId(12),
                    channel,
                    delivery: DeliveryOptions {
                        emit_book_snapshots: true,
                        emit_book_deltas: false,
                        emit_bbo: true,
                    },
                })
                .collect(),
        },
    };
    dto.catalog = (&catalog).into();
    dto.session = (&spec).into();
    dto.protocol_topics = HyperliquidFactory::public_plan_topics(false, &spec, &catalog).unwrap();
    assert_eq!(
        dto.protocol_topics,
        vec![r#"{"coin":"xyz:ABC","type":"activeAssetCtx"}"#]
    );
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let (actor, owner, _) = boot(dto.clone(), &path);
    let frames = [
        CaptureInput::Connected { connection: ConnectionId(1), now: TimestampNs(1) },
        text(format!(r#"{{"channel":"subscriptionResponse","data":{{"method":"subscribe","subscription":{}}}}}"#, dto.protocol_topics[0]), 2),
        text(r#"{"channel":"activeAssetCtx","data":{"coin":"xyz:ABC","ctx":{"dayNtlVlm":"100","prevDayPx":"9","markPx":"10","funding":"-0.00000125","openInterest":"12345.12345","oraclePx":"10.03"}}}"#.into(), 3),
    ];
    let mut events = Vec::new();
    for input in frames {
        assert!(matches!(
            owner
                .ingress()
                .try_submit(ConnectionId(1), input)
                .unwrap()
                .wait(),
            InputSettlement::Committed { .. }
        ));
        let mut release = None;
        wait(|| {
            release = owner.try_next();
            release.is_some()
        });
        for item in release.unwrap().released_items() {
            if let ItemDto::Market(batch) = item {
                let FeedRecord::Batch(batch) = batch.record().unwrap() else {
                    panic!("batch")
                };
                events.extend(batch.events.iter().cloned());
            }
        }
    }
    // Runner lifecycle records share the market lane. They remain visible;
    // semantic selection applies to asset-context observations.
    assert!(
        events
            .iter()
            .any(|event| matches!(event.payload, MarketEvent::VenueStatus(_)))
    );
    assert!(events.iter().all(|event| matches!(
        event.payload,
        MarketEvent::VenueStatus(_) | MarketEvent::Funding(_) | MarketEvent::OpenInterest(_)
    )));
    events.retain(|event| !matches!(event.payload, MarketEvent::VenueStatus(_)));
    assert_eq!(events.len(), 2);
    assert!(
        events
            .iter()
            .all(|event| event.instrument == Some(InstrumentId(12)) && event.venue == VenueId(22))
    );
    assert!(events.iter().any(
        |event| matches!(&event.payload, MarketEvent::Funding(f) if f.rate.0 == Fixed::new(-125, 8))
    ));
    assert!(events.iter().any(|event| matches!(&event.payload, MarketEvent::OpenInterest(oi) if oi.quantity.0 == Fixed::new(1234512345, 5))));
    owner.close_ingress();
    wait(|| owner.status().owner.phase == OwnerPhase::Stopped);
    owner.shutdown();
    actor.shutdown().unwrap();
    let mut reader = FeedReader::open_profile(&path, None, FeedProfile::EngineCausalV3).unwrap();
    let Some(FeedRecord::Causal(bootstrap)) = reader.read_next().unwrap() else {
        panic!("bootstrap")
    };
    assert_eq!(decode_bootstrap(&bootstrap).unwrap(), dto);
}
