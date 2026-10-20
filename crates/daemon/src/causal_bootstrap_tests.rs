use super::*;
use marketfeed_adapter_api::*;
use marketfeed_engine::*;
use marketfeed_model::*;
use std::{
    sync::{OnceLock, mpsc},
    time::Duration,
};

pub(super) fn fixture() -> BootstrapDto {
    static BUILD: OnceLock<BootstrapBuildDto> = OnceLock::new();
    let instruments = (1..=10)
        .map(|id| Instrument {
            id: InstrumentId(id),
            key: InstrumentKey {
                venue: VenueCode("bybit-linear".into()),
                native_symbol: format!("C{id:02}USDT"),
                kind: InstrumentKind::PerpetualLinear,
                settlement: Some(AssetCode("USDT".into())),
                expiry_ns: None,
            },
            base: AssetCode(format!("C{id:02}")),
            quote: AssetCode("USDT".into()),
            settlement: Some(AssetCode("USDT".into())),
            price_scale: 2,
            quantity_scale: 3,
            price_increment: Fixed::new(1, 2),
            quantity_increment: Fixed::new(1, 3),
            min_quantity: Some(Fixed::new(1, 3)),
            max_quantity: None,
            min_notional: None,
            contract_size: None,
            expiry_ns: None,
            status: InstrumentStatus::Active,
            inverse: false,
            catalog_version: CatalogVersion(7),
        })
        .collect::<Vec<_>>();
    let catalog = CatalogView::with_instruments(VenueId(5), CatalogVersion(9), instruments);
    let spec = SessionSpec {
        endpoint_name: "wss://stream.bybit.com/v5/public/linear".into(),
        subscriptions: ConcreteSubscriptionSet {
            items: catalog
                .instruments
                .iter()
                .flat_map(|i| {
                    [
                        Channel::Trades,
                        Channel::Quote,
                        Channel::Statistics24h,
                        Channel::MarkPrice,
                        Channel::IndexPrice,
                        Channel::Funding,
                        Channel::OpenInterest,
                        Channel::Liquidations,
                        Channel::L2Book {
                            depth: Some(50),
                            cadence: None,
                        },
                    ]
                    .into_iter()
                    .map(move |channel| ConcreteSubscription {
                        instrument: i.id,
                        channel,
                        delivery: DeliveryOptions {
                            emit_book_snapshots: true,
                            emit_book_deltas: true,
                            emit_bbo: true,
                        },
                    })
                })
                .collect(),
        },
    };
    let capture = CaptureConfig {
        venue: VenueId(5),
        session: SessionId(31),
        dispatch_capacity: 512,
        overflow: OverflowPolicy::FailEngine,
        max_bytes: 8 * 1024 * 1024,
        max_entries: 8192,
    };
    BootstrapDto {
        schema_version: 2,
        build: BUILD.get_or_init(|| current_build().unwrap()).clone(),
        factory: FactoryDto::BybitPublic {
            category: BybitCategoryDto::Linear,
            enable_l2: true,
        },
        environment: BootstrapEnvironmentDto::Production,
        output_profile: OutputProfileDto::DiskNullDispatchV1,
        adapter_policy: BybitEmissionPolicyDto::CompleteNativeBundleV1,
        protocol_topics: BybitFactory::public_plan_topics(
            BybitCategory::Linear,
            true,
            &spec,
            &catalog,
        )
        .unwrap(),
        session: (&spec).into(),
        catalog: (&catalog).into(),
        capture: capture.into(),
        runner_policy: capture.runtime_policy(),
        owner_policy: Some(JournalOwnerPolicyDto::default()),
        public_inputs: vec![PublicReproductionInputDto::new(
            "public-catalog-response",
            b"fixture",
        )],
    }
}
fn new_actor(
    path: &std::path::Path,
    profile: FeedProfile,
    limit: u64,
    bytes: usize,
) -> FeedBatchActor {
    FeedBatchActor::spawn(
        FeedWriter::create_profile(path, limit, profile).unwrap(),
        BatchConfig {
            max_pending_operations: 8,
            max_pending_bytes: bytes,
            max_group_operations: 8,
            max_group_bytes: bytes,
            max_age: Duration::from_secs(3600),
        },
    )
    .unwrap()
}
fn stamp() -> FrameStamp {
    FrameStamp {
        receive_ts: TimestampNs(1_000_000),
        mono_ns: 7,
    }
}
fn invalidation_capture(dto: &BootstrapDto) -> CapturedOperation {
    let FactoryDto::BybitPublic {
        enable_l2,
        category: selected,
    } = dto.factory
    else {
        panic!("Bybit fixture")
    };
    let factory = BybitFactory {
        category: category(selected),
        enable_l2,
    };
    let machine = factory
        .create_session(
            dto.session.session_spec().unwrap(),
            dto.catalog.catalog().unwrap(),
        )
        .unwrap();
    let mut session = CaptureSession::new(machine, dto.capture.config().unwrap()).unwrap();
    session
        .execute_turn(
            CaptureInput::Connected {
                connection: ConnectionId(44),
                now: TimestampNs(0),
            },
            CaptureDrainPolicy::NullDispatchTakeOutbound,
        )
        .unwrap();
    for row in &dto.catalog.instruments {
        let text = format!(
            r#"{{"topic":"orderbook.1.{0}","type":"snapshot","ts":1,"data":{{"s":"{0}","b":[["100","1"]],"a":[["101","2"]],"u":1,"seq":1}}}}"#,
            row.key.native_symbol
        );
        session
            .execute_turn(
                CaptureInput::Text {
                    payload: text.into_bytes(),
                    received: stamp(),
                },
                CaptureDrainPolicy::NullDispatchTakeOutbound,
            )
            .unwrap();
    }
    session
        .execute_turn(
            CaptureInput::Text {
                payload: b"{}".to_vec(),
                received: stamp(),
            },
            CaptureDrainPolicy::NullDispatchTakeOutbound,
        )
        .unwrap()
}

#[test]
fn fresh_actual_factories_have_identical_ordered_invalidation_capture() {
    let dto = fixture();
    let expected = invalidation_capture(&dto);
    let mut books = Vec::new();
    let mut quotes = Vec::new();
    for entry in &expected.entries {
        if let CaptureEntry::ProducedRecord(bytes) = &entry.effect {
            if let FeedRecord::System { event, .. } = serde_json::from_slice(bytes).unwrap() {
                match event {
                    SystemEvent::BookInvalidated { instrument, .. } => books.push(instrument.0),
                    SystemEvent::QuoteInvalidated { instrument, .. } => quotes.push(instrument.0),
                    _ => {}
                }
            }
        }
    }
    assert_eq!(books, (1..=10).collect::<Vec<_>>());
    assert_eq!(quotes, books);
    for _ in 0..8 {
        assert_eq!(invalidation_capture(&dto), expected);
    }
}

#[test]
fn full_exact_bootstrap_and_ordered_protocol_roundtrip() {
    let mut dto = fixture();
    dto.public_inputs.push(PublicReproductionInputDto::new(
        "binary-public-input",
        b"\0\xff\x80",
    ));
    dto.catalog.instruments[0].price_increment = FixedDto {
        coefficient: i128::MIN.to_string(),
        scale: 255,
    };
    dto.catalog.instruments[0].contract_size = Some(FixedDto {
        coefficient: i128::MAX.to_string(),
        scale: 254,
    });
    let channels = vec![
        Channel::Trades,
        Channel::AllTrades,
        Channel::Quote,
        Channel::L2Book {
            depth: Some(u32::MAX),
            cadence: Some(Duration::new(u64::MAX, 999_999_999)),
        },
        Channel::L3Book,
        Channel::MarkPrice,
        Channel::IndexPrice,
        Channel::Funding,
        Channel::OpenInterest,
        Channel::Liquidations,
        Channel::Statistics24h,
        Channel::InstrumentStatus,
        Channel::Candles {
            interval: CandleInterval::M1,
        },
        Channel::Candles {
            interval: CandleInterval::M5,
        },
        Channel::Candles {
            interval: CandleInterval::M15,
        },
        Channel::Candles {
            interval: CandleInterval::H1,
        },
        Channel::Candles {
            interval: CandleInterval::D1,
        },
    ];
    dto.session.subscriptions = channels
        .iter()
        .enumerate()
        .map(|(i, c)| SubscriptionDto {
            instrument: 1,
            channel: c.into(),
            delivery: DeliveryDto {
                emit_book_snapshots: i % 2 == 0,
                emit_book_deltas: i % 3 == 0,
                emit_bbo: i % 4 == 0,
            },
        })
        .collect();
    let native = dto.session.session_spec().unwrap();
    assert_eq!(
        native
            .subscriptions
            .items
            .iter()
            .map(|s| s.channel.clone())
            .collect::<Vec<_>>(),
        channels
    );
    let envelope = encode_bootstrap(&dto, MAX_OPERATION_BYTES).unwrap();
    assert_eq!(decode_bootstrap(&envelope).unwrap(), dto);
    assert_eq!(
        bootstrap_sha256(&envelope.payload().unwrap()),
        envelope.route.bootstrap_id
    );
    assert_eq!(SessionSpecDto::from(&native), dto.session);
    let mut reordered = dto.clone();
    reordered.session.subscriptions.reverse();
    assert_ne!(
        encode_bootstrap(&reordered, MAX_OPERATION_BYTES)
            .unwrap()
            .route
            .bootstrap_id,
        envelope.route.bootstrap_id
    );
    let bound = envelope.payload().unwrap().len();
    assert!(encode_bootstrap(&dto, bound).is_ok());
    assert!(matches!(
        encode_bootstrap(&dto, bound - 1),
        Err(CodecError::TooLarge)
    ));
}

#[test]
fn hash_canonical_route_and_unknown_schema_are_verified_before_factories() {
    let dto = fixture();
    let envelope = encode_bootstrap(&dto, MAX_OPERATION_BYTES).unwrap();
    let mut changed = envelope.clone();
    changed.route.bootstrap_id = "0".repeat(64);
    assert!(decode_bootstrap(&changed).is_err());
    let mut changed = envelope.clone();
    changed.route.session += 1;
    assert!(decode_bootstrap(&changed).is_err());
    let mut payload = envelope.payload().unwrap();
    payload.push(b' ');
    let mut route = envelope.route.clone();
    route.bootstrap_id = bootstrap_sha256(&payload);
    assert!(
        decode_bootstrap(&CausalEnvelope::new(CausalPhase::Bootstrap, route, &payload).unwrap())
            .is_err()
    );
    for (from, to) in [
        ("\"schema_version\":2", "\"schema_version\":3"),
        ("\"disk_null_dispatch_v1\"", "\"external_sink\""),
        ("\"bybit_public\"", "\"hyperliquid\""),
    ] {
        let bytes = String::from_utf8(envelope.payload().unwrap())
            .unwrap()
            .replacen(from, to, 1)
            .into_bytes();
        let mut route = envelope.route.clone();
        route.bootstrap_id = bootstrap_sha256(&bytes);
        assert!(
            PreparedBootstrap::from_envelope(
                CausalEnvelope::new(CausalPhase::Bootstrap, route, &bytes).unwrap()
            )
            .is_err()
        );
    }
}

#[test]
fn artifact_observation_hashes_actual_file_and_records_selected_features() {
    let observed = current_build().unwrap();
    let bytes = std::fs::read(std::env::current_exe().unwrap()).unwrap();
    assert_eq!(observed.executable_sha256, bootstrap_sha256(&bytes));
    assert_eq!(observed.executable_bytes, bytes.len() as u64);
    assert_eq!(
        observed.compiled.lock_sha256,
        bootstrap_sha256(include_bytes!("../../../Cargo.lock"))
    );
    assert_eq!(observed.compiled.package_version, env!("CARGO_PKG_VERSION"));
    assert_eq!(
        observed
            .compiled
            .package_features
            .iter()
            .find(|f| f.package == "marketfeed-engine")
            .unwrap()
            .features,
        marketfeed_engine::compiled_features()
    );
    assert_eq!(
        observed
            .compiled
            .package_features
            .iter()
            .find(|f| f.package == "marketfeed-adapter-bybit")
            .unwrap()
            .features,
        marketfeed_adapter_bybit::compiled_features()
    );
    assert!(observed.compiled.source_files > 100);
    assert_eq!(
        observed
            .compiled
            .package_features
            .iter()
            .find(|f| f.package == "marketfeed-adapter-hyperliquid")
            .unwrap()
            .features,
        marketfeed_adapter_hyperliquid::compiled_features()
    );
}

#[test]
fn unsupported_configuration_and_wrong_artifact_fail_before_admission() {
    type Mutation = Box<dyn Fn(&mut BootstrapDto)>;
    let mutations: Vec<Mutation> = vec![
        Box::new(|d| d.build.executable_sha256 = "a".repeat(64)),
        Box::new(|d| d.build.compiled.source_sha256 = "a".repeat(64)),
        Box::new(|d| d.environment = BootstrapEnvironmentDto::Sandbox),
        Box::new(|d| d.session.endpoint_name = "https://api.bybit.com".into()),
        Box::new(|d| {
            d.session.endpoint_name = "wss://stream.bybit.com/v5/public/linear?secret=x".into()
        }),
        Box::new(|d| d.capture.session = 0),
        Box::new(|d| d.capture.venue = 6),
        Box::new(|d| d.capture.max_bytes = u64::MAX),
        Box::new(|d| d.capture.overflow = OverflowDto::SpillToDisk),
        Box::new(|d| d.runner_policy.max_timers += 1),
        Box::new(|d| d.public_inputs[0].bytes += 1),
        Box::new(|d| d.public_inputs[0].sha256 = "a".repeat(64)),
        Box::new(|d| d.catalog.instruments[1].id = d.catalog.instruments[0].id),
        Box::new(|d| {
            d.catalog.instruments[1].key.native_symbol =
                d.catalog.instruments[0].key.native_symbol.clone()
        }),
        Box::new(|d| d.session.subscriptions[0].channel = ChannelDto::AllTrades),
        Box::new(|d| {
            d.session.subscriptions[0].channel = ChannelDto::L2Book {
                depth: Some(37),
                cadence: None,
            }
        }),
        Box::new(|d| {
            d.session.subscriptions[0].channel = ChannelDto::L2Book {
                depth: Some(200),
                cadence: None,
            }
        }),
        Box::new(|d| d.session.subscriptions[0].delivery.emit_bbo = false),
        Box::new(|d| {
            d.session
                .subscriptions
                .retain(|s| s.channel == ChannelDto::Quote)
        }),
        Box::new(|d| d.protocol_topics.reverse()),
        Box::new(|d| d.protocol_topics[0].push_str("-changed")),
        Box::new(|d| d.session.subscriptions.clear()),
    ];
    for mutate in mutations {
        let mut dto = fixture();
        mutate(&mut dto);
        assert!(PreparedBootstrap::new(dto, MAX_OPERATION_BYTES).is_err());
    }
    assert!(PreparedBootstrap::new(fixture(), 1).is_err());
}

#[test]
fn actual_bootstrap_receipt_precedes_factory_and_initialized_owner() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = new_actor(&path, FeedProfile::EngineCausalV1, 16_000_000, 8_000_000);
    let handle = actor.handle();
    let prepared = PreparedBootstrap::new(fixture(), MAX_OPERATION_BYTES).unwrap();
    let expected = prepared.identity().clone();
    let pending = prepared.try_admit(&handle).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        entered_tx.send(()).unwrap();
        let started = std::time::Instant::now();
        let committed = pending.wait().unwrap();
        let receipt_elapsed = started.elapsed();
        eprintln!(
            "bootstrap test durable receipt observed at {receipt_elapsed:?}; verifying artifact/factory"
        );
        let owner = committed.instantiate().unwrap();
        eprintln!(
            "bootstrap test receipt={receipt_elapsed:?}, total including artifact verification={:?}",
            started.elapsed()
        );
        done_tx.send(owner).unwrap();
    });
    entered_rx.recv().unwrap();
    assert!(matches!(
        done_rx.recv_timeout(Duration::from_millis(20)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    assert_eq!(handle.status().committed_operations, 0);
    assert!(
        FeedReader::open_profile(&path, None, FeedProfile::EngineCausalV1)
            .unwrap()
            .read_next()
            .unwrap()
            .is_none()
    );
    handle.close();
    // A test watchdog, not a runtime/admission deadline. Artifact verification
    // hashes the real test executable; hosted ARM with parallel hash-heavy
    // cases exceeded the old 10s watchdog. Preserve the pre-ACK assertions and
    // log both phases so future failures can distinguish receipt from factory.
    let mut owner = done_rx.recv_timeout(Duration::from_secs(120)).unwrap();
    thread.join().unwrap();
    let report = actor.shutdown().unwrap();
    assert_eq!(owner.identity(), &expected);
    assert_eq!(owner.bootstrap_receipt().published, report.published);
    assert!(!owner.is_poisoned());
    let op = owner
        .capture
        .execute_turn(
            CaptureInput::Connected {
                connection: ConnectionId(901),
                now: TimestampNs(0),
            },
            CaptureDrainPolicy::NullDispatchTakeOutbound,
        )
        .unwrap();
    assert_eq!(op.ordinal, 1);
    assert_eq!(op.generation, 1);
    assert!(matches!(op.outcome, CaptureOutcome::Returned(Ok(()))));
    let mut reader = FeedReader::open_profile(&path, None, FeedProfile::EngineCausalV1).unwrap();
    let Some(FeedRecord::Causal(envelope)) = reader.read_next().unwrap() else {
        panic!("bootstrap")
    };
    assert_eq!(decode_bootstrap(&envelope).unwrap(), fixture());
}

#[test]
fn actual_full_admission_retains_identical_prepared_bootstrap_for_retry() {
    let temp = tempfile::tempdir().unwrap();
    let first = PreparedBootstrap::new(fixture(), MAX_OPERATION_BYTES).unwrap();
    let mut second_dto = fixture();
    second_dto.capture.session += 1;
    let second = PreparedBootstrap::new(second_dto, MAX_OPERATION_BYTES).unwrap();
    assert_eq!(first.encoded_bytes_bound(), second.encoded_bytes_bound());
    let cap = first.encoded_bytes_bound() * 2 - 1;
    let actor = new_actor(
        &temp.path().join("first"),
        FeedProfile::EngineCausalV1,
        16_000_000,
        cap,
    );
    let handle = actor.handle();
    let pending = first.try_admit(&handle).unwrap();
    let identity = second.identity().clone();
    let bytes = second.encoded_bytes_bound();
    let rejected = second.try_admit(&handle).unwrap_err();
    assert_eq!(rejected.reason, AdmissionFailure::Full);
    let retry = rejected.into_prepared();
    assert_eq!(retry.identity(), &identity);
    assert_eq!(retry.encoded_bytes_bound(), bytes);
    handle.close();
    pending.wait().unwrap();
    actor.shutdown().unwrap();
    let next = new_actor(
        &temp.path().join("retry"),
        FeedProfile::EngineCausalV1,
        16_000_000,
        8_000_000,
    );
    let handle = next.handle();
    let accepted = retry.try_admit(&handle).unwrap();
    handle.close();
    let committed = accepted.wait().unwrap();
    assert_eq!(committed.identity, identity);
    next.shutdown().unwrap();
}

#[test]
fn profile_failure_and_failed_publication_cannot_create_owner() {
    let temp = tempfile::tempdir().unwrap();
    let legacy = new_actor(
        &temp.path().join("legacy"),
        FeedProfile::Legacy,
        16_000_000,
        8_000_000,
    );
    let handle = legacy.handle();
    let rejected = PreparedBootstrap::new(fixture(), MAX_OPERATION_BYTES)
        .unwrap()
        .try_admit(&handle)
        .unwrap_err();
    assert_eq!(rejected.reason, AdmissionFailure::ProfileMismatch);
    legacy.shutdown().unwrap();
    let failed = new_actor(
        &temp.path().join("failed"),
        FeedProfile::EngineCausalV1,
        64,
        8_000_000,
    );
    let handle = failed.handle();
    let pending = rejected.into_prepared().try_admit(&handle).unwrap();
    handle.close();
    let Err(BootstrapError::Commit(error)) = pending.wait() else {
        panic!("expected failed bootstrap publication")
    };
    assert!(matches!(
        error.disposition,
        CommitDisposition::Attempted { .. }
    ));
    assert!(failed.shutdown().is_err());
}

#[test]
fn dropping_pending_waiter_does_not_cancel_accepted_bootstrap() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("feed");
    let actor = new_actor(&path, FeedProfile::EngineCausalV1, 16_000_000, 8_000_000);
    let handle = actor.handle();
    drop(
        PreparedBootstrap::new(fixture(), MAX_OPERATION_BYTES)
            .unwrap()
            .try_admit(&handle)
            .unwrap(),
    );
    let report = actor.shutdown().unwrap();
    assert_eq!(report.committed_operations, 1);
    let mut reader = FeedReader::open_profile(&path, None, FeedProfile::EngineCausalV1).unwrap();
    let Some(FeedRecord::Causal(envelope)) = reader.read_next().unwrap() else {
        panic!("bootstrap")
    };
    assert_eq!(decode_bootstrap(&envelope).unwrap(), fixture());
}

#[test]
fn every_supported_category_constructs_only_after_real_bootstrap_commit() {
    let temp = tempfile::tempdir().unwrap();
    let actor = new_actor(
        &temp.path().join("feed"),
        FeedProfile::EngineCausalV1,
        16_000_000,
        8_000_000,
    );
    let handle = actor.handle();
    let mut pending = Vec::new();
    for (category, venue, code, kind, inverse) in [
        (
            BybitCategoryDto::Spot,
            6,
            "spot",
            InstrumentKindDto::Spot,
            false,
        ),
        (
            BybitCategoryDto::Linear,
            5,
            "linear",
            InstrumentKindDto::PerpetualLinear,
            false,
        ),
        (
            BybitCategoryDto::Inverse,
            11,
            "inverse",
            InstrumentKindDto::PerpetualInverse,
            true,
        ),
    ] {
        let mut dto = fixture();
        dto.factory = FactoryDto::BybitPublic {
            category,
            enable_l2: true,
        };
        dto.capture.venue = venue;
        dto.capture.session = 100 + venue as u64;
        dto.catalog.venue = venue;
        dto.session.endpoint_name = format!("wss://stream.bybit.com/v5/public/{code}");
        for row in &mut dto.catalog.instruments {
            row.key.venue = format!("bybit-{code}");
            row.key.kind = kind;
            row.inverse = inverse;
            if inverse {
                row.contract_size = Some(Fixed::new(1, 0).into());
                row.quote = "USD".into();
                row.settlement = Some(row.base.clone());
                row.key.settlement = row.settlement.clone();
            }
            if kind == InstrumentKindDto::Spot {
                row.settlement = None;
                row.key.settlement = None;
            }
        }
        if category == BybitCategoryDto::Spot {
            dto.session.subscriptions.retain(|s| {
                matches!(
                    s.channel,
                    ChannelDto::Trades
                        | ChannelDto::Quote
                        | ChannelDto::Statistics24h
                        | ChannelDto::L2Book { .. }
                        | ChannelDto::Candles { .. }
                )
            });
        }
        dto.protocol_topics = BybitFactory::public_plan_topics(
            super::category(category),
            true,
            &dto.session.session_spec().unwrap(),
            &dto.catalog.catalog().unwrap(),
        )
        .unwrap();
        pending.push(
            PreparedBootstrap::new(dto, MAX_OPERATION_BYTES)
                .unwrap()
                .try_admit(&handle)
                .unwrap(),
        );
    }
    assert_eq!(handle.status().committed_operations, 0);
    handle.close();
    for ticket in pending {
        let mut owner = ticket.wait().unwrap().instantiate().unwrap();
        let identity = owner.identity().clone();
        let operation = owner
            .capture
            .execute_turn(
                CaptureInput::Connected {
                    connection: ConnectionId(44),
                    now: TimestampNs(0),
                },
                CaptureDrainPolicy::NullDispatchTakeOutbound,
            )
            .unwrap();
        let envelope = encode_operation(&operation, &identity, MAX_OPERATION_BYTES).unwrap();
        assert!(
            decode_operation(&envelope, &identity)
                .unwrap()
                .matches_capture(&operation, &identity)
                .unwrap()
        );
        assert!(matches!(
            operation.outcome,
            CaptureOutcome::Returned(Ok(()))
        ));
    }
    assert_eq!(actor.shutdown().unwrap().committed_operations, 3);
}
