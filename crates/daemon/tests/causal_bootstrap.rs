//! Independent public-API bootstrap policy, byte binding, and admission tests.
#[cfg(not(feature = "causal-journal"))]
#[test]
fn default_build_rejects_causal_profile_explicitly() {
    use marketfeed_daemon::causal_bootstrap::{BootstrapError, current_build};
    assert!(matches!(
        current_build(),
        Err(BootstrapError::Unsupported(
            "causal-journal feature disabled"
        ))
    ));
}

#[cfg(feature = "causal-journal")]
mod enabled {
    use marketfeed_daemon::causal_bootstrap::{PreparedBootstrap, current_build};
    use marketfeed_engine::{CaptureConfig, causal::*};
    use marketfeed_model::*;
    use marketfeed_recording::{shared_batch::*, shared_feed::*};
    use std::{sync::OnceLock, time::Duration};

    fn fixture(session: u64) -> BootstrapDto {
        static BUILD: OnceLock<BootstrapBuildDto> = OnceLock::new();
        let catalog = CatalogView::with_instruments(
            VenueId(6),
            CatalogVersion(9),
            ["BTCUSDT", "ETHUSDT"]
                .into_iter()
                .enumerate()
                .map(|(index, symbol)| Instrument {
                    id: InstrumentId(index as u32 + 1),
                    key: InstrumentKey {
                        venue: VenueCode("bybit-spot".into()),
                        native_symbol: symbol.into(),
                        kind: InstrumentKind::Spot,
                        settlement: None,
                        expiry_ns: None,
                    },
                    base: AssetCode(symbol[..3].into()),
                    quote: AssetCode("USDT".into()),
                    settlement: None,
                    price_scale: 2,
                    quantity_scale: 3,
                    price_increment: Fixed::new(1, 2),
                    quantity_increment: Fixed::new(1, 3),
                    min_quantity: None,
                    max_quantity: None,
                    min_notional: None,
                    contract_size: None,
                    expiry_ns: None,
                    status: InstrumentStatus::Active,
                    inverse: false,
                    catalog_version: CatalogVersion(9),
                })
                .collect::<Vec<_>>(),
        );
        let capture = CaptureConfig {
            venue: catalog.venue,
            session: SessionId(session),
            dispatch_capacity: 32,
            overflow: OverflowPolicy::FailEngine,
            max_bytes: 1024 * 1024,
            max_entries: 1024,
        };
        BootstrapDto {
            schema_version: 2,
            build: BUILD.get_or_init(|| current_build().unwrap()).clone(),
            factory: FactoryDto::BybitPublic {
                category: BybitCategoryDto::Spot,
                enable_l2: false,
            },
            environment: BootstrapEnvironmentDto::Production,
            output_profile: OutputProfileDto::DiskNullDispatchV1,
            adapter_policy: BybitEmissionPolicyDto::CompleteNativeBundleV1,
            // Explicit native topics independently specify the supported spot bundle.
            protocol_topics: topics(false),
            session: SessionSpecDto {
                endpoint_name: "wss://stream.bybit.com/v5/public/spot".into(),
                subscriptions: [1, 2]
                    .into_iter()
                    .flat_map(|instrument| {
                        [
                            ChannelDto::Trades,
                            ChannelDto::Quote,
                            ChannelDto::Statistics24h,
                        ]
                        .into_iter()
                        .map(move |channel| SubscriptionDto {
                            instrument,
                            channel,
                            delivery: DeliveryDto {
                                emit_book_snapshots: true,
                                emit_book_deltas: true,
                                emit_bbo: true,
                            },
                        })
                    })
                    .collect(),
            },
            catalog: (&catalog).into(),
            capture: capture.into(),
            runner_policy: capture.runtime_policy(),
            owner_policy: Some(JournalOwnerPolicyDto::default()),
            public_inputs: vec![PublicReproductionInputDto::new(
                "public-original-bytes",
                b"native\0\xff",
            )],
        }
    }
    fn topics(candles: bool) -> Vec<String> {
        ["BTCUSDT", "ETHUSDT"]
            .into_iter()
            .flat_map(|symbol| {
                let mut topics = vec![
                    format!("publicTrade.{symbol}"),
                    format!("orderbook.1.{symbol}"),
                    format!("tickers.{symbol}"),
                ];
                if candles {
                    topics.push(format!("kline.1.{symbol}"));
                }
                topics
            })
            .collect()
    }
    fn actor(path: &std::path::Path, pending_bytes: usize) -> FeedBatchActor {
        FeedBatchActor::spawn(
            FeedWriter::create_profile(path, 32_000_000, FeedProfile::EngineCausalV1).unwrap(),
            BatchConfig {
                max_pending_operations: 8,
                max_pending_bytes: pending_bytes,
                max_group_operations: 8,
                max_group_bytes: pending_bytes,
                max_age: Duration::from_secs(3600),
            },
        )
        .unwrap()
    }
    fn read_bootstrap(path: &std::path::Path) -> CausalEnvelope {
        let mut reader = FeedReader::open_profile(path, None, FeedProfile::EngineCausalV1).unwrap();
        let Some(FeedRecord::Causal(envelope)) = reader.read_next().unwrap() else {
            panic!("expected committed bootstrap")
        };
        assert!(reader.read_next().unwrap().is_none());
        envelope
    }

    #[test]
    fn native_bundle_and_embedded_bytes_are_bound_to_actual_bootstrap_ack() {
        let dto = fixture(31);
        let expected = encode_bootstrap(&dto, MAX_OPERATION_BYTES).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("feed");
        let actor = actor(&path, 16_000_000);
        let handle = actor.handle();
        let pending = PreparedBootstrap::new(dto.clone(), MAX_OPERATION_BYTES)
            .unwrap()
            .try_admit(&handle)
            .unwrap();
        assert_eq!(handle.status().committed_operations, 0);
        handle.close();
        let committed = pending.wait().unwrap();
        let receipt = committed.receipt().clone();
        assert_eq!(receipt.first_sequence, receipt.last_sequence);
        let owner = committed.instantiate().unwrap();
        assert_eq!(owner.identity().bootstrap_id, expected.route.bootstrap_id);
        assert_eq!(owner.bootstrap_receipt(), &receipt);
        assert!(!owner.is_poisoned());
        assert_eq!(actor.shutdown().unwrap().committed_operations, 1);
        let observed = read_bootstrap(&path);
        assert_eq!(observed.payload().unwrap(), expected.payload().unwrap());
        let decoded = decode_bootstrap(&observed).unwrap();
        assert_eq!(decoded, dto);
        assert_eq!(
            decoded.public_inputs[0].payload.bytes().unwrap(),
            b"native\0\xff"
        );
    }

    #[test]
    fn self_consistent_outer_hash_cannot_admit_partial_policy_or_changed_repro_bytes() {
        type Mutation = Box<dyn Fn(&mut BootstrapDto)>;
        let mutations: Vec<Mutation> = vec![
            Box::new(|dto| {
                dto.session.subscriptions.remove(0);
            }),
            Box::new(|dto| dto.session.subscriptions[0].delivery.emit_bbo = false),
            Box::new(|dto| dto.protocol_topics.swap(0, 1)),
            Box::new(|dto| dto.public_inputs[0].bytes += 1),
            Box::new(|dto| {
                dto.public_inputs[0].payload = serde_json::from_str("\"AA==\"").unwrap();
            }),
            Box::new(|dto| {
                dto.session.subscriptions.push(SubscriptionDto {
                    instrument: 1,
                    channel: ChannelDto::Candles {
                        interval: CandleIntervalDto::M1,
                    },
                    delivery: DeliveryDto {
                        emit_book_snapshots: true,
                        emit_book_deltas: true,
                        emit_bbo: true,
                    },
                });
                // Actual legacy factory expands that one candle to both symbols.
                dto.protocol_topics = topics(true);
            }),
        ];
        for mutation in mutations {
            let mut dto = fixture(31);
            mutation(&mut dto);
            let payload = serde_json::to_vec(&dto).unwrap();
            let envelope = CausalEnvelope::new(
                CausalPhase::Bootstrap,
                CausalRoute {
                    venue: dto.capture.venue,
                    session: dto.capture.session,
                    ordinal: 0,
                    generation: 0,
                    bootstrap_id: bootstrap_sha256(&payload),
                },
                &payload,
            )
            .unwrap();
            assert!(PreparedBootstrap::from_envelope(envelope).is_err());
        }
    }

    #[test]
    fn full_rejection_preserves_exact_bootstrap_payload_until_retry() {
        let temp = tempfile::tempdir().unwrap();
        let first = PreparedBootstrap::new(fixture(31), MAX_OPERATION_BYTES).unwrap();
        let dto = fixture(32);
        let expected = encode_bootstrap(&dto, MAX_OPERATION_BYTES).unwrap();
        let second = PreparedBootstrap::new(dto, MAX_OPERATION_BYTES).unwrap();
        assert_eq!(first.encoded_bytes_bound(), second.encoded_bytes_bound());
        let first_actor = actor(
            &temp.path().join("first"),
            first.encoded_bytes_bound() * 2 - 1,
        );
        let handle = first_actor.handle();
        let pending = first.try_admit(&handle).unwrap();
        let rejected = second.try_admit(&handle).unwrap_err();
        assert_eq!(rejected.reason, AdmissionFailure::Full);
        let retry = rejected.into_prepared();
        handle.close();
        pending.wait().unwrap();
        first_actor.shutdown().unwrap();
        let retry_path = temp.path().join("retry");
        let retry_actor = actor(&retry_path, 16_000_000);
        let handle = retry_actor.handle();
        let pending = retry.try_admit(&handle).unwrap();
        handle.close();
        let owner = pending.wait().unwrap().instantiate().unwrap();
        assert_eq!(owner.identity().session, 32);
        retry_actor.shutdown().unwrap();
        let observed = read_bootstrap(&retry_path);
        assert_eq!(observed.route, expected.route);
        assert_eq!(observed.payload_base64, expected.payload_base64);
    }

    #[test]
    fn owner_history_profile_roundtrips_actual_bound_bootstrap_and_prefix_uncertainty() {
        let dto = fixture(55);
        let profile = FeedProfile::EngineCausalV2;
        let bootstrap = encode_bootstrap_for_profile(&dto, MAX_OPERATION_BYTES, profile).unwrap();
        let limits = OwnerHistoryLimits {
            max_retained_input_bytes: 16 * 1024 * 1024,
            max_input_bytes: 2 * 1024 * 1024,
            max_record_bytes: MAX_OPERATION_BYTES,
        };
        let mut history = OwnerHistory::from_bootstrap(&bootstrap, limits.clone()).unwrap();
        assert!(
            OwnerHistory::from_bootstrap(
                &encode_bootstrap(&dto, MAX_OPERATION_BYTES).unwrap(),
                limits.clone()
            )
            .is_err()
        );
        assert!(matches!(
            PreparedBootstrap::from_envelope(bootstrap.clone()),
            Err(
                marketfeed_daemon::causal_bootstrap::BootstrapError::Unsupported(
                    "owner-history journal is not yet wired to the daemon owner"
                )
            )
        ));
        let identity = history.identity().clone();
        let event = |ordinal, observed_ns, event| {
            encode_owner_history(
                &OwnerHistoryDto {
                    schema_version: 1,
                    route: CausalRoute {
                        venue: identity.venue,
                        session: identity.session,
                        bootstrap_id: identity.bootstrap_id.clone(),
                        ordinal,
                        generation: 0,
                    },
                    observed_ns,
                    event,
                },
                &identity,
                MAX_OPERATION_BYTES,
            )
            .unwrap()
        };
        let started = event(1, 0, OwnerEventDto::Started { unix_ns: i64::MIN });
        let ingress = event(
            2,
            1,
            OwnerEventDto::ingress(
                1,
                ConnectionId(99),
                1000,
                1024,
                &marketfeed_engine::CaptureInput::Connected {
                    connection: ConnectionId(99),
                    now: TimestampNs(7),
                },
                &identity,
            )
            .unwrap(),
        );
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("owner-profile");
        let actor = FeedBatchActor::spawn(
            FeedWriter::create_profile(&path, 32_000_000, profile).unwrap(),
            BatchConfig {
                max_pending_operations: 8,
                max_pending_bytes: 16_000_000,
                max_group_operations: 8,
                max_group_bytes: 16_000_000,
                max_age: Duration::from_secs(3600),
            },
        )
        .unwrap();
        let handle = actor.handle();
        let ticket = handle
            .try_submit(
                PreparedFeedTransaction::new(vec![
                    FeedRecord::Causal(bootstrap.clone()),
                    FeedRecord::Causal(started),
                    FeedRecord::Causal(ingress),
                    FeedRecord::Seal {
                        schema_version: 1,
                        now_ns: 9,
                        reason: "test".into(),
                    },
                ])
                .unwrap(),
            )
            .unwrap();
        handle.close();
        let receipt = ticket.wait().unwrap();
        assert_eq!((receipt.first_sequence, receipt.last_sequence), (1, 4));
        actor.shutdown().unwrap();
        let mut reader = FeedReader::open_profile(&path, None, profile).unwrap();
        let Some(FeedRecord::Causal(observed_bootstrap)) = reader.read_next().unwrap() else {
            panic!()
        };
        assert_eq!(observed_bootstrap, bootstrap);
        let Some(FeedRecord::Causal(started)) = reader.read_next().unwrap() else {
            panic!()
        };
        history.apply(&started).unwrap();
        let checkpoint = history.clone();
        let cursor = reader.cursor().clone();
        let Some(FeedRecord::Causal(ingress)) = reader.read_next().unwrap() else {
            panic!()
        };
        assert!(history.apply(&ingress).unwrap().is_none());
        assert_eq!(
            history.uncertainties(),
            [OwnerPrefixUncertainty::ExecutionUnknown { input_id: 1 }]
        );
        let mut restored = checkpoint;
        let mut resumed = FeedReader::open_profile(&path, Some(cursor), profile).unwrap();
        let Some(FeedRecord::Causal(ingress)) = resumed.read_next().unwrap() else {
            panic!()
        };
        restored.apply(&ingress).unwrap();
        assert_eq!(restored, history);
        let bytes = std::fs::read(path.join("events.mfsf")).unwrap();
        let mut sealed = SealedFeedReader::new_profile(&bytes, receipt.published, profile).unwrap();
        let mut count = 0;
        while sealed.read_next().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 4);
    }
}
