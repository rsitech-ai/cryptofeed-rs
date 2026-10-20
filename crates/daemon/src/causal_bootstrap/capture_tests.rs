use super::*;
use crate::causal_bootstrap::PreparedBootstrap;

const SPOT: &[u8] = br#"{"tokens":[{"name":"USDC","szDecimals":8,"weiDecimals":8,"index":0,"tokenId":"0x0"},{"name":"PURR","szDecimals":0,"weiDecimals":5,"index":7,"tokenId":"0x7"}],"universe":[{"name":"@12","tokens":[7,0],"index":12}]}"#;
const PERPS: &[u8] = br#"[{"collateralToken":0,"universe":[{"name":"BTC","szDecimals":5}]}]"#;
fn channels() -> Vec<ChannelDto> {
    vec![
        ChannelDto::Trades,
        ChannelDto::Quote,
        ChannelDto::L2Book {
            depth: Some(20),
            cadence: None,
        },
        ChannelDto::Candles {
            interval: CandleIntervalDto::M1,
        },
        ChannelDto::MarkPrice,
    ]
}
pub(super) fn request() -> CaptureRequest {
    CaptureRequest {
        schema_version: 1,
        markets: vec![
            MarketSelection {
                market: HyperliquidMarketDto::Spot,
                coins: vec!["@12".into()],
                channels: channels(),
            },
            MarketSelection {
                market: HyperliquidMarketDto::Perps,
                coins: vec!["BTC".into()],
                channels: channels(),
            },
        ],
        duration_ms: 60_000,
        source_max_bytes: 64 << 20,
        target_max_bytes: 64 << 20,
    }
}
pub(super) fn build(request: &CaptureRequest) -> Result<Vec<BootstrapDto>, BootstrapError> {
    prepare(
        request,
        DiscoveryBodies {
            spot: SPOT,
            perps: Some(PERPS),
        },
        PublicReproductionInputDto::new(
            "hyperliquid-bootstrap-http-acquisition-v1",
            br#"{"synthetic_test_only":true}"#,
        ),
    )
}
#[test]
fn selected_spot_perps_prepare_actual_factories_and_validate_before_admission() {
    let recipe = request();
    let bootstraps = build(&recipe).unwrap();
    assert_eq!(bootstraps.len(), 2);
    let mut sessions = BTreeSet::new();
    for (dto, selected) in bootstraps.into_iter().zip(recipe.markets) {
        assert!(sessions.insert(dto.capture.session));
        assert!(dto.session.subscriptions.iter().all(|s| {
            dto.catalog
                .instruments
                .iter()
                .any(|i| i.id == s.instrument && selected.coins.contains(&i.key.native_symbol))
        }));
        assert!(
            dto.public_inputs
                .iter()
                .any(|p| p.role == CAPTURE_REQUEST_ROLE)
        );
        assert_eq!(
            dto.public_inputs
                .iter()
                .find(|p| p.role == HYPERLIQUID_SPOT_META_ROLE)
                .unwrap()
                .payload
                .bytes()
                .unwrap(),
            SPOT
        );
        assert_eq!(dto.wire_policy().unwrap().max_pending_writes, 64);
        PreparedBootstrap::new_wire_history(dto, MAX_OPERATION_BYTES).unwrap();
    }
}
#[test]
fn duplicate_alias_wrong_market_and_unsupported_channel_are_not_silently_mapped() {
    for case in 0..5 {
        let mut recipe = request();
        match case {
            0 => recipe.markets.push(recipe.markets[0].clone()),
            1 => recipe.markets[0].coins.push("@12".into()),
            2 => recipe.markets[0].channels.push(ChannelDto::Trades),
            3 => recipe.markets[0].coins[0] = "PURR/USDC".into(),
            _ => recipe.markets[0].channels = vec![ChannelDto::Funding],
        }
        assert!(build(&recipe).is_err(), "case {case}");
    }
    let mut recipe = request();
    recipe.markets[0].coins[0] = "BTC".into();
    assert!(build(&recipe).is_err());
}
#[test]
fn malformed_discovery_and_missing_collateral_do_not_create_bootstraps() {
    for metadata in [
        b"{}".as_slice(),
        b"[{}]".as_slice(),
        br#"[{"collateralToken":99,"universe":[{"name":"BTC","szDecimals":5}]}]"#.as_slice(),
    ] {
        assert!(
            prepare(
                &request(),
                DiscoveryBodies {
                    spot: SPOT,
                    perps: Some(metadata)
                },
                PublicReproductionInputDto::new("hyperliquid-bootstrap-http-acquisition-v1", b"{}")
            )
            .is_err()
        );
    }
    assert!(
        prepare(
            &request(),
            DiscoveryBodies {
                spot: SPOT,
                perps: None
            },
            PublicReproductionInputDto::new("hyperliquid-bootstrap-http-acquisition-v1", b"{}")
        )
        .is_err()
    );
}
#[test]
fn initial_wire_bundle_bound_rejects_many_explicit_native_topics() {
    let universe = (1..=13)
        .map(|index| serde_json::json!({"name":format!("@{index}"),"tokens":[7,0],"index":index}))
        .collect::<Vec<_>>();
    let metadata = serde_json::to_vec(&serde_json::json!({"tokens":[{"name":"USDC","szDecimals":8,"weiDecimals":8,"index":0,"tokenId":"0x0"},{"name":"PURR","szDecimals":0,"weiDecimals":5,"index":7,"tokenId":"0x7"}],"universe":universe})).unwrap();
    let mut recipe = request();
    recipe.markets.truncate(1);
    recipe.markets[0].coins = (1..=13).map(|index| format!("@{index}")).collect();
    let error = prepare(
        &recipe,
        DiscoveryBodies {
            spot: &metadata,
            perps: None,
        },
        PublicReproductionInputDto::new("hyperliquid-bootstrap-http-acquisition-v1", b"{}"),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        BootstrapError::Unsupported("initial application wire bundle exceeds 64 writes")
    ));
}
#[test]
fn request_type_and_duration_storage_bounds_fail_before_acquisition() {
    for value in [
        serde_json::json!({"schema_version":1,"markets":[],"duration_ms":1,"source_max_bytes":67108864,"target_max_bytes":67108864,"unknown":true}),
        serde_json::json!({"schema_version":true,"markets":[],"duration_ms":1,"source_max_bytes":67108864,"target_max_bytes":67108864}),
    ] {
        assert!(serde_json::from_value::<CaptureRequest>(value).is_err());
    }
    for case in 0..4 {
        let mut recipe = request();
        match case {
            0 => recipe.duration_ms = 0,
            1 => recipe.duration_ms = 86_400_001,
            2 => recipe.source_max_bytes = 1,
            _ => recipe.target_max_bytes = (1u64 << 40) + 1,
        }
        assert!(recipe.validate().is_err());
    }
}

#[test]
fn source_admissible_large_metadata_is_refused_before_projection_start() {
    let mut spot: serde_json::Value = serde_json::from_slice(SPOT).unwrap();
    let mut perps: serde_json::Value = serde_json::from_slice(PERPS).unwrap();
    spot["unused_padding"] = serde_json::json!("x".repeat(3_300_000));
    perps[0]["unused_padding"] = serde_json::json!("x".repeat(3_300_000));
    let spot = serde_json::to_vec(&spot).unwrap();
    let perps = serde_json::to_vec(&perps).unwrap();
    assert!(spot.len() < MAX_METADATA_BYTES && perps.len() < MAX_METADATA_BYTES);
    let dtos = prepare(
        &request(),
        DiscoveryBodies {
            spot: &spot,
            perps: Some(&perps),
        },
        PublicReproductionInputDto::new("hyperliquid-bootstrap-http-acquisition-v1", b"{}"),
    )
    .unwrap();
    for dto in &dtos {
        PreparedBootstrap::new_wire_history(dto.clone(), MAX_OPERATION_BYTES).unwrap();
    }
    assert!(
        runtime::preflight(&dtos)
            .unwrap_err()
            .to_string()
            .contains("projection group")
    );
    runtime::preflight(&build(&request()).unwrap()).unwrap();
}

#[test]
fn recent_trade_snapshots_fit_frame_budget_and_excess_rows_still_fail_closed() {
    use marketfeed_engine::{
        CaptureDrainPolicy, CaptureEntry, CaptureInput, CaptureOutcome, CaptureQueueItem,
    };
    use marketfeed_model::{ConnectionId, FrameStamp, TimestampNs};
    let text = |payload: Vec<u8>| CaptureInput::Text {
        payload,
        received: FrameStamp {
            receive_ts: TimestampNs(3),
            mono_ns: 3,
        },
    };
    for count in [30usize, 128, 129] {
        for dto in build(&request()).unwrap() {
            let mut session = super::super::capture_from_supported(&dto).unwrap();
            assert!(matches!(
                session
                    .execute_turn(
                        CaptureInput::Connected {
                            connection: ConnectionId(1),
                            now: TimestampNs(1)
                        },
                        CaptureDrainPolicy::NullDispatchTakeOutbound
                    )
                    .unwrap()
                    .outcome,
                CaptureOutcome::Returned(Ok(()))
            ));
            for topic in &dto.protocol_topics {
                let payload=format!(r#"{{"channel":"subscriptionResponse","data":{{"method":"subscribe","subscription":{topic}}}}}"#).into_bytes();
                assert!(matches!(
                    session
                        .execute_turn(text(payload), CaptureDrainPolicy::NullDispatchTakeOutbound)
                        .unwrap()
                        .outcome,
                    CaptureOutcome::Returned(Ok(()))
                ));
            }
            let coin = &dto.catalog.instruments[0].key.native_symbol;
            let rows=(0..count).map(|index|serde_json::json!({"coin":coin,"side":"B","px":"10.125","sz":"0.2","time":1,"tid":index as u64,"hash":format!("0x{}","a".repeat(64)),"users":[format!("0x{}","b".repeat(40)),format!("0x{}","c".repeat(40))]})).collect::<Vec<_>>();
            let payload =
                serde_json::to_vec(&serde_json::json!({"channel":"trades","data":rows})).unwrap();
            assert!(payload.len() <= 64 << 10);
            let result = session
                .execute_turn(text(payload), CaptureDrainPolicy::NullDispatchTakeOutbound)
                .unwrap();
            let observed = result
                .entries
                .iter()
                .filter_map(|entry| {
                    if let CaptureEntry::QueueDrain { items, .. } = &entry.effect {
                        Some(items)
                    } else {
                        None
                    }
                })
                .flatten()
                .filter_map(|item| {
                    if let CaptureQueueItem::ObservedMarket(batch) = item {
                        Some(batch)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            if count <= 128 {
                assert!(
                    matches!(result.outcome, CaptureOutcome::Returned(Ok(()))),
                    "snapshot{count} rejected: {:?}",
                    result.outcome
                );
                assert_eq!(
                    observed
                        .iter()
                        .map(|batch| batch.native().rows.len())
                        .sum::<usize>(),
                    count
                );
                assert_eq!(
                    observed
                        .iter()
                        .map(|batch| batch.canonical().events.len())
                        .sum::<usize>(),
                    count
                );
            } else {
                assert!(!matches!(result.outcome, CaptureOutcome::Returned(Ok(()))));
                assert!(observed.is_empty(), "oversize rows partially released");
            }
        }
    }
}
