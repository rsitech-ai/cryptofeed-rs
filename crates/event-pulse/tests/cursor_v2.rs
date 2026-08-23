use std::collections::BTreeMap;

use marketfeed_event_pulse::{
    CursorError, IngestOutcome, Invalidity, MechanicsInputV2, SlotState, SourceStateMachineV2,
    wire::{
        ClockSourceKeyV1, ConfiguredTargetKeyV1, ConnectionKeyV1, ContributorKeyV1,
        ContributorRoleV1, ContributorSpecV1, ContributorV1, CoverageSourceKeyV1, CursorModeV1,
        CursorV1, FamilyV1, FaultScopeKindV1, FaultScopeV1, InstrumentIdentityV1,
        MechanicsConfigV1, MechanicsInputV1, Rfc3339Time, SystemFaultV1, SystemSourceKeyV1,
        SystemSourceV1,
    },
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn quote_value() -> Value {
    let contract: Value = serde_json::from_slice(include_bytes!(
        "../contracts/prospective/event-pulse-e2-wire-admission-v2-contract.json"
    ))
    .unwrap();
    contract["mechanics_input_v2"]["market_golden"].clone()
}

fn rehash(mut value: Value) -> MechanicsInputV2 {
    value.as_object_mut().unwrap().remove("payload_hash");
    value["payload_hash"] = json!(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&value).unwrap())
    ));
    MechanicsInputV2::from_json_line(&serde_json::to_vec(&value).unwrap()).unwrap()
}

fn config() -> (MechanicsConfigV1, ContributorKeyV1) {
    let instrument =
        InstrumentIdentityV1::new("BNB", "USDT", "PERPETUAL", "BINANCE", "BNBUSDT").unwrap();
    let public = ContributorKeyV1::new("binance_primary_public", instrument.clone()).unwrap();
    let market = ContributorKeyV1::new("binance_primary_market", instrument).unwrap();
    let confirmation = ContributorKeyV1::new(
        "hyperliquid_confirmation",
        InstrumentIdentityV1::new("BNB", "USDT", "PERPETUAL", "HYPERLIQUID", "BNB").unwrap(),
    )
    .unwrap();
    let public_connection = ConnectionKeyV1::new("binance_primary_public_connection").unwrap();
    let market_connection = ConnectionKeyV1::new("binance_primary_market_connection").unwrap();
    let confirmation_connection =
        ConnectionKeyV1::new("hyperliquid_confirmation_connection").unwrap();
    let contributors = vec![
        ContributorSpecV1::new(
            public.clone(),
            ContributorRoleV1::Primary,
            [FamilyV1::Quote, FamilyV1::Book],
        )
        .unwrap(),
        ContributorSpecV1::new(
            market.clone(),
            ContributorRoleV1::Primary,
            [
                FamilyV1::Trade,
                FamilyV1::OpenInterest,
                FamilyV1::Liquidation,
            ],
        )
        .unwrap(),
        ContributorSpecV1::new(
            confirmation.clone(),
            ContributorRoleV1::Confirmation,
            [FamilyV1::ConfirmationPrice],
        )
        .unwrap(),
    ];
    let clocks = [
        ("clock_binance_public", public.clone()),
        ("clock_binance_market", market.clone()),
        ("clock_hyperliquid_confirmation", confirmation.clone()),
    ]
    .into_iter()
    .map(|(id, subject)| ClockSourceKeyV1::new(id, subject).unwrap())
    .collect();
    let coverage = [
        (
            "coverage_binance_public_quote",
            public.clone(),
            FamilyV1::Quote,
        ),
        (
            "coverage_binance_public_book",
            public.clone(),
            FamilyV1::Book,
        ),
        (
            "coverage_binance_market_trade",
            market.clone(),
            FamilyV1::Trade,
        ),
        (
            "coverage_binance_market_open_interest",
            market.clone(),
            FamilyV1::OpenInterest,
        ),
        (
            "coverage_binance_market_liquidation",
            market.clone(),
            FamilyV1::Liquidation,
        ),
        (
            "coverage_hyperliquid_confirmation",
            confirmation.clone(),
            FamilyV1::ConfirmationPrice,
        ),
    ]
    .into_iter()
    .map(|(id, subject, family)| CoverageSourceKeyV1::new(id, subject, family).unwrap())
    .collect();
    let systems = vec![
        SystemSourceKeyV1::new(
            "system_contributor",
            FaultScopeKindV1::Contributor,
            ConfiguredTargetKeyV1::contributor(public.clone()),
            CursorModeV1::Derived,
        )
        .unwrap(),
        SystemSourceKeyV1::new(
            "system_connection",
            FaultScopeKindV1::ConnectionEpoch,
            ConfiguredTargetKeyV1::connection(public_connection.clone()),
            CursorModeV1::Derived,
        )
        .unwrap(),
        SystemSourceKeyV1::new(
            "system_processor",
            FaultScopeKindV1::Processor,
            ConfiguredTargetKeyV1::processor("event_pulse_e2_prospective").unwrap(),
            CursorModeV1::Derived,
        )
        .unwrap(),
    ];
    let config = MechanicsConfigV1::new(
        "event_pulse_e2_prospective",
        vec![
            public_connection.clone(),
            market_connection.clone(),
            confirmation_connection.clone(),
        ],
        contributors,
        BTreeMap::from([
            (public.clone(), public_connection),
            (market, market_connection),
            (confirmation, confirmation_connection),
        ]),
        clocks,
        coverage,
        systems,
    )
    .unwrap();
    (config, public)
}

fn retime_and_epoch(
    input: &MechanicsInputV2,
    at_ns: i64,
    frame: u64,
    epoch: &str,
    generation: u8,
) -> MechanicsInputV2 {
    let mut value = serde_json::to_value(input).unwrap();
    value.as_object_mut().unwrap().remove("payload_hash");
    value["envelope"]["receive_ts"] = json!(at_ns);
    value["envelope"]["exchange_ts"] = json!(at_ns);
    value["envelope"]["frame_seq"] = json!(frame);
    value["market_cursor"]["raw_frame_seq"] = json!(frame);
    value["catalog"]["connection_epochs"][0]["connection_epoch"] = json!(epoch);
    value["catalog"]["connection_epochs"][0]["epoch_generation"] = json!(generation);
    let at_ms = u64::try_from(at_ns.div_euclid(1_000_000)).unwrap();
    value["source_provenance"]["event_time_ms"] = json!(at_ms);
    value["source_provenance"]["transaction_time_ms"] = json!(at_ms);
    rehash(value)
}

#[test]
fn v2_cursor_ingest_uses_explicit_derived_coordinate_and_retains_v2_hash() {
    let (config, public) = config();
    let first = rehash(quote_value());
    let mut second_value = quote_value();
    second_value["envelope"]["frame_seq"] = json!(42);
    second_value["envelope"]["receive_ts"] = json!(1_000_000_200_i64);
    second_value["market_cursor"]["raw_frame_seq"] = json!(42);
    second_value["source_provenance"]["update_id"] = json!(9_999_u64);
    let second = rehash(second_value);

    let mut state = SourceStateMachineV2::new(config);
    assert_eq!(state.ingest(&first), Ok(IngestOutcome::AcceptedWarming));
    assert_eq!(state.ingest(&second), Ok(IngestOutcome::AcceptedWarming));
    let retained = state.market_cursor(&public, FamilyV1::Quote).unwrap();
    assert_eq!(retained.payload_hash, second.payload_hash());
    assert_eq!(retained.cursor.derived_coordinate(), Some((42, 2, 0)));

    let mut mutation = quote_value();
    mutation["envelope"]["frame_seq"] = json!(42);
    mutation["envelope"]["receive_ts"] = json!(1_000_000_200_i64);
    mutation["market_cursor"]["raw_frame_seq"] = json!(42);
    mutation["source_provenance"]["update_id"] = json!(10_000_u64);
    assert_eq!(
        state.ingest(&rehash(mutation)),
        Err(CursorError::MutatedDuplicate)
    );
}

#[test]
fn v2_state_retains_full_width_derived_frames_without_cursor_v1() {
    let (config, public) = config();
    let base = rehash(quote_value());
    let first = retime_and_epoch(&base, 2_000_000_000, 2_147_483_648, "epoch_public", 0);
    let last = retime_and_epoch(&base, 2_100_000_000, u64::MAX, "epoch_public", 0);
    let mut state = SourceStateMachineV2::new(config);
    state.ingest(&first).unwrap();
    state.ingest(&last).unwrap();
    assert_eq!(
        state
            .market_cursor(&public, FamilyV1::Quote)
            .unwrap()
            .cursor,
        marketfeed_event_pulse::MarketCursorV2::Derived {
            raw_frame_seq: u64::MAX,
            action_index: 2,
            item_index: 0,
        }
    );
}

#[test]
fn invalidating_system_scopes_and_terminal_epoch_reuse_hide_family_cursors() {
    let (config, public) = config();
    let first = retime_and_epoch(&rehash(quote_value()), 2_000_000_000, 41, "epoch_public", 0);
    let connection = config
        .contributor_connections()
        .get(&public)
        .unwrap()
        .clone();
    for (index, key) in config.system_sources().iter().enumerate() {
        let mut state = SourceStateMachineV2::new(config.clone());
        state.ingest(&first).unwrap();
        let scope = match key.scope_kind() {
            FaultScopeKindV1::Contributor => FaultScopeV1::contributor(
                ContributorV1::new(public.clone(), "epoch_public", 0).unwrap(),
            ),
            FaultScopeKindV1::ConnectionEpoch => {
                FaultScopeV1::connection(connection.clone(), "epoch_public", 0).unwrap()
            }
            FaultScopeKindV1::Processor => FaultScopeV1::processor(config.processor_id()).unwrap(),
        };
        let at = Rfc3339Time::from_unix_nanos(2_100_000_000 + index as i64 * 1_000).unwrap();
        let (cursor, fault) = match key.scope_kind() {
            FaultScopeKindV1::Contributor => (
                CursorV1::derived(50 + index as u64, 0, 0).unwrap(),
                SystemFaultV1::sequence_gap(1, 3),
            ),
            FaultScopeKindV1::ConnectionEpoch => (
                CursorV1::derived(50 + index as u64, 0, 0).unwrap(),
                SystemFaultV1::disconnected(),
            ),
            FaultScopeKindV1::Processor => (
                CursorV1::derived_drop(50 + index as u64, 1).unwrap(),
                SystemFaultV1::events_dropped(
                    1,
                    marketfeed_event_pulse::wire::DropCategoryV1::MarketDispatch,
                )
                .unwrap(),
            ),
        };
        let system = MechanicsInputV1::system(
            SystemSourceV1::new(key.clone(), &format!("epoch_system_{index}"), 0).unwrap(),
            scope,
            at.clone(),
            at,
            cursor,
            fault,
            None,
        )
        .unwrap();
        assert_eq!(
            state.ingest(&MechanicsInputV2::from_v1_non_market(system).unwrap()),
            Ok(IngestOutcome::Invalidated)
        );
        assert!(state.market_cursor(&public, FamilyV1::Quote).is_none());
        assert_eq!(
            state.market_state(&public, FamilyV1::Quote),
            Some(SlotState::Invalid)
        );
        assert_eq!(
            state.market_invalidity(&public, FamilyV1::Quote),
            Some(Invalidity::Recoverable)
        );
    }

    let mut state = SourceStateMachineV2::new(config);
    state.ingest(&first).unwrap();
    state
        .ingest(&retime_and_epoch(
            &first,
            3_000_000_000,
            42,
            "epoch_next",
            1,
        ))
        .unwrap();
    assert_eq!(
        state.ingest(&retime_and_epoch(
            &first,
            4_000_000_000,
            43,
            "epoch_public",
            2
        )),
        Err(CursorError::EpochReused)
    );
    assert!(state.market_cursor(&public, FamilyV1::Quote).is_none());
    assert_eq!(
        state.market_invalidity(&public, FamilyV1::Quote),
        Some(Invalidity::Terminal)
    );
}
