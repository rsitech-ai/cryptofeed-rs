//! R6: InstrumentUpdate / VenueStatus / Statistics24h emission paths.

use marketfeed_adapter_api::{
    ConcreteSubscriptionSet, SessionMachine, SessionSpec, SubscriptionPatch,
};
use marketfeed_adapter_synthetic::{SYNTHETIC_VENUE_ID, SyntheticFactory, SyntheticSession, proto};
use marketfeed_engine::{EngineControl, EngineSupervisor, SessionRunnerConfig};
use marketfeed_model::{
    AssetCode, CatalogVersion, CatalogView, EventFlags, Fixed, FrameStamp, Instrument,
    InstrumentId, InstrumentKey, InstrumentKind, InstrumentStatus, MarketEvent, SessionId,
    SystemEvent, TimestampNs, VenueCode, VenueId,
};

fn synth_session() -> SyntheticSession {
    let _ = SyntheticFactory;
    SyntheticSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(SYNTHETIC_VENUE_ID, CatalogVersion(1)),
    )
}

#[test]
fn connect_and_mark_live_emit_venue_status() {
    let mut engine = EngineSupervisor::new();
    engine.mark_running();
    let session = SessionId(1);
    engine
        .insert_session(
            Box::new(synth_session()),
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session,
                record: false,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    let runner = engine.session_mut(session).unwrap();
    runner.on_connected(TimestampNs(10)).unwrap();
    let _ = runner.drain_dispatch();

    let statuses: Vec<_> = runner
        .market_batches
        .iter()
        .flat_map(|b| b.events.iter())
        .filter_map(|e| match &e.payload {
            MarketEvent::VenueStatus(v) => Some(v.message.as_str()),
            _ => None,
        })
        .collect();
    assert!(
        statuses.contains(&"connected"),
        "expected connected VenueStatus, got {statuses:?}"
    );
}

#[test]
fn catalog_refresh_emits_instrument_updates() {
    let mut engine = EngineSupervisor::new();
    engine.mark_running();
    let session = SessionId(2);
    engine
        .insert_session(
            Box::new(synth_session()),
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session,
                record: false,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    let catalog = CatalogView::with_instruments(
        SYNTHETIC_VENUE_ID,
        CatalogVersion(9),
        vec![Instrument {
            id: InstrumentId(42),
            key: InstrumentKey {
                venue: VenueCode("SYN".into()),
                native_symbol: "BTC-USD".into(),
                kind: InstrumentKind::Spot,
                settlement: None,
                expiry_ns: None,
            },
            base: AssetCode("BTC".into()),
            quote: AssetCode("USD".into()),
            settlement: None,
            price_scale: 2,
            quantity_scale: 3,
            price_increment: Fixed::parse_str("0.01").unwrap(),
            quantity_increment: Fixed::parse_str("0.001").unwrap(),
            min_quantity: None,
            max_quantity: None,
            min_notional: None,
            contract_size: None,
            expiry_ns: None,
            status: InstrumentStatus::Suspended,
            inverse: false,
            catalog_version: CatalogVersion(9),
        }],
    );

    engine
        .publish_catalog_refresh(session, catalog, TimestampNs(20))
        .unwrap();

    let runner = engine.session_mut(session).unwrap();
    // publish_catalog_refresh already mirrors into market_batches / system_events.

    let updates: Vec<_> = runner
        .market_batches
        .iter()
        .flat_map(|b| b.events.iter())
        .filter_map(|e| match &e.payload {
            MarketEvent::InstrumentUpdate(u) => Some((e.instrument, u.status)),
            _ => None,
        })
        .collect();
    assert_eq!(
        updates,
        vec![(Some(InstrumentId(42)), InstrumentStatus::Suspended)]
    );
    assert!(
        runner
            .system_events
            .iter()
            .any(|e| matches!(e, SystemEvent::InstrumentCatalogUpdated { version: 9 })),
        "expected InstrumentCatalogUpdated"
    );
}

#[test]
fn synthetic_stats24h_fixture() {
    let mut session = synth_session();
    let mut buf = marketfeed_adapter_api::ActionBuffer::new();
    session
        .on_input(
            marketfeed_adapter_api::SessionInput::Connected {
                now: TimestampNs(1),
            },
            &mut buf,
        )
        .unwrap();
    let _ = buf.drain().count();

    // Confirm subscribe so market frames are accepted.
    let mut sub = b"SUB BTC-USD".to_vec();
    session
        .on_input(
            marketfeed_adapter_api::SessionInput::TextFrame {
                bytes: &mut sub,
                received: FrameStamp {
                    receive_ts: TimestampNs(2),
                    mono_ns: 2,
                },
            },
            &mut buf,
        )
        .unwrap();
    let _ = buf.drain().count();

    let line = format!(
        "{}100.00 110.00 90.00 105.00 12.5 1300.0",
        proto::STATS24H_PREFIX
    );
    let mut bytes = line.into_bytes();
    session
        .on_input(
            marketfeed_adapter_api::SessionInput::TextFrame {
                bytes: &mut bytes,
                received: FrameStamp {
                    receive_ts: TimestampNs(3),
                    mono_ns: 3,
                },
            },
            &mut buf,
        )
        .unwrap();

    let mut found = false;
    for action in buf.drain() {
        if let marketfeed_adapter_api::SessionAction::EmitBatch(batch) = action {
            for env in batch.events {
                if let MarketEvent::Statistics24h(s) = env.payload {
                    assert!(s.open.is_some() && s.high.is_some() && s.close.is_some());
                    assert!(env.flags.contains(EventFlags::SYNTHETIC));
                    found = true;
                }
            }
        }
    }
    assert!(found, "expected Statistics24h from STATS24H fixture");
}

#[test]
fn pause_emits_degraded_venue_status() {
    let mut engine = EngineSupervisor::new();
    engine.mark_running();
    let session = SessionId(4);
    engine
        .insert_session(
            Box::new(synth_session()),
            SessionRunnerConfig {
                venue: SYNTHETIC_VENUE_ID,
                session,
                record: false,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();
    engine
        .apply_subscriptions(
            SubscriptionPatch::Add {
                session,
                symbols: vec!["BTC-USD".into()],
            },
            TimestampNs(1),
        )
        .unwrap();
    // Clear mirrors from subscribe noise.
    {
        let runner = engine.session_mut(session).unwrap();
        runner.market_batches.clear();
        let _ = runner.drain_dispatch();
        runner.market_batches.clear();
    }

    engine
        .apply_subscriptions(
            SubscriptionPatch::PauseVenue {
                venue: SYNTHETIC_VENUE_ID,
            },
            TimestampNs(2),
        )
        .unwrap();

    let runner = engine.session_mut(session).unwrap();
    let _ = runner.drain_dispatch();
    let msgs: Vec<_> = runner
        .market_batches
        .iter()
        .flat_map(|b| b.events.iter())
        .filter_map(|e| match &e.payload {
            MarketEvent::VenueStatus(v) => Some(v.message.as_str()),
            _ => None,
        })
        .collect();
    assert!(
        msgs.iter()
            .any(|m| m.contains("paused") || *m == "venue paused"),
        "expected pause VenueStatus, got {msgs:?}"
    );
}

/// Wave-2/3/6 venues (13–19 + 20): R6 `VenueStatus` / `InstrumentUpdate` carry the session venue id.
#[test]
fn wave2_venues_status_and_catalog_paths() {
    // 13=kraken-futures … 19=coinbase-intl … 20=bitfinex-deriv
    for (offset, venue) in [
        VenueId(13),
        VenueId(14),
        VenueId(15),
        VenueId(16),
        VenueId(17),
        VenueId(18),
        VenueId(19),
        VenueId(20),
    ]
    .into_iter()
    .enumerate()
    {
        let session = SessionId(100 + offset as u64);
        let mut engine = EngineSupervisor::new();
        engine.mark_running();
        engine
            .insert_session(
                Box::new(synth_session()),
                SessionRunnerConfig {
                    venue,
                    session,
                    record: false,
                    ..SessionRunnerConfig::default()
                },
            )
            .unwrap();

        let runner = engine.session_mut(session).unwrap();
        runner.on_connected(TimestampNs(10)).unwrap();
        let _ = runner.drain_dispatch();

        let connected: Vec<_> = runner
            .market_batches
            .iter()
            .flat_map(|b| b.events.iter())
            .filter_map(|e| match &e.payload {
                MarketEvent::VenueStatus(v) if v.message == "connected" => Some(e.venue),
                _ => None,
            })
            .collect();
        assert!(
            connected.contains(&venue),
            "venue {venue:?}: expected connected VenueStatus, got {connected:?}"
        );

        // Live + degrade share emit_venue_status (R6).
        runner.mark_live_with_status(TimestampNs(11)).unwrap();
        let live: Vec<_> = runner
            .market_batches
            .iter()
            .flat_map(|b| b.events.iter())
            .filter_map(|e| match &e.payload {
                MarketEvent::VenueStatus(v) if v.message == "live" => Some(e.venue),
                _ => None,
            })
            .collect();
        assert!(
            live.contains(&venue),
            "venue {venue:?}: expected live VenueStatus, got {live:?}"
        );

        runner
            .mark_degraded_with_status("degraded", TimestampNs(12))
            .unwrap();
        let degraded: Vec<_> = runner
            .market_batches
            .iter()
            .flat_map(|b| b.events.iter())
            .filter_map(|e| match &e.payload {
                MarketEvent::VenueStatus(v) if v.message == "degraded" => Some(e.venue),
                _ => None,
            })
            .collect();
        assert!(
            degraded.contains(&venue),
            "venue {venue:?}: expected degraded VenueStatus, got {degraded:?}"
        );

        let catalog = CatalogView::with_instruments(
            venue,
            CatalogVersion(1),
            vec![Instrument {
                id: InstrumentId(venue.0 as u32),
                key: InstrumentKey {
                    venue: VenueCode(format!("v{}", venue.0)),
                    native_symbol: "BTC-USD".into(),
                    kind: InstrumentKind::Spot,
                    settlement: None,
                    expiry_ns: None,
                },
                base: AssetCode("BTC".into()),
                quote: AssetCode("USD".into()),
                settlement: None,
                price_scale: 2,
                quantity_scale: 8,
                price_increment: Fixed::parse_str("0.01").unwrap(),
                quantity_increment: Fixed::parse_str("0.00000001").unwrap(),
                min_quantity: None,
                max_quantity: None,
                min_notional: None,
                contract_size: None,
                expiry_ns: None,
                status: InstrumentStatus::Active,
                inverse: false,
                catalog_version: CatalogVersion(1),
            }],
        );

        // Clear prior mirrors so InstrumentUpdate assertions stay unique.
        {
            let runner = engine.session_mut(session).unwrap();
            runner.market_batches.clear();
            runner.system_events.clear();
        }

        engine
            .publish_catalog_refresh(session, catalog, TimestampNs(20))
            .unwrap();

        let runner = engine.session_mut(session).unwrap();
        let updates: Vec<_> = runner
            .market_batches
            .iter()
            .flat_map(|b| b.events.iter())
            .filter_map(|e| match &e.payload {
                MarketEvent::InstrumentUpdate(u) => Some((e.venue, e.instrument, u.status)),
                _ => None,
            })
            .collect();
        assert!(
            updates.contains(&(
                venue,
                Some(InstrumentId(venue.0 as u32)),
                InstrumentStatus::Active
            )),
            "venue {venue:?}: expected InstrumentUpdate from catalog refresh, got {updates:?}"
        );
        assert!(
            runner
                .system_events
                .iter()
                .any(|e| matches!(e, SystemEvent::InstrumentCatalogUpdated { version: 1 })),
            "venue {venue:?}: expected InstrumentCatalogUpdated"
        );
    }
}
