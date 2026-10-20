use std::sync::atomic::Ordering;

use marketfeed_adapter_api::{ConcreteSubscriptionSet, SessionSpec};
use marketfeed_adapter_synthetic::{SYNTHETIC_VENUE_ID, SyntheticSession};
use marketfeed_engine::{EngineError, SessionRunner, SessionRunnerConfig};
use marketfeed_model::{
    AssetCode, CatalogVersion, CatalogView, Fixed, Instrument, InstrumentId, InstrumentKey,
    InstrumentKind, InstrumentStatus, MarketEvent, SystemEvent, TimestampNs, VenueCode,
};
use marketfeed_recording::shared_feed::{FeedHandle, FeedReader, FeedWriter};

fn catalog(count: usize) -> CatalogView {
    CatalogView::with_instruments(
        SYNTHETIC_VENUE_ID,
        CatalogVersion(9),
        (0..count)
            .map(|index| Instrument {
                id: InstrumentId(index as u32 + 1),
                key: InstrumentKey {
                    venue: VenueCode("SYN".into()),
                    native_symbol: format!("asset{index}"),
                    kind: InstrumentKind::Spot,
                    settlement: None,
                    expiry_ns: None,
                },
                base: AssetCode("BASE".into()),
                quote: AssetCode("USD".into()),
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
                status: InstrumentStatus::Suspended,
                inverse: false,
                catalog_version: CatalogVersion(9),
            })
            .collect::<Vec<_>>(),
    )
}

fn runner(shared_feed: Option<FeedHandle>) -> SessionRunner {
    SessionRunner::new(
        Box::new(SyntheticSession::new(
            SessionSpec {
                endpoint_name: "ws".into(),
                subscriptions: ConcreteSubscriptionSet::default(),
            },
            CatalogView::new(SYNTHETIC_VENUE_ID, CatalogVersion(1)),
        )),
        SessionRunnerConfig {
            venue: SYNTHETIC_VENUE_ID,
            record: false,
            mirror_capacity: 0,
            shared_feed,
            ..Default::default()
        },
    )
    .unwrap()
}

#[test]
fn catalog_refresh_accepts_the_last_representable_event_index() {
    let mut runner = runner(None);
    runner
        .publish_catalog_refresh(catalog(65_536), TimestampNs(20))
        .unwrap();
    let batches = runner.dispatch_mut().drain_batches();
    assert_eq!(batches.len(), 1);
    let events = &batches[0].events;
    assert_eq!(events.len(), 65_536);
    assert_eq!(events[0].event_index, 0);
    assert_eq!(events.last().unwrap().event_index, u16::MAX);
    assert_eq!(
        events.last().unwrap().instrument,
        Some(InstrumentId(65_536))
    );
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].event_index < pair[1].event_index)
    );
    assert!(
        events
            .iter()
            .all(|event| matches!(event.payload, MarketEvent::InstrumentUpdate(_)))
    );
    assert_eq!(
        runner.dispatch_mut().drain_systems(),
        vec![SystemEvent::InstrumentCatalogUpdated { version: 9 }]
    );
}

#[test]
fn oversized_catalog_refresh_rejects_before_any_dispatch_or_metrics() {
    let mut runner = runner(None);
    let error = runner
        .publish_catalog_refresh(catalog(65_537), TimestampNs(20))
        .unwrap_err();
    assert!(
        matches!(error, EngineError::Internal(message) if message == "too many events in catalog refresh")
    );
    assert_eq!(runner.dispatch_pending(), (0, 0));
    assert_eq!(runner.metrics.events_normalized.load(Ordering::Relaxed), 0);
    assert!(runner.market_batches.is_empty());
    assert!(runner.system_events.is_empty());
    // Preparation refusal leaves the runner usable and does not publish the
    // rejected version's confirmation before this independent valid refresh.
    runner
        .publish_catalog_refresh(catalog(1), TimestampNs(21))
        .unwrap();
    assert_eq!(runner.dispatch_pending(), (1, 1));
}

#[test]
fn oversized_catalog_refresh_does_not_advance_the_durable_feed() {
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("feed");
    let feed = FeedHandle::new(FeedWriter::create(&directory, 64 * 1024 * 1024).unwrap());
    let before = std::fs::read(directory.join("committed.json")).unwrap();
    let mut runner = runner(Some(feed));
    assert!(matches!(
        runner.publish_catalog_refresh(catalog(65_537), TimestampNs(20)),
        Err(EngineError::Internal(_))
    ));
    assert_eq!(
        std::fs::read(directory.join("committed.json")).unwrap(),
        before
    );
    assert!(
        FeedReader::open(directory, None)
            .unwrap()
            .read_next()
            .unwrap()
            .is_none()
    );
    assert_eq!(runner.dispatch_pending(), (0, 0));
}
