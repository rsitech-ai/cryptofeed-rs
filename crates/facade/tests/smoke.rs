//! Smoke: facade re-exports compile and basic control + Fixed work.

use marketfeed::{
    AggressorSide, EngineControl, EngineLifecycle, EngineSupervisor, Fixed, MarketEvent,
    OverflowPolicy, Price, Quantity, SystemEvent, Trade,
    sinks::{EventSink, MemorySink},
};

#[test]
fn facade_smoke_fixed_control_sink() {
    let px = Fixed::new(42_00, 2);
    assert_eq!(px.coefficient, 42_00);
    assert_eq!(px.scale, 2);

    let event = MarketEvent::Trade(Trade {
        price: Price(px),
        quantity: Quantity(Fixed::new(1, 0)),
        aggressor: AggressorSide::Buy,
        trade_id: None,
    });
    assert!(matches!(event, MarketEvent::Trade(_)));

    let mut eng = EngineSupervisor::new();
    eng.mark_running();
    let health = eng.health().expect("running engine reports health");
    assert_eq!(health.lifecycle, EngineLifecycle::Running);

    let mut sink = MemorySink::new(8, 8, OverflowPolicy::DropNewest);
    sink.push_system(SystemEvent::HeartbeatMissed)
        .expect("bounded memory sink accepts system event");
}

#[test]
fn facade_exposes_exact_analytics_grid() {
    let grid = marketfeed::analytics::GridSpec::new(2, 3, Fixed::new(25, 2), 4)
        .expect("valid exact analytics grid");
    let bucket = grid
        .price_bucket(Price(Fixed::new(100_25, 2)))
        .expect("aligned price maps to a grouped bucket");
    assert_eq!(bucket.0, 100);
}
