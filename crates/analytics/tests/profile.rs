use marketfeed_analytics::{
    AnalyticsError, GridSpec, ProfileConfig, ProfileState, SessionProfileBuilder, TimeframeSpec,
    ValueAreaBasis,
};
use marketfeed_model::{Fixed, Price, Quantity};

fn grid() -> GridSpec {
    GridSpec::new(0, 0, Fixed::new(1, 0), 1).unwrap()
}

fn time() -> TimeframeSpec {
    TimeframeSpec::new(60, 60, 300, 0, 900).unwrap()
}

fn config(basis: ValueAreaBasis) -> ProfileConfig {
    ProfileConfig::new(basis, 7_000, 32, 32).unwrap()
}

fn price(value: i128) -> Price {
    Price(Fixed::new(value, 0))
}

fn quantity(value: i128) -> Quantity {
    Quantity(Fixed::new(value, 0))
}

#[test]
fn volume_profile_calculates_all_session_statistics_exactly() {
    let mut profile =
        SessionProfileBuilder::new(grid(), time(), config(ValueAreaBasis::Volume)).unwrap();

    profile.ingest(1, price(100), quantity(4)).unwrap();
    profile.ingest(2, price(101), quantity(2)).unwrap();
    profile.ingest(61, price(101), quantity(3)).unwrap();
    profile.ingest(62, price(102), quantity(1)).unwrap();
    profile.ingest(121, price(100), quantity(1)).unwrap();

    let snapshot = profile.finish().unwrap().unwrap();

    assert_eq!(snapshot.state, ProfileState::Final);
    assert_eq!(snapshot.start_ts, 0);
    assert_eq!(snapshot.end_ts, 300);
    assert_eq!(snapshot.high, Some(price(102)));
    assert_eq!(snapshot.low, Some(price(100)));
    assert_eq!(snapshot.range, Some(Fixed::new(2, 0)));
    assert_eq!(snapshot.total_volume, quantity(11));
    assert_eq!(snapshot.poc, Some(price(101)));
    assert_eq!(snapshot.val, Some(price(100)));
    assert_eq!(snapshot.vah, Some(price(101)));
    assert_eq!(snapshot.tpo_count, 5);
    assert_eq!(snapshot.rotation_factor, 0);
    assert_eq!(snapshot.levels.len(), 3);
    assert_eq!(snapshot.levels[0].volume, quantity(5));
    assert_eq!(snapshot.levels[0].tpo_count, 2);
    assert_eq!(snapshot.levels[1].volume, quantity(5));
    assert_eq!(snapshot.levels[1].tpo_count, 2);
    assert_eq!(snapshot.levels[2].volume, quantity(1));
    assert_eq!(snapshot.levels[2].tpo_count, 1);
}

#[test]
fn tpo_profile_counts_each_price_once_per_period_and_scores_rotation() {
    let mut profile =
        SessionProfileBuilder::new(grid(), time(), config(ValueAreaBasis::Tpo)).unwrap();

    profile.ingest(1, price(100), quantity(1)).unwrap();
    profile.ingest(2, price(100), quantity(9)).unwrap();
    profile.ingest(3, price(101), quantity(1)).unwrap();
    profile.ingest(61, price(101), quantity(1)).unwrap();
    profile.ingest(62, price(102), quantity(1)).unwrap();

    let live = profile.live_snapshot().unwrap().unwrap();
    assert_eq!(live.state, ProfileState::Live);
    assert_eq!(live.tpo_count, 4);
    assert_eq!(live.rotation_factor, 2);
    assert_eq!(live.poc, Some(price(101)));
    assert_eq!(live.val, Some(price(100)));
    assert_eq!(live.vah, Some(price(101)));
}

#[test]
fn rollover_returns_final_session_and_starts_the_next_one() {
    let mut profile =
        SessionProfileBuilder::new(grid(), time(), config(ValueAreaBasis::Volume)).unwrap();

    profile.ingest(1, price(100), quantity(1)).unwrap();
    let finalized = profile
        .ingest(301, price(200), quantity(2))
        .unwrap()
        .unwrap();

    assert_eq!(finalized.start_ts, 0);
    assert_eq!(finalized.end_ts, 300);
    assert_eq!(finalized.total_volume, quantity(1));

    let next = profile.live_snapshot().unwrap().unwrap();
    assert_eq!(next.start_ts, 300);
    assert_eq!(next.total_volume, quantity(2));
}

#[test]
fn empty_time_periods_do_not_change_rotation_or_fabricate_tpos() {
    let mut profile =
        SessionProfileBuilder::new(grid(), time(), config(ValueAreaBasis::Volume)).unwrap();

    profile.ingest(1, price(100), quantity(1)).unwrap();
    assert!(profile.advance_to(181).unwrap().is_none());
    profile.ingest(182, price(99), quantity(1)).unwrap();

    let snapshot = profile.finish().unwrap().unwrap();
    assert_eq!(snapshot.tpo_count, 2);
    assert_eq!(snapshot.rotation_factor, -2);
}

#[test]
fn grouped_profile_preserves_exact_high_low_and_range() {
    let grouped_grid = GridSpec::new(2, 0, Fixed::new(25, 2), 4).unwrap();
    let mut profile =
        SessionProfileBuilder::new(grouped_grid, time(), config(ValueAreaBasis::Volume)).unwrap();

    profile
        .ingest(1, Price(Fixed::new(100_25, 2)), Quantity(Fixed::new(1, 0)))
        .unwrap();
    profile
        .ingest(2, Price(Fixed::new(100_75, 2)), Quantity(Fixed::new(2, 0)))
        .unwrap();

    let snapshot = profile.finish().unwrap().unwrap();
    assert_eq!(snapshot.levels.len(), 1);
    assert_eq!(snapshot.levels[0].price, Price(Fixed::new(100_00, 2)));
    assert_eq!(snapshot.low, Some(Price(Fixed::new(100_25, 2))));
    assert_eq!(snapshot.high, Some(Price(Fixed::new(100_75, 2))));
    assert_eq!(snapshot.range, Some(Fixed::new(50, 2)));
}

#[test]
fn capacity_and_lateness_fail_atomically() {
    let limited = ProfileConfig::new(ValueAreaBasis::Volume, 7_000, 1, 1).unwrap();
    let mut profile = SessionProfileBuilder::new(grid(), time(), limited).unwrap();
    profile.ingest(1, price(100), quantity(1)).unwrap();
    let before = serde_json::to_vec(&profile).unwrap();

    assert!(matches!(
        profile.ingest(2, price(101), quantity(1)),
        Err(AnalyticsError::CapacityExceeded {
            resource: "session profile levels",
            limit: 1
        })
    ));
    assert_eq!(serde_json::to_vec(&profile).unwrap(), before);

    assert!(profile.advance_to(301).unwrap().is_some());
    let after_advance = serde_json::to_vec(&profile).unwrap();
    assert!(matches!(
        profile.ingest(299, price(100), quantity(1)),
        Err(AnalyticsError::LateTrade { .. })
    ));
    assert_eq!(serde_json::to_vec(&profile).unwrap(), after_advance);
}
