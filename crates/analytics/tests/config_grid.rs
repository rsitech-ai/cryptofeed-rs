use marketfeed_analytics::{
    GridSpec, MarketSegment, PriceBucket, PriceTick, QuantityUnits, SourceSelector, TimeframeSpec,
};
use marketfeed_model::{Fixed, Price, Quantity, VenueId};

#[test]
fn grid_requires_tick_alignment_and_groups_adjacent_ticks() {
    let grid = GridSpec::new(2, 3, Fixed::new(25, 2), 4).unwrap();

    assert_eq!(
        grid.price_tick(Price(Fixed::new(100_25, 2))).unwrap(),
        PriceTick(401)
    );
    assert_eq!(
        grid.price_at_tick(PriceTick(401)).unwrap(),
        Price(Fixed::new(100_25, 2))
    );
    assert_eq!(
        grid.price_bucket(Price(Fixed::new(100_00, 2))).unwrap(),
        PriceBucket(100)
    );
    assert_eq!(
        grid.price_bucket(Price(Fixed::new(100_25, 2))).unwrap(),
        PriceBucket(100)
    );
    assert_eq!(
        grid.price_bucket(Price(Fixed::new(100_75, 2))).unwrap(),
        PriceBucket(100)
    );
    assert_eq!(
        grid.price_bucket(Price(Fixed::new(101_00, 2))).unwrap(),
        PriceBucket(101)
    );
    assert_eq!(
        grid.price_at(PriceBucket(100)).unwrap(),
        Price(Fixed::new(100_00, 2))
    );
    assert_eq!(
        grid.quantity_units(Quantity(Fixed::new(1_234, 3))).unwrap(),
        QuantityUnits(1_234)
    );
    assert_eq!(
        grid.quantity_at(QuantityUnits(1_234)).unwrap(),
        Quantity(Fixed::new(1_234, 3))
    );

    assert!(grid.price_bucket(Price(Fixed::new(100_01, 2))).is_err());
}

#[test]
fn grid_rejects_non_positive_or_invalid_configuration() {
    assert!(GridSpec::new(2, 3, Fixed::ZERO, 1).is_err());
    assert!(GridSpec::new(2, 3, Fixed::new(1, 2), 0).is_err());

    let grid = GridSpec::new(2, 3, Fixed::new(1, 2), 1).unwrap();
    assert!(grid.price_bucket(Price(Fixed::ZERO)).is_err());
    assert!(grid.quantity_units(Quantity(Fixed::ZERO)).is_err());
    assert!(grid.quantity_units(Quantity(Fixed::new(-1, 3))).is_err());
}

#[test]
fn source_selector_matches_venues_and_segments_without_conflation() {
    let selector = SourceSelector::new(
        vec![VenueId(1), VenueId(2)],
        vec![MarketSegment::Spot, MarketSegment::LinearPerpetual],
    )
    .unwrap();

    assert!(selector.matches(VenueId(1), MarketSegment::Spot));
    assert!(selector.matches(VenueId(2), MarketSegment::LinearPerpetual));
    assert!(!selector.matches(VenueId(3), MarketSegment::Spot));
    assert!(!selector.matches(VenueId(1), MarketSegment::InversePerpetual));
    assert_ne!(MarketSegment::Spot, MarketSegment::LinearPerpetual);

    let decoded: SourceSelector = serde_json::from_str(
        r#"{"venues":[1,3,2],"segments":["Spot","Option","LinearPerpetual"]}"#,
    )
    .unwrap();
    assert!(decoded.matches(VenueId(2), MarketSegment::LinearPerpetual));
}

#[test]
fn timeframe_boundaries_are_validated_and_euclidean() {
    let time = TimeframeSpec::new(60, 30, 300, -10, 900).unwrap();

    assert_eq!(time.candle_start(-11), -70);
    assert_eq!(time.candle_start(-10), -10);
    assert_eq!(time.candle_start(49), -10);
    assert_eq!(time.candle_start(50), 50);
    assert_eq!(time.tpo_start(-11), -40);
    assert_eq!(time.session_start(289), -10);
    assert_eq!(time.session_start(290), 290);

    assert!(TimeframeSpec::new(70, 30, 300, 0, 900).is_err());
    assert!(TimeframeSpec::new(60, 40, 300, 0, 900).is_err());
    assert!(TimeframeSpec::new(0, 30, 300, 0, 900).is_err());
    assert!(TimeframeSpec::new(60, 30, 300, 0, 0).is_err());
}
