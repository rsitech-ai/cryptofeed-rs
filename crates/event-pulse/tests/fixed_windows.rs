use marketfeed_event_pulse::window::{
    CoverageInterval, FixedWindow, PER_WINDOW_CAPACITY, PROCESSOR_RECORD_CAPACITY, WindowBank,
    WindowError, WindowKey, has_exact_coverage,
};

#[test]
fn eviction_keeps_the_inclusive_left_boundary() {
    let mut window = FixedWindow::new(1_000_000_000, 0).unwrap();
    window.push(1_000_000_000, 1).unwrap();
    window.push(1_000_000_001, 2).unwrap();
    window.evict(2_000_000_000).unwrap();
    assert_eq!(
        window.records().iter().map(|x| x.value).collect::<Vec<_>>(),
        [1, 2]
    );
    window.evict(2_000_000_001).unwrap();
    assert_eq!(window.records().front().unwrap().value, 2);
}

#[test]
fn completeness_and_freshness_are_availability_time_properties() {
    let mut window = FixedWindow::new(1_000_000_000, 1).unwrap();
    window.push(1_000_000_001, ()).unwrap();
    assert!(window.is_complete(1_000_000_001).unwrap());
    assert!(window.is_fresh(1_250_000_001, 250_000_000).unwrap());
    assert!(!window.is_fresh(1_250_000_002, 250_000_000).unwrap());
    window.clear_for_new_epoch(500_000_000);
    assert!(!window.is_complete(1_000_000_000).unwrap());
}

#[test]
fn explicit_coverage_must_span_the_entire_window_without_a_gap() {
    let complete = [
        CoverageInterval {
            covered_from_ns: 0,
            covered_through_ns: 500_000_000,
            available_at_ns: 500_000_000,
        },
        CoverageInterval {
            covered_from_ns: 500_000_000,
            covered_through_ns: 1_000_000_000,
            available_at_ns: 1_000_000_000,
        },
    ];
    assert!(has_exact_coverage(&complete, 1_000_000_000, 1_000_000_000).unwrap());
    let gap = [
        complete[0],
        CoverageInterval {
            covered_from_ns: 500_000_001,
            ..complete[1]
        },
    ];
    assert!(!has_exact_coverage(&gap, 1_000_000_000, 1_000_000_000).unwrap());
    assert!(!has_exact_coverage(&[], 1_000_000_000, 1_000_000_000).unwrap());
}

#[test]
fn per_window_breach_clears_and_invalidates_instead_of_evicting() {
    let mut window = FixedWindow::new(60_000_000_000, 0).unwrap();
    for i in 0..PER_WINDOW_CAPACITY {
        window.push(i as i64, i).unwrap();
    }
    assert_eq!(
        window.push(PER_WINDOW_CAPACITY as i64, 0),
        Err(WindowError::QueueDrop)
    );
    assert!(window.is_invalid());
    assert!(window.is_empty());
}

#[test]
fn processor_cap_invalidates_only_the_affected_window() {
    let mut bank = WindowBank::default();
    let mut keys = Vec::new();
    for i in 0..17 {
        let key = WindowKey {
            source_id: format!("source-{i}"),
            horizon_ns: 60_000_000_000,
            kind: "trade".into(),
        };
        bank.insert_window(key.clone(), 0).unwrap();
        keys.push(key);
    }
    for key in &keys[..16] {
        for i in 0..PER_WINDOW_CAPACITY {
            bank.push(key, i as i64, ()).unwrap();
        }
    }
    assert_eq!(bank.total_records(), PROCESSOR_RECORD_CAPACITY);
    assert_eq!(bank.push(&keys[16], 0, ()), Err(WindowError::QueueDrop));
    assert!(bank.get(&keys[16]).unwrap().is_invalid());
    assert!(!bank.get(&keys[0]).unwrap().is_invalid());
}
