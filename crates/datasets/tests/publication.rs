use marketfeed_datasets::{
    export::{self, ExportRequest},
    store::Store,
};
use marketfeed_model::*;
use marketfeed_recording::{
    Direction, FrameOpcode, MetadataRecord, RawSegmentWriter,
    shared_feed::{FeedRecord, FeedWriter},
};
use serde_json::json;
use std::{fs::File, path::Path};
const NOW: i64 = 1_700_000_000_000_000_000;
fn fixture(root: &Path, fault: &str) {
    let build:MetadataRecord=serde_json::from_value(json!({"kind":"build","schema_version":1,"package_name":"marketfeed","package_version":"test-fixture","build_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","target_os":"test","target_arch":"test"})).unwrap();
    let session:MetadataRecord=serde_json::from_value(json!({"kind":"session","schema_version":1,"session_id":7,"venue_id":22,"adapter":"hyperliquid-perps","environment":"production","endpoint":"wss://api.hyperliquid.xyz/ws","catalog_version":1,"catalog":[{"instrument_id":1,"native_symbol":"BTC","kind":"PerpetualLinear","base":"BTC","quote":"USDC","settlement":"USDC","price_scale":1,"quantity_scale":5,"price_increment":{"coefficient":"0","scale":1},"quantity_increment":{"coefficient":"1","scale":5},"min_quantity":null,"max_quantity":null,"min_notional":null,"contract_size":null,"expiry_ns":null,"status":"Active","inverse":false}],"initial_subscriptions":[{"instrument_id":1,"channel":"Trades","emit_book_snapshots":false,"emit_book_deltas":false,"emit_bbo":false}]})).unwrap();
    std::fs::create_dir(root.join("raw")).unwrap();
    let mut raw =
        RawSegmentWriter::create(File::create(root.join("raw/segment.mfr1")).unwrap(), NOW)
            .unwrap();
    let mut feed = FeedWriter::create(root.join("feed"), 2_000_000).unwrap();
    for m in [build, session] {
        raw.write_metadata(&m, NOW).unwrap();
        feed.append(FeedRecord::Metadata(m)).unwrap();
    }
    feed.append(FeedRecord::artifact(
        "configuration",
        b"# explicit unit fixture",
    ))
    .unwrap();
    feed.append(FeedRecord::artifact("cargo_lock", b"unit-fixture-lock"))
        .unwrap();
    use base64::Engine;
    feed.append(FeedRecord::CatalogResponse {
        venue: 22,
        request_id: 1,
        request_body: r#"{"type":"meta"}"#.into(),
        status: 200,
        body_base64: base64::engine::general_purpose::STANDARD
            .encode(b"unit-fixture-public-catalog"),
        provenance: Some(marketfeed_recording::shared_feed::CatalogHttpProvenance {
            url: "https://api.hyperliquid.xyz/info".into(),
            method: "Post".into(),
            request_body_base64: base64::engine::general_purpose::STANDARD
                .encode(br#"{"type":"meta"}"#),
            request_headers: vec![],
            response_headers: vec![],
            received_at_ns: NOW,
        }),
    })
    .unwrap();
    let connection:FeedRecord=serde_json::from_value(json!({"kind":"connection","data":{"session":7,"connection":9,"now_ns":NOW,"connected":true,"reason":null}})).unwrap();
    feed.append(connection).unwrap();
    for seq in 1..=2 {
        let price = if fault == "conflict" && seq == 2 {
            "101"
        } else {
            "100"
        };
        let payload = format!(
            r#"{{"channel":"trades","data":[{{"coin":"BTC","side":"B","px":"{price}","sz":"2","time":1700000000000,"tid":1,"hash":"fixture"}}]}}"#
        );
        raw.write_record(
            SessionId(7),
            seq,
            NOW + seq as i64,
            seq,
            Direction::Inbound,
            FrameOpcode::Text,
            0,
            payload.as_bytes(),
        )
        .unwrap();
        let received = NOW + seq as i64 + if fault == "receipt" { 100 } else { 0 };
        feed.append(FeedRecord::frame(
            SessionId(7),
            seq,
            received,
            seq,
            0,
            1,
            payload.as_bytes(),
        ))
        .unwrap();
        let event = EventEnvelope {
            schema_version: 1,
            venue: VenueId(22),
            instrument: Some(InstrumentId(1)),
            connection: ConnectionId(if fault == "generation" { 999 } else { 9 }),
            session: SessionId(7),
            frame_seq: seq,
            event_index: 0,
            exchange_ts: Some(TimestampNs(NOW)),
            receive_ts: TimestampNs(received),
            source_sequence: None,
            flags: if seq == 1 {
                EventFlags::INITIAL_DELIVERY.union(EventFlags::ACQUISITION_ORIGIN_UNVERIFIED)
            } else {
                EventFlags::ACQUISITION_ORIGIN_UNVERIFIED
            },
            payload: MarketEvent::Trade(Trade {
                price: Price(Fixed::parse_str(price).unwrap()),
                quantity: Quantity(Fixed::new(2, 0)),
                aggressor: AggressorSide::Buy,
                trade_id: Some(SourceId("BTC:1700000000000:1".into())),
            }),
        };
        feed.append(FeedRecord::Batch(
            serde_json::from_value(json!({"session":7,"frame_seq":seq,"events":[event]})).unwrap(),
        ))
        .unwrap();
        if fault != "unaccepted" {
            feed.append(FeedRecord::FrameAccepted {
                session: SessionId(7),
                frame_seq: seq,
            })
            .unwrap();
        }
    }
    feed.append(FeedRecord::Seal {
        schema_version: 1,
        now_ns: NOW + 10,
        reason: "unit-fixture".into(),
    })
    .unwrap();
    raw.flush().unwrap();
    raw.get_ref().sync_all().unwrap();
    if fault == "tail" {
        use std::io::Write;
        std::fs::OpenOptions::new()
            .append(true)
            .open(root.join("raw/segment.mfr1"))
            .unwrap()
            .write_all(&[1, 2])
            .unwrap();
    }
}
fn publish(store: &Store, root: &Path) -> marketfeed_datasets::Result<String> {
    export::export(
        store,
        &ExportRequest {
            feed: &root.join("feed"),
            raw: &root.join("raw"),
            dataset: "unit-fixture",
            start_ns: NOW - 1,
            end_ns: NOW + 100,
            region: "test",
            expected_parent: None,
        },
    )
}
fn replace_reference(
    value: &mut serde_json::Value,
    old: &serde_json::Value,
    new: &serde_json::Value,
) {
    if value == old {
        *value = new.clone();
        return;
    }
    if let Some(object) = value.as_object_mut() {
        for child in object.values_mut() {
            replace_reference(child, old, new);
        }
    } else if let Some(array) = value.as_array_mut() {
        for child in array {
            replace_reference(child, old, new);
        }
    }
}
fn resign_rows(
    store: &Store,
    mut manifest: serde_json::Value,
    rows: &[marketfeed_datasets::parquet::TradeRow],
) -> serde_json::Value {
    let replacement = store
        .put(&marketfeed_datasets::parquet::write(rows).unwrap())
        .unwrap();
    let object = manifest["objects"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|o| o["kind"] == "canonical-parquet")
        .unwrap();
    object["object"] = serde_json::to_value(replacement).unwrap();
    object["semantic_sha256"] = json!(marketfeed_datasets::sha256(
        &serde_json::to_vec(rows).unwrap()
    ));
    let reference =
        serde_json::from_value(manifest["publication"]["commit_record"].clone()).unwrap();
    let mut commit: serde_json::Value =
        serde_json::from_slice(&store.object(&reference).unwrap()).unwrap();
    commit["objects"] = manifest["objects"].clone();
    manifest["publication"]["commit_record"] =
        serde_json::to_value(store.put(&serde_json::to_vec(&commit).unwrap()).unwrap()).unwrap();
    manifest
}
#[test]
fn coordinated_metadata_and_connection_forgery_cannot_rewrite_original_provenance() {
    let temp = tempfile::tempdir().unwrap();
    fixture(temp.path(), "");
    let store = Store::open(temp.path().join("store")).unwrap();
    let pin = publish(&store, temp.path()).unwrap();
    let manifest = store.published_manifest(&pin).unwrap();
    let rows = export::verify_manifest(&store, &manifest).unwrap();
    for (key, value) in [
        ("validation_policy_ref", json!("f".repeat(64))),
        ("native_attributes_ref", json!("wrong-coordinate")),
        ("collector_side_projection", json!("Sell")),
        ("quality_interval_ref", json!("wrong-stream")),
        ("acquisition_context_id", json!("wrong-context")),
        ("region", json!("invented-region")),
        ("flags", json!(["acquisition-origin-unverified"])),
        ("venue", json!("hyperliquid-spot")),
    ] {
        let mut wrong = rows.clone();
        wrong[0].fields[key] = value;
        let forged = resign_rows(&store, manifest.clone(), &wrong);
        assert!(
            export::verify_manifest(&store, &forged).is_err(),
            "accepted forged {key}"
        );
    }
    let mut wrong = rows.clone();
    wrong[0].fields["connection_generation"] = json!("999999");
    let forged = resign_rows(&store, manifest.clone(), &wrong);
    assert!(
        export::verify_manifest(&store, &forged)
            .unwrap_err()
            .to_string()
            .contains("runtime provenance")
    );
    let old = manifest["provenance"]["instrument_metadata"][0].clone();
    let reference = serde_json::from_value(old.clone()).unwrap();
    let mut session: serde_json::Value =
        serde_json::from_slice(&store.object(&reference).unwrap()).unwrap();
    session["catalog"][0]["quote"] = json!("FAKE_QUOTE");
    let replacement = store.put(&serde_json::to_vec(&session).unwrap()).unwrap();
    let mut wrong = rows;
    wrong[0].fields["metadata_ref"] = json!(replacement.sha256);
    wrong[0].fields["instrument_ref"] = json!(format!("{}:1", replacement.sha256));
    let mut forged = manifest;
    replace_reference(
        &mut forged,
        &old,
        &serde_json::to_value(replacement).unwrap(),
    );
    let forged = resign_rows(&store, forged, &wrong);
    assert!(
        export::verify_manifest(&store, &forged)
            .unwrap_err()
            .to_string()
            .contains("original MFR1 metadata")
    );
}
#[test]
fn exact_publication_deduplicates_sightings_restores_and_never_admits_unknown_coverage() {
    let temp = tempfile::tempdir().unwrap();
    fixture(temp.path(), "");
    let store = Store::open(temp.path().join("store")).unwrap();
    let pin = publish(&store, temp.path()).unwrap();
    assert_eq!(publish(&store, temp.path()).unwrap(), pin);
    let manifest = store.published_manifest(&pin).unwrap();
    let rows = export::verify_manifest(&store, &manifest).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(manifest["accounting"]["duplicate_sightings"], "1");
    for profile in [
        "descriptive",
        "execution-count",
        "signed-execution-flow",
        "confirmed-candle",
        "observed-availability-replay",
        "unknown",
    ] {
        assert!(export::admit(&store, &pin, profile).is_err());
    }
    let backup = temp.path().join("backup");
    store.backup_to(&pin, &backup).unwrap();
    let restored = Store::open(&backup).unwrap();
    assert_eq!(
        export::verify_manifest(&restored, &restored.published_manifest(&pin).unwrap()).unwrap(),
        rows
    );
    assert!(export::admit(&restored, &pin, "descriptive").is_err());
    let sidecar = &manifest["objects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["kind"] == "sidecar")
        .unwrap()["object"];
    let sidecar_ref = serde_json::from_value(sidecar.clone()).unwrap();
    let sidecar: serde_json::Value =
        serde_json::from_slice(&restored.object(&sidecar_ref).unwrap()).unwrap();
    assert_eq!(sidecar["catalog_http"].as_array().unwrap().len(), 1);
    for reference in sidecar["catalog_http"].as_array().unwrap() {
        restored
            .object(&serde_json::from_value(reference.clone()).unwrap())
            .unwrap();
    }

    // A self-consistent Parquet hash/semantic hash/commit cannot hide a price
    // that disagrees with the original immutable source row.
    let mut fake_rows = rows.clone();
    fake_rows[0].price.coefficient = "101".into();
    let replacement = store
        .put(&marketfeed_datasets::parquet::write(&fake_rows).unwrap())
        .unwrap();
    let mut forged = manifest.clone();
    let canonical = forged["objects"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|o| o["kind"] == "canonical-parquet")
        .unwrap();
    canonical["object"] = serde_json::to_value(replacement).unwrap();
    canonical["semantic_sha256"] = json!(marketfeed_datasets::sha256(
        &serde_json::to_vec(&fake_rows).unwrap()
    ));
    let reference =
        serde_json::from_value(manifest["publication"]["commit_record"].clone()).unwrap();
    let mut commit: serde_json::Value =
        serde_json::from_slice(&store.object(&reference).unwrap()).unwrap();
    commit["objects"] = forged["objects"].clone();
    forged["publication"]["commit_record"] =
        serde_json::to_value(store.put(&serde_json::to_vec(&commit).unwrap()).unwrap()).unwrap();
    assert!(export::verify_manifest(&store, &forged).is_err());
    let mut unknown = manifest.clone();
    unknown["unexpected_field"] = json!(true);
    assert!(export::verify_manifest(&store, &unknown).is_err());
}
#[test]
fn source_mismatch_torn_tail_conflicts_and_unaccepted_inputs_never_publish() {
    for fault in ["generation", "receipt", "tail", "conflict", "unaccepted"] {
        let temp = tempfile::tempdir().unwrap();
        fixture(temp.path(), fault);
        let store = Store::open(temp.path().join("store")).unwrap();
        assert!(publish(&store, temp.path()).is_err(), "accepted {fault}");
        assert!(store.head().unwrap().is_none());
    }
}
