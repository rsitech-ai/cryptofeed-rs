//! Reconcile the sealed collector journal against original MFR1 bytes before
//! immutable publication. No absent acquisition fact is silently synthesized.
use crate::{
    Result, fail,
    parquet::{self, Exact, TradeRow},
    sha256,
    store::{ObjectRef, Store},
};
use marketfeed_adapter_hyperliquid::protocol::{self, Message};
use marketfeed_model::{AggressorSide, EventFlags, MarketEvent};
use marketfeed_recording::{
    Direction, FrameOpcode, HEADER_SIZE, MetadataRecord, RawSegmentReader,
    shared_feed::{FeedCursor, FeedReader, FeedRecord, SealedFeedReader},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};
const MAX_INPUT: u64 = 128 * 1024 * 1024;
const MAX_TRADES: usize = 100_000;
pub const POLICY: &str = r#"{"version":1,"source":"hyperliquid-public-ws","admission":"validated-not-qualified","continuity":"unknown","granularity":"unknown","native_side_meaning":"unknown","aggressor":"unknown","quantity_unit":"unknown","availability":"unknown","history_origin":"native-unmarked","strict_profiles":[],"exact_encoding":"i128-twos-complement-big-endian-16+uint8-scale","dedup":"first-sighting-by-venue-coin-time-tid;conflicts-reject","interval":"event-time-[start,end);null-time-reject","lineage":"original-mfr1-bytes-offset-row;full-mfsf1-sidecar"}"#;
const MANIFEST_SCHEMA: &str = include_str!("../../../docs/spec/dataset_manifest_v1.schema.json");
const ROW_SCHEMA: &str = include_str!("../../../docs/spec/hyperliquid_trade_row_v1.schema.json");
fn schema_artifact() -> Result<Vec<u8>> {
    Ok(serde_json::to_vec(
        &json!({"parquet":parquet::SCHEMA,"row":serde_json::from_str::<Value>(ROW_SCHEMA)?}),
    )?)
}
pub struct ExportRequest<'a> {
    pub feed: &'a Path,
    pub raw: &'a Path,
    pub dataset: &'a str,
    pub start_ns: i64,
    pub end_ns: i64,
    pub region: &'a str,
    pub expected_parent: Option<&'a str>,
}
fn utc(ns: i64) -> Result<String> {
    Ok(
        time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(ns))?
            .format(&time::format_description::well_known::Rfc3339)?,
    )
}
fn bounded_file(path: &Path, remaining: &mut u64) -> Result<Vec<u8>> {
    let meta = fs::symlink_metadata(path)?;
    if !meta.is_file() || meta.file_type().is_symlink() || meta.len() > *remaining {
        return Err(fail("input file type or byte budget"));
    }
    use std::io::Read;
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(*remaining + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > *remaining {
        return Err(fail("input byte budget"));
    }
    *remaining -= bytes.len() as u64;
    Ok(bytes)
}
fn object_json(store: &Store, v: &impl serde::Serialize) -> Result<ObjectRef> {
    store.put(&serde_json::to_vec(v)?)
}
fn bounds(values: impl Iterator<Item = i64>) -> Value {
    let values: Vec<_> = values.collect();
    json!([
        values.iter().min().map(ToString::to_string),
        values.iter().max().map(ToString::to_string)
    ])
}
struct Original {
    object: ObjectRef,
    offset: u64,
    payload_sha: String,
    receive: i64,
    monotonic: u64,
}
struct NativeFrame {
    receive: i64,
    payload: Vec<u8>,
    canonical: Vec<marketfeed_model::EventEnvelope>,
    connection: marketfeed_model::ConnectionId,
}

pub fn export(store: &Store, request: &ExportRequest<'_>) -> Result<String> {
    if request.dataset.is_empty()
        || request.dataset.len() > 128
        || !request
            .dataset
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        || matches!(request.dataset, "." | "..")
    {
        return Err(fail("invalid dataset ID"));
    }
    if request.start_ns >= request.end_ns || request.region.is_empty() || request.region.len() > 128
    {
        return Err(fail("invalid requested interval or region"));
    }
    store.recover()?;
    if let Some(parent) = request.expected_parent
        && store.published_manifest(parent)?["dataset_id"] != request.dataset
    {
        return Err(fail("parent belongs to another dataset"));
    }
    let mut remaining = MAX_INPUT;
    let mut paths: Vec<_> = fs::read_dir(request.raw)?
        .map(|p| p.map(|e| e.path()))
        .collect::<std::io::Result<_>>()?;
    if paths.len() > 4096 {
        return Err(fail("raw object count budget"));
    }
    paths.sort();
    let mut original = BTreeMap::new();
    let mut raw_objects = Vec::new();
    let mut raw_counts = BTreeMap::new();
    let mut raw_metadata = BTreeMap::new();
    for path in paths {
        if path.extension().is_none_or(|s| s != "mfr1") {
            return Err(fail("unexpected file in raw capture directory"));
        }
        let bytes = bounded_file(&path, &mut remaining)?;
        let byte_len = bytes.len() as u64;
        let reference = store.put(&bytes)?;
        let records = RawSegmentReader::from_bytes(bytes)?.read_all()?;
        raw_counts.insert(reference.sha256.clone(), records.len());
        let mut offset = HEADER_SIZE as u64;
        for record in records {
            if record.header.opcode == FrameOpcode::Metadata {
                let metadata = marketfeed_recording::decode_metadata(&record.payload)?;
                if raw_metadata
                    .insert(metadata.stable_key(), metadata.clone())
                    .is_some_and(|previous| previous != metadata)
                {
                    return Err(fail("conflicting original segment metadata"));
                }
            }
            if record.header.direction == Direction::Inbound
                && record.header.opcode == FrameOpcode::Text
            {
                let key = (record.header.session.0, record.header.frame_seq);
                if original
                    .insert(
                        key,
                        Original {
                            object: reference.clone(),
                            offset,
                            payload_sha: sha256(&record.payload),
                            receive: record.header.receive_ts_ns,
                            monotonic: record.header.monotonic_ns,
                        },
                    )
                    .is_some()
                {
                    return Err(fail("duplicate original raw ordinal"));
                }
            }
            offset += u64::from(record.header.record_len);
        }
        if offset != byte_len {
            return Err(fail("raw segment has an incomplete tail"));
        }
        raw_objects.push(reference);
    }
    if raw_objects.is_empty() {
        return Err(fail("no original MFR1 objects"));
    }
    let mut feed_remaining = MAX_INPUT;
    let journal_bytes = bounded_file(&request.feed.join("events.mfsf"), &mut feed_remaining)?;
    let journal_ref = store.put(&journal_bytes)?;
    let mut reader = FeedReader::open(request.feed, None)?;
    let mut sessions = BTreeMap::new();
    let mut metadata_refs = BTreeMap::new();
    let mut artifacts = BTreeMap::new();
    let mut build = None;
    let mut catalog_http = Vec::new();
    let mut pending = BTreeMap::<(u64, u64), NativeFrame>::new();
    let mut accepted = BTreeSet::new();
    let mut rows = Vec::<TradeRow>::new();
    let mut identities = BTreeMap::<String, usize>::new();
    let mut sightings = Vec::new();
    let mut native_count = 0usize;
    let mut controls = 0usize;
    let mut trade_frames = 0usize;
    let mut excluded = 0usize;
    let mut duplicates = 0usize;
    let mut initial_rows = 0usize;
    let mut received = 0usize;
    let mut source_failures = Vec::new();
    let mut seal = None;
    let mut row_bytes = 0usize;
    let mut connection_start = BTreeMap::new();
    let mut active_connection = BTreeMap::new();
    let mut initial_seen = BTreeSet::new();
    while let Some(record) = reader.read_next()? {
        match record {
            FeedRecord::Artifact {
                role,
                payload_base64,
            } => {
                use base64::Engine;
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(payload_base64)
                    .map_err(|e| fail(&e.to_string()))?;
                if artifacts.insert(role, store.put(&bytes)?).is_some() {
                    return Err(fail("duplicate capture artifact"));
                }
            }
            FeedRecord::Metadata(MetadataRecord::Build(b)) => {
                if raw_metadata.get("build") != Some(&MetadataRecord::Build(b.clone())) {
                    return Err(fail("original/journal build metadata mismatch"));
                }
                if build.replace(b).is_some() {
                    return Err(fail("duplicate build metadata"));
                }
            }
            FeedRecord::Metadata(MetadataRecord::Session(m)) => {
                if raw_metadata.get(&format!("session:{}", m.session_id))
                    != Some(&MetadataRecord::Session(m.clone()))
                {
                    return Err(fail("original/journal session metadata mismatch"));
                }
                if !matches!(m.venue_id, 21 | 22)
                    || m.schema_version != 1
                    || m.environment != "production"
                {
                    return Err(fail("unsupported source/session version or environment"));
                }
                metadata_refs.insert(m.session_id, object_json(store, &m)?);
                if sessions.insert(m.session_id, m).is_some() || sessions.len() > 10 {
                    return Err(fail("duplicate or excessive sessions"));
                }
            }
            FeedRecord::CatalogResponse { ref provenance, .. } => {
                if provenance.is_none() {
                    return Err(fail("catalog acquisition provenance missing"));
                }
                catalog_http.push(object_json(store, &record)?);
            }
            FeedRecord::Frame {
                session,
                frame_seq,
                receive_ts_ns,
                monotonic_ns,
                direction: 0,
                opcode: 1,
                ..
            } => {
                if !sessions.contains_key(&session.0) {
                    return Err(fail("original frame precedes session metadata"));
                }
                let payload = record
                    .payload_bytes()?
                    .ok_or_else(|| fail("missing frame bytes"))?;
                let key = (session.0, frame_seq);
                let source = original
                    .get(&key)
                    .ok_or_else(|| fail("journal frame missing from original MFR1"))?;
                if sha256(&payload) != source.payload_sha
                    || receive_ts_ns != source.receive
                    || monotonic_ns != source.monotonic
                {
                    return Err(fail("MFR1/journal payload mismatch"));
                }
                if pending
                    .insert(
                        key,
                        NativeFrame {
                            receive: receive_ts_ns,
                            payload,
                            canonical: Vec::new(),
                            connection: *active_connection
                                .get(&session.0)
                                .ok_or_else(|| fail("source frame outside recorded connection"))?,
                        },
                    )
                    .is_some()
                    || pending.len() > 10
                {
                    return Err(fail("duplicate or unbounded pending input"));
                }
                received += 1;
            }
            FeedRecord::Batch(batch) => {
                for event in batch.events {
                    if let MarketEvent::Trade(_) = &event.payload {
                        if event.session != batch.session || event.frame_seq != batch.frame_seq {
                            return Err(fail("canonical batch identity mismatch"));
                        }
                        let input = pending
                            .get_mut(&(batch.session.0, batch.frame_seq))
                            .ok_or_else(|| fail("canonical trade has no original input"))?;
                        input.canonical.push(event);
                    }
                }
            }
            FeedRecord::FrameAccepted { session, frame_seq } => {
                let key = (session.0, frame_seq);
                if !accepted.insert(key) {
                    return Err(fail("duplicate input acceptance"));
                }
                let frame = pending
                    .remove(&key)
                    .ok_or_else(|| fail("acceptance has no original input"))?;
                let Message::Trades(native) = protocol::decode(&frame.payload)? else {
                    if !frame.canonical.is_empty() {
                        return Err(fail("trade emitted from a control/nontrade input"));
                    }
                    controls += 1;
                    continue;
                };
                trade_frames += 1;
                if native.len() != frame.canonical.len() {
                    return Err(fail("native/canonical row count mismatch"));
                }
                let m = &sessions[&session.0];
                let source = &original[&key];
                let initial_coins: BTreeSet<_> = native
                    .iter()
                    .map(|t| t.coin.clone())
                    .filter(|coin| {
                        !initial_seen.contains(&(session.0, frame.connection.0, coin.clone()))
                    })
                    .collect();
                for coin in &initial_coins {
                    initial_seen.insert((session.0, frame.connection.0, coin.clone()));
                }
                for (row_index, (native, event)) in
                    native.into_iter().zip(frame.canonical).enumerate()
                {
                    let MarketEvent::Trade(trade) = event.payload else {
                        return Err(fail("expected canonical trade"));
                    };
                    let instrument = m
                        .catalog
                        .iter()
                        .find(|i| {
                            Some(marketfeed_model::InstrumentId(i.instrument_id))
                                == event.instrument
                        })
                        .ok_or_else(|| fail("trade instrument absent from catalog"))?;
                    let expected_side = if native.side == "B" {
                        AggressorSide::Buy
                    } else {
                        AggressorSide::Sell
                    };
                    let time = native
                        .time
                        .checked_mul(1_000_000)
                        .ok_or_else(|| fail("source timestamp overflow"))?;
                    if event.connection != frame.connection
                        || event.venue.0 != m.venue_id
                        || instrument.native_symbol != native.coin
                        || event.event_index as usize != row_index
                        || trade.trade_id.as_ref().map(|s| s.0.as_str())
                            != Some(native.unique_id().as_str())
                        || trade.price.0 != native.px.0
                        || trade.quantity.0 != native.sz.0
                        || trade.aggressor != expected_side
                        || event.exchange_ts.map(|t| t.0) != Some(time)
                        || event.receive_ts.0 != frame.receive
                    {
                        return Err(fail("native/canonical semantic mismatch"));
                    }
                    native_count += 1;
                    if native_count > MAX_TRADES {
                        return Err(fail("trade row budget"));
                    }
                    let initial = event.flags.contains(EventFlags::INITIAL_DELIVERY);
                    if initial != initial_coins.contains(&native.coin)
                        || !event
                            .flags
                            .contains(EventFlags::ACQUISITION_ORIGIN_UNVERIFIED)
                    {
                        return Err(fail("canonical acquisition provenance mismatch"));
                    }
                    if initial {
                        initial_rows += 1;
                    }
                    let identity = format!("{}:{}", m.venue_id, native.unique_id());
                    let coordinate =
                        format!("{}:{}:{row_index}", source.object.sha256, source.offset);
                    if time < request.start_ns || time >= request.end_ns {
                        excluded += 1;
                        sightings.push(json!({"identity":identity,"coordinate":coordinate,"disposition":"outside-requested-event-time"}));
                        continue;
                    }
                    if let Some(previous) = identities.get(&identity) {
                        let row = &rows[*previous];
                        if row.price.fixed()?.numeric_cmp(native.px.0) != std::cmp::Ordering::Equal
                            || row.native_quantity.fixed()?.numeric_cmp(native.sz.0)
                                != std::cmp::Ordering::Equal
                            || row.fields["native_side"] != native.side
                        {
                            return Err(fail("conflicting duplicate identity"));
                        }
                        duplicates += 1;
                        sightings.push(json!({"identity":identity,"coordinate":coordinate,"disposition":"duplicate","canonical_row":previous.to_string()}));
                        continue;
                    }
                    let stream = format!("hyperliquid:{}:{}", m.venue_id, native.coin);
                    let fields = json!({
                        "schema_version":"1","dataset_id":request.dataset,"venue":format!("hyperliquid-{}", if m.venue_id==21 {"spot"} else {"perps"}),
                        "region":request.region,"api_product":if m.venue_id==21 {"spot"} else {"linear-perpetuals"},"protocol":"websocket-json","channel":"trades",
                        "native_symbol":native.coin,"instrument_ref":format!("{}:{}", metadata_refs[&session.0].sha256,instrument.instrument_id),"metadata_ref":metadata_refs[&session.0].sha256,
                        "native_trade_id":native.tid.to_string(),"identity":identity,"identity_scope":"venue-coin-block-time-tid","identity_epoch":"native-block-time",
                        "native_sequence":null,"granularity":"unknown","aggregate_id":null,"constituent_first_id":null,"constituent_last_id":null,"constituent_count":null,
                        "event_time_ns":time.to_string(),"publication_time_ns":null,"receipt_time_ns":frame.receive.to_string(),"retrieval_time_ns":null,"availability_time_ns":null,
                        "native_time_value":native.time.to_string(),"native_time_unit":"ms","native_time_resolution":"1ms","timestamp_origin":"venue-field",
                        "native_quantity_unit":"unknown","base_quantity":null,"quote_quantity":null,"conversion_ref":null,
                        "native_side":native.side,"native_side_meaning":"unknown","aggressor":"unknown","collector_side_projection":format!("{expected_side:?}"),
                        "flags":if initial {vec!["initial-delivery","acquisition-origin-unverified"]} else {vec!["acquisition-origin-unverified"]},"native_attributes_ref":coordinate,
                        "revision_operation":"insert","revision_target":null,"native_revision_id":null,"revision_order":null,"revision_known_at_ns":null,
                        "collector_id":reader.cursor().feed_id,"acquisition_context_id":format!("{}:session:{}", reader.cursor().feed_id, session.0),
                        "session_id":session.0.to_string(),"connection_generation":event.connection.0.to_string(),"capture_ordinal":frame_seq.to_string(),
                        "lineage_kind":"mfr1","raw_record_offset":source.offset.to_string(),"raw_row_index":row_index.to_string(),"raw_object_sha256":source.object.sha256,
                        "archive_member":null,"decoded_record_index":null,"source_coordinate":null,"codec_ref":null,
                        "acquisition_mode":"live","validation_policy_ref":sha256(POLICY.as_bytes()),"quality_interval_ref":stream
                    });
                    let row = TradeRow {
                        price: Exact::from(native.px.0),
                        native_quantity: Exact::from(native.sz.0),
                        fields,
                    };
                    row_bytes += serde_json::to_vec(&row)?.len();
                    if row_bytes > MAX_INPUT as usize {
                        return Err(fail("canonical memory byte budget"));
                    }
                    let index = rows.len();
                    identities.insert(identity.clone(), index);
                    sightings.push(json!({"identity":identity,"coordinate":coordinate,"disposition":"canonical","canonical_row":index.to_string()}));
                    rows.push(row);
                }
            }
            FeedRecord::Connection {
                session,
                connected: true,
                connection,
                now_ns,
                ..
            } => {
                connection_start.entry(session.0).or_insert(now_ns);
                if active_connection.insert(session.0, connection).is_some() {
                    return Err(fail("connection opened without closing prior generation"));
                }
            }
            FeedRecord::Connection {
                session,
                connection,
                connected: false,
                reason,
                ..
            } => {
                if active_connection
                    .remove(&session.0)
                    .is_some_and(|active| active != connection)
                {
                    return Err(fail("disconnect generation mismatch"));
                }
                if reason
                    .as_deref()
                    .is_none_or(|s| !s.starts_with("LocalStop"))
                {
                    source_failures.push("transport-discontinuity");
                }
            }
            FeedRecord::System {
                event:
                    marketfeed_model::SystemEvent::ParseError { .. }
                    | marketfeed_model::SystemEvent::EventsDropped { .. }
                    | marketfeed_model::SystemEvent::SequenceGap { .. }
                    | marketfeed_model::SystemEvent::ChecksumMismatch { .. },
                ..
            } => source_failures.push("source-system-failure"),
            FeedRecord::Seal {
                schema_version: 1,
                now_ns,
                ..
            } => seal = Some(now_ns),
            _ => {}
        }
    }
    let seal = seal.ok_or_else(|| fail("capture is not sealed"))?;
    if !pending.is_empty() || received != accepted.len() || original.len() != received {
        return Err(fail("unaccepted or unmatched original source frames"));
    }
    if reader.cursor().offset != journal_bytes.len() as u64 {
        return Err(fail("sealed journal has an uncommitted tail"));
    }
    if rows.is_empty() {
        return Err(fail("requested interval has no canonical rows"));
    }
    let build = build.ok_or_else(|| fail("missing build metadata"))?;
    let source_commit = build
        .build_sha
        .as_deref()
        .ok_or_else(|| fail("capture source commit not recorded"))?;
    if source_commit.len() != 40 || !source_commit.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(fail("invalid capture source commit"));
    }
    let configuration = artifacts
        .get("configuration")
        .ok_or_else(|| fail("capture configuration missing"))?;
    let lockfile = artifacts
        .get("cargo_lock")
        .ok_or_else(|| fail("capture lockfile missing"))?;
    let checkpoint = object_json(store, reader.cursor())?;
    let policy = store.put(POLICY.as_bytes())?;
    let schema = store.put(&schema_artifact()?)?;
    let build_ref = object_json(store, &build)?;
    let decoder = object_json(
        store,
        &json!({"collector_source_commit":source_commit,"exporter_build_sha":option_env!("MARKETFEED_BUILD_SHA"),"protocol_source":include_str!("../../adapters/hyperliquid/src/protocol.rs"),"exporter_source_sha256":sha256(include_bytes!("export.rs"))}),
    )?;
    let sidecar = object_json(
        store,
        &json!({"sightings":sightings,"initial_delivery_rows":initial_rows.to_string(),"catalog_http":catalog_http,"journal":journal_ref}),
    )?;
    let evidence = object_json(
        store,
        &json!({"native_canonical_comparison":"exact-price-quantity-time-side-token-runtime-instrument-ordinal","received":received.to_string(),"native_rows":native_count.to_string(),"canonical_rows":rows.len().to_string(),"duplicates":duplicates.to_string(),"exclusions":excluded.to_string(),"conflicts":"0","source_failures":source_failures,"capture_sealed_at_ns":seal.to_string(),"continuity":"unknown"}),
    )?;
    let revision = sha256(&serde_json::to_vec(
        &json!({"dataset":request.dataset,"parent":request.expected_parent,"feed":reader.cursor(),"journal":journal_ref,"artifacts":artifacts,"raw_objects":raw_objects,"policy":policy,"schema":schema,"decoder":decoder,"source_rows_sha256":sha256(&serde_json::to_vec(&rows)?),"region":request.region,"start":request.start_ns,"end":request.end_ns}),
    )?);
    if let Some(head) = store.head()? {
        let previous = store.published_manifest(&head)?;
        if previous["revision_id"] == revision && previous["dataset_id"] == request.dataset {
            verify_manifest(store, &previous)?;
            return Ok(head);
        }
    }
    if store.head()?.as_deref() != request.expected_parent {
        return Err(fail("manifest parent CAS failed"));
    }
    for row in &mut rows {
        row.fields["revision_id"] = json!(revision);
    }
    let parquet_bytes = parquet::write(&rows)?;
    if parquet::read(parquet_bytes.clone())? != rows {
        return Err(fail("Parquet semantic roundtrip mismatch"));
    }
    let canonical = store.put(&parquet_bytes)?;
    let semantic = sha256(&serde_json::to_vec(&rows)?);
    let observed: BTreeSet<_> = rows
        .iter()
        .map(|r| {
            r.fields["quality_interval_ref"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        })
        .collect();
    let interval = json!({"start_utc":utc(request.start_ns)?,"end_utc":utc(request.end_ns)?,"boundary":"[start,end)"});
    let event_bounds = bounds(rows.iter().map(|r| {
        r.fields["event_time_ns"]
            .as_str()
            .unwrap_or_default()
            .parse()
            .unwrap_or_default()
    }));
    let receipt_bounds = bounds(rows.iter().map(|r| {
        r.fields["receipt_time_ns"]
            .as_str()
            .unwrap_or_default()
            .parse()
            .unwrap_or_default()
    }));
    let metadata: Vec<_> = metadata_refs.values().cloned().collect();
    let acquisitions: Vec<_> = sessions.values().map(|m| Ok(json!({"mode":"live","venue":format!("hyperliquid-{}",m.venue_id),"region":request.region,"api_product":if m.venue_id==21 {"spot"} else {"linear-perpetuals"},"endpoint":m.endpoint,"channel":"trades","capture_or_retrieval_utc":utc(*connection_start.get(&m.session_id).ok_or_else(|| fail("missing acquisition connection time"))?)?,"context_id":format!("{}:session:{}",reader.cursor().feed_id,m.session_id),"cursor_checkpoint":checkpoint,"live_backfill_boundaries":[]}))).collect::<Result<_>>()?;
    let requested: BTreeSet<String> = sessions
        .values()
        .flat_map(|m| {
            m.initial_subscriptions
                .iter()
                .filter(|s| s.channel == "Trades")
                .filter_map(|s| {
                    m.catalog
                        .iter()
                        .find(|i| i.instrument_id == s.instrument_id)
                        .map(|i| format!("hyperliquid:{}:{}", m.venue_id, i.native_symbol))
                })
        })
        .collect();
    let coverage_state = if source_failures.is_empty() {
        "unknown-continuity"
    } else {
        "suspected-gap"
    };
    let decision = if source_failures.is_empty() {
        "validated-not-qualified"
    } else {
        "quarantined"
    };
    let coverage: Vec<_> = requested.iter().map(|s| json!({"stream_identity":s,"interval":interval,"state":coverage_state,"evidence":[evidence],"reason_codes":["native-continuity-not-proven"]})).collect();
    let mut objects: Vec<_> = raw_objects.iter().map(|r| json!({"object":r,"kind":"mfr1","rows":raw_counts[&r.sha256].to_string(),"event_time_bounds_ns":[null,null],"semantic_sha256":null,"raw_parents":[],"metadata_parents":metadata})).collect();
    objects.push(json!({"object":canonical,"kind":"canonical-parquet","rows":rows.len().to_string(),"event_time_bounds_ns":event_bounds,"semantic_sha256":semantic,"raw_parents":raw_objects,"metadata_parents":metadata}));
    let sidecar_dependencies: Vec<_> = metadata
        .iter()
        .chain(catalog_http.iter())
        .cloned()
        .collect();
    objects.push(json!({"object":sidecar,"kind":"sidecar","rows":native_count.to_string(),"event_time_bounds_ns":[null,null],"semantic_sha256":null,"raw_parents":[journal_ref],"metadata_parents":sidecar_dependencies}));
    let published_at = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos(),
    )?;
    let commit = object_json(
        store,
        &json!({"schema_version":1,"dataset_id":request.dataset,"revision_id":revision,"parent_manifest_sha256":request.expected_parent,"objects":objects,"checkpoint":checkpoint}),
    )?;
    let manifest = json!({
        "schema_version":1,"dataset_id":request.dataset,"revision_id":revision,"parent_manifest_sha256":request.expected_parent,
        "requested":{"instruments":requested,"utc_interval":interval},"observed":{"instruments":observed,"event_time_bounds_ns":event_bounds,"receipt_time_bounds_ns":receipt_bounds,"availability_time_bounds_ns":[null,null]},
        "provenance":{"source_commit":source_commit,"build":build_ref,"configuration":configuration,"features":["unknown"],"lockfile":lockfile,"decoder":decoder,"schema":schema,"instrument_metadata":metadata,"validation_policy":policy,"acquisitions":acquisitions},
        "objects":objects,"accounting":{"received_messages":received.to_string(),"trade_bearing_messages":trade_frames.to_string(),"native_rows":native_count.to_string(),"canonical_rows":rows.len().to_string(),"controls":controls.to_string(),"intentional_exclusions":excluded.to_string(),"rejected_rows":"0","quarantined_rows":"0","duplicate_sightings":duplicates.to_string(),"conflicting_duplicates":"0","writer_accepted_rows":rows.len().to_string(),"synced_rows":rows.len().to_string(),"manifest_committed_rows":rows.len().to_string()},
        "aggregate_constituents":null,"coverage":coverage,"reconciliation_evidence":[evidence],"watermark_checkpoint":checkpoint,
        "quality":{"decision":decision,"supported_profiles":[],"reason_codes":["unknown-continuity","unverified-native-granularity","unverified-native-side-meaning","unverified-native-quantity-unit","availability-time-unrecorded","build-features-unrecorded"],"evidence":[evidence]},
        "publication":{"status":"published","backend":"local-filesystem","commit_record":commit,"published_at_utc":utc(published_at)?}
    });
    verify_manifest(store, &manifest)?;
    store.publish(manifest, request.expected_parent)
}

/// Inspect is explicit and cannot authorize a research profile. Strict admission
/// always requires both the requested profile and qualified interval coverage.
pub fn verify_manifest(store: &Store, manifest: &Value) -> Result<Vec<TradeRow>> {
    let schema: Value = serde_json::from_str(MANIFEST_SCHEMA)?;
    let validator = jsonschema::options()
        .should_validate_formats(true)
        .build(&schema)?;
    validator
        .validate(manifest)
        .map_err(|error| fail(&format!("manifest schema: {error}")))?;
    if manifest["schema_version"] != 1 || manifest["publication"]["status"] != "published" {
        return Err(fail("unsupported or unpublished manifest"));
    }
    store.verify_references(manifest)?;
    let policy: ObjectRef =
        serde_json::from_value(manifest["provenance"]["validation_policy"].clone())?;
    let schema: ObjectRef = serde_json::from_value(manifest["provenance"]["schema"].clone())?;
    if store.object(&policy)? != POLICY.as_bytes() || store.object(&schema)? != schema_artifact()? {
        return Err(fail("unsupported validation policy or row schema"));
    }
    if !matches!(
        manifest["quality"]["decision"].as_str(),
        Some("validated-not-qualified" | "quarantined")
    ) || manifest["quality"]["supported_profiles"] != json!([])
    {
        return Err(fail("this validation policy cannot qualify a dataset"));
    }
    let objects = manifest["objects"]
        .as_array()
        .ok_or_else(|| fail("missing dataset objects"))?;
    let mut rows = Vec::new();
    for object in objects.iter().filter(|o| o["kind"] == "canonical-parquet") {
        let reference: ObjectRef = serde_json::from_value(object["object"].clone())?;
        let part = parquet::read(store.object(&reference)?)?;
        if object["rows"].as_str() != Some(part.len().to_string().as_str())
            || object["semantic_sha256"].as_str()
                != Some(sha256(&serde_json::to_vec(&part)?).as_str())
        {
            return Err(fail("canonical semantic hash/count mismatch"));
        }
        rows.extend(part);
        if rows.len() > MAX_TRADES {
            return Err(fail("dataset reader row budget"));
        }
    }
    if manifest["accounting"]["canonical_rows"].as_str() != Some(rows.len().to_string().as_str()) {
        return Err(fail("manifest accounting mismatch"));
    }
    let row_schema: Value = serde_json::from_str(ROW_SCHEMA)?;
    let row_validator = jsonschema::validator_for(&row_schema)?;
    for row in &rows {
        row_validator
            .validate(&serde_json::to_value(row)?)
            .map_err(|error| fail(&format!("row schema: {error}")))?;
        if row.fields["dataset_id"] != manifest["dataset_id"]
            || row.fields["revision_id"] != manifest["revision_id"]
            || row.price.fixed()?.coefficient <= 0
            || row.native_quantity.fixed()?.coefficient <= 0
        {
            return Err(fail("row identity or financial value mismatch"));
        }
    }
    let commit_ref: ObjectRef =
        serde_json::from_value(manifest["publication"]["commit_record"].clone())?;
    let commit: Value = serde_json::from_slice(&store.object(&commit_ref)?)?;
    if commit["schema_version"] != 1
        || commit["dataset_id"] != manifest["dataset_id"]
        || commit["revision_id"] != manifest["revision_id"]
        || commit["parent_manifest_sha256"] != manifest["parent_manifest_sha256"]
        || commit["objects"] != manifest["objects"]
        || commit["checkpoint"] != manifest["watermark_checkpoint"]
    {
        return Err(fail("publication commit does not bind this manifest"));
    }
    verify_row_lineage(store, manifest, &rows)?;
    Ok(rows)
}

fn verify_row_lineage(store: &Store, manifest: &Value, rows: &[TradeRow]) -> Result<()> {
    let mut metadata = BTreeMap::new();
    for value in manifest["provenance"]["instrument_metadata"]
        .as_array()
        .ok_or_else(|| fail("missing instrument metadata"))?
    {
        let reference: ObjectRef = serde_json::from_value(value.clone())?;
        let session: marketfeed_recording::SessionRecordingMetadata =
            serde_json::from_slice(&store.object(&reference)?)?;
        if metadata.insert(reference.sha256, session).is_some() {
            return Err(fail("duplicate metadata parent"));
        }
    }
    let needed: BTreeSet<_> = rows
        .iter()
        .map(|row| {
            Ok((
                row.fields["raw_object_sha256"]
                    .as_str()
                    .ok_or_else(|| fail("missing raw hash"))?
                    .to_owned(),
                row.fields["raw_record_offset"]
                    .as_str()
                    .ok_or_else(|| fail("missing raw offset"))?
                    .parse::<u64>()?,
            ))
        })
        .collect::<Result<_>>()?;
    let mut sources = BTreeMap::new();
    let mut original_metadata = BTreeMap::new();
    let mut total = 0usize;
    for object in manifest["objects"]
        .as_array()
        .ok_or_else(|| fail("missing objects"))?
        .iter()
        .filter(|o| o["kind"] == "mfr1")
    {
        let reference: ObjectRef = serde_json::from_value(object["object"].clone())?;
        let bytes = store.object(&reference)?;
        total += bytes.len();
        if total > MAX_INPUT as usize {
            return Err(fail("lineage byte budget"));
        }
        let mut reader = RawSegmentReader::open(std::io::Cursor::new(bytes.as_slice()))?;
        let mut offset = HEADER_SIZE as u64;
        while let Some(record) = reader.read_record()? {
            if record.header.opcode == FrameOpcode::Metadata {
                let item = marketfeed_recording::decode_metadata(&record.payload)?;
                if original_metadata
                    .insert(item.stable_key(), item.clone())
                    .is_some_and(|previous| previous != item)
                {
                    return Err(fail("conflicting immutable original metadata"));
                }
            }
            if needed.contains(&(reference.sha256.clone(), offset)) {
                if record.header.direction != Direction::Inbound
                    || record.header.opcode != FrameOpcode::Text
                {
                    return Err(fail("trade lineage is not a native text input"));
                }
                let Message::Trades(trades) = protocol::decode(&record.payload)? else {
                    return Err(fail("trade lineage points to a nontrade input"));
                };
                sources.insert(
                    (reference.sha256.clone(), offset),
                    (record.header.clone(), trades, sha256(&record.payload)),
                );
            }
            offset += u64::from(record.header.record_len);
        }
        if offset != bytes.len() as u64 {
            return Err(fail("incomplete immutable raw object"));
        }
    }
    for session in metadata.values() {
        if original_metadata.get(&format!("session:{}", session.session_id))
            != Some(&MetadataRecord::Session(session.clone()))
        {
            return Err(fail(
                "effective metadata differs from original MFR1 metadata",
            ));
        }
    }
    let sidecars: Vec<_> = manifest["objects"]
        .as_array()
        .ok_or_else(|| fail("missing objects"))?
        .iter()
        .filter(|o| o["kind"] == "sidecar")
        .collect();
    if sidecars.len() != 1 {
        return Err(fail("expected one source journal sidecar"));
    }
    let sidecar_ref: ObjectRef = serde_json::from_value(sidecars[0]["object"].clone())?;
    let sidecar: Value = serde_json::from_slice(&store.object(&sidecar_ref)?)?;
    let journal_ref: ObjectRef = serde_json::from_value(sidecar["journal"].clone())?;
    let checkpoint_ref: ObjectRef =
        serde_json::from_value(manifest["watermark_checkpoint"].clone())?;
    let checkpoint: FeedCursor = serde_json::from_slice(&store.object(&checkpoint_ref)?)?;
    let journal_bytes = store.object(&journal_ref)?;
    if journal_bytes.len() > MAX_INPUT as usize {
        return Err(fail("journal lineage byte budget"));
    }
    let mut journal = SealedFeedReader::new(&journal_bytes, checkpoint.clone())?;
    let ordinals: BTreeSet<_> = sources
        .values()
        .map(|source| (source.0.session.0, source.0.frame_seq))
        .collect();
    let mut connections = BTreeMap::new();
    let mut journal_sources = BTreeMap::new();
    let mut accepted = BTreeSet::new();
    let mut initial_seen = BTreeSet::new();
    let mut initial_inputs = BTreeSet::new();
    let mut trade_count = 0usize;
    let mut journal_catalog = Vec::new();
    while let Some(record) = journal.read_next()? {
        match record {
            FeedRecord::Artifact {
                role,
                payload_base64,
            } => {
                let key = match role.as_str() {
                    "configuration" => "configuration",
                    "cargo_lock" => "lockfile",
                    _ => return Err(fail("unknown capture artifact")),
                };
                use base64::Engine;
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(payload_base64)
                    .map_err(|e| fail(&e.to_string()))?;
                if manifest["provenance"][key]["sha256"] != sha256(&bytes) {
                    return Err(fail("capture artifact differs from immutable journal"));
                }
            }
            FeedRecord::CatalogResponse { .. } => {
                journal_catalog.push(sha256(&serde_json::to_vec(&record)?));
            }
            FeedRecord::Metadata(item) => {
                if original_metadata.get(&item.stable_key()) != Some(&item) {
                    return Err(fail("journal/original metadata mismatch"));
                }
                if let MetadataRecord::Build(build) = item {
                    let reference: ObjectRef =
                        serde_json::from_value(manifest["provenance"]["build"].clone())?;
                    if store.object(&reference)? != serde_json::to_vec(&build)?
                        || manifest["provenance"]["source_commit"].as_str()
                            != build.build_sha.as_deref()
                    {
                        return Err(fail("build provenance differs from immutable original"));
                    }
                }
            }
            FeedRecord::Connection {
                session,
                connection,
                connected: true,
                ..
            } => {
                if connections.len() >= 10 || connections.insert(session.0, connection.0).is_some()
                {
                    return Err(fail("invalid journal connection open"));
                }
            }
            FeedRecord::Connection {
                session,
                connection,
                connected: false,
                ..
            } => {
                if connections
                    .remove(&session.0)
                    .is_some_and(|active| active != connection.0)
                {
                    return Err(fail("invalid journal disconnect"));
                }
            }
            FeedRecord::Frame {
                session,
                frame_seq,
                direction: 0,
                opcode: 1,
                receive_ts_ns,
                monotonic_ns,
                ref payload_base64,
            } => {
                use base64::Engine;
                let payload = base64::engine::general_purpose::STANDARD
                    .decode(payload_base64)
                    .map_err(|e| fail(&e.to_string()))?;
                let generation = *connections
                    .get(&session.0)
                    .ok_or_else(|| fail("journal frame outside connection"))?;
                if let Message::Trades(trades) = protocol::decode(&payload)? {
                    trade_count += trades.len();
                    if trade_count > MAX_TRADES {
                        return Err(fail("journal trade lineage budget"));
                    }
                    let first: BTreeSet<_> = trades
                        .iter()
                        .map(|t| t.coin.clone())
                        .filter(|coin| {
                            !initial_seen.contains(&(session.0, generation, coin.clone()))
                        })
                        .collect();
                    for coin in first {
                        initial_seen.insert((session.0, generation, coin.clone()));
                        initial_inputs.insert((session.0, frame_seq, coin));
                    }
                }
                if !ordinals.contains(&(session.0, frame_seq)) {
                    continue;
                }
                if journal_sources
                    .insert(
                        (session.0, frame_seq),
                        (generation, receive_ts_ns, monotonic_ns, sha256(&payload)),
                    )
                    .is_some()
                {
                    return Err(fail("duplicate journal ordinal"));
                }
            }
            FeedRecord::FrameAccepted { session, frame_seq }
                if ordinals.contains(&(session.0, frame_seq)) =>
            {
                if !journal_sources.contains_key(&(session.0, frame_seq))
                    || !accepted.insert((session.0, frame_seq))
                {
                    return Err(fail("invalid journal acceptance"));
                }
            }
            _ => {}
        }
    }
    let catalog_refs: Vec<ObjectRef> = serde_json::from_value(sidecar["catalog_http"].clone())?;
    if catalog_refs
        .iter()
        .map(|r| r.sha256.clone())
        .collect::<Vec<_>>()
        != journal_catalog
    {
        return Err(fail("catalog lineage differs from immutable journal"));
    }
    for row in rows {
        let text = |key: &str| {
            row.fields[key]
                .as_str()
                .ok_or_else(|| fail("missing lineage field"))
        };
        let source = sources
            .get(&(
                text("raw_object_sha256")?.to_owned(),
                text("raw_record_offset")?.parse()?,
            ))
            .ok_or_else(|| fail("missing raw lineage record"))?;
        let native = source
            .1
            .get(text("raw_row_index")?.parse::<usize>()?)
            .ok_or_else(|| fail("raw row index outside native frame"))?;
        let ordinal = (source.0.session.0, source.0.frame_seq);
        let journal_source = journal_sources
            .get(&ordinal)
            .ok_or_else(|| fail("source absent from immutable journal"))?;
        if !accepted.contains(&ordinal)
            || text("connection_generation")? != journal_source.0.to_string()
            || source.0.receive_ts_ns != journal_source.1
            || source.0.monotonic_ns != journal_source.2
            || source.2 != journal_source.3
            || text("collector_id")? != checkpoint.feed_id
        {
            return Err(fail("row runtime provenance differs from original journal"));
        }
        let session = metadata
            .get(text("metadata_ref")?)
            .ok_or_else(|| fail("row metadata ref is not a manifest parent"))?;
        let instrument_ref = text("instrument_ref")?
            .split_once(':')
            .ok_or_else(|| fail("invalid instrument ref"))?;
        let instrument = session
            .catalog
            .iter()
            .find(|i| i.instrument_id.to_string() == instrument_ref.1)
            .ok_or_else(|| fail("instrument ref absent from effective metadata"))?;
        let stream = format!("hyperliquid:{}:{}", session.venue_id, native.coin);
        let context = format!("{}:session:{}", checkpoint.feed_id, session.session_id);
        let coordinate = format!(
            "{}:{}:{}",
            text("raw_object_sha256")?,
            text("raw_record_offset")?,
            text("raw_row_index")?
        );
        let is_initial =
            initial_inputs.contains(&(source.0.session.0, source.0.frame_seq, native.coin.clone()));
        let flags = if is_initial {
            json!(["initial-delivery", "acquisition-origin-unverified"])
        } else {
            json!(["acquisition-origin-unverified"])
        };
        let acquisition = manifest["provenance"]["acquisitions"]
            .as_array()
            .and_then(|a| a.iter().find(|a| a["context_id"] == context))
            .ok_or_else(|| fail("missing row acquisition context"))?;
        if row.fields["validation_policy_ref"]
            != manifest["provenance"]["validation_policy"]["sha256"]
            || text("native_attributes_ref")? != coordinate
            || text("collector_side_projection")? != if native.side == "B" { "Buy" } else { "Sell" }
            || text("quality_interval_ref")? != stream
            || text("acquisition_context_id")? != context
            || row.fields["region"] != acquisition["region"]
            || row.fields["flags"] != flags
            || text("venue")?
                != if session.venue_id == 21 {
                    "hyperliquid-spot"
                } else {
                    "hyperliquid-perps"
                }
            || text("api_product")?
                != if session.venue_id == 21 {
                    "spot"
                } else {
                    "linear-perpetuals"
                }
            || !manifest["coverage"]
                .as_array()
                .is_some_and(|a| a.iter().any(|c| c["stream_identity"] == stream))
        {
            return Err(fail(
                "row policy, acquisition or native attribute reference mismatch",
            ));
        }
        if instrument_ref.0 != text("metadata_ref")?
            || instrument.native_symbol != native.coin
            || row.price.fixed()? != native.px.0
            || row.native_quantity.fixed()? != native.sz.0
            || text("native_symbol")? != native.coin
            || text("native_trade_id")? != native.tid.to_string()
            || text("native_side")? != native.side
            || text("native_time_value")? != native.time.to_string()
            || text("event_time_ns")?
                != native
                    .time
                    .checked_mul(1_000_000)
                    .ok_or_else(|| fail("native timestamp overflow"))?
                    .to_string()
            || text("receipt_time_ns")? != source.0.receive_ts_ns.to_string()
            || text("capture_ordinal")? != source.0.frame_seq.to_string()
            || text("session_id")? != source.0.session.0.to_string()
            || session.session_id != source.0.session.0
            || text("identity")? != format!("{}:{}", session.venue_id, native.unique_id())
        {
            return Err(fail("immutable row/raw/metadata lineage mismatch"));
        }
    }
    Ok(())
}
pub fn admit(store: &Store, pin: &str, profile: &str) -> Result<Vec<TradeRow>> {
    let manifest = store.published_manifest(pin)?;
    let rows = verify_manifest(store, &manifest)?;
    if ![
        "descriptive",
        "execution-count",
        "signed-execution-flow",
        "confirmed-candle",
        "observed-availability-replay",
    ]
    .contains(&profile)
        || manifest["quality"]["decision"] != "qualified"
        || !manifest["quality"]["supported_profiles"]
            .as_array()
            .is_some_and(|p| p.iter().any(|v| v == profile))
    {
        return Err(fail(
            "dataset is not qualified for the explicitly requested profile",
        ));
    }
    if !manifest["coverage"].as_array().is_some_and(|c| {
        !c.is_empty()
            && c.iter().all(|v| {
                matches!(
                    v["state"].as_str(),
                    Some("verified-continuity" | "recovered" | "verified-no-trades")
                )
            })
    }) {
        return Err(fail("unknown or gapped interval cannot be admitted"));
    }
    Ok(rows)
}
