//! Bounded audit tool, not a production dataset reader or full engine replay.
//! Demultiplexes OKX MFR1 sessions using embedded reproduction metadata.
use std::collections::HashMap;
use std::error::Error;
use std::io::Write;

use marketfeed_adapter_api::{ConcreteSubscriptionSet, SessionSpec};
use marketfeed_adapter_okx::{OkxSession, OkxSessionConfig};
use marketfeed_model::{
    CatalogVersion, CatalogView, InstrumentId, SessionId, TimestampNs, VenueId,
};
use marketfeed_recording::{
    FrameOpcode, MetadataRecord, RawSegmentReader, decode_metadata, event_envelope_json,
};
use marketfeed_replay::ReplayRunner;

fn main() -> Result<(), Box<dyn Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: audit_okx_replay segment.mfr1")?;
    if std::fs::metadata(&path)?.len() > 128 * 1024 * 1024 {
        return Err("audit input exceeds 128 MiB".into());
    }
    let records = RawSegmentReader::from_bytes(std::fs::read(path)?)?.read_all()?;
    let mut metadata = HashMap::new();
    for record in &records {
        if record.header.opcode == FrameOpcode::Metadata
            && let MetadataRecord::Session(session) = decode_metadata(&record.payload)?
            && session.adapter == "okx"
            && let Some(previous) = metadata.insert(session.session_id, session.clone())
            && previous != session
        {
            return Err("conflicting session metadata".into());
        }
    }
    let mut sessions: Vec<_> = metadata.into_values().collect();
    sessions.sort_by_key(|session| session.session_id);
    let mut output = std::io::BufWriter::new(std::io::stdout().lock());
    for session in sessions {
        let selected: Vec<_> = records
            .iter()
            .filter(|record| record.header.session.0 == session.session_id)
            .cloned()
            .collect();
        if selected.len() > 100_000 {
            return Err("audit exceeds 100000 records per session".into());
        }
        let cfg = OkxSessionConfig {
            venue: VenueId(session.venue_id),
            session: SessionId(session.session_id),
            symbols: session
                .catalog
                .iter()
                .map(|item| item.native_symbol.clone())
                .collect(),
            instrument_ids: session
                .catalog
                .iter()
                .map(|item| (item.native_symbol.clone(), InstrumentId(item.instrument_id)))
                .collect(),
            enable_l2: false,
            ..OkxSessionConfig::default()
        };
        let mut machine = OkxSession::new(
            SessionSpec {
                endpoint_name: session.endpoint,
                subscriptions: ConcreteSubscriptionSet::default(),
            },
            CatalogView::new(
                VenueId(session.venue_id),
                CatalogVersion(session.catalog_version),
            ),
            cfg,
        );
        // Bounded fixture-style runner retains output; not scalable streaming replay.
        let mut replay = ReplayRunner::new(100_001);
        let outcome = replay.replay_records(&mut machine, &selected, TimestampNs(0))?;
        if outcome.dropped_events != 0 {
            return Err("replay reported drops".into());
        }
        for batch in outcome.market_batches {
            for event in batch.events {
                serde_json::to_writer(&mut output, &event_envelope_json(&event))?;
                output.write_all(b"\n")?;
            }
        }
    }
    output.flush()?;
    Ok(())
}
