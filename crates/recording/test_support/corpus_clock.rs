/// Historical fixtures stored wall time in the outbound monotonic field.
/// Preserve those immutable bytes and compare the corrected unavailable stamp
/// explicitly. All other bytes (including original payloads/CRCs) remain exact.
fn normalized_outbound_clock(bytes: &[u8]) -> Vec<u8> {
    let mut out = bytes.to_vec();
    let records = marketfeed_recording::RawSegmentReader::from_bytes(bytes.to_vec())
        .unwrap().read_all().unwrap();
    let mut offset = marketfeed_recording::HEADER_SIZE;
    for record in records {
        if record.header.direction == marketfeed_recording::Direction::Outbound {
            let mono = record.header.monotonic_ns;
            assert!(mono == 0 || mono == record.header.receive_ts_ns as u64,
                "unexpected outbound clock drift");
            out[offset + 28..offset + 36].fill(0);
        }
        offset += record.header.record_len as usize;
    }
    assert_eq!(offset, out.len());
    out
}
