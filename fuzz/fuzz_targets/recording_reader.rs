//! Fuzz `RawSegmentReader` for no-panic + error-or-ok only.
#![no_main]

use libfuzzer_sys::fuzz_target;
use marketfeed_recording::RawSegmentReader;

fuzz_target!(|data: &[u8]| {
    let Ok(mut reader) = RawSegmentReader::from_bytes(data.to_vec()) else {
        return;
    };
    let _ = reader.read_all();
});
