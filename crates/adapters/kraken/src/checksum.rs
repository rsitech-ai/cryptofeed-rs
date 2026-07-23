//! Kraken WS v2 book checksum: IEEE CRC32 over stripped top-10 ask+bid strings.
//!
//! Spec: <https://docs.kraken.com/exchange/guides/websockets/book-checksum-v2>.
//! Not the CRC-32C used by `marketfeed-recording` (different polynomial) — a
//! tiny local IEEE table is implemented here rather than pulling a crate.

const POLY: u32 = 0xEDB8_8320; // reflected IEEE 802.3

fn table() -> [u32; 256] {
    let mut table = [0u32; 256];
    for (i, slot) in table.iter_mut().enumerate() {
        let mut crc = i as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ POLY;
            } else {
                crc >>= 1;
            }
        }
        *slot = crc;
    }
    table
}

/// IEEE CRC-32 of `data` (init 0xFFFF_FFFF, final XOR 0xFFFF_FFFF).
pub fn crc32_ieee(data: &[u8]) -> u32 {
    // ponytail: build table each call; ceiling = tiny CPU per checksum; upgrade = OnceLock table.
    let table = table();
    let mut crc = 0xFFFF_FFFF;
    for &b in data {
        let idx = ((crc ^ u32::from(b)) & 0xFF) as usize;
        crc = table[idx] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

/// Strip `.` and leading zeros from a wire price/qty string per Kraken's checksum rule.
/// An all-zero result collapses to `"0"` (never observed for real levels, but keeps
/// the function total).
pub fn strip_price_or_qty(s: &str) -> String {
    let no_dot: String = s.chars().filter(|&c| c != '.').collect();
    let stripped = no_dot.trim_start_matches('0');
    if stripped.is_empty() {
        "0".into()
    } else {
        stripped.into()
    }
}

/// Build the checksum input string: asks (low→high) then bids (high→low), each
/// level as `strip(price) + strip(qty)`, and hash with [`crc32_ieee`].
pub fn book_checksum<'a>(
    asks_low_to_high: impl Iterator<Item = (&'a str, &'a str)>,
    bids_high_to_low: impl Iterator<Item = (&'a str, &'a str)>,
) -> u32 {
    let mut buf = String::new();
    for (price, qty) in asks_low_to_high {
        buf.push_str(&strip_price_or_qty(price));
        buf.push_str(&strip_price_or_qty(qty));
    }
    for (price, qty) in bids_high_to_low {
        buf.push_str(&strip_price_or_qty(price));
        buf.push_str(&strip_price_or_qty(qty));
    }
    crc32_ieee(buf.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_matches_docs_example() {
        assert_eq!(strip_price_or_qty("45285.2"), "452852");
        assert_eq!(strip_price_or_qty("0.00100000"), "100000");
    }

    #[test]
    fn crc32_ieee_known_vector() {
        // Standard CRC-32/ISO-HDLC check value for "123456789".
        assert_eq!(crc32_ieee(b""), 0);
        assert_eq!(crc32_ieee(b"123456789"), 0xCBF4_3926);
    }

    /// Golden vector from Kraken's book-checksum-v2 guide (BTC/USD snapshot example).
    #[test]
    fn golden_kraken_checksum_example() {
        let asks = [
            ("45285.2", "0.00100000"),
            ("45286.4", "1.54571953"),
            ("45286.6", "1.54571109"),
            ("45289.6", "1.54560911"),
            ("45290.2", "0.15890660"),
            ("45291.8", "1.54553491"),
            ("45294.7", "0.04454749"),
            ("45296.1", "0.35380000"),
            ("45297.5", "0.09945542"),
            ("45299.5", "0.18772827"),
        ];
        let bids = [
            ("45283.5", "0.10000000"),
            ("45283.4", "1.54582015"),
            ("45282.1", "0.10000000"),
            ("45281.0", "0.10000000"),
            ("45280.3", "1.54592586"),
            ("45279.0", "0.07990000"),
            ("45277.6", "0.03310103"),
            ("45277.5", "0.30000000"),
            ("45277.3", "1.54602737"),
            ("45276.6", "0.15445238"),
        ];
        let cs = book_checksum(
            asks.iter().map(|&(p, q)| (p, q)),
            bids.iter().map(|&(p, q)| (p, q)),
        );
        assert_eq!(cs, 3_310_070_434);
    }
}
