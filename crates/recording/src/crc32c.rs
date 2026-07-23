//! Castagnoli CRC-32C (spec §18.3). No extra dependency.

const POLY: u32 = 0x82F63B78; // reflected Castagnoli

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

/// CRC-32C of `data` (init 0xFFFF_FFFF, final XOR 0xFFFF_FFFF).
pub fn crc32c(data: &[u8]) -> u32 {
    // ponytail: build table each call; ceiling = tiny CPU on large files; upgrade = OnceLock table.
    let table = table();
    let mut crc = 0xFFFF_FFFF;
    for &b in data {
        let idx = ((crc ^ u32::from(b)) & 0xFF) as usize;
        crc = table[idx] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vector_empty_and_abc() {
        assert_eq!(crc32c(b""), 0);
        // Castagnoli("123456789") == 0xE3069283
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }
}
