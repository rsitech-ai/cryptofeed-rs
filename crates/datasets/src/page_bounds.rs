//! Inspect compact-Thrift page headers without allocating/decompressing pages.
//! The Parquet decoder remains responsible for format decoding. This preflight
//! restricts declared allocation sizes even when compressed input is tiny.
use crate::{Result, fail};
use parquet::file::metadata::ParquetMetaData;
const MAX_PAGE: u64 = 8 * 1024 * 1024;
const MAX_VALUES: u64 = 100_000;
struct Header<'a> {
    input: &'a [u8],
    position: usize,
    work: usize,
    byte_limit: usize,
}
impl Header<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8]> {
        let end = self
            .position
            .checked_add(n)
            .ok_or_else(|| fail("page header overflow"))?;
        if end > self.byte_limit {
            return Err(fail("page header byte budget"));
        }
        let bytes = self
            .input
            .get(self.position..end)
            .ok_or_else(|| fail("truncated page header"))?;
        self.position = end;
        Ok(bytes)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn var(&mut self) -> Result<u64> {
        let mut value = 0u64;
        for shift in (0..70).step_by(7) {
            let b = self.byte()?;
            if shift == 63 && b > 1 {
                return Err(fail("compact integer overflow"));
            }
            value |= u64::from(b & 127) << shift;
            if b & 128 == 0 {
                return Ok(value);
            }
        }
        Err(fail("compact integer overflow"))
    }
    fn integer(&mut self) -> Result<i64> {
        let n = self.var()?;
        Ok((n >> 1) as i64 ^ -((n & 1) as i64))
    }
    fn skip(&mut self, kind: u8, depth: usize, inline_bool: bool) -> Result<()> {
        self.work += 1;
        if depth > 16 || self.work > 100_000 {
            return Err(fail("page header structural budget"));
        }
        match kind {
            1 | 2 => {
                if !inline_bool {
                    self.byte()?;
                }
            }
            3 => {
                self.byte()?;
            }
            4..=6 => {
                self.var()?;
            }
            7 => {
                self.take(8)?;
            }
            8 => {
                let length = usize::try_from(self.var()?)?;
                self.take(length)?;
            }
            9 | 10 => {
                let header = self.byte()?;
                let mut count = u64::from(header >> 4);
                if count == 15 {
                    count = self.var()?;
                }
                if count > 4096 {
                    return Err(fail("page header collection budget"));
                }
                for _ in 0..count {
                    self.skip(header & 15, depth + 1, false)?;
                }
            }
            11 => {
                let count = self.var()?;
                if count > 4096 {
                    return Err(fail("page header map budget"));
                }
                if count > 0 {
                    let kinds = self.byte()?;
                    for _ in 0..count {
                        self.skip(kinds >> 4, depth + 1, false)?;
                        self.skip(kinds & 15, depth + 1, false)?;
                    }
                }
            }
            12 => loop {
                let field = self.byte()?;
                if field == 0 {
                    break;
                }
                if field >> 4 == 0 {
                    self.var()?;
                }
                self.skip(field & 15, depth + 1, true)?;
            },
            _ => return Err(fail("unsupported compact page header type")),
        }
        Ok(())
    }
    fn page_detail(&mut self, page_field: i64) -> Result<()> {
        let mut id = 0i64;
        let mut seen = std::collections::BTreeSet::new();
        loop {
            let field = self.byte()?;
            if field == 0 {
                break;
            }
            id = if field >> 4 == 0 {
                self.integer()?
            } else {
                id + i64::from(field >> 4)
            };
            if !seen.insert(id) {
                return Err(fail("duplicate page detail field"));
            }
            let is_count = id == 1 || (page_field == 8 && matches!(id, 2 | 3));
            let is_encoding =
                (matches!(page_field, 5 | 7) && id == 2) || (page_field == 8 && id == 4);
            if is_count || is_encoding {
                if field & 15 != 5 {
                    return Err(fail("invalid page count/encoding field"));
                }
                let value = u64::try_from(self.integer()?)?;
                if (is_count && value > MAX_VALUES) || (is_encoding && !matches!(value, 0 | 2 | 8))
                {
                    return Err(fail("unsupported page encoding or value allocation budget"));
                }
            } else {
                self.skip(field & 15, 0, true)?;
            }
        }
        if !seen.contains(&1) || !seen.contains(&(if page_field == 8 { 4 } else { 2 })) {
            return Err(fail("missing page value count/encoding"));
        }
        Ok(())
    }
    fn sizes(&mut self) -> Result<(u64, u64)> {
        let mut id = 0i64;
        let mut sizes = [None, None];
        loop {
            let field = self.byte()?;
            if field == 0 {
                break;
            }
            id = if field >> 4 == 0 {
                self.integer()?
            } else {
                id + i64::from(field >> 4)
            };
            if id == 2 || id == 3 {
                if field & 15 != 5 || sizes[(id - 2) as usize].is_some() {
                    return Err(fail("invalid page size field"));
                }
                let size = u64::try_from(self.integer()?)?;
                if size > MAX_PAGE {
                    return Err(fail("Parquet page allocation budget"));
                }
                sizes[(id - 2) as usize] = Some(size);
            } else if matches!(id, 5 | 7 | 8) {
                if field & 15 != 12 {
                    return Err(fail("invalid nested page header"));
                }
                self.page_detail(id)?;
            } else {
                self.skip(field & 15, 0, true)?;
            }
        }
        Ok((
            sizes[0].ok_or_else(|| fail("missing uncompressed page size"))?,
            sizes[1].ok_or_else(|| fail("missing compressed page size"))?,
        ))
    }
}
/// Bound every footer collection/string/depth before Parquet allocates metadata
/// from untrusted declared counts. Only bounded plain/dictionary data pages are
/// accepted; delta encodings have additional internal allocation counts.
pub(crate) fn footer(bytes: &[u8]) -> Result<()> {
    if bytes.len() < 12 || &bytes[..4] != b"PAR1" || &bytes[bytes.len() - 4..] != b"PAR1" {
        return Err(fail("invalid Parquet magic"));
    }
    let length = u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into()?) as usize;
    if length > 1024 * 1024 || length > bytes.len() - 12 {
        return Err(fail("Parquet footer byte budget"));
    }
    let input = &bytes[bytes.len() - 8 - length..bytes.len() - 8];
    let mut header = Header {
        input,
        position: 0,
        work: 0,
        byte_limit: 1024 * 1024,
    };
    header.skip(12, 0, true)?;
    if header.position != length {
        return Err(fail("trailing Parquet footer bytes"));
    }
    Ok(())
}
pub(crate) fn check(bytes: &[u8], metadata: &ParquetMetaData) -> Result<()> {
    let mut total = 0u64;
    let mut page_count = 0usize;
    for group in metadata.row_groups() {
        for column in group.columns() {
            let (start, length) = column.byte_range();
            let end = start
                .checked_add(length)
                .ok_or_else(|| fail("column range overflow"))?;
            let chunk = bytes
                .get(usize::try_from(start)?..usize::try_from(end)?)
                .ok_or_else(|| fail("column outside file"))?;
            let mut position = 0usize;
            while position < chunk.len() {
                page_count += 1;
                if page_count > 1_000_000 {
                    return Err(fail("Parquet page count budget"));
                }
                let mut header = Header {
                    input: &chunk[position..],
                    position: 0,
                    work: 0,
                    byte_limit: 64 * 1024,
                };
                let (uncompressed, compressed) = header.sizes()?;
                total = total
                    .checked_add(uncompressed)
                    .ok_or_else(|| fail("page byte overflow"))?;
                if total > 256 * 1024 * 1024 {
                    return Err(fail("aggregate page allocation budget"));
                }
                position = position
                    .checked_add(header.position)
                    .and_then(|p| p.checked_add(compressed as usize))
                    .ok_or_else(|| fail("page range overflow"))?;
                if position > chunk.len() {
                    return Err(fail("page exceeds column chunk"));
                }
            }
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn malicious_footer_count_is_rejected_before_library_allocation() {
        // FileMetaData field4=row_groups LIST<STRUCT> count=2^28.
        let footer = [0x49, 0xfc, 0x80, 0x80, 0x80, 0x80, 1, 0];
        let mut bytes = b"PAR1".to_vec();
        bytes.extend(footer);
        bytes.extend((footer.len() as u32).to_le_bytes());
        bytes.extend(b"PAR1");
        assert!(
            super::footer(&bytes)
                .unwrap_err()
                .to_string()
                .contains("collection budget")
        );
    }
    #[test]
    fn dictionary_count_and_delta_encoding_are_rejected_without_decoding() {
        for bytes in [
            // DictionaryPageHeader num_values=2^28, encoding=PLAIN.
            vec![0x15, 0x80, 0x80, 0x80, 0x80, 2, 0x15, 0, 0],
            // DataPageHeader num_values=1, encoding=DELTA_BYTE_ARRAY(7).
            vec![0x15, 2, 0x15, 14, 0],
        ] {
            assert!(
                Header {
                    input: &bytes,
                    position: 0,
                    work: 0,
                    byte_limit: 65536
                }
                .page_detail(7)
                .is_err()
            );
        }
    }
    #[test]
    fn oversized_decompression_is_rejected_from_its_header() {
        // field1 type=DATA_PAGE, field2 uncompressed=2^30, field3 compressed=1.
        let bytes = [0x15, 0, 0x15, 0x80, 0x80, 0x80, 0x80, 8, 0x15, 2, 0];
        assert!(
            Header {
                input: &bytes,
                position: 0,
                work: 0,
                byte_limit: 64 * 1024
            }
            .sizes()
            .is_err()
        );
        let normal = [0x15, 0, 0x15, 20, 0x15, 2, 0];
        assert_eq!(
            Header {
                input: &normal,
                position: 0,
                work: 0,
                byte_limit: 64 * 1024
            }
            .sizes()
            .unwrap(),
            (10, 1)
        );
    }
}
