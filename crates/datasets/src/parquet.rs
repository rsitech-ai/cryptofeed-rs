//! Financial coefficients are signed big-endian 16-byte integers, NOT decimal128
//! (which cannot represent every i128 value). Scale is an independent uint8.
//! The record JSON contains lexical/string coefficients for every additional
//! field and is cross-checked against the typed columns on read.
use crate::{Result, fail};
use marketfeed_model::Fixed;
use parquet::{
    basic::{Compression, ZstdLevel},
    data_type::{
        ByteArray, ByteArrayType, FixedLenByteArray, FixedLenByteArrayType, Int32Type, Int64Type,
    },
    file::{
        properties::WriterProperties,
        reader::{FileReader, SerializedFileReader},
        writer::SerializedFileWriter,
    },
    record::RowAccessor,
    schema::{parser::parse_message_type, types::SchemaDescriptor},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{io::Cursor, sync::Arc};
pub const SCHEMA: &str = r#"message marketfeed_exact_trade_v1 {
 REQUIRED GROUP price { REQUIRED FIXED_LEN_BYTE_ARRAY(16) coefficient; REQUIRED INT32 scale (INTEGER(8,false)); }
 REQUIRED GROUP native_quantity { REQUIRED FIXED_LEN_BYTE_ARRAY(16) coefficient; REQUIRED INT32 scale (INTEGER(8,false)); }
 REQUIRED INT64 event_time_ns;
 REQUIRED INT64 receipt_time_ns;
 REQUIRED BINARY native_symbol (UTF8);
 REQUIRED BINARY native_trade_id (UTF8);
 REQUIRED BINARY record (UTF8);
}"#;
pub const MAX_ROWS: usize = 100_000;
const MAX_DECODED_BYTES: usize = 128 * 1024 * 1024;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Exact {
    pub coefficient: String,
    pub scale: u8,
}
impl From<Fixed> for Exact {
    fn from(f: Fixed) -> Self {
        Self {
            coefficient: f.coefficient.to_string(),
            scale: f.scale,
        }
    }
}
impl Exact {
    pub fn fixed(&self) -> Result<Fixed> {
        let coefficient: i128 = self.coefficient.parse()?;
        if coefficient.to_string() != self.coefficient {
            return Err(fail("noncanonical coefficient"));
        }
        Ok(Fixed::new(coefficient, self.scale))
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeRow {
    pub price: Exact,
    pub native_quantity: Exact,
    /// Row semantics and provenance. JSON integer-like values are strings;
    /// the schema object bound by the manifest defines the complete contract.
    pub fields: Value,
}
/// Bounded chunk writer: caller partitions before this offline conversion.
pub fn write(rows: &[TradeRow]) -> Result<Vec<u8>> {
    if rows.len() > MAX_ROWS {
        return Err(fail("Parquet row budget exceeded"));
    }
    let mut total = 0usize;
    for row in rows {
        let length = serde_json::to_vec(row)?.len();
        total = total
            .checked_add(length)
            .ok_or_else(|| fail("row byte overflow"))?;
        if length > 64 * 1024 || total > MAX_DECODED_BYTES {
            return Err(fail("Parquet row/decoded byte budget"));
        }
    }
    let schema = Arc::new(parse_message_type(SCHEMA)?);
    let props = Arc::new(
        WriterProperties::builder()
            .set_write_batch_size(64)
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .build(),
    );
    let mut output = Cursor::new(Vec::new());
    {
        let mut writer = SerializedFileWriter::new(&mut output, schema, props)?;
        for chunk in rows.chunks(8192) {
            let mut group = writer.next_row_group()?;
            for quantity in [false, true] {
                let exact: Vec<_> = chunk
                    .iter()
                    .map(|r| {
                        if quantity {
                            &r.native_quantity
                        } else {
                            &r.price
                        }
                    })
                    .collect();
                let coefficients: Vec<_> = exact
                    .iter()
                    .map(|v| {
                        Ok(FixedLenByteArray::from(
                            v.fixed()?.coefficient.to_be_bytes().to_vec(),
                        ))
                    })
                    .collect::<Result<_>>()?;
                let mut col = group
                    .next_column()?
                    .ok_or_else(|| fail("missing coefficient column"))?;
                col.typed::<FixedLenByteArrayType>()
                    .write_batch(&coefficients, None, None)?;
                col.close()?;
                let scales: Vec<_> = exact.iter().map(|v| i32::from(v.scale)).collect();
                let mut col = group
                    .next_column()?
                    .ok_or_else(|| fail("missing scale column"))?;
                col.typed::<Int32Type>().write_batch(&scales, None, None)?;
                col.close()?;
            }
            for key in ["event_time_ns", "receipt_time_ns"] {
                let values: Vec<i64> = chunk
                    .iter()
                    .map(|r| {
                        r.fields[key]
                            .as_str()
                            .ok_or_else(|| fail("missing typed timestamp"))
                            .and_then(|s| Ok(s.parse()?))
                    })
                    .collect::<Result<_>>()?;
                let mut col = group
                    .next_column()?
                    .ok_or_else(|| fail("missing timestamp column"))?;
                col.typed::<Int64Type>().write_batch(&values, None, None)?;
                col.close()?;
            }
            for key in ["native_symbol", "native_trade_id"] {
                let values: Vec<_> = chunk
                    .iter()
                    .map(|r| {
                        r.fields[key]
                            .as_str()
                            .map(ByteArray::from)
                            .ok_or_else(|| fail("missing typed identity"))
                    })
                    .collect::<Result<_>>()?;
                let mut col = group
                    .next_column()?
                    .ok_or_else(|| fail("missing identity column"))?;
                col.typed::<ByteArrayType>()
                    .write_batch(&values, None, None)?;
                col.close()?;
            }
            let records: Vec<_> = chunk
                .iter()
                .map(|r| Ok(ByteArray::from(serde_json::to_vec(r)?)))
                .collect::<Result<_>>()?;
            let mut col = group
                .next_column()?
                .ok_or_else(|| fail("missing record column"))?;
            col.typed::<ByteArrayType>()
                .write_batch(&records, None, None)?;
            col.close()?;
            group.close()?;
        }
        writer.close()?;
    }
    Ok(output.into_inner())
}
pub fn read(bytes: Vec<u8>) -> Result<Vec<TradeRow>> {
    if bytes.len() > 512 * 1024 * 1024 {
        return Err(fail("Parquet byte budget exceeded"));
    }
    crate::page_bounds::footer(&bytes)?;
    let input = bytes::Bytes::from(bytes);
    let reader = SerializedFileReader::new(input.clone())?;
    crate::page_bounds::check(&input, reader.metadata())?;
    let expected = SchemaDescriptor::new(Arc::new(parse_message_type(SCHEMA)?));
    let actual = reader.metadata().file_metadata().schema_descr();
    if actual.num_columns() != expected.num_columns()
        || actual
            .columns()
            .iter()
            .zip(expected.columns())
            .any(|(a, e)| {
                a.physical_type() != e.physical_type()
                    || a.type_length() != e.type_length()
                    || a.converted_type() != e.converted_type()
                    || a.max_def_level() != e.max_def_level()
                    || a.max_rep_level() != e.max_rep_level()
                    || a.path() != e.path()
                    || (a.converted_type() == parquet::basic::ConvertedType::NONE
                        && a.logical_type_ref() != e.logical_type_ref())
            })
    {
        return Err(fail("unexpected exact Parquet schema"));
    }
    if reader.metadata().file_metadata().num_rows() < 0
        || reader.metadata().file_metadata().num_rows() as u64 > MAX_ROWS as u64
    {
        return Err(fail("Parquet row budget exceeded"));
    }
    let mut rows = Vec::new();
    let mut decoded_bytes = 0usize;
    let mut uncompressed = 0u64;
    for group in reader.metadata().row_groups() {
        let length = u64::try_from(group.total_byte_size())?;
        uncompressed = uncompressed
            .checked_add(length)
            .ok_or_else(|| fail("Parquet uncompressed byte overflow"))?;
        if uncompressed > 256 * 1024 * 1024 {
            return Err(fail("Parquet uncompressed byte budget"));
        }
    }
    for row in reader.get_row_iter(None)? {
        let row = row?;
        let record = row.get_string(6)?;
        if record.len() > 64 * 1024 {
            return Err(fail("trade row too large"));
        }
        decoded_bytes = decoded_bytes
            .checked_add(record.len())
            .ok_or_else(|| fail("decoded byte overflow"))?;
        if decoded_bytes > MAX_DECODED_BYTES {
            return Err(fail("decoded row byte budget"));
        }
        let trade: TradeRow = serde_json::from_str(record)?;
        for (index, value) in [&trade.price, &trade.native_quantity]
            .into_iter()
            .enumerate()
        {
            let group = row.get_group(index)?;
            let bytes: [u8; 16] = group.get_bytes(0)?.data().try_into()?;
            let scale = group.get_ubyte(1)?;
            if value.fixed()? != Fixed::new(i128::from_be_bytes(bytes), scale) {
                return Err(fail("Parquet typed/record decimal mismatch"));
            }
        }
        for (index, key) in ["event_time_ns", "receipt_time_ns"].into_iter().enumerate() {
            if trade.fields[key].as_str() != Some(row.get_long(index + 2)?.to_string().as_str()) {
                return Err(fail("Parquet typed/record timestamp mismatch"));
            }
        }
        for (index, key) in ["native_symbol", "native_trade_id"].into_iter().enumerate() {
            if trade.fields[key].as_str() != Some(row.get_string(index + 4)?) {
                return Err(fail("Parquet typed/record identity mismatch"));
            }
        }
        rows.push(trade);
    }
    Ok(rows)
}
