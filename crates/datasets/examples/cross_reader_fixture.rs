use marketfeed_datasets::{
    Result,
    parquet::{self, Exact, TradeRow},
};
use marketfeed_model::Fixed;
use std::{fs::OpenOptions, io::Write};
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 {
        return Err("usage: cross_reader_fixture write|read PATH".into());
    }
    let rows = if args[1] == "write" {
        let rows: Vec<_>=[i128::MIN,i128::MAX,-9007199254740993,9007199254740993,0].into_iter().flat_map(|coefficient| [0,18,38,255].into_iter().map(move |scale| TradeRow {
            price:Exact::from(Fixed::new(coefficient,scale)),native_quantity:Exact::from(Fixed::new(coefficient,scale)),
            fields:serde_json::json!({"event_time_ns":i64::MIN.to_string(),"receipt_time_ns":i64::MAX.to_string(),"native_symbol":"xyz:TSLA","native_trade_id":"9007199254740993"}),
        })).collect();
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&args[2])?;
        file.write_all(&parquet::write(&rows)?)?;
        file.sync_all()?;
        rows
    } else if args[1] == "read" {
        parquet::read(std::fs::read(&args[2])?)?
    } else {
        return Err("unknown fixture operation".into());
    };
    println!("{}", serde_json::to_string(&rows)?);
    Ok(())
}
