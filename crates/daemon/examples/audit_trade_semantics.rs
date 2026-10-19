//! Independent synthetic challenges: emits measured verdicts, not qualification.
use marketfeed_adapter_kraken::DecodedEvent as KrakenEvent;
use marketfeed_adapter_okx::DecodedEvent as OkxEvent;
use marketfeed_model::Fixed;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let okx = br#"{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"9007199254740993","px":"1.000000000000000001","sz":"0.1","side":"buy","ts":"1000","count":"2"},{"instId":"BTC-USDT","tradeId":"9007199254740994","px":"1.000000000000000001","sz":"0.1","side":"sell","ts":"1000","count":"1"}]}"#;
    let result = marketfeed_adapter_okx::decode_text(okx)?;
    let emitted_rows = match &result {
        OkxEvent::Trade { .. } => 1,
        OkxEvent::Batch(rows) => rows.len(),
        _ => 0,
    };
    let kraken = br#"{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","trade_id":9007199254740993,"price":0.1234567890123456789012345678,"qty":0.1,"side":"buy","ord_type":"limit","timestamp":"2026-10-19T00:00:00.123456789Z"}]}"#;
    let KrakenEvent::Trades(trades) = marketfeed_adapter_kraken::decode_text(kraken)? else {
        return Err("expected trade challenge to decode".into());
    };
    let expected = Fixed::parse_str("0.1234567890123456789012345678")?;
    println!(
        "{}",
        serde_json::json!({
            "provenance": "synthetic independently written challenge; not live source evidence",
            "okx": {"native_rows": 2, "emitted_rows": emitted_rows, "all_rows_preserved": emitted_rows == 2,
                "decoded": format!("{result:?}")},
            "kraken": {"expected_price": format!("{expected:?}"), "decoded_price": format!("{:?}", trades[0].price.0),
                "exact": trades[0].price.0 == expected}
        })
    );
    Ok(())
}
