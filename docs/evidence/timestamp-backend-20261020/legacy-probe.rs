use marketfeed_event_pulse::wire::Rfc3339Time;
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

fn main() {
    let source = std::fs::read(std::env::args().nth(1).unwrap()).unwrap();
    let input: Value = serde_json::from_slice(&source).unwrap();
    let parsed: Vec<_> = input["parse"].as_array().unwrap().iter().map(|v| {
        let text = v.as_str().unwrap();
        match Rfc3339Time::parse(text) {
            Err(_) => json!({"input":text,"accepted":false}),
            Ok(value) => {
                let utc = OffsetDateTime::from_unix_timestamp_nanos(i128::from(value.utc_micros())*1000)
                    .ok().and_then(|t|t.format(&Rfc3339).ok())
                    .and_then(|t|Rfc3339Time::parse(&t).ok())
                    .map(|t|t.canonical().to_owned());
                json!({"input":text,"accepted":true,"canonical":value.canonical(),"utc_micros":value.utc_micros(),"utc_canonical":utc})
            }
        }
    }).collect();
    let nanos: Vec<_> = input["nanos"].as_array().unwrap().iter().map(|v| {
        let n=v.as_i64().unwrap();let t=Rfc3339Time::from_unix_nanos(n).unwrap();
        json!({"input":n,"canonical":t.canonical(),"utc_micros":t.utc_micros(),"export_utc":OffsetDateTime::from_unix_timestamp_nanos(i128::from(n)).unwrap().format(&Rfc3339).unwrap()})
    }).collect();
    println!("{}",json!({"parse":parsed,"nanos":nanos}));
}
