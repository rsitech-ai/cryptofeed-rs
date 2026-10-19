//! A lexical financial JSON boundary without serde_json's global
//! arbitrary_precision feature. Decimal/exponent tokens remain strings; native
//! i64/u64 integers remain Numbers, preserving control IDs and array counters.
//! Callers must parse financial fields through Fixed, never f64.
use serde::Deserializer;
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Value, value::RawValue};
use std::fmt;
const MAX_INPUT: usize = 16 * 1024 * 1024;
const MAX_NODES: usize = 200_000;
struct Budget {
    nodes: usize,
    work: usize,
}
struct Container<'a> {
    budget: &'a mut Budget,
    depth: usize,
}
impl<'de> Visitor<'de> for Container<'_> {
    type Value = Value;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("JSON object or array")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut input: A) -> Result<Value, A::Error> {
        let mut values = Map::new();
        while let Some(key) = input.next_key::<String>()? {
            let raw = input.next_value::<&RawValue>()?;
            let value =
                convert(raw, self.budget, self.depth + 1).map_err(serde::de::Error::custom)?;
            if values.insert(key, value).is_some() {
                return Err(serde::de::Error::custom("duplicate JSON object key"));
            }
        }
        Ok(Value::Object(values))
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut input: A) -> Result<Value, A::Error> {
        let mut values = Vec::new();
        while let Some(raw) = input.next_element::<&RawValue>()? {
            values
                .push(convert(raw, self.budget, self.depth + 1).map_err(serde::de::Error::custom)?);
        }
        Ok(Value::Array(values))
    }
}
fn convert(raw: &RawValue, budget: &mut Budget, depth: usize) -> Result<Value, String> {
    budget.nodes += 1;
    budget.work += raw.get().len();
    if depth > 64 || budget.nodes > MAX_NODES || budget.work > 128 * 1024 * 1024 {
        return Err("exact JSON structural/work budget".into());
    }
    let token = raw.get();
    match token.as_bytes()[0] {
        b'{' | b'[' => serde_json::Deserializer::from_str(token)
            .deserialize_any(Container { budget, depth })
            .map_err(|e| e.to_string()),
        b'"' | b't' | b'f' | b'n' => serde_json::from_str(token).map_err(|e| e.to_string()),
        _ => {
            // Parsing an integral token never passes through f64. A fractional
            // or oversized integer remains its exact lexical representation.
            if let Ok(n) = token.parse::<i64>() {
                return Ok(Value::from(n));
            }
            if let Ok(n) = token.parse::<u64>() {
                return Ok(Value::from(n));
            }
            Ok(Value::String(token.to_owned()))
        }
    }
}
pub fn exact_value_from_slice(bytes: &[u8]) -> Result<Value, String> {
    if bytes.len() > MAX_INPUT {
        return Err("exact JSON input byte budget".into());
    }
    let raw: &RawValue = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    convert(raw, &mut Budget { nodes: 0, work: 0 }, 0)
}
#[cfg(test)]
mod tests {
    use super::*;
    use marketfeed_model::Fixed;
    #[test]
    fn decimal_lexemes_never_cross_float_and_control_integers_remain_exact() {
        let v = exact_value_from_slice(br#"{"id":9007199254740993,"negative":-1,"a":[12345678901234567890.123456789012345678,1.2500e-8,170141183460469231731687303715884105727],"null":null}"#).unwrap();
        assert_eq!(v["id"].as_u64(), Some(9007199254740993));
        assert_eq!(v["negative"].as_i64(), Some(-1));
        for (i, expected) in [
            Fixed::new(12345678901234567890123456789012345678, 18),
            Fixed::new(12500, 12),
            Fixed::new(i128::MAX, 0),
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(
                Fixed::parse_str(v["a"][i].as_str().unwrap()).unwrap(),
                expected
            );
        }
    }
    #[test]
    fn duplicate_keys_and_unbounded_structure_are_rejected() {
        assert!(exact_value_from_slice(br#"{"px":1,"px":2}"#).is_err());
        let nested = format!("{}0{}", "[".repeat(65), "]".repeat(65));
        assert!(exact_value_from_slice(nested.as_bytes()).is_err());
        assert!(exact_value_from_slice(b"[1e999999999999999999999]").is_ok());
        let v = exact_value_from_slice(b"[1e999999999999999999999]").unwrap();
        assert!(Fixed::parse_str(v[0].as_str().unwrap()).is_err());
    }
}
