//! Exact native leaves. The surrounding tagged DTO only buffers a JSON string;
//! its financial contents are parsed directly into typed structs afterwards.
use super::*;
#[cfg(test)]
#[path = "causal_observation_tests.rs"]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ObservedBatchJson(String);
impl ObservedBatchJson {
    pub(super) fn new(batch: &ObservedEventBatch) -> Result<Self, CodecError> {
        batch
            .encoded_len(ObservedBatchLimits::HARD_MAX.encoded_bytes)
            .map_err(|e| invalid(&e.to_string()))?;
        Ok(Self(serde_json::to_string(batch)?))
    }
    pub fn batch(&self) -> Result<ObservedEventBatch, CodecError> {
        if self.0.len() > ObservedBatchLimits::HARD_MAX.encoded_bytes {
            return Err(CodecError::TooLarge);
        }
        Ok(serde_json::from_str(&self.0)?)
    }
    fn validate(&self) -> Result<(), CodecError> {
        let batch = self.batch()?;
        let mut check = Canonical {
            expected: self.0.as_bytes(),
            offset: 0,
        };
        serde_json::to_writer(&mut check, &batch)?;
        if check.offset != self.0.len() {
            return Err(invalid("noncanonical native observation leaf"));
        }
        Ok(())
    }
}
impl<'de> Deserialize<'de> for ObservedBatchJson {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let leaf = Self(String::deserialize(d)?);
        leaf.validate().map_err(serde::de::Error::custom)?;
        Ok(leaf)
    }
}

impl OperationDto {
    pub(super) fn validate_observation_policy(
        &self,
        policy: Option<NativeObservationPolicyDto>,
    ) -> Result<(), CodecError> {
        let mut check =
            |leaf: &ObservedBatchJson| {
                let Some(NativeObservationPolicyDto::HyperliquidV1 { limits }) = policy else {
                    return Err(invalid(
                        "native observations not admitted by bootstrap policy",
                    ));
                };
                let batch = leaf.batch()?;
                if batch.limits() != limits
                    || batch.native().rows.iter().any(|row| {
                        matches!(row.observation, NativeObservation::HyperliquidAllMids(_))
                    })
                {
                    return Err(invalid(
                        "native limits or family differ from bootstrap policy",
                    ));
                }
                Ok(())
            };
        let check_item =
            |item: &ItemDto,
             check: &mut dyn FnMut(&ObservedBatchJson) -> Result<(), CodecError>| {
                if let ItemDto::ObservedMarket(leaf) = item {
                    check(leaf)?;
                }
                Ok::<_, CodecError>(())
            };
        for (_, items) in self.before.queues.iter().chain(&self.after.queues) {
            for item in items {
                check_item(item, &mut check)?;
            }
        }
        for entry in &self.entries {
            match &entry.effect {
                EffectDto::Invocation { actions, .. } => {
                    for action in actions {
                        if let ActionDto::EmitObservedBatch(leaf) = &action.action {
                            check(leaf)?;
                        }
                    }
                }
                EffectDto::QueueAttempt {
                    incoming,
                    evicted_front,
                    ..
                } => {
                    check_item(incoming, &mut check)?;
                    if let Some(item) = evicted_front {
                        check_item(item, &mut check)?;
                    }
                }
                EffectDto::QueueDrain { items, .. } => {
                    for item in items {
                        check_item(item, &mut check)?;
                    }
                }
                EffectDto::SinkAttempt { incoming, .. } => check_item(incoming, &mut check)?,
                _ => {}
            }
        }
        Ok(())
    }
    pub(super) fn has_native_observations(&self) -> bool {
        let native_item = |item: &ItemDto| matches!(item, ItemDto::ObservedMarket(_));
        self.before
            .queues
            .iter()
            .chain(&self.after.queues)
            .any(|(_, items)| items.iter().any(native_item))
            || self.entries.iter().any(|entry| match &entry.effect {
                EffectDto::Invocation { actions, .. } => actions
                    .iter()
                    .any(|a| matches!(a.action, ActionDto::EmitObservedBatch(_))),
                EffectDto::QueueAttempt {
                    incoming,
                    evicted_front,
                    ..
                } => native_item(incoming) || evicted_front.as_ref().is_some_and(native_item),
                EffectDto::QueueDrain { items, .. } => items.iter().any(native_item),
                EffectDto::SinkAttempt { incoming, .. } => native_item(incoming),
                _ => false,
            })
    }
}
