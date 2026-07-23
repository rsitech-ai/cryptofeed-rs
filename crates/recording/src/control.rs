//! Bounded payload for accepted dynamic-subscription mutations in MFR1 v3.

use marketfeed_adapter_api::{SessionCommand, SubscriptionWireAction};
use serde::{Deserialize, Serialize};

use crate::RecordingError;

const MAX_CONTROL_BYTES: usize = 1024 * 1024;
const MAX_CONTROL_SYMBOLS: usize = 4096;
const MAX_CONTROL_SYMBOL_BYTES: usize = 1024;
const MAX_CONTROL_WIRE_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordedSubscriptionMutation {
    command: RecordedSubscriptionCommand,
    wire: RecordedWireAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "symbols", rename_all = "snake_case")]
enum RecordedSubscriptionCommand {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
    Replace(Vec<String>),
}

impl RecordedSubscriptionCommand {
    fn from_command(command: &SessionCommand) -> Result<Self, RecordingError> {
        match command {
            SessionCommand::Subscribe(symbols) => {
                validate_symbols(symbols)?;
                Ok(Self::Subscribe(symbols.clone()))
            }
            SessionCommand::Unsubscribe(symbols) => {
                validate_symbols(symbols)?;
                Ok(Self::Unsubscribe(symbols.clone()))
            }
            SessionCommand::Replace(symbols) => {
                validate_symbols(symbols)?;
                Ok(Self::Replace(symbols.clone()))
            }
            SessionCommand::Resync(_) | SessionCommand::Stop => {
                Err(RecordingError::InvalidControlCommand(
                    "only dynamic subscription commands are recordable".into(),
                ))
            }
        }
    }

    fn into_command(self) -> SessionCommand {
        match self {
            Self::Subscribe(symbols) => SessionCommand::Subscribe(symbols),
            Self::Unsubscribe(symbols) => SessionCommand::Unsubscribe(symbols),
            Self::Replace(symbols) => SessionCommand::Replace(symbols),
        }
    }

    fn symbols(&self) -> &[String] {
        match self {
            Self::Subscribe(symbols) | Self::Unsubscribe(symbols) | Self::Replace(symbols) => {
                symbols
            }
        }
    }

    fn validate(&self) -> Result<(), RecordingError> {
        validate_symbols(self.symbols())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "opcode", content = "payload", rename_all = "snake_case")]
enum RecordedWireAction {
    Text(Vec<u8>),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
}

impl RecordedWireAction {
    fn from_wire(wire: &SubscriptionWireAction) -> Result<Self, RecordingError> {
        let payload = match wire {
            SubscriptionWireAction::Text(payload)
            | SubscriptionWireAction::Binary(payload)
            | SubscriptionWireAction::Ping(payload) => payload,
        };
        if payload.len() > MAX_CONTROL_WIRE_BYTES {
            return Err(RecordingError::InvalidControlCommand(
                "subscription wire payload exceeds limit".into(),
            ));
        }
        Ok(match wire {
            SubscriptionWireAction::Text(payload) => Self::Text(payload.to_vec()),
            SubscriptionWireAction::Binary(payload) => Self::Binary(payload.to_vec()),
            SubscriptionWireAction::Ping(payload) => Self::Ping(payload.to_vec()),
        })
    }

    fn into_wire(self) -> SubscriptionWireAction {
        match self {
            Self::Text(payload) => SubscriptionWireAction::Text(payload.into()),
            Self::Binary(payload) => SubscriptionWireAction::Binary(payload.into()),
            Self::Ping(payload) => SubscriptionWireAction::Ping(payload.into()),
        }
    }

    fn validate(&self) -> Result<(), RecordingError> {
        let payload = match self {
            Self::Text(payload) | Self::Binary(payload) | Self::Ping(payload) => payload,
        };
        if payload.len() > MAX_CONTROL_WIRE_BYTES {
            return Err(RecordingError::InvalidControlCommand(
                "subscription wire payload exceeds limit".into(),
            ));
        }
        Ok(())
    }
}

fn validate_symbols(symbols: &[String]) -> Result<(), RecordingError> {
    if symbols.len() > MAX_CONTROL_SYMBOLS {
        return Err(RecordingError::InvalidControlCommand(
            "subscription symbol count exceeds limit".into(),
        ));
    }
    if symbols
        .iter()
        .any(|symbol| symbol.is_empty() || symbol.len() > MAX_CONTROL_SYMBOL_BYTES)
    {
        return Err(RecordingError::InvalidControlCommand(
            "subscription symbol length is invalid".into(),
        ));
    }
    Ok(())
}

pub fn encode_subscription_command(
    command: &SessionCommand,
    wire: &SubscriptionWireAction,
) -> Result<Vec<u8>, RecordingError> {
    let recorded = RecordedSubscriptionMutation {
        command: RecordedSubscriptionCommand::from_command(command)?,
        wire: RecordedWireAction::from_wire(wire)?,
    };
    let payload = serde_json::to_vec(&recorded).map_err(|_| {
        RecordingError::InvalidControlCommand("subscription command encoding failed".into())
    })?;
    if payload.len() > MAX_CONTROL_BYTES {
        return Err(RecordingError::InvalidControlCommand(
            "subscription command payload exceeds limit".into(),
        ));
    }
    Ok(payload)
}

pub fn decode_subscription_command(
    payload: &[u8],
) -> Result<(SessionCommand, SubscriptionWireAction), RecordingError> {
    if payload.len() > MAX_CONTROL_BYTES {
        return Err(RecordingError::InvalidControlCommand(
            "subscription command payload exceeds limit".into(),
        ));
    }
    let recorded: RecordedSubscriptionMutation = serde_json::from_slice(payload).map_err(|_| {
        RecordingError::InvalidControlCommand("subscription command payload is invalid".into())
    })?;
    recorded.command.validate()?;
    recorded.wire.validate()?;
    Ok((recorded.command.into_command(), recorded.wire.into_wire()))
}

#[cfg(test)]
mod tests {
    use marketfeed_model::InstrumentId;

    use super::*;

    #[test]
    fn subscription_commands_round_trip() {
        for command in [
            SessionCommand::Subscribe(vec!["BTC-USD".into()]),
            SessionCommand::Unsubscribe(vec!["ETH-USD".into()]),
            SessionCommand::Replace(vec!["SOL-USD".into(), "XRP-USD".into()]),
        ] {
            let wire = SubscriptionWireAction::Text(b"subscribe".as_slice().to_vec().into());
            let encoded = encode_subscription_command(&command, &wire).unwrap();
            assert_eq!(
                decode_subscription_command(&encoded).unwrap(),
                (command, wire)
            );
        }
    }

    #[test]
    fn non_subscription_commands_are_rejected() {
        for command in [
            SessionCommand::Resync(InstrumentId(1)),
            SessionCommand::Stop,
        ] {
            assert!(matches!(
                encode_subscription_command(
                    &command,
                    &SubscriptionWireAction::Text(b"x".as_slice().to_vec().into())
                ),
                Err(RecordingError::InvalidControlCommand(_))
            ));
        }
    }

    #[test]
    fn oversized_or_empty_symbols_are_rejected() {
        assert!(matches!(
            encode_subscription_command(
                &SessionCommand::Subscribe(vec![String::new()]),
                &SubscriptionWireAction::Text(b"x".as_slice().to_vec().into())
            ),
            Err(RecordingError::InvalidControlCommand(_))
        ));
        assert!(matches!(
            encode_subscription_command(
                &SessionCommand::Subscribe(vec!["x".repeat(MAX_CONTROL_SYMBOL_BYTES + 1)]),
                &SubscriptionWireAction::Text(b"x".as_slice().to_vec().into())
            ),
            Err(RecordingError::InvalidControlCommand(_))
        ));
    }
}
