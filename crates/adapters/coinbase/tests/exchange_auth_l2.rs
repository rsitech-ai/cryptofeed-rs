use base64::Engine;
use marketfeed_adapter_api::{
    ActionBuffer, Capability, ConcreteSubscriptionSet, DisconnectReason, ReconnectReason,
    SessionAction, SessionInput, SessionMachine, SessionSpec, VenueFactory,
};
use marketfeed_adapter_coinbase::{
    CoinbaseExchangeCredentials, CoinbaseSessionConfig, CoinbaseSpotFactory, CoinbaseSpotSession,
};
use marketfeed_engine::{SessionRunner, SessionRunnerConfig};
use marketfeed_model::{
    CatalogVersion, CatalogView, OverflowPolicy, SessionId, TimestampNs, VenueId,
};

fn credentials() -> CoinbaseExchangeCredentials {
    CoinbaseExchangeCredentials::new(
        "fixture-key",
        base64::engine::general_purpose::STANDARD.encode(b"secret"),
        "fixture-passphrase",
    )
    .expect("synthetic credentials")
}

fn l2_session(credentials: Option<CoinbaseExchangeCredentials>) -> CoinbaseSpotSession {
    CoinbaseSpotSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(16), CatalogVersion(1)),
        CoinbaseSessionConfig {
            enable_l2: true,
            credentials,
            ..CoinbaseSessionConfig::default()
        },
    )
}

fn subscribe_json(actions: &ActionBuffer) -> serde_json::Value {
    let payload = actions
        .as_slice()
        .iter()
        .find_map(|action| match action {
            SessionAction::SendText(payload) => Some(payload),
            SessionAction::SendSensitiveText(payload) => Some(payload.expose()),
            _ => None,
        })
        .expect("subscribe frame");
    serde_json::from_slice(payload).expect("subscribe JSON")
}

#[test]
fn l2_factory_plans_without_credentials_and_advertises_capability() {
    let factory = CoinbaseSpotFactory {
        enable_l2: true,
        credentials: None,
    };
    let catalog = CatalogView::new(VenueId(16), CatalogVersion(1));

    let plan = factory
        .plan(&ConcreteSubscriptionSet::default(), &catalog)
        .expect("planning must remain credential-free");

    assert_eq!(plan.len(), 1);
    assert!(
        factory
            .specification()
            .capabilities
            .contains(&Capability::L2Book)
    );
}

#[test]
fn l2_factory_creates_session_with_injected_credentials() {
    let factory = CoinbaseSpotFactory {
        enable_l2: true,
        credentials: Some(credentials()),
    };
    let catalog = CatalogView::new(VenueId(16), CatalogVersion(1));
    let spec = factory
        .plan(&ConcreteSubscriptionSet::default(), &catalog)
        .expect("plan")
        .remove(0);

    factory
        .create_session(spec, catalog)
        .expect("injected credentials must avoid environment access");
}

#[test]
fn authenticated_l2_connect_sends_documented_fields() {
    let mut session = l2_session(Some(credentials()));
    let mut actions = ActionBuffer::new();

    session
        .on_input(
            SessionInput::Connected {
                now: TimestampNs(1_700_000_000_000_000_000),
            },
            &mut actions,
        )
        .expect("connect");

    let subscribe = subscribe_json(&actions);
    assert_eq!(subscribe["type"], "subscribe");
    assert_eq!(subscribe["timestamp"], "1700000000");
    assert_eq!(subscribe["key"], "fixture-key");
    assert_eq!(subscribe["passphrase"], "fixture-passphrase");
    assert_eq!(
        subscribe["signature"],
        "lhmJXK08fk9SI1ZwFXKFRrPtzfbNOwC+D1xMJJ/1KZg="
    );
    assert!(
        subscribe["channels"]
            .as_array()
            .expect("channels")
            .iter()
            .any(|channel| channel == "level2")
    );
}

#[test]
fn authenticated_subscribe_is_sent_but_never_recorded_or_mirrored() {
    let mut runner = SessionRunner::new(
        Box::new(l2_session(Some(credentials()))),
        SessionRunnerConfig {
            venue: VenueId(16),
            session: SessionId(1),
            record: true,
            overflow: OverflowPolicy::FailEngine,
            mirror_capacity: 16,
            ..SessionRunnerConfig::default()
        },
    )
    .expect("runner");

    runner
        .on_connected(TimestampNs(1_700_000_000_000_000_000))
        .expect("connect");

    let recording = runner.recording_bytes().expect("in-memory recording");
    for secret in [
        b"fixture-key".as_slice(),
        b"fixture-passphrase".as_slice(),
        b"lhmJXK08fk9SI1ZwFXKFRrPtzfbNOwC+D1xMJJ/1KZg=".as_slice(),
    ] {
        assert!(
            !recording
                .windows(secret.len())
                .any(|window| window == secret),
            "authentication material entered raw recording"
        );
    }
    assert!(
        runner
            .other_actions
            .iter()
            .all(|action| !matches!(action, SessionAction::SendSensitiveText(_)))
    );
    let mirrored_debug = format!("{:?}", runner.other_actions);
    assert!(!mirrored_debug.contains("fixture-key"));
    assert!(!mirrored_debug.contains("fixture-passphrase"));

    let writes = runner.take_pending_writes();
    assert_eq!(writes.len(), 1);
    let wire = String::from_utf8_lossy(&writes[0].payload);
    assert!(wire.contains("fixture-key"));
    assert!(wire.contains("fixture-passphrase"));
}

#[test]
fn reconnect_refreshes_l2_timestamp_and_signature() {
    let mut session = l2_session(Some(credentials()));
    let mut first = ActionBuffer::new();
    session
        .on_input(
            SessionInput::Connected {
                now: TimestampNs(1_700_000_000_000_000_000),
            },
            &mut first,
        )
        .expect("first connect");
    let mut disconnected = ActionBuffer::new();
    session
        .on_input(
            SessionInput::Disconnected {
                reason: DisconnectReason::ReconnectRequested,
                now: TimestampNs(1_700_000_000_500_000_000),
            },
            &mut disconnected,
        )
        .expect("disconnect");
    let mut second = ActionBuffer::new();
    session
        .on_input(
            SessionInput::Connected {
                now: TimestampNs(1_700_000_001_000_000_000),
            },
            &mut second,
        )
        .expect("second connect");

    let first = subscribe_json(&first);
    let second = subscribe_json(&second);
    assert_eq!(first["timestamp"], "1700000000");
    assert_eq!(second["timestamp"], "1700000001");
    assert_ne!(first["signature"], second["signature"]);
}

#[test]
fn anonymous_connect_has_no_authentication_fields() {
    let mut session = CoinbaseSpotSession::new(
        SessionSpec {
            endpoint_name: "ws".into(),
            subscriptions: ConcreteSubscriptionSet::default(),
        },
        CatalogView::new(VenueId(16), CatalogVersion(1)),
        CoinbaseSessionConfig::default(),
    );
    let mut actions = ActionBuffer::new();

    session
        .on_input(
            SessionInput::Connected {
                now: TimestampNs(1_700_000_000_000_000_000),
            },
            &mut actions,
        )
        .expect("anonymous connect");

    let subscribe = subscribe_json(&actions);
    for field in ["timestamp", "key", "passphrase", "signature"] {
        assert!(subscribe.get(field).is_none(), "unexpected {field}");
    }
}

#[test]
fn l2_replay_start_requires_no_credentials_or_wire_action() {
    let mut session = l2_session(None);
    let mut actions = ActionBuffer::new();

    session
        .on_replay_start(TimestampNs(1_700_000_000_000_000_000), &mut actions)
        .expect("offline replay must not need credentials");

    assert!(actions.as_slice().is_empty(), "{:?}", actions.as_slice());
}

#[test]
fn coinbase_error_frame_requests_protocol_reconnect() {
    let mut session = l2_session(Some(credentials()));
    let mut bytes = br#"{"type":"error","message":"authentication failed"}"#.to_vec();
    let mut actions = ActionBuffer::new();

    session
        .on_input(
            SessionInput::TextFrame {
                bytes: &mut bytes,
                received: marketfeed_model::FrameStamp {
                    receive_ts: TimestampNs(1),
                    mono_ns: 1,
                },
            },
            &mut actions,
        )
        .expect("error frame");

    assert!(
        actions
            .as_slice()
            .iter()
            .any(|action| matches!(action, SessionAction::Reconnect(ReconnectReason::Protocol)))
    );
}
