//! Live network smoke (ignored by default — keep CI offline).
//! Alpha only — does **not** unlock beta.

use marketfeed_adapter_api::{
    CandleInterval, Channel, ConcreteSubscription, ConcreteSubscriptionSet, DeliveryOptions,
    EventBatch, ReconnectPolicy, VenueFactory,
};
use marketfeed_adapter_coinbase::{
    CoinbaseAdvFactory, CoinbaseExchangeCredentials, CoinbaseIntlCredentials, CoinbaseIntlFactory,
    CoinbaseSpotFactory,
};
use marketfeed_dispatch::PushOutcome;
use marketfeed_engine::{EngineSupervisor, SessionRunnerConfig};
use marketfeed_model::{
    CatalogVersion, CatalogView, InstrumentId, MarketEvent, OverflowPolicy, SessionId, SystemEvent,
    VenueId,
};
use marketfeed_sinks::{EventSink, SinkError};
use marketfeed_transport::{ReqwestHttpTransport, TungsteniteWebSocket, WebSocketSpec};
use tokio::sync::watch;

#[derive(Clone, Copy)]
enum ProbeTarget {
    Market,
    Candle,
    BookSnapshot,
}

impl ProbeTarget {
    fn matches(self, event: &MarketEvent) -> bool {
        match self {
            Self::Market => matches!(event, MarketEvent::Trade(_) | MarketEvent::Quote(_)),
            Self::Candle => matches!(event, MarketEvent::Candle(_)),
            Self::BookSnapshot => matches!(event, MarketEvent::BookSnapshot(_)),
        }
    }
}

struct EventProbe {
    target: ProbeTarget,
    observed: watch::Sender<bool>,
}

impl EventProbe {
    fn new(target: ProbeTarget) -> (Self, watch::Receiver<bool>) {
        let (observed, receiver) = watch::channel(false);
        (Self { target, observed }, receiver)
    }
}

impl EventSink for EventProbe {
    fn push_batch(&mut self, batch: EventBatch) -> Result<PushOutcome, SinkError> {
        if batch
            .events
            .iter()
            .any(|event| self.target.matches(&event.payload))
        {
            let _ = self.observed.send(true);
        }
        Ok(PushOutcome::Accepted)
    }

    fn push_system(&mut self, _event: SystemEvent) -> Result<PushOutcome, SinkError> {
        Ok(PushOutcome::Accepted)
    }
}

#[tokio::test]
#[ignore = "live network: cargo test -p marketfeed-adapter-coinbase --test live_ignored -- --ignored --nocapture"]
async fn live_coinbase_spot_trade_or_quote() {
    let factory = CoinbaseSpotFactory {
        enable_l2: false,
        credentials: None,
    };
    let plan = factory
        .plan(
            &Default::default(),
            &CatalogView::new(VenueId(16), CatalogVersion(1)),
        )
        .unwrap();
    let url = plan[0].endpoint_name.clone();
    let machine = factory
        .create_session(
            plan.into_iter().next().unwrap(),
            CatalogView::new(VenueId(16), CatalogVersion(1)),
        )
        .unwrap();

    let mut supervisor = EngineSupervisor::new();
    supervisor.mark_running();
    let session = SessionId(1);
    supervisor
        .insert_session(
            machine,
            SessionRunnerConfig {
                session,
                record: true,
                // Live smoke has no consumer; DropOldest keeps high-rate venues up while
                // mirrors retain recent events for assertions.
                overflow: OverflowPolicy::DropOldest,
                mirror_capacity: 64,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    let mut ws = TungsteniteWebSocket::new();
    let http = ReqwestHttpTransport::new().expect("reqwest rustls");
    let spec = WebSocketSpec {
        url,
        max_frame_bytes: 4 * 1024 * 1024,
        ..WebSocketSpec::default()
    };
    let policy = ReconnectPolicy {
        min_delay_ms: 200,
        max_delay_ms: 2_000,
        reset_after_live_ms: 5_000,
    };

    let (mut probe, mut observed) = EventProbe::new(ProbeTarget::Market);
    let run =
        supervisor.run_session_loop_to(session, &mut ws, &http, &spec, policy, 2, Some(&mut probe));
    tokio::select! {
        res = run => {
            res.unwrap();
            assert!(*observed.borrow(), "session ended before a live trade or quote was observed");
        }
        result = observed.changed() => {
            result.expect("event probe remains connected");
            assert!(*observed.borrow(), "expected at least one live trade or quote");
        }
        _ = tokio::time::sleep(std::time::Duration::from_secs(20)) => {
            panic!("expected at least one live trade or quote within 20s");
        }
    }
}

#[tokio::test]
#[ignore = "live authenticated network: requires COINBASE_EXCHANGE_API_KEY / COINBASE_EXCHANGE_API_SECRET / COINBASE_EXCHANGE_API_PASSPHRASE"]
async fn live_coinbase_spot_l2() {
    let Ok(credentials) = CoinbaseExchangeCredentials::from_env() else {
        eprintln!("SKIP: Coinbase Exchange credentials are unavailable");
        return;
    };
    let factory = CoinbaseSpotFactory {
        enable_l2: true,
        credentials: Some(credentials),
    };
    let catalog = CatalogView::new(VenueId(16), CatalogVersion(1));
    let plan = factory
        .plan(&Default::default(), &catalog)
        .expect("authenticated L2 plan");
    let url = plan[0].endpoint_name.clone();
    let machine = factory
        .create_session(plan.into_iter().next().unwrap(), catalog)
        .expect("authenticated L2 session");

    let mut supervisor = EngineSupervisor::new();
    supervisor.mark_running();
    let session = SessionId(1);
    supervisor
        .insert_session(
            machine,
            SessionRunnerConfig {
                session,
                venue: VenueId(16),
                record: true,
                overflow: OverflowPolicy::DropOldest,
                mirror_capacity: 64,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    let mut ws = TungsteniteWebSocket::new();
    let http = ReqwestHttpTransport::new().expect("reqwest rustls");
    let spec = WebSocketSpec {
        url,
        max_frame_bytes: 8 * 1024 * 1024,
        ..WebSocketSpec::default()
    };
    let policy = ReconnectPolicy {
        min_delay_ms: 200,
        max_delay_ms: 2_000,
        reset_after_live_ms: 5_000,
    };

    let (mut probe, mut observed) = EventProbe::new(ProbeTarget::BookSnapshot);
    let run =
        supervisor.run_session_loop_to(session, &mut ws, &http, &spec, policy, 2, Some(&mut probe));
    tokio::select! {
        result = run => {
            result.unwrap();
            assert!(
                *observed.borrow(),
                "session ended before an authenticated Coinbase Exchange L2 snapshot was observed"
            );
        }
        result = observed.changed() => {
            result.expect("event probe remains connected");
            assert!(*observed.borrow(), "expected an authenticated Coinbase Exchange L2 snapshot");
        }
        () = tokio::time::sleep(std::time::Duration::from_secs(25)) => {
            panic!("expected an authenticated Coinbase Exchange L2 snapshot within 25s");
        }
    }
}

#[tokio::test]
#[ignore = "live network: cargo test -p marketfeed-adapter-coinbase --test live_ignored live_coinbase_adv_candle -- --ignored --nocapture"]
async fn live_coinbase_adv_candle() {
    let factory = CoinbaseAdvFactory { enable_l2: false };
    let request = ConcreteSubscriptionSet {
        items: vec![ConcreteSubscription {
            instrument: InstrumentId(1),
            channel: Channel::Candles {
                interval: CandleInterval::M1,
            },
            delivery: DeliveryOptions::default(),
        }],
    };
    let catalog = CatalogView::new(VenueId(18), CatalogVersion(1));
    let plan = factory.plan(&request, &catalog).unwrap();
    let url = plan[0].endpoint_name.clone();
    let machine = factory
        .create_session(plan.into_iter().next().unwrap(), catalog)
        .unwrap();

    let mut supervisor = EngineSupervisor::new();
    supervisor.mark_running();
    let session = SessionId(1);
    supervisor
        .insert_session(
            machine,
            SessionRunnerConfig {
                session,
                venue: VenueId(18),
                record: true,
                overflow: OverflowPolicy::DropOldest,
                mirror_capacity: 64,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    let mut ws = TungsteniteWebSocket::new();
    let http = ReqwestHttpTransport::new().expect("reqwest rustls");
    let spec = WebSocketSpec {
        url,
        max_frame_bytes: 4 * 1024 * 1024,
        ..WebSocketSpec::default()
    };
    let policy = ReconnectPolicy {
        min_delay_ms: 200,
        max_delay_ms: 2_000,
        reset_after_live_ms: 5_000,
    };

    let (mut probe, mut observed) = EventProbe::new(ProbeTarget::Candle);
    let run =
        supervisor.run_session_loop_to(session, &mut ws, &http, &spec, policy, 2, Some(&mut probe));
    tokio::select! {
        res = run => {
            res.unwrap();
            assert!(*observed.borrow(), "session ended before a live candle was observed");
        }
        result = observed.changed() => {
            result.expect("event probe remains connected");
            assert!(*observed.borrow(), "expected at least one live candle");
        }
        _ = tokio::time::sleep(std::time::Duration::from_secs(20)) => {
            panic!("expected at least one live candle within 20s");
        }
    }
}

#[tokio::test]
#[ignore = "live network: cargo test -p marketfeed-adapter-coinbase --test live_ignored live_coinbase_adv_trade_or_quote -- --ignored --nocapture"]
async fn live_coinbase_adv_trade_or_quote() {
    let factory = CoinbaseAdvFactory { enable_l2: false };
    let catalog = CatalogView::new(VenueId(18), CatalogVersion(1));
    let plan = factory.plan(&Default::default(), &catalog).unwrap();
    let url = plan[0].endpoint_name.clone();
    let machine = factory
        .create_session(plan.into_iter().next().unwrap(), catalog)
        .unwrap();

    let mut supervisor = EngineSupervisor::new();
    supervisor.mark_running();
    let session = SessionId(1);
    supervisor
        .insert_session(
            machine,
            SessionRunnerConfig {
                session,
                venue: VenueId(18),
                record: true,
                overflow: OverflowPolicy::DropOldest,
                mirror_capacity: 64,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    let mut ws = TungsteniteWebSocket::new();
    let http = ReqwestHttpTransport::new().expect("reqwest rustls");
    let spec = WebSocketSpec {
        url,
        max_frame_bytes: 4 * 1024 * 1024,
        ..WebSocketSpec::default()
    };
    let policy = ReconnectPolicy {
        min_delay_ms: 200,
        max_delay_ms: 2_000,
        reset_after_live_ms: 5_000,
    };

    let (mut probe, mut observed) = EventProbe::new(ProbeTarget::Market);
    let run =
        supervisor.run_session_loop_to(session, &mut ws, &http, &spec, policy, 2, Some(&mut probe));
    tokio::select! {
        res = run => {
            res.unwrap();
            assert!(*observed.borrow(), "session ended before a live trade or quote was observed");
        }
        result = observed.changed() => {
            result.expect("event probe remains connected");
            assert!(*observed.borrow(), "expected at least one live trade or quote");
        }
        _ = tokio::time::sleep(std::time::Duration::from_secs(20)) => {
            panic!("expected at least one live trade or quote within 20s");
        }
    }
}

#[tokio::test]
#[ignore = "live network: cargo test -p marketfeed-adapter-coinbase --test live_ignored live_coinbase_adv_l2 -- --ignored --nocapture"]
async fn live_coinbase_adv_l2() {
    let factory = CoinbaseAdvFactory { enable_l2: true };
    let catalog = CatalogView::new(VenueId(18), CatalogVersion(1));
    let plan = factory.plan(&Default::default(), &catalog).unwrap();
    let url = plan[0].endpoint_name.clone();
    let machine = factory
        .create_session(plan.into_iter().next().unwrap(), catalog)
        .unwrap();

    let mut supervisor = EngineSupervisor::new();
    supervisor.mark_running();
    let session = SessionId(1);
    supervisor
        .insert_session(
            machine,
            SessionRunnerConfig {
                session,
                venue: VenueId(18),
                record: true,
                overflow: OverflowPolicy::DropOldest,
                mirror_capacity: 64,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();

    let mut ws = TungsteniteWebSocket::new();
    let http = ReqwestHttpTransport::new().expect("reqwest rustls");
    let spec = WebSocketSpec {
        url,
        max_frame_bytes: 8 * 1024 * 1024,
        ..WebSocketSpec::default()
    };
    let policy = ReconnectPolicy {
        min_delay_ms: 200,
        max_delay_ms: 2_000,
        reset_after_live_ms: 5_000,
    };

    let (mut probe, mut observed) = EventProbe::new(ProbeTarget::BookSnapshot);
    let run =
        supervisor.run_session_loop_to(session, &mut ws, &http, &spec, policy, 2, Some(&mut probe));
    tokio::select! {
        res = run => {
            res.unwrap();
            assert!(*observed.borrow(), "session ended before a live Adv L2 snapshot was observed");
        }
        result = observed.changed() => {
            result.expect("event probe remains connected");
            assert!(*observed.borrow(), "expected a live Adv L2 BookSnapshot");
        }
        _ = tokio::time::sleep(std::time::Duration::from_secs(25)) => {
            panic!("expected at least one live Adv L2 BookSnapshot within 25s");
        }
    }
}

#[tokio::test]
#[ignore = "live network: cargo test -p marketfeed-adapter-coinbase --test live_ignored live_coinbase_intl_trade_or_quote -- --ignored --nocapture"]
async fn live_coinbase_intl_trade_or_quote() {
    if CoinbaseIntlCredentials::from_env().is_err() {
        return;
    }
    let factory = CoinbaseIntlFactory {
        enable_l2: false,
        credentials: None,
    };
    let catalog = CatalogView::new(VenueId(19), CatalogVersion(1));
    let plan = factory.plan(&Default::default(), &catalog).unwrap();
    let url = plan[0].endpoint_name.clone();
    let machine = factory
        .create_session(plan.into_iter().next().unwrap(), catalog)
        .unwrap();
    let mut supervisor = EngineSupervisor::new();
    supervisor.mark_running();
    let session = SessionId(1);
    supervisor
        .insert_session(
            machine,
            SessionRunnerConfig {
                session,
                venue: VenueId(19),
                record: true,
                overflow: OverflowPolicy::DropOldest,
                mirror_capacity: 64,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();
    let mut ws = TungsteniteWebSocket::new();
    let http = ReqwestHttpTransport::new().expect("reqwest rustls");
    let spec = WebSocketSpec {
        url,
        max_frame_bytes: 8 * 1024 * 1024,
        ..WebSocketSpec::default()
    };
    let policy = ReconnectPolicy {
        min_delay_ms: 200,
        max_delay_ms: 2_000,
        reset_after_live_ms: 5_000,
    };
    let (mut probe, mut observed) = EventProbe::new(ProbeTarget::Market);
    let run =
        supervisor.run_session_loop_to(session, &mut ws, &http, &spec, policy, 2, Some(&mut probe));
    tokio::select! {
        res = run => {
            res.unwrap();
            assert!(*observed.borrow(), "session ended before a live INTX trade or quote was observed");
        }
        result = observed.changed() => {
            result.expect("event probe remains connected");
            assert!(*observed.borrow(), "expected at least one live INTX trade or quote");
        }
        _ = tokio::time::sleep(std::time::Duration::from_secs(25)) => {
            panic!("expected at least one live INTX trade or quote within 25s");
        }
    }
}

#[tokio::test]
#[ignore = "live network: cargo test -p marketfeed-adapter-coinbase --test live_ignored live_coinbase_intl_l2 -- --ignored --nocapture"]
async fn live_coinbase_intl_l2() {
    if CoinbaseIntlCredentials::from_env().is_err() {
        return;
    }
    let factory = CoinbaseIntlFactory {
        enable_l2: true,
        credentials: None,
    };
    let catalog = CatalogView::new(VenueId(19), CatalogVersion(1));
    let plan = factory.plan(&Default::default(), &catalog).unwrap();
    let url = plan[0].endpoint_name.clone();
    let machine = factory
        .create_session(plan.into_iter().next().unwrap(), catalog)
        .unwrap();
    let mut supervisor = EngineSupervisor::new();
    supervisor.mark_running();
    let session = SessionId(1);
    supervisor
        .insert_session(
            machine,
            SessionRunnerConfig {
                session,
                venue: VenueId(19),
                record: true,
                overflow: OverflowPolicy::DropOldest,
                mirror_capacity: 64,
                ..SessionRunnerConfig::default()
            },
        )
        .unwrap();
    let mut ws = TungsteniteWebSocket::new();
    let http = ReqwestHttpTransport::new().expect("reqwest rustls");
    let spec = WebSocketSpec {
        url,
        max_frame_bytes: 8 * 1024 * 1024,
        ..WebSocketSpec::default()
    };
    let policy = ReconnectPolicy {
        min_delay_ms: 200,
        max_delay_ms: 2_000,
        reset_after_live_ms: 5_000,
    };
    let (mut probe, mut observed) = EventProbe::new(ProbeTarget::BookSnapshot);
    let run =
        supervisor.run_session_loop_to(session, &mut ws, &http, &spec, policy, 2, Some(&mut probe));
    tokio::select! {
        res = run => {
            res.unwrap();
            assert!(*observed.borrow(), "session ended before a live INTX L2 snapshot was observed");
        }
        result = observed.changed() => {
            result.expect("event probe remains connected");
            assert!(*observed.borrow(), "expected a live INTX L2 BookSnapshot");
        }
        _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
            panic!("expected at least one live INTX L2 BookSnapshot within 30s");
        }
    }
}
