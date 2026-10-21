use super::*;
use crate::causal_bootstrap::{PreparedBootstrap, current_build};
use futures_util::FutureExt;
use marketfeed_adapter_api::*;
use marketfeed_adapter_hyperliquid::HyperliquidFactory;
use marketfeed_engine::CaptureConfig;
use marketfeed_model::*;
use marketfeed_recording::{shared_batch::*, shared_feed::*};
use std::sync::{
    OnceLock,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::mpsc;
const SPOT: &[u8] = br#"{"tokens":[{"name":"USDC","szDecimals":8,"weiDecimals":8,"index":0,"tokenId":"0x0"},{"name":"PURR","szDecimals":0,"weiDecimals":5,"index":7,"tokenId":"0x7"}],"universe":[{"name":"@12","tokens":[7,0],"index":12}]}"#;
const PERPS: &[u8] = br#"[{"collateralToken":0,"universe":[{"name":"BTC","szDecimals":5}]}]"#;
fn fixture(spot: bool) -> BootstrapDto {
    fixture_with_meta(spot, SPOT, PERPS, &[1])
}
fn fixture_with_meta(
    spot: bool,
    spot_meta: &[u8],
    perp_meta: &[u8],
    selected: &[u32],
) -> BootstrapDto {
    static BUILD: OnceLock<BootstrapBuildDto> = OnceLock::new();
    let factory = HyperliquidFactory { spot };
    let mut definitions = Vec::new();
    factory
        .parse_instruments(
            &[spot_meta, perp_meta].map(|bytes| HttpResponse {
                status: 200,
                headers: vec![],
                body: bytes.to_vec().into(),
            }),
            &mut definitions,
        )
        .unwrap();
    let catalog = CatalogView::with_instruments(
        factory.specification().id,
        CatalogVersion(7),
        definitions
            .into_iter()
            .enumerate()
            .map(|(i, d)| d.into_instrument(InstrumentId(i as u32 + 1), CatalogVersion(7)))
            .collect::<Vec<_>>(),
    );
    let channels = [Channel::Trades];
    let request = ConcreteSubscriptionSet {
        items: channels
            .iter()
            .flat_map(|channel| {
                selected.iter().map(move |instrument| ConcreteSubscription {
                    instrument: InstrumentId(*instrument),
                    channel: channel.clone(),
                    delivery: DeliveryOptions {
                        emit_book_snapshots: true,
                        emit_book_deltas: false,
                        emit_bbo: true,
                    },
                })
            })
            .collect(),
    };
    let mut plans = factory.plan(&request, &catalog).unwrap();
    assert_eq!(plans.len(), 1);
    let spec = plans.remove(0);
    let capture = CaptureConfig {
        venue: catalog.venue,
        session: SessionId(31),
        dispatch_capacity: 64,
        overflow: OverflowPolicy::FailEngine,
        max_bytes: 1 << 20,
        max_entries: 2048,
    };
    let mut dto = BootstrapDto {
        schema_version: 2,
        build: BUILD.get_or_init(|| current_build().unwrap()).clone(),
        factory: FactoryDto::HyperliquidPublic {
            market: if spot {
                HyperliquidMarketDto::Spot
            } else {
                HyperliquidMarketDto::Perps
            },
        },
        environment: BootstrapEnvironmentDto::Production,
        output_profile: OutputProfileDto::DiskNullDispatchV1,
        adapter_policy: AdapterEmissionPolicyDto::HyperliquidSelectedChannelsV1,
        protocol_topics: HyperliquidFactory::public_plan_topics(spot, &spec, &catalog).unwrap(),
        session: (&spec).into(),
        catalog: (&catalog).into(),
        capture: capture.into(),
        runner_policy: capture.runtime_policy(),
        owner_policy: Some(JournalOwnerPolicyDto {
            max_pending_operations: 16,
            max_pending_bytes: 2 << 20,
            max_input_bytes: 8192,
            max_operation_bytes: 256 << 10,
            max_ingress_age: Duration::from_secs(10).into(),
            receipt_poll_interval: Duration::from_millis(1).into(),
        }),
        public_inputs: vec![
            PublicReproductionInputDto::new(HYPERLIQUID_SPOT_META_ROLE, spot_meta),
            PublicReproductionInputDto::new(HYPERLIQUID_PERP_METAS_ROLE, perp_meta),
        ],
    };
    dto.bind_history_policy(HistoryRuntimePolicyDto {
        schema_version: 1,
        max_pending_observations: 32,
        max_pending_observation_bytes: 4 << 20,
        max_record_bytes: 256 << 10,
        max_finalization_age: Duration::from_secs(2).into(),
    })
    .unwrap();
    dto.bind_observation_policy(NativeObservationPolicyDto::HyperliquidV1 {
        limits: ObservedBatchLimits {
            layout: NativeObservationLimits {
                rows: 64,
                associations: 128,
                book_levels: 40,
                mids: 0,
                users: 128,
                string_bytes: 4096,
            },
            encoded_bytes: 64 << 10,
        },
    })
    .unwrap();
    dto.bind_wire_policy(WirePolicyDto {
        schema_version: 1,
        max_pending_writes: 16,
        max_pending_bytes: 64 << 10,
        transport_controls: TransportControlsDto::ManagedConnectReadAutomaticPongCloseUnjournaledV1,
    })
    .unwrap();
    bounds().bind(&mut dto).unwrap();
    dto
}
fn bounds() -> DriverPolicy {
    DriverPolicy {
        schema_version: 1,
        max_frame_bytes: 1024,
        connect_timeout_ms: 5000,
        write_timeout_ms: 5000,
        close_timeout_ms: 5000,
        owner_poll_ms: 1,
        timer_interval_ms: 100,
        reconnect_delay_ms: 1,
        max_reconnects: 1,
        max_session_ms: 30000,
    }
}
fn due_bounds() -> DriverPolicy {
    DriverPolicy {
        schema_version: 2,
        ..bounds()
    }
}
#[derive(Clone, Copy)]
enum WriteMode {
    Success,
    Pending,
    Failed,
}
#[derive(Default)]
struct Probe {
    connects: AtomicUsize,
    reads_started: AtomicUsize,
    delivered: AtomicUsize,
    closes: AtomicUsize,
    close_error: Mutex<Option<TransportError>>,
    read_hook: Mutex<Option<ReadHook>>,
    writes: Mutex<Vec<marketfeed_transport::OutboundFrame>>,
    entered: Notify,
}
enum ReadHook {
    Stop(DriverControl),
    Fill(DriverControl),
}
struct TestTransport {
    probe: Arc<Probe>,
    inbound: mpsc::Receiver<
        Result<marketfeed_transport::InboundFrame, marketfeed_transport::TransportError>,
    >,
    mode: WriteMode,
}
impl WebSocketTransport for TestTransport {
    async fn connect(
        &mut self,
        _: &WebSocketSpec,
    ) -> Result<(), marketfeed_transport::TransportError> {
        self.probe.connects.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn read_frame(
        &mut self,
        _: &mut FrameBuffer,
    ) -> Result<marketfeed_transport::InboundFrame, marketfeed_transport::TransportError> {
        self.probe.reads_started.fetch_add(1, Ordering::SeqCst);
        let frame = self
            .inbound
            .recv()
            .await
            .unwrap_or(Err(marketfeed_transport::TransportError::Closed));
        self.probe.delivered.fetch_add(1, Ordering::SeqCst);
        if let Some(hook) = self.probe.read_hook.lock().unwrap().take() {
            match hook {
                ReadHook::Stop(control) => {
                    control.request_stop().unwrap();
                }
                ReadHook::Fill(control) => {
                    let mut refused = false;
                    for _ in 0..100 {
                        if control.request_resync(InstrumentId(1)).is_err() {
                            refused = true;
                            break;
                        }
                    }
                    assert!(refused, "must reach actual bounded ingress refusal");
                }
            }
        }
        frame
    }
    async fn write_frame(
        &mut self,
        frame: marketfeed_transport::OutboundFrame,
    ) -> Result<(), marketfeed_transport::TransportError> {
        self.probe.writes.lock().unwrap().push(frame);
        self.probe.entered.notify_one();
        match self.mode {
            WriteMode::Success => Ok(()),
            WriteMode::Pending => std::future::pending().await,
            WriteMode::Failed => Err(marketfeed_transport::TransportError::Stub(
                "declared failure".into(),
            )),
        }
    }
    async fn close(&mut self, _: CloseReason) -> Result<(), marketfeed_transport::TransportError> {
        self.probe.closes.fetch_add(1, Ordering::SeqCst);
        if let Some(error) = self.probe.close_error.lock().unwrap().take() {
            return Err(error);
        }
        Ok(())
    }
}
struct Fixture {
    _dir: tempfile::TempDir,
    actor: FeedBatchActor,
    core: Core<TestTransport>,
    control: DriverControl,
    probe: Arc<Probe>,
    input: mpsc::Sender<
        Result<marketfeed_transport::InboundFrame, marketfeed_transport::TransportError>,
    >,
    topic: String,
}
fn start(mode: WriteMode) -> Fixture {
    start_with_capacity(mode, 16)
}
fn start_with_capacity(mode: WriteMode, capacity: usize) -> Fixture {
    start_with_policy(mode, capacity, bounds())
}
fn start_with_policy(mode: WriteMode, capacity: usize, driver_policy: DriverPolicy) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let mut dto = fixture(true);
    dto.public_inputs.retain(|p| p.role != DRIVER_POLICY_ROLE);
    driver_policy.clone().bind(&mut dto).unwrap();
    let topic = dto.protocol_topics[0].clone();
    let actor = FeedBatchActor::spawn(
        FeedWriter::create_profile(
            dir.path().join("feed"),
            64 << 20,
            FeedProfile::EngineCausalV5,
        )
        .unwrap(),
        BatchConfig {
            max_pending_operations: 8,
            max_pending_bytes: 8 << 20,
            max_group_operations: 8,
            max_group_bytes: 8 << 20,
            max_age: Duration::from_millis(1),
        },
    )
    .unwrap();
    let committed = PreparedBootstrap::new_wire_history(dto, MAX_OPERATION_BYTES)
        .unwrap()
        .try_admit(&actor.handle())
        .unwrap()
        .wait()
        .unwrap();
    let owner = committed
        .instantiate()
        .unwrap()
        .spawn_history_owner()
        .unwrap();
    let probe = Arc::new(Probe::default());
    let (input, inbound) = mpsc::channel(capacity);
    let transport = TestTransport {
        probe: probe.clone(),
        inbound,
        mode,
    };
    let limits = Arc::new(HyperliquidLimits::default());
    limits.reserve_causal(true, 1).unwrap();
    let (core, control) = Core::new(
        owner,
        transport,
        driver_policy,
        WebSocketSpec::default(),
        BTreeSet::from([1]),
        Reservation {
            limits,
            spot: true,
            subscriptions: 1,
        },
    );
    Fixture {
        _dir: dir,
        actor,
        core,
        control,
        probe,
        input,
        topic,
    }
}
#[track_caller]
fn wait_for(mut predicate: impl FnMut() -> bool) -> impl std::future::Future<Output = ()> {
    let caller = std::panic::Location::caller();
    async move {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !predicate() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("driver fixture watchdog at {caller}"));
    }
}
fn guarded(core: Core<TestTransport>) -> DriverRun {
    let ingress = core.control.shared.ingress.clone();
    DriverRun {
        inner: Box::pin(core.run()),
        ingress,
        completed: false,
    }
}
#[tokio::test]
async fn stop_before_run_never_connects_and_requires_exact_committed_stop() {
    let Fixture {
        _dir,
        actor,
        core,
        control,
        probe,
        ..
    } = start(WriteMode::Success);
    let id = control.request_stop().unwrap();
    assert_eq!(control.request_stop().unwrap(), id);
    let report = guarded(core).await;
    assert_eq!(probe.connects.load(Ordering::SeqCst), 0);
    assert!(probe.writes.lock().unwrap().is_empty());
    assert_eq!(report.stop_input_id, Some(id));
    assert!(
        report.stop_committed && report.wire_clean && report.clean_stop,
        "{report:?}"
    );
    actor.shutdown().unwrap();
}
#[tokio::test]
async fn actual_owner_drains_empty_control_and_native_market_handoffs_and_gates_json_ping() {
    let Fixture {
        _dir,
        actor,
        core,
        control,
        probe,
        input,
        topic,
    } = start(WriteMode::Success);
    let task = tokio::spawn(guarded(core));
    wait_for(|| probe.writes.lock().unwrap().len() == 1).await;
    eprintln!(
        "native driver first write committed: {:?}",
        control.shared.ingress.status()
    );
    let first = probe.writes.lock().unwrap()[0].clone();
    assert_eq!(first.opcode, FrameOpcode::Text);
    assert_eq!(
        String::from_utf8(first.payload).unwrap(),
        format!(r#"{{"method":"subscribe","subscription":{topic}}}"#)
    );
    let ack = format!(
        r#"{{"channel":"subscriptionResponse","data":{{"method":"subscribe","subscription":{topic}}}}}"#
    );
    input
        .send(Ok(marketfeed_transport::InboundFrame {
            opcode: FrameOpcode::Text,
            payload: ack.into_bytes(),
        }))
        .await
        .unwrap();
    input
        .send(Ok(marketfeed_transport::InboundFrame {
            opcode: FrameOpcode::Ping,
            payload: b"native-ping".to_vec(),
        }))
        .await
        .unwrap();
    input.send(Ok(marketfeed_transport::InboundFrame {opcode:FrameOpcode::Text,payload:br#"{"channel":"trades","data":[{"coin":"@12","side":"B","px":"10","sz":"2","time":1,"tid":1,"hash":"h1"}]}"#.to_vec()})).await.unwrap();
    wait_for(|| probe.delivered.load(Ordering::SeqCst) >= 3).await;
    // Insert an exact accepted marker after all three acquired frames. Its
    // committed execution proves prior ACK/native/control callbacks completed;
    // waiting for an idle instant would race the driver's periodic timer polls.
    let mut marker = control
        .shared
        .ingress
        .try_submit(
            lock(&control.shared).connection,
            CaptureInput::TimerPoll {
                now: wall_time().unwrap(),
            },
        )
        .unwrap();
    wait_for(|| matches!(marker.try_result(), Some(InputSettlement::Committed { .. }))).await;
    let connection = lock(&control.shared).connection;
    // Exercise the native timer through the same actual owner. No direct
    // outbound frame or caller-manufactured receipt enters the driver.
    eprintln!(
        "native driver received/handoff stage complete: {:?}",
        control.shared.ingress.status()
    );
    let future_time = TimestampNs(wall_time().unwrap().0 + 31_000_000_000);
    control
        .shared
        .ingress
        .try_submit(connection, CaptureInput::TimerPoll { now: future_time })
        .unwrap();
    wait_for(|| {
        let status = control.shared.ingress.status();
        assert!(
            status.owner.failure.is_none(),
            "native driver timer failed: {status:?}"
        );
        probe.writes.lock().unwrap().len() == 2
    })
    .await;
    assert_eq!(
        probe.writes.lock().unwrap()[1].payload,
        br#"{"method":"ping"}"#
    );
    wait_for(|| control.shared.wire_completed.load(Ordering::Relaxed) == 2).await;
    control.request_stop().unwrap();
    let report = task.await.unwrap();
    assert!(report.clean_stop, "{report:?}");
    assert_eq!(
        (
            report.inbound_text,
            report.native_controls,
            report.wire_completions
        ),
        (2, 1, 2)
    );
    assert!(report.handoffs >= 5);
    actor.shutdown().unwrap();
}
#[tokio::test]
async fn stop_during_real_transport_future_records_unknown_and_never_retries() {
    let Fixture {
        _dir,
        actor,
        core,
        control,
        probe,
        input: _input,
        ..
    } = start(WriteMode::Pending);
    let task = tokio::spawn(guarded(core));
    probe.entered.notified().await;
    control.request_stop().unwrap();
    let report = task.await.unwrap();
    assert_eq!(probe.writes.lock().unwrap().len(), 1);
    assert!(!report.wire_clean && !report.clean_stop, "{report:?}");
    actor.shutdown().unwrap();
}
#[tokio::test]
async fn failed_transport_write_never_becomes_clean_or_retries() {
    let Fixture {
        _dir,
        actor,
        core,
        probe,
        input: _input,
        ..
    } = start(WriteMode::Failed);
    let report = guarded(core).await;
    assert_eq!(report.reason, ExitReason::WriteFailure);
    assert_eq!(probe.writes.lock().unwrap().len(), 1);
    assert!(!report.wire_clean && !report.clean_stop);
    assert_eq!(report.transport_error_count, 1);
    assert_eq!(
        report.last_transport_error.as_ref().unwrap().stage,
        TransportStage::ApplicationWrite
    );
    assert_eq!(
        report.last_transport_error.as_ref().unwrap().detail,
        WireErrorDto::Transport(TransportErrorDto::Stub("declared failure".into()))
    );
    actor.shutdown().unwrap();
}
#[tokio::test]
async fn ingress_full_is_terminal_without_silently_dropping_frames() {
    let Fixture {
        _dir,
        actor,
        core,
        control,
        probe,
        ..
    } = start(WriteMode::Success);
    let mut refused = false;
    for _ in 0..100 {
        if control.request_resync(InstrumentId(1)).is_err() {
            refused = true;
            break;
        }
    }
    assert!(refused);
    let report = guarded(core).await;
    assert!(!report.clean_stop);
    assert_eq!(probe.connects.load(Ordering::SeqCst), 0);
    assert!(probe.writes.lock().unwrap().is_empty());
    actor.shutdown().unwrap();
}
#[test]
fn policy_canonical_presence_digest_and_bounds_are_required_before_admission() {
    let dto = fixture(true);
    assert_eq!(policy(&dto).unwrap(), bounds());
    let mut no = dto.clone();
    no.public_inputs.retain(|p| p.role != DRIVER_POLICY_ROLE);
    assert!(policy(&no).is_err());
    let mut duplicate = dto.clone();
    duplicate.public_inputs.push(
        dto.public_inputs
            .iter()
            .find(|p| p.role == DRIVER_POLICY_ROLE)
            .unwrap()
            .clone(),
    );
    assert!(PreparedBootstrap::new_wire_history(duplicate, MAX_OPERATION_BYTES).is_err());
    let mut corrupt = dto.clone();
    corrupt
        .public_inputs
        .iter_mut()
        .find(|p| p.role == DRIVER_POLICY_ROLE)
        .unwrap()
        .sha256 = "0".repeat(64);
    assert!(PreparedBootstrap::new_wire_history(corrupt, MAX_OPERATION_BYTES).is_err());
    let mut bad = bounds();
    bad.max_frame_bytes = 8192;
    assert!(bad.validate(&dto).is_err());
    let mut bad = dto.clone();
    bad.session.endpoint_name = "wss://other.invalid/ws".into();
    assert!(policy(&bad).is_err());
}
#[tokio::test]
async fn transport_loss_fences_then_reconnects_with_new_source_identity() {
    let Fixture {
        _dir,
        actor,
        core,
        control,
        probe,
        input,
        ..
    } = start(WriteMode::Success);
    let task = tokio::spawn(guarded(core));
    wait_for(|| probe.writes.lock().unwrap().len() == 1).await;
    assert_eq!(lock(&control.shared).connection, ConnectionId(1));
    input
        .send(Err(marketfeed_transport::TransportError::Closed))
        .await
        .unwrap();
    wait_for(|| probe.writes.lock().unwrap().len() == 2).await;
    assert_eq!(lock(&control.shared).connection, ConnectionId(2));
    assert_eq!(probe.connects.load(Ordering::SeqCst), 2);
    assert!(probe.closes.load(Ordering::SeqCst) >= 1);
    wait_for(|| control.shared.wire_completed.load(Ordering::Relaxed) == 2).await;
    control.request_stop().unwrap();
    let report = task.await.unwrap();
    assert!(report.clean_stop, "{report:?}");
    assert_eq!((report.connections, report.wire_completions), (2, 2));
    actor.shutdown().unwrap();
}
#[tokio::test]
async fn dropping_public_run_future_fences_before_cancelling_actual_write() {
    let Fixture {
        _dir,
        actor,
        core,
        control,
        probe,
        input: _input,
        ..
    } = start(WriteMode::Pending);
    let task = tokio::spawn(guarded(core));
    probe.entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        control.shared.ingress.status().owner.phase,
        OwnerPhase::Stopped
    );
    assert!(control.shared.ingress.status().owner.failure.is_some());
    assert_eq!(probe.writes.lock().unwrap().len(), 1);
    assert!(control.request_resync(InstrumentId(1)).is_err());
    actor.shutdown().unwrap();
}
#[tokio::test]
async fn bounded_session_budget_requests_real_stop_and_joins() {
    let Fixture {
        _dir,
        actor,
        mut core,
        probe,
        input: _input,
        ..
    } = start(WriteMode::Success);
    core.policy.max_session_ms = 30;
    let report = guarded(core).await;
    assert_eq!(report.reason, ExitReason::SessionBudget);
    assert!(
        report.stop_input_id.is_some() && report.stop_committed,
        "{report:?}"
    );
    assert!(report.clean_stop, "{report:?}");
    assert_eq!(probe.connects.load(Ordering::SeqCst), 1);
    actor.shutdown().unwrap();
}
#[test]
fn opaque_collector_reuses_limits_and_refuses_legacy_hyperliquid_shadow() {
    let state = crate::DaemonState::new(crate::DaemonConfig::from_toml_str("").unwrap());
    let budget = CollectorBudget::from_collector(state.clone()).unwrap();
    let second = CollectorBudget::from_collector(state.clone()).unwrap();
    assert!(Arc::ptr_eq(&budget.limits, &state.hyperliquid_limits));
    assert!(Arc::ptr_eq(&budget.limits, &second.limits));
    assert!(Arc::get_mut(&mut state.clone()).is_none());
    let config = crate::DaemonConfig::from_toml_str(
        r#"[[venues]]
id="hl"
adapter="hyperliquid"
segment="spot"
symbols=["PURR/USDC"]
channels=["trades"]
"#,
    )
    .unwrap();
    let legacy = crate::DaemonState::new(config);
    assert!(CollectorBudget::from_collector(legacy).is_err());
}
#[test]
fn public_constructor_reserves_once_and_releases_reservation_when_dropped() {
    fn commit(path: &std::path::Path) -> (FeedBatchActor, CommittedBootstrap) {
        let actor = FeedBatchActor::spawn(
            FeedWriter::create_profile(path, 64 << 20, FeedProfile::EngineCausalV5).unwrap(),
            BatchConfig {
                max_pending_operations: 8,
                max_pending_bytes: 8 << 20,
                max_group_operations: 8,
                max_group_bytes: 8 << 20,
                max_age: Duration::from_millis(1),
            },
        )
        .unwrap();
        let committed = PreparedBootstrap::new_wire_history(fixture(true), MAX_OPERATION_BYTES)
            .unwrap()
            .try_admit(&actor.handle())
            .unwrap()
            .wait()
            .unwrap();
        (actor, committed)
    }
    let dir = tempfile::tempdir().unwrap();
    let state = crate::DaemonState::new(crate::DaemonConfig::from_toml_str("").unwrap());
    let budget = CollectorBudget::from_collector(state).unwrap();
    let (actor, bootstrap) = commit(&dir.path().join("first"));
    let (driver, _) = CausalHyperliquidDriver::new(bootstrap, budget.clone()).unwrap();
    let (actor2, bootstrap2) = commit(&dir.path().join("duplicate"));
    assert!(CausalHyperliquidDriver::new(bootstrap2, budget.clone()).is_err());
    actor2.shutdown().unwrap();
    drop(driver);
    actor.shutdown().unwrap();
    let (actor3, bootstrap3) = commit(&dir.path().join("after-drop"));
    let (driver3, _) = CausalHyperliquidDriver::new(bootstrap3, budget).unwrap();
    drop(driver3);
    actor3.shutdown().unwrap();
}

#[test]
fn managed_diagnostic_reuses_bounded_exact_error_or_capacity_hash() {
    let started = Instant::now();
    let mut counters = Counters::default();
    let text = "é".repeat(3000);
    counters.error(
        TransportStage::Read,
        TransportError::Io(text.clone()),
        started,
    );
    let diagnostic = counters.last_transport_error.as_ref().unwrap();
    assert_eq!(
        diagnostic.detail,
        WireErrorDto::CapacityExceeded {
            kind: WireErrorKindDto::Io,
            utf8_bytes: 6000,
            sha256: bootstrap_sha256(text.as_bytes())
        }
    );
    assert!(diagnostic.managed_unjournaled);
    assert!(diagnostic.observed_wall.is_some());
    counters.error(TransportStage::Close, TransportError::Closed, started);
    assert_eq!(counters.transport_error_count, 2);
    assert_eq!(
        counters.last_transport_error.unwrap().detail,
        WireErrorDto::Transport(TransportErrorDto::Closed)
    );
}
#[tokio::test]
async fn managed_close_failure_is_reported_without_forging_journal_wire_success() {
    let Fixture {
        _dir,
        actor,
        core,
        control,
        probe,
        input: _input,
        ..
    } = start(WriteMode::Success);
    let task = tokio::spawn(guarded(core));
    wait_for(|| control.shared.wire_completed.load(Ordering::Relaxed) == 1).await;
    *probe.close_error.lock().unwrap() = Some(TransportError::Io("managed close failed".into()));
    control.request_stop().unwrap();
    let report = task.await.unwrap();
    assert!(report.clean_stop, "semantic stop evidence: {report:?}");
    assert_eq!(report.transport_error_count, 1);
    let diagnostic = report.last_transport_error.unwrap();
    assert_eq!(diagnostic.stage, TransportStage::Close);
    assert!(diagnostic.managed_unjournaled);
    assert_eq!(
        diagnostic.detail,
        WireErrorDto::Transport(TransportErrorDto::Io("managed close failed".into()))
    );
    actor.shutdown().unwrap();
}

#[tokio::test]
async fn acquired_text_racing_stop_cannot_report_clean_semantic_completion() {
    let Fixture {
        _dir,
        actor,
        core,
        control,
        probe,
        input,
        topic,
    } = start(WriteMode::Success);
    let task = tokio::spawn(guarded(core));
    wait_for(|| control.shared.wire_completed.load(Ordering::Relaxed) == 1).await;
    *probe.read_hook.lock().unwrap() = Some(ReadHook::Stop(control.clone()));
    let ack = format!(
        r#"{{"channel":"subscriptionResponse","data":{{"method":"subscribe","subscription":{topic}}}}}"#
    );
    input
        .send(Ok(marketfeed_transport::InboundFrame {
            opcode: FrameOpcode::Text,
            payload: ack.into_bytes(),
        }))
        .await
        .unwrap();
    let report = task.await.unwrap();
    assert!(report.stop_committed && report.wire_clean, "{report:?}");
    assert!(
        !report.clean_stop,
        "acquired unadmitted Text must deny clean completion: {report:?}"
    );
    assert_eq!(report.unaccepted_frames, 1);
    assert_eq!(report.reason, ExitReason::IngressFailure);
    actor.shutdown().unwrap();
}

#[tokio::test]
async fn acquired_oversize_frame_is_counted_unaccepted_and_denies_clean_completion() {
    let Fixture {
        _dir,
        actor,
        core,
        control,
        probe,
        input,
        ..
    } = start(WriteMode::Success);
    let bytes = core.policy.max_frame_bytes as usize + 1;
    let task = tokio::spawn(guarded(core));
    wait_for(|| control.shared.wire_completed.load(Ordering::Relaxed) == 1).await;
    input
        .send(Ok(marketfeed_transport::InboundFrame {
            opcode: FrameOpcode::Binary,
            payload: vec![0; bytes],
        }))
        .await
        .unwrap();
    let report = task.await.unwrap();
    assert_eq!(report.unaccepted_frames, 1);
    assert_eq!(report.reason, ExitReason::IngressFailure);
    assert!(!report.clean_stop);
    assert_eq!(probe.writes.lock().unwrap().len(), 1);
    actor.shutdown().unwrap();
}
#[tokio::test]
async fn acquired_frame_facing_actual_ingress_full_is_counted_and_never_retried() {
    let Fixture {
        _dir,
        actor,
        core,
        control,
        probe,
        input,
        ..
    } = start(WriteMode::Success);
    let task = tokio::spawn(guarded(core));
    wait_for(|| control.shared.wire_completed.load(Ordering::Relaxed) == 1).await;
    *probe.read_hook.lock().unwrap() = Some(ReadHook::Fill(control.clone()));
    input
        .send(Ok(marketfeed_transport::InboundFrame {
            opcode: FrameOpcode::Text,
            payload: br#"{"channel":"pong"}"#.to_vec(),
        }))
        .await
        .unwrap();
    let report = task.await.unwrap();
    assert_eq!(report.unaccepted_frames, 1);
    assert_eq!(report.reason, ExitReason::IngressFailure);
    assert!(!report.clean_stop);
    assert_eq!(probe.writes.lock().unwrap().len(), 1);
    assert_eq!(report.status.owner.failure, Some(OwnerFailure::IngressFull));
    actor.shutdown().unwrap();
}

#[tokio::test]
async fn paused_native_source_backpressures_more_than_owner_capacity_and_resumes_exactly() {
    let _publication = super::super::native_publication_fixture_permit().await;
    const FRAMES: u64 = 32;
    let Fixture {
        _dir,
        actor,
        core,
        control,
        probe,
        input,
        topic,
    } = start_with_capacity(WriteMode::Success, FRAMES as usize + 8);
    let mut run = Box::pin(guarded(core));
    tokio::select! {
        report = &mut run => panic!("early driver exit: {report:?}"),
        _ = wait_for(|| probe.writes.lock().unwrap().len() == 1) => {}
    }
    // Per-owner RAII release is installed before any assertion/poll that can
    // panic. The actual worker pauses after Text acceptance, outside its lock.
    let pause = control.shared.ingress.pause_next_text_for_test();
    let ack = format!(
        r#"{{"channel":"subscriptionResponse","data":{{"method":"subscribe","subscription":{topic}}}}}"#
    );
    input
        .try_send(Ok(marketfeed_transport::InboundFrame {
            opcode: FrameOpcode::Text,
            payload: ack.into_bytes(),
        }))
        .unwrap();
    tokio::select! {
        report = &mut run => panic!("early driver exit: {report:?}"),
        _ = wait_for(|| pause.entered()) => {}
    }
    assert_eq!(probe.delivered.load(Ordering::SeqCst), 1);
    assert!(
        FRAMES > u64::from(fixture(true).owner_policy.unwrap().max_pending_operations),
        "ready frames must exceed the actual unchanged owner fixture cap"
    );
    for tid in 1..=FRAMES {
        let payload = format!(r#"{{"channel":"trades","data":[{{"coin":"@12","side":"B","px":"10","sz":"2","time":1,"tid":{tid},"hash":"h{tid}"}}]}}"#).into_bytes();
        input
            .try_send(Ok(marketfeed_transport::InboundFrame {
                opcode: FrameOpcode::Text,
                payload,
            }))
            .unwrap();
    }
    // Ready transport frames and an explicitly paused owner make this poll a
    // deterministic old-loop queue overflow, not a sleep/throughput guess.
    let polled = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::future::Future::poll(run.as_mut(), cx))
    })
    .await;
    assert!(polled.is_pending());
    assert_eq!(
        probe.delivered.load(Ordering::SeqCst),
        1,
        "transport must not acquire a second frame while native source is paused"
    );
    let paused = control.shared.ingress.status();
    assert!(paused.owner.failure.is_none(), "{paused:?}");
    assert_eq!(paused.owner.pending_operations, 1, "{paused:?}");
    let trade_pause = control.shared.ingress.pause_next_text_for_test();
    pause.release();
    tokio::select! {
        report = &mut run => panic!("early driver exit: {report:?}"),
        _ = wait_for(|| trade_pause.entered()) => {}
    }
    assert_eq!(probe.delivered.load(Ordering::SeqCst), 2);
    // Actual native worker barrier remains held while the unchanged timer
    // interval expires. Readiness is therefore deterministically overdue before
    // the first trade completes; this is not a throughput/scheduling threshold.
    tokio::time::sleep(Duration::from_millis(bounds().timer_interval_ms + 1)).await;
    trade_pause.release();
    tokio::select! {
        report = &mut run => panic!("early driver exit: {report:?}"),
        _ = async {
          let result = std::panic::AssertUnwindSafe(wait_for(|| {
            let status = control.shared.ingress.status();
            probe.delivered.load(Ordering::SeqCst) == FRAMES as usize + 1
                && status.owner.phase == OwnerPhase::Idle
                && status.owner.pending_operations == 0
                && status.owner.handed_off >= FRAMES + 2
          })).catch_unwind().await;
          if result.is_err() { panic!("resume watchdog: delivered={} status={:?}", probe.delivered.load(Ordering::SeqCst), control.shared.ingress.status()); }
        } => {}
    }
    control.request_stop().unwrap();
    let report = run.await;
    assert!(report.clean_stop, "{report:?}");
    assert_eq!(report.unaccepted_frames, 0);
    assert_eq!(report.inbound_text, FRAMES + 1);
    assert_eq!(report.status.owner.accepted, report.status.owner.executed);
    assert_eq!(report.status.owner.abandoned, 0);
    actor.shutdown().unwrap();
    let mut reader =
        FeedReader::open_profile(_dir.path().join("feed"), None, FeedProfile::EngineCausalV5)
            .unwrap();
    let mut tids = Vec::new();
    let mut timer_during_backlog = false;
    while let Some(record) = reader.read_next().unwrap() {
        let FeedRecord::Causal(envelope) = record else {
            continue;
        };
        if envelope.phase == CausalPhase::Bootstrap {
            continue;
        }
        let event: OwnerHistoryDto = serde_json::from_slice(&envelope.payload().unwrap()).unwrap();
        let OwnerEventDto::ExecutionCompleted { operation, .. } = event.event else {
            continue;
        };
        if matches!(operation.input, InputDto::TimerPoll { .. })
            && !tids.is_empty()
            && tids.len() < FRAMES as usize
        {
            timer_during_backlog = true;
        }
        for entry in operation.entries {
            let EffectDto::Invocation { actions, .. } = entry.effect else {
                continue;
            };
            for action in actions {
                let ActionDto::EmitObservedBatch(leaf) = action.action else {
                    continue;
                };
                for row in &leaf.batch().unwrap().native().rows {
                    let NativeObservation::HyperliquidTrade(trade) = &row.observation else {
                        continue;
                    };
                    assert_eq!(trade.coin, "@12");
                    assert_eq!(trade.side, HyperliquidTradeSide::Buy);
                    assert_eq!(trade.price, Fixed::parse_str("10").unwrap());
                    assert_eq!(trade.size, Fixed::parse_str("2").unwrap());
                    assert_eq!(trade.time_ms, 1);
                    assert_eq!(trade.hash, format!("h{}", trade.tid));
                    tids.push(trade.tid);
                }
            }
        }
    }
    assert_eq!(tids, (1..=FRAMES).collect::<Vec<_>>());
    assert!(
        timer_during_backlog,
        "overdue timer must execute between first and last queued native trades"
    );
    // Actual same-artifact native reexecution validates every source decision;
    // compare only committed/eligible release items, not merely callback DTOs.
    use crate::causal_bootstrap::replay::{MultiHistoryReplay, MultiReplayLimits, ReplayResources};
    let mut replay = MultiHistoryReplay::open_profile(
        _dir.path().join("feed"),
        MultiReplayLimits {
            per_owner: OwnerHistoryLimits {
                max_retained_input_bytes: 4 << 20,
                max_input_bytes: 8192,
                max_record_bytes: 256 << 10,
            },
            maximum: ReplayResources {
                owners: 1,
                bootstrap_payload_bytes: 8 << 20,
                pending_inputs: 64,
                pending_input_bytes: 4 << 20,
                active_operation_and_state_bytes: 4 << 20,
                source_owned_pending_inputs: 64,
                source_owned_pending_bytes: 4 << 20,
                capture_trace_reservation_bytes: 8 << 20,
                catalog_entries: 128,
                selected_instruments: 128,
                maintained_book_levels: 4096,
                cached_book_levels: 32768,
            },
        },
        FeedProfile::EngineCausalV5,
    )
    .unwrap();
    let mut released_tids = Vec::new();
    while let Some(step) = replay.read_next().unwrap() {
        if let Some(handoff) = step.handoff {
            for item in handoff.released_items() {
                let ItemDto::ObservedMarket(leaf) = item else {
                    continue;
                };
                for row in &leaf.batch().unwrap().native().rows {
                    if let NativeObservation::HyperliquidTrade(trade) = &row.observation {
                        released_tids.push(trade.tid);
                    }
                }
            }
        }
    }
    assert_eq!(released_tids, tids);
    let progress = replay.progress().unwrap();
    assert!(!progress.sealed, "test owner closure is not a storage Seal");
    assert_eq!(progress.owners.len(), 1);
    assert!(progress.owners[0].terminal);
    assert!(progress.owners[0].uncertainties.is_empty());
}

#[tokio::test]
async fn stop_fences_and_wakes_driver_while_actual_text_source_is_paused() {
    let Fixture {
        _dir,
        actor,
        core,
        control,
        probe,
        input,
        topic,
    } = start(WriteMode::Success);
    let mut run = Box::pin(guarded(core));
    tokio::select! {
        report = &mut run => panic!("early driver exit: {report:?}"),
        _ = wait_for(|| probe.writes.lock().unwrap().len() == 1) => {}
    }
    let pause = control.shared.ingress.pause_next_text_for_test();
    input.try_send(Ok(marketfeed_transport::InboundFrame {
        opcode: FrameOpcode::Text,
        payload: format!(r#"{{"channel":"subscriptionResponse","data":{{"method":"subscribe","subscription":{topic}}}}}"#).into_bytes(),
    })).unwrap();
    tokio::select! {
        report = &mut run => panic!("early driver exit: {report:?}"),
        _ = wait_for(|| pause.entered()) => {}
    }
    let text_id = control.shared.ingress.status().owner.accepted;
    let stop_id = control.request_stop().unwrap();
    assert_eq!(stop_id, text_id + 1);
    assert!(control.shared.ingress.status().owner.pending_operations >= 2);
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(std::future::Future::poll(
            run.as_mut(),
            cx
        )))
        .await
        .is_pending()
    );
    assert_eq!(probe.delivered.load(Ordering::SeqCst), 1);
    assert_eq!(
        probe.closes.load(Ordering::SeqCst),
        1,
        "Stop wake must reach managed close even though accepted work cannot join yet"
    );
    pause.release();
    let report = run.await;
    assert!(report.clean_stop && report.stop_committed, "{report:?}");
    assert_eq!(report.stop_input_id, Some(stop_id));
    assert_eq!(report.unaccepted_frames, 0);
    assert_eq!(report.inbound_text, 1);
    assert_eq!(report.status.owner.abandoned, 0);
    actor.shutdown().unwrap();
    let mut reader =
        FeedReader::open_profile(_dir.path().join("feed"), None, FeedProfile::EngineCausalV5)
            .unwrap();
    let mut ingress = false;
    let mut retired = false;
    while let Some(record) = reader.read_next().unwrap() {
        let FeedRecord::Causal(envelope) = record else {
            continue;
        };
        if envelope.phase == CausalPhase::Bootstrap {
            continue;
        }
        let dto: OwnerHistoryDto = serde_json::from_slice(&envelope.payload().unwrap()).unwrap();
        match dto.event {
            OwnerEventDto::Ingress {
                input_id,
                input: InputDto::Text { .. },
                ..
            } if input_id == text_id => ingress = true,
            OwnerEventDto::Withheld { input_id, .. } if input_id == text_id => retired = true,
            OwnerEventDto::HandoffObserved { input_id, .. } if input_id == text_id => {
                panic!("paused text gained source-visible authority after Stop")
            }
            _ => {}
        }
    }
    assert!(ingress && retired);
}
#[tokio::test]
async fn idle_socket_still_executes_overdue_timer_polls_without_acquiring_frames() {
    let Fixture {
        _dir,
        actor,
        core,
        control,
        probe,
        input: _input,
        ..
    } = start(WriteMode::Success);
    let mut run = Box::pin(guarded(core));
    tokio::select! {
        report = &mut run => panic!("early driver exit: {report:?}"),
        _ = wait_for(|| {
            let status = control.shared.ingress.status();
            status.owner.executed >= 3 && status.owner.handed_off >= 3
        }) => {}
    }
    assert_eq!(probe.delivered.load(Ordering::SeqCst), 0);
    control.request_stop().unwrap();
    let report = run.await;
    assert!(report.clean_stop, "{report:?}");
    assert_eq!(report.inbound_text, 0);
    assert_eq!(report.native_controls, 0);
    actor.shutdown().unwrap();
}

async fn idle_timer_operations(driver_policy: DriverPolicy) -> Vec<OperationDto> {
    let Fixture {
        _dir,
        actor,
        core,
        control,
        probe,
        input: _input,
        ..
    } = start_with_policy(WriteMode::Success, 16, driver_policy);
    let task = tokio::spawn(guarded(core));
    wait_for(|| {
        // read_frame is polled only after complete owner/wire quiescence.
        control.shared.ingress.status().owner.handed_off >= 1
            && probe.writes.lock().unwrap().len() == 1
            && probe.reads_started.load(Ordering::SeqCst) > 0
    })
    .await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    control.request_stop().unwrap();
    let report = task.await.unwrap();
    assert!(report.clean_stop, "{report:?}");
    actor.shutdown().unwrap();
    let mut reader =
        FeedReader::open_profile(_dir.path().join("feed"), None, FeedProfile::EngineCausalV5)
            .unwrap();
    let mut polls = Vec::new();
    while let Some(record) = reader.read_next().unwrap() {
        let FeedRecord::Causal(envelope) = record else {
            continue;
        };
        if envelope.phase != CausalPhase::Operation {
            continue;
        }
        let dto: OwnerHistoryDto = serde_json::from_slice(&envelope.payload().unwrap()).unwrap();
        if let OwnerEventDto::ExecutionCompleted { operation, .. } = dto.event {
            if matches!(operation.input, InputDto::TimerPoll { .. }) {
                polls.push(*operation);
            }
        }
    }
    polls
}
#[tokio::test]
async fn due_only_idle_driver_does_not_journal_no_due_timer_polls() {
    // Same startup/wire-quiescence gate and250ms window as the retained legacy
    // red. Version2 is explicitly admitted before factory/owner construction.
    assert!(idle_timer_operations(due_bounds()).await.is_empty());
}
#[tokio::test]
async fn legacy_policy_keeps_actual_periodic_empty_timer_turns() {
    let polls = idle_timer_operations(bounds()).await;
    assert!(!polls.is_empty());
    for operation in polls {
        assert_eq!(operation.before, operation.after);
        assert!(
            operation
                .entries
                .iter()
                .all(|e| !matches!(e.effect, EffectDto::Invocation { .. }))
        );
    }
}

#[test]
fn committed_timer_schedule_replaces_cancels_and_keeps_signed_due_deadlines() {
    let mut schedule = CommittedTimerSchedule::default();
    assert!(!schedule.due(TimestampNs(i64::MAX)), "unknown startup");
    schedule.replace(&[(1, 100), (2, 50)]);
    assert!(!schedule.due(TimestampNs(49)));
    assert!(schedule.due(TimestampNs(50)));
    // A complete new committed schedule postpones the previously earliest ID.
    schedule.replace(&[(1, 100), (2, 200)]);
    assert!(!schedule.due(TimestampNs(50)));
    assert!(schedule.due(TimestampNs(100)));
    // Cancellation removes old IDs; this is replacement, never an accumulating
    // cache. A still-due reschedule remains eligible for another actual poll,
    // including after the engine's64-callback-per-poll ceiling.
    schedule.replace(&[(2, 40)]);
    assert!(schedule.due(TimestampNs(100)));
    schedule.replace(&[]);
    assert!(!schedule.due(TimestampNs(i64::MAX)));
    schedule.replace(&[(1, i64::MAX)]);
    assert!(!schedule.due(TimestampNs(i64::MIN)));
    assert!(schedule.due(TimestampNs(i64::MAX)));
    schedule.replace(&[(1, i64::MIN)]);
    assert!(schedule.due(TimestampNs(i64::MIN)));
    assert!(schedule.due(TimestampNs(i64::MAX)));
}

#[test]
fn due_only_policy_is_explicitly_committed_and_legacy_bytes_stay_distinct() {
    let legacy = fixture(true);
    let legacy_bytes = serde_json::to_vec(&bounds()).unwrap();
    assert_eq!(policy(&legacy).unwrap().schema_version, 1);
    let mut due = legacy.clone();
    due.public_inputs.retain(|p| p.role != DRIVER_POLICY_ROLE);
    due_bounds().bind(&mut due).unwrap();
    assert_eq!(policy(&due).unwrap(), due_bounds());
    assert_ne!(serde_json::to_vec(&due_bounds()).unwrap(), legacy_bytes);
    assert_ne!(
        encode_bootstrap_for_profile(&legacy, MAX_OPERATION_BYTES, FeedProfile::EngineCausalV5)
            .unwrap(),
        encode_bootstrap_for_profile(&due, MAX_OPERATION_BYTES, FeedProfile::EngineCausalV5)
            .unwrap()
    );
    let mut unknown = due_bounds();
    unknown.schema_version = 3;
    assert!(unknown.validate(&legacy).is_err());
    let mut unsupported = legacy.clone();
    unsupported
        .public_inputs
        .retain(|p| p.role != DRIVER_POLICY_ROLE);
    unsupported
        .public_inputs
        .push(PublicReproductionInputDto::new(
            DRIVER_POLICY_ROLE,
            &serde_json::to_vec(&unknown).unwrap(),
        ));
    assert!(PreparedBootstrap::new_wire_history(unsupported, MAX_OPERATION_BYTES).is_err());
}

fn native_operations(directory: &std::path::Path) -> Vec<OperationDto> {
    let mut reader =
        FeedReader::open_profile(directory.join("feed"), None, FeedProfile::EngineCausalV5)
            .unwrap();
    let mut out = Vec::new();
    while let Some(record) = reader.read_next().unwrap() {
        let FeedRecord::Causal(envelope) = record else {
            continue;
        };
        if envelope.phase != CausalPhase::Operation {
            continue;
        }
        let dto: OwnerHistoryDto = serde_json::from_slice(&envelope.payload().unwrap()).unwrap();
        if let OwnerEventDto::ExecutionCompleted { operation, .. } = dto.event {
            out.push(*operation);
        }
    }
    out
}

#[tokio::test]
async fn due_only_actual_callback_reschedules_and_does_not_starve_ready_frames_or_stop() {
    let Fixture {
        _dir,
        actor,
        mut core,
        control,
        probe,
        input,
        topic,
    } = start_with_policy(WriteMode::Success, 16, due_bounds());
    let clock = Arc::new(std::sync::atomic::AtomicI64::new(0));
    core.wall_clock = Some(clock.clone());
    let task = tokio::spawn(guarded(core));
    wait_for(|| probe.reads_started.load(Ordering::SeqCst) > 0).await;
    let connected = native_operations(_dir.path());
    let deadline = connected
        .iter()
        .find(|op| matches!(op.input, InputDto::Connected { .. }))
        .unwrap()
        .after
        .timers
        .iter()
        .map(|(_, deadline)| *deadline)
        .min()
        .expect("actual connected callback scheduled a native timer");
    assert!(deadline > 0);
    input
        .send(Ok(marketfeed_transport::InboundFrame {
            opcode: FrameOpcode::Text,
            payload: format!(r#"{{"channel":"subscriptionResponse","data":{{"method":"subscribe","subscription":{topic}}}}}"#).into_bytes(),
        }))
        .await
        .unwrap();
    wait_for(|| {
        let s = control.shared.ingress.status();
        s.owner.executed == 2 && s.owner.handed_off == 2 && s.owner.phase == OwnerPhase::Idle
    })
    .await;
    clock.store(deadline - 1, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(control.shared.ingress.status().owner.executed, 2);
    // Pause the first accepted frame outside the owner lock. This proves the
    // local wake is already overdue before releasing the owner into simultaneous
    // ready-frame/timer choices, independently of filesystem speed.
    let pause = control.shared.ingress.pause_next_text_for_test();
    input
        .try_send(Ok(marketfeed_transport::InboundFrame {
            opcode: FrameOpcode::Text,
            payload: br#"{"channel":"pong"}"#.to_vec(),
        }))
        .unwrap();
    wait_for(|| pause.entered()).await;
    for _ in 0..7 {
        input
            .try_send(Ok(marketfeed_transport::InboundFrame {
                opcode: FrameOpcode::Text,
                payload: br#"{"channel":"pong"}"#.to_vec(),
            }))
            .unwrap();
    }
    clock.store(deadline, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(bounds().timer_interval_ms + 1)).await;
    pause.release();
    wait_for(|| {
        probe.delivered.load(Ordering::SeqCst) == 9
            && probe.writes.lock().unwrap().len() == 2
            && control.shared.ingress.status().owner.phase == OwnerPhase::Idle
    })
    .await;
    assert_eq!(
        probe.writes.lock().unwrap()[1].payload,
        br#"{"method":"ping"}"#
    );
    // The actual callback replaces its timer with a future deadline. Further
    // unchanged local ticks cannot turn that stale cached deadline into polls.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let stop_id = control.request_stop().unwrap();
    let report = task.await.unwrap();
    assert!(report.clean_stop && report.stop_committed, "{report:?}");
    assert_eq!(report.stop_input_id, Some(stop_id));
    assert_eq!(report.unaccepted_frames, 0);
    actor.shutdown().unwrap();
    let ops = native_operations(_dir.path());
    let timers = ops
        .iter()
        .filter(|op| matches!(op.input, InputDto::TimerPoll { .. }))
        .collect::<Vec<_>>();
    assert_eq!(timers.len(), 1, "only the actual due poll is journaled");
    let timer = timers[0];
    assert!(timer.entries.iter().any(|e| matches!(e.effect, EffectDto::Invocation { kind: InvocationDto::Timer { now, .. }, .. } if now == deadline)));
    assert!(timer.after.timers.iter().all(|(_, next)| *next > deadline));
    assert!(
        ops.iter().any(|op| op.route.ordinal > timer.route.ordinal
            && matches!(op.input, InputDto::Text { .. })),
        "a ready frame must run after the actual timer"
    );
    // Every physical record is checked by the real persistent native replayer,
    // including the committed policy2 bootstrap, exact timer callback/state,
    // application-wire decisions and terminal withheld Stop. This is not just
    // serde/physical inspection of the expected OperationDto.
    use crate::causal_bootstrap::replay::{MultiHistoryReplay, VerifiedSourceRecord};
    let mut replay = MultiHistoryReplay::open_profile(
        _dir.path().join("feed"),
        crate::causal_bootstrap::capture::replay_limits(),
        FeedProfile::EngineCausalV5,
    )
    .unwrap();
    let mut physical = 0;
    let mut completions = 0;
    let mut due_completions = 0;
    let mut successful_wires = 0;
    let mut handoffs = 0;
    while let Some(step) = replay.read_next_verified().unwrap() {
        physical += 1;
        handoffs += usize::from(step.handoff().is_some());
        match step.record() {
            VerifiedSourceRecord::Bootstrap(dto) => {
                assert_eq!(policy(dto).unwrap().schema_version, 2);
            }
            VerifiedSourceRecord::Owner { observation, .. } => match &observation.event {
                OwnerEventDto::ExecutionCompleted { operation, .. } => {
                    completions += 1;
                    if matches!(operation.input, InputDto::TimerPoll { .. }) {
                        assert_eq!(**operation, *timer);
                        due_completions += 1;
                    }
                }
                OwnerEventDto::Wire(WireEventDto::Settled { outcome, .. }) => {
                    assert_eq!(*outcome, WireOutcomeDto::Succeeded);
                    successful_wires += 1;
                }
                _ => {}
            },
            VerifiedSourceRecord::Seal { .. } => panic!("owner shutdown does not fabricate Seal"),
        }
    }
    let progress = replay.progress().unwrap();
    assert_eq!(physical, progress.cursor.sequence);
    assert_eq!(completions, ops.len());
    assert_eq!(due_completions, 1);
    assert_eq!(successful_wires, 2);
    assert_eq!(handoffs as u64, report.handoffs);
    assert_eq!(progress.owners.len(), 1);
    assert!(progress.owners[0].terminal);
    assert!(progress.owners[0].uncertainties.is_empty());
    assert!(!progress.sealed);
}
