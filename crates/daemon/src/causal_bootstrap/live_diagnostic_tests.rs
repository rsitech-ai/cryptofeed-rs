//! Additional diagnostics tests. Existing driver fixtures/cases are unchanged.
use super::super::diagnostics::{TimingCounters, observed_read};
use super::*;

#[tokio::test]
async fn inner_read_unwind_is_one_dropped_polled_future_without_a_return_outcome() {
    let mut timing = TimingCounters::default();
    let read = std::future::poll_fn::<Result<marketfeed_transport::InboundFrame, TransportError>, _>(
        |_| panic!("declared inner read panic"),
    );
    let mut future = Box::pin(observed_read(read, &mut timing));
    let caught = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || std::future::Future::poll(future.as_mut(), cx),
        )))
    })
    .await;
    assert!(caught.is_err());
    drop(future);
    assert_eq!(timing.data.read_calls.polled_future_dropped.intervals, 1);
    assert_eq!(timing.data.read_calls.frames.intervals, 0);
    assert_eq!(timing.data.read_calls.errors.intervals, 0);
    assert_eq!(timing.data.read_calls.returned_timeouts.intervals, 0);
    // These counters survive only because the enclosing test still owns them;
    // dropping an entire driver does not promise a terminal diagnostic report.
}

#[tokio::test]
async fn read_diagnostics_count_first_poll_drop_and_returned_outcomes_exactly_once() {
    let probe = Arc::new(Probe::default());
    let (input, inbound) = mpsc::channel(4);
    let mut transport = TestTransport {
        probe: probe.clone(),
        inbound,
        mode: WriteMode::Success,
    };
    let mut buffer = FrameBuffer::with_capacity(64);
    let mut timing = TimingCounters::default();
    drop(observed_read(
        transport.read_frame(&mut buffer),
        &mut timing,
    ));
    assert_eq!(probe.reads_started.load(Ordering::SeqCst), 0);
    assert_eq!(timing.data, ReadDiagnostics::default());
    let mut pending = Box::pin(observed_read(
        transport.read_frame(&mut buffer),
        &mut timing,
    ));
    let polled = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::future::Future::poll(pending.as_mut(), cx))
    })
    .await;
    assert!(polled.is_pending());
    assert_eq!(probe.reads_started.load(Ordering::SeqCst), 1);
    drop(pending);
    assert_eq!(timing.data.read_calls.polled_future_dropped.intervals, 1);
    for result in [
        Ok(marketfeed_transport::InboundFrame {
            opcode: FrameOpcode::Text,
            payload: b"original".to_vec(),
        }),
        Err(TransportError::Closed),
        Err(TransportError::Timeout),
    ] {
        input.try_send(result).unwrap();
        let _ = observed_read(transport.read_frame(&mut buffer), &mut timing).await;
    }
    assert_eq!(probe.reads_started.load(Ordering::SeqCst), 4);
    assert_eq!(probe.delivered.load(Ordering::SeqCst), 3);
    assert_eq!(timing.data.read_calls.frames.intervals, 1);
    assert_eq!(timing.data.read_calls.errors.intervals, 1);
    assert_eq!(timing.data.read_calls.returned_timeouts.intervals, 1);
    assert_eq!(timing.data.read_calls.polled_future_dropped.intervals, 1);
    assert!(!timing.data.saturated);
}

#[tokio::test]
async fn buffered_frames_wait_for_actual_owner_and_report_application_read_timing() {
    // Existing fixture resource permit is acquired before construction/watchdog.
    let _publication = super::super::super::native_publication_fixture_permit().await;
    let Fixture {
        _dir,
        actor,
        mut core,
        control,
        probe,
        input,
        topic,
    } = start_with_policy(WriteMode::Success, 16, due_bounds());
    let clock = Arc::new(std::sync::atomic::AtomicI64::new(1_000));
    core.wall_clock = Some(clock.clone());
    let mut run = Box::pin(guarded(core));
    tokio::select! {
        report = &mut run => panic!("early driver exit: {report:?}"),
        _ = wait_for(|| probe.writes.lock().unwrap().len() == 1) => {}
    }
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
    let trade = br#"{"channel":"trades","data":[{"coin":"@12","side":"B","px":"10","sz":"2","time":1,"tid":1,"hash":"h1"}]}"#.to_vec();
    input
        .try_send(Ok(marketfeed_transport::InboundFrame {
            opcode: FrameOpcode::Text,
            payload: trade.clone(),
        }))
        .unwrap();
    let polled = std::future::poll_fn(|cx| {
        std::task::Poll::Ready(std::future::Future::poll(run.as_mut(), cx))
    })
    .await;
    assert!(polled.is_pending());
    assert_eq!(probe.delivered.load(Ordering::SeqCst), 1);
    // Wall timestamp seam is deliberately NOT the monotonic diagnostic clock.
    clock.store(1_000_000, Ordering::SeqCst);
    pause.release();
    tokio::select! {
        report = &mut run => panic!("early driver exit: {report:?}"),
        _ = wait_for(|| {
            let s = control.shared.ingress.status();
            probe.delivered.load(Ordering::SeqCst) == 2
                && s.owner.phase == OwnerPhase::Idle && s.owner.handed_off >= 3
        }) => {}
    }
    control.request_stop().unwrap();
    let report = run.await;
    assert!(report.clean_stop, "{report:?}");
    assert_eq!(report.inbound_text, 2);
    assert_eq!(report.unaccepted_frames, 0);
    let timing = &report.read_diagnostics;
    assert_eq!(timing.diagnostics_version, 1);
    assert_eq!(timing.read_calls.frames.intervals, 2);
    assert_eq!(timing.read_calls.errors.intervals, 0);
    assert_eq!(timing.read_calls.returned_timeouts.intervals, 0);
    assert!(
        timing
            .gate_wait
            .idle_pending_inputs_or_observations
            .intervals
            > 0
    );
    assert!(timing.observed_gate_runs.intervals > 0);
    assert!(timing.observed_gate_runs.total_ns > 0);
    assert!(!timing.saturated);
    // Exercise the actual production terminal-report formatter, not a copy.
    let json = super::super::driver_report_value(&HyperliquidMarketDto::Spot, &report);
    assert_eq!(json["inbound_text"], 2);
    assert_eq!(json["clean_stop"], true);
    assert_eq!(json["read_diagnostics"]["diagnostics_version"], 1);
    assert_eq!(
        json["read_diagnostics"]["read_calls"]["frames"]["intervals"],
        2
    );
    assert!(
        json["read_diagnostics_scope"]
            .as_str()
            .unwrap()
            .contains("non-additive")
    );
    actor.shutdown().unwrap();
    let operations = native_operations(_dir.path());
    let text = operations
        .iter()
        .filter_map(|op| match &op.input {
            InputDto::Text { payload, received } => {
                Some((payload.bytes().unwrap(), received.receive_ts.0))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(text.len(), 2);
    assert_eq!(text[0].1, 1_000);
    assert_eq!(text[1], (trade, 1_000_000));
}
