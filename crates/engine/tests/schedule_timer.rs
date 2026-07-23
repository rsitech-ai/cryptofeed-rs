//! Offline: ScheduleTimer registers, fires as SessionInput::Timer, drives ping path.

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, SessionAction, SessionInput, SessionMachine, TimerSpec,
};
use marketfeed_engine::{SessionRunner, SessionRunnerConfig};
use marketfeed_model::{SessionId, TimestampNs};
use marketfeed_transport::FrameOpcode;

const PING_TIMER_ID: u64 = 1;
const PING_INTERVAL_NS: i64 = 100;

/// Minimal machine mirroring OKX/Bybit app-ping: schedule on connect, ping+reschedule on fire.
struct PingTimerMachine {
    pings: u32,
}

impl SessionMachine for PingTimerMachine {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Connected { now } => {
                output.push(SessionAction::ScheduleTimer(TimerSpec {
                    timer_id: PING_TIMER_ID,
                    fire_at: TimestampNs(now.0 + PING_INTERVAL_NS),
                }));
                Ok(())
            }
            SessionInput::Timer { timer_id, now } if timer_id == PING_TIMER_ID => {
                self.pings += 1;
                output.push(SessionAction::SendText(Bytes::from_static(b"ping")));
                output.push(SessionAction::ScheduleTimer(TimerSpec {
                    timer_id: PING_TIMER_ID,
                    fire_at: TimestampNs(now.0 + PING_INTERVAL_NS),
                }));
                Ok(())
            }
            SessionInput::Disconnected { .. } => {
                output.push(SessionAction::CancelTimer(PING_TIMER_ID));
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

fn runner() -> SessionRunner {
    SessionRunner::new(
        Box::new(PingTimerMachine { pings: 0 }),
        SessionRunnerConfig {
            session: SessionId(1),
            record: false,
            ..SessionRunnerConfig::default()
        },
    )
    .expect("runner")
}

#[test]
fn schedule_timer_fires_ping_and_reschedules() {
    let mut runner = runner();
    runner.on_connected(TimestampNs(0)).unwrap();
    assert_eq!(runner.timer_count(), 1);
    assert!(runner.take_pending_writes().is_empty());

    // Not due yet.
    runner.poll_timers(TimestampNs(50)).unwrap();
    assert_eq!(runner.timer_count(), 1);
    assert!(runner.take_pending_writes().is_empty());

    // Due: deliver Timer → SendText("ping") + reschedule.
    runner.poll_timers(TimestampNs(100)).unwrap();
    let writes = runner.take_pending_writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].opcode, FrameOpcode::Text);
    assert_eq!(writes[0].payload, b"ping");
    assert_eq!(runner.timer_count(), 1);

    // Second fire at 200.
    runner.poll_timers(TimestampNs(200)).unwrap();
    let writes = runner.take_pending_writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].payload, b"ping");
}

#[test]
fn schedule_timer_same_id_replaces_deadline() {
    struct ReplaceMachine;

    impl SessionMachine for ReplaceMachine {
        fn on_input(
            &mut self,
            input: SessionInput<'_>,
            output: &mut ActionBuffer,
        ) -> Result<(), AdapterError> {
            match input {
                SessionInput::Connected { now } => {
                    output.push(SessionAction::ScheduleTimer(TimerSpec {
                        timer_id: 1,
                        fire_at: TimestampNs(now.0 + 50),
                    }));
                    Ok(())
                }
                SessionInput::TextFrame { .. } => {
                    // Same id, later deadline — must replace, not stack.
                    output.push(SessionAction::ScheduleTimer(TimerSpec {
                        timer_id: 1,
                        fire_at: TimestampNs(500),
                    }));
                    Ok(())
                }
                SessionInput::Timer { .. } => {
                    output.push(SessionAction::SendText(Bytes::from_static(b"fired")));
                    Ok(())
                }
                _ => Ok(()),
            }
        }
    }

    let mut runner = SessionRunner::new(
        Box::new(ReplaceMachine),
        SessionRunnerConfig {
            session: SessionId(3),
            record: false,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();

    runner.on_connected(TimestampNs(0)).unwrap();
    assert_eq!(runner.next_timer_deadline(), Some(TimestampNs(50)));
    assert_eq!(runner.timer_count(), 1);

    let mut bytes = b"bump".to_vec();
    runner
        .on_text_frame(
            &mut bytes,
            marketfeed_model::FrameStamp {
                receive_ts: TimestampNs(1),
                mono_ns: 1,
            },
        )
        .unwrap();
    assert_eq!(runner.timer_count(), 1);
    assert_eq!(runner.next_timer_deadline(), Some(TimestampNs(500)));

    runner.poll_timers(TimestampNs(50)).unwrap();
    assert!(runner.take_pending_writes().is_empty());
    runner.poll_timers(TimestampNs(500)).unwrap();
    assert_eq!(runner.take_pending_writes().len(), 1);
}

#[test]
fn cancel_timer_prevents_fire() {
    struct CancelOnSub;

    impl SessionMachine for CancelOnSub {
        fn on_input(
            &mut self,
            input: SessionInput<'_>,
            output: &mut ActionBuffer,
        ) -> Result<(), AdapterError> {
            match input {
                SessionInput::Connected { now } => {
                    output.push(SessionAction::ScheduleTimer(TimerSpec {
                        timer_id: 7,
                        fire_at: TimestampNs(now.0 + 50),
                    }));
                    Ok(())
                }
                SessionInput::TextFrame { .. } => {
                    output.push(SessionAction::CancelTimer(7));
                    Ok(())
                }
                SessionInput::Timer { .. } => {
                    output.push(SessionAction::SendText(Bytes::from_static(
                        b"should-not-fire",
                    )));
                    Ok(())
                }
                _ => Ok(()),
            }
        }
    }

    let mut runner = SessionRunner::new(
        Box::new(CancelOnSub),
        SessionRunnerConfig {
            session: SessionId(2),
            record: false,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();

    runner.on_connected(TimestampNs(0)).unwrap();
    assert_eq!(runner.timer_count(), 1);
    let mut bytes = b"cancel".to_vec();
    runner
        .on_text_frame(
            &mut bytes,
            marketfeed_model::FrameStamp {
                receive_ts: TimestampNs(1),
                mono_ns: 1,
            },
        )
        .unwrap();
    assert_eq!(runner.timer_count(), 0);
    runner.poll_timers(TimestampNs(50)).unwrap();
    assert!(runner.take_pending_writes().is_empty());
}
