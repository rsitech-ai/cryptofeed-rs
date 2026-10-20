//! Offline: ScheduleTimer registers, fires as SessionInput::Timer, drives ping path.

use bytes::Bytes;
use marketfeed_adapter_api::{
    ActionBuffer, AdapterError, SessionAction, SessionInput, SessionMachine, TimerSpec,
};
use marketfeed_engine::{SessionRunner, SessionRunnerConfig};
use marketfeed_model::{SessionId, TimestampNs};
use marketfeed_transport::FrameOpcode;
use std::time::Duration;

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

enum SecondTimerChange {
    Reschedule(i64),
    Cancel,
}

struct DeadlineMachine {
    deadlines: Vec<(u64, i64)>,
    second_change: Option<SecondTimerChange>,
    repeat: bool,
}

impl SessionMachine for DeadlineMachine {
    fn on_input(
        &mut self,
        input: SessionInput<'_>,
        output: &mut ActionBuffer,
    ) -> Result<(), AdapterError> {
        match input {
            SessionInput::Connected { .. } => {
                for &(timer_id, deadline) in &self.deadlines {
                    output.push(SessionAction::ScheduleTimer(TimerSpec {
                        timer_id,
                        fire_at: TimestampNs(deadline),
                    }));
                }
            }
            SessionInput::Timer { timer_id, .. } => {
                output.push(SessionAction::SendText(timer_id.to_string().into()));
                if timer_id == 1 {
                    match self.second_change.take() {
                        Some(SecondTimerChange::Reschedule(deadline)) => {
                            output.push(SessionAction::ScheduleTimer(TimerSpec {
                                timer_id: 2,
                                fire_at: TimestampNs(deadline),
                            }));
                        }
                        Some(SecondTimerChange::Cancel) => {
                            output.push(SessionAction::CancelTimer(2));
                        }
                        None => {}
                    }
                }
                if self.repeat {
                    output.push(SessionAction::ScheduleTimer(TimerSpec {
                        timer_id,
                        fire_at: TimestampNs(0),
                    }));
                }
            }
            _ => {}
        }
        Ok(())
    }
}

fn deadline_runner(
    deadlines: Vec<(u64, i64)>,
    second_change: Option<SecondTimerChange>,
    repeat: bool,
) -> SessionRunner {
    SessionRunner::new(
        Box::new(DeadlineMachine {
            deadlines,
            second_change,
            repeat,
        }),
        SessionRunnerConfig {
            record: false,
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap()
}

#[test]
fn timer_delay_is_zero_when_due_or_overdue_and_exact_when_future() {
    let mut runner = deadline_runner(vec![(1, 100)], None, false);
    assert_eq!(runner.delay_until_next_timer(TimestampNs(0)), None);
    runner.on_connected(TimestampNs(0)).unwrap();
    for (now, nanos) in [(50, 50), (100, 0), (101, 0), (i64::MAX, 0)] {
        assert_eq!(
            runner.delay_until_next_timer(TimestampNs(now)),
            Some(Duration::from_nanos(nanos)),
            "now={now}"
        );
    }
    // A backward clock jump postpones the timer without wrapping its delay.
    assert_eq!(
        runner.delay_until_next_timer(TimestampNs(-100)),
        Some(Duration::from_nanos(200))
    );
    runner.poll_timers(TimestampNs(101)).unwrap();
    assert_eq!(runner.take_pending_writes().len(), 1);
    assert_eq!(runner.delay_until_next_timer(TimestampNs(101)), None);
}

#[test]
fn timer_delay_handles_the_entire_signed_timestamp_range() {
    let mut future = deadline_runner(vec![(1, i64::MAX)], None, false);
    future.on_connected(TimestampNs(0)).unwrap();
    assert_eq!(
        future.delay_until_next_timer(TimestampNs(i64::MIN)),
        Some(Duration::from_nanos(u64::MAX))
    );
    let mut overdue = deadline_runner(vec![(1, i64::MIN)], None, false);
    overdue.on_connected(TimestampNs(0)).unwrap();
    assert_eq!(
        overdue.delay_until_next_timer(TimestampNs(i64::MAX)),
        Some(Duration::ZERO)
    );
}

#[test]
fn callback_can_postpone_another_due_timer_without_firing_it_early() {
    use marketfeed_recording::shared_feed::{FeedHandle, FeedReader, FeedRecord, FeedWriter};
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("timers");
    let feed = FeedHandle::new(FeedWriter::create(&dir, 1_000_000).unwrap());
    let mut runner = SessionRunner::new(
        Box::new(DeadlineMachine {
            deadlines: vec![(2, 100), (1, 100)],
            second_change: Some(SecondTimerChange::Reschedule(500)),
            repeat: false,
        }),
        SessionRunnerConfig {
            record: false,
            shared_feed: Some(feed),
            ..SessionRunnerConfig::default()
        },
    )
    .unwrap();
    runner.on_connected(TimestampNs(0)).unwrap();
    runner.poll_timers(TimestampNs(100)).unwrap();
    let writes = runner.take_pending_writes();
    assert_eq!(writes.len(), 1, "postponed timer must not fire at 100");
    assert_eq!(writes[0].payload, b"1");
    assert_eq!(runner.next_timer_deadline(), Some(TimestampNs(500)));
    runner.poll_timers(TimestampNs(499)).unwrap();
    assert!(runner.take_pending_writes().is_empty());
    runner.poll_timers(TimestampNs(500)).unwrap();
    let writes = runner.take_pending_writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].payload, b"2");
    assert_eq!(runner.timer_count(), 0);
    let mut reader = FeedReader::open(&dir, None).unwrap();
    let mut firings = Vec::new();
    while let Some(record) = reader.read_next().unwrap() {
        if let FeedRecord::Timer {
            session,
            timer_id,
            now_ns,
        } = record
        {
            firings.push((session, timer_id, now_ns));
        }
    }
    assert_eq!(
        firings,
        vec![(SessionId(1), 1, 100), (SessionId(1), 2, 500)],
        "the shared feed must preserve the corrected firing chronology"
    );
}

#[test]
fn callback_can_replace_another_due_timer_with_an_already_due_deadline() {
    let mut runner = deadline_runner(
        vec![(2, 100), (1, 100)],
        Some(SecondTimerChange::Reschedule(99)),
        false,
    );
    runner.on_connected(TimestampNs(0)).unwrap();
    runner.poll_timers(TimestampNs(100)).unwrap();
    let writes = runner.take_pending_writes();
    assert_eq!(writes.len(), 2);
    assert_eq!(writes[0].payload, b"1");
    assert_eq!(writes[1].payload, b"2");
    assert_eq!(runner.timer_count(), 0);
}

#[test]
fn callback_can_cancel_another_due_timer() {
    let mut runner = deadline_runner(
        vec![(2, 100), (1, 100)],
        Some(SecondTimerChange::Cancel),
        false,
    );
    runner.on_connected(TimestampNs(0)).unwrap();
    runner.poll_timers(TimestampNs(100)).unwrap();
    let writes = runner.take_pending_writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].payload, b"1");
    assert_eq!(runner.timer_count(), 0);
    runner.poll_timers(TimestampNs(500)).unwrap();
    assert!(runner.take_pending_writes().is_empty());
}

#[test]
fn overdue_reschedule_is_bounded_and_remains_immediately_pollable() {
    let mut runner = deadline_runner(vec![(1, 0)], None, true);
    runner.on_connected(TimestampNs(0)).unwrap();
    for _ in 0..2 {
        runner.poll_timers(TimestampNs(1)).unwrap();
        assert_eq!(runner.take_pending_writes().len(), 64);
        assert_eq!(runner.timer_count(), 1);
        assert_eq!(
            runner.delay_until_next_timer(TimestampNs(1)),
            Some(Duration::ZERO)
        );
    }
}
