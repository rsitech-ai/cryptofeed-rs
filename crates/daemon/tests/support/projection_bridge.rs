//! Actual committed native fixture -> real read-only Rust bridge subprocess.
use super::*;
use marketfeed_engine::causal::projection::ProjectionReader;
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
    sync::mpsc,
};

struct Bridge {
    child: Child,
    input: Option<ChildStdin>,
    output: mpsc::Receiver<String>,
    reader: Option<thread::JoinHandle<()>>,
}
impl Bridge {
    fn spawn() -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_marketfeed-observations"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let input = child.stdin.take();
        let stdout = child.stdout.take().unwrap();
        let (tx, output) = mpsc::channel();
        let reader = thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            input,
            output,
            reader: Some(reader),
        }
    }
    fn send(&mut self, request: Value) -> Value {
        serde_json::to_writer(self.input.as_mut().unwrap(), &request).unwrap();
        self.input.as_mut().unwrap().write_all(b"\n").unwrap();
        self.input.as_mut().unwrap().flush().unwrap();
        let raw = self.output.recv_timeout(WAIT).unwrap();
        serde_json::from_str(&raw).unwrap()
    }
    fn request(&mut self, request: Value) {
        serde_json::to_writer(self.input.as_mut().unwrap(), &request).unwrap();
        self.input.as_mut().unwrap().write_all(b"\n").unwrap();
        self.input.as_mut().unwrap().flush().unwrap();
    }
    fn exit(&mut self) -> std::process::ExitStatus {
        self.input.take();
        let mut status = None;
        wait(|| {
            status = self.child.try_wait().unwrap();
            status.is_some()
        });
        status.unwrap()
    }
}
impl Drop for Bridge {
    fn drop(&mut self) {
        self.input.take();
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}
fn fixture(
    spot: bool,
) -> (
    tempfile::TempDir,
    PathBuf,
    Vec<marketfeed_engine::causal::projection::ProjectionGroup>,
) {
    let (temp, _, _) = native_history_tests::recorded(spot);
    let target = temp.path().join("bridge-target");
    projection_tests::project(&temp.path().join("original"), &target);
    let groups = projection_tests::groups(&target);
    (temp, target, groups)
}
fn open(bridge: &mut Bridge, path: &Path, resume: Value) -> Value {
    bridge.send(json!({"schema_version":1,"open":{"directory":path,"resume":resume}}))["opened"]
        .clone()
}
fn labels(value: &Value) {
    assert_eq!(value["validation"], "mfop1_structure");
    assert_eq!(value["native_source_verified"], false);
    assert!(value["availability_time_ns"].is_null());
}
#[test]
fn bridge_actual_spot_perps_native_groups_preserve_exact_values_and_ack_pairs() {
    for spot in [true, false] {
        let (_temp, target, expected) = fixture(spot);
        let mut bridge = Bridge::spawn();
        let opened = open(&mut bridge, &target, Value::Null);
        labels(&opened);
        let mut checkpoint = opened["checkpoint"].clone();
        let mut native_only = false;
        for group in &expected {
            let response = bridge.send(json!({"schema_version":1,"next":{}}));
            let offer = &response["offer"];
            labels(offer);
            assert_eq!(offer["before"], checkpoint);
            assert_eq!(
                serde_json::to_string(&offer["group"]).unwrap(),
                serde_json::to_string(&serde_json::to_value(group).unwrap()).unwrap()
            );
            if let Some(item) = group.items.iter().find_map(|item| match item {
                ItemDto::ObservedMarket(leaf) => Some(leaf),
                _ => None,
            }) {
                let native = item.batch().unwrap();
                native_only |= native.canonical().events.is_empty();
            }
            checkpoint = offer["after"].clone();
            let response = bridge
                .send(json!({"schema_version":1,"ack":{"id":offer["id"],"checkpoint":checkpoint}}));
            let ack = &response["acknowledged"];
            labels(ack);
            assert_eq!(ack["checkpoint"], checkpoint);
        }
        assert!(native_only);
        let caught = bridge.send(json!({"schema_version":1,"next":{}}));
        let idle = &caught["caught_up"];
        labels(idle);
        assert_eq!(idle["checkpoint"], checkpoint);
        assert_eq!(idle["sealed"], true);
        assert!(bridge.exit().success());
        // Optional explicit test-artifact export for the real Quants subprocess
        // boundary. This never writes a production or historical dataset.
        if !spot {
            if let Some(path) = std::env::var_os("MARKETFEED_BRIDGE_FIXTURE_EXPORT") {
                let root = PathBuf::from(path);
                std::fs::create_dir(&root).unwrap();
                for (name, original) in [
                    ("feed", target.clone()),
                    ("source", _temp.path().join("original")),
                ] {
                    let destination = root.join(name);
                    std::fs::create_dir(&destination).unwrap();
                    for file in ["events.mfsf", "committed.json"] {
                        std::fs::copy(original.join(file), destination.join(file)).unwrap();
                    }
                }
                let source =
                    marketfeed_daemon::causal_bootstrap::replay::MultiHistoryReplay::open_profile(
                        _temp.path().join("original"),
                        multi_owner_tests::limits(),
                        FeedProfile::EngineCausalV4,
                    )
                    .unwrap();
                let comparison =
                    marketfeed_daemon::causal_bootstrap::projection::compare_with_source(
                        source,
                        ProjectionReader::open(&target, projection_tests::limits()).unwrap(),
                    )
                    .unwrap();
                assert_eq!(comparison.groups, expected.len() as u64);
                std::fs::write(
                    root.join("expected-groups.json"),
                    serde_json::to_vec(&expected).unwrap(),
                )
                .unwrap();
                std::fs::write(
                    root.join("fixture.json"),
                    serde_json::to_vec(&json!({
                        "schema_version":1,"purpose":"offline synthetic native-to-Quants boundary",
                        "groups":expected.len(),"source_comparison_verified_at_creation":true,
                        "build":current_build().unwrap(),"economics_supported":false,
                        "historical_availability_qualified":false
                    }))
                    .unwrap(),
                )
                .unwrap();
                std::fs::copy(
                    std::env::current_exe().unwrap(),
                    root.join("source-test-executable"),
                )
                .unwrap();
            }
        }
    }
}
#[test]
fn bridge_pending_eof_second_next_and_wrong_ack_fail_without_acknowledgment() {
    let (_temp, target, _) = fixture(true);
    for mutation in 0..3 {
        let mut bridge = Bridge::spawn();
        open(&mut bridge, &target, Value::Null);
        let offer = bridge.send(json!({"schema_version":1,"next":{}}))["offer"].clone();
        if mutation == 1 {
            bridge.request(json!({"schema_version":1,"next":{}}));
        }
        if mutation == 2 {
            bridge.request(json!({"schema_version":1,"ack":{"id":offer["id"].as_u64().unwrap()+1,"checkpoint":offer["after"]}}));
        }
        assert!(!bridge.exit().success());
        assert!(bridge.output.try_recv().is_err());
    }
}
#[test]
fn bridge_resume_rebuilds_only_validator_and_rejects_wrong_cursor_pair() {
    let (_temp, target, expected) = fixture(false);
    let mut reader = ProjectionReader::open(&target, super::projection_tests::limits()).unwrap();
    reader.read_next().unwrap().unwrap();
    let p = reader.progress().unwrap();
    let pair = json!({"target":p.target_cursor,"source":p.source_cursor});
    let mut bridge = Bridge::spawn();
    let opened = open(&mut bridge, &target, pair.clone());
    assert_eq!(opened["checkpoint"], pair);
    let offer = bridge.send(json!({"schema_version":1,"next":{}}));
    assert_eq!(
        offer["offer"]["group"],
        serde_json::to_value(&expected[1]).unwrap()
    );
    assert!(!bridge.exit().success());
    let mut bad = pair;
    bad["source"]["sequence"] = json!(99);
    let mut bridge = Bridge::spawn();
    bridge.request(json!({"schema_version":1,"open":{"directory":target,"resume":bad}}));
    assert!(!bridge.exit().success());
    assert!(bridge.output.try_recv().is_err());
}

#[test]
fn bridge_continuous_reader_observes_each_committed_prefix_without_seal_or_cursor_leak() {
    let (temp, _, expected) = fixture(true);
    let target = temp.path().join("growing-bridge");
    let mut writer =
        FeedWriter::create_profile(&target, 64 << 20, FeedProfile::ObservationProjectionV1)
            .unwrap();
    let mut bridge = Bridge::spawn();
    let initial = open(&mut bridge, &target, Value::Null);
    let mut checkpoint = initial["checkpoint"].clone();
    for (index, group) in expected.iter().enumerate() {
        let idle = bridge.send(json!({"schema_version":1,"next":{}}));
        labels(&idle["caught_up"]);
        assert_eq!(idle["caught_up"]["checkpoint"], checkpoint);
        assert_eq!(idle["caught_up"]["sealed"], false);
        writer
            .append(FeedRecord::Projection(group.encode().unwrap()))
            .unwrap();
        let offered = bridge.send(json!({"schema_version":1,"next":{}}));
        let offer = &offered["offer"];
        assert_eq!(offer["before"], checkpoint);
        assert_eq!(offer["group"], serde_json::to_value(group).unwrap());
        assert_eq!(offer["sealed"], index + 1 == expected.len());
        let after = offer["after"].clone();
        let ack =
            bridge.send(json!({"schema_version":1,"ack":{"id":offer["id"],"checkpoint":after}}));
        assert_eq!(ack["acknowledged"]["checkpoint"], after);
        checkpoint = after;
    }
    let idle = bridge.send(json!({"schema_version":1,"next":{}}));
    assert_eq!(idle["caught_up"]["checkpoint"], checkpoint);
    assert_eq!(idle["caught_up"]["sealed"], true);
    assert!(bridge.exit().success());
}
