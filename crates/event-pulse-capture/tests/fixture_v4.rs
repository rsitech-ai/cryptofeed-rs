use std::{fs, path::PathBuf, process::Command};

use marketfeed_event_pulse::{
    ProspectiveCaptureAdmissionV2, ProspectiveSystemArtifactPolicyV2, wire::Rfc3339Time,
};
use marketfeed_event_pulse_capture::{FixtureV4Assembler, FixtureV4Error, FixtureV4Request};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const CONTRACT: &[u8] =
    include_bytes!("../contracts/fixture-v4/event-pulse-e2-fixture-v4-contract.json");
const AMENDMENT: &[u8] =
    include_bytes!("../contracts/fixture-v4/2026-08-24-event-pulse-e2-fixture-v4-amendment.md");
const ORACLE: &[u8] = include_bytes!("fixtures/event-pulse-e2-fixture-v4-rust-writer.jsonl");

fn admission() -> ProspectiveCaptureAdmissionV2 {
    let contract: Value = serde_json::from_slice(CONTRACT).unwrap();
    let descriptor = json!({
        "schema": "event-pulse-e2-prospective-admission/2.0",
        "topology_binding": contract["bindings"]["topology"],
        "wire_contract_binding": contract["bindings"]["wire"],
        "capture_starts_at": "2026-08-24T00:00:00Z",
        "evidence_claim": "PROSPECTIVE_CAUSAL_CAPTURE",
        "source_qualification": "UNVERIFIED",
        "authority": contract["authority"],
    });
    ProspectiveCaptureAdmissionV2::from_json(&serde_json::to_vec(&descriptor).unwrap()).unwrap()
}

fn globally_ordered_oracle() -> Vec<u8> {
    let lines = ORACLE
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    assert_eq!(lines.len(), 17);
    let order = [4, 0, 5, 6, 1, 2, 3, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let mut bytes = Vec::with_capacity(ORACLE.len());
    for index in order {
        bytes.extend_from_slice(lines[index]);
        bytes.push(b'\n');
    }
    bytes
}

fn minimum_complete_oracle() -> Vec<u8> {
    let lines = ORACLE
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    let order = [4, 0, 5, 6, 1, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let mut bytes = Vec::new();
    for index in order {
        bytes.extend_from_slice(lines[index]);
        bytes.push(b'\n');
    }
    bytes
}

fn canonical_line(value: &Value) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    bytes
}

fn rehash(mut value: Value) -> Vec<u8> {
    value.as_object_mut().unwrap().remove("payload_hash");
    let hash = format!("{:x}", Sha256::digest(serde_json::to_vec(&value).unwrap()));
    value["payload_hash"] = Value::String(hash);
    canonical_line(&value)
}

fn replace_line(input: &[u8], index: usize, replacement: &[u8]) -> Vec<u8> {
    let mut lines = input
        .split_inclusive(|byte| *byte == b'\n')
        .map(<[u8]>::to_vec)
        .collect::<Vec<_>>();
    lines[index] = replacement.to_vec();
    lines.concat()
}

fn mutate_line(input: &[u8], index: usize, mutate: impl FnOnce(&mut Value)) -> Vec<u8> {
    let line = input
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .nth(index)
        .unwrap();
    let mut value: Value = serde_json::from_slice(line).unwrap();
    mutate(&mut value);
    replace_line(input, index, &rehash(value))
}

fn assembler() -> FixtureV4Assembler {
    let admission = admission();
    let policy = ProspectiveSystemArtifactPolicyV2::from_admission(&admission).unwrap();
    FixtureV4Assembler::new(admission, policy).unwrap()
}

fn request(jsonl: &[u8]) -> FixtureV4Request<'_> {
    FixtureV4Request {
        fixture_id: "bnb-usdt-prospective-v4",
        capture_ends_at: Rfc3339Time::parse("2026-08-24T00:00:16Z").unwrap(),
        decision_time: Rfc3339Time::parse("2026-08-24T00:00:17Z").unwrap(),
        source_terms: "test-only synthetic package",
        complete_jsonl: jsonl,
    }
}

#[test]
fn published_root_fixture_v4_contract_and_oracle_bytes_are_exact() {
    assert_eq!(CONTRACT.len(), 5_527);
    assert_eq!(
        format!("{:x}", Sha256::digest(CONTRACT)),
        "cb899211245fe039f30d9f0d595133365f36d28fff5b508c20e1bf52363a9f47"
    );
    assert_eq!(AMENDMENT.len(), 10_647);
    assert_eq!(
        format!("{:x}", Sha256::digest(AMENDMENT)),
        "2c19540bcc953700318a09738dfdbcf167c591827e8825adcad8003889fff965"
    );
    assert_eq!(ORACLE.len(), 17_189);
    assert_eq!(
        format!("{:x}", Sha256::digest(ORACLE)),
        "fe9a7de25a34a57ff3565bd039929a47891ef69b5ffa147b19555c71eaac20d1"
    );
    assert_eq!(ORACLE.iter().filter(|byte| **byte == b'\n').count(), 17);
    assert!(!ORACLE.contains(&b'\r'));

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    if Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(&manifest_dir)
        .output()
        .is_ok_and(|output| output.status.success())
    {
        for path in [
            "contracts/fixture-v4/event-pulse-e2-fixture-v4-contract.json",
            "contracts/fixture-v4/2026-08-24-event-pulse-e2-fixture-v4-amendment.md",
            "tests/fixtures/event-pulse-e2-fixture-v4-rust-writer.jsonl",
        ] {
            let output = Command::new("git")
                .args(["check-attr", "eol", "--", path])
                .current_dir(&manifest_dir)
                .output()
                .unwrap();
            assert!(output.status.success());
            assert!(
                String::from_utf8(output.stdout)
                    .unwrap()
                    .ends_with(": eol: lf\n")
            );
        }
    }
}

#[test]
fn assembles_exact_eleven_file_structural_candidate_and_reads_it_back() {
    let jsonl = globally_ordered_oracle();
    let assembler = assembler();
    let first = assembler.assemble(request(&jsonl)).unwrap();
    let second = assembler.assemble(request(&jsonl)).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.files().len(), 11);
    assert_eq!(
        first
            .files()
            .iter()
            .map(|file| file.path())
            .collect::<Vec<_>>(),
        [
            "manifest.json",
            "admission.json",
            "inputs/trade.jsonl",
            "inputs/quote.jsonl",
            "inputs/book.jsonl",
            "inputs/open_interest.jsonl",
            "inputs/liquidation.jsonl",
            "inputs/confirmation.jsonl",
            "inputs/clock.jsonl",
            "inputs/coverage.jsonl",
            "inputs/system.jsonl",
        ]
    );
    assert_eq!(first.file("inputs/system.jsonl"), Some(&[][..]));
    assert_eq!(first.status(), "STRUCTURAL_V4_CANDIDATE");
    assert_eq!(first.blocker(), "blocked:fixture-provenance");
    assert!(!first.evidence_authoring_allowed());
    assert!(!first.capture_allowed());
    assert!(!first.execution_allowed());
    let manifest: Value = serde_json::from_slice(first.file("manifest.json").unwrap()).unwrap();
    assert_eq!(
        manifest["schema_version"],
        "event-pulse-e2-prospective-fixture/4.0"
    );
    assert_eq!(
        manifest["causality"]["max_available_at"],
        "2026-08-24T00:00:00.015000Z"
    );
    assert_eq!(
        manifest["artifacts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["record_count"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        [1, 1, 3, 1, 1, 1, 3, 6, 0]
    );
    assert!(
        manifest["artifacts"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["record_identities"] == json!([]))
    );

    let views = first
        .files()
        .iter()
        .map(|file| (file.path(), file.bytes()))
        .collect::<Vec<_>>();
    let adopted = assembler
        .readback(&views, Rfc3339Time::parse("2026-08-24T00:00:17Z").unwrap())
        .unwrap();
    assert_eq!(adopted, first);
}

#[test]
fn request_boundary_failures_are_typed_and_atomic() {
    let jsonl = globally_ordered_oracle();
    let assembler = assembler();
    let mut invalid = request(&jsonl);
    invalid.fixture_id = "UPPER";
    assert_eq!(
        assembler.assemble(invalid).unwrap_err(),
        FixtureV4Error::FixtureId
    );
    let mut invalid = request(&jsonl);
    invalid.source_terms = " trailing ";
    assert_eq!(
        assembler.assemble(invalid).unwrap_err(),
        FixtureV4Error::SourceTerms
    );
    let mut invalid = request(&jsonl);
    invalid.capture_ends_at = Rfc3339Time::parse("2026-08-23T23:59:59Z").unwrap();
    assert_eq!(
        assembler.assemble(invalid).unwrap_err(),
        FixtureV4Error::CaptureInterval
    );
    assert_eq!(
        assembler.assemble(request(&jsonl)).unwrap().files().len(),
        11
    );
}

#[test]
fn accepts_the_universal_fifteen_record_minimum_and_rejects_incomplete_or_noncanonical_input() {
    let minimum = minimum_complete_oracle();
    assert_eq!(minimum.iter().filter(|byte| **byte == b'\n').count(), 15);
    assert_eq!(
        assembler()
            .assemble(request(&minimum))
            .unwrap()
            .files()
            .len(),
        11
    );

    let mut incomplete = minimum.clone();
    let first_end = incomplete.iter().position(|byte| *byte == b'\n').unwrap() + 1;
    incomplete.drain(..first_end);
    assert!(assembler().assemble(request(&incomplete)).is_err());

    let mut missing_lf = minimum.clone();
    missing_lf.pop();
    assert!(assembler().assemble(request(&missing_lf)).is_err());

    let mut out_of_order = minimum.clone();
    let split = out_of_order.iter().position(|byte| *byte == b'\n').unwrap() + 1;
    let first = out_of_order.drain(..split).collect::<Vec<_>>();
    out_of_order.extend(first);
    assert!(assembler().assemble(request(&out_of_order)).is_err());
}

#[test]
fn strict_readback_rejects_coordinated_manifest_authority_drift() {
    let jsonl = globally_ordered_oracle();
    let assembler = assembler();
    let package = assembler.assemble(request(&jsonl)).unwrap();
    let mut owned = package
        .files()
        .iter()
        .map(|file| (file.path().to_owned(), file.bytes().to_vec()))
        .collect::<Vec<_>>();
    let mut manifest: Value = serde_json::from_slice(&owned[0].1).unwrap();
    manifest["authority"]["capture_allowed"] = Value::Bool(true);
    owned[0].1 = canonical_line(&manifest);
    let views = owned
        .iter()
        .map(|(path, bytes)| (path.as_str(), bytes.as_slice()))
        .collect::<Vec<_>>();
    assert_eq!(
        assembler
            .readback(&views, Rfc3339Time::parse("2026-08-24T00:00:17Z").unwrap(),)
            .unwrap_err(),
        FixtureV4Error::ReadbackMismatch
    );
}

#[test]
fn contract_rejects_canonically_rehashed_null_binance_quote_quantity() {
    let jsonl = globally_ordered_oracle();
    let mut quote: Value = serde_json::from_slice(
        jsonl
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .nth(1)
            .unwrap(),
    )
    .unwrap();
    quote["envelope"]["payload"]["Quote"]["bid_quantity"] = Value::Null;
    let mutated = replace_line(&jsonl, 1, &rehash(quote));
    assert!(matches!(
        assembler().assemble(request(&mutated)),
        Err(FixtureV4Error::Contract("binance quote payload"))
    ));
}

#[test]
fn contract_rejects_rehashed_catalog_payload_provenance_time_and_frame_drift() {
    let jsonl = globally_ordered_oracle();
    let cases = [
        (
            mutate_line(&jsonl, 0, |trade| {
                trade["catalog"]["open_interest"] = json!({});
            }),
            "source-specific catalog",
        ),
        (
            mutate_line(&jsonl, 0, |trade| {
                trade["envelope"]["payload"]["Trade"]["trade_id"] = json!("999");
            }),
            "binance trade payload",
        ),
        (
            mutate_line(&jsonl, 4, |book| {
                book["envelope"]["payload"]["BookSnapshot"]["depth"] = json!(999);
            }),
            "binance book payload",
        ),
    ];
    for (mutated, rule) in cases {
        let result = assembler().assemble(request(&mutated));
        assert!(
            matches!(
                &result,
                Err(FixtureV4Error::Contract(observed)) if *observed == rule
            ),
            "rule={rule} result={result:?}"
        );
    }

    let invalid_time = mutate_line(&jsonl, 0, |trade| {
        let receive = trade["envelope"]["receive_ts"].as_i64().unwrap();
        trade["envelope"]["exchange_ts"] = json!(receive + 1);
    });
    assert!(assembler().assemble(request(&invalid_time)).is_err());

    let lines = jsonl
        .split_inclusive(|byte| *byte == b'\n')
        .map(<[u8]>::to_vec)
        .collect::<Vec<_>>();
    let mut second_quote: Value = serde_json::from_slice(&lines[1][..lines[1].len() - 1]).unwrap();
    second_quote["action_index"] = json!(1);
    second_quote["market_cursor"]["action_index"] = json!(1);
    let mut duplicate_frame = Vec::new();
    duplicate_frame.extend_from_slice(&lines[0]);
    duplicate_frame.extend_from_slice(&lines[1]);
    duplicate_frame.extend_from_slice(&rehash(second_quote));
    for line in &lines[2..] {
        duplicate_frame.extend_from_slice(line);
    }
    assert!(matches!(
        assembler().assemble(request(&duplicate_frame)),
        Err(FixtureV4Error::Contract("binance frame grammar"))
    ));
}

#[test]
fn strict_readback_rejects_coordinated_artifact_and_manifest_rehash() {
    let jsonl = globally_ordered_oracle();
    let assembler = assembler();
    let package = assembler.assemble(request(&jsonl)).unwrap();
    let mut owned = package
        .files()
        .iter()
        .map(|file| (file.path().to_owned(), file.bytes().to_vec()))
        .collect::<Vec<_>>();
    let quote_index = owned
        .iter()
        .position(|(path, _)| path == "inputs/quote.jsonl")
        .unwrap();
    let mut quote: Value =
        serde_json::from_slice(&owned[quote_index].1[..owned[quote_index].1.len() - 1]).unwrap();
    quote["envelope"]["payload"]["Quote"]["ask_quantity"] = Value::Null;
    owned[quote_index].1 = rehash(quote);
    let mut manifest: Value = serde_json::from_slice(&owned[0].1).unwrap();
    let report = manifest["artifacts"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|report| report["role"] == "QUOTE")
        .unwrap();
    report["byte_length"] = json!(owned[quote_index].1.len());
    report["sha256"] = Value::String(format!("{:x}", Sha256::digest(&owned[quote_index].1)));
    owned[0].1 = canonical_line(&manifest);
    let views = owned
        .iter()
        .map(|(path, bytes)| (path.as_str(), bytes.as_slice()))
        .collect::<Vec<_>>();
    assert!(matches!(
        assembler.readback(&views, Rfc3339Time::parse("2026-08-24T00:00:17Z").unwrap(),),
        Err(FixtureV4Error::Contract("binance quote payload"))
    ));
}

#[test]
fn contract_and_wire_reject_sidecar_domains_continuity_and_json_type_aliases() {
    let jsonl = globally_ordered_oracle();
    let invalid_clock = mutate_line(&jsonl, 8, |clock| {
        clock["freshness_limit_ms"] = json!(0);
    });
    assert!(assembler().assemble(request(&invalid_clock)).is_err());

    let invalid_coverage = mutate_line(&jsonl, 11, |coverage| {
        coverage["family"] = json!("BOOK");
    });
    assert!(assembler().assemble(request(&invalid_coverage)).is_err());

    let invalid_type = mutate_line(&jsonl, 1, |quote| {
        quote["action_index"] = Value::Bool(false);
    });
    assert!(assembler().assemble(request(&invalid_type)).is_err());

    let mut clock: Value = serde_json::from_slice(
        jsonl
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .nth(8)
            .unwrap(),
    )
    .unwrap();
    clock["available_at"] = json!("2026-08-24T00:00:00.016000Z");
    clock["observed_at"] = json!("2026-08-24T00:00:00.016000Z");
    clock["clock_cursor"]["start"] = json!(3);
    clock["clock_cursor"]["end"] = json!(3);
    let mut sidecar_gap = jsonl.clone();
    sidecar_gap.extend_from_slice(&rehash(clock));
    assert!(assembler().assemble(request(&sidecar_gap)).is_err());
}

#[test]
#[ignore = "requires the exact published root Fixture V4 validator checkout"]
fn assembled_package_passes_published_root_cross_language_validator() {
    let validator = std::env::var_os("EVENT_PULSE_ROOT_V4_VALIDATOR")
        .map(PathBuf::from)
        .expect("EVENT_PULSE_ROOT_V4_VALIDATOR must name the pinned root validator");
    let jsonl = globally_ordered_oracle();
    let package = assembler().assemble(request(&jsonl)).unwrap();
    let root = std::env::temp_dir().join(format!("event-pulse-fixture-v4-{}", std::process::id()));
    if root.exists() {
        fs::remove_dir_all(&root).unwrap();
    }
    for file in package.files() {
        let target = root.join(file.path());
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(target, file.bytes()).unwrap();
    }
    let output = Command::new("python3")
        .arg(validator)
        .arg("--package")
        .arg(&root)
        .output()
        .unwrap();
    fs::remove_dir_all(&root).unwrap();
    assert!(
        output.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("STRUCTURAL_V4_CANDIDATE"));
}

#[test]
#[ignore = "requires the exact published root Fixture V4 validator checkout"]
fn rust_and_published_root_both_reject_coordinated_quote_domain_drift() {
    let validator = std::env::var_os("EVENT_PULSE_ROOT_V4_VALIDATOR")
        .map(PathBuf::from)
        .expect("EVENT_PULSE_ROOT_V4_VALIDATOR must name the pinned root validator");
    let jsonl = globally_ordered_oracle();
    let assembler = assembler();
    let package = assembler.assemble(request(&jsonl)).unwrap();
    let mut owned = package
        .files()
        .iter()
        .map(|file| (file.path().to_owned(), file.bytes().to_vec()))
        .collect::<Vec<_>>();
    let quote_index = owned
        .iter()
        .position(|(path, _)| path == "inputs/quote.jsonl")
        .unwrap();
    let mut quote: Value =
        serde_json::from_slice(&owned[quote_index].1[..owned[quote_index].1.len() - 1]).unwrap();
    quote["envelope"]["payload"]["Quote"]["bid_quantity"] = Value::Null;
    owned[quote_index].1 = rehash(quote);
    let mut manifest: Value = serde_json::from_slice(&owned[0].1).unwrap();
    let report = manifest["artifacts"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|report| report["role"] == "QUOTE")
        .unwrap();
    report["byte_length"] = json!(owned[quote_index].1.len());
    report["sha256"] = Value::String(format!("{:x}", Sha256::digest(&owned[quote_index].1)));
    owned[0].1 = canonical_line(&manifest);
    let views = owned
        .iter()
        .map(|(path, bytes)| (path.as_str(), bytes.as_slice()))
        .collect::<Vec<_>>();
    assert!(matches!(
        assembler.readback(&views, Rfc3339Time::parse("2026-08-24T00:00:17Z").unwrap(),),
        Err(FixtureV4Error::Contract("binance quote payload"))
    ));

    let root = std::env::temp_dir().join(format!(
        "event-pulse-fixture-v4-negative-{}",
        std::process::id()
    ));
    if root.exists() {
        fs::remove_dir_all(&root).unwrap();
    }
    for (path, bytes) in &owned {
        let target = root.join(path);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(target, bytes).unwrap();
    }
    let output = Command::new("python3")
        .arg(validator)
        .arg("--package")
        .arg(&root)
        .output()
        .unwrap();
    fs::remove_dir_all(&root).unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("Quote bid_quantity"));
}
