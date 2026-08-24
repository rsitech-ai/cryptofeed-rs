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

fn admission_at(capture_starts_at: &str) -> ProspectiveCaptureAdmissionV2 {
    let contract: Value = serde_json::from_slice(CONTRACT).unwrap();
    let descriptor = json!({
        "schema": "event-pulse-e2-prospective-admission/2.0",
        "topology_binding": contract["bindings"]["topology"],
        "wire_contract_binding": contract["bindings"]["wire"],
        "capture_starts_at": capture_starts_at,
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
    assembler_at("2026-08-24T00:00:00Z")
}

fn assembler_at(capture_starts_at: &str) -> FixtureV4Assembler {
    let admission = admission_at(capture_starts_at);
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

fn owned_package() -> Vec<(String, Vec<u8>)> {
    assembler()
        .assemble(request(&globally_ordered_oracle()))
        .unwrap()
        .files()
        .iter()
        .map(|file| (file.path().to_owned(), file.bytes().to_vec()))
        .collect()
}

fn mutate_package_record(
    owned: &mut [(String, Vec<u8>)],
    path: &str,
    line_index: usize,
    mutate: fn(&mut Value),
) {
    let artifact_index = owned
        .iter()
        .position(|(candidate, _)| candidate == path)
        .unwrap();
    let line = owned[artifact_index]
        .1
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .nth(line_index)
        .unwrap();
    let mut value: Value = serde_json::from_slice(line).unwrap();
    mutate(&mut value);
    owned[artifact_index].1 = replace_line(&owned[artifact_index].1, line_index, &rehash(value));

    let mut manifest: Value = serde_json::from_slice(&owned[0].1).unwrap();
    let report = manifest["artifacts"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|report| report["path"] == path)
        .unwrap();
    report["byte_length"] = json!(owned[artifact_index].1.len());
    report["sha256"] = Value::String(format!("{:x}", Sha256::digest(&owned[artifact_index].1)));
    owned[0].1 = canonical_line(&manifest);
}

fn mutate_manifest(owned: &mut [(String, Vec<u8>)], mutate: impl FnOnce(&mut Value)) {
    let mut manifest: Value = serde_json::from_slice(&owned[0].1).unwrap();
    mutate(&mut manifest);
    owned[0].1 = canonical_line(&manifest);
}

fn set_package_capture_start(owned: &mut [(String, Vec<u8>)], capture_start: &str) {
    let mut admission: Value = serde_json::from_slice(&owned[1].1).unwrap();
    admission["capture_starts_at"] = json!(capture_start);
    owned[1].1 = canonical_line(&admission);
    let admission_length = owned[1].1.len();
    let admission_sha = format!("{:x}", Sha256::digest(&owned[1].1));
    let mut manifest: Value = serde_json::from_slice(&owned[0].1).unwrap();
    manifest["capture"]["started_at"] = json!(capture_start);
    manifest["admission_binding"]["byte_length"] = json!(admission_length);
    manifest["admission_binding"]["sha256"] = json!(admission_sha);
    owned[0].1 = canonical_line(&manifest);
}

fn set_admission_capture_start(owned: &mut [(String, Vec<u8>)], capture_start: &str) {
    let mut admission: Value = serde_json::from_slice(&owned[1].1).unwrap();
    admission["capture_starts_at"] = json!(capture_start);
    owned[1].1 = canonical_line(&admission);
    let mut manifest: Value = serde_json::from_slice(&owned[0].1).unwrap();
    manifest["admission_binding"]["byte_length"] = json!(owned[1].1.len());
    manifest["admission_binding"]["sha256"] =
        Value::String(format!("{:x}", Sha256::digest(&owned[1].1)));
    owned[0].1 = canonical_line(&manifest);
}

fn rust_readback_accepts(owned: &[(String, Vec<u8>)]) -> bool {
    rust_readback_accepts_at(owned, "2026-08-24T00:00:00Z")
}

fn rust_readback_accepts_at(owned: &[(String, Vec<u8>)], capture_start: &str) -> bool {
    rust_readback_accepts_at_with_decision(owned, capture_start, "2026-08-24T00:00:17Z")
}

fn rust_readback_accepts_at_with_decision(
    owned: &[(String, Vec<u8>)],
    capture_start: &str,
    decision_time: &str,
) -> bool {
    let views = owned
        .iter()
        .map(|(path, bytes)| (path.as_str(), bytes.as_slice()))
        .collect::<Vec<_>>();
    assembler_at(capture_start)
        .readback(&views, Rfc3339Time::parse(decision_time).unwrap())
        .is_ok()
}

fn root_validator_accepts(validator: &PathBuf, owned: &[(String, Vec<u8>)], label: &str) -> bool {
    let root = std::env::temp_dir().join(format!(
        "event-pulse-fixture-v4-parity-{}-{label}",
        std::process::id()
    ));
    if root.exists() {
        fs::remove_dir_all(&root).unwrap();
    }
    for (path, bytes) in owned {
        let target = root.join(path);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(target, bytes).unwrap();
    }
    let accepted = Command::new("python3")
        .arg(validator)
        .arg("--package")
        .arg(&root)
        .output()
        .unwrap()
        .status
        .success();
    fs::remove_dir_all(root).unwrap();
    accepted
}

#[test]
fn published_root_fixture_v4_contract_and_oracle_bytes_are_exact() {
    assert_eq!(CONTRACT.len(), 5_625);
    assert_eq!(
        format!("{:x}", Sha256::digest(CONTRACT)),
        "62dd6298cce3cc9390fc0996e085fa0dff795d5eedf22fd65f21403b1fccc1a7"
    );
    assert_eq!(AMENDMENT.len(), 11_180);
    assert_eq!(
        format!("{:x}", Sha256::digest(AMENDMENT)),
        "39771adec792dabd38e4f1de1994b0b2f46c9b8207338af3946534b1ab6d34ad"
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
fn carried_v1_timestamp_wire_accepts_offsets_and_rejects_aliases() {
    for (label, path, mutate, accepted) in [
        (
            "clock-short",
            "inputs/clock.jsonl",
            (|value: &mut Value| {
                value["observed_at"] = json!("2026-08-24T00:00:00.007Z");
            }) as fn(&mut Value),
            false,
        ),
        (
            "clock-offset",
            "inputs/clock.jsonl",
            |value: &mut Value| {
                value["observed_at"] = json!("2026-08-24T05:30:00.007000+05:30");
                value["available_at"] = json!("2026-08-24T05:30:00.007000+05:30");
            },
            true,
        ),
        (
            "clock-positive-zero",
            "inputs/clock.jsonl",
            |value: &mut Value| {
                value["observed_at"] = json!("2026-08-24T00:00:00.007000+00:00");
            },
            false,
        ),
        (
            "clock-negative-zero",
            "inputs/clock.jsonl",
            |value: &mut Value| {
                value["observed_at"] = json!("2026-08-24T00:00:00.007000-00:00");
            },
            false,
        ),
        (
            "coverage-short",
            "inputs/coverage.jsonl",
            |value: &mut Value| {
                value["covered_through"] = json!("2026-08-24T00:00:00.010Z");
            },
            false,
        ),
        (
            "coverage-offset-boundaries",
            "inputs/coverage.jsonl",
            |value: &mut Value| {
                value["covered_from"] = json!("2026-08-24T23:59:00+23:59");
                value["covered_through"] = json!("2026-08-23T00:01:00.010000-23:59");
                value["available_at"] = json!("2026-08-23T00:01:00.010000-23:59");
            },
            true,
        ),
        (
            "coverage-positive-zero",
            "inputs/coverage.jsonl",
            |value: &mut Value| {
                value["covered_through"] = json!("2026-08-24T00:00:00.010000+00:00");
            },
            false,
        ),
        (
            "coverage-negative-zero",
            "inputs/coverage.jsonl",
            |value: &mut Value| {
                value["covered_through"] = json!("2026-08-24T00:00:00.010000-00:00");
            },
            false,
        ),
    ] {
        let mut owned = owned_package();
        mutate_package_record(&mut owned, path, 0, mutate);
        assert_eq!(rust_readback_accepts(&owned), accepted, "{label}");
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
fn full_u32_market_flags_are_valid_but_type_alias_and_one_over_are_rejected() {
    let jsonl = globally_ordered_oracle();
    let maximum = mutate_line(&jsonl, 1, |quote| {
        quote["envelope"]["flags"] = json!(u32::MAX);
    });
    let assembler = assembler();
    let package = assembler.assemble(request(&maximum)).unwrap();
    let views = package
        .files()
        .iter()
        .map(|file| (file.path(), file.bytes()))
        .collect::<Vec<_>>();
    assembler
        .readback(&views, Rfc3339Time::parse("2026-08-24T00:00:17Z").unwrap())
        .unwrap();

    for invalid in [Value::Bool(false), json!(u64::from(u32::MAX) + 1)] {
        let mutated = mutate_line(&jsonl, 1, |quote| {
            quote["envelope"]["flags"] = invalid;
        });
        assert!(assembler.assemble(request(&mutated)).is_err());
    }
}

#[test]
fn capture_start_strictly_postdates_the_latest_embedded_publication_binding() {
    let jsonl = globally_ordered_oracle();
    let accepted_start = "2026-08-23T21:57:00.000001Z";
    let accepted = assembler_at(accepted_start)
        .assemble(request(&jsonl))
        .unwrap();
    let accepted_views = accepted
        .files()
        .iter()
        .map(|file| (file.path(), file.bytes()))
        .collect::<Vec<_>>();
    assembler_at(accepted_start)
        .readback(
            &accepted_views,
            Rfc3339Time::parse("2026-08-24T00:00:17Z").unwrap(),
        )
        .unwrap();

    for rejected_start in ["2026-08-23T21:56:59.999999Z", "2026-08-23T21:57:00Z"] {
        assert!(matches!(
            assembler_at(rejected_start).assemble(request(&jsonl)),
            Err(FixtureV4Error::Contract("capture publication floor"))
        ));
        let mut owned = owned_package();
        set_package_capture_start(&mut owned, rejected_start);
        let views = owned
            .iter()
            .map(|(path, bytes)| (path.as_str(), bytes.as_slice()))
            .collect::<Vec<_>>();
        assert!(matches!(
            assembler_at(rejected_start)
                .readback(&views, Rfc3339Time::parse("2026-08-24T00:00:17Z").unwrap(),),
            Err(FixtureV4Error::Contract("capture publication floor"))
        ));
    }
}

#[test]
fn adopted_manifest_timestamps_follow_their_published_field_specific_lexical_rules() {
    let mut matching_capture_spelling = owned_package();
    set_package_capture_start(&mut matching_capture_spelling, "2026-08-24T00:00:00.0Z");
    assert!(rust_readback_accepts(&matching_capture_spelling));

    let mut mismatched_capture_spelling = owned_package();
    set_admission_capture_start(&mut mismatched_capture_spelling, "2026-08-24T00:00:00.0Z");
    assert!(!rust_readback_accepts(&mismatched_capture_spelling));

    for capture_end in [
        "2026-08-24T00:00:16.1Z",
        "2026-08-24T00:00:16.10Z",
        "2026-08-24T00:00:16.000001Z",
    ] {
        let mut owned = owned_package();
        mutate_manifest(&mut owned, |manifest| {
            manifest["capture"]["ended_at"] = json!(capture_end);
        });
        assert!(rust_readback_accepts(&owned));
    }
    for capture_end in ["2026-08-24T00:00:16.0000001Z", "2026-08-24T00:00:16+00:00"] {
        let mut owned = owned_package();
        mutate_manifest(&mut owned, |manifest| {
            manifest["capture"]["ended_at"] = json!(capture_end);
        });
        assert!(!rust_readback_accepts(&owned));
    }

    for (reachable_at, accepted) in [
        ("2026-08-22T07:35:52.1Z", true),
        ("2026-08-22T07:35:52.000001Z", true),
        ("2026-08-22T07:35:52.10Z", false),
        ("2026-08-22T07:35:52.0000001Z", false),
        ("2026-08-22T07:35:52+00:00", false),
    ] {
        let mut owned = owned_package();
        mutate_manifest(&mut owned, |manifest| {
            manifest["amendment_binding"]["default_reachable_at"] = json!(reachable_at);
        });
        assert_eq!(rust_readback_accepts(&owned), accepted);
    }

    for (bound, accepted) in [
        ("2026-08-24T00:00:00.002Z", true),
        ("2026-08-24T00:00:00.0020Z", true),
        ("2026-08-24T00:00:00.0020000Z", false),
        ("2026-08-24T00:00:00.002+00:00", false),
    ] {
        let mut owned = owned_package();
        mutate_manifest(&mut owned, |manifest| {
            manifest["artifacts"][1]["first_available_at"] = json!(bound);
            manifest["artifacts"][1]["last_available_at"] = json!(bound);
        });
        assert_eq!(rust_readback_accepts(&owned), accepted);
    }
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

#[test]
#[ignore = "requires the exact published root Fixture V4 validator checkout"]
fn rust_contract_matches_published_root_over_semantic_mutation_matrix() {
    fn flags_max(value: &mut Value) {
        value["envelope"]["flags"] = json!(u32::MAX);
    }
    fn flags_bool(value: &mut Value) {
        value["envelope"]["flags"] = Value::Bool(false);
    }
    fn flags_over(value: &mut Value) {
        value["envelope"]["flags"] = json!(u64::from(u32::MAX) + 1);
    }
    fn schema(value: &mut Value) {
        value["envelope"]["schema_version"] = json!(2);
    }
    fn venue(value: &mut Value) {
        value["envelope"]["venue"] = json!(4);
    }
    fn connection(value: &mut Value) {
        value["envelope"]["connection"] = json!(12);
    }
    fn source(value: &mut Value) {
        value["catalog"]["venue_sources"]["3"]["source_id"] = json!("wrong");
    }
    fn quote_quantity(value: &mut Value) {
        value["envelope"]["payload"]["Quote"]["bid_quantity"] = Value::Null;
    }
    fn quote_price(value: &mut Value) {
        value["envelope"]["payload"]["Quote"]["ask_price"]["coefficient"] = json!(0);
    }
    fn event_item(value: &mut Value) {
        value["envelope"]["event_index"] = json!(1);
        value["market_cursor"]["item_index"] = json!(1);
    }
    fn book_depth(value: &mut Value) {
        value["envelope"]["payload"]["BookSnapshot"]["depth"] = json!(999);
    }
    fn book_checksum(value: &mut Value) {
        value["envelope"]["payload"]["BookSnapshot"]["checksum"] = json!("x");
    }
    fn book_delete_quantity(value: &mut Value) {
        let change = &mut value["envelope"]["payload"]["BookDelta"]["changes"][0];
        change["operation"] = json!("Delete");
    }
    fn trade_id(value: &mut Value) {
        value["envelope"]["payload"]["Trade"]["trade_id"] = json!("51");
    }
    fn trade_aggressor(value: &mut Value) {
        value["envelope"]["payload"]["Trade"]["aggressor"] = json!("Unknown");
    }
    fn open_interest(value: &mut Value) {
        value["envelope"]["payload"]["OpenInterest"]["quantity"] = Value::Bool(false);
    }
    fn liquidation_side(value: &mut Value) {
        value["envelope"]["payload"]["Liquidation"]["side"] = json!("Bid");
    }
    fn confirmation_price(value: &mut Value) {
        value["envelope"]["payload"]["MarkPrice"]["price"] = Value::Null;
    }
    fn exchange_after_receive(value: &mut Value) {
        value["envelope"]["exchange_ts"] = json!(1_787_529_600_003_000_001_u64);
    }
    fn clock_state(value: &mut Value) {
        value["clock_state"] = json!("unsynchronized");
    }
    fn clock_quality(value: &mut Value) {
        value["quality_state"] = json!("degraded");
    }
    fn clock_observed(value: &mut Value) {
        value["observed_at"] = json!("2026-08-24T00:00:00.007001Z");
    }
    fn clock_short_fraction(value: &mut Value) {
        value["observed_at"] = json!("2026-08-24T00:00:00.007Z");
    }
    fn clock_nonzero_offset(value: &mut Value) {
        value["observed_at"] = json!("2026-08-24T05:30:00.007000+05:30");
        value["available_at"] = json!("2026-08-24T05:30:00.007000+05:30");
    }
    fn clock_positive_zero_offset(value: &mut Value) {
        value["observed_at"] = json!("2026-08-24T00:00:00.007000+00:00");
    }
    fn clock_negative_zero_offset(value: &mut Value) {
        value["observed_at"] = json!("2026-08-24T00:00:00.007000-00:00");
    }
    fn clock_freshness(value: &mut Value) {
        value["freshness_limit_ms"] = json!(0);
    }
    fn clock_reason(value: &mut Value) {
        value["reason_code"] = json!("normal");
    }
    fn coverage_family(value: &mut Value) {
        value["family"] = json!("TRADE");
    }
    fn coverage_interval(value: &mut Value) {
        value["covered_from"] = json!("2026-08-24T00:00:00.011000Z");
    }
    fn coverage_short_fraction(value: &mut Value) {
        value["covered_through"] = json!("2026-08-24T00:00:00.010Z");
    }
    fn coverage_boundary_offsets(value: &mut Value) {
        value["covered_from"] = json!("2026-08-24T23:59:00+23:59");
        value["covered_through"] = json!("2026-08-23T00:01:00.010000-23:59");
        value["available_at"] = json!("2026-08-23T00:01:00.010000-23:59");
    }
    fn coverage_positive_zero_offset(value: &mut Value) {
        value["covered_through"] = json!("2026-08-24T00:00:00.010000+00:00");
    }
    fn coverage_negative_zero_offset(value: &mut Value) {
        value["covered_through"] = json!("2026-08-24T00:00:00.010000-00:00");
    }
    fn coverage_generation(value: &mut Value) {
        value["coverage_source"]["epoch_generation"] = json!(1);
    }
    fn cursor_kind(value: &mut Value) {
        value["coverage_cursor"]["kind"] = json!("DERIVED");
    }
    fn capture_mode(value: &mut Value) {
        value["capture"]["mode"] = json!("HISTORICAL");
    }
    fn capture_end(value: &mut Value) {
        value["capture"]["ended_at"] = json!("2026-08-24T00:00:00.014000Z");
    }
    fn decision(value: &mut Value) {
        value["causality"]["decision_time"] = json!("2026-08-24T00:00:15Z");
    }
    fn source_terms(value: &mut Value) {
        value["retention"]["source_terms"] = json!("");
    }
    fn authority(value: &mut Value) {
        value["authority"]["evidence_authoring_allowed"] = Value::Bool(true);
    }
    fn published_binding(value: &mut Value) {
        value["published_bindings"]["topology"]["sha256"] = json!("0".repeat(64));
    }
    fn transformation(value: &mut Value) {
        value["transformation"]["sha256"] = json!("0".repeat(64));
    }
    fn admission_binding(value: &mut Value) {
        value["admission_binding"]["byte_length"] = json!(0);
    }
    fn evidence_claim(value: &mut Value) {
        value["evidence_claim"] = json!("CAPTURED");
    }
    fn amendment_binding(value: &mut Value) {
        value["amendment_binding"]["commit"] = json!("0".repeat(40));
    }
    fn contract_binding(value: &mut Value) {
        value["fixture_v4_contract_binding"]["sha256"] = json!("0".repeat(64));
    }
    fn availability_authority(value: &mut Value) {
        value["causality"]["availability_authority"] = json!("receive_ts");
    }
    fn max_available_at(value: &mut Value) {
        value["causality"]["max_available_at"] = json!("2026-08-24T00:00:00.014000Z");
    }
    fn retention(value: &mut Value) {
        value["retention"]["sanitized"] = Value::Bool(false);
    }

    struct Case {
        label: &'static str,
        path: &'static str,
        line: usize,
        mutate: fn(&mut Value),
        accepted: bool,
    }
    let cases = [
        Case {
            label: "flags-max",
            path: "inputs/quote.jsonl",
            line: 0,
            mutate: flags_max,
            accepted: true,
        },
        Case {
            label: "flags-bool",
            path: "inputs/quote.jsonl",
            line: 0,
            mutate: flags_bool,
            accepted: false,
        },
        Case {
            label: "flags-over",
            path: "inputs/quote.jsonl",
            line: 0,
            mutate: flags_over,
            accepted: false,
        },
        Case {
            label: "schema",
            path: "inputs/quote.jsonl",
            line: 0,
            mutate: schema,
            accepted: false,
        },
        Case {
            label: "venue",
            path: "inputs/quote.jsonl",
            line: 0,
            mutate: venue,
            accepted: false,
        },
        Case {
            label: "connection",
            path: "inputs/quote.jsonl",
            line: 0,
            mutate: connection,
            accepted: false,
        },
        Case {
            label: "source",
            path: "inputs/quote.jsonl",
            line: 0,
            mutate: source,
            accepted: false,
        },
        Case {
            label: "quote-quantity",
            path: "inputs/quote.jsonl",
            line: 0,
            mutate: quote_quantity,
            accepted: false,
        },
        Case {
            label: "quote-price",
            path: "inputs/quote.jsonl",
            line: 0,
            mutate: quote_price,
            accepted: false,
        },
        Case {
            label: "event-item",
            path: "inputs/quote.jsonl",
            line: 0,
            mutate: event_item,
            accepted: false,
        },
        Case {
            label: "book-depth",
            path: "inputs/book.jsonl",
            line: 0,
            mutate: book_depth,
            accepted: false,
        },
        Case {
            label: "book-checksum",
            path: "inputs/book.jsonl",
            line: 0,
            mutate: book_checksum,
            accepted: false,
        },
        Case {
            label: "book-delete-quantity",
            path: "inputs/book.jsonl",
            line: 1,
            mutate: book_delete_quantity,
            accepted: false,
        },
        Case {
            label: "trade-id",
            path: "inputs/trade.jsonl",
            line: 0,
            mutate: trade_id,
            accepted: false,
        },
        Case {
            label: "trade-aggressor",
            path: "inputs/trade.jsonl",
            line: 0,
            mutate: trade_aggressor,
            accepted: false,
        },
        Case {
            label: "open-interest",
            path: "inputs/open_interest.jsonl",
            line: 0,
            mutate: open_interest,
            accepted: false,
        },
        Case {
            label: "liquidation-side",
            path: "inputs/liquidation.jsonl",
            line: 0,
            mutate: liquidation_side,
            accepted: false,
        },
        Case {
            label: "confirmation-price",
            path: "inputs/confirmation.jsonl",
            line: 0,
            mutate: confirmation_price,
            accepted: false,
        },
        Case {
            label: "exchange-after-receive",
            path: "inputs/quote.jsonl",
            line: 0,
            mutate: exchange_after_receive,
            accepted: false,
        },
        Case {
            label: "clock-state",
            path: "inputs/clock.jsonl",
            line: 0,
            mutate: clock_state,
            accepted: false,
        },
        Case {
            label: "clock-quality",
            path: "inputs/clock.jsonl",
            line: 0,
            mutate: clock_quality,
            accepted: false,
        },
        Case {
            label: "clock-observed",
            path: "inputs/clock.jsonl",
            line: 0,
            mutate: clock_observed,
            accepted: false,
        },
        Case {
            label: "clock-short-fraction",
            path: "inputs/clock.jsonl",
            line: 0,
            mutate: clock_short_fraction,
            accepted: false,
        },
        Case {
            label: "clock-nonzero-offset",
            path: "inputs/clock.jsonl",
            line: 0,
            mutate: clock_nonzero_offset,
            accepted: true,
        },
        Case {
            label: "clock-positive-zero-offset",
            path: "inputs/clock.jsonl",
            line: 0,
            mutate: clock_positive_zero_offset,
            accepted: false,
        },
        Case {
            label: "clock-negative-zero-offset",
            path: "inputs/clock.jsonl",
            line: 0,
            mutate: clock_negative_zero_offset,
            accepted: false,
        },
        Case {
            label: "clock-freshness",
            path: "inputs/clock.jsonl",
            line: 0,
            mutate: clock_freshness,
            accepted: false,
        },
        Case {
            label: "clock-reason",
            path: "inputs/clock.jsonl",
            line: 0,
            mutate: clock_reason,
            accepted: false,
        },
        Case {
            label: "coverage-family",
            path: "inputs/coverage.jsonl",
            line: 0,
            mutate: coverage_family,
            accepted: false,
        },
        Case {
            label: "coverage-interval",
            path: "inputs/coverage.jsonl",
            line: 0,
            mutate: coverage_interval,
            accepted: false,
        },
        Case {
            label: "coverage-short-fraction",
            path: "inputs/coverage.jsonl",
            line: 0,
            mutate: coverage_short_fraction,
            accepted: false,
        },
        Case {
            label: "coverage-boundary-offsets",
            path: "inputs/coverage.jsonl",
            line: 0,
            mutate: coverage_boundary_offsets,
            accepted: true,
        },
        Case {
            label: "coverage-positive-zero-offset",
            path: "inputs/coverage.jsonl",
            line: 0,
            mutate: coverage_positive_zero_offset,
            accepted: false,
        },
        Case {
            label: "coverage-negative-zero-offset",
            path: "inputs/coverage.jsonl",
            line: 0,
            mutate: coverage_negative_zero_offset,
            accepted: false,
        },
        Case {
            label: "coverage-generation",
            path: "inputs/coverage.jsonl",
            line: 0,
            mutate: coverage_generation,
            accepted: false,
        },
        Case {
            label: "cursor-kind",
            path: "inputs/coverage.jsonl",
            line: 0,
            mutate: cursor_kind,
            accepted: false,
        },
    ];

    let validator = std::env::var_os("EVENT_PULSE_ROOT_V4_VALIDATOR")
        .map(PathBuf::from)
        .expect("EVENT_PULSE_ROOT_V4_VALIDATOR must name the pinned root validator");
    for case in cases {
        let mut owned = owned_package();
        mutate_package_record(&mut owned, case.path, case.line, case.mutate);
        let rust = rust_readback_accepts(&owned);
        let root = root_validator_accepts(&validator, &owned, case.label);
        assert_eq!(rust, root, "Rust/root mismatch for {}", case.label);
        assert_eq!(rust, case.accepted, "unexpected result for {}", case.label);
    }

    let manifest_cases = [
        ("capture-mode", capture_mode as fn(&mut Value), false),
        ("capture-end", capture_end, false),
        ("decision", decision, false),
        ("source-terms", source_terms, false),
        ("authority", authority, false),
        ("published-binding", published_binding, false),
        ("transformation", transformation, false),
        ("admission-binding", admission_binding, false),
        ("evidence-claim", evidence_claim, false),
        ("amendment-binding", amendment_binding, true),
        ("contract-binding", contract_binding, false),
        ("availability-authority", availability_authority, false),
        ("max-available-at", max_available_at, false),
        ("retention", retention, false),
    ];
    for (label, mutate, accepted) in manifest_cases {
        let mut owned = owned_package();
        mutate_manifest(&mut owned, mutate);
        let rust = rust_readback_accepts(&owned);
        let root = root_validator_accepts(&validator, &owned, label);
        assert_eq!(rust, root, "Rust/root mismatch for {label}");
        assert_eq!(rust, accepted, "unexpected result for {label}");
    }

    for (label, capture_start, accepted) in [
        (
            "publication-minus-one",
            "2026-08-23T21:56:59.999999Z",
            false,
        ),
        ("publication-equal", "2026-08-23T21:57:00Z", false),
        ("publication-plus-one", "2026-08-23T21:57:00.000001Z", true),
    ] {
        let mut owned = owned_package();
        set_package_capture_start(&mut owned, capture_start);
        let rust = rust_readback_accepts_at(&owned, capture_start);
        let root = root_validator_accepts(&validator, &owned, label);
        assert_eq!(rust, root, "Rust/root mismatch for {label}");
        assert_eq!(rust, accepted, "unexpected result for {label}");
    }

    let mut owned = owned_package();
    set_package_capture_start(&mut owned, "2026-08-24T00:00:00.0Z");
    let rust = rust_readback_accepts(&owned);
    let root = root_validator_accepts(&validator, &owned, "capture-spelling-identical");
    assert_eq!(
        rust, root,
        "Rust/root mismatch for identical capture spelling"
    );
    assert!(rust);

    let mut owned = owned_package();
    set_admission_capture_start(&mut owned, "2026-08-24T00:00:00.0Z");
    let rust = rust_readback_accepts(&owned);
    let root = root_validator_accepts(&validator, &owned, "capture-spelling-mismatch");
    assert_eq!(
        rust, root,
        "Rust/root mismatch for capture spelling mismatch"
    );
    assert!(!rust);

    let mut owned = owned_package();
    mutate_manifest(&mut owned, |manifest| {
        manifest["causality"]["max_available_at"] = json!("2026-08-24T00:00:00.0150Z");
    });
    let rust = rust_readback_accepts(&owned);
    let root = root_validator_accepts(&validator, &owned, "max-artifact-spelling-differs");
    assert_eq!(rust, root, "Rust/root mismatch for max/artifact spelling");
    assert!(rust);

    for (label, capture_end, accepted) in [
        ("capture-end-tenth", "2026-08-24T00:00:16.1Z", true),
        ("capture-end-trailing-zero", "2026-08-24T00:00:16.10Z", true),
        (
            "capture-end-microsecond",
            "2026-08-24T00:00:16.000001Z",
            true,
        ),
        (
            "capture-end-overprecision",
            "2026-08-24T00:00:16.0000001Z",
            false,
        ),
        ("capture-end-offset", "2026-08-24T00:00:16+00:00", false),
        ("capture-end-year-zero", "0000-08-24T00:00:16Z", false),
        ("capture-end-three-digit-year", "026-08-24T00:00:16Z", false),
        (
            "capture-end-five-digit-year",
            "02026-08-24T00:00:16Z",
            false,
        ),
        ("capture-end-month", "2026-13-24T00:00:16Z", false),
        ("capture-end-day", "2026-08-32T00:00:16Z", false),
        ("capture-end-nonleap-day", "2026-02-29T00:00:16Z", false),
        ("capture-end-hour", "2026-08-24T24:00:16Z", false),
        ("capture-end-second", "2026-08-24T00:00:60Z", false),
    ] {
        let mut owned = owned_package();
        let mut manifest: Value = serde_json::from_slice(&owned[0].1).unwrap();
        manifest["capture"]["ended_at"] = json!(capture_end);
        owned[0].1 = canonical_line(&manifest);
        let rust = rust_readback_accepts(&owned);
        let root = root_validator_accepts(&validator, &owned, label);
        assert_eq!(rust, root, "Rust/root mismatch for {label}");
        assert_eq!(rust, accepted, "unexpected result for {label}");
    }

    for (label, decision_time, accepted) in [
        ("decision-tenth", "2026-08-24T00:00:17.1Z", true),
        ("decision-trailing-zero", "2026-08-24T00:00:17.10Z", true),
        ("decision-microsecond", "2026-08-24T00:00:17.000001Z", true),
        (
            "decision-overprecision",
            "2026-08-24T00:00:17.0000001Z",
            false,
        ),
        ("decision-offset", "2026-08-24T00:00:17+00:00", false),
    ] {
        let mut owned = owned_package();
        let mut manifest: Value = serde_json::from_slice(&owned[0].1).unwrap();
        manifest["causality"]["decision_time"] = json!(decision_time);
        owned[0].1 = canonical_line(&manifest);
        let rust = Rfc3339Time::parse(decision_time).is_ok()
            && rust_readback_accepts_at_with_decision(
                &owned,
                "2026-08-24T00:00:00Z",
                decision_time,
            );
        let root = root_validator_accepts(&validator, &owned, label);
        assert_eq!(rust, root, "Rust/root mismatch for {label}");
        assert_eq!(rust, accepted, "unexpected result for {label}");
    }

    for (label, bound, accepted) in [
        ("bound-short", "2026-08-24T00:00:00.002Z", true),
        ("bound-trailing-zero", "2026-08-24T00:00:00.0020Z", true),
        ("bound-overprecision", "2026-08-24T00:00:00.0020000Z", false),
        ("bound-offset", "2026-08-24T00:00:00.002+00:00", false),
    ] {
        let mut owned = owned_package();
        let mut manifest: Value = serde_json::from_slice(&owned[0].1).unwrap();
        manifest["artifacts"][1]["first_available_at"] = json!(bound);
        manifest["artifacts"][1]["last_available_at"] = json!(bound);
        owned[0].1 = canonical_line(&manifest);
        let rust = rust_readback_accepts(&owned);
        let root = root_validator_accepts(&validator, &owned, label);
        assert_eq!(rust, root, "Rust/root mismatch for {label}");
        assert_eq!(rust, accepted, "unexpected result for {label}");
    }

    for (label, reachable_at, accepted) in [
        ("amendment-tenth", "2026-08-22T07:35:52.1Z", true),
        ("amendment-microsecond", "2026-08-22T07:35:52.000001Z", true),
        ("amendment-trailing-zero", "2026-08-22T07:35:52.10Z", false),
        (
            "amendment-overprecision",
            "2026-08-22T07:35:52.0000001Z",
            false,
        ),
        ("amendment-offset", "2026-08-22T07:35:52+00:00", false),
        ("amendment-year-zero", "0000-08-22T07:35:52Z", false),
        ("amendment-month", "2026-13-22T07:35:52Z", false),
        ("amendment-day", "2026-02-30T07:35:52Z", false),
    ] {
        let mut owned = owned_package();
        let mut manifest: Value = serde_json::from_slice(&owned[0].1).unwrap();
        manifest["amendment_binding"]["default_reachable_at"] = json!(reachable_at);
        owned[0].1 = canonical_line(&manifest);
        let rust = rust_readback_accepts(&owned);
        let root = root_validator_accepts(&validator, &owned, label);
        assert_eq!(rust, root, "Rust/root mismatch for {label}");
        assert_eq!(rust, accepted, "unexpected result for {label}");
    }
}
