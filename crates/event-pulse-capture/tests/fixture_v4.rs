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
