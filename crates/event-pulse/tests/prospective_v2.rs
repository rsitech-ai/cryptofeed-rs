use marketfeed_event_pulse::{
    ProspectiveAdmissionErrorV2, ProspectiveCaptureAdmissionV2, ProspectiveSystemArtifactPolicyV2,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn valid_admission() -> Value {
    json!({
        "schema": "event-pulse-e2-prospective-admission/2.0",
        "topology_binding": {
            "repository_url": "https://github.com/s1korrrr/rsibot.git",
            "merge_commit": "05994ccd514ddb69fdd5c21a8c78af8bbe75d506",
            "merged_at": "2026-08-23T06:58:18Z",
            "path": "docs/superpowers/specs/event-pulse-e2-producer-evidence-freeze-v2.json",
            "byte_length": 6955,
            "sha256": "7216d9c5bc4b5bcd463b644c53309594413608586288beb0d32623412a42f0d7"
        },
        "wire_contract_binding": {
            "repository_url": "https://github.com/s1korrrr/rsibot.git",
            "merge_commit": "44f3e091cb47c1b081f673e8bb09e8723a2090c6",
            "merged_at": "2026-08-23T08:10:48Z",
            "path": "docs/superpowers/specs/event-pulse-e2-wire-admission-v2-contract.json",
            "byte_length": 10119,
            "sha256": "dc79576062caf952be44e4808359c4328c2976282291838973d4884fadafa50b"
        },
        "capture_starts_at": "2026-08-23T08:10:48.000001Z",
        "evidence_claim": "PROSPECTIVE_CAUSAL_CAPTURE",
        "source_qualification": "UNVERIFIED",
        "authority": {
            "allocation_allowed": false,
            "canary_allowed": false,
            "capture_allowed": false,
            "credentials_allowed": false,
            "evidence_authoring_allowed": false,
            "execution_allowed": false,
            "live_allowed": false,
            "orders_allowed": false,
            "paper_allowed": false,
            "private_endpoints_allowed": false,
            "promotion_allowed": false,
            "risk_allowed": false
        }
    })
}

#[test]
fn root_contract_pins_are_exact_and_independent() {
    let topology =
        include_bytes!("../contracts/prospective/event-pulse-e2-producer-evidence-freeze-v2.json");
    let wire =
        include_bytes!("../contracts/prospective/event-pulse-e2-wire-admission-v2-contract.json");

    assert_eq!(topology.len(), 6_955);
    assert_eq!(
        format!("{:x}", Sha256::digest(topology)),
        "7216d9c5bc4b5bcd463b644c53309594413608586288beb0d32623412a42f0d7"
    );
    assert_eq!(wire.len(), 10_119);
    assert_eq!(
        format!("{:x}", Sha256::digest(wire)),
        "dc79576062caf952be44e4808359c4328c2976282291838973d4884fadafa50b"
    );

    let historical: serde_json::Value =
        serde_json::from_slice(include_bytes!("../contracts/provenance.json")).unwrap();
    assert!(
        historical["artifacts"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| !row["embedded_path"]
                .as_str()
                .unwrap()
                .contains("producer-evidence-freeze-v2")
                && !row["embedded_path"]
                    .as_str()
                    .unwrap()
                    .contains("wire-admission-v2"))
    );
}

#[test]
fn admission_v2_derives_exact_fixed_topology_and_truthful_empty_policy() {
    let bytes = serde_json::to_vec(&valid_admission()).unwrap();
    let admission = ProspectiveCaptureAdmissionV2::from_json(&bytes).unwrap();
    let config = admission.mechanics_config();
    assert_eq!(config.connections().len(), 3);
    assert_eq!(config.contributors().len(), 3);
    assert_eq!(config.clock_sources().len(), 3);
    assert_eq!(config.coverage_sources().len(), 6);
    assert_eq!(config.system_sources().len(), 1);
    assert_eq!(admission.unique_non_system_source_count(), 12);
    assert!(!admission.evidence_authoring_allowed());
    assert_eq!(admission.blocker(), "blocked:fixture-provenance");

    let policy = ProspectiveSystemArtifactPolicyV2::from_admission(&admission).unwrap();
    assert_eq!(policy.mode(), "TRUTHFUL_EMPTY");
    assert!(!policy.evidence_authoring_allowed());
}

#[test]
fn admission_v2_rejects_binding_timing_authority_and_canonical_drift() {
    for (pointer, replacement, expected) in [
        (
            "/topology_binding/sha256",
            json!("0".repeat(64)),
            ProspectiveAdmissionErrorV2::RootBinding,
        ),
        (
            "/wire_contract_binding/merge_commit",
            json!("0".repeat(40)),
            ProspectiveAdmissionErrorV2::RootBinding,
        ),
        (
            "/capture_starts_at",
            json!("2026-08-23T08:10:48Z"),
            ProspectiveAdmissionErrorV2::CaptureTiming,
        ),
        (
            "/authority/capture_allowed",
            json!(true),
            ProspectiveAdmissionErrorV2::Authority,
        ),
    ] {
        let mut value = valid_admission();
        *value.pointer_mut(pointer).unwrap() = replacement;
        assert_eq!(
            ProspectiveCaptureAdmissionV2::from_json(&serde_json::to_vec(&value).unwrap()),
            Err(expected)
        );
    }
    let canonical = serde_json::to_vec(&valid_admission()).unwrap();
    let spaced = [b" ".as_slice(), canonical.as_slice()].concat();
    assert_eq!(
        ProspectiveCaptureAdmissionV2::from_json(&spaced),
        Err(ProspectiveAdmissionErrorV2::Shape)
    );
}
