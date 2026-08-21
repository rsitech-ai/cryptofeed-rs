use marketfeed_event_pulse::{
    ContractError, EXPECTED_ROOT_COMMIT, EventPulseErrorCode, ProvenanceError, embedded_provenance,
    verify_artifact_bytes, verify_embedded_contracts, verify_manifest,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

fn rehash(mut value: Value) -> Value {
    value.as_object_mut().unwrap().remove("content_hash");
    value["content_hash"] = Value::String(marketfeed_event_pulse::content_hash(&value).unwrap());
    value
}

fn assert_named_contract_error(name: &str, error: ContractError) {
    let structural = matches!(
        name,
        "wrong_schema_version" | "float_score" | "forbidden_strategy_field"
    );
    if structural {
        assert!(
            matches!(error, ContractError::Structure(_)),
            "{name}: {error}"
        );
    } else {
        assert!(
            matches!(error, ContractError::Semantic(_)),
            "{name}: {error}"
        );
    }
}

fn expected_event_pulse_code(value: &Value) -> Option<EventPulseErrorCode> {
    match value["error_code"].as_str()? {
        "EVENTPULSE_HASH_BINDING" => Some(EventPulseErrorCode::HashBinding),
        "EVENTPULSE_IDENTITY" => Some(EventPulseErrorCode::Identity),
        "EVENTPULSE_INPUT_AVAILABILITY" => Some(EventPulseErrorCode::InputAvailability),
        "EVENTPULSE_NUMERIC_BOUNDS" => Some(EventPulseErrorCode::NumericBounds),
        "EVENTPULSE_ORDERING" => Some(EventPulseErrorCode::Ordering),
        "EVENTPULSE_QUALITY" => Some(EventPulseErrorCode::Quality),
        "EVENTPULSE_CONTEXT_REVISION" => Some(EventPulseErrorCode::ContextRevision),
        "FUTURE_AVAILABILITY" => Some(EventPulseErrorCode::FutureAvailability),
        "" => None,
        other => panic!("unknown published EventPulse error code {other}"),
    }
}

fn assert_exact_event_pulse_error(vector: &Value, error: ContractError) {
    if let Some(expected) = expected_event_pulse_code(vector) {
        assert_eq!(
            error,
            ContractError::EventPulse(expected),
            "{}",
            vector["name"]
        );
    } else {
        let _ = error;
    }
}

#[test]
fn q1_and_e1_published_vectors_are_wire_compatible() {
    let bundle = marketfeed_event_pulse::ContractBundle::load_embedded().expect("pinned bundle");
    for bytes in [
        include_bytes!("../contracts/quant-harness/quant_harness_v1_golden.json").as_slice(),
        include_bytes!("../contracts/event-pulse/event_pulse_v1_golden.json").as_slice(),
    ] {
        let vectors: Value = serde_json::from_slice(bytes).expect("published vectors");
        for vector in vectors["vectors"].as_array().expect("vectors") {
            let payload = serde_json::to_vec(&vector["payload"]).expect("payload");
            let accepted = bundle
                .validate_json(&payload)
                .unwrap_or_else(|error| panic!("{}: {error}", vector["name"]));
            assert_eq!(
                accepted.canonical_json(),
                vector["canonical_json"].as_str().unwrap()
            );
            assert_eq!(
                accepted.content_hash(),
                vector["content_hash"].as_str().unwrap()
            );
        }
    }
}

#[test]
fn q1_and_e1_published_rejections_fail_closed() {
    let bundle = marketfeed_event_pulse::ContractBundle::load_embedded().expect("pinned bundle");
    let q1: Value = serde_json::from_slice(include_bytes!(
        "../contracts/quant-harness/quant_harness_v1_rejections.json"
    ))
    .unwrap();
    for vector in q1["vectors"].as_array().unwrap() {
        let payload = serde_json::to_vec(&vector["payload"]).unwrap();
        if vector["name"] == "post_hoc_mutation" {
            let previous = bundle
                .validate_json(&serde_json::to_vec(&vector["previous"]).unwrap())
                .unwrap();
            let current = bundle.validate_json(&payload).unwrap();
            assert!(
                marketfeed_event_pulse::validate_revision_transition(&previous, &current).is_err()
            );
        } else {
            assert!(
                bundle.validate_json(&payload).is_err(),
                "{}",
                vector["name"]
            );
            let error = bundle.validate_json(&payload).unwrap_err();
            if vector["error_code"]
                .as_str()
                .is_some_and(|code| !code.is_empty())
            {
                assert_exact_event_pulse_error(vector, error);
            } else {
                assert_named_contract_error(vector["name"].as_str().unwrap(), error);
            }
        }
    }
    let e1: Value = serde_json::from_slice(include_bytes!(
        "../contracts/event-pulse/event_pulse_v1_rejections.json"
    ))
    .unwrap();
    for group in ["semantic_vectors", "structural_vectors"] {
        for vector in e1[group].as_array().unwrap() {
            let payload = serde_json::to_vec(&vector["payload"]).unwrap();
            if vector["operation"] == "bind_composite" {
                let mechanics = bundle
                    .validate_json(&serde_json::to_vec(&vector["mechanics"]).unwrap())
                    .unwrap();
                let context = bundle
                    .validate_json(&serde_json::to_vec(&vector["context"]).unwrap())
                    .unwrap();
                let composite_payload = rehash(vector["payload"].clone());
                let composite = bundle
                    .validate_json(&serde_json::to_vec(&composite_payload).unwrap())
                    .unwrap();
                assert_exact_event_pulse_error(
                    vector,
                    bundle
                        .bind_composite(&mechanics, Some(&context), &composite)
                        .unwrap_err(),
                );
            } else if vector["name"] == "context_revision_rewrites_evidence" {
                let previous = bundle
                    .validate_json(&serde_json::to_vec(&vector["previous"]).unwrap())
                    .unwrap();
                let current = bundle.validate_json(&payload).unwrap();
                assert_exact_event_pulse_error(
                    vector,
                    marketfeed_event_pulse::validate_context_revision(&previous, &current)
                        .unwrap_err(),
                );
            } else if vector["name"] == "eventpulse_revision_changes_scope" {
                let previous = bundle
                    .validate_json(&serde_json::to_vec(&vector["previous"]).unwrap())
                    .unwrap();
                let current = bundle.validate_json(&payload).unwrap();
                assert_exact_event_pulse_error(
                    vector,
                    marketfeed_event_pulse::validate_revision_transition(&previous, &current)
                        .unwrap_err(),
                );
            } else {
                assert_exact_event_pulse_error(vector, bundle.validate_json(&payload).unwrap_err());
            }
        }
    }
}

#[test]
fn provenance_accepts_exact_embedded_artifacts() {
    let verified = verify_embedded_contracts().expect("accepted contract bytes must verify");
    assert_eq!(verified.len(), 8);
    assert!(verified.iter().all(|artifact| !artifact.bytes.is_empty()));
}

#[test]
fn provenance_rejects_missing_or_drifted_bytes() {
    let manifest = embedded_provenance().expect("embedded provenance");

    let mut missing = manifest.clone();
    missing.artifacts[0].embedded_path = "contracts/quant-harness/missing.json".into();
    assert!(matches!(
        verify_manifest(&missing),
        Err(ProvenanceError::MissingArtifact { .. })
    ));

    let drifted = manifest;
    assert!(matches!(
        verify_artifact_bytes(&drifted.artifacts[0], b"drifted artifact bytes"),
        Err(ProvenanceError::ArtifactDrift { .. })
    ));
}

#[test]
fn provenance_rejects_wrong_root_commit() {
    let mut manifest = embedded_provenance().expect("embedded provenance");
    assert_eq!(manifest.source_root_commit, EXPECTED_ROOT_COMMIT);
    manifest.source_root_commit = "f".repeat(40);
    assert!(matches!(
        verify_manifest(&manifest),
        Err(ProvenanceError::WrongRootCommit { .. })
    ));
}

#[test]
fn provenance_rejects_escaped_or_duplicate_paths() {
    let manifest = embedded_provenance().expect("embedded provenance");

    let mut escaped = manifest.clone();
    escaped.artifacts[0].source_path = "../escaped.json".into();
    assert!(matches!(
        verify_manifest(&escaped),
        Err(ProvenanceError::UnsafePath { .. })
    ));

    let mut duplicate = manifest;
    duplicate.artifacts[1].source_path = duplicate.artifacts[0].source_path.clone();
    assert!(matches!(
        verify_manifest(&duplicate),
        Err(ProvenanceError::DuplicatePath { .. })
    ));
}

#[test]
fn provenance_rejects_unapproved_family() {
    let mut manifest = embedded_provenance().expect("embedded provenance");
    manifest.artifacts[0].family = "execution/1.0".into();
    assert!(matches!(
        verify_manifest(&manifest),
        Err(ProvenanceError::UnapprovedFamily { .. })
    ));
}

#[test]
fn provenance_rejects_coordinated_bytes_and_metadata_drift() {
    let mut manifest = embedded_provenance().expect("embedded provenance");
    let drifted_bytes = b"coordinated artifact and metadata drift";
    manifest.artifacts[0].byte_length = drifted_bytes.len() as u64;
    manifest.artifacts[0].sha256 = format!("{:x}", Sha256::digest(drifted_bytes));

    assert!(matches!(
        verify_artifact_bytes(&manifest.artifacts[0], drifted_bytes),
        Err(ProvenanceError::PinnedRecordMismatch { .. })
    ));
}

#[test]
fn provenance_rejects_safe_wrong_source_path() {
    let mut manifest = embedded_provenance().expect("embedded provenance");
    manifest.artifacts[0].source_path =
        "research_os/schemas/quant-harness/renamed.schema.json".into();

    assert!(matches!(
        verify_manifest(&manifest),
        Err(ProvenanceError::PinnedRecordMismatch { .. })
    ));
}

#[test]
fn provenance_rejects_approved_wrong_family_swap() {
    let mut manifest = embedded_provenance().expect("embedded provenance");
    manifest.artifacts[0].family = "event-pulse/1.0".into();

    assert!(matches!(
        verify_manifest(&manifest),
        Err(ProvenanceError::PinnedRecordMismatch { .. })
    ));
}

#[test]
fn provenance_rejects_unknown_top_level_or_artifact_field() {
    let manifest = embedded_provenance().expect("embedded provenance");

    let mut top_level = serde_json::to_value(&manifest).expect("serialize provenance");
    top_level["unknown"] = serde_json::json!(true);
    assert!(
        serde_json::from_value::<marketfeed_event_pulse::ProvenanceManifest>(top_level).is_err()
    );

    let mut artifact = serde_json::to_value(&manifest).expect("serialize provenance");
    artifact["artifacts"][0]["unknown"] = serde_json::json!(true);
    assert!(
        serde_json::from_value::<marketfeed_event_pulse::ProvenanceManifest>(artifact).is_err()
    );
}
