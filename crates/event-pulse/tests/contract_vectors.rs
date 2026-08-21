use marketfeed_event_pulse::{
    EXPECTED_ROOT_COMMIT, ProvenanceError, embedded_provenance, verify_artifact_bytes,
    verify_embedded_contracts, verify_manifest,
};
use sha2::{Digest, Sha256};

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
