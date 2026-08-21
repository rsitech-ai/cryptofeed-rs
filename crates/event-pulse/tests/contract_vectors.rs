use marketfeed_event_pulse::{
    EXPECTED_ROOT_COMMIT, ProvenanceError, embedded_provenance, verify_embedded_contracts,
    verify_manifest,
};

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

    let mut drifted = manifest;
    drifted.artifacts[0].sha256 = "0".repeat(64);
    assert!(matches!(
        verify_manifest(&drifted),
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
