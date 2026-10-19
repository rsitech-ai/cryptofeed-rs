//! Namespace barriers shared by recording and dataset initialization.
use std::{fs::File, io, path::Path};

/// Synchronize the existing directory and its resolved ancestor chain, from
/// child to root. Syncing only the immediate parent misses newly created
/// grandparents, including paths left by an earlier failed initialization.
/// Errors are propagated; this is no substitute for a supported platform
/// directory-sync backend or storage-device durability qualification.
pub fn sync_directory_ancestry(directory: &Path) -> io::Result<()> {
    sync_ancestry_with(directory, |path| File::open(path)?.sync_all())
}

fn sync_ancestry_with(
    directory: &Path,
    mut sync: impl FnMut(&Path) -> io::Result<()>,
) -> io::Result<()> {
    let resolved = directory.canonicalize()?;
    if !resolved.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotADirectory,
            "directory required",
        ));
    }
    for ancestor in resolved.ancestors() {
        sync(ancestor)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_bootstrap_syncs_every_resolved_ancestor_and_propagates_failure() {
        let temp = tempfile::tempdir().unwrap();
        let directory = temp.path().join("new-parent/new-child/feed");
        std::fs::create_dir_all(&directory).unwrap();
        let resolved = directory.canonicalize().unwrap();
        let mut synced = Vec::new();
        sync_ancestry_with(&directory, |path| {
            synced.push(path.to_path_buf());
            File::open(path)?.sync_all()
        })
        .unwrap();
        assert_eq!(
            synced,
            resolved
                .ancestors()
                .map(Path::to_path_buf)
                .collect::<Vec<_>>()
        );
        let mut attempted = Vec::new();
        let failure = sync_ancestry_with(&directory, |path| {
            attempted.push(path.to_path_buf());
            if path.file_name().is_some_and(|name| name == "new-parent") {
                return Err(io::Error::other("injected ancestor barrier failure"));
            }
            Ok(())
        })
        .unwrap_err();
        assert!(failure.to_string().contains("injected"));
        assert_eq!(attempted, synced[..3]);
    }
}
