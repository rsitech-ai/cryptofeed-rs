//! Checked directory synchronization. Windows support is limited to local fixed
//! NTFS volumes; unsupported or unknown storage is an error, never a no-op.
#![deny(unsafe_code)]

use std::{io, path::Path};

#[cfg(any(windows, test))]
mod volume;
#[cfg(windows)]
#[allow(unsafe_code)]
mod windows;

/// Synchronize an opened directory's namespace. Windows requires write access
/// (including on bootstrap ancestors), successful handle-based NTFS/locality
/// checks and FlushFileBuffers through File::sync_all. Unix uses directory
/// fsync. Success follows the filesystem/OS barrier contract, not a guarantee
/// against lying device caches or storage hardware failure.
pub fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        windows::sync_directory(path)
    }
    #[cfg(unix)]
    {
        let file = std::fs::File::open(path)?;
        require_directory(&file)?;
        file.sync_all()
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "no directory durability backend for this platform",
        ))
    }
}

fn require_directory(file: &std::fs::File) -> io::Result<()> {
    if !file.metadata()?.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotADirectory,
            "directory required",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn real_directory_barrier_rejects_regular_and_missing_files() {
        let temp = tempfile::tempdir().unwrap();
        sync_directory(temp.path()).unwrap();
        let regular = temp.path().join("regular");
        std::fs::write(&regular, b"bytes").unwrap();
        assert_eq!(
            sync_directory(&regular).unwrap_err().kind(),
            io::ErrorKind::NotADirectory
        );
        assert!(sync_directory(&temp.path().join("missing")).is_err());
    }
}
