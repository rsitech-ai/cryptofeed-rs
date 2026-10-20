use std::io;

pub(crate) const DRIVE_FIXED: u32 = 3;

pub(crate) fn require_local_ntfs(filesystem: &str, drive_type: u32) -> io::Result<()> {
    if !filesystem.eq_ignore_ascii_case("NTFS") || drive_type != DRIVE_FIXED {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Windows durable directory publication requires a verified local fixed NTFS volume",
        ));
    }
    Ok(())
}

pub(crate) fn volume_guid_root(final_path: &str) -> io::Result<&str> {
    const PREFIX: &str = r"\\?\Volume{";
    let unsupported = || {
        io::Error::new(
            io::ErrorKind::Unsupported,
            "directory handle did not resolve to a volume GUID path",
        )
    };
    if final_path.contains('\0')
        || !final_path
            .get(..PREFIX.len())
            .is_some_and(|s| s.eq_ignore_ascii_case(PREFIX))
    {
        return Err(unsupported());
    }
    let (guid, _) = final_path[PREFIX.len()..]
        .split_once("}\\")
        .ok_or_else(unsupported)?;
    if guid.len() != 36
        || !guid.bytes().enumerate().all(|(index, byte)| {
            if [8, 13, 18, 23].contains(&index) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
    {
        return Err(unsupported());
    }
    Ok(&final_path[..PREFIX.len() + guid.len() + 2])
}

pub(crate) fn nul_terminated_utf16(buffer: &[u16]) -> io::Result<String> {
    let end = buffer.iter().position(|value| *value == 0).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "unterminated native filesystem name",
        )
    })?;
    String::from_utf16(&buffer[..end]).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid native filesystem name UTF16",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_fixed_ntfs_is_admitted() {
        require_local_ntfs("NTFS", DRIVE_FIXED).unwrap();
        for filesystem in ["", "ReFS", "FAT", "FAT32", "exFAT", "CSVFS", "NTFS "] {
            assert_eq!(
                require_local_ntfs(filesystem, DRIVE_FIXED)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::Unsupported
            );
        }
        for drive in [0, 1, 2, 4, 5, 6, 7, u32::MAX] {
            assert!(require_local_ntfs("NTFS", drive).is_err());
        }
    }

    #[test]
    fn guid_root_requires_complete_native_volume_identity() {
        const ROOT: &str = r"\\?\Volume{7765622a-0000-0000-0000-100000000000}\";
        assert_eq!(
            volume_guid_root(&format!("{ROOT}some\\directory")).unwrap(),
            ROOT
        );
        assert_eq!(volume_guid_root(ROOT).unwrap(), ROOT);
        for path in [
            r"C:\data",
            r"\\?\C:\data",
            r"\\server\share\data",
            r"\\?\UNC\server\share\data",
            r"\\?\Volume{invalid}\data",
            r"\\?\Volume{7765622a-0000-0000-0000-100000000000}data",
            r"\\?\Volume{7765622a-0000-0000-0000-100000000000/}\data",
            "\0",
            "💰",
        ] {
            assert!(volume_guid_root(path).is_err(), "{path:?}");
        }
    }

    #[test]
    fn native_name_requires_terminated_valid_utf16() {
        assert_eq!(
            nul_terminated_utf16(&[78, 84, 70, 83, 0, 99]).unwrap(),
            "NTFS"
        );
        assert!(nul_terminated_utf16(&[78, 84, 70, 83]).is_err());
        assert!(nul_terminated_utf16(&[0xd800, 0]).is_err());
    }
}
