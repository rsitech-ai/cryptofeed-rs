//! The only unsafe code is three synchronous Win32 filesystem identity queries.
//! Ownership and flushing remain in std::fs::File. No raw handle escapes.
use crate::{require_directory, volume};
use std::{
    fs::OpenOptions,
    io,
    os::windows::{fs::OpenOptionsExt, io::AsRawHandle},
    path::Path,
    ptr,
};
use windows_sys::Win32::Foundation::GENERIC_WRITE;
use windows_sys::Win32::Storage::FileSystem::{
    FILE_FLAG_BACKUP_SEMANTICS, FILE_READ_ATTRIBUTES, GetDriveTypeW, GetFinalPathNameByHandleW,
    GetVolumeInformationByHandleW, VOLUME_NAME_GUID,
};

pub(super) fn sync_directory(path: &Path) -> io::Result<()> {
    // std keeps FILE_SHARE_READ|WRITE|DELETE, allowing live readers and atomic
    // replacement. GENERIC_WRITE is required by FlushFileBuffers; std metadata
    // checks also need FILE_READ_ATTRIBUTES. No creation or truncation flags.
    let file = OpenOptions::new()
        .write(true)
        .access_mode(GENERIC_WRITE | FILE_READ_ATTRIBUTES)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?;
    require_directory(&file)?;
    let handle = file.as_raw_handle();
    let mut filesystem = [0u16; 261];
    // SAFETY: handle is borrowed from the live owned File. The only non-null
    // output points to an initialized writable buffer of the supplied WCHAR
    // capacity. Optional outputs are null. This synchronous API retains none.
    let ok = unsafe {
        GetVolumeInformationByHandleW(
            handle,
            ptr::null_mut(),
            0,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            filesystem.as_mut_ptr(),
            filesystem.len() as u32,
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    let filesystem = volume::nul_terminated_utf16(&filesystem)?;
    let mut final_path = vec![0u16; 32768];
    // SAFETY: the same File still owns handle; final_path owns the initialized
    // writable WCHAR buffer for its entire supplied capacity. No pointers are
    // retained. The returned length is checked before any slice is constructed.
    let length = unsafe {
        GetFinalPathNameByHandleW(
            handle,
            final_path.as_mut_ptr(),
            final_path.len() as u32,
            VOLUME_NAME_GUID,
        )
    };
    if length == 0 {
        return Err(io::Error::last_os_error());
    }
    if length as usize >= final_path.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "native directory path exceeds query bound",
        ));
    }
    let final_path = String::from_utf16(&final_path[..length as usize]).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid native directory path UTF16",
        )
    })?;
    let root = volume::volume_guid_root(&final_path)?;
    let root: Vec<u16> = root.encode_utf16().chain(Some(0)).collect();
    // SAFETY: root is a validated volume GUID root with trailing backslash,
    // encoded into a live NUL-terminated buffer. GetDriveTypeW only reads it
    // synchronously and retains no pointer. No drive-letter/network fallback.
    let drive_type = unsafe { GetDriveTypeW(root.as_ptr()) };
    volume::require_local_ntfs(&filesystem, drive_type)?;
    file.sync_all()
}
