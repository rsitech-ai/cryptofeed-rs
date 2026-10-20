# Local filesystem durability boundary

The `marketfeed-filesystem` crate supplies one safe `sync_directory(&Path)`
operation. Recording's public durability helper uses it for directory ancestry,
shared-feed watermark replacement, and raw-segment installation. Datasets uses
the same backend for immutable objects, intents, HEAD and backup publication.
Regular-file data/metadata syncs, replacement order, writer exclusion, poisoning
and recovery remain owned by their existing components. No file format changes.

On Unix, the backend opens the directory, checks its type through that same
handle and calls `File::sync_all`. On Windows, it opens without creation or
truncation, with `GENERIC_WRITE | FILE_READ_ATTRIBUTES`,
`FILE_FLAG_BACKUP_SEMANTICS` and Rust's default read/write/delete sharing. The
handle stays alive through metadata validation, both identity queries and flush.

Windows admission requires all of:

- An opened directory with successful metadata retrieval.
- `GetVolumeInformationByHandleW` reports exactly NTFS, case insensitive.
- `GetFinalPathNameByHandleW(VOLUME_NAME_GUID)` returns a bounded valid UTF16
  path with a complete volume GUID root. No drive-letter or UNC fallback.
- `GetDriveTypeW` reports `DRIVE_FIXED` for that GUID root.
- `File::sync_all` succeeds, invoking the normal `FlushFileBuffers` barrier.

Unknown/query-failed/oversize/unsupported storage returns an error. NTFS support
is deliberately explicit because Microsoft's MS-FSA directory-structure flush
guarantee is NTFS specific. Fixed-volume classification and native GUID identity
exclude ordinary network-share paths; they do not prove physical disk location,
device-cache honesty, hardware resilience or storage-stack power-loss behavior.
This backend does not implement object-store publication or remote-share durability.

Initialization syncs the resolved directory and every ancestor from child to
volume root, including a chain left by an earlier failed initialization. Windows
therefore needs appropriate write/attribute access on those ancestors too.
Owning the leaf directory alone does not establish sufficient permissions.
The earlier hosted API probe used an administrative token; it is not evidence
of ordinary-user usability. Permission failures remain errors; no automatic
privilege elevation, ignored barrier or successful no-op fallback exists.

Only a private Windows module permits unsafe Rust: three synchronous filesystem
identity queries with borrowed live File handles and initialized bounded buffers.
No raw handles/pointers escape. The crate otherwise denies unsafe code. Recording,
datasets and the financial crates retain their workspace prohibition. The Windows
binding uses already-locked windows-sys 0.61.2; no registry package is updated.

Adding Windows to cargo-deny's target graph exposes fs4's existing incompatible
windows-sys 0.59 binding. A version-specific duplicate-binding exception records
that dependency edge; it is not an advisory ignore. Full-lock RustSec remains a
separate gate and retains only the pre-existing time/MSRV exception.

Offline checks cover unsupported filesystem/media classifications, malformed
GUID/UNC paths, invalid/unterminated UTF16, real directory barriers, reader
coexistence, nested bootstrap and the existing storage failure/recovery cases.
The dedicated Windows job runs the actual recording/backend tests on Rust 1.85
and dataset publication/recovery on Rust 1.88, in addition to the full platform
matrix. The expanded job has a 20-minute budget for toolchain installation and
compilation; it does not alter runtime shutdown or failure deadlines. Runtime
qualification is pending until those actual native results are inspected.

Primary contracts:

- [Directory handles](https://learn.microsoft.com/en-us/windows/win32/fileio/obtaining-a-handle-to-a-directory)
- [FlushFileBuffers](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers)
- [Volume identity by handle](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getvolumeinformationbyhandlew)
- [Final path by handle](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getfinalpathnamebyhandlew)
- [Drive type](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getdrivetypew)
- [MS-FSA flush operation](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/0de7dc40-9627-437e-a4df-c4696cdc3d02)
- [MS-FSA product note 80](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/4e3695bd-7574-4f24-a223-b4679c065b63)
